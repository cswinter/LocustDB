use itertools::Itertools;
use locustdb_derive::reify_types;
use regex::Regex;

use engine::*;
use engine::Aggregator;
use ingest::raw_val::RawVal;
use mem_store::*;
use QueryError;
use std::fmt::Write;
use std::intrinsics::type_name;
use std::marker::PhantomData;
use std::result::Result;

use super::aggregate::*;
use super::assemble_nullable::AssembleNullable;
use super::binary_operator::*;
use super::bit_unpack::BitUnpackOperator;
use super::bool_op::*;
use super::column_ops::*;
use super::combine_null_maps::CombineNullMaps;
use super::compact::Compact;
use super::comparison_operators::*;
use super::constant::Constant;
use super::constant_expand::ConstantExpand;
use super::constant_vec::ConstantVec;
use super::delta_decode::*;
use super::dict_lookup::*;
use super::encode_const::*;
use super::exists::Exists;
use super::filter::{Filter, NullableFilter};
use super::functions::*;
use super::fuse_nulls::*;
use super::hashmap_grouping::HashMapGrouping;
use super::hashmap_grouping_byte_slices::HashMapGroupingByteSlices;
use super::hashmap_grouping_val_rows::HashMapGroupingValRows;
use super::identity::Identity;
use super::indices::Indices;
use super::is_null::*;
use super::make_nullable::MakeNullable;
use super::map_operator::MapOperator;
use super::merge::Merge;
use super::merge_aggregate::MergeAggregate;
use super::merge_deduplicate::MergeDeduplicate;
use super::merge_deduplicate_partitioned::MergeDeduplicatePartitioned;
use super::merge_drop::MergeDrop;
use super::merge_keep::*;
use super::merge_partitioned::MergePartitioned;
use super::nonzero_compact::NonzeroCompact;
use super::nonzero_indices::NonzeroIndices;
use super::null_vec::NullVec;
use super::numeric_operators::*;
use super::parameterized_vec_vec_int_op::*;
use super::partition::Partition;
use super::propagate_nullability::PropagateNullability;
use super::scalar_i64::ScalarI64;
use super::scalar_str::ScalarStr;
use super::select::*;
use super::slice_pack::*;
use super::slice_unpack::*;
use super::sort_by::*;
use super::sort_by_slices::SortBySlices;
use super::sort_by_val_rows::SortByValRows;
use super::subpartition::SubPartition;
use super::to_val::*;
use super::top_n::TopN;
use super::type_conversion::TypeConversionOperator;
use super::unhexpack_strings::UnhexpackStrings;
use super::unpack_strings::UnpackStrings;
use super::val_rows_pack::*;
use super::val_rows_unpack::*;

pub type BoxedOperator<'a> = Box<VecOperator<'a> + 'a>;

pub trait VecOperator<'a> {
    fn execute(&mut self, stream: bool, scratchpad: &mut Scratchpad<'a>);
    fn finalize(&mut self, _scratchpad: &mut Scratchpad<'a>) {}
    fn init(&mut self, _total_count: usize, _batch_size: usize, _scratchpad: &mut Scratchpad<'a>) {}

    fn inputs(&self) -> Vec<BufferRef<Any>>;
    fn outputs(&self) -> Vec<BufferRef<Any>>;
    fn can_stream_input(&self, i: usize) -> bool;
    fn can_stream_output(&self, i: usize) -> bool;
    fn mutates(&self, _i: usize) -> bool { false }
    fn allocates(&self) -> bool;
    fn is_streaming_producer(&self) -> bool { false }
    fn has_more(&self) -> bool { false }
    fn custom_output_len(&self) -> Option<usize> { None }

    fn display(&self, full: bool) -> String {
        let mut s = String::new();
        // Spacing is off if ansi color codes are used
        if self.display_output() {
            write!(s, "{:<12} = ", self.outputs().iter().map(|o| format!("{}", o)).join(", ")).unwrap();
        }
        write!(s, "{}", self.display_op(full)).unwrap();
        format!("{:<60} {}", s, short_type_name::<Self>())
    }

    fn display_output(&self) -> bool { true }
    fn display_op(&self, alternate: bool) -> String;
}


fn short_type_name<T: ?Sized>() -> String {
    let full_name = unsafe { type_name::<T>() };
    let re = Regex::new(r"\w+::").unwrap();
    re.replace_all(full_name, "").into_owned()
}


impl<'a> VecOperator<'a> {
    pub fn read_column_data(colname: String,
                            section_index: usize,
                            output: BufferRef<Any>) -> BoxedOperator<'a> {
        Box::new(ReadColumnData {
            colname,
            section_index,
            output,
            batch_size: 0,
            current_index: 0,
            has_more: true,
        })
    }

    pub fn nullable(data: TypedBufferRef,
                    present: BufferRef<u8>,
                    nullable_data: TypedBufferRef) -> Result<BoxedOperator<'a>, QueryError> {
        match data.tag {
            EncodingType::U8 => Ok(Box::new(AssembleNullable { data: data.u8()?, present, nullable_data: nullable_data.nullable_u8()? })),
            EncodingType::I64 => Ok(Box::new(AssembleNullable { data: data.i64()?, present, nullable_data: nullable_data.nullable_i64()? })),
            EncodingType::Str => Ok(Box::new(AssembleNullable { data: data.str()?, present, nullable_data: nullable_data.nullable_str()? })),
            _ => Err(fatal!("nullable not implemented for type {:?}", data.tag)),
        }
    }

    pub fn make_nullable(data: TypedBufferRef,
                         present: BufferRef<u8>,
                         nullable_data: TypedBufferRef) -> Result<BoxedOperator<'a>, QueryError> {
        match data.tag {
            EncodingType::U8 => Ok(Box::new(MakeNullable { data: data.u8()?, present, nullable_data: nullable_data.nullable_u8()? })),
            EncodingType::I64 => Ok(Box::new(MakeNullable { data: data.i64()?, present, nullable_data: nullable_data.nullable_i64()? })),
            EncodingType::Str => Ok(Box::new(MakeNullable { data: data.str()?, present, nullable_data: nullable_data.nullable_str()? })),
            _ => Err(fatal!("make_nullable not implemented for type {:?}", data.tag)),
        }
    }

    pub fn combine_null_maps(lhs: TypedBufferRef,
                             rhs: TypedBufferRef,
                             output: BufferRef<u8>) -> Result<BoxedOperator<'a>, QueryError> {
        Ok(Box::new(CombineNullMaps { lhs: lhs.nullable_any()?, rhs: rhs.nullable_any()?, output }))
    }

    pub fn propagate_nullability(nullability: BufferRef<Nullable<Any>>,
                                 data: TypedBufferRef,
                                 output: TypedBufferRef) -> Result<BoxedOperator<'a>, QueryError> {
        match data.tag {
            EncodingType::U8 => Ok(Box::new(PropagateNullability { from: nullability, to: data.u8()?, output: output.nullable_u8()? })),
            EncodingType::I64 => Ok(Box::new(PropagateNullability { from: nullability, to: data.i64()?, output: output.nullable_i64()? })),
            EncodingType::Str => Ok(Box::new(PropagateNullability { from: nullability, to: data.str()?, output: output.nullable_str()? })),
            _ => Err(fatal!("propagate_nullability not implemented for type {:?}", data.tag)),
        }
    }

    pub fn fuse_nulls(input: TypedBufferRef, fused: TypedBufferRef) -> Result<BoxedOperator<'a>, QueryError> {
        if input.tag == EncodingType::NullableI64 {
            Ok(Box::new(FuseNullsI64 { input: input.nullable_i64()?, fused: fused.i64()? }))
        } else {
            Ok(Box::new(FuseNullsStr { input: input.nullable_str()?, fused: fused.opt_str()? }))
        }
    }

    pub fn fuse_int_nulls(offset: i64, input: TypedBufferRef, fused: TypedBufferRef) -> Result<BoxedOperator<'a>, QueryError> {
        match fused.tag {
            EncodingType::U8 => Ok(Box::new(FuseIntNulls { offset: offset as u8, input: input.nullable_u8()?, fused: fused.u8()? })),
            EncodingType::U16 => Ok(Box::new(FuseIntNulls { offset: offset as u16, input: input.nullable_u16()?, fused: fused.u16()? })),
            EncodingType::U32 => Ok(Box::new(FuseIntNulls { offset: offset as u32, input: input.nullable_u32()?, fused: fused.u32()? })),
            EncodingType::I64 => Ok(Box::new(FuseIntNulls { offset: offset as i64, input: input.nullable_i64()?, fused: fused.i64()? })),
            _ => Err(fatal!("fuse_int_nulls not implemented for type {:?}", fused.tag)),
        }
    }

    pub fn unfuse_int_nulls(offset: i64,
                            fused: TypedBufferRef,
                            data: TypedBufferRef,
                            present: BufferRef<u8>,
                            unfused: TypedBufferRef) -> Result<BoxedOperator<'a>, QueryError> {
        match fused.tag {
            EncodingType::U8 => Ok(Box::new(UnfuseIntNulls { offset: offset as u8, fused: fused.u8()?, data: data.u8()?, present, unfused: unfused.nullable_u8()? })),
            EncodingType::U16 => Ok(Box::new(UnfuseIntNulls { offset: offset as u16, fused: fused.u16()?, data: data.u16()?, present, unfused: unfused.nullable_u16()? })),
            EncodingType::U32 => Ok(Box::new(UnfuseIntNulls { offset: offset as u32, fused: fused.u32()?, data: data.u32()?, present, unfused: unfused.nullable_u32()? })),
            EncodingType::I64 => Ok(Box::new(UnfuseIntNulls { offset: offset as i64, fused: fused.i64()?, data: data.i64()?, present, unfused: unfused.nullable_i64()? })),
            _ => Err(fatal!("unfuse_int_nulls not implemented for type {:?}", fused.tag)),
        }
    }

    pub fn unfuse_nulls(fused: TypedBufferRef,
                        data: TypedBufferRef,
                        present: BufferRef<u8>,
                        unfused: TypedBufferRef) -> Result<BoxedOperator<'a>, QueryError> {
        if fused.tag == EncodingType::I64 {
            Ok(Box::new(UnfuseNullsI64 { fused: fused.i64()?, present, unfused: unfused.nullable_i64()? }))
        } else {
            Ok(Box::new(UnfuseNullsStr { fused: fused.opt_str()?, data: data.str()?, present, unfused: unfused.nullable_str()? }))
        }
    }

    pub fn is_null(input: BufferRef<Nullable<Any>>, is_null: BufferRef<u8>) -> BoxedOperator<'a> {
        Box::new(IsNull { input, is_null })
    }

    pub fn is_not_null(input: BufferRef<Nullable<Any>>, is_not_null: BufferRef<u8>) -> BoxedOperator<'a> {
        Box::new(IsNotNull { input, is_not_null })
    }

    pub fn identity(input: TypedBufferRef, output: TypedBufferRef) -> BoxedOperator<'a> {
        Box::new(Identity {
            input: input.any(),
            output: output.any(),
        })
    }

    pub fn dict_lookup(indices: TypedBufferRef,
                       dict_indices: BufferRef<u64>,
                       dict_data: BufferRef<u8>,
                       output: BufferRef<&'a str>) -> Result<BoxedOperator<'a>, QueryError> {
        reify_types![
            "dict_lookup";
            indices: Integer;
            Ok(Box::new(DictLookup { indices, output, dict_indices, dict_data }))
        ]
    }

    #[cfg(feature = "enable_lz4")]
    pub fn lz4_decode(encoded: BufferRef<u8>,
                      decoded_len: usize,
                      decoded: TypedBufferRef) -> Result<BoxedOperator<'a>, QueryError> {
        use super::lz4_decode::LZ4Decode;
        use std::io::Read;
        let reader: Box<Read> = Box::new(&[] as &[u8]);
        reify_types! {
            "lz4_decode";
            decoded: Integer;
            Ok(Box::new(LZ4Decode::<'a, _> { encoded, decoded, decoded_len, reader, has_more: true }))
        }
    }

    #[cfg(not(feature = "enable_lz4"))]
    pub fn lz4_decode(_: BufferRef<u8>,
                      _: usize,
                      _: TypedBufferRef) -> Result<BoxedOperator<'a>, QueryError> {
        panic!("LZ4 is not enabled in this build of LocustDB. Recompile with `features enable_lz4`")
    }

    pub fn unpack_strings(packed: BufferRef<u8>, unpacked: BufferRef<&'a str>) -> BoxedOperator<'a> {
        Box::new(UnpackStrings::<'a> { packed, unpacked, iterator: None, has_more: true })
    }

    pub fn unhexpack_strings(packed: BufferRef<u8>,
                             uppercase: bool,
                             total_bytes: usize,
                             stringstore: BufferRef<u8>,
                             unpacked: BufferRef<&'a str>) -> BoxedOperator<'a> {
        Box::new(UnhexpackStrings::<'a> { packed, unpacked, stringstore, uppercase, total_bytes, iterator: None, has_more: true })
    }

    pub fn delta_decode(encoded: TypedBufferRef, decoded: BufferRef<i64>) -> Result<BoxedOperator<'a>, QueryError> {
        reify_types! {
            "delta_decode";
            encoded: Integer;
            Ok(Box::new(DeltaDecode { encoded, decoded, previous: 0 }))
        }
    }

    pub fn inverse_dict_lookup(dict_indices: BufferRef<u64>,
                               dict_data: BufferRef<u8>,
                               constant: BufferRef<Scalar<&'a str>>,
                               output: BufferRef<Scalar<i64>>) -> BoxedOperator<'a> {
        Box::new(InverseDictLookup { dict_indices, dict_data, constant, output })
    }

    pub fn encode_int_const(constant: BufferRef<Scalar<i64>>,
                            codec: Codec,
                            output: BufferRef<Scalar<i64>>) -> BoxedOperator<'a> {
        Box::new(EncodeIntConstant { constant, output, codec })
    }

    pub fn filter(input: TypedBufferRef,
                  filter: BufferRef<u8>,
                  output: TypedBufferRef) -> Result<BoxedOperator<'a>, QueryError> {
        reify_types! {
            "filter";
            input, output: PrimitiveUSize;
            Ok(Box::new(Filter { input, filter, output }))
        }
    }

    pub fn nullable_filter(input: TypedBufferRef,
                           filter: BufferRef<Nullable<u8>>,
                           output: TypedBufferRef) -> Result<BoxedOperator<'a>, QueryError> {
        reify_types! {
            "nullable_filter";
            input, output: PrimitiveUSize;
            Ok(Box::new(NullableFilter { input, filter, output }))
        }
    }

    pub fn select(input: TypedBufferRef,
                  indices: BufferRef<usize>,
                  output: TypedBufferRef) -> Result<BoxedOperator<'a>, QueryError> {
        reify_types! {
            "select";
            input, output: PrimitiveUSize;
            Ok(Box::new(Select { input, indices, output }));
            input, output: NullablePrimitive;
            Ok(Box::new(SelectNullable { input, indices, output }))
        }
    }

    pub fn constant(val: RawVal, hide_value: bool, output: BufferRef<RawVal>) -> BoxedOperator<'a> {
        Box::new(Constant { val, hide_value, output })
    }

    pub fn scalar_i64(val: i64, hide_value: bool, output: BufferRef<Scalar<i64>>) -> BoxedOperator<'a> {
        Box::new(ScalarI64 { val, hide_value, output })
    }

    pub fn scalar_str(val: String, pinned: BufferRef<Scalar<String>>, output: BufferRef<Scalar<&'a str>>) -> BoxedOperator<'a> {
        Box::new(ScalarStr { val, pinned, output })
    }

    pub fn null_vec(len: usize, output: BufferRef<Any>) -> BoxedOperator<'a> {
        Box::new(NullVec { len, output })
    }

    pub fn constant_expand(val: i64, len: usize, output: TypedBufferRef) -> Result<BoxedOperator<'a>, QueryError> {
        match output.tag {
            EncodingType::U8 => Ok(Box::new(ConstantExpand {
                val: val as u8,
                output: output.u8()?,
                current_index: 0,
                len,
                batch_size: 0, // initialized later
            })),
            _ => Err(fatal!("constant_expand not implemented for type {:?}", output.tag)),
        }
    }

    pub fn constant_vec(val: BoxedData<'a>, output: BufferRef<Any>) -> BoxedOperator<'a> {
        Box::new(ConstantVec { val, output })
    }

    pub fn less_than(lhs: TypedBufferRef, rhs: TypedBufferRef, output: BufferRef<u8>) -> Result<BoxedOperator<'a>, QueryError> {
        reify_types! {
            "less_than";
            lhs: Str, rhs: ScalarStr;
            Ok(Box::new(BinaryVSOperator { lhs, rhs, output, op: PhantomData::<LessThan> }));
            lhs: ScalarStr, rhs: Str;
            Ok(Box::new(BinarySVOperator { lhs, rhs, output, op: PhantomData::<LessThan> }));
            lhs: Str, rhs: Str;
            Ok(Box::new(BinaryOperator { lhs, rhs, output, op: PhantomData::<LessThan> }));

            lhs: IntegerNoU64, rhs: ScalarI64;
            Ok(Box::new(BinaryVSOperator { lhs, rhs, output, op: PhantomData::<LessThan> }));
            lhs: ScalarI64, rhs: IntegerNoU64;
            Ok(Box::new(BinarySVOperator { lhs, rhs, output, op: PhantomData::<LessThan> }));
            lhs: IntegerNoU64, rhs: IntegerNoU64;
            Ok(Box::new(BinaryOperator { lhs, rhs, output, op: PhantomData::<LessThan> }))
        }
    }

    pub fn less_than_equals(lhs: TypedBufferRef, rhs: TypedBufferRef, output: BufferRef<u8>) -> Result<BoxedOperator<'a>, QueryError> {
        reify_types! {
            "less_than_equals";
            lhs: Str, rhs: ScalarStr;
            Ok(Box::new(BinaryVSOperator { lhs, rhs, output, op: PhantomData::<LessThanEquals> }));
            lhs: ScalarStr, rhs: Str;
            Ok(Box::new(BinarySVOperator { lhs, rhs, output, op: PhantomData::<LessThanEquals> }));
            lhs: Str, rhs: Str;
            Ok(Box::new(BinaryOperator { lhs, rhs, output, op: PhantomData::<LessThanEquals> }));

            lhs: IntegerNoU64, rhs: ScalarI64;
            Ok(Box::new(BinaryVSOperator { lhs, rhs, output, op: PhantomData::<LessThanEquals> }));
            lhs: ScalarI64, rhs: IntegerNoU64;
            Ok(Box::new(BinarySVOperator { lhs, rhs, output, op: PhantomData::<LessThanEquals> }));
            lhs: IntegerNoU64, rhs: IntegerNoU64;
            Ok(Box::new(BinaryOperator { lhs, rhs, output, op: PhantomData::<LessThanEquals> }))
        }
    }

    pub fn equals(lhs: TypedBufferRef,
                  rhs: TypedBufferRef,
                  output: BufferRef<u8>) -> Result<BoxedOperator<'a>, QueryError> {
        reify_types! {
            "equals";
            lhs: Str, rhs: ScalarStr;
            Ok(Box::new(BinaryVSOperator { lhs, rhs, output, op: PhantomData::<Equals> }));
            lhs: ScalarStr, rhs: Str;
            Ok(Box::new(BinaryVSOperator { lhs: rhs, rhs: lhs, output, op: PhantomData::<Equals> }));
            lhs: Str, rhs: Str;
            Ok(Box::new(BinaryOperator { lhs, rhs, output, op: PhantomData::<Equals> }));

            lhs: IntegerNoU64, rhs: ScalarI64;
            Ok(Box::new(BinaryVSOperator { lhs, rhs, output, op: PhantomData::<Equals> }));
            lhs: ScalarI64, rhs: IntegerNoU64;
            Ok(Box::new(BinaryVSOperator { lhs: rhs, rhs: lhs, output, op: PhantomData::<Equals> }));
            lhs: IntegerNoU64, rhs: IntegerNoU64;
            Ok(Box::new(BinaryOperator { lhs, rhs, output, op: PhantomData::<Equals> }))
        }
    }

    pub fn not_equals(lhs: TypedBufferRef,
                      rhs: TypedBufferRef,
                      output: BufferRef<u8>) -> Result<BoxedOperator<'a>, QueryError> {
        reify_types! {
            "not_equals";
            lhs: Str, rhs: ScalarStr;
            Ok(Box::new(BinaryVSOperator { lhs, rhs, output, op: PhantomData::<NotEquals> }));
            lhs: ScalarStr, rhs: Str;
            Ok(Box::new(BinaryVSOperator { lhs: rhs, rhs: lhs, output, op: PhantomData::<NotEquals> }));
            lhs: Str, rhs: Str;
            Ok(Box::new(BinaryOperator { lhs, rhs, output, op: PhantomData::<NotEquals> }));

            lhs: IntegerNoU64, rhs: ScalarI64;
            Ok(Box::new(BinaryVSOperator { lhs, rhs, output, op: PhantomData::<NotEquals> }));
            lhs: ScalarI64, rhs: IntegerNoU64;
            Ok(Box::new(BinaryVSOperator { lhs: rhs, rhs: lhs, output, op: PhantomData::<NotEquals> }));
            lhs: IntegerNoU64, rhs: IntegerNoU64;
            Ok(Box::new(BinaryOperator { lhs, rhs, output, op: PhantomData::<NotEquals> }))
        }
    }

    pub fn addition(lhs: TypedBufferRef,
                    rhs: TypedBufferRef,
                    output: BufferRef<i64>) -> Result<BoxedOperator<'a>, QueryError> {
        reify_types! {
            "addition";
            lhs: ScalarI64, rhs: IntegerNoU64;
            Ok(Box::new(BinaryVSOperator { lhs: rhs, rhs: lhs, output, op: PhantomData::<Addition<_, _>> }));
            lhs: IntegerNoU64, rhs: ScalarI64;
            Ok(Box::new(BinaryVSOperator { lhs, rhs, output, op: PhantomData::<Addition<_, _>> }));
            lhs: IntegerNoU64, rhs: IntegerNoU64;
            Ok(Box::new(BinaryOperator { lhs, rhs, output, op: PhantomData::<Addition<_, _>> }))
        }
    }

    pub fn subtraction(lhs: TypedBufferRef,
                       rhs: TypedBufferRef,
                       output: BufferRef<i64>) -> Result<BoxedOperator<'a>, QueryError> {
        reify_types! {
            "subtraction";
            lhs: ScalarI64, rhs: IntegerNoU64;
            Ok(Box::new(BinarySVOperator { lhs, rhs, output, op: PhantomData::<Subtraction<_, _>> }));
            lhs: IntegerNoU64, rhs: ScalarI64;
            Ok(Box::new(BinaryVSOperator { lhs, rhs, output, op: PhantomData::<Subtraction<_, _>> }));
            lhs: IntegerNoU64, rhs: IntegerNoU64;
            Ok(Box::new(BinaryOperator { lhs, rhs, output, op: PhantomData::<Subtraction<_, _>> }))
        }
    }

    pub fn multiplication(lhs: TypedBufferRef,
                          rhs: TypedBufferRef,
                          output: BufferRef<i64>) -> Result<BoxedOperator<'a>, QueryError> {
        reify_types! {
            "multiplication";
            lhs: ScalarI64, rhs: IntegerNoU64;
            Ok(Box::new(BinaryVSOperator { lhs: rhs, rhs: lhs, output, op: PhantomData::<Multiplication<_, _>> }));
            lhs: IntegerNoU64, rhs: ScalarI64;
            Ok(Box::new(BinaryVSOperator { lhs, rhs, output, op: PhantomData::<Multiplication<_, _>> }));
            lhs: IntegerNoU64, rhs: IntegerNoU64;
            Ok(Box::new(BinaryOperator { lhs, rhs, output, op: PhantomData::<Multiplication<_, _>> }))
        }
    }

    pub fn division(lhs: TypedBufferRef,
                    rhs: TypedBufferRef,
                    output: BufferRef<i64>) -> Result<BoxedOperator<'a>, QueryError> {
        reify_types! {
            "division";
            lhs: ScalarI64, rhs: IntegerNoU64;
            Ok(Box::new(BinarySVOperator { lhs, rhs, output, op: PhantomData::<Division<_, _>> }));
            lhs: IntegerNoU64, rhs: ScalarI64;
            Ok(Box::new(BinaryVSOperator { lhs, rhs, output, op: PhantomData::<Division<_, _>> }));
            lhs: IntegerNoU64, rhs: IntegerNoU64;
            Ok(Box::new(BinaryOperator { lhs, rhs, output, op: PhantomData::<Division<_, _>> }))
        }
    }

    pub fn modulo(lhs: TypedBufferRef,
                  rhs: TypedBufferRef,
                  output: BufferRef<i64>) -> Result<BoxedOperator<'a>, QueryError> {
        reify_types! {
            "modulo";
            lhs: ScalarI64, rhs: IntegerNoU64;
            Ok(Box::new(BinarySVOperator { lhs, rhs, output, op: PhantomData::<Modulo<_, _>> }));
            lhs: IntegerNoU64, rhs: ScalarI64;
            Ok(Box::new(BinaryVSOperator { lhs, rhs, output, op: PhantomData::<Modulo<_, _>> }));
            lhs: IntegerNoU64, rhs: IntegerNoU64;
            Ok(Box::new(BinaryOperator { lhs, rhs, output, op: PhantomData::<Modulo<_, _>> }))
        }
    }

    pub fn or(lhs: BufferRef<u8>, rhs: BufferRef<u8>, output: BufferRef<u8>) -> BoxedOperator<'a> {
        BooleanOperator::<BooleanOr>::compare(lhs, rhs, output)
    }

    pub fn and(lhs: BufferRef<u8>, rhs: BufferRef<u8>, output: BufferRef<u8>) -> BoxedOperator<'a> {
        BooleanOperator::<BooleanAnd>::compare(lhs, rhs, output)
    }

    pub fn bit_shift_left_add(lhs: BufferRef<i64>,
                              rhs: BufferRef<i64>,
                              output: BufferRef<i64>,
                              shift_amount: i64) -> BoxedOperator<'a> {
        Box::new(ParameterizedVecVecIntegerOperator::<BitShiftLeftAdd> { lhs, rhs, output, parameter: shift_amount, op: PhantomData })
    }

    pub fn bit_unpack(inner: BufferRef<i64>, shift: u8, width: u8, output: BufferRef<i64>) -> BoxedOperator<'a> {
        Box::new(BitUnpackOperator { input: inner, output, shift, width })
    }

    pub fn slice_pack(input: TypedBufferRef, stride: usize, offset: usize, output: BufferRef<Any>) -> Result<BoxedOperator<'a>, QueryError> {
        if let EncodingType::Str = input.tag {
            return Ok(Box::new(SlicePackString { input: input.str()?, output, stride, offset }));
        }
        reify_types! {
            "slice_pack";
            input: Integer;
            Ok(Box::new(SlicePackInt { input, output, stride, offset }))
        }
    }

    pub fn slice_unpack(input: BufferRef<Any>, stride: usize, offset: usize, output: TypedBufferRef) -> Result<BoxedOperator<'a>, QueryError> {
        if let EncodingType::Str = output.tag {
            return Ok(Box::new(SliceUnpackString { input, output: output.str()?, stride, offset }));
        }
        reify_types! {
            "slice_unpack";
            output: Integer;
            Ok(Box::new(SliceUnpackInt { input, output, stride, offset }))
        }
    }

    pub fn val_rows_pack(input: BufferRef<Val<'a>>, stride: usize, offset: usize, output: BufferRef<ValRows<'a>>) -> BoxedOperator<'a> {
        Box::new(ValRowsPack { input, stride, offset, output })
    }

    pub fn val_rows_unpack(input: BufferRef<ValRows<'a>>, stride: usize, offset: usize, output: BufferRef<Val<'a>>) -> BoxedOperator<'a> {
        Box::new(ValRowsUnpack { input, stride, offset, output })
    }

    pub fn type_conversion(input: TypedBufferRef, output: TypedBufferRef) -> Result<BoxedOperator<'a>, QueryError> {
        if output.tag == EncodingType::Val {
            let output = output.val()?;
            if input.tag.is_nullable() {
                if input.tag == EncodingType::NullableStr {
                    Ok(Box::new(NullableStrToVal { input: input.nullable_str()?, vals: output }) as BoxedOperator<'a>)
                } else {
                    reify_types! {
                        "nullable_int_to_val";
                        input: NullableInteger;
                        Ok(Box::new(NullableIntToVal { input, vals: output }) as BoxedOperator<'a>)
                    }
                }
            } else {
                reify_types! {
                    "type_conversion";
                    input: PrimitiveNoU64;
                    Ok(Box::new(TypeConversionOperator { input, output }) as BoxedOperator<'a>)
                }
            }
        } else if input.tag == EncodingType::Val {
            let input = input.val()?;
            if output.tag.is_nullable() {
                if output.tag == EncodingType::NullableStr {
                    Ok(Box::new(ValToNullableStr { vals: input, nullable: output.nullable_str()? }) as BoxedOperator<'a>)
                } else {
                    reify_types! {
                        "nullable_int_to_val";
                        output: NullableInteger;
                        Ok(Box::new(ValToNullableInt { vals: input, nullable: output }) as BoxedOperator<'a>)
                    }
                }
            } else {
                reify_types! {
                    "type_conversion";
                    output: PrimitiveNoU64;
                    Ok(Box::new(TypeConversionOperator { input, output }) as BoxedOperator<'a>)
                }
            }
        } else {
            if input.tag == EncodingType::Str && output.tag == EncodingType::OptStr {
                return Ok(Box::new(TypeConversionOperator { input: input.str()?, output: output.opt_str()? }));
            }
            reify_types! {
                "type_conversion";
                input: Integer, output: Integer;
                Ok(Box::new(TypeConversionOperator { input, output }))
            }
        }
    }

    pub fn not(input: BufferRef<u8>, output: BufferRef<u8>) -> BoxedOperator<'a> {
        Box::new(MapOperator { input, output, map: BooleanNot })
    }

    #[allow(clippy::wrong_self_convention)]
    pub fn to_year(input: BufferRef<i64>, output: BufferRef<i64>) -> BoxedOperator<'a> {
        Box::new(MapOperator { input, output, map: ToYear })
    }

    pub fn regex(input: BufferRef<&'a str>, r: &str, output: BufferRef<u8>) -> BoxedOperator<'a> {
        Box::new(MapOperator { input, output, map: RegexMatch { r: regex::Regex::new(r).unwrap() } })
    }

    pub fn length(input: BufferRef<&'a str>, output: BufferRef<i64>) -> BoxedOperator<'a> {
        Box::new(MapOperator { input, output, map: Length })
    }

    pub fn aggregate(input: TypedBufferRef,
                     grouping: TypedBufferRef,
                     max_index: BufferRef<Scalar<i64>>,
                     aggregator: Aggregator,
                     output: TypedBufferRef) -> Result<BoxedOperator<'a>, QueryError> {
        if input.is_nullable() {
            reify_types! {
                "nullable_aggregation";
                input: NullableInteger, grouping: Integer, aggregator: Aggregator;
                Ok(Box::new(AggregateNullable { input, grouping, output: output.into(), max_index, a: aggregator }))
            }
        } else {
            reify_types! {
                "aggregation";
                input: IntegerNoU64, grouping: Integer, aggregator: Aggregator;
                Ok(Box::new(Aggregate { input, grouping, output: output.into(), max_index, a: aggregator }))
            }
        }
    }

    pub fn exists(input: TypedBufferRef, max_index: BufferRef<Scalar<i64>>, output: BufferRef<u8>) -> Result<BoxedOperator<'a>, QueryError> {
        reify_types! {
            "exists";
            input: Integer;
            Ok(Box::new(Exists { input, output, max_index }))
        }
    }

    pub fn nonzero_compact(data: TypedBufferRef, compacted: TypedBufferRef) -> Result<BoxedOperator<'a>, QueryError> {
        reify_types! {
            "nonzero_compact";
            data, compacted: Integer;
            Ok(Box::new(NonzeroCompact { data, compacted }))
        }
    }

    pub fn nonzero_indices(input: TypedBufferRef, output: TypedBufferRef) -> Result<BoxedOperator<'a>, QueryError> {
        reify_types! {
            "nonzero_indices";
            input: Integer, output: Integer;
            Ok(Box::new(NonzeroIndices { input, output }))
        }
    }

    pub fn compact(data: TypedBufferRef, select: TypedBufferRef, compacted: TypedBufferRef) -> Result<BoxedOperator<'a>, QueryError> {
        reify_types! {
            "compact";
            data, compacted: Integer, select: Integer;
            Ok(Box::new(Compact { data, select, compacted }))
        }
    }

    pub fn hash_map_grouping(raw_grouping_key: TypedBufferRef,
                             max_cardinality: usize,
                             unique_out: TypedBufferRef,
                             grouping_key_out: BufferRef<u32>,
                             cardinality_out: BufferRef<Scalar<i64>>) -> Result<BoxedOperator<'a>, QueryError> {
        if let EncodingType::ByteSlices(columns) = raw_grouping_key.tag {
            return Ok(HashMapGroupingByteSlices::boxed(
                raw_grouping_key.buffer, unique_out.buffer, grouping_key_out, cardinality_out, columns));
        }
        reify_types! {
            "hash_map_grouping";
            raw_grouping_key, unique_out: Primitive;
            Ok(HashMapGrouping::boxed(raw_grouping_key, unique_out, grouping_key_out, cardinality_out, max_cardinality))
        }
    }

    pub fn hash_map_grouping_val_rows(raw_grouping_key: BufferRef<ValRows<'a>>,
                                      columns: usize,
                                      _max_cardinality: usize,
                                      unique_out: BufferRef<ValRows<'a>>,
                                      grouping_key_out: BufferRef<u32>,
                                      cardinality_out: BufferRef<Scalar<i64>>) -> Result<BoxedOperator<'a>, QueryError> {
        Ok(HashMapGroupingValRows::boxed(raw_grouping_key, unique_out, grouping_key_out, cardinality_out, columns))
    }

    pub fn indices(input: TypedBufferRef, indices_out: BufferRef<usize>) -> BoxedOperator<'a> {
        Box::new(Indices { input: input.buffer, indices_out })
    }

    pub fn sort_by(ranking: TypedBufferRef,
                   indices: BufferRef<usize>,
                   descending: bool,
                   stable: bool,
                   output: BufferRef<usize>) -> Result<BoxedOperator<'a>, QueryError> {
        if let EncodingType::ByteSlices(_) = ranking.tag {
            return Ok(Box::new(
                SortBySlices { ranking: ranking.any(), output, indices, descending, stable }));
        }
        if let EncodingType::ValRows = ranking.tag {
            return Ok(Box::new(
                SortByValRows { ranking: ranking.val_rows()?, output, indices, descending, stable }));
        }
        if ranking.is_nullable() {
            reify_types! {
                "sort_indices";
                ranking: NullablePrimitive;
                Ok(Box::new(SortByNullable { ranking, output, indices, descending, stable }))
            }
        } else {
            reify_types! {
                "sort_indices";
                ranking: Primitive;
                Ok(Box::new(SortBy { ranking, output, indices, descending, stable }))
            }
        }
    }

    pub fn top_n(input: TypedBufferRef,
                 keys: TypedBufferRef,
                 n: usize, desc: bool,
                 indices_out: BufferRef<usize>) -> Result<BoxedOperator<'a>, QueryError> {
        if desc {
            reify_types! {
                "top_n_desc";
                input, keys: Primitive;
                Ok(Box::new(TopN { input, keys, indices: indices_out, last_index: 0, n, c: PhantomData::<CmpGreaterThan> }))
            }
        } else {
            reify_types! {
                "top_n_asc";
                input, keys: Primitive;
                Ok(Box::new(TopN { input, keys, indices: indices_out, last_index: 0, n, c: PhantomData::<CmpLessThan> }))
            }
        }
    }

    pub fn merge_deduplicate(left: TypedBufferRef,
                             right: TypedBufferRef,
                             ops_out: BufferRef<MergeOp>,
                             merged_out: TypedBufferRef) -> Result<BoxedOperator<'a>, QueryError> {
        reify_types! {
            "merge_deduplicate";
            left, right, merged_out: Primitive;
            Ok(Box::new(MergeDeduplicate { left, right, deduplicated: merged_out, merge_ops: ops_out }))
        }
    }

    pub fn partition(left: TypedBufferRef,
                     right: TypedBufferRef,
                     limit: usize, desc: bool,
                     partition_out: BufferRef<Premerge>) -> Result<BoxedOperator<'a>, QueryError> {
        if desc {
            reify_types! {
                "partition";
                left, right: Primitive;
                Ok(Box::new(Partition { left, right, partitioning: partition_out, limit, c: PhantomData::<CmpGreaterThan> }))
            }
        } else {
            reify_types! {
                "partition";
                left, right: Primitive;
                Ok(Box::new(Partition { left, right, partitioning: partition_out, limit, c: PhantomData::<CmpLessThan> }))
            }
        }
    }


    pub fn subpartition(partitioning: BufferRef<Premerge>,
                        left: TypedBufferRef,
                        right: TypedBufferRef,
                        desc: bool,
                        subpartition_out: BufferRef<Premerge>) -> Result<BoxedOperator<'a>, QueryError> {
        if desc {
            reify_types! {
                "subpartition";
                left, right: Primitive;
                Ok(Box::new(SubPartition { partitioning, left, right, sub_partitioning: subpartition_out, c: PhantomData::<CmpGreaterThan> }))
            }
        } else {
            reify_types! {
                "subpartition";
                left, right: Primitive;
                Ok(Box::new(SubPartition { partitioning, left, right, sub_partitioning: subpartition_out, c: PhantomData::<CmpLessThan> }))
            }
        }
    }

    pub fn merge_deduplicate_partitioned(partitioning: BufferRef<Premerge>,
                                         left: TypedBufferRef,
                                         right: TypedBufferRef,
                                         ops_out: BufferRef<MergeOp>,
                                         merged_out: TypedBufferRef) -> Result<BoxedOperator<'a>, QueryError> {
        reify_types! {
            "merge_deduplicate_partitioned";
            left, right, merged_out: Primitive;
            Ok(Box::new(MergeDeduplicatePartitioned { partitioning, left, right, deduplicated: merged_out, merge_ops: ops_out }))
        }
    }

    pub fn merge_drop(merge_ops: BufferRef<MergeOp>,
                      left: TypedBufferRef,
                      right: TypedBufferRef,
                      merged_out: TypedBufferRef) -> Result<BoxedOperator<'a>, QueryError> {
        reify_types! {
            "merge_drop";
            left, right, merged_out: Primitive;
            Ok(Box::new(MergeDrop { merge_ops, left, right, deduplicated: merged_out }))
        }
    }

    pub fn merge_aggregate(merge_ops: BufferRef<MergeOp>,
                           left: BufferRef<i64>,
                           right: BufferRef<i64>,
                           aggregator: Aggregator,
                           aggregated_out: BufferRef<i64>) -> BoxedOperator<'a> {
        Box::new(MergeAggregate { merge_ops, left, right, aggregated: aggregated_out, aggregator })
    }

    pub fn merge_partitioned(partitioning: BufferRef<Premerge>,
                             left: TypedBufferRef,
                             right: TypedBufferRef,
                             limit: usize, desc: bool,
                             ops_out: BufferRef<u8>,
                             merged_out: TypedBufferRef) -> Result<BoxedOperator<'a>, QueryError> {
        if desc {
            reify_types! {
                "merge_partitioned_desc";
                left, right, merged_out: Primitive;
                Ok(Box::new(MergePartitioned { partitioning, left, right, merged: merged_out, take_left: ops_out, limit, c: PhantomData::<CmpGreaterThan> }))
            }
        } else {
            reify_types! {
                "merge_partitioned_asc";
                left, right, merged_out: Primitive;
                Ok(Box::new(MergePartitioned { partitioning, left, right, merged: merged_out, take_left: ops_out, limit, c: PhantomData::<CmpLessThan> }))
            }
        }
    }

    pub fn merge(left: TypedBufferRef,
                 right: TypedBufferRef,
                 limit: usize,
                 desc: bool,
                 ops_out: BufferRef<u8>,
                 merged_out: TypedBufferRef) -> Result<BoxedOperator<'a>, QueryError> {
        if desc {
            reify_types! {
                "merge_desc";
                left, right, merged_out: Primitive;
                Ok(Box::new(Merge { left, right, merged: merged_out, merge_ops: ops_out, limit, c: PhantomData::<CmpGreaterThan> }))
            }
        } else {
            reify_types! {
                "merge_desc";
                left, right, merged_out: Primitive;
                Ok(Box::new(Merge { left, right, merged: merged_out, merge_ops: ops_out, limit, c: PhantomData::<CmpLessThan> }))
            }
        }
    }

    pub fn merge_keep(merge_ops: BufferRef<u8>,
                      left: TypedBufferRef,
                      right: TypedBufferRef,
                      merged_out: TypedBufferRef) -> Result<BoxedOperator<'a>, QueryError> {
        reify_types! {
                "merge_keep";
                left, right, merged_out: NullablePrimitive;
                Ok(Box::new(MergeKeepNullable { merge_ops, left, right, merged: merged_out }));
                left, right, merged_out: Primitive;
                Ok(Box::new(MergeKeep { merge_ops, left, right, merged: merged_out }))
        }
    }
}
