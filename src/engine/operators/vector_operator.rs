use std::fmt::Write;
use std::fmt;
use std::intrinsics::type_name;
use std::marker::PhantomData;

use itertools::Itertools;
use regex::Regex;

use engine::*;
use ingest::raw_val::RawVal;
use mem_store::*;
use locustdb_derive::reify_types;

use super::addition_vs::AdditionVS;
use super::bit_unpack::BitUnpackOperator;
use super::bool_op::*;
use super::column_ops::*;
use super::compact::Compact;
use super::constant::Constant;
use super::constant_vec::ConstantVec;
use super::count::VecCount;
use super::delta_decode::*;
use super::dict_lookup::*;
use super::division_vs::DivideVS;
use super::encode_const::*;
use super::exists::Exists;
use super::filter::Filter;
use super::hashmap_grouping::HashMapGrouping;
use super::hashmap_grouping_byte_slices::HashMapGroupingByteSlices;
use super::merge::Merge;
use super::merge_aggregate::MergeAggregate;
use super::merge_deduplicate::MergeDeduplicate;
use super::merge_deduplicate_partitioned::MergeDeduplicatePartitioned;
use super::merge_drop::MergeDrop;
use super::merge_keep::MergeKeep;
use super::nonzero_compact::NonzeroCompact;
use super::nonzero_indices::NonzeroIndices;
use super::parameterized_vec_vec_int_op::*;
use super::partition::Partition;
use super::select::Select;
use super::slice_pack::*;
use super::slice_unpack::*;
use super::sort_indices::SortIndices;
use super::subpartition::SubPartition;
use super::sum::VecSum;
use super::to_year::ToYear;
use super::top_n::TopN;
use super::type_conversion::TypeConversionOperator;
use super::unhexpack_strings::UnhexpackStrings;
use super::unpack_strings::UnpackStrings;
use super::vec_const_bool_op::*;


pub type BoxedOperator<'a> = Box<VecOperator<'a> + 'a>;

pub trait VecOperator<'a>: fmt::Debug {
    fn execute(&mut self, stream: bool, scratchpad: &mut Scratchpad<'a>);
    fn finalize(&mut self, _scratchpad: &mut Scratchpad<'a>) {}
    fn init(&mut self, _total_count: usize, _batch_size: usize, _scratchpad: &mut Scratchpad<'a>) {}

    fn inputs(&self) -> Vec<BufferRef<Any>>;
    fn outputs(&self) -> Vec<BufferRef<Any>>;
    fn can_stream_input(&self, i: usize) -> bool;
    fn can_stream_output(&self, i: usize) -> bool;
    fn allocates(&self) -> bool;
    fn is_streaming_producer(&self) -> bool { false }
    fn has_more(&self) -> bool { false }
    fn custom_output_len(&self) -> Option<usize> { None }

    fn display(&self, full: bool) -> String {
        let mut s = String::new();
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

    pub fn dict_lookup(indices: TypedBufferRef,
                       dict_indices: BufferRef<u64>,
                       dict_data: BufferRef<u8>,
                       output: BufferRef<&'a str>) -> BoxedOperator<'a> {
        reify_types![
            "dict_lookup";
            indices: Integer;
            Box::new(DictLookup { indices, output, dict_indices, dict_data });
        ]
    }

    #[cfg(feature = "enable_lz4")]
    pub fn lz4_decode(encoded: BufferRef<u8>,
                      decoded: TypedBufferRef,
                      decoded_len: usize) -> BoxedOperator<'a> {
        use super::lz4_decode::LZ4Decode;
        use std::io::Read;
        let reader: Box<Read> = Box::new(&[] as &[u8]);
        reify_types! {
            "lz4_decode";
            decoded: Integer;
            Box::new(LZ4Decode::<'a, _> { encoded, decoded, decoded_len, reader, has_more: true });
        }
    }

    #[cfg(not(feature = "enable_lz4"))]
    pub fn lz4_decode(_: BufferRef<u8>,
                      _: TypedBufferRef,
                      _: usize) -> BoxedOperator<'a> {
        panic!("LZ4 is not enabled in this build of LocustDB. Recompile with `features enable_lz4`")
    }

    pub fn unpack_strings(packed: BufferRef<u8>, unpacked: BufferRef<&'a str>) -> BoxedOperator<'a> {
        Box::new(UnpackStrings::<'a> { packed, unpacked, iterator: None, has_more: true })
    }

    pub fn unhexpack_strings(packed: BufferRef<u8>,
                             unpacked: BufferRef<&'a str>,
                             stringstore: BufferRef<u8>,
                             uppercase: bool,
                             total_bytes: usize) -> BoxedOperator<'a> {
        Box::new(UnhexpackStrings::<'a> { packed, unpacked, stringstore, uppercase, total_bytes, iterator: None, has_more: true })
    }

    pub fn delta_decode(encoded: TypedBufferRef, decoded: BufferRef<i64>) -> BoxedOperator<'a> {
        reify_types! {
            "delta_decode";
            encoded: Integer;
            Box::new(DeltaDecode { encoded, decoded, previous: 0 });
        }
    }

    pub fn inverse_dict_lookup(dict_indices: BufferRef<u64>,
                               dict_data: BufferRef<u8>,
                               constant: BufferRef<String>,
                               output: BufferRef<RawVal>) -> BoxedOperator<'a> {
        Box::new(InverseDictLookup { dict_indices, dict_data, constant, output })
    }

    pub fn encode_int_const(constant: BufferRef<i64>,
                            output: BufferRef<i64>,
                            codec: Codec) -> BoxedOperator<'a> {
        Box::new(EncodeIntConstant { constant, output, codec })
    }

    pub fn filter(input: TypedBufferRef,
                  filter: BufferRef<u8>,
                  output: TypedBufferRef) -> BoxedOperator<'a> {
        reify_types! {
            "filter";
            input, output: Primitive;
            Box::new(Filter { input, filter, output });
        }
    }

    pub fn select(input: TypedBufferRef,
                  indices: BufferRef<usize>,
                  output: TypedBufferRef) -> BoxedOperator<'a> {
        reify_types! {
            "select";
            input, output: Primitive;
            Box::new(Select { input, indices, output });
        }
    }

    pub fn constant(val: RawVal, hide_value: bool, output: BufferRef<RawVal>) -> BoxedOperator<'a> {
        Box::new(Constant { val, hide_value, output })
    }

    pub fn constant_vec(val: BoxedVec<'a>, output: BufferRef<Any>) -> BoxedOperator<'a> {
        Box::new(ConstantVec { val, output })
    }

    pub fn less_than_vs(lhs: TypedBufferRef, rhs: BufferRef<i64>, output: BufferRef<u8>) -> BoxedOperator<'a> {
        reify_types! {
            "less_than_vs";
            lhs: IntegerNoU64;
            Box::new(VecConstBoolOperator::<_, i64, LessThanInt<_>> { lhs, rhs, output, op: PhantomData });
        }
    }

    pub fn equals_vs(lhs: TypedBufferRef,
                     rhs: TypedBufferRef,
                     output: BufferRef<u8>) -> BoxedOperator<'a> {
        // TODO(clemens): use specialization to get general Equals<T, U> class and unify
        if let EncodingType::Str = lhs.tag {
            return Box::new(VecConstBoolOperator { lhs: lhs.str(), rhs: rhs.string(), output, op: PhantomData::<EqualsString> });
        }
        reify_types! {
            "slice_pack";
            lhs: IntegerNoU64;
            Box::new(VecConstBoolOperator { lhs, rhs: rhs.const_i64(), output, op: PhantomData::<EqualsInt<_>> });
        }
    }

    pub fn not_equals_vs(lhs: TypedBufferRef,
                         rhs: TypedBufferRef,
                         output: BufferRef<u8>) -> BoxedOperator<'a> {
        if let EncodingType::Str = lhs.tag {
            return Box::new(VecConstBoolOperator { lhs: lhs.str(), rhs: rhs.string(), output, op: PhantomData::<NotEqualsString> });
        }
        reify_types! {
            "slice_pack";
            lhs: IntegerNoU64;
            Box::new(VecConstBoolOperator { lhs, rhs: rhs.i64(), output, op: PhantomData::<NotEqualsInt<_>> });
        }
    }

    pub fn divide_vs(lhs: BufferRef<i64>,
                     rhs: BufferRef<i64>,
                     output: BufferRef<i64>) -> BoxedOperator<'a> {
        Box::new(DivideVS { lhs, rhs, output })
    }

    pub fn addition_vs(lhs: TypedBufferRef,
                       rhs: BufferRef<i64>,
                       output: BufferRef<i64>) -> BoxedOperator<'a> {
        reify_types! {
            "addition_vs";
            lhs: IntegerNoU64;
            Box::new(AdditionVS { lhs, rhs, output });
        }
    }

    pub fn or(lhs: BufferRef<u8>, rhs: BufferRef<u8>) -> BoxedOperator<'a> {
        BooleanOperator::<BooleanOr>::compare(lhs, rhs)
    }

    pub fn and(lhs: BufferRef<u8>, rhs: BufferRef<u8>) -> BoxedOperator<'a> {
        BooleanOperator::<BooleanAnd>::compare(lhs, rhs)
    }

    pub fn bit_shift_left_add(lhs: BufferRef<i64>,
                              rhs: BufferRef<i64>,
                              output: BufferRef<i64>,
                              shift_amount: i64) -> BoxedOperator<'a> {
        Box::new(ParameterizedVecVecIntegerOperator::<BitShiftLeftAdd> { lhs, rhs, output, parameter: shift_amount, op: PhantomData })
    }

    pub fn bit_unpack(inner: BufferRef<i64>, output: BufferRef<i64>, shift: u8, width: u8) -> BoxedOperator<'a> {
        Box::new(BitUnpackOperator { input: inner, output, shift, width })
    }

    pub fn slice_pack(input: TypedBufferRef, output: BufferRef<Any>, stride: usize, offset: usize) -> BoxedOperator<'a> {
        if let EncodingType::Str = input.tag {
            return Box::new(SlicePackString { input: input.str(), output, stride, offset });
        }
        reify_types! {
            "slice_pack";
            input: Integer;
            Box::new(SlicePackInt { input, output, stride, offset });
        }
    }

    pub fn slice_unpack(input: BufferRef<Any>, output: TypedBufferRef, stride: usize, offset: usize) -> BoxedOperator<'a> {
        if let EncodingType::Str = output.tag {
            return Box::new(SliceUnpackString { input, output: output.str(), stride, offset });
        }
        reify_types! {
            "slice_unpack";
            output: Integer;
            Box::new(SliceUnpackInt { input, output, stride, offset });
        }
    }

    pub fn type_conversion(input: TypedBufferRef, output: TypedBufferRef) -> BoxedOperator<'a> {
        reify_types! {
            "type_conversion";
            input: Integer, output: Integer;
            Box::new(TypeConversionOperator { input, output });
        }
    }

    #[allow(clippy::wrong_self_convention)]
    pub fn to_year(input: BufferRef<i64>, output: BufferRef<i64>) -> BoxedOperator<'a> {
        Box::new(ToYear { input, output })
    }

    pub fn summation(input: TypedBufferRef,
                     grouping: TypedBufferRef,
                     output: BufferRef<i64>,
                     max_index: BufferRef<i64>) -> BoxedOperator<'a> {
        reify_types! {
            "summation";
            input: IntegerNoU64, grouping: Integer;
            Box::new(VecSum { input, grouping, output, max_index });
        }
    }

    pub fn count(grouping: TypedBufferRef, output: BufferRef<u32>, max_index: BufferRef<i64>) -> BoxedOperator<'a> {
        reify_types! {
            "count";
            grouping: Integer;
            Box::new(VecCount { grouping, output, max_index });
        }
    }

    pub fn exists(input: TypedBufferRef, output: BufferRef<u8>, max_index: BufferRef<i64>) -> BoxedOperator<'a> {
        reify_types! {
            "exists";
            input: Integer;
            Box::new(Exists { input, output, max_index });
        }
    }

    pub fn nonzero_compact(data: TypedBufferRef) -> BoxedOperator<'a> {
        reify_types! {
            "nonzero_compact";
            data: Integer;
            Box::new(NonzeroCompact { data });
        }
    }

    pub fn nonzero_indices(input: TypedBufferRef, output: TypedBufferRef) -> BoxedOperator<'a> {
        reify_types! {
            "nonzero_indices";
            input: Integer, output: Integer;
            Box::new(NonzeroIndices { input, output });
        }
    }

    pub fn compact(data: TypedBufferRef, select: TypedBufferRef) -> BoxedOperator<'a> {
        reify_types! {
            "compact";
            data: Integer, select: Integer;
            Compact::boxed(data, select);
        }
    }

    // TODO(clemens): allow different types on raw input grouping key and output grouping key
    pub fn hash_map_grouping(raw_grouping_key: TypedBufferRef,
                             unique_out: TypedBufferRef,
                             grouping_key_out: BufferRef<u32>,
                             cardinality_out: BufferRef<i64>,
                             max_cardinality: usize) -> BoxedOperator<'a> {
        if let EncodingType::ByteSlices(columns) = raw_grouping_key.tag {
            return HashMapGroupingByteSlices::boxed(
                raw_grouping_key.buffer, unique_out.buffer, grouping_key_out, cardinality_out, columns);
        }
        reify_types! {
            "hash_map_grouping";
            raw_grouping_key, unique_out: Primitive;
            HashMapGrouping::boxed(raw_grouping_key, unique_out, grouping_key_out, cardinality_out, max_cardinality);
        }
    }

    pub fn sort_indices(input: BufferRef<Any>, output: BufferRef<usize>, descending: bool) -> BoxedOperator<'a> {
        Box::new(SortIndices { input, output, descending })
    }

    pub fn top_n(input: TypedBufferRef,
                 keys: TypedBufferRef,
                 indices_out: BufferRef<usize>,
                 n: usize, desc: bool) -> BoxedOperator<'a> {
        if desc {
            reify_types! {
                "top_n_desc";
                input, keys: Primitive;
                Box::new(TopN { input, keys, indices: indices_out, last_index: 0, n, c: PhantomData::<CmpGreaterThan> });
            }
        } else {
            reify_types! {
                "top_n_asc";
                input, keys: Primitive;
                Box::new(TopN { input, keys, indices: indices_out, last_index: 0, n, c: PhantomData::<CmpLessThan> });
            }
        }
    }

    pub fn merge_deduplicate(left: TypedBufferRef,
                             right: TypedBufferRef,
                             merged_out: TypedBufferRef,
                             ops_out: BufferRef<MergeOp>) -> BoxedOperator<'a> {
        reify_types! {
            "merge_deduplicate";
            left, right, merged_out: Primitive;
            Box::new(MergeDeduplicate { left, right, deduplicated: merged_out, merge_ops: ops_out });
        }
    }

    pub fn partition(left: TypedBufferRef,
                     right: TypedBufferRef,
                     partition_out: BufferRef<Premerge>,
                     limit: usize) -> BoxedOperator<'a> {
        reify_types! {
            "partition";
            left, right: Primitive;
            Box::new(Partition { left, right, partitioning: partition_out, limit });
        }
    }


    pub fn subpartition(partitioning: BufferRef<Premerge>,
                        left: TypedBufferRef,
                        right: TypedBufferRef,
                        subpartition_out: BufferRef<Premerge>) -> BoxedOperator<'a> {
        reify_types! {
            "subpartition";
            left, right: Primitive;
            Box::new(SubPartition { partitioning, left, right, sub_partitioning: subpartition_out });
        }
    }

    pub fn merge_deduplicate_partitioned(partitioning: BufferRef<Premerge>,
                                         left: TypedBufferRef,
                                         right: TypedBufferRef,
                                         merged_out: TypedBufferRef,
                                         ops_out: BufferRef<MergeOp>) -> BoxedOperator<'a> {
        reify_types! {
            "merge_deduplicate_partitioned";
            left, right, merged_out: Primitive;
            Box::new(MergeDeduplicatePartitioned { partitioning, left, right, deduplicated: merged_out, merge_ops: ops_out });
        }
    }

    pub fn merge_drop(merge_ops: BufferRef<MergeOp>,
                      left: TypedBufferRef,
                      right: TypedBufferRef,
                      merged_out: TypedBufferRef) -> BoxedOperator<'a> {
        reify_types! {
            "merge_drop";
            left, right, merged_out: Primitive;
            Box::new(MergeDrop { merge_ops, left, right, deduplicated: merged_out });
        }
    }

    pub fn merge_aggregate(merge_ops: BufferRef<MergeOp>,
                           left: BufferRef<i64>,
                           right: BufferRef<i64>,
                           aggregated_out: BufferRef<i64>,
                           aggregator: Aggregator) -> BoxedOperator<'a> {
        Box::new(MergeAggregate { merge_ops, left, right, aggregated: aggregated_out, aggregator })
    }

    pub fn merge(left: TypedBufferRef,
                 right: TypedBufferRef,
                 merged_out: TypedBufferRef,
                 ops_out: BufferRef<u8>,
                 limit: usize,
                 desc: bool) -> BoxedOperator<'a> {
        if desc {
            reify_types! {
                "merge_desc";
                left, right, merged_out: Primitive;
                Box::new(Merge { left, right, merged: merged_out, merge_ops: ops_out, limit, c: PhantomData::<CmpGreaterThan> });
            }
        } else {
            reify_types! {
                "merge_desc";
                left, right, merged_out: Primitive;
                Box::new(Merge { left, right, merged: merged_out, merge_ops: ops_out, limit, c: PhantomData::<CmpLessThan> });
            }
        }
    }

    pub fn merge_keep(merge_ops: BufferRef<u8>,
                      left: TypedBufferRef,
                      right: TypedBufferRef,
                      merged_out: TypedBufferRef) -> BoxedOperator<'a> {
        reify_types! {
                "merge_keep";
                left, right, merged_out: Primitive;
                Box::new(MergeKeep { merge_ops, left, right, merged: merged_out });
        }
    }
}
