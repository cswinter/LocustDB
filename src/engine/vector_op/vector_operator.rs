use std::borrow::BorrowMut;
use std::cell::{RefCell, Ref, RefMut};
use std::collections::HashMap;
use std::fmt::Write;
use std::fmt;
use std::intrinsics::type_name;
use std::marker::PhantomData;
use std::mem;

use itertools::Itertools;
use regex::Regex;

use engine::*;
use engine::aggregator::Aggregator;
use engine::typed_vec::AnyVec;
use engine::types::EncodingType;
use engine::vector_op::comparator::*;
use ingest::raw_val::RawVal;
use mem_store::*;

use engine::vector_op::addition_vs::AdditionVS;
use engine::vector_op::bit_unpack::BitUnpackOperator;
use engine::vector_op::bool_op::*;
use engine::vector_op::column_ops::*;
use engine::vector_op::compact::Compact;
use engine::vector_op::constant::Constant;
use engine::vector_op::constant_vec::ConstantVec;
use engine::vector_op::count::VecCount;
use engine::vector_op::delta_decode::*;
use engine::vector_op::dict_lookup::*;
use engine::vector_op::division_vs::DivideVS;
use engine::vector_op::encode_const::*;
use engine::vector_op::exists::Exists;
use engine::vector_op::filter::Filter;
use engine::vector_op::hashmap_grouping::HashMapGrouping;
use engine::vector_op::hashmap_grouping_byte_slices::HashMapGroupingByteSlices;
use engine::vector_op::merge::Merge;
use engine::vector_op::merge_aggregate::MergeAggregate;
use engine::vector_op::merge_deduplicate::MergeDeduplicate;
use engine::vector_op::merge_deduplicate_partitioned::MergeDeduplicatePartitioned;
use engine::vector_op::merge_drop::MergeDrop;
use engine::vector_op::merge_keep::MergeKeep;
use engine::vector_op::nonzero_compact::NonzeroCompact;
use engine::vector_op::nonzero_indices::NonzeroIndices;
use engine::vector_op::parameterized_vec_vec_int_op::*;
use engine::vector_op::partition::Partition;
use engine::vector_op::select::Select;
use engine::vector_op::slice_pack::*;
use engine::vector_op::slice_unpack::*;
use engine::vector_op::sort_indices::SortIndices;
use engine::vector_op::subpartition::SubPartition;
use engine::vector_op::sum::VecSum;
use engine::vector_op::to_year::ToYear;
use engine::vector_op::top_n::TopN;
use engine::vector_op::type_conversion::TypeConversionOperator;
use engine::vector_op::unhexpack_strings::UnhexpackStrings;
use engine::vector_op::unpack_strings::UnpackStrings;
use engine::vector_op::vec_const_bool_op::*;


pub type BoxedOperator<'a> = Box<VecOperator<'a> + 'a>;

pub type TypedBufferRef = (BufferRef<Any>, EncodingType);

#[derive(Debug, Copy, Clone, PartialEq)]
pub struct BufferRef<T> {
    pub i: usize,
    pub name: &'static str,
    t: PhantomData<T>,
}

#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum Any {}

pub fn any_buffer_ref(i: usize, name: &'static str) -> BufferRef<Any> {
    BufferRef { i, name, t: PhantomData }
}

pub fn error_buffer_ref(name: &'static str) -> BufferRef<Any> {
    BufferRef {
        i: 0xdead_beef,
        name,
        t: PhantomData,
    }
}

impl BufferRef<Any> {
    pub fn merge_op(self) -> BufferRef<typed_vec::MergeOp> { self.transmute() }
    pub fn premerge(self) -> BufferRef<typed_vec::Premerge> { self.transmute() }
    pub fn raw_val(self) -> BufferRef<RawVal> { self.transmute() }
    pub fn i64(self) -> BufferRef<i64> { self.transmute() }
    pub fn u64(self) -> BufferRef<u64> { self.transmute() }
    pub fn u32(self) -> BufferRef<u32> { self.transmute() }
    pub fn u16(self) -> BufferRef<u16> { self.transmute() }
    pub fn u8(self) -> BufferRef<u8> { self.transmute() }
    pub fn string(self) -> BufferRef<String> { self.transmute() }
    pub fn str<'a>(self) -> BufferRef<&'a str> { self.transmute() }
    pub fn usize(self) -> BufferRef<usize> { self.transmute() }
    fn transmute<T>(self) -> BufferRef<T> { unsafe { mem::transmute(self) } }
}

impl<T: Clone> BufferRef<T> {
    pub fn any(&self) -> BufferRef<Any> { unsafe { mem::transmute(self.clone()) } }
}

impl<T> fmt::Display for BufferRef<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}_{}", self.name, self.i)
    }
}

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

pub struct Scratchpad<'a> {
    buffers: Vec<RefCell<BoxedVec<'a>>>,
    columns: HashMap<String, Vec<&'a AnyVec<'a>>>,
    pinned: Vec<bool>,
}

impl<'a> Scratchpad<'a> {
    pub fn new(count: usize, columns: HashMap<String, Vec<&'a AnyVec<'a>>>) -> Scratchpad<'a> {
        let mut buffers = Vec::with_capacity(count);
        for _ in 0..count {
            buffers.push(RefCell::new(AnyVec::empty(0)));
        }
        Scratchpad {
            buffers,
            columns,
            pinned: vec![false; count],
        }
    }

    pub fn get_any(&self, index: BufferRef<Any>) -> Ref<AnyVec<'a>> {
        Ref::map(self.buffers[index.i].borrow(), |x| x.as_ref())
    }

    pub fn get_any_mut(&self, index: BufferRef<Any>) -> RefMut<AnyVec<'a> + 'a> {
        assert!(!self.pinned[index.i], "Trying to mutably borrow pinned buffer {}", index);
        RefMut::map(self.buffers[index.i].borrow_mut(), |x| x.borrow_mut())
    }

    pub fn get_column_data(&self, name: &str, section_index: usize) -> &'a AnyVec<'a> {
        match self.columns.get(name) {
            Some(ref col) => col[section_index],
            None => panic!("No column of name {} ({:?})", name, self.columns.keys()),
        }
    }

    pub fn get<T: GenericVec<T> + 'a>(&self, index: BufferRef<T>) -> Ref<[T]> {
        Ref::map(self.buffers[index.i].borrow(), |x| T::unwrap(x.as_ref()))
    }

    pub fn get_pinned<T: GenericVec<T> + 'a>(&mut self, index: BufferRef<T>) -> &'a [T] {
        self.pinned[index.i] = true;
        let buffer = self.get(index);
        unsafe {
            mem::transmute::<&[T], &'a [T]>(&*buffer)
        }
    }

    pub fn get_mut<T: GenericVec<T> + 'a>(&self, index: BufferRef<T>) -> RefMut<Vec<T>> {
        assert!(!self.pinned[index.i], "Trying to mutably borrow pinned buffer {}", index);
        RefMut::map(self.buffers[index.i].borrow_mut(), |x| {
            let a: &mut AnyVec<'a> = x.borrow_mut();
            T::unwrap_mut(a)
        })
    }

    pub fn get_const<T: ConstType<T>>(&self, index: &BufferRef<T>) -> T {
        T::unwrap(&*self.get_any(index.any()))
    }

    pub fn collect(&mut self, index: BufferRef<Any>) -> BoxedVec<'a> {
        let owned = mem::replace(&mut self.buffers[index.i], RefCell::new(AnyVec::empty(0)));
        owned.into_inner()
    }

    pub fn set_any(&mut self, index: BufferRef<Any>, vec: BoxedVec<'a>) {
        assert!(!self.pinned[index.i], "Trying to set pinned buffer {}", index);
        self.buffers[index.i] = RefCell::new(vec);
    }

    pub fn set<T: GenericVec<T> + 'a>(&mut self, index: BufferRef<T>, vec: Vec<T>) {
        assert!(!self.pinned[index.i], "Trying to set pinned buffer {}", index);
        self.buffers[index.i] = RefCell::new(AnyVec::owned(vec));
    }

    pub fn pin(&mut self, index: BufferRef<Any>) {
        self.pinned[index.i] = true;
    }

    pub unsafe fn unpin(&mut self, index: BufferRef<Any>) {
        self.pinned[index.i] = false;
    }

    pub fn collect_pinned(self) -> Vec<BoxedVec<'a>> {
        self.buffers
            .into_iter()
            .zip(self.pinned.iter())
            .filter_map(|(d, pinned)|
                if *pinned {
                    Some(d.into_inner())
                } else {
                    None
                })
            .collect()
    }
}


use self::EncodingType::*;

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

    pub fn dict_lookup((indices, t): TypedBufferRef,
                       dict_indices: BufferRef<u64>,
                       dict_data: BufferRef<u8>,
                       output: BufferRef<&'a str>) -> BoxedOperator<'a> {
        match t {
            EncodingType::U8 => Box::new(DictLookup::<u8> { indices: indices.u8(), output, dict_indices, dict_data }),
            EncodingType::U16 => Box::new(DictLookup::<u16> { indices: indices.u16(), output, dict_indices, dict_data }),
            EncodingType::U32 => Box::new(DictLookup::<u32> { indices: indices.u32(), output, dict_indices, dict_data }),
            EncodingType::I64 => Box::new(DictLookup::<i64> { indices: indices.i64(), output, dict_indices, dict_data }),
            _ => panic!("dict_lookup not supported for type {:?}", t),
        }
    }

    #[cfg(feature = "enable_lz4")]
    pub fn lz4_decode(encoded: BufferRef<u8>,
                      (decoded, t): TypedBufferRef,
                      decoded_len: usize) -> BoxedOperator<'a> {
        use engine::vector_op::lz4_decode::LZ4Decode;
        use std::io::Read;
        let reader: Box<Read> = Box::new(&[] as &[u8]);
        match t {
            EncodingType::U8 => Box::new(LZ4Decode::<'a, u8> { encoded, decoded: decoded.u8(), decoded_len, reader, has_more: true }),
            EncodingType::U16 => Box::new(LZ4Decode::<'a, u16> { encoded, decoded: decoded.u16(), decoded_len, reader, has_more: true }),
            EncodingType::U32 => Box::new(LZ4Decode::<'a, u32> { encoded, decoded: decoded.u32(), decoded_len, reader, has_more: true }),
            EncodingType::U64 => Box::new(LZ4Decode::<'a, u64> { encoded, decoded: decoded.u64(), decoded_len, reader, has_more: true }),
            EncodingType::I64 => Box::new(LZ4Decode::<'a, i64> { encoded, decoded: decoded.i64(), decoded_len, reader, has_more: true }),
            _ => panic!("lz4_decode not supported for type {:?}", t),
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

    pub fn delta_decode((encoded, t): TypedBufferRef, decoded: BufferRef<i64>) -> BoxedOperator<'a> {
        match t {
            EncodingType::U8 => Box::new(DeltaDecode::<u8> { encoded: encoded.u8(), decoded, previous: 0 }),
            EncodingType::U16 => Box::new(DeltaDecode::<u16> { encoded: encoded.u16(), decoded, previous: 0 }),
            EncodingType::U32 => Box::new(DeltaDecode::<u32> { encoded: encoded.u32(), decoded, previous: 0 }),
            EncodingType::I64 => Box::new(DeltaDecode::<i64> { encoded: encoded.i64(), decoded, previous: 0 }),
            _ => panic!("delta_decode not supported for type {:?}", t),
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

    pub fn filter((input, t1): TypedBufferRef,
                  filter: BufferRef<u8>,
                  (output, t2): TypedBufferRef) -> BoxedOperator<'a> {
        assert!(t1 == t2);
        match t1 {
            EncodingType::I64 => Box::new(Filter::<i64> { input: input.i64(), filter, output: output.i64() }),
            EncodingType::U32 => Box::new(Filter::<u32> { input: input.u32(), filter, output: output.u32() }),
            EncodingType::U16 => Box::new(Filter::<u16> { input: input.u16(), filter, output: output.u16() }),
            EncodingType::U8 => Box::new(Filter::<u8> { input: input.u8(), filter, output: output.u8() }),
            EncodingType::Str => Box::new(Filter::<&str> { input: input.str(), filter, output: output.str() }),
            _ => panic!("filter not supported for type {:?}", t1),
        }
    }

    pub fn select((input, t1): TypedBufferRef,
                  indices: BufferRef<usize>,
                  (output, t2): TypedBufferRef) -> BoxedOperator<'a> {
        assert!(t1 == t2);
        match t1 {
            EncodingType::I64 => Box::new(Select::<i64> { input: input.i64(), indices, output: output.i64() }),
            EncodingType::U32 => Box::new(Select::<u32> { input: input.u32(), indices, output: output.u32() }),
            EncodingType::U16 => Box::new(Select::<u16> { input: input.u16(), indices, output: output.u16() }),
            EncodingType::U8 => Box::new(Select::<u8> { input: input.u8(), indices, output: output.u8() }),
            EncodingType::Str => Box::new(Select::<&str> { input: input.str(), indices, output: output.str() }),
            _ => panic!("filter not supported for type {:?}", t1),
        }
    }

    pub fn constant(val: RawVal, hide_value: bool, output: BufferRef<RawVal>) -> BoxedOperator<'a> {
        Box::new(Constant { val, hide_value, output })
    }

    pub fn constant_vec(val: BoxedVec<'a>, output: BufferRef<Any>) -> BoxedOperator<'a> {
        Box::new(ConstantVec { val, output })
    }

    pub fn less_than_vs((lhs, t): TypedBufferRef, rhs: BufferRef<i64>, output: BufferRef<u8>) -> BoxedOperator<'a> {
        match t {
            EncodingType::U8 => Box::new(
                VecConstBoolOperator::<u8, i64, LessThanInt<u8>> { lhs: lhs.u8(), rhs, output, op: PhantomData }),
            EncodingType::U16 => Box::new(
                VecConstBoolOperator::<u16, i64, LessThanInt<u16>> { lhs: lhs.u16(), rhs, output, op: PhantomData }),
            EncodingType::U32 => Box::new(
                VecConstBoolOperator::<u32, i64, LessThanInt<u32>> { lhs: lhs.u32(), rhs, output, op: PhantomData }),
            EncodingType::I64 => Box::new(
                VecConstBoolOperator::<i64, i64, LessThanInt<i64>> { lhs: lhs.i64(), rhs, output, op: PhantomData }),
            _ => panic!("less_than_vs not supported for type {:?}", t),
        }
    }

    pub fn equals_vs((lhs, t1): TypedBufferRef,
                     (rhs, t2): TypedBufferRef,
                     output: BufferRef<u8>) -> BoxedOperator<'a> {
        assert!(t1 == t2);
        match t1 {
            EncodingType::Str => Box::new(VecConstBoolOperator { lhs: lhs.str(), rhs: rhs.string(), output, op: PhantomData::<EqualsString> }),
            EncodingType::U8 => Box::new(VecConstBoolOperator::<_, _, EqualsInt<u8>> { lhs: lhs.u8(), rhs: rhs.i64(), output, op: PhantomData }),
            EncodingType::U16 => Box::new(VecConstBoolOperator::<_, _, EqualsInt<u16>> { lhs: lhs.u16(), rhs: rhs.i64(), output, op: PhantomData }),
            EncodingType::U32 => Box::new(VecConstBoolOperator::<_, _, EqualsInt<u32>> { lhs: lhs.u32(), rhs: rhs.i64(), output, op: PhantomData }),
            EncodingType::I64 => Box::new(VecConstBoolOperator::<_, _, Equals<i64>> { lhs: lhs.i64(), rhs: rhs.i64(), output, op: PhantomData }),
            _ => panic!("equals_vs not supported for type {:?}", t1),
        }
    }

    pub fn not_equals_vs((lhs, t1): TypedBufferRef,
                         (rhs, t2): TypedBufferRef,
                         output: BufferRef<u8>) -> BoxedOperator<'a> {
        assert!(t1 == t2);
        match t1 {
            EncodingType::Str => Box::new(VecConstBoolOperator::<_, _, NotEqualsString> { lhs: lhs.str(), rhs: rhs.string(), output, op: PhantomData }),
            EncodingType::U8 => Box::new(VecConstBoolOperator::<_, _, NotEqualsInt<u8>> { lhs: lhs.u8(), rhs: rhs.i64(), output, op: PhantomData }),
            EncodingType::U16 => Box::new(VecConstBoolOperator::<_, _, NotEqualsInt<u16>> { lhs: lhs.u16(), rhs: rhs.i64(), output, op: PhantomData }),
            EncodingType::U32 => Box::new(VecConstBoolOperator::<_, _, NotEqualsInt<u32>> { lhs: lhs.u32(), rhs: rhs.i64(), output, op: PhantomData }),
            EncodingType::I64 => Box::new(VecConstBoolOperator::<_, _, NotEquals<i64>> { lhs: lhs.i64(), rhs: rhs.i64(), output, op: PhantomData }),
            _ => panic!("not_equals_vs not supported for type {:?}", t1),
        }
    }

    pub fn divide_vs(lhs: BufferRef<i64>,
                     rhs: BufferRef<i64>,
                     output: BufferRef<i64>) -> BoxedOperator<'a> {
        Box::new(DivideVS { lhs, rhs, output })
    }

    pub fn addition_vs((lhs, t1): TypedBufferRef,
                       rhs: BufferRef<i64>,
                       output: BufferRef<i64>) -> BoxedOperator<'a> {
        match t1 {
            EncodingType::U8 => Box::new(AdditionVS::<u8> { lhs: lhs.u8(), rhs, output }),
            EncodingType::U16 => Box::new(AdditionVS::<u16> { lhs: lhs.u16(), rhs, output }),
            EncodingType::U32 => Box::new(AdditionVS::<u32> { lhs: lhs.u32(), rhs, output }),
            EncodingType::I64 => Box::new(AdditionVS::<i64> { lhs: lhs.i64(), rhs, output }),
            _ => panic!("addition_vs not supported for type {:?}", t1),
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

    pub fn slice_pack((input, t): TypedBufferRef, output: BufferRef<Any>, stride: usize, offset: usize) -> BoxedOperator<'a> {
        match t {
            Str => Box::new(SlicePackString { input: input.str(), output, stride, offset }),
            U8 => Box::new(SlicePackInt { input: input.u8(), output, stride, offset }),
            U16 => Box::new(SlicePackInt { input: input.u16(), output, stride, offset }),
            U32 => Box::new(SlicePackInt { input: input.u32(), output, stride, offset }),
            U64 => Box::new(SlicePackInt { input: input.u64(), output, stride, offset }),
            I64 => Box::new(SlicePackInt { input: input.i64(), output, stride, offset }),
            _ => panic!("slice_pack is not supported for type {:?}", t),
        }
    }

    pub fn slice_unpack(input: BufferRef<Any>, (output, t): TypedBufferRef, stride: usize, offset: usize) -> BoxedOperator<'a> {
        match t {
            Str => Box::new(SliceUnpackString { input, output: output.str(), stride, offset }),
            U8 => Box::new(SliceUnpackInt { input, output: output.u8(), stride, offset }),
            U16 => Box::new(SliceUnpackInt { input, output: output.u16(), stride, offset }),
            U32 => Box::new(SliceUnpackInt { input, output: output.u32(), stride, offset }),
            U64 => Box::new(SliceUnpackInt { input, output: output.u64(), stride, offset }),
            I64 => Box::new(SliceUnpackInt { input, output: output.i64(), stride, offset }),
            _ => panic!("slice_unpack is not supported for type {:?}", t),
        }
    }

    pub fn type_conversion((inner, initial_type): TypedBufferRef,
                           (output, target_type): TypedBufferRef) -> BoxedOperator<'a> {
        use self::EncodingType::*;
        match (initial_type, target_type) {
            (U8, U16) => Box::new(TypeConversionOperator { input: inner.u8(), output: output.u16() }),
            (U8, U32) => Box::new(TypeConversionOperator { input: inner.u8(), output: output.u32() }),
            (U8, I64) => Box::new(TypeConversionOperator { input: inner.u8(), output: output.i64() }),

            (U16, U8) => Box::new(TypeConversionOperator { input: inner.u16(), output: output.u8() }),
            (U16, U32) => Box::new(TypeConversionOperator { input: inner.u16(), output: output.u32() }),
            (U16, I64) => Box::new(TypeConversionOperator { input: inner.u16(), output: output.i64() }),

            (U32, U8) => Box::new(TypeConversionOperator { input: inner.u32(), output: output.u8() }),
            (U32, U16) => Box::new(TypeConversionOperator { input: inner.u32(), output: output.u16() }),
            (U32, I64) => Box::new(TypeConversionOperator { input: inner.u32(), output: output.i64() }),

            (I64, U8) => Box::new(TypeConversionOperator { input: inner.i64(), output: output.u8() }),
            (I64, U16) => Box::new(TypeConversionOperator { input: inner.i64(), output: output.u16() }),
            (I64, U32) => Box::new(TypeConversionOperator { input: inner.i64(), output: output.u32() }),

            (U8, U8) | (U16, U16) | (U32, U32) | (I64, I64) => panic!("type_conversion from type {:?} to itself", initial_type),
            _ => panic!("type_conversion not supported for types {:?} -> {:?}", initial_type, target_type)
        }
    }

    #[allow(clippy::wrong_self_convention)]
    pub fn to_year(input: BufferRef<i64>, output: BufferRef<i64>) -> BoxedOperator<'a> {
        Box::new(ToYear { input, output })
    }

    pub fn summation((input, input_type): TypedBufferRef,
                     (grouping, grouping_type): TypedBufferRef,
                     output: BufferRef<i64>,
                     max_index: BufferRef<i64>) -> BoxedOperator<'a> {
        use self::EncodingType::*;
        match (input_type, grouping_type) {
            (U8, U8) => Box::new(VecSum::<u8, u8> { input: input.u8(), grouping: grouping.u8(), output, max_index }),
            (U8, U16) => Box::new(VecSum::<u8, u16> { input: input.u8(), grouping: grouping.u16(), output, max_index }),
            (U8, U32) => Box::new(VecSum::<u8, u32> { input: input.u8(), grouping: grouping.u32(), output, max_index }),
            (U8, I64) => Box::new(VecSum::<u8, i64> { input: input.u8(), grouping: grouping.i64(), output, max_index }),
            (U16, U8) => Box::new(VecSum::<u16, u8> { input: input.u16(), grouping: grouping.u8(), output, max_index }),
            (U16, U16) => Box::new(VecSum::<u16, u16> { input: input.u16(), grouping: grouping.u16(), output, max_index }),
            (U16, U32) => Box::new(VecSum::<u16, u32> { input: input.u16(), grouping: grouping.u32(), output, max_index }),
            (U16, I64) => Box::new(VecSum::<u16, i64> { input: input.u16(), grouping: grouping.i64(), output, max_index }),
            (U32, U8) => Box::new(VecSum::<u32, u8> { input: input.u32(), grouping: grouping.u8(), output, max_index }),
            (U32, U16) => Box::new(VecSum::<u32, u16> { input: input.u32(), grouping: grouping.u16(), output, max_index }),
            (U32, U32) => Box::new(VecSum::<u32, u32> { input: input.u32(), grouping: grouping.u32(), output, max_index }),
            (U32, I64) => Box::new(VecSum::<u32, i64> { input: input.u32(), grouping: grouping.i64(), output, max_index }),
            (I64, U8) => Box::new(VecSum::<i64, u8> { input: input.i64(), grouping: grouping.u8(), output, max_index }),
            (I64, U16) => Box::new(VecSum::<i64, u16> { input: input.i64(), grouping: grouping.u16(), output, max_index }),
            (I64, U32) => Box::new(VecSum::<i64, u32> { input: input.i64(), grouping: grouping.u32(), output, max_index }),
            (I64, I64) => Box::new(VecSum::<i64, i64> { input: input.i64(), grouping: grouping.i64(), output, max_index }),
            (pt, gt) => panic!("invalid aggregation types {:?}, {:?}", pt, gt),
        }
    }

    pub fn count((grouping, grouping_type): TypedBufferRef,
                 output: BufferRef<u32>,
                 max_index: BufferRef<i64>) -> BoxedOperator<'a> {
        match grouping_type {
            EncodingType::U8 => Box::new(VecCount::<u8> { grouping: grouping.u8(), output, max_index }),
            EncodingType::U16 => Box::new(VecCount::<u16> { grouping: grouping.u16(), output, max_index }),
            EncodingType::U32 => Box::new(VecCount::<u32> { grouping: grouping.u32(), output, max_index }),
            EncodingType::I64 => Box::new(VecCount::<i64> { grouping: grouping.i64(), output, max_index }),
            t => panic!("unsupported type {:?} for grouping key", t),
        }
    }

    pub fn exists((grouping, grouping_type): TypedBufferRef,
                  output: BufferRef<u8>,
                  max_index: BufferRef<i64>) -> BoxedOperator<'a> {
        match grouping_type {
            EncodingType::U8 => Box::new(Exists::<u8> { input: grouping.u8(), output, max_index }),
            EncodingType::U16 => Box::new(Exists::<u16> { input: grouping.u16(), output, max_index }),
            EncodingType::U32 => Box::new(Exists::<u32> { input: grouping.u32(), output, max_index }),
            EncodingType::I64 => Box::new(Exists::<i64> { input: grouping.i64(), output, max_index }),
            t => panic!("unsupported type {:?} for grouping key", t),
        }
    }

    pub fn nonzero_compact((data, data_type): TypedBufferRef) -> BoxedOperator<'a> {
        match data_type {
            EncodingType::U8 => Box::new(NonzeroCompact::<u8> { data: data.u8() }),
            EncodingType::U16 => Box::new(NonzeroCompact::<u16> { data: data.u16() }),
            EncodingType::U32 => Box::new(NonzeroCompact::<u32> { data: data.u32() }),
            EncodingType::I64 => Box::new(NonzeroCompact::<i64> { data: data.i64() }),
            t => panic!("unsupported type {:?} for grouping key", t),
        }
    }

    pub fn nonzero_indices((input, input_type): TypedBufferRef,
                           (output, output_type): TypedBufferRef) -> BoxedOperator<'a> {
        use self::EncodingType::*;
        match (input_type, output_type) {
            (U8, U8) => Box::new(NonzeroIndices::<u8, u8> { input: input.u8(), output: output.u8() }),
            (U8, U16) => Box::new(NonzeroIndices::<u8, u16> { input: input.u8(), output: output.u16() }),
            (U8, U32) => Box::new(NonzeroIndices::<u8, u32> { input: input.u8(), output: output.u32() }),
            (U8, I64) => Box::new(NonzeroIndices::<u8, i64> { input: input.u8(), output: output.i64() }),
            (U16, U8) => Box::new(NonzeroIndices::<u16, u8> { input: input.u16(), output: output.u8() }),
            (U16, U16) => Box::new(NonzeroIndices::<u16, u16> { input: input.u16(), output: output.u16() }),
            (U16, U32) => Box::new(NonzeroIndices::<u16, u32> { input: input.u16(), output: output.u32() }),
            (U16, I64) => Box::new(NonzeroIndices::<u16, i64> { input: input.u16(), output: output.i64() }),
            (U32, U8) => Box::new(NonzeroIndices::<u32, u8> { input: input.u32(), output: output.u8() }),
            (U32, U16) => Box::new(NonzeroIndices::<u32, u16> { input: input.u32(), output: output.u16() }),
            (U32, U32) => Box::new(NonzeroIndices::<u32, u32> { input: input.u32(), output: output.u32() }),
            (U32, I64) => Box::new(NonzeroIndices::<u32, i64> { input: input.u32(), output: output.i64() }),
            (I64, U8) => Box::new(NonzeroIndices::<i64, u8> { input: input.i64(), output: output.u8() }),
            (I64, U16) => Box::new(NonzeroIndices::<i64, u16> { input: input.i64(), output: output.u16() }),
            (I64, U32) => Box::new(NonzeroIndices::<i64, u32> { input: input.i64(), output: output.u32() }),
            (I64, I64) => Box::new(NonzeroIndices::<i64, i64> { input: input.i64(), output: output.i64() }),
            t => panic!("unsupported type {:?} for grouping key", t),
        }
    }

    pub fn compact((data, input_type): TypedBufferRef,
                   (select, output_type): TypedBufferRef) -> BoxedOperator<'a> {
        match (input_type, output_type) {
            (U8, U8) => Compact::<u8, u8>::boxed(data.u8(), select.u8()),
            (U8, U16) => Compact::<u8, u16>::boxed(data.u8(), select.u16()),
            (U8, U32) => Compact::<u8, u32>::boxed(data.u8(), select.u32()),
            (U8, I64) => Compact::<u8, i64>::boxed(data.u8(), select.i64()),
            (U16, U8) => Compact::<u16, u8>::boxed(data.u16(), select.u8()),
            (U16, U16) => Compact::<u16, u16>::boxed(data.u16(), select.u16()),
            (U16, U32) => Compact::<u16, u32>::boxed(data.u16(), select.u32()),
            (U16, I64) => Compact::<u16, i64>::boxed(data.u16(), select.i64()),
            (U32, U8) => Compact::<u32, u8>::boxed(data.u32(), select.u8()),
            (U32, U16) => Compact::<u32, u16>::boxed(data.u32(), select.u16()),
            (U32, U32) => Compact::<u32, u32>::boxed(data.u32(), select.u32()),
            (U32, I64) => Compact::<u32, i64>::boxed(data.u32(), select.i64()),
            (I64, U8) => Compact::<i64, u8>::boxed(data.i64(), select.u8()),
            (I64, U16) => Compact::<i64, u16>::boxed(data.i64(), select.u16()),
            (I64, U32) => Compact::<i64, u32>::boxed(data.i64(), select.u32()),
            (I64, I64) => Compact::<i64, i64>::boxed(data.i64(), select.i64()),
            t => panic!("unsupported type {:?} for grouping key", t),
        }
    }


    // TODO(clemens): allow different types on raw input grouping key and output grouping key
    pub fn hash_map_grouping((raw_grouping_key, grouping_key_type): TypedBufferRef,
                             (unique_out, t1): TypedBufferRef,
                             grouping_key_out: BufferRef<u32>,
                             cardinality_out: BufferRef<i64>,
                             max_cardinality: usize) -> BoxedOperator<'a> {
        assert!(grouping_key_type == t1);
        match grouping_key_type {
            EncodingType::U8 => HashMapGrouping::<u8>::boxed(raw_grouping_key.u8(), unique_out.u8(), grouping_key_out, cardinality_out, max_cardinality),
            EncodingType::U16 => HashMapGrouping::<u16>::boxed(raw_grouping_key.u16(), unique_out.u16(), grouping_key_out, cardinality_out, max_cardinality),
            EncodingType::U32 => HashMapGrouping::<u32>::boxed(raw_grouping_key.u32(), unique_out.u32(), grouping_key_out, cardinality_out, max_cardinality),
            EncodingType::I64 => HashMapGrouping::<i64>::boxed(raw_grouping_key.i64(), unique_out.i64(), grouping_key_out, cardinality_out, max_cardinality),
            EncodingType::Str => HashMapGrouping::<&str>::boxed(raw_grouping_key.str(), unique_out.str(), grouping_key_out, cardinality_out, max_cardinality),
            EncodingType::ByteSlices(columns) => HashMapGroupingByteSlices::boxed(raw_grouping_key, unique_out, grouping_key_out, cardinality_out, columns),
            t => panic!("unsupported type {:?} for grouping key", t),
        }
    }

    pub fn sort_indices(input: BufferRef<Any>, output: BufferRef<usize>, descending: bool) -> BoxedOperator<'a> {
        Box::new(SortIndices { input, output, descending })
    }

    pub fn top_n((input, t1): TypedBufferRef,
                 (keys_out, t2): TypedBufferRef,
                 indices_out: BufferRef<usize>,
                 n: usize, desc: bool) -> BoxedOperator<'a> {
        assert!(t1 == t2);
        if desc {
            match t1 {
                I64 => Box::new(TopN::<i64, CmpGreaterThan> { input: input.i64(), keys: keys_out.i64(), indices: indices_out, last_index: 0, n, c: PhantomData }),
                U32 => Box::new(TopN::<u32, CmpGreaterThan> { input: input.u32(), keys: keys_out.u32(), indices: indices_out, last_index: 0, n, c: PhantomData }),
                U16 => Box::new(TopN::<u16, CmpGreaterThan> { input: input.u16(), keys: keys_out.u16(), indices: indices_out, last_index: 0, n, c: PhantomData }),
                U8 => Box::new(TopN::<u8, CmpGreaterThan> { input: input.u8(), keys: keys_out.u8(), indices: indices_out, last_index: 0, n, c: PhantomData }),
                Str => Box::new(TopN::<&str, CmpGreaterThan> { input: input.str(), keys: keys_out.str(), indices: indices_out, last_index: 0, n, c: PhantomData }),
                _ => panic!("top_n not supported for type {:?}", t1),
            }
        } else {
            match t1 {
                I64 => Box::new(TopN::<i64, CmpLessThan> { input: input.i64(), keys: keys_out.i64(), indices: indices_out, last_index: 0, n, c: PhantomData }),
                U32 => Box::new(TopN::<u32, CmpLessThan> { input: input.u32(), keys: keys_out.u32(), indices: indices_out, last_index: 0, n, c: PhantomData }),
                U16 => Box::new(TopN::<u16, CmpLessThan> { input: input.u16(), keys: keys_out.u16(), indices: indices_out, last_index: 0, n, c: PhantomData }),
                U8 => Box::new(TopN::<u8, CmpLessThan> { input: input.u8(), keys: keys_out.u8(), indices: indices_out, last_index: 0, n, c: PhantomData }),
                Str => Box::new(TopN::<&str, CmpLessThan> { input: input.str(), keys: keys_out.str(), indices: indices_out, last_index: 0, n, c: PhantomData }),
                _ => panic!("top_n not supported for type {:?}", t1),
            }
        }
    }

    pub fn merge_deduplicate((left, left_t): TypedBufferRef,
                             (right, right_t): TypedBufferRef,
                             (merged_out, t): TypedBufferRef,
                             ops_out: BufferRef<typed_vec::MergeOp>) -> BoxedOperator<'a> {
        assert!(left_t == t);
        match (left_t, right_t) {
            (Str, Str) =>
                Box::new(MergeDeduplicate::<&str> { left: left.str(), right: right.str(), deduplicated: merged_out.str(), merge_ops: ops_out }),
            (U8, U8) =>
                Box::new(MergeDeduplicate::<u8> { left: left.u8(), right: right.u8(), deduplicated: merged_out.u8(), merge_ops: ops_out }),
            (I64, I64) =>
                Box::new(MergeDeduplicate::<i64> { left: left.i64(), right: right.i64(), deduplicated: merged_out.i64(), merge_ops: ops_out }),
            (t1, t2) => panic!("merge_deduplicate types {:?}, {:?}", t1, t2),
        }
    }

    pub fn partition((left, left_t): TypedBufferRef,
                     (right, right_t): TypedBufferRef,
                     partition_out: BufferRef<typed_vec::Premerge>,
                     limit: usize) -> BoxedOperator<'a> {
        match (left_t, right_t) {
            (Str, Str) =>
                Box::new(Partition::<&str> { left: left.str(), right: right.str(), partitioning: partition_out, limit }),
            (I64, I64) =>
                Box::new(Partition::<i64> { left: left.i64(), right: right.i64(), partitioning: partition_out, limit }),
            (t1, t2) => panic!("partition types {:?}, {:?}", t1, t2),
        }
    }


    pub fn subpartition(partitioning: BufferRef<typed_vec::Premerge>,
                        (left, left_t): TypedBufferRef,
                        (right, right_t): TypedBufferRef,
                        subpartition_out: BufferRef<typed_vec::Premerge>) -> BoxedOperator<'a> {
        match (left_t, right_t) {
            (EncodingType::Str, EncodingType::Str) =>
                Box::new(SubPartition::<&str> { partitioning, left: left.str(), right: right.str(), sub_partitioning: subpartition_out }),
            (EncodingType::I64, EncodingType::I64) =>
                Box::new(SubPartition::<i64> { partitioning, left: left.i64(), right: right.i64(), sub_partitioning: subpartition_out }),
            (t1, t2) => panic!("partition types {:?}, {:?}", t1, t2),
        }
    }

    pub fn merge_deduplicate_partitioned(partitioning: BufferRef<typed_vec::Premerge>,
                                         (left, left_t): TypedBufferRef,
                                         (right, right_t): TypedBufferRef,
                                         (merged_out, t): TypedBufferRef,
                                         ops_out: BufferRef<typed_vec::MergeOp>) -> BoxedOperator<'a> {
        assert!(left_t == t);
        match (left_t, right_t) {
            (EncodingType::Str, EncodingType::Str) =>
                Box::new(MergeDeduplicatePartitioned::<&str> { partitioning, left: left.str(), right: right.str(), deduplicated: merged_out.str(), merge_ops: ops_out }),
            (EncodingType::I64, EncodingType::I64) =>
                Box::new(MergeDeduplicatePartitioned::<i64> { partitioning, left: left.i64(), right: right.i64(), deduplicated: merged_out.i64(), merge_ops: ops_out }),
            (t1, t2) => panic!("merge_deduplicate_partitioned types {:?}, {:?}", t1, t2),
        }
    }

    pub fn merge_drop(merge_ops: BufferRef<typed_vec::MergeOp>,
                      (left, left_t): TypedBufferRef,
                      (right, right_t): TypedBufferRef,
                      (merged_out, t): TypedBufferRef) -> BoxedOperator<'a> {
        assert!(left_t == t);
        match (left_t, right_t) {
            (EncodingType::Str, EncodingType::Str) =>
                Box::new(MergeDrop::<&str> { merge_ops, left: left.str(), right: right.str(), deduplicated: merged_out.str() }),
            (EncodingType::I64, EncodingType::I64) =>
                Box::new(MergeDrop::<i64> { merge_ops, left: left.i64(), right: right.i64(), deduplicated: merged_out.i64() }),
            (t1, t2) => panic!("merge_drop types {:?}, {:?}", t1, t2),
        }
    }

    pub fn merge_aggregate(merge_ops: BufferRef<typed_vec::MergeOp>,
                           left: BufferRef<i64>,
                           right: BufferRef<i64>,
                           aggregated_out: BufferRef<i64>,
                           aggregator: Aggregator) -> BoxedOperator<'a> {
        Box::new(MergeAggregate { merge_ops, left, right, aggregated: aggregated_out, aggregator })
    }

    pub fn merge((left, left_t): TypedBufferRef,
                 (right, right_t): TypedBufferRef,
                 (merged_out, t): TypedBufferRef,
                 ops_out: BufferRef<u8>,
                 limit: usize,
                 desc: bool) -> BoxedOperator<'a> {
        assert!(left_t == t);
        if desc {
            match (left_t, right_t) {
                (EncodingType::Str, EncodingType::Str) =>
                    Box::new(Merge::<&str, CmpGreaterThan> { left: left.str(), right: right.str(), merged: merged_out.str(), merge_ops: ops_out, limit, c: PhantomData }),
                (EncodingType::I64, EncodingType::I64) =>
                    Box::new(Merge::<i64, CmpGreaterThan> { left: left.i64(), right: right.i64(), merged: merged_out.i64(), merge_ops: ops_out, limit, c: PhantomData }),
                (t1, t2) => panic!("merge types {:?}, {:?}", t1, t2),
            }
        } else {
            match (left_t, right_t) {
                (EncodingType::Str, EncodingType::Str) =>
                    Box::new(Merge::<&str, CmpLessThan> { left: left.str(), right: right.str(), merged: merged_out.str(), merge_ops: ops_out, limit, c: PhantomData }),
                (EncodingType::I64, EncodingType::I64) =>
                    Box::new(Merge::<i64, CmpLessThan> { left: left.i64(), right: right.i64(), merged: merged_out.i64(), merge_ops: ops_out, limit, c: PhantomData }),
                (t1, t2) => panic!("merge types {:?}, {:?}", t1, t2),
            }
        }
    }

    pub fn merge_keep(merge_ops: BufferRef<u8>,
                      (left, left_t): TypedBufferRef,
                      (right, right_t): TypedBufferRef,
                      (merged_out, t): TypedBufferRef) -> BoxedOperator<'a> {
        assert!(left_t == t);
        match (left_t, right_t) {
            (EncodingType::Str, EncodingType::Str) =>
                Box::new(MergeKeep::<&str> { merge_ops, left: left.str(), right: right.str(), merged: merged_out.str() }),
            (EncodingType::I64, EncodingType::I64) =>
                Box::new(MergeKeep::<i64> { merge_ops, left: left.i64(), right: right.i64(), merged: merged_out.i64() }),
            (t1, t2) => panic!("merge_keep types {:?}, {:?}", t1, t2),
        }
    }
}
