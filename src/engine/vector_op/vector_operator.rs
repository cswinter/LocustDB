use std::borrow::BorrowMut;
use std::cell::{RefCell, Ref, RefMut};
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
use engine::vector_op::dict_lookup::DictLookup;
use engine::vector_op::division_vs::DivideVS;
use engine::vector_op::encode_const::*;
use engine::vector_op::exists::Exists;
use engine::vector_op::filter::Filter;
use engine::vector_op::hashmap_grouping::HashMapGrouping;
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
use engine::vector_op::sort_indices::SortIndices;
use engine::vector_op::subpartition::SubPartition;
use engine::vector_op::sum::VecSum;
use engine::vector_op::to_year::ToYear;
use engine::vector_op::top_n::TopN;
use engine::vector_op::type_conversion::TypeConversionOperator;
use engine::vector_op::vec_const_bool_op::*;


pub type BoxedOperator<'a> = Box<VecOperator<'a> + 'a>;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BufferRef(pub usize, pub &'static str);

impl fmt::Display for BufferRef {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}_{}", self.1, self.0)
    }
}


pub trait VecOperator<'a>: fmt::Debug {
    fn execute(&mut self, stream: bool, scratchpad: &mut Scratchpad<'a>);
    fn finalize(&mut self, _scratchpad: &mut Scratchpad<'a>) {}
    fn init(&mut self, _total_count: usize, _batch_size: usize, _stream_outputs: bool, _scratchpad: &mut Scratchpad<'a>) {}

    fn inputs(&self) -> Vec<BufferRef>;
    fn outputs(&self) -> Vec<BufferRef>;
    fn can_stream_input(&self) -> bool;
    fn can_stream_output(&self, i: BufferRef) -> bool;
    fn allocates(&self) -> bool;

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
}

impl<'a> Scratchpad<'a> {
    pub fn new(count: usize) -> Scratchpad<'a> {
        let mut buffers = Vec::with_capacity(count);
        for _ in 0..count {
            buffers.push(RefCell::new(AnyVec::empty(0)));
        }
        Scratchpad { buffers }
    }

    pub fn get_any(&self, index: BufferRef) -> Ref<AnyVec<'a>> {
        Ref::map(self.buffers[index.0].borrow(), |x| x.as_ref())
    }

    pub fn get<T: GenericVec<T> + 'a>(&self, index: BufferRef) -> Ref<[T]> {
        Ref::map(self.buffers[index.0].borrow(), |x| T::unwrap(x.as_ref()))
    }

    pub fn get_mut<T: GenericVec<T> + 'a>(&self, index: BufferRef) -> RefMut<Vec<T>> {
        RefMut::map(self.buffers[index.0].borrow_mut(), |x| {
            let a: &mut AnyVec<'a> = x.borrow_mut();
            T::unwrap_mut(a)
        })
    }

    pub fn get_const<T: ConstType<T>>(&self, index: BufferRef) -> T {
        T::unwrap(&*self.get_any(index))
    }

    pub fn collect(&mut self, index: BufferRef) -> BoxedVec<'a> {
        let owned = mem::replace(&mut self.buffers[index.0], RefCell::new(AnyVec::empty(0)));
        owned.into_inner()
    }

    pub fn set(&mut self, index: BufferRef, vec: BoxedVec<'a>) {
        self.buffers[index.0] = RefCell::new(vec);
    }
}


use self::EncodingType::*;

impl<'a> VecOperator<'a> {
    pub fn get_decode(col: &'a Column, output: BufferRef) -> BoxedOperator<'a> {
        Box::new(GetDecode { col, output })
    }

    pub fn get_encoded(col: &'a Column, output: BufferRef) -> BoxedOperator<'a> {
        Box::new(GetEncoded { col, output, batch_size: 0, current_index: 0 })
    }

    pub fn dict_lookup(indices: BufferRef, dictionary: &'a Vec<String>, output: BufferRef, t: EncodingType) -> BoxedOperator<'a> {
        match t {
            EncodingType::U8 => Box::new(DictLookup::<'a, u8> { indices, output, dictionary, t: PhantomData }),
            EncodingType::U16 => Box::new(DictLookup::<'a, u16> { indices, output, dictionary, t: PhantomData }),
            EncodingType::U32 => Box::new(DictLookup::<'a, u32> { indices, output, dictionary, t: PhantomData }),
            EncodingType::I64 => Box::new(DictLookup::<'a, i64> { indices, output, dictionary, t: PhantomData }),
            _ => panic!("dict_lookup not supported for type {:?}", t),
        }
    }

    pub fn encode_int_const(constant: BufferRef, output: BufferRef, codec: Codec<'a>) -> BoxedOperator<'a> {
        Box::new(EncodeIntConstant { constant, output, codec })
    }

    pub fn encode_str_const(constant: BufferRef, output: BufferRef, codec: Codec<'a>) -> BoxedOperator<'a> {
        Box::new(EncodeStrConstant { constant, output, codec })
    }

    pub fn filter(t: EncodingType, input: BufferRef, filter: BufferRef, output: BufferRef) -> BoxedOperator<'a> {
        match t {
            EncodingType::I64 => Box::new(Filter::<i64> { input, filter, output, t: PhantomData }),
            EncodingType::U32 => Box::new(Filter::<u32> { input, filter, output, t: PhantomData }),
            EncodingType::U16 => Box::new(Filter::<u16> { input, filter, output, t: PhantomData }),
            EncodingType::U8 => Box::new(Filter::<u8> { input, filter, output, t: PhantomData }),
            EncodingType::Str => Box::new(Filter::<&str> { input, filter, output, t: PhantomData }),
            _ => panic!("filter not supported for type {:?}", t),
        }
    }

    pub fn select(t: EncodingType, input: BufferRef, indices: BufferRef, output: BufferRef) -> BoxedOperator<'a> {
        match t {
            EncodingType::I64 => Box::new(Select::<i64> { input, indices, output, t: PhantomData }),
            EncodingType::U32 => Box::new(Select::<u32> { input, indices, output, t: PhantomData }),
            EncodingType::U16 => Box::new(Select::<u16> { input, indices, output, t: PhantomData }),
            EncodingType::U8 => Box::new(Select::<u8> { input, indices, output, t: PhantomData }),
            EncodingType::Str => Box::new(Select::<&str> { input, indices, output, t: PhantomData }),
            _ => panic!("filter not supported for type {:?}", t),
        }
    }

    pub fn constant(val: RawVal, hide_value: bool, output: BufferRef) -> BoxedOperator<'a> {
        Box::new(Constant { val, hide_value, output })
    }

    pub fn constant_vec(val: BoxedVec<'a>, output: BufferRef) -> BoxedOperator<'a> {
        Box::new(ConstantVec { val, output })
    }

    pub fn less_than_vs(t: EncodingType, lhs: BufferRef, rhs: BufferRef, output: BufferRef) -> BoxedOperator<'a> {
        match t {
            EncodingType::U8 => Box::new(VecConstBoolOperator::<u8, i64, LessThanInt<u8>>::new(lhs, rhs, output)),
            EncodingType::U16 => Box::new(VecConstBoolOperator::<u16, i64, LessThanInt<u16>>::new(lhs, rhs, output)),
            EncodingType::U32 => Box::new(VecConstBoolOperator::<u32, i64, LessThanInt<u32>>::new(lhs, rhs, output)),
            EncodingType::I64 => Box::new(VecConstBoolOperator::<i64, i64, LessThanInt<i64>>::new(lhs, rhs, output)),
            _ => panic!("less_than_vs not supported for type {:?}", t),
        }
    }

    pub fn equals_vs(t: EncodingType, lhs: BufferRef, rhs: BufferRef, output: BufferRef) -> BoxedOperator<'a> {
        match t {
            EncodingType::Str => Box::new(VecConstBoolOperator::<_, _, EqualsString>::new(lhs, rhs, output)),
            EncodingType::U8 => Box::new(VecConstBoolOperator::<_, _, EqualsInt<u8>>::new(lhs, rhs, output)),
            EncodingType::U16 => Box::new(VecConstBoolOperator::<_, _, EqualsInt<u16>>::new(lhs, rhs, output)),
            EncodingType::U32 => Box::new(VecConstBoolOperator::<_, _, EqualsInt<u32>>::new(lhs, rhs, output)),
            EncodingType::I64 => Box::new(VecConstBoolOperator::<_, _, Equals<i64>>::new(lhs, rhs, output)),
            _ => panic!("equals_vs not supported for type {:?}", t),
        }
    }

    pub fn not_equals_vs(t: EncodingType, lhs: BufferRef, rhs: BufferRef, output: BufferRef) -> BoxedOperator<'a> {
        match t {
            EncodingType::Str => Box::new(VecConstBoolOperator::<_, _, NotEqualsString>::new(lhs, rhs, output)),
            EncodingType::U8 => Box::new(VecConstBoolOperator::<_, _, NotEqualsInt<u8>>::new(lhs, rhs, output)),
            EncodingType::U16 => Box::new(VecConstBoolOperator::<_, _, NotEqualsInt<u16>>::new(lhs, rhs, output)),
            EncodingType::U32 => Box::new(VecConstBoolOperator::<_, _, NotEqualsInt<u32>>::new(lhs, rhs, output)),
            EncodingType::I64 => Box::new(VecConstBoolOperator::<_, _, NotEquals<i64>>::new(lhs, rhs, output)),
            _ => panic!("not_equals_vs not supported for type {:?}", t),
        }
    }

    pub fn divide_vs(lhs: BufferRef, rhs: BufferRef, output: BufferRef) -> BoxedOperator<'a> {
        Box::new(DivideVS { lhs, rhs, output })
    }

    pub fn addition_vs(lhs: BufferRef, rhs: BufferRef, output: BufferRef, left_type: EncodingType) -> BoxedOperator<'a> {
        match left_type {
            EncodingType::U8 => Box::new(AdditionVS::<u8> { lhs, rhs, output, t: PhantomData }),
            EncodingType::U16 => Box::new(AdditionVS::<u16> { lhs, rhs, output, t: PhantomData }),
            EncodingType::U32 => Box::new(AdditionVS::<u32> { lhs, rhs, output, t: PhantomData }),
            EncodingType::I64 => Box::new(AdditionVS::<i64> { lhs, rhs, output, t: PhantomData }),
            _ => panic!("addition_vs not supported for type {:?}", left_type),
        }
    }

    pub fn or(lhs: BufferRef, rhs: BufferRef) -> BoxedOperator<'a> {
        BooleanOperator::<BooleanOr>::compare(lhs, rhs)
    }

    pub fn and(lhs: BufferRef, rhs: BufferRef) -> BoxedOperator<'a> {
        BooleanOperator::<BooleanAnd>::compare(lhs, rhs)
    }

    pub fn bit_shift_left_add(lhs: BufferRef, rhs: BufferRef, output: BufferRef, shift_amount: i64) -> BoxedOperator<'a> {
        Box::new(ParameterizedVecVecIntegerOperator::<BitShiftLeftAdd>::new(lhs, rhs, output, shift_amount))
    }

    pub fn bit_unpack(inner: BufferRef, output: BufferRef, shift: u8, width: u8) -> BoxedOperator<'a> {
        Box::new(BitUnpackOperator::new(inner, output, shift, width))
    }

    pub fn type_conversion(inner: BufferRef, output: BufferRef, initial_type: EncodingType, target_type: EncodingType) -> BoxedOperator<'a> {
        use self::EncodingType::*;
        match (initial_type, target_type) {
            (U8, U16) => Box::new(TypeConversionOperator::<u8, u16>::new(inner, output)),
            (U8, U32) => Box::new(TypeConversionOperator::<u8, u32>::new(inner, output)),
            (U8, I64) => Box::new(TypeConversionOperator::<u8, i64>::new(inner, output)),

            (U16, U8) => Box::new(TypeConversionOperator::<u16, u8>::new(inner, output)),
            (U16, U32) => Box::new(TypeConversionOperator::<u16, u32>::new(inner, output)),
            (U16, I64) => Box::new(TypeConversionOperator::<u16, i64>::new(inner, output)),

            (U32, U8) => Box::new(TypeConversionOperator::<u32, u8>::new(inner, output)),
            (U32, U16) => Box::new(TypeConversionOperator::<u32, u16>::new(inner, output)),
            (U32, I64) => Box::new(TypeConversionOperator::<u32, i64>::new(inner, output)),

            (I64, U8) => Box::new(TypeConversionOperator::<i64, u8>::new(inner, output)),
            (I64, U16) => Box::new(TypeConversionOperator::<i64, u16>::new(inner, output)),
            (I64, U32) => Box::new(TypeConversionOperator::<i64, u32>::new(inner, output)),

            (U8, U8) | (U16, U16) | (U32, U32) | (I64, I64) => panic!("type_conversion from type {:?} to itself", initial_type),
            _ => panic!("type_conversion not supported for types {:?} -> {:?}", initial_type, target_type)
        }
    }

    pub fn to_year(input: BufferRef, output: BufferRef) -> BoxedOperator<'a> {
        Box::new(ToYear { input, output })
    }

    pub fn summation(input: BufferRef,
                     grouping: BufferRef,
                     output: BufferRef,
                     input_type: EncodingType,
                     grouping_type: EncodingType,
                     max_index: BufferRef) -> BoxedOperator<'a> {
        use self::EncodingType::*;
        match (input_type, grouping_type) {
            (U8, U8) => VecSum::<u8, u8>::boxed(input, grouping, output, max_index),
            (U8, U16) => VecSum::<u8, u16>::boxed(input, grouping, output, max_index),
            (U8, U32) => VecSum::<u8, u32>::boxed(input, grouping, output, max_index),
            (U8, I64) => VecSum::<u8, i64>::boxed(input, grouping, output, max_index),
            (U16, U8) => VecSum::<u16, u8>::boxed(input, grouping, output, max_index),
            (U16, U16) => VecSum::<u16, u16>::boxed(input, grouping, output, max_index),
            (U16, U32) => VecSum::<u16, u32>::boxed(input, grouping, output, max_index),
            (U16, I64) => VecSum::<u16, i64>::boxed(input, grouping, output, max_index),
            (U32, U8) => VecSum::<u32, u8>::boxed(input, grouping, output, max_index),
            (U32, U16) => VecSum::<u32, u16>::boxed(input, grouping, output, max_index),
            (U32, U32) => VecSum::<u32, u32>::boxed(input, grouping, output, max_index),
            (U32, I64) => VecSum::<u32, i64>::boxed(input, grouping, output, max_index),
            (I64, U8) => VecSum::<i64, u8>::boxed(input, grouping, output, max_index),
            (I64, U16) => VecSum::<i64, u16>::boxed(input, grouping, output, max_index),
            (I64, U32) => VecSum::<i64, u32>::boxed(input, grouping, output, max_index),
            (I64, I64) => VecSum::<i64, i64>::boxed(input, grouping, output, max_index),
            (pt, gt) => panic!("invalid aggregation types {:?}, {:?}", pt, gt),
        }
    }

    pub fn count(grouping: BufferRef, output: BufferRef, grouping_type: EncodingType, max_index: BufferRef) -> BoxedOperator<'a> {
        match grouping_type {
            EncodingType::U8 => Box::new(VecCount::<u8>::new(grouping, output, max_index)),
            EncodingType::U16 => Box::new(VecCount::<u16>::new(grouping, output, max_index)),
            EncodingType::U32 => Box::new(VecCount::<u32>::new(grouping, output, max_index)),
            EncodingType::I64 => Box::new(VecCount::<i64>::new(grouping, output, max_index)),
            t => panic!("unsupported type {:?} for grouping key", t),
        }
    }

    pub fn exists(grouping: BufferRef, output: BufferRef, grouping_type: EncodingType, max_index: BufferRef) -> BoxedOperator<'a> {
        match grouping_type {
            EncodingType::U8 => Exists::<u8>::boxed(grouping, output, max_index),
            EncodingType::U16 => Exists::<u16>::boxed(grouping, output, max_index),
            EncodingType::U32 => Exists::<u32>::boxed(grouping, output, max_index),
            EncodingType::I64 => Exists::<i64>::boxed(grouping, output, max_index),
            t => panic!("unsupported type {:?} for grouping key", t),
        }
    }

    pub fn nonzero_compact(data: BufferRef, data_type: EncodingType) -> BoxedOperator<'a> {
        match data_type {
            EncodingType::U8 => NonzeroCompact::<u8>::boxed(data),
            EncodingType::U16 => NonzeroCompact::<u16>::boxed(data),
            EncodingType::U32 => NonzeroCompact::<u32>::boxed(data),
            EncodingType::I64 => NonzeroCompact::<i64>::boxed(data),
            t => panic!("unsupported type {:?} for grouping key", t),
        }
    }

    pub fn nonzero_indices(input: BufferRef,
                           output: BufferRef,
                           input_type: EncodingType,
                           output_type: EncodingType) -> BoxedOperator<'a> {
        use self::EncodingType::*;
        match (input_type, output_type) {
            (U8, U8) => NonzeroIndices::<u8, u8>::boxed(input, output),
            (U8, U16) => NonzeroIndices::<u8, u16>::boxed(input, output),
            (U8, U32) => NonzeroIndices::<u8, u32>::boxed(input, output),
            (U8, I64) => NonzeroIndices::<u8, i64>::boxed(input, output),
            (U16, U8) => NonzeroIndices::<u16, u8>::boxed(input, output),
            (U16, U16) => NonzeroIndices::<u16, u16>::boxed(input, output),
            (U16, U32) => NonzeroIndices::<u16, u32>::boxed(input, output),
            (U16, I64) => NonzeroIndices::<u16, i64>::boxed(input, output),
            (U32, U8) => NonzeroIndices::<u32, u8>::boxed(input, output),
            (U32, U16) => NonzeroIndices::<u32, u16>::boxed(input, output),
            (U32, U32) => NonzeroIndices::<u32, u32>::boxed(input, output),
            (U32, I64) => NonzeroIndices::<u32, i64>::boxed(input, output),
            (I64, U8) => NonzeroIndices::<i64, u8>::boxed(input, output),
            (I64, U16) => NonzeroIndices::<i64, u16>::boxed(input, output),
            (I64, U32) => NonzeroIndices::<i64, u32>::boxed(input, output),
            (I64, I64) => NonzeroIndices::<i64, i64>::boxed(input, output),
            t => panic!("unsupported type {:?} for grouping key", t),
        }
    }

    pub fn compact(data: BufferRef,
                   select: BufferRef,
                   input_type: EncodingType,
                   output_type: EncodingType) -> BoxedOperator<'a> {
        match (input_type, output_type) {
            (U8, U8) => Compact::<u8, u8>::boxed(data, select),
            (U8, U16) => Compact::<u8, u16>::boxed(data, select),
            (U8, U32) => Compact::<u8, u32>::boxed(data, select),
            (U8, I64) => Compact::<u8, i64>::boxed(data, select),
            (U16, U8) => Compact::<u16, u8>::boxed(data, select),
            (U16, U16) => Compact::<u16, u16>::boxed(data, select),
            (U16, U32) => Compact::<u16, u32>::boxed(data, select),
            (U16, I64) => Compact::<u16, i64>::boxed(data, select),
            (U32, U8) => Compact::<u32, u8>::boxed(data, select),
            (U32, U16) => Compact::<u32, u16>::boxed(data, select),
            (U32, U32) => Compact::<u32, u32>::boxed(data, select),
            (U32, I64) => Compact::<u32, i64>::boxed(data, select),
            (I64, U8) => Compact::<i64, u8>::boxed(data, select),
            (I64, U16) => Compact::<i64, u16>::boxed(data, select),
            (I64, U32) => Compact::<i64, u32>::boxed(data, select),
            (I64, I64) => Compact::<i64, i64>::boxed(data, select),
            t => panic!("unsupported type {:?} for grouping key", t),
        }
    }


    // TODO(clemens): allow different types on raw input grouping key and output grouping key
    pub fn hash_map_grouping(raw_grouping_key: BufferRef,
                             unique_out: BufferRef,
                             grouping_key_out: BufferRef,
                             cardinality_out: BufferRef,
                             grouping_key_type: EncodingType,
                             max_cardinality: usize) -> BoxedOperator<'a> {
        match grouping_key_type {
            EncodingType::U8 => HashMapGrouping::<u8>::boxed(raw_grouping_key, unique_out, grouping_key_out, cardinality_out, max_cardinality),
            EncodingType::U16 => HashMapGrouping::<u16>::boxed(raw_grouping_key, unique_out, grouping_key_out, cardinality_out, max_cardinality),
            EncodingType::U32 => HashMapGrouping::<u32>::boxed(raw_grouping_key, unique_out, grouping_key_out, cardinality_out, max_cardinality),
            EncodingType::I64 => HashMapGrouping::<i64>::boxed(raw_grouping_key, unique_out, grouping_key_out, cardinality_out, max_cardinality),
            t => panic!("unsupported type {:?} for grouping key", t),
        }
    }

    pub fn sort_indices(input: BufferRef, output: BufferRef, descending: bool) -> BoxedOperator<'a> {
        Box::new(SortIndices { input, output, descending })
    }

    pub fn top_n(input: BufferRef, keys_out: BufferRef, indices_out: BufferRef, t: EncodingType, n: usize, desc: bool) -> BoxedOperator<'a> {
        if desc {
            match t {
                I64 => Box::new(TopN::<i64, CmpGreaterThan> { input, keys: keys_out, indices: indices_out, last_index: 0, n, t: PhantomData, c: PhantomData }),
                U32 => Box::new(TopN::<u32, CmpGreaterThan> { input, keys: keys_out, indices: indices_out, last_index: 0, n, t: PhantomData, c: PhantomData }),
                U16 => Box::new(TopN::<u16, CmpGreaterThan> { input, keys: keys_out, indices: indices_out, last_index: 0, n, t: PhantomData, c: PhantomData }),
                U8 => Box::new(TopN::<u8, CmpGreaterThan> { input, keys: keys_out, indices: indices_out, last_index: 0, n, t: PhantomData, c: PhantomData }),
                Str => Box::new(TopN::<&str, CmpGreaterThan> { input, keys: keys_out, indices: indices_out, last_index: 0, n, t: PhantomData, c: PhantomData }),
                _ => panic!("top_n not supported for type {:?}", t),
            }
        } else {
            match t {
                I64 => Box::new(TopN::<i64, CmpLessThan> { input, keys: keys_out, indices: indices_out, last_index: 0, n, t: PhantomData, c: PhantomData }),
                U32 => Box::new(TopN::<u32, CmpLessThan> { input, keys: keys_out, indices: indices_out, last_index: 0, n, t: PhantomData, c: PhantomData }),
                U16 => Box::new(TopN::<u16, CmpLessThan> { input, keys: keys_out, indices: indices_out, last_index: 0, n, t: PhantomData, c: PhantomData }),
                U8 => Box::new(TopN::<u8, CmpLessThan> { input, keys: keys_out, indices: indices_out, last_index: 0, n, t: PhantomData, c: PhantomData }),
                Str => Box::new(TopN::<&str, CmpLessThan> { input, keys: keys_out, indices: indices_out, last_index: 0, n, t: PhantomData, c: PhantomData }),
                _ => panic!("top_n not supported for type {:?}", t),
            }
        }
    }

    pub fn merge_deduplicate(left: BufferRef,
                             right: BufferRef,
                             merged_out: BufferRef,
                             ops_out: BufferRef,
                             left_t: EncodingType,
                             right_t: EncodingType) -> BoxedOperator<'a> {
        match (left_t, right_t) {
            (EncodingType::Str, EncodingType::Str) =>
                Box::new(MergeDeduplicate::<&str> { left, right, deduplicated: merged_out, merge_ops: ops_out, t: PhantomData }),
            (EncodingType::U8, EncodingType::U8) =>
                Box::new(MergeDeduplicate::<u8> { left, right, deduplicated: merged_out, merge_ops: ops_out, t: PhantomData }),
            (EncodingType::I64, EncodingType::I64) =>
                Box::new(MergeDeduplicate::<i64> { left, right, deduplicated: merged_out, merge_ops: ops_out, t: PhantomData }),
            (t1, t2) => panic!("merge_deduplicate types {:?}, {:?}", t1, t2),
        }
    }

    pub fn partition(left: BufferRef,
                     right: BufferRef,
                     partition_out: BufferRef,
                     left_t: EncodingType,
                     right_t: EncodingType,
                     limit: usize) -> BoxedOperator<'a> {
        match (left_t, right_t) {
            (EncodingType::Str, EncodingType::Str) =>
                Box::new(Partition::<&str> { left, right, partitioning: partition_out, limit, t: PhantomData }),
            (EncodingType::I64, EncodingType::I64) =>
                Box::new(Partition::<i64> { left, right, partitioning: partition_out, limit, t: PhantomData }),
            (t1, t2) => panic!("partition types {:?}, {:?}", t1, t2),
        }
    }


    pub fn subpartition(partitioning: BufferRef,
                        left: BufferRef,
                        right: BufferRef,
                        subpartition_out: BufferRef,
                        left_t: EncodingType,
                        right_t: EncodingType) -> BoxedOperator<'a> {
        match (left_t, right_t) {
            (EncodingType::Str, EncodingType::Str) =>
                Box::new(SubPartition::<&str> { partitioning, left, right, sub_partitioning: subpartition_out, t: PhantomData }),
            (EncodingType::I64, EncodingType::I64) =>
                Box::new(SubPartition::<i64> { partitioning, left, right, sub_partitioning: subpartition_out, t: PhantomData }),
            (t1, t2) => panic!("partition types {:?}, {:?}", t1, t2),
        }
    }

    pub fn merge_deduplicate_partitioned(partitioning: BufferRef,
                                         left: BufferRef,
                                         right: BufferRef,
                                         merged_out: BufferRef,
                                         ops_out: BufferRef,
                                         left_t: EncodingType,
                                         right_t: EncodingType) -> BoxedOperator<'a> {
        match (left_t, right_t) {
            (EncodingType::Str, EncodingType::Str) =>
                Box::new(MergeDeduplicatePartitioned::<&str> { partitioning, left, right, deduplicated: merged_out, merge_ops: ops_out, t: PhantomData }),
            (EncodingType::I64, EncodingType::I64) =>
                Box::new(MergeDeduplicatePartitioned::<i64> { partitioning, left, right, deduplicated: merged_out, merge_ops: ops_out, t: PhantomData }),
            (t1, t2) => panic!("merge_deduplicate_partitioned types {:?}, {:?}", t1, t2),
        }
    }

    pub fn merge_drop(merge_ops: BufferRef,
                      left: BufferRef,
                      right: BufferRef,
                      merged_out: BufferRef,
                      left_t: EncodingType,
                      right_t: EncodingType) -> BoxedOperator<'a> {
        match (left_t, right_t) {
            (EncodingType::Str, EncodingType::Str) =>
                Box::new(MergeDrop::<&str> { merge_ops, left, right, deduplicated: merged_out, t: PhantomData }),
            (EncodingType::I64, EncodingType::I64) =>
                Box::new(MergeDrop::<i64> { merge_ops, left, right, deduplicated: merged_out, t: PhantomData }),
            (t1, t2) => panic!("merge_drop types {:?}, {:?}", t1, t2),
        }
    }

    pub fn merge_aggregate(merge_ops: BufferRef,
                           left: BufferRef,
                           right: BufferRef,
                           aggregated_out: BufferRef,
                           aggregator: Aggregator) -> BoxedOperator<'a> {
        Box::new(MergeAggregate { merge_ops, left, right, aggregated: aggregated_out, aggregator })
    }

    pub fn merge(left: BufferRef,
                 right: BufferRef,
                 merged_out: BufferRef,
                 ops_out: BufferRef,
                 left_t: EncodingType,
                 right_t: EncodingType,
                 limit: usize,
                 desc: bool) -> BoxedOperator<'a> {
        if desc {
            match (left_t, right_t) {
                (EncodingType::Str, EncodingType::Str) =>
                    Box::new(Merge::<&str, CmpGreaterThan> { left, right, merged: merged_out, merge_ops: ops_out, limit, t: PhantomData, c: PhantomData }),
                (EncodingType::I64, EncodingType::I64) =>
                    Box::new(Merge::<i64, CmpGreaterThan> { left, right, merged: merged_out, merge_ops: ops_out, limit, t: PhantomData, c: PhantomData }),
                (t1, t2) => panic!("merge types {:?}, {:?}", t1, t2),
            }
        } else {
            match (left_t, right_t) {
                (EncodingType::Str, EncodingType::Str) =>
                    Box::new(Merge::<&str, CmpLessThan> { left, right, merged: merged_out, merge_ops: ops_out, limit, t: PhantomData, c: PhantomData }),
                (EncodingType::I64, EncodingType::I64) =>
                    Box::new(Merge::<i64, CmpLessThan> { left, right, merged: merged_out, merge_ops: ops_out, limit, t: PhantomData, c: PhantomData }),
                (t1, t2) => panic!("merge types {:?}, {:?}", t1, t2),
            }
        }
    }

    pub fn merge_keep(merge_ops: BufferRef,
                      left: BufferRef,
                      right: BufferRef,
                      merged_out: BufferRef,
                      left_t: EncodingType,
                      right_t: EncodingType) -> BoxedOperator<'a> {
        match (left_t, right_t) {
            (EncodingType::Str, EncodingType::Str) =>
                Box::new(MergeKeep::<&str> { merge_ops, left, right, merged: merged_out, t: PhantomData }),
            (EncodingType::I64, EncodingType::I64) =>
                Box::new(MergeKeep::<i64> { merge_ops, left, right, merged: merged_out, t: PhantomData }),
            (t1, t2) => panic!("merge_keep types {:?}, {:?}", t1, t2),
        }
    }
}
