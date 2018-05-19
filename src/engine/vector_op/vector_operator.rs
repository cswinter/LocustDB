use std::borrow::BorrowMut;
use std::marker::PhantomData;
use std::cell::{RefCell, Ref, RefMut};
use std::fmt;
use std::mem;

use bit_vec::BitVec;

use engine::aggregation_operator::*;
use engine::typed_vec::TypedVec;
use engine::types::EncodingType;
use engine::*;
use ingest::raw_val::RawVal;
use mem_store::*;

use engine::vector_op::bit_unpack::BitUnpackOperator;
use engine::vector_op::bool_op::*;
use engine::vector_op::column_ops::*;
use engine::vector_op::constant::Constant;
use engine::vector_op::decode::Decode;
use engine::vector_op::encode_const::*;
use engine::vector_op::filter::Filter;
use engine::vector_op::parameterized_vec_vec_int_op::*;
use engine::vector_op::sort_indices::SortIndices;
use engine::vector_op::type_conversion::TypeConversionOperator;
use engine::vector_op::select::Select;
use engine::vector_op::vec_const_bool_op::*;


pub type BoxedOperator<'a> = Box<VecOperator<'a> + 'a>;

#[derive(Debug, Clone, Copy)]
pub struct BufferRef(pub usize);

pub trait VecOperator<'a>: fmt::Debug {
    fn execute(&mut self, scratchpad: &mut Scratchpad<'a>);
}

pub struct Scratchpad<'a> {
    buffers: Vec<RefCell<BoxedVec<'a>>>,
}

impl<'a> Scratchpad<'a> {
    pub fn new(count: usize) -> Scratchpad<'a> {
        let mut buffers = Vec::with_capacity(count);
        for _ in 0..count {
            buffers.push(RefCell::new(TypedVec::empty(0)));
        }
        Scratchpad { buffers }
    }

    pub fn get_any(&self, index: BufferRef) -> Ref<TypedVec<'a>> {
        Ref::map(self.buffers[index.0].borrow(), |x| x.as_ref())
    }

    pub fn get<T: VecType<T> + 'a>(&self, index: BufferRef) -> Ref<[T]> {
        Ref::map(self.buffers[index.0].borrow(), |x| T::unwrap(x.as_ref()))
    }

    pub fn get_mut<T: VecType<T> + 'a>(&self, index: BufferRef) -> RefMut<[T]> {
        RefMut::map(self.buffers[index.0].borrow_mut(), |x| {
            let a: &mut TypedVec<'a> = x.borrow_mut();
            T::unwrap_mut(a)
        })
    }

    pub fn get_const<T: ConstType<T>>(&self, index: BufferRef) -> T {
        T::unwrap(&*self.get_any(index))
    }

    pub fn get_bit_vec(&self, index: BufferRef) -> Ref<BitVec> {
        Ref::map(self.buffers[index.0].borrow(), |x| {
            let a: &TypedVec = x.as_ref();
            a.cast_ref_bit_vec()
        })
    }

    pub fn get_mut_bit_vec(&self, index: BufferRef) -> RefMut<BitVec> {
        RefMut::map(self.buffers[index.0].borrow_mut(), |x: &mut BoxedVec| {
            let a: &mut TypedVec = x.borrow_mut();
            a.cast_ref_mut_bit_vec()
        })
    }

    pub fn collect(&mut self, index: BufferRef) -> BoxedVec<'a> {
        let owned = mem::replace(&mut self.buffers[index.0], RefCell::new(TypedVec::empty(0)));
        owned.into_inner()
    }

    pub fn set(&mut self, index: BufferRef, vec: BoxedVec<'a>) {
        self.buffers[index.0] = RefCell::new(vec);
    }
}


impl<'a> VecOperator<'a> {
    pub fn get_decode(col: &'a Column, output: BufferRef) -> BoxedOperator<'a> {
        Box::new(GetDecode { col, output })
    }

    pub fn get_encoded(col: &'a Column, output: BufferRef) -> BoxedOperator<'a> {
        Box::new(GetEncoded { col, output })
    }

    pub fn decode(input: BufferRef, output: BufferRef, codec: Codec<'a>) -> BoxedOperator<'a> {
        Box::new(Decode { input, output, codec })
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

    pub fn constant(val: RawVal, output: BufferRef) -> BoxedOperator<'a> {
        Box::new(Constant { val, output })
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

    pub fn summation(input: BufferRef,
                     grouping: BufferRef,
                     output: BufferRef,
                     input_type: EncodingType,
                     grouping_type: EncodingType,
                     max_index: usize,
                     dense_grouping: bool) -> BoxedOperator<'a> {
        use self::EncodingType::*;
        match (input_type, grouping_type) {
            (U8, U8) => VecSum::<u8, u8>::boxed(input, grouping, output, max_index, dense_grouping),
            (U8, U16) => VecSum::<u8, u16>::boxed(input, grouping, output, max_index, dense_grouping),
            (U8, U32) => VecSum::<u8, u32>::boxed(input, grouping, output, max_index, dense_grouping),
            // (U8, I64) => VecSum::<u8, u64>::boxed(input, grouping, output, max_index, dense_grouping),
            (U16, U8) => VecSum::<u16, u8>::boxed(input, grouping, output, max_index, dense_grouping),
            (U16, U16) => VecSum::<u16, u16>::boxed(input, grouping, output, max_index, dense_grouping),
            (U16, U32) => VecSum::<u16, u32>::boxed(input, grouping, output, max_index, dense_grouping),
            // (U16, I64) => VecSum::<u16, u64>::boxed(input, grouping, output, max_index, dense_grouping),
            (U32, U8) => VecSum::<u32, u8>::boxed(input, grouping, output, max_index, dense_grouping),
            (U32, U16) => VecSum::<u32, u16>::boxed(input, grouping, output, max_index, dense_grouping),
            (U32, U32) => VecSum::<u32, u32>::boxed(input, grouping, output, max_index, dense_grouping),
            // (U32, I64) => VecSum::<u32, u64>::boxed(input, grouping, output, max_index, dense_grouping),
            (I64, U8) => VecSum::<i64, u8>::boxed(input, grouping, output, max_index, dense_grouping),
            (I64, U16) => VecSum::<i64, u16>::boxed(input, grouping, output, max_index, dense_grouping),
            (I64, U32) => VecSum::<i64, u32>::boxed(input, grouping, output, max_index, dense_grouping),
            // (I64, I64) => VecSum::<i64, u64>::boxed(input, grouping, output, max_index, dense_grouping),
            (pt, gt) => panic!("invalid aggregation types {:?}, {:?}", pt, gt),
        }
    }

    pub fn count(grouping: BufferRef, output: BufferRef, grouping_type: EncodingType, max_index: usize, dense_grouping: bool) -> BoxedOperator<'a> {
        match grouping_type {
            EncodingType::U8 => Box::new(VecCount::<u8>::new(grouping, output, max_index, dense_grouping)),
            EncodingType::U16 => Box::new(VecCount::<u16>::new(grouping, output, max_index, dense_grouping)),
            EncodingType::U32 => Box::new(VecCount::<u32>::new(grouping, output, max_index, dense_grouping)),
            EncodingType::I64 => Box::new(VecCount::<i64>::new(grouping, output, max_index, dense_grouping)),
            t => panic!("unsupported type {:?} for grouping key", t),
        }
    }

    pub fn unique(input: BufferRef,
                  output: BufferRef,
                  input_type: EncodingType,
                  max_index: usize) -> BoxedOperator<'a> {
        match input_type {
            EncodingType::U8 => Unique::<u8>::boxed(input, output, max_index),
            EncodingType::U16 => Unique::<u16>::boxed(input, output, max_index),
            EncodingType::U32 => Unique::<u32>::boxed(input, output, max_index),
            EncodingType::I64 => Unique::<i64>::boxed(input, output, max_index),
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
}


