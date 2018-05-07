use std::cell::{RefCell, Ref, RefMut};
use std::fmt;
use std::marker::PhantomData;
use std::mem;
use std::borrow::BorrowMut;

use bit_vec::BitVec;
use engine::aggregation_operator::*;
use engine::typed_vec::TypedVec;
use engine::types::EncodingType;
use engine::*;
use ingest::raw_val::RawVal;
use mem_store::column::{ColumnData, ColumnCodec};


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


#[derive(Debug)]
pub struct GetDecode<'a> {
    output: BufferRef,
    col: &'a ColumnData
}

impl<'a> GetDecode<'a> {
    pub fn new(col: &'a ColumnData, output: BufferRef) -> GetDecode { GetDecode { col, output } }
}

impl<'a> VecOperator<'a> for GetDecode<'a> {
    fn execute(&mut self, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.output, self.col.collect_decoded());
    }
}

#[derive(Debug)]
pub struct FilterDecode<'a> {
    col: &'a ColumnData,
    filter: BufferRef,
    output: BufferRef,
}

impl<'a> FilterDecode<'a> {
    pub fn new(col: &'a ColumnData, filter: BufferRef, output: BufferRef) -> FilterDecode<'a> {
        FilterDecode {
            col,
            filter,
            output,
        }
    }
}

impl<'a> VecOperator<'a> for FilterDecode<'a> {
    fn execute(&mut self, scratchpad: &mut Scratchpad<'a>) {
        let result = {
            let filter = scratchpad.get_mut_bit_vec(self.filter);
            self.col.filter_decode(&filter)
        };
        scratchpad.set(self.output, result);
    }
}

#[derive(Debug)]
pub struct IndexDecode<'a> {
    col: &'a ColumnData,
    filter: BufferRef,
    output: BufferRef,
}

impl<'a> IndexDecode<'a> {
    pub fn new(col: &'a ColumnData, filter: BufferRef, output: BufferRef) -> IndexDecode<'a> {
        IndexDecode {
            col,
            filter,
            output,
        }
    }
}

impl<'a> VecOperator<'a> for IndexDecode<'a> {
    fn execute(&mut self, scratchpad: &mut Scratchpad<'a>) {
        let result = {
            let indices = scratchpad.get::<usize>(self.filter);
            self.col.index_decode(&indices)
        };
        scratchpad.set(self.output, result);
    }
}


#[derive(Debug)]
pub struct GetEncoded<'a> {
    col: &'a ColumnCodec,
    output: BufferRef,
}

impl<'a> GetEncoded<'a> {
    pub fn new(col: &'a ColumnCodec, output: BufferRef) -> GetEncoded {
        GetEncoded { col, output }
    }
}

impl<'a> VecOperator<'a> for GetEncoded<'a> {
    fn execute(&mut self, scratchpad: &mut Scratchpad<'a>) {
        let result = self.col.get_encoded();
        scratchpad.set(self.output, result);
    }
}


#[derive(Debug)]
pub struct FilterEncoded<'a> {
    col: &'a ColumnCodec,
    filter: BufferRef,
    output: BufferRef,
}

impl<'a> FilterEncoded<'a> {
    pub fn new(col: &'a ColumnCodec, filter: BufferRef, output: BufferRef) -> FilterEncoded<'a> {
        FilterEncoded {
            col,
            filter,
            output,
        }
    }
}

impl<'a> VecOperator<'a> for FilterEncoded<'a> {
    fn execute(&mut self, scratchpad: &mut Scratchpad<'a>) {
        let result = {
            let filter = scratchpad.get_bit_vec(self.filter);
            self.col.filter_encoded(&filter)
        };
        scratchpad.set(self.output, result);
    }
}

#[derive(Debug)]
pub struct IndexEncoded<'a> {
    col: &'a ColumnCodec,
    filter: BufferRef,
    output: BufferRef,
}

impl<'a> IndexEncoded<'a> {
    pub fn new(col: &'a ColumnCodec, filter: BufferRef, output: BufferRef) -> IndexEncoded<'a> {
        IndexEncoded {
            col,
            filter,
            output,
        }
    }
}

impl<'a> VecOperator<'a> for IndexEncoded<'a> {
    fn execute(&mut self, scratchpad: &mut Scratchpad<'a>) {
        let result = {
            let indices = scratchpad.get::<usize>(self.filter);
            self.col.index_encoded(&indices)
        };
        scratchpad.set(self.output, result);
    }
}

#[derive(Debug)]
pub struct DecodeWith<'a> {
    input: BufferRef,
    output: BufferRef,
    codec: &'a ColumnCodec,
}

impl<'a> DecodeWith<'a> {
    pub fn new(input: BufferRef, output: BufferRef, codec: &'a ColumnCodec) -> DecodeWith<'a> {
        DecodeWith { input, output, codec }
    }
}

impl<'a> VecOperator<'a> for DecodeWith<'a> {
    fn execute(&mut self, scratchpad: &mut Scratchpad<'a>) {
        let result = {
            let encoded = scratchpad.get_any(self.input);
            self.codec.unwrap_decode(&*encoded)
        };
        scratchpad.set(self.output, result);
    }
}

#[derive(Debug)]
pub struct SortIndices {
    input: BufferRef,
    output: BufferRef,
    descending: bool,
}

impl<'a> VecOperator<'a> for SortIndices {
    fn execute(&mut self, scratchpad: &mut Scratchpad<'a>) {
        let result = {
            let input = scratchpad.get_any(self.input);
            let mut result = (0..input.len()).collect();
            if self.descending {
                input.sort_indices_desc(&mut result);
            } else {
                input.sort_indices_asc(&mut result);
            }
            TypedVec::owned(result)
        };
        scratchpad.set(self.output, result);
    }
}

#[derive(Debug)]
pub struct Constant {
    val: RawVal,
    output: BufferRef,
}

impl Constant {
    pub fn new(val: RawVal, output: BufferRef) -> Constant {
        Constant { val, output }
    }
}

impl<'a> VecOperator<'a> for Constant {
    fn execute(&mut self, scratchpad: &mut Scratchpad<'a>) {
        let result = TypedVec::constant(self.val.clone());
        scratchpad.set(self.output, result);
    }
}

/*
pub struct VectorConstant<'a> {
    pub val: BoxedOperator<'a>,
    pub output: BufferRef
}

impl<'a> VecOperator<'a> for VectorConstant<'a> {
    fn execute(&mut self, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.output, self.val.clone());
    }
}

impl<'a> fmt::Debug for VectorConstant<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} <- VectorConstant(...)", self.output.0)
    }
}*/

impl<'a> VecOperator<'a> {
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


#[derive(Debug)]
struct VecConstBoolOperator<T, U, Op> {
    lhs: BufferRef,
    rhs: BufferRef,
    output: BufferRef,
    t: PhantomData<T>,
    u: PhantomData<U>,
    op: PhantomData<Op>,
}

impl<'a, T: 'a, U, Op> VecConstBoolOperator<T, U, Op> where
    T: VecType<T>, U: ConstType<U> + fmt::Debug, Op: BoolOperation<T, U> {
    fn new(lhs: BufferRef, rhs: BufferRef, output: BufferRef) -> VecConstBoolOperator<T, U, Op> {
        VecConstBoolOperator {
            lhs,
            rhs,
            output,
            t: PhantomData,
            u: PhantomData,
            op: PhantomData,
        }
    }
}

impl<'a, T: 'a, U, Op> VecOperator<'a> for VecConstBoolOperator<T, U, Op> where
    T: VecType<T>, U: ConstType<U> + fmt::Debug, Op: BoolOperation<T, U> + fmt::Debug {
    fn execute(&mut self, scratchpad: &mut Scratchpad<'a>) {
        let result = {
            let data = scratchpad.get::<T>(self.lhs);
            let c = &scratchpad.get_const::<U>(self.rhs);
            let mut output = BitVec::with_capacity(data.len());
            for d in data.iter() {
                output.push(Op::perform(d, &c));
            }
            TypedVec::bit_vec(output)
        };
        scratchpad.set(self.output, result);
    }
}

trait BoolOperation<T, U> {
    fn perform(lhs: &T, rhs: &U) -> bool;
}

#[derive(Debug)]
struct LessThanInt<T> { t: PhantomData<T> }

impl<T: Into<i64> + Copy> BoolOperation<T, i64> for LessThanInt<T> {
    #[inline]
    fn perform(l: &T, r: &i64) -> bool { Into::<i64>::into(*l) < *r }
}

#[derive(Debug)]
struct Equals<T> { t: PhantomData<T> }

impl<T: PartialEq> BoolOperation<T, T> for Equals<T> {
    #[inline]
    fn perform(l: &T, r: &T) -> bool { l == r }
}

#[derive(Debug)]
struct EqualsInt<T> { t: PhantomData<T> }

impl<T: Into<i64> + Copy> BoolOperation<T, i64> for EqualsInt<T> {
    #[inline]
    fn perform(l: &T, r: &i64) -> bool { Into::<i64>::into(*l) == *r }
}

#[derive(Debug)]
struct EqualsString;

impl<'a> BoolOperation<&'a str, String> for EqualsString {
    #[inline]
    fn perform(l: &&'a str, r: &String) -> bool { l == r }
}

#[derive(Debug)]
struct BooleanOperator<T> {
    lhs: BufferRef,
    rhs: BufferRef,
    op: PhantomData<T>,
}

impl<'a, T: BooleanOp + fmt::Debug + 'a> BooleanOperator<T> {
    fn compare(lhs: BufferRef, rhs: BufferRef) -> BoxedOperator<'a> {
        Box::new(BooleanOperator::<T> {
            lhs,
            rhs,
            op: PhantomData,
        })
    }
}

pub struct Boolean;

impl Boolean {
    pub fn or<'a>(lhs: BufferRef, rhs: BufferRef) -> BoxedOperator<'a> {
        BooleanOperator::<BooleanOr>::compare(lhs, rhs)
    }

    pub fn and<'a>(lhs: BufferRef, rhs: BufferRef) -> BoxedOperator<'a> {
        BooleanOperator::<BooleanAnd>::compare(lhs, rhs)
    }
}

impl<'a, T: BooleanOp + fmt::Debug> VecOperator<'a> for BooleanOperator<T> {
    fn execute(&mut self, scratchpad: &mut Scratchpad<'a>) {
        let mut result = scratchpad.get_mut_bit_vec(self.lhs);
        let rhs = scratchpad.get_bit_vec(self.rhs);
        T::evaluate(&mut result, &rhs);
    }
}

trait BooleanOp {
    fn evaluate(lhs: &mut BitVec, rhs: &BitVec);
    fn name() -> &'static str;
}

#[derive(Debug)]
struct BooleanOr;

#[derive(Debug)]
struct BooleanAnd;

impl BooleanOp for BooleanOr {
    fn evaluate(lhs: &mut BitVec, rhs: &BitVec) { lhs.union(rhs); }
    fn name() -> &'static str { "bit_vec_or" }
}

impl BooleanOp for BooleanAnd {
    fn evaluate(lhs: &mut BitVec, rhs: &BitVec) { lhs.intersect(rhs); }
    fn name() -> &'static str { "bit_vec_and" }
}

#[derive(Debug)]
pub struct EncodeStrConstant<'a> {
    constant: BufferRef,
    output: BufferRef,
    codec: &'a ColumnCodec,
}

impl<'a> EncodeStrConstant<'a> {
    pub fn new(constant: BufferRef, output: BufferRef, codec: &'a ColumnCodec) -> EncodeStrConstant<'a> {
        EncodeStrConstant {
            constant,
            output,
            codec,
        }
    }
}

impl<'a> VecOperator<'a> for EncodeStrConstant<'a> {
    fn execute(&mut self, scratchpad: &mut Scratchpad<'a>) {
        let result = {
            let constant = scratchpad.get_const::<String>(self.constant);
            self.codec.encode_str(&constant)
        };
        scratchpad.set(self.output, TypedVec::constant(result));
    }
}


#[derive(Debug)]
pub struct EncodeIntConstant<'a> {
    constant: BufferRef,
    output: BufferRef,
    codec: &'a ColumnCodec,
}

impl<'a> EncodeIntConstant<'a> {
    pub fn new(constant: BufferRef, output: BufferRef, codec: &'a ColumnCodec) -> EncodeIntConstant<'a> {
        EncodeIntConstant {
            constant,
            output,
            codec,
        }
    }
}

impl<'a> VecOperator<'a> for EncodeIntConstant<'a> {
    fn execute(&mut self, scratchpad: &mut Scratchpad<'a>) {
        let constant = scratchpad.get_const::<i64>(self.constant);
        let result = self.codec.encode_int(constant);
        scratchpad.set(self.output, TypedVec::constant(result));
    }
}


#[derive(Debug)]
struct ParameterizedVecVecIntegerOperator<Op> {
    lhs: BufferRef,
    rhs: BufferRef,
    output: BufferRef,
    parameter: i64,
    op: PhantomData<Op>,
}

impl<Op> ParameterizedVecVecIntegerOperator<Op> {
    fn new(lhs: BufferRef, rhs: BufferRef, output: BufferRef, parameter: i64) -> ParameterizedVecVecIntegerOperator<Op> {
        ParameterizedVecVecIntegerOperator {
            lhs,
            rhs,
            output,
            parameter,
            op: PhantomData,
        }
    }
}

impl<'a, Op: ParameterizedIntegerOperation + fmt::Debug> VecOperator<'a> for ParameterizedVecVecIntegerOperator<Op> {
    fn execute(&mut self, scratchpad: &mut Scratchpad<'a>) {
        let result = {
            let lhs = scratchpad.get::<i64>(self.lhs);
            let rhs = scratchpad.get::<i64>(self.rhs);
            let mut output = Vec::with_capacity(lhs.len());
            for (l, r) in lhs.iter().zip(rhs.iter()) {
                output.push(Op::perform(*l, *r, self.parameter));
            }
            TypedVec::owned(output)
        };
        scratchpad.set(self.output, result)
    }
}


#[derive(Debug)]
struct BitUnpackOperator {
    input: BufferRef,
    output: BufferRef,
    shift: u8,
    width: u8,
}

impl BitUnpackOperator {
    pub fn new(input: BufferRef, output: BufferRef, shift: u8, width: u8) -> BitUnpackOperator {
        BitUnpackOperator {
            input,
            output,
            shift,
            width,
        }
    }
}

impl<'a> VecOperator<'a> for BitUnpackOperator {
    fn execute(&mut self, scratchpad: &mut Scratchpad<'a>) {
        let result = {
            let data = scratchpad.get::<i64>(self.input);
            let mask = (1 << self.width) - 1;
            let mut output = Vec::with_capacity(data.len());
            for d in data.iter() {
                output.push((d >> self.shift) & mask);
            }
            TypedVec::owned(output)
        };
        scratchpad.set(self.output, result);
    }
}

trait ParameterizedIntegerOperation {
    fn perform(lhs: i64, rhs: i64, param: i64) -> i64;
}

#[derive(Debug)]
struct BitShiftLeftAdd;

impl ParameterizedIntegerOperation for BitShiftLeftAdd {
    fn perform(lhs: i64, rhs: i64, param: i64) -> i64 { lhs + (rhs << param) }
}


#[derive(Debug)]
struct TypeConversionOperator<T, U> {
    input: BufferRef,
    output: BufferRef,
    t: PhantomData<T>,
    s: PhantomData<U>,
}

impl<T, U> TypeConversionOperator<T, U> {
    pub fn new(input: BufferRef, output: BufferRef) -> TypeConversionOperator<T, U> {
        TypeConversionOperator {
            input,
            output,
            t: PhantomData,
            s: PhantomData,
        }
    }
}

impl<'a, T: 'a, U: 'a> VecOperator<'a> for TypeConversionOperator<T, U> where
    T: VecType<T> + Copy, U: VecType<U>, T: Cast<U> {
    fn execute(&mut self, scratchpad: &mut Scratchpad<'a>) {
        let result = {
            let data = scratchpad.get::<T>(self.input);
            let mut output = Vec::with_capacity(data.len());
            for d in data.iter() {
                output.push(d.cast());
            }
            TypedVec::owned(output)
        };
        scratchpad.set(self.output, result);
    }
}

#[derive(Debug)]
struct Identity {
    input: BufferRef,
    output: BufferRef,
}

impl Identity {
    pub fn new(input: BufferRef, output: BufferRef) -> Identity {
        Identity { input, output }
    }
}

impl<'a> VecOperator<'a> for Identity {
    fn execute(&mut self, _scratchpad: &mut Scratchpad<'a>) {}
}


trait Cast<T> {
    fn cast(self) -> T;
}

impl Cast<u8> for u16 { fn cast(self) -> u8 { self as u8 } }

impl Cast<u8> for u32 { fn cast(self) -> u8 { self as u8 } }

impl Cast<u8> for i64 { fn cast(self) -> u8 { self as u8 } }

impl Cast<u16> for u8 { fn cast(self) -> u16 { u16::from(self) } }

impl Cast<u16> for u32 { fn cast(self) -> u16 { self as u16 } }

impl Cast<u16> for i64 { fn cast(self) -> u16 { self as u16 } }

impl Cast<u32> for u8 { fn cast(self) -> u32 { u32::from(self) } }

impl Cast<u32> for u16 { fn cast(self) -> u32 { u32::from(self) } }

impl Cast<u32> for i64 { fn cast(self) -> u32 { self as u32 } }

impl Cast<i64> for u8 { fn cast(self) -> i64 { i64::from(self) } }

impl Cast<i64> for u16 { fn cast(self) -> i64 { i64::from(self) } }

impl Cast<i64> for u32 { fn cast(self) -> i64 { i64::from(self) } }

