use std::marker::PhantomData;
use std::rc::Rc;

use bit_vec::BitVec;
use engine::typed_vec::TypedVec;
use engine::types::EncodingType;
use engine::vector_op::types::*;
use ingest::raw_val::RawVal;
use mem_store::column::{ColumnData, ColumnCodec};


pub type BoxedOperator<'a> = Box<VecOperator<'a> + 'a>;

pub trait VecOperator<'a> {
    fn execute(&mut self) -> TypedVec<'a>;
}


pub struct GetDecode<'a> { col: &'a ColumnData }

impl<'a> GetDecode<'a> {
    pub fn new(col: &'a ColumnData) -> GetDecode { GetDecode { col } }
}

impl<'a> VecOperator<'a> for GetDecode<'a> {
    fn execute(&mut self) -> TypedVec<'a> {
        self.col.collect_decoded()
    }
}

pub struct FilterDecode<'a> {
    col: &'a ColumnData,
    filter: Rc<BitVec>,
}

impl<'a> FilterDecode<'a> {
    pub fn new(col: &'a ColumnData, filter: Rc<BitVec>) -> FilterDecode<'a> {
        FilterDecode {
            col,
            filter,
        }
    }
}

impl<'a> VecOperator<'a> for FilterDecode<'a> {
    fn execute(&mut self) -> TypedVec<'a> {
        self.col.filter_decode(self.filter.as_ref())
    }
}

pub struct IndexDecode<'a> {
    col: &'a ColumnData,
    filter: Rc<Vec<usize>>,
}

impl<'a> IndexDecode<'a> {
    pub fn new(col: &'a ColumnData, filter: Rc<Vec<usize>>) -> IndexDecode<'a> {
        IndexDecode {
            col,
            filter,
        }
    }
}

impl<'a> VecOperator<'a> for IndexDecode<'a> {
    fn execute(&mut self) -> TypedVec<'a> {
        self.col.index_decode(self.filter.as_ref())
    }
}


pub struct GetEncoded<'a> { col: &'a ColumnCodec }

impl<'a> GetEncoded<'a> {
    pub fn new(col: &'a ColumnCodec) -> GetEncoded { GetEncoded { col } }
}

impl<'a> VecOperator<'a> for GetEncoded<'a> {
    fn execute(&mut self) -> TypedVec<'a> {
        self.col.get_encoded()
    }
}


pub struct FilterEncoded<'a> {
    col: &'a ColumnCodec,
    filter: Rc<BitVec>,
}

impl<'a> FilterEncoded<'a> {
    pub fn new(col: &'a ColumnCodec, filter: Rc<BitVec>) -> FilterEncoded<'a> {
        FilterEncoded {
            col,
            filter,
        }
    }
}

impl<'a> VecOperator<'a> for FilterEncoded<'a> {
    fn execute(&mut self) -> TypedVec<'a> {
        self.col.filter_encoded(self.filter.as_ref())
    }
}

pub struct IndexEncoded<'a> {
    col: &'a ColumnCodec,
    filter: Rc<Vec<usize>>,
}

impl<'a> IndexEncoded<'a> {
    pub fn new(col: &'a ColumnCodec, filter: Rc<Vec<usize>>) -> IndexEncoded<'a> {
        IndexEncoded {
            col,
            filter,
        }
    }
}

impl<'a> VecOperator<'a> for IndexEncoded<'a> {
    fn execute(&mut self) -> TypedVec<'a> {
        self.col.index_encoded(self.filter.as_ref())
    }
}

pub struct Decode<'a> { plan: BoxedOperator<'a> }

impl<'a> Decode<'a> {
    pub fn new(plan: BoxedOperator<'a>) -> Decode<'a> {
        Decode { plan }
    }
}

impl<'a> VecOperator<'a> for Decode<'a> {
    fn execute(&mut self) -> TypedVec<'a> {
        let encoded = self.plan.execute();
        encoded.decode()
    }
}


pub struct DecodeWith<'a> { plan: BoxedOperator<'a>, codec: &'a ColumnCodec }

impl<'a> DecodeWith<'a> {
    pub fn new(plan: BoxedOperator<'a>, codec: &'a ColumnCodec) -> DecodeWith<'a> {
        DecodeWith { plan, codec }
    }
}

impl<'a> VecOperator<'a> for DecodeWith<'a> {
    fn execute(&mut self) -> TypedVec<'a> {
        let encoded = self.plan.execute();
        self.codec.unwrap_decode(&encoded)
    }
}


pub struct Constant { val: RawVal }

impl Constant {
    pub fn new(val: RawVal) -> Constant {
        Constant { val }
    }
}

impl<'a> VecOperator<'a> for Constant {
    fn execute(&mut self) -> TypedVec<'static> {
        TypedVec::Constant(self.val.clone())
    }
}

pub struct VectorConstant<'a> { pub val: TypedVec<'a> }

impl<'a> VecOperator<'a> for VectorConstant<'a> {
    fn execute(&mut self) -> TypedVec<'a> {
        self.val.clone()
    }
}

impl<'a> VecOperator<'a> {
    pub fn less_than_vs(t: EncodingType, lhs: BoxedOperator<'a>, rhs: BoxedOperator<'a>) -> BoxedOperator<'a> {
        match t {
            EncodingType::U8 => Box::new(VecConstBoolOperator::<u8, i64, LessThanInt<u8>>::new(lhs, rhs)),
            EncodingType::U16 => Box::new(VecConstBoolOperator::<u16, i64, LessThanInt<u16>>::new(lhs, rhs)),
            EncodingType::U32 => Box::new(VecConstBoolOperator::<u32, i64, LessThanInt<u32>>::new(lhs, rhs)),
            EncodingType::I64 => Box::new(VecConstBoolOperator::<i64, i64, LessThanInt<i64>>::new(lhs, rhs)),
            _ => panic!("less_than_vs not supported for type {:?}", t),
        }
    }

    pub fn equals_vs(t: EncodingType, lhs: BoxedOperator<'a>, rhs: BoxedOperator<'a>) -> BoxedOperator<'a> {
        match t {
            EncodingType::Str => Box::new(VecConstBoolOperator::<_, _, EqualsString>::new(lhs, rhs)),
            EncodingType::U8 => Box::new(VecConstBoolOperator::<_, _, EqualsInt<u8>>::new(lhs, rhs)),
            EncodingType::U16 => Box::new(VecConstBoolOperator::<_, _, EqualsInt<u16>>::new(lhs, rhs)),
            EncodingType::U32 => Box::new(VecConstBoolOperator::<_, _, EqualsInt<u32>>::new(lhs, rhs)),
            EncodingType::I64 => Box::new(VecConstBoolOperator::<_, _, Equals<i64>>::new(lhs, rhs)),
            _ => panic!("equals_vs not supported for type {:?}", t),
        }
    }

    pub fn bit_shift_left_add(lhs: BoxedOperator<'a>, rhs: BoxedOperator<'a>, shift_amount: i64) -> BoxedOperator<'a> {
        Box::new(ParameterizedVecVecIntegerOperator::<BitShiftLeftAdd>::new(lhs, rhs, shift_amount))
    }

    pub fn bit_unpack(inner: BoxedOperator<'a>, shift: u8, width: u8) -> BoxedOperator<'a> {
        Box::new(BitUnpackOperator::new(inner, shift, width))
    }

    pub fn type_conversion(inner: BoxedOperator<'a>, initial_type: EncodingType, target_type: EncodingType) -> BoxedOperator<'a> {
        // disallow potentially lossy conversions?
        use self::EncodingType::*;
        match (initial_type, target_type) {
            (U8, U8) | (U16, U16) | (U32, U32) | (I64, I64) => inner,

            (U8, U16) => Box::new(TypeConversionOperator::<u8, u16>::new(inner)),
            (U8, U32) => Box::new(TypeConversionOperator::<u8, u32>::new(inner)),
            (U8, I64) => Box::new(TypeConversionOperator::<u8, i64>::new(inner)),

            (U16, U8) => Box::new(TypeConversionOperator::<u16, u8>::new(inner)),
            (U16, U32) => Box::new(TypeConversionOperator::<u16, u32>::new(inner)),
            (U16, I64) => Box::new(TypeConversionOperator::<u16, i64>::new(inner)),

            (U32, U8) => Box::new(TypeConversionOperator::<u32, u8>::new(inner)),
            (U32, U16) => Box::new(TypeConversionOperator::<u32, u16>::new(inner)),
            (U32, I64) => Box::new(TypeConversionOperator::<u32, i64>::new(inner)),

            (I64, U8) => Box::new(TypeConversionOperator::<i64, u8>::new(inner)),
            (I64, U16) => Box::new(TypeConversionOperator::<i64, u16>::new(inner)),
            (I64, U32) => Box::new(TypeConversionOperator::<i64, u32>::new(inner)),

            _ => panic!("type_conversion not supported for types {:?} -> {:?}", initial_type, target_type)
        }
    }
}


struct VecConstBoolOperator<'a, T: 'a, U, Op> where
    T: VecType<T>, U: ConstType<U>, Op: BoolOperation<T, U> {
    lhs: BoxedOperator<'a>,
    rhs: BoxedOperator<'a>,
    t: PhantomData<T>,
    u: PhantomData<U>,
    op: PhantomData<Op>,
}

impl<'a, T: 'a, U, Op> VecConstBoolOperator<'a, T, U, Op> where
    T: VecType<T>, U: ConstType<U>, Op: BoolOperation<T, U> {
    fn new(lhs: BoxedOperator<'a>, rhs: BoxedOperator<'a>) -> VecConstBoolOperator<'a, T, U, Op> {
        VecConstBoolOperator {
            lhs,
            rhs,
            t: PhantomData,
            u: PhantomData,
            op: PhantomData,
        }
    }
}

impl<'a, T: 'a, U, Op> VecOperator<'a> for VecConstBoolOperator<'a, T, U, Op> where
    T: VecType<T>, U: ConstType<U>, Op: BoolOperation<T, U> {
    fn execute(&mut self) -> TypedVec<'a> {
        let lhs = self.lhs.execute();
        let rhs = self.rhs.execute();
        let data = T::unwrap(&lhs);
        let c = U::unwrap(&rhs);
        let mut output = BitVec::with_capacity(data.len());
        for d in data {
            output.push(Op::perform(d, &c));
        }
        TypedVec::Boolean(output)
    }
}

trait BoolOperation<T, U> {
    fn perform(lhs: &T, rhs: &U) -> bool;
}

struct LessThanInt<T> { t: PhantomData<T> }

impl<T: Into<i64> + Copy> BoolOperation<T, i64> for LessThanInt<T> {
    #[inline]
    fn perform(l: &T, r: &i64) -> bool { Into::<i64>::into(*l) < *r }
}

struct Equals<T> { t: PhantomData<T> }

impl<T: PartialEq> BoolOperation<T, T> for Equals<T> {
    #[inline]
    fn perform(l: &T, r: &T) -> bool { l == r }
}

struct EqualsInt<T> { t: PhantomData<T> }

impl<T: Into<i64> + Copy> BoolOperation<T, i64> for EqualsInt<T> {
    #[inline]
    fn perform(l: &T, r: &i64) -> bool { Into::<i64>::into(*l) == *r }
}

struct EqualsString;

impl<'a> BoolOperation<&'a str, String> for EqualsString {
    #[inline]
    fn perform(l: &&'a str, r: &String) -> bool { l == r }
}


struct BooleanOperator<'a, T> {
    lhs: BoxedOperator<'a>,
    rhs: BoxedOperator<'a>,
    op: PhantomData<T>,
}

impl<'a, T: BooleanOp + 'a> BooleanOperator<'a, T> {
    fn compare(lhs: BoxedOperator<'a>, rhs: BoxedOperator<'a>) -> BoxedOperator<'a> {
        Box::new(BooleanOperator::<'a, T> {
            lhs,
            rhs,
            op: PhantomData,
        })
    }
}

pub struct Boolean;

impl Boolean {
    pub fn or<'a>(lhs: BoxedOperator<'a>, rhs: BoxedOperator<'a>) -> BoxedOperator<'a> {
        BooleanOperator::<BooleanOr>::compare(lhs, rhs)
    }

    pub fn and<'a>(lhs: BoxedOperator<'a>, rhs: BoxedOperator<'a>) -> BoxedOperator<'a> {
        BooleanOperator::<BooleanAnd>::compare(lhs, rhs)
    }
}

impl<'a, T: BooleanOp> VecOperator<'a> for BooleanOperator<'a, T> {
    fn execute(&mut self) -> TypedVec<'a> {
        let _lhs = self.lhs.execute();
        let _rhs = self.rhs.execute();

        let mut result = _lhs.cast_bit_vec();
        let rhs = _rhs.cast_bit_vec();
        T::evaluate(&mut result, &rhs);

        TypedVec::Boolean(result)
    }
}

trait BooleanOp {
    fn evaluate(lhs: &mut BitVec, rhs: &BitVec);
    fn name() -> &'static str;
}

struct BooleanOr;

struct BooleanAnd;

impl BooleanOp for BooleanOr {
    fn evaluate(lhs: &mut BitVec, rhs: &BitVec) { lhs.union(rhs); }
    fn name() -> &'static str { "bit_vec_or" }
}

impl BooleanOp for BooleanAnd {
    fn evaluate(lhs: &mut BitVec, rhs: &BitVec) { lhs.intersect(rhs); }
    fn name() -> &'static str { "bit_vec_and" }
}


pub struct EncodeStrConstant<'a> {
    constant: BoxedOperator<'a>,
    codec: &'a ColumnCodec,
}

impl<'a> EncodeStrConstant<'a> {
    pub fn new(constant: BoxedOperator<'a>, codec: &'a ColumnCodec) -> EncodeStrConstant<'a> {
        EncodeStrConstant {
            constant,
            codec,
        }
    }
}

impl<'a> VecOperator<'a> for EncodeStrConstant<'a> {
    fn execute(&mut self) -> TypedVec<'a> {
        let constant = self.constant.execute();
        let s = constant.cast_str_const();
        let result = self.codec.encode_str(&s);
        TypedVec::Constant(result)
    }
}


pub struct EncodeIntConstant<'a> {
    constant: BoxedOperator<'a>,
    codec: &'a ColumnCodec,
}

impl<'a> EncodeIntConstant<'a> {
    pub fn new(constant: BoxedOperator<'a>, codec: &'a ColumnCodec) -> EncodeIntConstant<'a> {
        EncodeIntConstant {
            constant,
            codec,
        }
    }
}

impl<'a> VecOperator<'a> for EncodeIntConstant<'a> {
    fn execute(&mut self) -> TypedVec<'a> {
        let constant = self.constant.execute();
        let s = constant.cast_int_const();
        let result = self.codec.encode_int(s);
        TypedVec::Constant(result)
    }
}


struct ParameterizedVecVecIntegerOperator<'a, Op> {
    lhs: BoxedOperator<'a>,
    rhs: BoxedOperator<'a>,
    parameter: i64,
    op: PhantomData<Op>,
}

impl<'a, Op> ParameterizedVecVecIntegerOperator<'a, Op> {
    fn new(lhs: BoxedOperator<'a>, rhs: BoxedOperator<'a>, parameter: i64) -> ParameterizedVecVecIntegerOperator<'a, Op> {
        ParameterizedVecVecIntegerOperator {
            lhs,
            rhs,
            parameter,
            op: PhantomData,
        }
    }
}

impl<'a, Op: ParameterizedIntegerOperation> VecOperator<'a> for ParameterizedVecVecIntegerOperator<'a, Op> {
    fn execute(&mut self) -> TypedVec<'a> {
        let lhs = self.lhs.execute();
        let rhs = self.rhs.execute();
        let lhs = lhs.cast_ref_i64();
        let rhs = rhs.cast_ref_i64();
        let mut output = Vec::with_capacity(lhs.len());
        for (l, r) in lhs.iter().zip(rhs) {
            output.push(Op::perform(*l, *r, self.parameter));
        }
        TypedVec::Integer(output)
    }
}

struct BitUnpackOperator<'a> {
    inner: BoxedOperator<'a>,
    shift: u8,
    width: u8,
}

impl<'a> BitUnpackOperator<'a> {
    pub fn new(inner: BoxedOperator<'a>, shift: u8, width: u8) -> BitUnpackOperator<'a> {
        BitUnpackOperator {
            inner,
            shift,
            width,
        }
    }
}

impl<'a> VecOperator<'a> for BitUnpackOperator<'a> {
    fn execute(&mut self) -> TypedVec<'a> {
        let data = self.inner.execute();
        let data = data.cast_ref_i64();
        let mask = (1 << self.width) - 1;
        let mut output = Vec::with_capacity(data.len());
        for d in data {
            output.push((d >> self.shift) & mask);
        }
        TypedVec::Integer(output)
    }
}

trait ParameterizedIntegerOperation {
    fn perform(lhs: i64, rhs: i64, param: i64) -> i64;
}

struct BitShiftLeftAdd;

impl ParameterizedIntegerOperation for BitShiftLeftAdd {
    fn perform(lhs: i64, rhs: i64, param: i64) -> i64 { lhs + (rhs << param) }
}


struct TypeConversionOperator<'a, T, U> {
    inner: BoxedOperator<'a>,
    t: PhantomData<T>,
    s: PhantomData<U>,
}

impl<'a, T, U> TypeConversionOperator<'a, T, U> {
    pub fn new(inner: BoxedOperator<'a>) -> TypeConversionOperator<'a, T, U> {
        TypeConversionOperator {
            inner,
            t: PhantomData,
            s: PhantomData,
        }
    }
}

impl<'a, T: 'a, U: 'a> VecOperator<'a> for TypeConversionOperator<'a, T, U> where
    T: VecType<T> + Copy, U: VecType<U>, T: Cast<U> {
    fn execute(&mut self) -> TypedVec<'a> {
        let data = self.inner.execute();
        let data = T::unwrap(&data);
        let mut output = Vec::with_capacity(data.len());
        for d in data {
            output.push(d.cast());
        }
        U::wrap(output)
    }
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

