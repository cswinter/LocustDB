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
}


struct VecConstBoolOperator<'a, T: 'a, U, Op> where
    T: VecType<'a, T>, U: ConstType<U>, Op: BoolOperation<T, U> {
    lhs: BoxedOperator<'a>,
    rhs: BoxedOperator<'a>,
    t: PhantomData<T>,
    u: PhantomData<U>,
    op: PhantomData<Op>,
}

impl<'a, T: 'a, U, Op> VecConstBoolOperator<'a, T, U, Op> where
    T: VecType<'a, T>, U: ConstType<U>, Op: BoolOperation<T, U> {
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
    T: VecType<'a, T>, U: ConstType<U>, Op: BoolOperation<T, U> {
    fn execute(&mut self) -> TypedVec<'a> {
        let lhs = self.lhs.execute();
        let rhs = self.rhs.execute();
        let data = T::cast(&lhs);
        let c = U::cast(&rhs);
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
