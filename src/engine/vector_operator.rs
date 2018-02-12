use std::marker::PhantomData;
use bit_vec::BitVec;

use mem_store::column::ColIter;
use mem_store::ingest::RawVal;
use value::Val;
use mem_store::column::Column;
use engine::typed_vec::TypedVec;


pub type BoxedOperator<'a> = Box<VecOperator + 'a>;

pub trait VecOperator {
    fn execute(&mut self, filter: &Option<BitVec>) -> TypedVec;
}


pub struct CollectDecoded<'a> { col: &'a Column }

impl<'a> CollectDecoded<'a> {
    pub fn new(col: &'a Column) -> CollectDecoded {
        CollectDecoded { col: col }
    }
}

impl<'a> VecOperator for CollectDecoded<'a> {
    fn execute(&mut self, filter: &Option<BitVec>) -> TypedVec {
        self.col.collect_decoded(filter)
    }
}


pub struct Constant { val: RawVal }

impl Constant {
    pub fn new(val: RawVal) -> Constant {
        Constant { val: val }
    }
}

impl VecOperator for Constant {
    fn execute(&mut self, filter: &Option<BitVec>) -> TypedVec {
        TypedVec::Constant(self.val.clone())
    }
}


pub struct LessThanVSi64<'a> {
    lhs: BoxedOperator<'a>,
    rhs: i64,
}

impl<'a> LessThanVSi64<'a> {
    pub fn new(lhs: BoxedOperator, rhs: i64) -> LessThanVSi64 {
        LessThanVSi64 {
            lhs: lhs,
            rhs: rhs,
        }
    }
}

impl<'a> VecOperator for LessThanVSi64<'a> {
    fn execute(&mut self, filter: &Option<BitVec>) -> TypedVec {
        let lhs = self.lhs.execute(filter);
        let mut result = BitVec::with_capacity(lhs.len());
        let i = self.rhs;
        for l in lhs.cast_i64().iter() {
            result.push(*l < i);
        }
        TypedVec::Boolean(result)
    }
}
