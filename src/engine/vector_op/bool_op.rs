use std::fmt;
use std::marker::PhantomData;

use bit_vec::BitVec;

use engine::vector_op::vector_operator::*;


#[derive(Debug)]
pub struct BooleanOperator<T> {
    lhs: BufferRef,
    rhs: BufferRef,
    op: PhantomData<T>,
}

impl<'a, T: BooleanOp + fmt::Debug + 'a> BooleanOperator<T> {
    pub fn compare(lhs: BufferRef, rhs: BufferRef) -> BoxedOperator<'a> {
        Box::new(BooleanOperator::<T> {
            lhs,
            rhs,
            op: PhantomData,
        })
    }
}

impl<'a, T: BooleanOp + fmt::Debug> VecOperator<'a> for BooleanOperator<T> {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) {
        let mut result = scratchpad.get_mut_bit_vec(self.lhs);
        let rhs = scratchpad.get_bit_vec(self.rhs);
        T::evaluate(&mut result, &rhs);
    }

    fn inputs(&self) -> Vec<BufferRef> { vec![self.lhs, self.rhs] }
    fn outputs(&self) -> Vec<BufferRef> { vec![self.rhs] }
    fn can_stream_input(&self) -> bool { true }
    fn can_stream_output(&self) -> bool { true }
    fn allocates(&self) -> bool { false }
}

pub trait BooleanOp {
    fn evaluate(lhs: &mut BitVec, rhs: &BitVec);
    fn name() -> &'static str;
}

#[derive(Debug)]
pub struct BooleanOr;

#[derive(Debug)]
pub struct BooleanAnd;

impl BooleanOp for BooleanOr {
    fn evaluate(lhs: &mut BitVec, rhs: &BitVec) { lhs.union(rhs); }
    fn name() -> &'static str { "bit_vec_or" }
}

impl BooleanOp for BooleanAnd {
    fn evaluate(lhs: &mut BitVec, rhs: &BitVec) { lhs.intersect(rhs); }
    fn name() -> &'static str { "bit_vec_and" }
}

