use std::fmt;
use std::marker::PhantomData;

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
        let mut result = scratchpad.get_mut::<u8>(self.lhs);
        let rhs = scratchpad.get::<u8>(self.rhs);
        T::evaluate(&mut result, &rhs);
    }

    fn inputs(&self) -> Vec<BufferRef> { vec![self.lhs, self.rhs] }
    fn outputs(&self) -> Vec<BufferRef> { vec![self.lhs] }
    fn can_stream_input(&self, _: BufferRef) -> bool { true }
    fn can_stream_output(&self, _: BufferRef) -> bool { true }
    fn allocates(&self) -> bool { false }

    fn display_op(&self, _: bool) -> String {
        format!("{} {} {}", self.lhs, T::symbol(), self.rhs)
    }
}

pub trait BooleanOp {
    fn evaluate(lhs: &mut [u8], rhs: &[u8]);
    fn name() -> &'static str;
    fn symbol() -> &'static str;
}

#[derive(Debug)]
pub struct BooleanOr;

impl BooleanOp for BooleanOr {
    fn evaluate(lhs: &mut [u8], rhs: &[u8]) {
        for (l, r) in lhs.iter_mut().zip(rhs) {
            *l |= r;
        }
    }

    fn name() -> &'static str { "bit_vec_or" }
    fn symbol() -> &'static str { "|" }
}

#[derive(Debug)]
pub struct BooleanAnd;

impl BooleanOp for BooleanAnd {
    fn evaluate(lhs: &mut [u8], rhs: &[u8]) {
        for (l, r) in lhs.iter_mut().zip(rhs) {
            *l &= r;
        }
    }

    fn name() -> &'static str { "bit_vec_and" }
    fn symbol() -> &'static str { "&" }
}

