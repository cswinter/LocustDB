use std::fmt;
use std::marker::PhantomData;

use engine::*;


#[derive(Debug)]
pub struct BooleanOperator<T> {
    lhs: BufferRef<u8>,
    rhs: BufferRef<u8>,
    op: PhantomData<T>,
}

impl<'a, T: BooleanOp + fmt::Debug + 'a> BooleanOperator<T> {
    pub fn compare(lhs: BufferRef<u8>, rhs: BufferRef<u8>) -> BoxedOperator<'a> {
        Box::new(BooleanOperator::<T> {
            lhs,
            rhs,
            op: PhantomData,
        })
    }
}

impl<'a, T: BooleanOp + fmt::Debug> VecOperator<'a> for BooleanOperator<T> {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) {
        let mut result = scratchpad.get_mut(self.lhs);
        let rhs = scratchpad.get(self.rhs);
        T::evaluate(&mut result, &rhs);
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.lhs.any(), self.rhs.any()] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.lhs.any()] }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, _: usize) -> bool { true }
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

