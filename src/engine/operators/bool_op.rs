#![allow(unused)]
use crate::engine::*;
use std::fmt;
use std::marker::PhantomData;

#[derive(Debug)]
pub struct InplaceBooleanOperator<T> {
    lhs: BufferRef<u8>,
    rhs: BufferRef<u8>,
    output: BufferRef<u8>,
    op: PhantomData<T>,
}

impl<'a, T: BooleanOp + fmt::Debug + 'a> InplaceBooleanOperator<T> {
    pub fn compare(
        lhs: BufferRef<u8>,
        rhs: BufferRef<u8>,
        output: BufferRef<u8>,
    ) -> BoxedOperator<'a> {
        Box::new(InplaceBooleanOperator::<T> {
            lhs,
            rhs,
            output,
            op: PhantomData,
        })
    }
}

impl<'a, T: BooleanOp + fmt::Debug> VecOperator<'a> for InplaceBooleanOperator<T> {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError> {
        if scratchpad.is_alias(self.lhs, self.rhs) {
            let mut result = scratchpad.get_mut(self.lhs);
            T::evaluate_aliased(&mut result);
        } else {
            let mut result = scratchpad.get_mut(self.lhs);
            let rhs = scratchpad.get(self.rhs);
            T::evaluate_inplace(&mut result, &rhs);
        }
        Ok(())
    }

    fn init(&mut self, _: usize, _: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.alias(self.lhs, self.output);
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> {
        vec![self.lhs.any(), self.rhs.any()]
    }
    fn inputs_mut(&mut self) -> Vec<&mut usize> { vec![&mut self.lhs.i, &mut self.rhs.i] }
    fn outputs(&self) -> Vec<BufferRef<Any>> {
        vec![self.output.any()]
    }
    fn can_stream_input(&self, _: usize) -> bool {
        true
    }
    fn can_stream_output(&self, _: usize) -> bool {
        true
    }
    fn mutates(&self, i: usize) -> bool {
        self.lhs.i == i
    }
    fn allocates(&self) -> bool {
        false
    }

    fn display_op(&self, _: bool) -> String {
        format!("mut {} {} {}", self.lhs, T::symbol(), self.rhs)
    }
}

pub trait BooleanOp {
    fn evaluate_inplace(lhs: &mut [u8], rhs: &[u8]);
    // Specialized case for when lhs refers to the same buffer as rhs
    fn evaluate_aliased(lhs: &mut [u8]);
    fn symbol() -> &'static str;
}

#[derive(Debug)]
pub struct BooleanOr;

impl BooleanOp for BooleanOr {
    fn evaluate_inplace(lhs: &mut [u8], rhs: &[u8]) {
        for (l, r) in lhs.iter_mut().zip(rhs) {
            *l |= r;
        }
    }

    fn evaluate_aliased(_lhs: &mut [u8]) {}

    fn symbol() -> &'static str {
        "|"
    }
}

#[derive(Debug)]
pub struct BooleanAnd;

impl BooleanOp for BooleanAnd {
    fn evaluate_inplace(lhs: &mut [u8], rhs: &[u8]) {
        for (l, r) in lhs.iter_mut().zip(rhs) {
            *l &= r;
        }
    }

    fn evaluate_aliased(_lhs: &mut [u8]) {}

    fn symbol() -> &'static str {
        "&"
    }
}
