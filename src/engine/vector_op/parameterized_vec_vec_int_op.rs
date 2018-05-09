use std::fmt;
use std::marker::PhantomData;

use engine::*;
use engine::vector_op::vector_operator::*;


#[derive(Debug)]
pub struct ParameterizedVecVecIntegerOperator<Op> {
    lhs: BufferRef,
    rhs: BufferRef,
    output: BufferRef,
    parameter: i64,
    op: PhantomData<Op>,
}

impl<Op> ParameterizedVecVecIntegerOperator<Op> {
    pub fn new(lhs: BufferRef, rhs: BufferRef, output: BufferRef, parameter: i64) -> ParameterizedVecVecIntegerOperator<Op> {
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


pub trait ParameterizedIntegerOperation {
    fn perform(lhs: i64, rhs: i64, param: i64) -> i64;
}

#[derive(Debug)]
pub struct BitShiftLeftAdd;

impl ParameterizedIntegerOperation for BitShiftLeftAdd {
    fn perform(lhs: i64, rhs: i64, param: i64) -> i64 { lhs + (rhs << param) }
}

