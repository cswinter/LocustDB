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
    fn execute(&mut self, stream: bool, scratchpad: &mut Scratchpad<'a>) {
        let mut output = scratchpad.get_mut::<i64>(self.output);
        let lhs = scratchpad.get::<i64>(self.lhs);
        let rhs = scratchpad.get::<i64>(self.rhs);
        if stream { output.clear(); }
        for (l, r) in lhs.iter().zip(rhs.iter()) {
            output.push(Op::perform(*l, *r, self.parameter));
        }
    }

    fn init(&mut self, _: usize, batch_size: usize, _: bool, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.output, TypedVec::owned(Vec::<i64>::with_capacity(batch_size)));
    }

    fn inputs(&self) -> Vec<BufferRef> { vec![self.lhs, self.rhs] }
    fn outputs(&self) -> Vec<BufferRef> { vec![self.output] }
    fn can_stream_input(&self) -> bool { true }
    fn can_stream_output(&self) -> bool { true }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, alternate: bool) -> String {
        Op::display(self.lhs, self.rhs, self.parameter, alternate)
    }
}


pub trait ParameterizedIntegerOperation {
    fn perform(lhs: i64, rhs: i64, param: i64) -> i64;
    fn display(lhs: BufferRef, rhs: BufferRef, param: i64, alternate: bool) -> String;
}

// TODO(clemens): reuse/mutate left buffer?
#[derive(Debug)]
pub struct BitShiftLeftAdd;

impl ParameterizedIntegerOperation for BitShiftLeftAdd {
    fn perform(lhs: i64, rhs: i64, param: i64) -> i64 { lhs + (rhs << param) }
    fn display(lhs: BufferRef, rhs: BufferRef, param: i64, alternate: bool) -> String {
        if alternate {
            format!("{} + ({} << {})", lhs, rhs, param)
        } else {
            format!("{} + ({} << $shift)", lhs, rhs)
        }
    }
}

