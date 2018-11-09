use std::fmt;
use std::marker::PhantomData;

use engine::*;


#[derive(Debug)]
pub struct ParameterizedVecVecIntegerOperator<Op> {
    pub lhs: BufferRef<i64>,
    pub rhs: BufferRef<i64>,
    pub output: BufferRef<i64>,
    pub parameter: i64,
    pub op: PhantomData<Op>,
}

impl<'a, Op: ParameterizedIntegerOperation + fmt::Debug> VecOperator<'a> for ParameterizedVecVecIntegerOperator<Op> {
    fn execute(&mut self, stream: bool, scratchpad: &mut Scratchpad<'a>) {
        let mut output = scratchpad.get_mut(self.output);
        let lhs = scratchpad.get(self.lhs);
        let rhs = scratchpad.get(self.rhs);
        if stream { output.clear(); }
        for (l, r) in lhs.iter().zip(rhs.iter()) {
            output.push(Op::perform(*l, *r, self.parameter));
        }
    }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.output, Vec::with_capacity(batch_size));
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.lhs.any(), self.rhs.any()] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.output.any()] }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, _: usize) -> bool { true }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, alternate: bool) -> String {
        Op::display(self.lhs, self.rhs, self.parameter, alternate)
    }
}


pub trait ParameterizedIntegerOperation {
    fn perform(lhs: i64, rhs: i64, param: i64) -> i64;
    fn display(lhs: BufferRef<i64>, rhs: BufferRef<i64>, param: i64, alternate: bool) -> String;
}

// TODO(clemens): reuse/mutate left buffer?
#[derive(Debug)]
pub struct BitShiftLeftAdd;

impl ParameterizedIntegerOperation for BitShiftLeftAdd {
    fn perform(lhs: i64, rhs: i64, param: i64) -> i64 { lhs + (rhs << param) }
    fn display(lhs: BufferRef<i64>, rhs: BufferRef<i64>, param: i64, alternate: bool) -> String {
        if alternate {
            format!("{} + ({} << {})", lhs, rhs, param)
        } else {
            format!("{} + ({} << $shift)", lhs, rhs)
        }
    }
}

