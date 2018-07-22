use std::fmt;
use std::mem;

use engine::typed_vec::*;
use engine::vector_op::vector_operator::*;


pub struct ConstantVec<'a> {
    pub val: BoxedVec<'a>,
    pub output: BufferRef,
}

impl<'a> VecOperator<'a> for ConstantVec<'a> {
    fn execute(&mut self, _: bool, _: &mut Scratchpad<'a>) {}

    fn init(&mut self, _: usize, _: usize, _: bool, scratchpad: &mut Scratchpad<'a>) {
        let owned = mem::replace(&mut self.val, AnyVec::empty(0));
        scratchpad.set(self.output, owned);
    }

    fn inputs(&self) -> Vec<BufferRef> { vec![] }
    fn outputs(&self) -> Vec<BufferRef> { vec![self.output] }
    fn can_stream_input(&self, _: BufferRef) -> bool { false }
    fn can_stream_output(&self, _: BufferRef) -> bool { true }
    fn allocates(&self) -> bool { false }

    fn display_op(&self, _: bool) -> String {
        format!("ConstantVec")
    }
}

impl<'a> fmt::Debug for ConstantVec<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_op(false))
    }
}

