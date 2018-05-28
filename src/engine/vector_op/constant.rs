use engine::typed_vec::TypedVec;
use engine::vector_op::vector_operator::*;
use ingest::raw_val::RawVal;


#[derive(Debug)]
pub struct Constant {
    pub val: RawVal,
    pub output: BufferRef,
}

impl<'a> VecOperator<'a> for Constant {
    fn execute(&mut self, _: bool, _: &mut Scratchpad<'a>) {}

    fn init(&mut self, _: usize, _: usize, _: bool, scratchpad: &mut Scratchpad<'a>) {
        let result = TypedVec::constant(self.val.clone());
        scratchpad.set(self.output, result);
    }

    fn inputs(&self) -> Vec<BufferRef> { vec![] }
    fn outputs(&self) -> Vec<BufferRef> { vec![self.output] }
    fn can_stream_input(&self) -> bool { false }
    fn can_stream_output(&self) -> bool { true }
    fn allocates(&self) -> bool { false }
}
