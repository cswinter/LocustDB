use engine::typed_vec::TypedVec;
use engine::vector_op::vector_operator::*;
use ingest::raw_val::RawVal;


#[derive(Debug)]
pub struct Constant {
    pub val: RawVal,
    pub output: BufferRef,
}

impl<'a> VecOperator<'a> for Constant {
    fn execute(&mut self, scratchpad: &mut Scratchpad<'a>) {
        let result = TypedVec::constant(self.val.clone());
        scratchpad.set(self.output, result);
    }
}
