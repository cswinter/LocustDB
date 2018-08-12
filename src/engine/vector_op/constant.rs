use engine::typed_vec::AnyVec;
use engine::vector_op::vector_operator::*;
use ingest::raw_val::RawVal;


#[derive(Debug)]
pub struct Constant {
    pub val: RawVal,
    pub hide_value: bool,
    pub output: BufferRef,
}

impl<'a> VecOperator<'a> for Constant {
    fn execute(&mut self, _: bool, _: &mut Scratchpad<'a>) {}

    fn init(&mut self, _: usize, _: usize, scratchpad: &mut Scratchpad<'a>) {
        let result = AnyVec::constant(self.val.clone());
        scratchpad.set(self.output, result);
    }

    fn inputs(&self) -> Vec<BufferRef> { vec![] }
    fn outputs(&self) -> Vec<BufferRef> { vec![self.output] }
    fn can_stream_input(&self, _: BufferRef) -> bool { false }
    fn can_stream_output(&self, _: BufferRef) -> bool { true }
    fn allocates(&self) -> bool { false }

    fn display_op(&self, alternate: bool) -> String {
        if self.hide_value && !alternate {
            format!("Constant<{:?}>", self.val.get_type())
        } else {
            format!("{}", &self.val)
        }
    }
}
