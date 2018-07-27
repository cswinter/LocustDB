use engine::*;
use engine::vector_op::vector_operator::*;
use mem_store::*;

#[derive(Debug)]
pub struct EncodeIntConstant {
    pub constant: BufferRef,
    pub output: BufferRef,
    pub codec: Codec,
}

impl<'a> VecOperator<'a> for EncodeIntConstant {
    fn execute(&mut self, _: bool, _: &mut Scratchpad<'a>) {}

    fn init(&mut self, _: usize, _: usize, scratchpad: &mut Scratchpad<'a>) {
        let constant = scratchpad.get_const::<i64>(self.constant);
        let result = self.codec.encode_int(constant);
        scratchpad.set(self.output, AnyVec::constant(result));
    }

    fn inputs(&self) -> Vec<BufferRef> { vec![self.constant] }
    fn outputs(&self) -> Vec<BufferRef> { vec![self.output] }
    fn can_stream_input(&self, _: BufferRef) -> bool { true }
    fn can_stream_output(&self, _: BufferRef) -> bool { true }
    fn allocates(&self) -> bool { false }

    fn display_op(&self, _: bool) -> String {
        format!("encode({}; {:?})", self.constant, self.codec)
    }
}

