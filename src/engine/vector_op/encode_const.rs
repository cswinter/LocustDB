use engine::*;
use engine::vector_op::vector_operator::*;
use mem_store::*;

#[derive(Debug)]
pub struct EncodeIntConstant {
    pub constant: BufferRef<i64>,
    pub output: BufferRef<i64>,
    pub codec: Codec,
}

impl<'a> VecOperator<'a> for EncodeIntConstant {
    fn execute(&mut self, _: bool, _: &mut Scratchpad<'a>) {}

    fn init(&mut self, _: usize, _: usize, scratchpad: &mut Scratchpad<'a>) {
        let constant = scratchpad.get_const::<i64>(&self.constant);
        let result = self.codec.encode_int(constant);
        scratchpad.set_any(self.output.any(), AnyVec::constant(result));
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.constant.any()] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.output.any()] }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, _: usize) -> bool { true }
    fn allocates(&self) -> bool { false }

    fn display_op(&self, _: bool) -> String {
        format!("encode({}; {:?})", self.constant, self.codec)
    }
}

