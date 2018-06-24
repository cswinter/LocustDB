use engine::*;
use engine::vector_op::vector_operator::*;
use mem_store::*;

#[derive(Debug)]
pub struct EncodeStrConstant<'a> {
    pub constant: BufferRef,
    pub output: BufferRef,
    pub codec: Codec<'a>,
}

impl<'a> VecOperator<'a> for EncodeStrConstant<'a> {
    fn execute(&mut self, _: bool, _: &mut Scratchpad<'a>) {}

    fn init(&mut self, _: usize, _: usize, _: bool, scratchpad: &mut Scratchpad<'a>) {
        let result = {
            let constant = scratchpad.get_const::<String>(self.constant);
            self.codec.encode_str(&constant)
        };
        scratchpad.set(self.output, TypedVec::constant(result));
    }

    fn inputs(&self) -> Vec<BufferRef> { vec![self.constant] }
    fn outputs(&self) -> Vec<BufferRef> { vec![self.output] }
    fn can_stream_input(&self) -> bool { true }
    fn can_stream_output(&self) -> bool { true }
    fn allocates(&self) -> bool { false }

    fn display_op(&self, _: bool) -> String {
        format!("encode({}; {:?})", self.constant, self.codec)
    }
}


#[derive(Debug)]
pub struct EncodeIntConstant<'a> {
    pub constant: BufferRef,
    pub output: BufferRef,
    pub codec: Codec<'a>,
}

impl<'a> VecOperator<'a> for EncodeIntConstant<'a> {
    fn execute(&mut self, _: bool, _: &mut Scratchpad<'a>) {}

    fn init(&mut self, _: usize, _: usize, _: bool, scratchpad: &mut Scratchpad<'a>) {
        let constant = scratchpad.get_const::<i64>(self.constant);
        let result = self.codec.encode_int(constant);
        scratchpad.set(self.output, TypedVec::constant(result));
    }

    fn inputs(&self) -> Vec<BufferRef> { vec![self.constant] }
    fn outputs(&self) -> Vec<BufferRef> { vec![self.output] }
    fn can_stream_input(&self) -> bool { true }
    fn can_stream_output(&self) -> bool { true }
    fn allocates(&self) -> bool { false }

    fn display_op(&self, _: bool) -> String {
        format!("encode({}; {:?})", self.constant, self.codec)
    }
}

