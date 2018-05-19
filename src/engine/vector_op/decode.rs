use engine::vector_op::vector_operator::*;
use mem_store::*;


#[derive(Debug)]
pub struct Decode<'a> {
    pub input: BufferRef,
    pub output: BufferRef,
    pub codec: Codec<'a>,
}

impl<'a> VecOperator<'a> for Decode<'a> {
    fn execute(&mut self, scratchpad: &mut Scratchpad<'a>) {
        let result = {
            let encoded = scratchpad.get_any(self.input);
            self.codec.unwrap_decode(&*encoded)
        };
        scratchpad.set(self.output, result);
    }
}

