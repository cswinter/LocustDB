use engine::*;
use engine::vector_op::vector_operator::*;


#[derive(Debug)]
pub struct BitUnpackOperator {
    input: BufferRef,
    output: BufferRef,
    shift: u8,
    width: u8,
}

impl BitUnpackOperator {
    pub fn new(input: BufferRef, output: BufferRef, shift: u8, width: u8) -> BitUnpackOperator {
        BitUnpackOperator {
            input,
            output,
            shift,
            width,
        }
    }
}

impl<'a> VecOperator<'a> for BitUnpackOperator {
    fn execute(&mut self, stream: bool, scratchpad: &mut Scratchpad<'a>) {
        let data = scratchpad.get::<i64>(self.input);
        let mut unpacked = scratchpad.get_mut::<i64>(self.output);
        if stream { unpacked.clear(); }
        let mask = (1 << self.width) - 1;
        for d in data.iter() {
            unpacked.push((d >> self.shift) & mask);
        }
    }

    fn init(&mut self, _: usize, batch_size: usize, _: bool, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.output, TypedVec::owned(Vec::<i64>::with_capacity(batch_size)));
    }

    fn inputs(&self) -> Vec<BufferRef> { vec![self.input] }
    fn outputs(&self) -> Vec<BufferRef> { vec![self.output] }
    fn can_stream_input(&self) -> bool { true }
    fn can_stream_output(&self) -> bool { true }
    fn allocates(&self) -> bool { true }
}
