use std::str;

use engine::vector_op::vector_operator::*;


#[derive(Debug)]
pub struct SliceUnpackString {
    pub input: BufferRef,
    pub output: BufferRef,
    pub stride: usize,
    pub offset: usize,
}

impl<'a> VecOperator<'a> for SliceUnpackString {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) {
        let packed_any = scratchpad.get_any(self.input);
        let packed = packed_any.cast_ref_byte_slices();
        let mut unpacked = scratchpad.get_mut::<&'a str>(self.output);
        for datum in packed.data.iter().skip(self.offset).step_by(self.stride) {
            unpacked.push(unsafe { str::from_utf8_unchecked(datum) });
        }
    }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.output, Box::new(Vec::<&'a str>::with_capacity(batch_size)));
    }

    fn inputs(&self) -> Vec<BufferRef> { vec![self.input] }
    fn outputs(&self) -> Vec<BufferRef> { vec![self.output] }
    fn can_stream_input(&self, _: BufferRef) -> bool { true }
    fn can_stream_output(&self, _: BufferRef) -> bool { true }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("{}[{}, {}, ...] = {}", self.output, self.offset, self.offset + self.stride, self.input)
    }
    fn display_output(&self) -> bool { false }
}
