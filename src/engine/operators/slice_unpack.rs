use engine::*;
use std::str;

#[derive(Debug)]
pub struct SliceUnpackInt<T> {
    pub input: BufferRef<Any>,
    pub output: BufferRef<T>,
    pub stride: usize,
    pub offset: usize,
}

impl<'a, T: GenericIntVec<T>> VecOperator<'a> for SliceUnpackInt<T> {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError> {
        let packed_any = scratchpad.get_any(self.input);
        let packed = packed_any.cast_ref_byte_slices();
        let mut unpacked = scratchpad.get_mut(self.output);
        for datum in packed.data.iter().skip(self.offset).step_by(self.stride) {
            unpacked.push(T::from_bytes(datum));
        }
        Ok(())
    }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.output, Vec::with_capacity(batch_size));
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.input] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.output.any()] }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, _: usize) -> bool { true }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("{}[{}, {}, ...] = {}", self.output, self.offset, self.offset + self.stride, self.input)
    }
    fn display_output(&self) -> bool { false }
}

#[derive(Debug)]
pub struct SliceUnpackString<'a> {
    pub input: BufferRef<Any>,
    pub output: BufferRef<&'a str>,
    pub stride: usize,
    pub offset: usize,
}

impl<'a> VecOperator<'a> for SliceUnpackString<'a> {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError> {
        let packed_any = scratchpad.get_any(self.input);
        let packed = packed_any.cast_ref_byte_slices();
        let mut unpacked = scratchpad.get_mut(self.output);
        for datum in packed.data.iter().skip(self.offset).step_by(self.stride) {
            unpacked.push(unsafe { str::from_utf8_unchecked(datum) });
        }
        Ok(())
    }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.output, Vec::with_capacity(batch_size));
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.input] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.output.any()] }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, _: usize) -> bool { true }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("{}[{}, {}, ...] = {}", self.output, self.offset, self.offset + self.stride, self.input)
    }
    fn display_output(&self) -> bool { false }
}
