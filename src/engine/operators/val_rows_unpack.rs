use crate::engine::*;
use crate::mem_store::Val;

#[derive(Debug)]
pub struct ValRowsUnpack<'a> {
    pub input: BufferRef<ValRows<'a>>,
    pub output: BufferRef<Val<'a>>,
    pub stride: usize,
    pub offset: usize,
}

impl<'a> VecOperator<'a> for ValRowsUnpack<'a> {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError> {
        let packed = scratchpad.get_mut_val_rows(self.input);
        let mut unpacked = scratchpad.get_mut(self.output);
        for &datum in packed.data.iter().skip(self.offset).step_by(self.stride) {
            unpacked.push(datum);
        }
        Ok(())
    }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.output, Vec::with_capacity(batch_size));
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.input.any()] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.output.any()] }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, _: usize) -> bool { true }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("{}[{}, {}, ...] = {}", self.output, self.offset, self.offset + self.stride, self.input)
    }
    fn display_output(&self) -> bool { false }
}
