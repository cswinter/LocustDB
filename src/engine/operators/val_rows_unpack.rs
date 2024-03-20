use crate::engine::*;
use crate::mem_store::Val;

#[derive(Debug)]
pub struct ValRowsUnpack<'a> {
    pub input: BufferRef<ValRows<'a>>,
    pub output: BufferRef<Val<'a>>,
    pub stride: usize,
    pub offset: usize,

    pub batch_size: usize,
    pub curr_index: usize,
    pub has_more: bool,
}

impl<'a> VecOperator<'a> for ValRowsUnpack<'a> {
    fn execute(&mut self, stream: bool, scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError> {
        let packed = scratchpad.get_mut_val_rows(self.input);
        let mut unpacked = scratchpad.get_mut(self.output);
        if stream {
            self.curr_index += unpacked.len();
            unpacked.clear();
        }
        for &datum in packed.data.iter().skip(self.offset).step_by(self.stride).skip(self.curr_index).take(self.batch_size) {
            unpacked.push(datum);
        }
        self.has_more = (packed.data.len() + self.stride - self.offset - 1) / self.stride > self.curr_index;
        Ok(())
    }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        self.batch_size = batch_size;
        scratchpad.set(self.output, Vec::with_capacity(batch_size));
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.input.any()] }
    fn inputs_mut(&mut self) -> Vec<&mut usize> { vec![&mut self.input.i] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.output.any()] }
    // TODO: make sliced/streamable version of val rows? but have to make ValRowsPack streaming first
    fn can_stream_input(&self, _: usize) -> bool { false }
    fn can_stream_output(&self, _: usize) -> bool { true }
    fn allocates(&self) -> bool { true }
    fn has_more(&self) -> bool { self.has_more }

    fn display_op(&self, _: bool) -> String {
        format!("{}[{}, {}, ...] = {}", self.output, self.offset, self.offset + self.stride, self.input)
    }
    fn display_output(&self) -> bool { false }
}
