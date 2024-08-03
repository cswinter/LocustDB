use crate::engine::*;

// Take a null count and expands it into a nullable vec of the same length with arbitrary type and all values set to null
#[derive(Debug)]
pub struct NullToI64 {
    pub input: BufferRef<Any>,
    pub output: BufferRef<i64>,

    pub batch_size: usize,
}

impl<'a> VecOperator<'a> for NullToI64 {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError> {
        let len = scratchpad.get_any(self.input).len();
        if self.batch_size > len {
            let mut output = scratchpad.get_mut(self.output);
            output.truncate(len);
        }
        Ok(())
    }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        self.batch_size = batch_size;
        scratchpad.set(self.output, vec![I64_NULL; batch_size]);
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> {
        vec![self.input.any()]
    }
    fn inputs_mut(&mut self) -> Vec<&mut usize> {
        vec![&mut self.input.i]
    }
    fn outputs(&self) -> Vec<BufferRef<Any>> {
        vec![self.output.any()]
    }
    fn can_stream_input(&self, _: usize) -> bool {
        true
    }
    fn can_stream_output(&self, _: usize) -> bool {
        true
    }
    fn allocates(&self) -> bool {
        true
    }

    fn display_op(&self, _: bool) -> String {
        format!("{} expand as <i64>", self.input)
    }
}
