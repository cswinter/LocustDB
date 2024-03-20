use crate::engine::*;

#[derive(Debug)]
pub struct Indices {
    pub input: BufferRef<Any>,
    pub indices_out: BufferRef<usize>,
}

impl<'a> VecOperator<'a> for Indices {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError> {
        let len = scratchpad.get_any(self.input).len();
        let indices = (0..len).collect::<Vec<usize>>();
        scratchpad.set(self.indices_out, indices);
        Ok(())
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.input.any()] }
    fn inputs_mut(&mut self) -> Vec<&mut usize> { vec![&mut self.input.i] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.indices_out.any()] }
    // TODO: could make streaming? (need to set streaming_producer)
    fn can_stream_input(&self, _: usize) -> bool { false }
    fn can_stream_output(&self, _: usize) -> bool { false }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("indices({})", self.input)
    }
}
