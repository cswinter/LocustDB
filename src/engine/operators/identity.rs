use crate::engine::*;

pub struct Identity {
    pub input: BufferRef<Any>,
    pub output: BufferRef<Any>,
}


impl<'a> VecOperator<'a> for Identity {
    fn execute(&mut self, _: bool, _scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError> { Ok(()) }

    fn init(&mut self, _: usize, _: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.alias(self.input, self.output);
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.input.any()] }
    fn inputs_mut(&mut self) -> Vec<&mut usize> { vec![&mut self.input.i] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.output.any()] }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, _: usize) -> bool { true }
    fn allocates(&self) -> bool { false }
    fn display_op(&self, _: bool) -> String { format!("{}", self.input) }
}
