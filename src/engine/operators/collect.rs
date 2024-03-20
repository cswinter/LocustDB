use crate::engine::*;

pub struct Collect {
    pub input: BufferRef<Any>,
    pub output: BufferRef<Any>,
    pub name: String,
}


impl<'a> VecOperator<'a> for Collect {
    fn execute(&mut self, _: bool, _scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError> { Ok(()) }

    fn init(&mut self, _: usize, _: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.alias(self.input, self.output);
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.input] }
    fn inputs_mut(&mut self) -> Vec<&mut usize> { vec![&mut self.input.i] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.output] }
    fn can_stream_input(&self, _: usize) -> bool { false }
    fn can_stream_output(&self, _: usize) -> bool { false }
    fn allocates(&self) -> bool { false }
    fn display_op(&self, _: bool) -> String { format!("collect(\"{}\", {})", self.name, self.input) }
}
