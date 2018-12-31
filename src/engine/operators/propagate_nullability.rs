use engine::*;

#[derive(Debug)]
pub struct PropagateNullability<T> {
    pub from: BufferRef<Nullable<Any>>,
    pub to: BufferRef<T>,
    pub output: BufferRef<Nullable<T>>,
}

impl<'a, T: VecData<T>> VecOperator<'a> for PropagateNullability<T> {
    fn execute(&mut self, _streaming: bool, _scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError> { Ok(()) }

    fn init(&mut self, _: usize, _: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.reassemble_nullable(self.from, self.to, self.output);
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.from.any(), self.to.any()] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.output.any()] }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, _: usize) -> bool { true }
    fn allocates(&self) -> bool { true }
    fn display_op(&self, _: bool) -> String { format!("reassemble_nullable({}, {})", self.from, self.to) }
}

