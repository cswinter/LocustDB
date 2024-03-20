use crate::engine::*;

#[derive(Debug)]
pub struct AssembleNullable<T> {
    pub data: BufferRef<T>,
    pub present: BufferRef<u8>,
    pub nullable_data: BufferRef<Nullable<T>>,
}

impl<'a, T: VecData<T>> VecOperator<'a> for AssembleNullable<T> {
    fn execute(&mut self, _streaming: bool, _scratchpad: &mut Scratchpad<'a>)
        -> Result<(), QueryError> {
        Ok(())
    }

    fn init(&mut self, _: usize, _: usize, scratchpad: &mut Scratchpad<'a>) {
        // This works even when streaming since it just creates an nullable_data->data alias and sets the null map of nullable_data to present.
        // It would incorrect to perform this operation in the `execute` function since otherwise it would results in incorrect ordering with potential `PropagateNullability` operations.
        scratchpad.assemble_nullable(self.data, self.present, self.nullable_data);
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.data.any(), self.present.any()] }
    fn inputs_mut(&mut self) -> Vec<&mut usize> { vec![&mut self.data.i, &mut self.present.i] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.nullable_data.any()] }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, _: usize) -> bool { true }
    fn allocates(&self) -> bool { false }
    fn display_op(&self, _: bool) -> String { format!("nullable({}, {})", self.data, self.present) }
}
