use engine::*;

#[derive(Debug)]
pub struct AssembleNullable<T> {
    pub data: BufferRef<T>,
    pub present: BufferRef<u8>,
    pub nullable_data: BufferRef<Nullable<T>>,
}

impl<'a, T: GenericIntVec<T>> VecOperator<'a> for AssembleNullable<T> {
    fn execute(&mut self, _streaming: bool, _scratchpad: &mut Scratchpad<'a>) {}

    fn init(&mut self, _: usize, _: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.assemble_nullable(self.data, self.present, self.nullable_data);
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.data.any(), self.present.any()] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.nullable_data.any()] }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, _: usize) -> bool { true }
    fn allocates(&self) -> bool { true }
    fn display_op(&self, _: bool) -> String { format!("nullable({}, {})", self.data, self.present) }
}

