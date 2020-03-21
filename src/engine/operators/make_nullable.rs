use crate::engine::*;

#[derive(Debug)]
pub struct MakeNullable<T> {
    pub data: BufferRef<T>,
    pub present: BufferRef<u8>,
    pub nullable_data: BufferRef<Nullable<T>>,
}

impl<'a, T: VecData<T> + 'a> VecOperator<'a> for MakeNullable<T> {
    fn execute(&mut self, _streaming: bool, _scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError> { Ok(()) }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        let present = vec![255u8; batch_size / 8 + 1];
        scratchpad.set(self.present, present);
        scratchpad.assemble_nullable(self.data, self.present, self.nullable_data);
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.data.any()] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.nullable_data.any(), self.present.any()] }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, _: usize) -> bool { true }
    fn allocates(&self) -> bool { true }
    fn display_op(&self, _: bool) -> String { format!("nullable({}, {})", self.data, self.present) }
}

