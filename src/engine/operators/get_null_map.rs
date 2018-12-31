use engine::*;

#[derive(Debug)]
pub struct GetNullMap {
    pub from: BufferRef<Nullable<Any>>,
    pub present: BufferRef<u8>,
}

impl<'a> VecOperator<'a> for GetNullMap {
    fn execute(&mut self, _streaming: bool, _scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError> { Ok(()) }

    fn init(&mut self, _: usize, _: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.alias_null_map(self.from, self.present);
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.from.any()] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.present.any()] }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, _: usize) -> bool { true }
    fn allocates(&self) -> bool { true }
    fn display_op(&self, _: bool) -> String { format!("null_map({})", self.from) }
}

