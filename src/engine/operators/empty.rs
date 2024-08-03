use crate::engine::*;
use std::fmt;

pub struct Empty<T> {
    pub output: BufferRef<T>,
}

impl<'a, T> VecOperator<'a> for Empty<T> where T: VecData<T> + 'a {
    fn execute(&mut self, _: bool, _: &mut Scratchpad<'a>) -> Result<(), QueryError> {
        Ok(())
    }

    fn init(&mut self, _: usize, _: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.output, vec![]);
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> {
        vec![]
    }
    fn inputs_mut(&mut self) -> Vec<&mut usize> { vec![] }
    fn outputs(&self) -> Vec<BufferRef<Any>> {
        vec![self.output.any()]
    }
    fn can_stream_input(&self, _: usize) -> bool {
        false
    }
    fn can_stream_output(&self, _: usize) -> bool {
        false
    }
    fn allocates(&self) -> bool {
        false
    }

    fn display_op(&self, _: bool) -> String {
        "Empty".to_string()
    }
}

impl<T> fmt::Debug for Empty<T> where T: VecData<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_op(false))
    }
}
