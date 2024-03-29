use crate::engine::*;
use std::fmt;
use std::mem;

pub struct ConstantVec<'a> {
    pub val: BoxedData<'a>,
    pub output: BufferRef<Any>,
}

impl<'a> VecOperator<'a> for ConstantVec<'a> {
    fn execute(&mut self, _: bool, _: &mut Scratchpad<'a>) -> Result<(), QueryError> {
        Ok(())
    }

    fn init(&mut self, _: usize, _: usize, scratchpad: &mut Scratchpad<'a>) {
        let owned = mem::replace(&mut self.val, empty_data(0));
        scratchpad.set_any(self.output, owned);
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> {
        vec![]
    }
    fn inputs_mut(&mut self) -> Vec<&mut usize> { vec![] }
    fn outputs(&self) -> Vec<BufferRef<Any>> {
        vec![self.output]
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
        "ConstantVec".to_string()
    }
}

impl<'a> fmt::Debug for ConstantVec<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_op(false))
    }
}
