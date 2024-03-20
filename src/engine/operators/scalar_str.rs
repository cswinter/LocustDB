use crate::engine::*;

#[derive(Debug)]
pub struct ScalarStr<'a> {
    pub val: String,
    pub pinned: BufferRef<Scalar<String>>,
    pub output: BufferRef<Scalar<&'a str>>,
}

impl<'a> VecOperator<'a> for ScalarStr<'a> {
    fn execute(&mut self, _: bool, _: &mut Scratchpad<'a>) -> Result<(), QueryError> {
        Ok(())
    }

    fn init(&mut self, _: usize, _: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set_const(self.pinned, self.val.clone());
        let output = scratchpad.get_scalar_string_pinned(&self.pinned);
        scratchpad.set_const(self.output, output);
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
        true
    }
    fn allocates(&self) -> bool {
        true
    }
    fn display_op(&self, _: bool) -> String {
        format!("\"{}\"", &self.val)
    }
}
