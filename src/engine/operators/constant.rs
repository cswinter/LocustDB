use engine::*;
use ingest::raw_val::RawVal;

#[derive(Debug)]
pub struct Constant {
    pub val: RawVal,
    pub hide_value: bool,
    pub output: BufferRef<RawVal>,
}

impl<'a> VecOperator<'a> for Constant {
    fn execute(&mut self, _: bool, _: &mut Scratchpad<'a>) -> Result<(), QueryError> { Ok(()) }

    fn init(&mut self, _: usize, _: usize, scratchpad: &mut Scratchpad<'a>) {
        let result = Data::constant(self.val.clone());
        scratchpad.set_any(self.output.any(), result);
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.output.any()] }
    fn can_stream_input(&self, _: usize) -> bool { false }
    fn can_stream_output(&self, _: usize) -> bool { true }
    fn allocates(&self) -> bool { false }

    fn display_op(&self, alternate: bool) -> String {
        if self.hide_value && !alternate {
            format!("Constant<{:?}>", self.val.get_type())
        } else {
            format!("{}", &self.val)
        }
    }
}
