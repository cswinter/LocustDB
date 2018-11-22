use engine::*;


#[derive(Debug)]
pub struct ScalarI64 {
    pub val: i64,
    pub hide_value: bool,
    pub output: BufferRef<Scalar<i64>>,
}

impl<'a> VecOperator<'a> for ScalarI64 {
    fn execute(&mut self, _: bool, _: &mut Scratchpad<'a>) {}

    fn init(&mut self, _: usize, _: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set_const(self.output, self.val);
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.output.any()] }
    fn can_stream_input(&self, _: usize) -> bool { false }
    fn can_stream_output(&self, _: usize) -> bool { true }
    fn allocates(&self) -> bool { false }

    fn display_op(&self, alternate: bool) -> String {
        if self.hide_value && !alternate {
            format!("ScalarI64")
        } else {
            format!("{}", &self.val)
        }
    }
}
