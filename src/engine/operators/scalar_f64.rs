use ordered_float::OrderedFloat;

use crate::engine::*;

#[derive(Debug)]
pub struct ScalarF64 {
    pub val: OrderedFloat<f64>,
    pub hide_value: bool,
    pub output: BufferRef<Scalar<OrderedFloat<f64>>>,
}

impl<'a> VecOperator<'a> for ScalarF64 {
    fn execute(&mut self, _: bool, _: &mut Scratchpad<'a>) -> Result<(), QueryError> { Ok(()) }

    fn init(&mut self, _: usize, _: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set_const(self.output, self.val);
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![] }
    fn inputs_mut(&mut self) -> Vec<&mut usize> { vec![] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.output.any()] }
    fn can_stream_input(&self, _: usize) -> bool { false }
    fn can_stream_output(&self, _: usize) -> bool { true }
    fn allocates(&self) -> bool { false }

    fn display_op(&self, alternate: bool) -> String {
        if self.hide_value && !alternate {
            "ScalarF64".to_string()
        } else {
            format!("{}", &self.val)
        }
    }
}
