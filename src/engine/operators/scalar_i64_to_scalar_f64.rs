use ordered_float::OrderedFloat;

use crate::engine::*;


#[derive(Debug)]
pub struct ScalarI64ToScalarF64 {
    pub input: BufferRef<Scalar<i64>>,
    pub output: BufferRef<Scalar<of64>>,
}

impl<'a> VecOperator<'a> for ScalarI64ToScalarF64 {
    fn execute(&mut self, _: bool, _: &mut Scratchpad<'a>) -> Result<(), QueryError>{ Ok(()) }

    fn init(&mut self, _: usize, _: usize, scratchpad: &mut Scratchpad<'a>) {
        let input = scratchpad.get_scalar(&self.input);
        scratchpad.set_const(self.output, OrderedFloat(input as f64));
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.input.any()] }
    fn inputs_mut(&mut self) -> Vec<&mut usize> { vec![&mut self.input.i] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.output.any()] }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, _: usize) -> bool { true }
    fn can_block_output(&self) -> bool { true }
    fn allocates(&self) -> bool { false }

    fn display_op(&self, _: bool) -> String {
        format!("{} as f64", self.input)
    }
}