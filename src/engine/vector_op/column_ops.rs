use engine::vector_op::vector_operator::*;
use mem_store::*;


#[derive(Debug)]
pub struct GetDecode<'a> {
    pub output: BufferRef,
    pub col: &'a Column
}

impl<'a> VecOperator<'a> for GetDecode<'a> {
    fn execute(&mut self, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.output, self.col.decode());
    }
}

#[derive(Debug)]
pub struct GetEncoded<'a> {
    pub col: &'a Column,
    pub output: BufferRef,
}

impl<'a> VecOperator<'a> for GetEncoded<'a> {
    fn execute(&mut self, scratchpad: &mut Scratchpad<'a>) {
        let result = self.col.get_encoded();
        scratchpad.set(self.output, result.unwrap());
    }
}
