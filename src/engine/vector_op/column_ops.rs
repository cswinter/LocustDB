use engine::vector_op::vector_operator::*;
use mem_store::*;


#[derive(Debug)]
pub struct GetDecode<'a> {
    pub output: BufferRef,
    pub col: &'a Column
}

impl<'a> VecOperator<'a> for GetDecode<'a> {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.output, self.col.decode());
    }

    fn inputs(&self) -> Vec<BufferRef> { vec![] }
    fn outputs(&self) -> Vec<BufferRef> { vec![self.output] }
    fn can_stream_input(&self) -> bool { false }
    // TODO(clemens): Make streaming possible
    fn can_stream_output(&self) -> bool { false }
    fn allocates(&self) -> bool { true }
}

#[derive(Debug)]
pub struct GetEncoded<'a> {
    pub col: &'a Column,
    pub output: BufferRef,
    pub current_index: usize,
    pub batch_size: usize,
}

impl<'a> VecOperator<'a> for GetEncoded<'a> {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) {
        let result = self.col.get_encoded(self.current_index, self.current_index + self.batch_size);
        self.current_index += self.batch_size;
        scratchpad.set(self.output, result.unwrap());
    }

    fn init(&mut self, _: usize, batch_size: usize, _: bool, _: &mut Scratchpad<'a>) {
        self.batch_size = batch_size;
    }

    fn inputs(&self) -> Vec<BufferRef> { vec![] }
    fn outputs(&self) -> Vec<BufferRef> { vec![self.output] }
    fn can_stream_input(&self) -> bool { false }
    fn can_stream_output(&self) -> bool { true }
    fn allocates(&self) -> bool { false }

    fn display_op(&self) -> Option<String> {
        Some(format!("{:?}", self.col))
    }
}
