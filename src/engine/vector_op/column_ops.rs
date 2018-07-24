use engine::vector_op::vector_operator::*;

#[derive(Debug)]
pub struct ReadColumnData {
    pub colname: String,
    pub section_index: usize,
    pub output: BufferRef,

    pub current_index: usize,
    pub batch_size: usize,
}

impl<'a> VecOperator<'a> for ReadColumnData {
    fn execute(&mut self, streaming: bool, scratchpad: &mut Scratchpad<'a>) {
        let data_section = scratchpad.get_column_data(&self.colname, self.section_index);
        let end = if streaming { self.current_index + self.batch_size } else { data_section.len() };
        let result = data_section.slice_box(self.current_index, end);
        self.current_index += self.batch_size;
        scratchpad.set(self.output, result);
    }

    fn init(&mut self, _: usize, batch_size: usize, _: bool, _: &mut Scratchpad<'a>) {
        self.batch_size = batch_size;
    }

    fn inputs(&self) -> Vec<BufferRef> { vec![] }
    fn outputs(&self) -> Vec<BufferRef> { vec![self.output] }
    fn can_stream_input(&self, _: BufferRef) -> bool { false }
    fn can_stream_output(&self, _: BufferRef) -> bool { true }
    fn allocates(&self) -> bool { false }

    fn display_op(&self, _: bool) -> String {
        format!("{:?}.{}", self.colname, self.section_index)
    }
}
