use engine::vector_op::vector_operator::*;

#[derive(Debug)]
pub struct ReadColumnData {
    pub colname: String,
    pub section_index: usize,
    pub output: BufferRef<Any>,

    pub current_index: usize,
    pub batch_size: usize,
    pub has_more: bool,
}

impl<'a> VecOperator<'a> for ReadColumnData {
    fn execute(&mut self, streaming: bool, scratchpad: &mut Scratchpad<'a>) {
        let data_section = scratchpad.get_column_data(&self.colname, self.section_index);
        let end = if streaming { self.current_index + self.batch_size } else { data_section.len() };
        let result = data_section.slice_box(self.current_index, end);
        self.current_index += self.batch_size;
        scratchpad.set_any(self.output, result);
        self.has_more = end < data_section.len();
    }

    fn init(&mut self, _: usize, batch_size: usize, _: &mut Scratchpad<'a>) {
        self.batch_size = batch_size;
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.output] }
    fn can_stream_input(&self, _: usize) -> bool { false }
    fn can_stream_output(&self, _: usize) -> bool { true }
    fn allocates(&self) -> bool { false }
    fn is_streaming_producer(&self) -> bool { true }
    fn has_more(&self) -> bool { self.has_more }

    fn display_op(&self, _: bool) -> String {
        format!("{:?}.{}", self.colname, self.section_index)
    }
}
