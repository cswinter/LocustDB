use crate::engine::*;

#[derive(Debug)]
pub struct ReadColumnData {
    pub colname: String,
    pub section_index: usize,
    pub output: BufferRef<Any>,
    pub tag: EncodingType,
}

impl<'a> VecOperator<'a> for ReadColumnData {
    fn execute(
        &mut self,
        _: bool,
        scratchpad: &mut Scratchpad<'a>,
    ) -> Result<(), QueryError> {
        let data_section = scratchpad.get_column_data(&self.colname, self.section_index);
        let result = data_section.slice_box(0, data_section.len());
        scratchpad.set_any(self.output, result);
        Ok(())
    }

    fn init(&mut self, _: usize, _: usize, _: &mut Scratchpad<'a>) {}
    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![] }
    fn inputs_mut(&mut self) -> Vec<&mut usize> { vec![] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.output] }
    fn can_stream_input(&self, _: usize) -> bool { false }
    fn can_stream_output(&self, _: usize) -> bool { false }
    fn allocates(&self) -> bool { false }
    fn display_op(&self, _: bool) -> String { format!("{:?}.{}: {:?}", self.colname, self.section_index, self.tag) }
}
