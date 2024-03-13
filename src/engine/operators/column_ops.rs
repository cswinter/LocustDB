use crate::engine::*;

#[derive(Debug)]
pub struct ReadColumnData {
    pub colname: String,
    pub section_index: usize,
    pub output: BufferRef<Any>,
    pub is_bitvec: bool,

    pub current_index: usize,
    pub batch_size: usize,
    pub has_more: bool,
}

impl<'a> VecOperator<'a> for ReadColumnData {
    fn execute(
        &mut self,
        streaming: bool,
        scratchpad: &mut Scratchpad<'a>,
    ) -> Result<(), QueryError> {
        let data_section = scratchpad.get_column_data(&self.colname, self.section_index);
        if self.is_bitvec {
            // Basic sanity check, this will panic if the data is not u8
            data_section.cast_ref_u8();
            assert!(
                self.current_index & 7 == 0,
                "Bitvec read must be aligned to byte boundary"
            );
        }
        let (from, to) = if streaming {
            if self.is_bitvec {
                (
                    (self.current_index + 7) / 8,
                    (self.current_index + self.batch_size + 7) / 8,
                )
            } else {
                (self.current_index, self.current_index + self.batch_size)
            }
        } else {
            (0, data_section.len())
        };
        let result = data_section.slice_box(from, to);
        scratchpad.set_any(self.output, result);
        self.current_index += self.batch_size;
        self.has_more = to < data_section.len();
        Ok(())
    }

    fn init(&mut self, _: usize, batch_size: usize, _: &mut Scratchpad<'a>) {
        self.batch_size = batch_size;
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> {
        vec![]
    }
    fn outputs(&self) -> Vec<BufferRef<Any>> {
        vec![self.output]
    }
    fn can_stream_input(&self, _: usize) -> bool {
        false
    }
    fn can_stream_output(&self, _: usize) -> bool {
        true
    }
    fn allocates(&self) -> bool {
        false
    }
    fn is_streaming_producer(&self) -> bool {
        true
    }
    fn has_more(&self) -> bool {
        self.has_more
    }

    fn display_op(&self, _: bool) -> String {
        format!("{:?}.{}", self.colname, self.section_index)
    }
}
