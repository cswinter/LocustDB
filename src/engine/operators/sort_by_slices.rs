use engine::*;


#[derive(Debug)]
pub struct SortBySlices {
    pub ranking: BufferRef<Any>,
    pub indices: BufferRef<usize>,
    pub output: BufferRef<usize>,
    pub descending: bool,
    pub stable: bool,
}

impl<'a> VecOperator<'a> for SortBySlices {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.alias(self.indices, self.output);
        let ranking_any = scratchpad.get_any(self.ranking);
        let ranking = ranking_any.cast_ref_byte_slices();
        let mut result = scratchpad.get_mut(self.indices);
        if self.descending {
            if self.stable {
                result.sort_by(|i, j| ranking.row(*i).cmp(&ranking.row(*j)).reverse());
            } else {
                result.sort_unstable_by(|i, j| ranking.row(*i).cmp(&ranking.row(*j)).reverse());
            }
        } else {
            if self.stable {
                result.sort_by_key(|i| ranking.row(*i));
            } else {
                result.sort_unstable_by_key(|i| ranking.row(*i));
            }
        }
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.ranking.any(), self.indices.any()] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.output.any()] }
    fn can_stream_input(&self, _: usize) -> bool { false }
    fn can_stream_output(&self, _: usize) -> bool { false }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("sort_by({}, {}, desc={}, stable={})", self.ranking, self.indices, self.descending, self.stable)
    }
}
