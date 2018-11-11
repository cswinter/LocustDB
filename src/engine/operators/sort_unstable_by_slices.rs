use engine::*;


#[derive(Debug)]
pub struct SortUnstableBySlices {
    pub ranking: BufferRef<Any>,
    pub output: BufferRef<usize>,
    pub descending: bool,
}

impl<'a> VecOperator<'a> for SortUnstableBySlices {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) {
        let result = {
            let ranking_any = scratchpad.get_any(self.ranking);
            let ranking = ranking_any.cast_ref_byte_slices();
            let mut result = (0..ranking.len()).collect::<Vec<usize>>();
            if self.descending {
                result.sort_unstable_by(|i, j| ranking.row(*i).cmp(&ranking.row(*j)).reverse());
            } else {
                result.sort_unstable_by_key(|i| ranking.row(*i));
            }
            result
        };
        scratchpad.set(self.output, result);
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.ranking.any()] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.output.any()] }
    fn can_stream_input(&self, _: usize) -> bool { false }
    fn can_stream_output(&self, _: usize) -> bool { false }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("sort_indices({}; desc={})", self.ranking, self.descending)
    }
}
