use engine::*;


pub struct SortByValRows<'a> {
    pub ranking: BufferRef<ValRows<'a>>,
    pub indices: BufferRef<usize>,
    pub output: BufferRef<usize>,
    pub descending: bool,
    pub stable: bool,
}

impl<'a> VecOperator<'a> for SortByValRows<'a> {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.alias(self.indices, self.output);
        let ranking = scratchpad.get_mut_val_rows(self.ranking);
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
