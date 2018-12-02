use engine::*;


#[derive(Debug)]
pub struct SortBy<T> {
    pub ranking: BufferRef<T>,
    pub indices: BufferRef<usize>,
    pub output: BufferRef<usize>,
    pub descending: bool,
    pub stable: bool,
}

impl<'a, T: VecData<T> + 'a> VecOperator<'a> for SortBy<T> {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.alias(self.indices, self.output);
        let ranking = scratchpad.get(self.ranking);
        let mut indices = scratchpad.get_mut(self.indices);
        if self.descending {
            if self.stable {
                indices.sort_by(|i, j| ranking[*i].cmp(&ranking[*j]).reverse());
            } else {
                indices.sort_unstable_by(|i, j| ranking[*i].cmp(&ranking[*j]).reverse());
            }
        } else {
            if self.stable {
                indices.sort_by_key(|i| ranking[*i]);
            } else {
                indices.sort_unstable_by_key(|i| ranking[*i]);
            }
        }
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.ranking.any(), self.indices.any()] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.output.any()] }
    fn can_stream_input(&self, _: usize) -> bool { false }
    fn can_stream_output(&self, _: usize) -> bool { false }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("sort_by({}, {}; desc={}, stable={})", self.ranking, self.indices, self.descending, self.stable)
    }
}
