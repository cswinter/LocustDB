use engine::*;


#[derive(Debug)]
pub struct SortUnstableBy<T> {
    pub ranking: BufferRef<T>,
    pub output: BufferRef<usize>,
    pub descending: bool,
}

impl<'a, T: GenericVec<T> + 'a> VecOperator<'a> for SortUnstableBy<T> {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) {
        let result = {
            let ranking = scratchpad.get(self.ranking);
            let mut result = (0..ranking.len()).collect::<Vec<usize>>();
            if self.descending {
                result.sort_unstable_by(|i, j| ranking[*i].cmp(&ranking[*j]).reverse());
            } else {
                result.sort_unstable_by_key(|i| ranking[*i]);
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
