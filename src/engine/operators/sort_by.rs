use bitvec::*;
use engine::*;
use std::cmp::Ordering;

pub struct SortBy<T> {
    pub ranking: BufferRef<T>,
    pub indices: BufferRef<usize>,
    pub output: BufferRef<usize>,
    pub descending: bool,
    pub stable: bool,
}

impl<'a, T: VecData<T> + 'a> VecOperator<'a> for SortBy<T> {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError> {
        scratchpad.alias(self.indices, self.output);
        let ranking = scratchpad.get(self.ranking);
        let mut indices = scratchpad.get_mut(self.indices);
        if self.descending {
            if self.stable {
                indices.sort_by(|j, i| ranking[*i].cmp(&ranking[*j]));
            } else {
                indices.sort_unstable_by(|j, i| ranking[*i].cmp(&ranking[*j]));
            }
        } else if self.stable {
            indices.sort_by_key(|i| ranking[*i]);
        } else {
            indices.sort_unstable_by_key(|i| ranking[*i]);
        }
        Ok(())
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

pub struct SortByNullable<T> {
    pub ranking: BufferRef<Nullable<T>>,
    pub indices: BufferRef<usize>,
    pub output: BufferRef<usize>,
    pub descending: bool,
    pub stable: bool,
}

impl<'a, T: VecData<T> + 'a> VecOperator<'a> for SortByNullable<T> {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError> {
        scratchpad.alias(self.indices, self.output);
        let (ranking, ranking_present) = scratchpad.get_nullable(self.ranking);
        let present = &*ranking_present;
        let mut indices = scratchpad.get_mut(self.indices);
        if self.descending {
            if self.stable {
                indices.sort_by(|&j, &i| match (present.is_set(i), present.is_set(j)) {
                    (true, true) => ranking[i].cmp(&ranking[j]),
                    (false, true) => Ordering::Less,
                    (true, false) => Ordering::Greater,
                    (false, false) => Ordering::Equal,
                })
            } else {
                indices.sort_unstable_by(|&j, &i| match (present.is_set(i), present.is_set(j)) {
                    (true, true) => ranking[i].cmp(&ranking[j]),
                    (false, true) => Ordering::Less,
                    (true, false) => Ordering::Greater,
                    (false, false) => Ordering::Equal,
                })
            }
        } else if self.stable {
                indices.sort_by(|&i, &j| match (present.is_set(i), present.is_set(j)) {
                    (true, true) => ranking[i].cmp(&ranking[j]),
                    (false, true) => Ordering::Less,
                    (true, false) => Ordering::Greater,
                    (false, false) => Ordering::Equal,
                })
        } else {
            indices.sort_unstable_by(|&i, &j| match (present.is_set(i), present.is_set(j)) {
                (true, true) => ranking[i].cmp(&ranking[j]),
                (false, true) => Ordering::Less,
                (true, false) => Ordering::Greater,
                (false, false) => Ordering::Equal,
            })
        }
        Ok(())
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
