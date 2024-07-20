use crate::bitvec::*;
use crate::engine::*;
use std::fmt::Debug;
use std::cmp::Ordering;
use std::marker::PhantomData;

pub struct SortBy<T, C> {
    pub ranking: BufferRef<T>,
    pub indices: BufferRef<usize>,
    pub output: BufferRef<usize>,
    pub stable: bool,
    pub c: PhantomData<C>,
}

impl<'a, T: VecData<T> + 'a, C: Comparator<T> + Debug> VecOperator<'a> for SortBy<T, C> {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError> {
        scratchpad.alias(self.indices, self.output);
        let ranking = scratchpad.get(self.ranking);
        let mut indices = scratchpad.get_mut(self.indices);
        if self.stable {
            indices.sort_by(|i, j| C::ordering(ranking[*i], ranking[*j]));
        } else {
            indices.sort_unstable_by(|i, j| C::ordering(ranking[*i], ranking[*j]));
        }
        Ok(())
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.ranking.any(), self.indices.any()] }
    fn inputs_mut(&mut self) -> Vec<&mut usize> { vec![&mut self.ranking.i, &mut self.indices.i] } 
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.output.any()] }
    fn can_stream_input(&self, _: usize) -> bool { false }
    fn can_stream_output(&self, _: usize) -> bool { false }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("sort_by({}, {}; cmp={:?}, stable={})", self.ranking, self.indices, self.c, self.stable)
    }
}

pub struct SortByNullable<T, C> {
    pub ranking: BufferRef<Nullable<T>>,
    pub indices: BufferRef<usize>,
    pub output: BufferRef<usize>,
    pub stable: bool,
    pub c: PhantomData<C>,
}

impl<'a, T: VecData<T> + 'a, C: Comparator<T> + Debug> VecOperator<'a> for SortByNullable<T, C> {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError> {
        scratchpad.alias(self.indices, self.output);
        let (ranking, ranking_present) = scratchpad.get_nullable(self.ranking);
        let present = &*ranking_present;
        let mut indices = scratchpad.get_mut(self.indices);
        if self.stable {
                indices.sort_by(|&i, &j| match (present.is_set(i), present.is_set(j)) {
                    (true, true) => C::ordering(ranking[i], ranking[j]),
                    (false, true) => if C::is_less_than() { Ordering::Greater } else { Ordering::Less },
                    (true, false) => if C::is_less_than() { Ordering::Less } else { Ordering::Greater },
                    (false, false) => Ordering::Equal,
                })
        } else {
            indices.sort_unstable_by(|&i, &j| match (present.is_set(i), present.is_set(j)) {
                (true, true) => C::ordering(ranking[i], ranking[j]),
                (false, true) => if C::is_less_than() { Ordering::Greater } else { Ordering::Less },
                (true, false) => if C::is_less_than() { Ordering::Less } else { Ordering::Greater },
                (false, false) => Ordering::Equal,
            })
        }
        Ok(())
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.ranking.any(), self.indices.any()] }
    fn inputs_mut(&mut self) -> Vec<&mut usize> { vec![&mut self.ranking.i, &mut self.indices.i] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.output.any()] }
    fn can_stream_input(&self, _: usize) -> bool { false }
    fn can_stream_output(&self, _: usize) -> bool { false }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("sort_by_nullable({}, {}; cmp={:?}, stable={})", self.ranking, self.indices, self.c, self.stable)
    }
}
