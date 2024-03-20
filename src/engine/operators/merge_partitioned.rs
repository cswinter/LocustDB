use crate::engine::*;
use std::cmp;
use std::fmt::Debug;
use std::marker::PhantomData;

#[derive(Debug)]
pub struct MergePartitioned<T, C> {
    pub partitioning: BufferRef<Premerge>,
    pub left: BufferRef<T>,
    pub right: BufferRef<T>,
    pub merged: BufferRef<T>,
    pub take_left: BufferRef<u8>,
    pub limit: usize,
    pub c: PhantomData<C>,
}

impl<'a, T: VecData<T> + 'a, C: Comparator<T> + Debug> VecOperator<'a> for MergePartitioned<T, C> {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError> {
        let (merged, merge_ops) = {
            let partitioning = scratchpad.get(self.partitioning);
            let left = scratchpad.get(self.left);
            let right = scratchpad.get(self.right);
            merge_partitioned::<_, C>(&partitioning, &left, &right, self.limit)
        };
        scratchpad.set(self.merged, merged);
        scratchpad.set(self.take_left, merge_ops);
        Ok(())
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.partitioning.any(), self.left.any(), self.right.any()] }
    fn inputs_mut(&mut self) -> Vec<&mut usize> { vec![&mut self.partitioning.i, &mut self.left.i, &mut self.right.i] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.merged.any(), self.take_left.any()] }
    fn can_stream_input(&self, _: usize) -> bool { false }
    fn can_stream_output(&self, _: usize) -> bool { false }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("merge_partitioned({}, {}, {})", self.partitioning, self.left, self.right)
    }
}

pub fn merge_partitioned<'a, T, C>(partitioning: &[Premerge], left: &[T], right: &[T], limit: usize)
                                   -> (Vec<T>, Vec<u8>) where T: PartialOrd + Debug + Copy + 'a, C: Comparator<T> {
    let len = cmp::min(left.len() + right.len(), limit);
    let mut result = Vec::with_capacity(len);
    let mut take_left = Vec::<u8>::with_capacity(len);

    let mut i = 0;
    let mut j = 0;
    'outer: for group in partitioning {
        let i_max = i + group.left as usize;
        let j_max = j + group.right as usize;
        for _ in 0..(group.left + group.right) {
            if j == j_max || (i < i_max && C::cmp_eq(left[i], right[j])) {
                take_left.push(1);
                result.push(left[i]);
                i += 1;
            } else {
                take_left.push(0);
                result.push(right[j]);
                j += 1;
            }
            if i + j == limit {
                break 'outer;
            }
        }
    }
    (result, take_left)
}

