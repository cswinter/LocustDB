use crate::engine::*;
use std::cmp::{max, min};

#[derive(Debug)]
pub struct MergeDeduplicatePartitioned<T> {
    pub partitioning: BufferRef<Premerge>,
    pub left: BufferRef<T>,
    pub right: BufferRef<T>,
    pub deduplicated: BufferRef<T>,
    pub merge_ops: BufferRef<MergeOp>,
}

impl<'a, T: VecData<T> + 'a> VecOperator<'a> for MergeDeduplicatePartitioned<T> {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError> {
        let (deduplicated, merge_ops) = {
            let partitioning = scratchpad.get(self.partitioning);
            let left = scratchpad.get(self.left);
            let right = scratchpad.get(self.right);
            merge_deduplicate_partitioned(&partitioning, &left, &right)
        };
        scratchpad.set(self.deduplicated, deduplicated);
        scratchpad.set(self.merge_ops, merge_ops);
        Ok(())
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.partitioning.any(), self.left.any(), self.right.any()] }
    fn inputs_mut(&mut self) -> Vec<&mut usize> { vec![&mut self.partitioning.i, &mut self.left.i, &mut self.right.i] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.deduplicated.any(), self.merge_ops.any()] }
    fn can_stream_input(&self, _: usize) -> bool { false }
    fn can_stream_output(&self, _: usize) -> bool { false }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("merge_deduplicate_partitioned({}, {}, {})", self.partitioning, self.left, self.right)
    }
}

pub fn merge_deduplicate_partitioned<'a, T: PartialOrd + Copy + 'a>(partitioning: &[Premerge],
                                                                    left: &[T],
                                                                    right: &[T]) -> (Vec<T>, Vec<MergeOp>) {
    let output_len_estimate = max(left.len(), right.len()) + min(left.len(), right.len()) / 2;
    let mut result = Vec::with_capacity(output_len_estimate);
    let mut ops = Vec::<MergeOp>::with_capacity(output_len_estimate);

    let mut i = 0;
    let mut j = 0;
    for group in partitioning {
        let mut last = None;
        let i_max = i + group.left as usize;
        let j_max = j + group.right as usize;
        // println!("i_max = {}, j_max = {}", i_max, j_max);
        for _ in 0..(group.left + group.right) {
            // println!("i = {}, j = {}, last = {:?}", i, j, last);
            // println!("{:?} {:?}", left.get(i), right.get(j));
            if j < j_max && last == Some(right[j]) {
                ops.push(MergeOp::MergeRight);
                j += 1;
            } else if i < i_max && (j >= j_max || left[i] <= right[j]) {
                result.push(left[i]);
                ops.push(MergeOp::TakeLeft);
                last = Some(left[i]);
                i += 1;
            } else {
                result.push(right[j]);
                ops.push(MergeOp::TakeRight);
                last = Some(right[j]);
                j += 1;
            }
            // println!("{:?}", ops.last().unwrap());
        }
    }
    (result, ops)
}

