use std::cmp::{max, min};
use std::marker::PhantomData;

use engine::typed_vec::{MergeOp, Premerge};
use engine::vector_op::*;
use engine::*;


#[derive(Debug)]
pub struct MergeDeduplicatePartitioned<T> {
    pub partitioning: BufferRef,
    pub left: BufferRef,
    pub right: BufferRef,
    pub deduplicated: BufferRef,
    pub merge_ops: BufferRef,
    pub t: PhantomData<T>,
}

impl<'a, T: GenericVec<T> + 'a> VecOperator<'a> for MergeDeduplicatePartitioned<T> {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) {
        let (deduplicated, merge_ops) = {
            let partitioning = scratchpad.get::<Premerge>(self.partitioning);
            let left = scratchpad.get::<T>(self.left);
            let right = scratchpad.get::<T>(self.right);
            merge_deduplicate_partitioned(&partitioning, &left, &right)
        };
        scratchpad.set(self.deduplicated, deduplicated);
        scratchpad.set(self.merge_ops, Box::new(merge_ops));
    }

    fn inputs(&self) -> Vec<BufferRef> { vec![self.partitioning, self.left, self.right] }
    fn outputs(&self) -> Vec<BufferRef> { vec![self.deduplicated, self.merge_ops] }
    fn can_stream_input(&self, _: BufferRef) -> bool { false }
    fn can_stream_output(&self, _: BufferRef) -> bool { false }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("merge_deduplicate_partitioned({}, {}, {})", self.partitioning, self.left, self.right)
    }
}

pub fn merge_deduplicate_partitioned<'a, T: GenericVec<T> + 'a>(partitioning: &[Premerge],
                                                                left: &[T],
                                                                right: &[T]) -> (BoxedVec<'a>, Vec<MergeOp>) {
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
    (AnyVec::owned(result), ops)
}

