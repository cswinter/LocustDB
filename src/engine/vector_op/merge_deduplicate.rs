use std::cmp::{max, min};
use std::marker::PhantomData;

use engine::typed_vec::MergeOp;
use engine::vector_op::*;
use engine::*;


#[derive(Debug)]
pub struct MergeDeduplicate<T> {
    pub left: BufferRef,
    pub right: BufferRef,
    pub deduplicated: BufferRef,
    pub merge_ops: BufferRef,
    pub t: PhantomData<T>,
}

impl<'a, T: GenericVec<T> + 'a> VecOperator<'a> for MergeDeduplicate<T> {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) {
        let (deduplicated, merge_ops) = {
            let left = scratchpad.get::<T>(self.left);
            let right = scratchpad.get::<T>(self.right);
            merge_deduplicate(&left, &right)
        };
        scratchpad.set(self.deduplicated, deduplicated);
        scratchpad.set(self.merge_ops, Box::new(merge_ops));
    }

    fn inputs(&self) -> Vec<BufferRef> { vec![self.left, self.right] }
    fn outputs(&self) -> Vec<BufferRef> { vec![self.deduplicated, self.merge_ops] }
    fn can_stream_input(&self) -> bool { false }
    fn can_stream_output(&self, _: BufferRef) -> bool { false }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("merge_deduplicate({}, {})", self.left, self.right)
    }
}

fn merge_deduplicate<'a, T: GenericVec<T> + 'a>(left: &[T], right: &[T]) -> (BoxedVec<'a>, Vec<MergeOp>) {
    // TODO(clemens): figure out maths for precise estimate + variance derived from how much grouping reduced cardinality
    let output_len_estimate = max(left.len(), right.len()) + min(left.len(), right.len()) / 2;
    let mut result = Vec::with_capacity(output_len_estimate);
    let mut ops = Vec::<MergeOp>::with_capacity(output_len_estimate);

    let mut i = 0;
    let mut j = 0;
    while i < left.len() && j < right.len() {
        if result.last() == Some(&right[j]) {
            ops.push(MergeOp::MergeRight);
            j += 1;
        } else if left[i] <= right[j] {
            result.push(left[i]);
            ops.push(MergeOp::TakeLeft);
            i += 1;
        } else {
            result.push(right[j]);
            ops.push(MergeOp::TakeRight);
            j += 1;
        }
    }

    for x in left[i..].iter() {
        result.push(*x);
        ops.push(MergeOp::TakeLeft);
    }
    if j < right.len() && result.last() == Some(&right[j]) {
        ops.push(MergeOp::MergeRight);
        j += 1;
    }
    for x in right[j..].iter() {
        result.push(*x);
        ops.push(MergeOp::TakeRight);
    }

    (AnyVec::owned(result), ops)
}


