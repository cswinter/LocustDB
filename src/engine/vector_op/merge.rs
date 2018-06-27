use std::cmp::min;
use std::fmt::Debug;
use std::marker::PhantomData;

use engine::*;
use engine::vector_op::*;
use engine::vector_op::comparator::*;


#[derive(Debug)]
pub struct Merge<T, C: Debug> {
    pub left: BufferRef,
    pub right: BufferRef,
    pub merged: BufferRef,
    pub merge_ops: BufferRef,
    pub limit: usize,
    pub t: PhantomData<T>,
    pub c: PhantomData<C>,
}

impl<'a, T: VecType<T> + 'a, C: Comparator<T> + Debug> VecOperator<'a> for Merge<T, C> {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) {
        let (merged, ops) = {
            let left = scratchpad.get::<T>(self.left);
            let right = scratchpad.get::<T>(self.right);
            merge::<_, C>(&left, &right, self.limit)
        };
        scratchpad.set(self.merged, merged);
        scratchpad.set(self.merge_ops, Box::new(ops));
    }

    fn inputs(&self) -> Vec<BufferRef> { vec![self.left, self.right] }
    fn outputs(&self) -> Vec<BufferRef> { vec![self.merged, self.merge_ops] }
    fn can_stream_input(&self) -> bool { false }
    fn can_stream_output(&self, _: BufferRef) -> bool { false }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("merge({}, {})", self.left, self.right)
    }
}

fn merge<'a, T: VecType<T> + 'a, C: Comparator<T>>(left: &[T], right: &[T], limit: usize) -> (BoxedVec<'a>, Vec<u8>) {
    let mut result = Vec::with_capacity(left.len() + right.len());
    let mut ops = Vec::<u8>::with_capacity(left.len() + right.len());

    let mut i = 0;
    let mut j = 0;
    while i < left.len() && j < right.len() && i + j < limit {
        if C::cmp(left[i], right[j]) {
            result.push(left[i]);
            ops.push(1);
            i += 1;
        } else {
            result.push(right[j]);
            ops.push(0);
            j += 1;
        }
    }

    for x in left[i..min(left.len(), limit - j)].iter() {
        result.push(*x);
        ops.push(1);
    }
    for x in right[j..min(right.len(), limit - i)].iter() {
        result.push(*x);
        ops.push(0);
    }

    (TypedVec::owned(result), ops)
}

