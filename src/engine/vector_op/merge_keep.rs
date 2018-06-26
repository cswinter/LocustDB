use std::marker::PhantomData;

use engine::vector_op::*;
use engine::*;


#[derive(Debug)]
pub struct MergeKeep<T> {
    pub merge_ops: BufferRef,
    pub left: BufferRef,
    pub right: BufferRef,
    pub merged: BufferRef,
    pub t: PhantomData<T>,
}

impl<'a, T: VecType<T> + 'a> VecOperator<'a> for MergeKeep<T> {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) {
        let merged = {
            let ops = scratchpad.get::<u8>(self.merge_ops);
            let left = scratchpad.get::<T>(self.left);
            let right = scratchpad.get::<T>(self.right);
            merge_keep(&ops, &left, &right)
        };
        scratchpad.set(self.merged, merged);
    }

    fn inputs(&self) -> Vec<BufferRef> { vec![self.merge_ops, self.left, self.right] }
    fn outputs(&self) -> Vec<BufferRef> { vec![self.merged] }
    fn can_stream_input(&self) -> bool { false }
    fn can_stream_output(&self) -> bool { false }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("merge_keep({}, {}, {})", self.merge_ops, self.left, self.right)
    }
}

fn merge_keep<'a, T: 'a>(ops: &[u8], left: &[T], right: &[T]) -> BoxedVec<'a>
    where T: VecType<T> {
    let mut result = Vec::with_capacity(ops.len());
    let mut i = 0;
    let mut j = 0;
    for take_left in ops {
        if *take_left == 1 {
            result.push(left[i]);
            i += 1;
        } else {
            result.push(right[j]);
            j += 1;
        }
    }
    TypedVec::owned(result)
}

