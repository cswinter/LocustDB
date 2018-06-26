use std::marker::PhantomData;

use engine::typed_vec::MergeOp;
use engine::vector_op::*;
use engine::*;


#[derive(Debug)]
pub struct MergeDrop<T> {
    pub merge_ops: BufferRef,
    pub left: BufferRef,
    pub right: BufferRef,
    pub deduplicated: BufferRef,
    pub t: PhantomData<T>,
}

impl<'a, T: VecType<T> + 'a> VecOperator<'a> for MergeDrop<T> {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) {
        let deduplicated = {
            let ops = scratchpad.get::<MergeOp>(self.merge_ops);
            let left = scratchpad.get::<T>(self.left);
            let right = scratchpad.get::<T>(self.right);
            merge_drop(&ops, &left, &right)
        };
        scratchpad.set(self.deduplicated, deduplicated);
    }

    fn inputs(&self) -> Vec<BufferRef> { vec![self.merge_ops, self.left, self.right] }
    fn outputs(&self) -> Vec<BufferRef> { vec![self.deduplicated] }
    fn can_stream_input(&self) -> bool { false }
    fn can_stream_output(&self) -> bool { false }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("merge_drop({}, {}, {})", self.merge_ops, self.left, self.right)
    }
}

fn merge_drop<'a, T: VecType<T> + 'a>(ops: &[MergeOp], left: &[T], right: &[T]) -> BoxedVec<'a> {
    // TODO(clemens): this is an overestimate
    let mut result = Vec::with_capacity(ops.len());
    let mut i = 0;
    let mut j = 0;
    for op in ops {
        match *op {
            MergeOp::TakeLeft => {
                result.push(left[i]);
                i += 1;
            }
            MergeOp::TakeRight => {
                result.push(right[j]);
                j += 1;
            }
            MergeOp::MergeRight => {
                j += 1;
            }
        }
    }
    TypedVec::owned(result)
}

