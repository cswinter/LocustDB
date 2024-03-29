use crate::engine::*;

#[derive(Debug)]
pub struct MergeDrop<T> {
    pub merge_ops: BufferRef<MergeOp>,
    pub left: BufferRef<T>,
    pub right: BufferRef<T>,
    pub deduplicated: BufferRef<T>,
}

impl<'a, T: VecData<T> + 'a> VecOperator<'a> for MergeDrop<T> {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError> {
        let deduplicated = {
            let ops = scratchpad.get(self.merge_ops);
            let left = scratchpad.get(self.left);
            let right = scratchpad.get(self.right);
            merge_drop(&ops, &left, &right)
        };
        scratchpad.set(self.deduplicated, deduplicated);
        Ok(())
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.merge_ops.any(), self.left.any(), self.right.any()] }
    fn inputs_mut(&mut self) -> Vec<&mut usize> { vec![&mut self.merge_ops.i, &mut self.left.i, &mut self.right.i] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.deduplicated.any()] }
    fn can_stream_input(&self, _: usize) -> bool { false }
    fn can_stream_output(&self, _: usize) -> bool { false }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("merge_drop({}, {}, {})", self.merge_ops, self.left, self.right)
    }
}

fn merge_drop<'a, T: VecData<T> + 'a>(ops: &[MergeOp], left: &[T], right: &[T]) -> Vec<T> {
    // This is an overestimate
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
    result
}

