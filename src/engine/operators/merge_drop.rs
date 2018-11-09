use engine::*;


#[derive(Debug)]
pub struct MergeDrop<T> {
    pub merge_ops: BufferRef<MergeOp>,
    pub left: BufferRef<T>,
    pub right: BufferRef<T>,
    pub deduplicated: BufferRef<T>,
}

impl<'a, T: GenericVec<T> + 'a> VecOperator<'a> for MergeDrop<T> {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) {
        let deduplicated = {
            let ops = scratchpad.get(self.merge_ops);
            let left = scratchpad.get(self.left);
            let right = scratchpad.get(self.right);
            merge_drop(&ops, &left, &right)
        };
        scratchpad.set(self.deduplicated, deduplicated);
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.merge_ops.any(), self.left.any(), self.right.any()] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.deduplicated.any()] }
    fn can_stream_input(&self, _: usize) -> bool { false }
    fn can_stream_output(&self, _: usize) -> bool { false }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("merge_drop({}, {}, {})", self.merge_ops, self.left, self.right)
    }
}

fn merge_drop<'a, T: GenericVec<T> + 'a>(ops: &[MergeOp], left: &[T], right: &[T]) -> Vec<T> {
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
    result
}

