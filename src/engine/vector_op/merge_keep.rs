use engine::vector_op::*;
use engine::*;


#[derive(Debug)]
pub struct MergeKeep<T> {
    pub merge_ops: BufferRef<u8>,
    pub left: BufferRef<T>,
    pub right: BufferRef<T>,
    pub merged: BufferRef<T>,
}

impl<'a, T: GenericVec<T> + 'a> VecOperator<'a> for MergeKeep<T> {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) {
        let merged = {
            let ops = scratchpad.get::<u8>(self.merge_ops);
            let left = scratchpad.get::<T>(self.left);
            let right = scratchpad.get::<T>(self.right);
            merge_keep(&ops, &left, &right)
        };
        scratchpad.set(self.merged, merged);
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.merge_ops.any(), self.left.any(), self.right.any()] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.merged.any()] }
    fn can_stream_input(&self, _: usize) -> bool { false }
    fn can_stream_output(&self, _: usize) -> bool { false }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("merge_keep({}, {}, {})", self.merge_ops, self.left, self.right)
    }
}

fn merge_keep<'a, T: Copy + 'a>(ops: &[u8], left: &[T], right: &[T]) -> Vec<T> {
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
    result
}

