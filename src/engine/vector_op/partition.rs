use std::marker::PhantomData;

use engine::typed_vec::Premerge;
use engine::vector_op::*;
use engine::*;


#[derive(Debug)]
pub struct Partition<T> {
    pub left: BufferRef,
    pub right: BufferRef,
    pub partitioning: BufferRef,
    pub limit: usize,
    pub t: PhantomData<T>,
}

impl<'a, T: GenericVec<T> + 'a> VecOperator<'a> for Partition<T> {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) {
        let premerge = {
            let left = scratchpad.get::<T>(self.left);
            let right = scratchpad.get::<T>(self.right);
            partition(&left, &right, self.limit)
        };
        scratchpad.set(self.partitioning, Box::new(premerge));
    }

    fn inputs(&self) -> Vec<BufferRef> { vec![self.left, self.right] }
    fn outputs(&self) -> Vec<BufferRef> { vec![self.partitioning] }
    fn can_stream_input(&self, _: BufferRef) -> bool { false }
    fn can_stream_output(&self, _: BufferRef) -> bool { false }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("partition({}, {})", self.left, self.right)
    }
}

pub fn partition<'a, T: GenericVec<T> + 'a>(left: &[T], right: &[T], limit: usize) -> Vec<Premerge> {
    let mut result = Vec::new();
    let mut i = 0;
    let mut j = 0;
    while i < left.len() && j < right.len() && i + j < limit {
        let mut partition = Premerge { left: 0, right: 0 };
        let elem = if left[i] <= right[j] { left[i] } else { right[j] };
        while i < left.len() && elem == left[i] {
            partition.left += 1;
            i += 1;
        }
        while j < right.len() && elem == right[j] {
            partition.right += 1;
            j += 1;
        }
        result.push(partition);
    }

    // Remaining elements on left
    while i < left.len() && i + j < limit {
        let elem = left[i];
        let i_start = i;
        while i < left.len() && elem == left[i] {
            i += 1;
        }
        result.push(Premerge { left: (i - i_start) as u32, right: 0 });
    }

    // Remaining elements on right
    while j < right.len() && i + j < limit {
        let elem = right[j];
        let j_start = j;
        while j < right.len() && elem == right[j] {
            j += 1;
        }
        result.push(Premerge { right: (j - j_start) as u32, left: 0 });
    }
    result
}

