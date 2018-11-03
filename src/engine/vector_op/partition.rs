use std::cmp;
use std::u32;

use engine::typed_vec::Premerge;
use engine::vector_op::*;
use engine::*;


#[derive(Debug)]
pub struct Partition<T> {
    pub left: BufferRef<T>,
    pub right: BufferRef<T>,
    pub partitioning: BufferRef<Premerge>,
    pub limit: usize,
}

impl<'a, T: GenericVec<T> + 'a> VecOperator<'a> for Partition<T> {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) {
        let premerge = {
            let left = scratchpad.get::<T>(self.left);
            let right = scratchpad.get::<T>(self.right);
            partition(&left, &right, self.limit)
        };
        scratchpad.set(self.partitioning, premerge);
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.left.any(), self.right.any()] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.partitioning.any()] }
    fn can_stream_input(&self, _: usize) -> bool { false }
    fn can_stream_output(&self, _: usize) -> bool { false }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("partition({}, {})", self.left, self.right)
    }
}

pub fn partition<'a, T: GenericVec<T> + 'a>(left: &[T], right: &[T], limit: usize) -> Vec<Premerge> {
    let mut result = Vec::new();
    let mut i = 0;
    let mut j = 0;
    let limit = if limit > u32::MAX as usize { u32::MAX } else { limit as u32 };
    let mut min_elems = 0u32;
    while i < left.len() && j < right.len() && min_elems < limit {
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
        min_elems += cmp::max(partition.left, partition.right);
        result.push(partition);
    }

    // Remaining elements on left
    while i < left.len() && min_elems < limit {
        let elem = left[i];
        let i_start = i;
        while i < left.len() && elem == left[i] {
            i += 1;
        }
        let partition = Premerge { left: (i - i_start) as u32, right: 0 };
        min_elems += cmp::max(partition.left, partition.right);
        result.push(partition);
    }

    // Remaining elements on right
    while j < right.len() && min_elems < limit {
        let elem = right[j];
        let j_start = j;
        while j < right.len() && elem == right[j] {
            j += 1;
        }
        let partition = Premerge { right: (j - j_start) as u32, left: 0 };
        min_elems += cmp::max(partition.left, partition.right);
        result.push(partition);
    }
    result
}

