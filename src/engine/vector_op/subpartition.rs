use std::marker::PhantomData;

use engine::typed_vec::Premerge;
use engine::vector_op::*;
use engine::*;


#[derive(Debug)]
pub struct SubPartition<T> {
    pub partitioning: BufferRef,
    pub left: BufferRef,
    pub right: BufferRef,
    pub sub_partitioning: BufferRef,
    pub t: PhantomData<T>,
}

impl<'a, T: GenericVec<T> + 'a> VecOperator<'a> for SubPartition<T> {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) {
        let sub_partitioning = {
            let partitioning = scratchpad.get::<Premerge>(self.partitioning);
            let left = scratchpad.get::<T>(self.left);
            let right = scratchpad.get::<T>(self.right);
            subpartition(&partitioning, &left, &right)
        };
        scratchpad.set(self.sub_partitioning, Box::new(sub_partitioning));
    }

    fn inputs(&self) -> Vec<BufferRef> { vec![self.partitioning, self.left, self.right] }
    fn outputs(&self) -> Vec<BufferRef> { vec![self.sub_partitioning] }
    fn can_stream_input(&self) -> bool { false }
    fn can_stream_output(&self, _: BufferRef) -> bool { false }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("subpartition({}, {}, {})", self.partitioning, self.left, self.right)
    }
}

fn subpartition<'a, T: GenericVec<T> + 'a>(
    partitioning: &[Premerge],
    left: &[T],
    right: &[T]) -> Vec<Premerge> {
    // TODO(clemens): better estimate?
    let mut result = Vec::with_capacity(2 * partitioning.len());
    let mut i = 0;
    let mut j = 0;
    for group in partitioning {
        let i_max = i + group.left as usize;
        let j_max = j + group.right as usize;
        while i < i_max || j < j_max {
            let mut subpartition = Premerge { left: 0, right: 0 };
            let elem = if i < i_max && (j == j_max || left[i] <= right[j]) { left[i] } else { right[j] };
            while i < i_max && elem == left[i] {
                subpartition.left += 1;
                i += 1;
            }
            while j < j_max && elem == right[j] {
                subpartition.right += 1;
                j += 1;
            }
            result.push(subpartition);
        }
    }
    result
}


#[cfg(test)]
mod tests {
    use engine::*;
    use engine::typed_vec::Premerge;
    use engine::typed_vec::MergeOp::*;
    use engine::vector_op::partition::partition;
    use engine::vector_op::subpartition::subpartition;
    use engine::vector_op::merge_deduplicate_partitioned::merge_deduplicate_partitioned;

    #[test]
    fn test_multipass_grouping() {
        let left1 = vec!["A", "A", "A", "C", "P"];
        let right1 = vec!["A", "A", "B", "C", "X", "X", "Z"];
        let result = partition::<&str>(&left1, &right1, 10);
        assert_eq!(result, vec![
            Premerge { left: 3, right: 2 },
            Premerge { left: 0, right: 1 },
            Premerge { left: 1, right: 1 },
            Premerge { left: 1, right: 0 },
            Premerge { left: 0, right: 2 },
        ]);

        let left2 = vec![1u32, 3, 7, 2, 1];
        let right2 = vec![3u32, 5, 0, 2, 1, 2, 1];
        let (merging, merge_ops) = merge_deduplicate_partitioned::<u32>(&result, &left2, &right2);
        assert_eq!(u32::unwrap(merging.as_ref()), &[1, 3, 5, 7, 0, 2, 1, 1, 2]);
        assert_eq!(&merge_ops, &[
            TakeLeft,
            TakeLeft,
            MergeRight,
            TakeRight,
            TakeLeft,
            TakeRight,
            TakeLeft,
            MergeRight,
            TakeLeft,
            TakeRight,
            TakeRight,
        ]);

        let subpartition = subpartition::<u32>(&result, &left2, &right2);
        assert_eq!(subpartition, vec![
            Premerge { left: 1, right: 0 },
            Premerge { left: 1, right: 1 },
            Premerge { left: 0, right: 1 },
            Premerge { left: 1, right: 0 },
            Premerge { left: 0, right: 1 },
            Premerge { left: 1, right: 1 },
            Premerge { left: 1, right: 0 },
            Premerge { left: 0, right: 1 },
            Premerge { left: 0, right: 1 },
        ]);
    }
}
