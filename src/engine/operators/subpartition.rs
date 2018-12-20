use std::fmt::Debug;
use std::marker::PhantomData;

use engine::*;


#[derive(Debug)]
pub struct SubPartition<T, C> {
    pub partitioning: BufferRef<Premerge>,
    pub left: BufferRef<T>,
    pub right: BufferRef<T>,
    pub sub_partitioning: BufferRef<Premerge>,
    pub c: PhantomData<C>,
}

impl<'a, T: VecData<T> + 'a, C: Comparator<T> + Debug> VecOperator<'a> for SubPartition<T, C> {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) {
        let sub_partitioning = {
            let partitioning = scratchpad.get(self.partitioning);
            let left = scratchpad.get(self.left);
            let right = scratchpad.get(self.right);
            subpartition::<_, C>(&partitioning, &left, &right)
        };
        scratchpad.set(self.sub_partitioning, sub_partitioning);
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.partitioning.any(), self.left.any(), self.right.any()] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.sub_partitioning.any()] }
    fn can_stream_input(&self, _: usize) -> bool { false }
    fn can_stream_output(&self, _: usize) -> bool { false }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("subpartition({}, {}, {})", self.partitioning, self.left, self.right)
    }
}

fn subpartition<'a, T: VecData<T> + 'a, C: Comparator<T>>(
    partitioning: &[Premerge],
    left: &[T],
    right: &[T]) -> Vec<Premerge> {
    // Could probably derive better estimate
    let mut result = Vec::with_capacity(2 * partitioning.len());
    let mut i = 0;
    let mut j = 0;
    #[allow(clippy::explicit_counter_loop)] // false positive
        for group in partitioning {
        let i_max = i + group.left as usize;
        let j_max = j + group.right as usize;
        while i < i_max || j < j_max {
            let mut subpartition = Premerge { left: 0, right: 0 };
            let elem = if i < i_max && (j == j_max || C::cmp_eq(left[i], right[j])) {
                left[i]
            } else {
                right[j]
            };
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
    use self::MergeOp::*;
    use engine::operators::partition::partition;
    use engine::operators::subpartition::subpartition;
    use engine::operators::merge_deduplicate_partitioned::merge_deduplicate_partitioned;

    #[test]
    fn test_multipass_grouping() {
        let left1 = vec!["A", "A", "A", "C", "P"];
        let right1 = vec!["A", "A", "B", "C", "X", "X", "Z"];
        let result = partition::<&str, CmpLessThan>(&left1, &right1, 7);
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
        assert_eq!(&merging, &[1, 3, 5, 7, 0, 2, 1, 1, 2]);
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

        let subpartition = subpartition::<u32, CmpLessThan>(&result, &left2, &right2);
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
