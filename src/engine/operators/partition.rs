use crate::engine::*;
use std::cmp;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::u32;

#[derive(Debug)]
pub struct Partition<T, C> {
    pub left: BufferRef<T>,
    pub right: BufferRef<T>,
    pub partitioning: BufferRef<Premerge>,
    pub limit: usize,
    pub c: PhantomData<C>,
}

impl<'a, T, C> VecOperator<'a> for Partition<T, C>
where
    T: VecData<T> + 'a,
    C: Comparator<T> + Debug,
{
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError> {
        let premerge = {
            let left = scratchpad.get(self.left);
            let right = scratchpad.get(self.right);
            partition::<_, C>(&left, &right, self.limit)
        };
        scratchpad.set(self.partitioning, premerge);
        Ok(())
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> {
        vec![self.left.any(), self.right.any()]
    }
    fn inputs_mut(&mut self) -> Vec<&mut usize> {
        vec![&mut self.left.i, &mut self.right.i]
    }
    fn outputs(&self) -> Vec<BufferRef<Any>> {
        vec![self.partitioning.any()]
    }
    fn can_stream_input(&self, _: usize) -> bool {
        false
    }
    fn can_stream_output(&self, _: usize) -> bool {
        false
    }
    fn allocates(&self) -> bool {
        true
    }

    fn display_op(&self, _: bool) -> String {
        format!("partition({}, {})", self.left, self.right)
    }
}

// Finds runs of identical elements in the left and right vectors
pub fn partition<'a, T, C>(left: &[T], right: &[T], limit: usize) -> Vec<Premerge>
where
    T: VecData<T> + 'a,
    C: Comparator<T>,
{
    let mut result = Vec::new();
    let mut i = 0;
    let mut j = 0;
    let limit = if limit > u32::MAX as usize {
        u32::MAX
    } else {
        limit as u32
    };
    let mut min_elems = 0u32;
    while i < left.len() && j < right.len() && min_elems < limit {
        let mut partition = Premerge { left: 0, right: 0 };
        // Determine whether next run of elements is started from left or right
        let elem = if C::cmp_eq(left[i], right[j]) {
            left[i]
        } else {
            right[j]
        };
        while i < left.len() && elem == left[i] {
            partition.left += 1;
            i += 1;
        }
        while j < right.len() && elem == right[j] {
            partition.right += 1;
            j += 1;
        }
        // TODO: why max not sum?
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
        let partition = Premerge {
            left: (i - i_start) as u32,
            right: 0,
        };
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
        let partition = Premerge {
            right: (j - j_start) as u32,
            left: 0,
        };
        min_elems += cmp::max(partition.left, partition.right);
        result.push(partition);
    }
    result
}


#[cfg(test)]
mod tests {
    use crate::mem_store::Val;

    use super::*;

    #[test]
    fn test_partition() {
        let left = vec![-10, 0, 0, 1, 4, 4];
        let right = vec![0, 1, 1, 1, 3, 4, 5];
        let limit = 10;
        let result = partition::<i64, CmpLessThan>(&left, &right, limit);
        assert_eq!(
            result,
            vec![
                Premerge { left: 1, right: 0 }, // -10
                Premerge { left: 2, right: 1 }, // 0
                Premerge { left: 1, right: 3 }, // 1
                Premerge { left: 0, right: 1 }, // 3
                Premerge { left: 2, right: 1 }, // 4
                Premerge { left: 0, right: 1 }, // 4
            ]
        );
    }

    #[test]
    fn test_partition_val_f64_null() {
        let left = vec![Val::from(0.4), Val::Null, Val::Null];
        let right = vec![Val::from(1e-32), Val::Null, Val::Null, Val::Null, Val::Null, Val::Null];

        let limit = 10;
        let result = partition::<Val, CmpLessThan>(&left, &right, limit);
        assert_eq!(
            result,
            vec![
                Premerge { left: 0, right: 1 }, Premerge { left: 1, right: 0 }, Premerge { left: 2, right: 5 },
            ]
        );
    }
}
