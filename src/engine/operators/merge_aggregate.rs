use ordered_float::OrderedFloat;

use crate::engine::*;

#[derive(Debug)]
pub struct MergeAggregate<T> {
    pub merge_ops: BufferRef<MergeOp>,
    pub left: BufferRef<T>,
    pub right: BufferRef<T>,
    pub aggregated: BufferRef<T>,
    pub aggregator: Aggregator,
}

impl<'a, T> VecOperator<'a> for MergeAggregate<T>
where
    T: VecData<T> + Combinable<T> + 'a,
{
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError> {
        let aggregated = {
            let ops = scratchpad.get(self.merge_ops);
            let left = scratchpad.get(self.left);
            let right = scratchpad.get(self.right);
            merge_aggregate(&ops, &left, &right, self.aggregator)
        };
        scratchpad.set(self.aggregated, aggregated?);
        Ok(())
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> {
        vec![self.left.any(), self.right.any(), self.merge_ops.any()]
    }
    fn inputs_mut(&mut self) -> Vec<&mut usize> {
        vec![&mut self.left.i, &mut self.right.i, &mut self.merge_ops.i]
    }
    fn outputs(&self) -> Vec<BufferRef<Any>> {
        vec![self.aggregated.any()]
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
        format!(
            "merge_aggregate({:?}; {}, {}, {})",
            self.aggregator, self.merge_ops, self.left, self.right
        )
    }
}

fn merge_aggregate<T: Combinable<T>>(
    ops: &[MergeOp],
    left: &[T],
    right: &[T],
    aggregator: Aggregator,
) -> Result<Vec<T>, QueryError> {
    if left.is_empty() {
        return Ok(right.to_vec());
    } else if right.is_empty() {
        return Ok(left.to_vec());
    }

    let mut result = Vec::with_capacity(ops.len());
    let mut i = 0;
    let mut j = 0;
    for op in ops {
        match *op {
            MergeOp::TakeLeft => {
                if i == left.len() {
                    error!("{} {} {}", left.len(), right.len(), ops.len());
                }
                result.push(left[i]);
                i += 1;
            }
            MergeOp::TakeRight => {
                if j == right.len() {
                    error!("{} {} {}", left.len(), right.len(), ops.len());
                }
                result.push(right[j]);
                j += 1;
            }
            MergeOp::MergeRight => {
                let last = result.len() - 1;
                result[last] = T::combine(aggregator, result[last], right[j])?;
                j += 1;
            }
        }
    }
    Ok(result)
}

trait Combinable<T>: Clone + Copy {
    fn combine(op: Aggregator, a: T, b: T) -> Result<T, QueryError>;
}

impl Combinable<i64> for i64 {
    fn combine(op: Aggregator, a: i64, b: i64) -> Result<i64, QueryError> {
        fn null_coalesce(a: i64, b: i64, combined: i64) -> Result<i64, QueryError> {
            if a == I64_NULL {
                Ok(b)
            } else if b == I64_NULL {
                Ok(a)
            } else {
                Ok(combined)
            }
        }
        // TODO: remove null handling hack?
        match op {
            Aggregator::SumI64 => {
                if a == I64_NULL {
                    Ok(b)
                } else if b == I64_NULL {
                    Ok(a)
                } else {
                    a.checked_add(b).ok_or(QueryError::Overflow)
                }
            }
            Aggregator::Count => {
                if a == I64_NULL {
                    Ok(b)
                } else if b == I64_NULL {
                    Ok(a)
                } else {
                    Ok(a + b)
                }
            }
            Aggregator::MaxI64 => null_coalesce(a, b, std::cmp::max(a, b)),
            Aggregator::MinI64 => null_coalesce(a, b, std::cmp::min(a, b)),
            _ => Err(fatal!("Unsupported aggregator for i64: {:?}", op)),
        }
    }
}

impl Combinable<OrderedFloat<f64>> for OrderedFloat<f64> {
    fn combine(op: Aggregator, a: of64, b: of64) -> Result<OrderedFloat<f64>, QueryError> {
        // possibly Aggregator::XI64 is masking a bug
        fn null_coalesce(a: of64, b: of64, combined: of64) -> Result<of64, QueryError> {
            if a.to_bits() == F64_NULL.to_bits() {
                Ok(b)
            } else if b.to_bits() == F64_NULL.to_bits() {
                Ok(a)
            } else {
                Ok(combined)
            }
        }
        match op {
            Aggregator::SumF64 | Aggregator::SumI64 => null_coalesce(a, b, a + b),
            Aggregator::MaxF64 | Aggregator::MaxI64 => null_coalesce(a, b, std::cmp::max(a, b)),
            Aggregator::MinF64 | Aggregator::MinI64 => null_coalesce(a, b, std::cmp::min(a, b)),
            _ => Err(fatal!("Unsupported aggregator for f64: {:?}", op)),
        }
    }
}
