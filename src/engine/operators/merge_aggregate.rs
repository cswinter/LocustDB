use crate::engine::*;

#[derive(Debug)]
pub struct MergeAggregate {
    pub merge_ops: BufferRef<MergeOp>,
    pub left: BufferRef<i64>,
    pub right: BufferRef<i64>,
    pub aggregated: BufferRef<i64>,
    pub aggregator: Aggregator,
}

impl<'a> VecOperator<'a> for MergeAggregate {
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

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.left.any(), self.right.any(), self.merge_ops.any()] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.aggregated.any()] }
    fn can_stream_input(&self, _: usize) -> bool { false }
    fn can_stream_output(&self, _: usize) -> bool { false }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("merge_aggregate({:?}; {}, {}, {})", self.aggregator, self.merge_ops, self.left, self.right)
    }
}

fn merge_aggregate(ops: &[MergeOp], left: &[i64], right: &[i64], aggregator: Aggregator) -> Result<Vec<i64>, QueryError> {
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
                result[last] = aggregator.combine_i64(result[last], right[j])?;
                j += 1;
            }
        }
    }
    Ok(result)
}

