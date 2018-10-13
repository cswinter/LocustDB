use std::cmp::min;
use std::usize;

use engine::*;
use engine::aggregator::Aggregator;
use errors::QueryError;


pub struct BatchResult<'a> {
    pub group_by: Option<Vec<BoxedVec<'a>>>,
    pub sort_by: Option<usize>,
    pub desc: bool,
    pub select: Vec<BoxedVec<'a>>,
    pub aggregators: Vec<Aggregator>,
    pub level: u32,
    pub batch_count: usize,
    pub show: bool,
    // Buffers that are referenced by query result - unsafe to drop before results are converted into owned values
    pub unsafe_referenced_buffers: Vec<BoxedVec<'a>>,
}

impl<'a> BatchResult<'a> {
    pub fn len(&self) -> usize {
        match self.group_by {
            Some(ref g) => g[0].len(),
            None => self.select.get(0).map_or(0, |s| s.len()),
        }
    }

    pub fn validate(&self) -> Result<(), QueryError> {
        let mut lengths = Vec::new();
        let mut info_str = "".to_owned();
        if let Some(ref group_bys) = self.group_by {
            for (i, group_by) in group_bys.iter().enumerate() {
                lengths.push(group_by.len());
                info_str = format!("{}:group_by[{}].len = {}", info_str, i, group_by.len());
            }
        }
        for (i, select) in self.select.iter().enumerate() {
            lengths.push(select.len());
            info_str = format!("{}:select[{}].len = {}", info_str, i, select.len()).to_owned();
        }
        let all_lengths_same = lengths.iter().all(|x| *x == lengths[0]);
        if all_lengths_same {
            Ok(())
        } else {
            Err(QueryError::FatalError(info_str))
        }
    }
}

pub fn combine<'a>(batch1: BatchResult<'a>, batch2: BatchResult<'a>, limit: usize) -> Result<BatchResult<'a>, QueryError> {
    match (batch1.group_by, batch2.group_by) {
        // Aggregation query
        (Some(g1), Some(g2)) => {
            let mut executor = QueryExecutor::default();
            let left_t = g1.iter().map(|vec| { vec.get_type() }).collect::<Vec<_>>();
            let right_t = g2.iter().map(|vec| { vec.get_type() }).collect::<Vec<_>>();
            let left = g1.into_iter().map(|vec| { set("left", vec, &mut executor) }).collect::<Vec<_>>();
            let right = g2.into_iter().map(|vec| { set("right", vec, &mut executor) }).collect::<Vec<_>>();
            let (group_by_cols, ops) = if left.len() == 1 {
                // TODO(clemens): other types, val coercion
                let merged = executor.named_buffer("merged");
                let ops = executor.named_buffer("merge_ops");
                executor.push(VecOperator::merge_deduplicate(left[0], right[0], merged, ops, left_t[0], right_t[0]));
                (vec![merged], ops)
            } else {
                let mut partitioning = executor.named_buffer("partitioning");
                executor.push(VecOperator::partition(left[0], right[0], partitioning, left_t[0], right_t[0], limit));

                for i in 1..(left.len() - 1) {
                    let subpartitioning = executor.named_buffer("subpartitioning");
                    executor.push(VecOperator::subpartition(partitioning, left[i], right[i], subpartitioning, left_t[i], right_t[i]));
                    partitioning = subpartitioning;
                }

                let last = left.len() - 1;
                let merged = executor.named_buffer("merged");
                let ops = executor.named_buffer("merge_ops");
                executor.push(VecOperator::merge_deduplicate_partitioned(
                    partitioning, left[last], right[last], merged, ops, left_t[last], right_t[last]));

                let mut group_by_cols = Vec::with_capacity(left.len());
                for i in 0..last {
                    let merged = executor.named_buffer("merged");
                    executor.push(VecOperator::merge_drop(ops, left[i], right[i], merged, left_t[i], right_t[i]));
                    group_by_cols.push(merged);
                }
                group_by_cols.push(merged);

                (group_by_cols, ops)
            };

            let mut aggregates = Vec::with_capacity(batch1.aggregators.len());
            for ((aggregator, select1), select2) in batch1.aggregators.iter().zip(batch1.select).zip(batch2.select) {
                let left = set("left", select1, &mut executor);
                let right = set("right", select2, &mut executor);
                let aggregated = executor.named_buffer("aggregated");
                executor.push(VecOperator::merge_aggregate(ops, left, right, aggregated, *aggregator));
                aggregates.push(aggregated);
            }

            let mut results = executor.prepare_no_columns();
            executor.run(1, &mut results, batch1.show || batch2.show);
            let group_by_cols = group_by_cols.into_iter().map(|i| results.collect(i)).collect();
            let select = aggregates.into_iter().map(|i| results.collect(i)).collect();

            Ok(BatchResult {
                group_by: Some(group_by_cols),
                sort_by: None,
                desc: batch1.desc,
                select,
                aggregators: batch1.aggregators,
                level: batch1.level + 1,
                batch_count: batch1.batch_count + batch2.batch_count,
                show: batch1.show && batch2.show,
                unsafe_referenced_buffers: {
                    let mut urb = batch1.unsafe_referenced_buffers;
                    urb.extend(batch2.unsafe_referenced_buffers.into_iter());
                    urb
                },
            })
        }
        // No aggregation
        (None, None) => {
            match batch1.sort_by {
                // Sort query
                Some(index) => {
                    let mut executor = QueryExecutor::default();
                    let left_t = batch1.select.iter().map(|vec| { vec.get_type() }).collect::<Vec<_>>();
                    let right_t = batch2.select.iter().map(|vec| { vec.get_type() }).collect::<Vec<_>>();
                    let left = batch1.select.into_iter().map(|vec| { set("left", vec, &mut executor) }).collect::<Vec<_>>();
                    let right = batch2.select.into_iter().map(|vec| { set("right", vec, &mut executor) }).collect::<Vec<_>>();
                    let ops = executor.named_buffer("take_left");
                    let merged_sort_cols = executor.named_buffer("merged_sort_cols");
                    executor.push(VecOperator::merge(
                        (left[index], left_t[index]),
                        (right[index], right_t[index]),
                        merged_sort_cols,
                        ops,
                        limit,
                        batch1.desc));

                    let mut select = Vec::with_capacity(left.len());
                    for (i, (&left, right)) in left.iter().zip(right).enumerate() {
                        if i == index {
                            select.push(BufferRef(0xdead_beef, "MERGE_ERROR"));
                        } else {
                            let merged = executor.named_buffer("merged_sort_cols");
                            executor.push(VecOperator::merge_keep(ops, left, right, merged, left_t[i], right_t[i]));
                            select.push(merged);
                        }
                    }
                    select[index] = merged_sort_cols;

                    let mut results = executor.prepare_no_columns();
                    executor.run(1, &mut results, batch1.show || batch2.show);
                    let select = select.into_iter().map(|i| results.collect(i)).collect();

                    Ok(BatchResult {
                        group_by: None,
                        sort_by: Some(index),
                        select,
                        desc: batch1.desc,
                        aggregators: Vec::new(),
                        level: batch1.level + 1,
                        batch_count: batch1.batch_count + batch2.batch_count,
                        show: batch1.show && batch2.show,
                        unsafe_referenced_buffers: {
                            let mut urb = batch1.unsafe_referenced_buffers;
                            urb.extend(batch2.unsafe_referenced_buffers.into_iter());
                            urb
                        },
                    })
                }
                // Select query
                None => {
                    let mut result = Vec::with_capacity(batch1.select.len());
                    for (mut col1, col2) in batch1.select.into_iter().zip(batch2.select) {
                        let count = if col1.len() >= limit { 0 } else {
                            min(col2.len(), limit - col1.len())
                        };
                        if let Some(newcol) = col1.append_all(&*col2, count) {
                            result.push(newcol)
                        } else {
                            result.push(col1)
                        }
                    }
                    Ok(BatchResult {
                        group_by: None,
                        sort_by: None,
                        select: result,
                        desc: batch1.desc,
                        aggregators: Vec::new(),
                        level: batch1.level + 1,
                        batch_count: batch1.batch_count + batch2.batch_count,
                        show: batch1.show && batch2.show,
                        unsafe_referenced_buffers: {
                            let mut urb = batch1.unsafe_referenced_buffers;
                            urb.extend(batch2.unsafe_referenced_buffers.into_iter());
                            urb
                        },
                    })
                }
            }
        }
        _ => bail!(QueryError::FatalError,  "Trying to merge incompatible batch results")
    }
}

fn set<'a>(name: &'static str, vec: BoxedVec<'a>, executor: &mut QueryExecutor<'a>) -> BufferRef {
    let buffer = executor.named_buffer(name);
    let op = VecOperator::constant_vec(vec, buffer);
    executor.push(op);
    buffer
}

