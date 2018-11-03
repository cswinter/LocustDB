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
            let left = g1.into_iter().map(|vec| {
                let t = vec.get_type();
                let buffer = set("left", vec, &mut executor);
                (buffer, t)
            }).collect::<Vec<_>>();
            let right = g2.into_iter().map(|vec| {
                let t = vec.get_type();
                let buffer = set("right", vec, &mut executor);
                (buffer, t)
            }).collect::<Vec<_>>();
            let (group_by_cols, ops) = if left.len() == 1 {
                // TODO(clemens): other types, val coercion
                let merged = (executor.named_buffer("merged"), left[0].1);
                let ops = executor.named_buffer("merge_ops").merge_op();
                executor.push(VecOperator::merge_deduplicate(left[0], right[0], merged, ops));
                (vec![merged], ops)
            } else {
                let mut partitioning = executor.named_buffer("partitioning").premerge();
                executor.push(VecOperator::partition(left[0], right[0], partitioning, limit));

                for i in 1..(left.len() - 1) {
                    let subpartitioning = executor.named_buffer("subpartitioning").premerge();
                    executor.push(VecOperator::subpartition(partitioning,
                                                            left[i],
                                                            right[i],
                                                            subpartitioning));
                    partitioning = subpartitioning;
                }

                let last = left.len() - 1;
                let merged = (executor.named_buffer("merged"), left[last].1);
                let ops = executor.named_buffer("merge_ops").merge_op();
                executor.push(VecOperator::merge_deduplicate_partitioned(partitioning,
                                                                         left[last],
                                                                         right[last],
                                                                         merged,
                                                                         ops));

                let mut group_by_cols = Vec::with_capacity(left.len());
                for i in 0..last {
                    let merged = (executor.named_buffer("merged"), left[i].1);
                    executor.push(VecOperator::merge_drop(ops, left[i], right[i], merged));
                    group_by_cols.push(merged);
                }
                group_by_cols.push(merged);

                (group_by_cols, ops)
            };

            let mut aggregates = Vec::with_capacity(batch1.aggregators.len());
            for ((aggregator, select1), select2) in batch1.aggregators.iter().zip(batch1.select).zip(batch2.select) {
                let left = set("left", select1, &mut executor).i64();
                let right = set("right", select2, &mut executor).i64();
                let aggregated = executor.named_buffer("aggregated");
                executor.push(VecOperator::merge_aggregate(ops,
                                                           left,
                                                           right,
                                                           aggregated.i64(),
                                                           *aggregator));
                aggregates.push(aggregated);
            }

            let mut results = executor.prepare_no_columns();
            executor.run(1, &mut results, batch1.show || batch2.show);
            let group_by_cols = group_by_cols.into_iter().map(|i| results.collect(i.0)).collect();
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
                    let left = batch1.select.into_iter().map(|vec| {
                        let t = vec.get_type();
                        let buffer = set("left", vec, &mut executor);
                        (buffer, t)
                    }).collect::<Vec<_>>();
                    let right = batch2.select.into_iter().map(|vec| {
                        let t = vec.get_type();
                        let buffer = set("right", vec, &mut executor);
                        (buffer, t)
                    }).collect::<Vec<_>>();
                    let ops = executor.named_buffer("take_left").u8();
                    let merged_sort_cols = (executor.named_buffer("merged_sort_cols"), left[index].1);
                    executor.push(VecOperator::merge(
                        left[index],
                        right[index],
                        merged_sort_cols,
                        ops,
                        limit,
                        batch1.desc));

                    let mut select = Vec::with_capacity(left.len());
                    for (i, (&left, right)) in left.iter().zip(right).enumerate() {
                        if i == index {
                            select.push(error_buffer_ref("MERGE_ERROR"));
                        } else {
                            let merged = (executor.named_buffer("merged_sort_cols"), left.1);
                            executor.push(VecOperator::merge_keep(ops, left, right, merged));
                            select.push(merged.0);
                        }
                    }
                    select[index] = merged_sort_cols.0;

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

fn set<'a>(name: &'static str,
           vec: BoxedVec<'a>,
           executor: &mut QueryExecutor<'a>) -> BufferRef<Any> {
    let buffer = executor.named_buffer(name);
    let op = VecOperator::constant_vec(vec, buffer);
    executor.push(op);
    buffer
}

