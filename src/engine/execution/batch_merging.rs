use std::cmp::min;
use std::collections::HashMap;
use std::usize;
use std::result::Result;

use engine::*;
use errors::QueryError;


pub struct BatchResult<'a> {
    pub columns: Vec<BoxedVec<'a>>,
    pub group_by_columns: Option<Vec<BoxedVec<'a>>>,
    pub projection: Vec<usize>,
    pub order_by: Vec<(usize, bool)>,
    pub aggregators: Vec<Aggregator>,
    pub level: u32,
    pub batch_count: usize,
    pub show: bool,
    // Buffers that are referenced by query result - unsafe to drop before results are converted into owned values
    pub unsafe_referenced_buffers: Vec<BoxedVec<'a>>,
}

impl<'a> BatchResult<'a> {
    pub fn len(&self) -> usize {
        match self.group_by_columns {
            Some(ref g) => g[0].len(),
            None => self.columns.get(0).map_or(0, |s| s.len()),
        }
    }

    pub fn validate(&self) -> Result<(), QueryError> {
        let mut lengths = Vec::new();
        let mut info_str = "".to_owned();
        if let Some(ref group_bys) = self.group_by_columns {
            for (i, group_by) in group_bys.iter().enumerate() {
                lengths.push(group_by.len());
                info_str = format!("{}:group_by[{}].len = {}", info_str, i, group_by.len());
            }
        }
        for (i, select) in self.columns.iter().enumerate() {
            lengths.push(select.len());
            info_str = format!("{}:select[{}].len = {}", info_str, i, select.len()).to_owned();
        }
        let all_lengths_same = lengths.iter().all(|x| *x == lengths[0]);
        if all_lengths_same {
            Ok(())
        } else {
            Err(fatal!(info_str))
        }
    }
}

pub fn combine<'a>(batch1: BatchResult<'a>, batch2: BatchResult<'a>, limit: usize) -> Result<BatchResult<'a>, QueryError> {
    ensure!(
        batch1.projection.len()  == batch2.projection.len(),
        "Unequal number of projections in left ({}) and right ({}) batch result.",
        batch1.projection.len(), batch2.projection.len(),
    );
    ensure!(
        batch1.order_by.len()  == batch2.order_by.len(),
        "Unequal number of order by in left ({}) and right ({}) batch result.",
        batch1.order_by.len(), batch2.order_by.len(),
    );

    match (batch1.group_by_columns, batch2.group_by_columns) {
        // Aggregation query
        (Some(g1), Some(g2)) => {
            let mut executor = QueryExecutor::default();
            let left = g1.into_iter()
                .map(|vec| set(&mut executor, "left", vec))
                .collect::<Vec<_>>();
            let right = g2.into_iter()
                .map(|vec| set(&mut executor, "right", vec))
                .collect::<Vec<_>>();
            let (group_by_cols, ops) = if left.len() == 1 {
                // TODO(clemens): other types, val coercion
                let merged = executor.named_buffer("merged", left[0].tag);
                let ops = executor.buffer_merge_op("merge_ops");
                executor.push(VecOperator::merge_deduplicate(left[0], right[0], merged, ops)?);
                (vec![merged], ops)
            } else {
                let mut partitioning = executor.buffer_premerge("partitioning");
                executor.push(VecOperator::partition(left[0], right[0], partitioning, limit, false)?);

                for i in 1..(left.len() - 1) {
                    let subpartitioning = executor.buffer_premerge("subpartitioning");
                    executor.push(VecOperator::subpartition(partitioning,
                                                            left[i], right[i],
                                                            subpartitioning,
                                                            false)?);
                    partitioning = subpartitioning;
                }

                let last = left.len() - 1;
                let merged = executor.named_buffer("merged", left[last].tag);
                let ops = executor.buffer_merge_op("merge_ops");
                executor.push(VecOperator::merge_deduplicate_partitioned(partitioning,
                                                                         left[last],
                                                                         right[last],
                                                                         merged,
                                                                         ops)?);

                let mut group_by_cols = Vec::with_capacity(left.len());
                for i in 0..last {
                    let merged = executor.named_buffer("merged", left[i].tag);
                    executor.push(VecOperator::merge_drop(ops, left[i], right[i], merged)?);
                    group_by_cols.push(merged);
                }
                group_by_cols.push(merged);

                (group_by_cols, ops)
            };

            let mut aggregates = Vec::with_capacity(batch1.aggregators.len());
            for ((aggregator, select1), select2) in batch1.aggregators.iter().zip(batch1.columns).zip(batch2.columns) {
                let left = set(&mut executor, "left", select1).i64()?;
                let right = set(&mut executor, "right", select2).i64()?;
                let aggregated = executor.buffer_i64("aggregated");
                executor.push(VecOperator::merge_aggregate(ops,
                                                           left,
                                                           right,
                                                           aggregated,
                                                           *aggregator));
                aggregates.push(aggregated.any());
            }

            let mut results = executor.prepare_no_columns();
            executor.run(1, &mut results, batch1.show || batch2.show);

            let group_by_cols = group_by_cols.into_iter().map(|i| results.collect(i.any())).collect();
            let (columns, projection, _) = results.collect_aliased(&aggregates, &[]);
            Ok(BatchResult {
                group_by_columns: Some(group_by_cols),
                columns,
                projection,
                order_by: vec![],
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
            // Sort query
            if !batch1.order_by.is_empty() {
                let mut executor = QueryExecutor::default();
                let left = batch1.columns.into_iter()
                    .map(|vec| set(&mut executor, "left", vec))
                    .collect::<Vec<_>>();
                let right = batch2.columns.into_iter()
                    .map(|vec| set(&mut executor, "right", vec))
                    .collect::<Vec<_>>();


                let (final_sort_col_index1, final_desc) = *batch1.order_by.last().unwrap();
                let final_sort_col_index2 = batch2.order_by.last().unwrap().0;
                let merge_ops = executor.buffer_u8("take_left");
                let merged_final_sort_col = executor.named_buffer("merged_final_sort_col",
                                                                  left[final_sort_col_index1].tag);
                if batch1.order_by.len() == 1 {
                    let (index1, desc) = batch1.order_by[0];
                    let (index2, _) = batch2.order_by[0];
                    executor.push(VecOperator::merge(
                        left[index1], right[index2],
                        merged_final_sort_col,
                        merge_ops,
                        limit, desc)?);
                } else {
                    let (first_sort_col_index1, desc) = batch1.order_by[0];
                    let (first_sort_col_index2, _) = batch2.order_by[0];
                    let mut partitioning = executor.buffer_premerge("partitioning");
                    executor.push(VecOperator::partition(
                        left[first_sort_col_index1], right[first_sort_col_index2],
                        partitioning,
                        limit, desc)?);

                    for i in 1..(left.len() - 1) {
                        let subpartitioning = executor.buffer_premerge("subpartitioning");
                        let (index1, desc) = batch1.order_by[i];
                        let (index2, _) = batch1.order_by[i];
                        executor.push(VecOperator::subpartition(partitioning,
                                                                left[index1], right[index2],
                                                                subpartitioning,
                                                                desc)?);
                        partitioning = subpartitioning;
                    }
                    executor.push(VecOperator::merge_partitioned(
                        partitioning,
                        left[final_sort_col_index1], right[final_sort_col_index2],
                        merged_final_sort_col,
                        merge_ops,
                        limit, batch1.order_by.last().unwrap().1)?);
                };

                // TODO(clemens): leverage common subexpression elimination pass in query plan to preserve aliased selection columns and simplify code
                let mut projection = Vec::new();
                let mut merges = HashMap::<(usize, usize), BufferRef<Any>>::default();
                for (&ileft, &iright) in batch1.projection.iter().zip(batch2.projection.iter()) {
                    if ileft == final_sort_col_index1 && iright == final_sort_col_index2 {
                        projection.push(merged_final_sort_col.any());
                    } else {
                        let merged = executor.named_buffer("merged_cols", left[ileft].tag);
                        executor.push(VecOperator::merge_keep(merge_ops, left[ileft], right[iright], merged)?);
                        projection.push(merged.any());
                        merges.insert((ileft, iright), merged.any());
                    }
                }
                let mut order_by = vec![];
                for (&(ileft, desc), &(iright, _)) in
                    batch1.order_by[0..batch1.order_by.len() - 1].iter()
                        .zip(batch2.order_by.iter()) {
                    let buffer = match merges.get(&(ileft, iright)) {
                        Some(buffer) => *buffer,
                        None => {
                            let merged = executor.named_buffer("merged_sort_cols", left[ileft].tag);
                            executor.push(VecOperator::merge_keep(merge_ops, left[ileft], right[iright], merged)?);
                            merged.any()
                        }
                    };
                    order_by.push((buffer, desc));
                }
                order_by.push((merged_final_sort_col.any(), final_desc));

                let mut results = executor.prepare_no_columns();
                executor.run(1, &mut results, batch1.show || batch2.show);

                let (columns, projection, order_by) = results.collect_aliased(&projection, &order_by);

                Ok(BatchResult {
                    columns,
                    projection,
                    order_by,
                    group_by_columns: None,
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
            } else { // Select query
                // TODO(clemens): make this work for differently aliased columns (need to send through query planner)
                ensure!(
                        batch1.projection  == batch2.projection,
                        "Different projections in select batches ({:?}, {:?})",
                        &batch1.projection, &batch2.projection
                    );
                let mut result = Vec::with_capacity(batch1.columns.len());
                for (mut col1, col2) in batch1.columns.into_iter().zip(batch2.columns) {
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
                    columns: result,
                    projection: batch1.projection,
                    group_by_columns: None,
                    order_by: vec![],
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
        _ => return Err(fatal!("Trying to merge incompatible batch results"))
    }
}

fn set<'a>(executor: &mut QueryExecutor<'a>,
           name: &'static str,
           vec: BoxedVec<'a>) -> TypedBufferRef {
    let buffer = executor.named_buffer(name, vec.get_type());
    let op = VecOperator::constant_vec(vec, buffer.any());
    executor.push(op);
    buffer
}

