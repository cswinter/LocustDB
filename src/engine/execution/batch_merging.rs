use std::cmp::min;
use std::sync::Arc;
use std::collections::HashMap;
use std::usize;
use std::result::Result;

use mem_store::column::DataSource;
use engine::*;
use errors::QueryError;

#[derive(Debug)]
pub struct BatchResult<'a> {
    pub columns: Vec<BoxedData<'a>>,
    pub projection: Vec<usize>,
    pub aggregations: Vec<(usize, Aggregator)>,
    pub order_by: Vec<(usize, bool)>,
    pub level: u32,
    pub batch_count: usize,
    pub show: bool,
    // Buffers that are referenced by query result - unsafe to drop before results are converted into owned values
    pub unsafe_referenced_buffers: Vec<BoxedData<'a>>,
}

impl<'a> BatchResult<'a> {
    pub fn len(&self) -> usize {
        self.columns.get(0).map_or(0, |s| s.len())
    }

    pub fn validate(&self) -> Result<(), QueryError> {
        let mut lengths = Vec::new();
        let mut info_str = "".to_owned();
        for (i, select) in self.columns.iter().enumerate() {
            lengths.push(select.len());
            info_str = format!("{}:columns[{}].len = {}", info_str, i, select.len()).to_owned();
        }
        let all_lengths_same = lengths.iter().all(|x| *x == lengths[0]);
        if !all_lengths_same {
            return Err(fatal!(info_str));
        }
        for (i, _) in &self.aggregations {
            if *i >= self.columns.len() {
                return Err(fatal!("Aggregation exceeds number of columns ({}): {:?}", self.columns.len(), &self.aggregations));
            }
        }

        Ok(())
    }

    pub fn into_columns(self) -> HashMap<String, Arc<DataSource + 'a>> {
        let mut cols = HashMap::<String, Arc<DataSource>>::default();
        let columns = self.columns.into_iter().map(|c| Arc::new(c)).collect::<Vec<_>>();
        for projection in self.projection {
            cols.insert(format!("_cs{}", projection), columns[projection].clone());
        }
        for (i, &(aggregation, _)) in self.aggregations.iter().enumerate() {
            cols.insert(format!("_ca{}", i), columns[aggregation].clone());
        }
        cols
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
    ensure!(
        batch1.aggregations.len()  == batch2.aggregations.len(),
        "Unequal number of aggregations in left ({:?}) and right ({:?}) batch result.",
        batch1.aggregations.len(), batch2.aggregations.len(),
    );

    if !batch1.aggregations.is_empty() {
        // Aggregation query
        let mut executor = QueryExecutor::default();
        let left = batch1.columns.into_iter()
            .map(|vec| set(&mut executor, "left", vec))
            .collect::<Vec<_>>();
        let right = batch2.columns.into_iter()
            .map(|vec| set(&mut executor, "right", vec))
            .collect::<Vec<_>>();

        let lprojection = batch1.projection;
        let rprojection = batch2.projection;
        let (group_by_cols, ops) = if lprojection.len() == 0 {
            let ops = executor.buffer_merge_op("merge_ops");
            executor.push(VecOperator::constant_vec(Box::new(vec![MergeOp::TakeLeft, MergeOp::MergeRight]), ops.any()));
            (vec![], ops)
        } else if lprojection.len() == 1 {
            // TODO(clemens): other types, val coercion
            let merged = executor.named_buffer("merged", left[lprojection[0]].tag);
            let ops = executor.buffer_merge_op("merge_ops");
            executor.push(VecOperator::merge_deduplicate(left[lprojection[0]], right[rprojection[0]],
                                                         merged, ops)?);
            (vec![merged.any()], ops)
        } else {
            let mut partitioning = executor.buffer_premerge("partitioning");
            executor.push(VecOperator::partition(left[lprojection[0]], right[rprojection[0]],
                                                 partitioning, limit, false)?);

            for i in 1..(lprojection.len() - 1) {
                let subpartitioning = executor.buffer_premerge("subpartitioning");
                executor.push(VecOperator::subpartition(partitioning,
                                                        left[lprojection[i]], right[rprojection[i]],
                                                        subpartitioning,
                                                        false)?);
                partitioning = subpartitioning;
            }

            let last = lprojection.len() - 1;
            let merged = executor.named_buffer("merged", left[lprojection[last]].tag);
            let ops = executor.buffer_merge_op("merge_ops");
            executor.push(VecOperator::merge_deduplicate_partitioned(partitioning,
                                                                     left[lprojection[last]],
                                                                     right[rprojection[last]],
                                                                     merged,
                                                                     ops)?);

            let mut group_by_cols = Vec::with_capacity(lprojection.len());
            for i in 0..last {
                let merged = executor.named_buffer("merged", left[lprojection[i]].tag);
                executor.push(VecOperator::merge_drop(ops, left[lprojection[i]], right[rprojection[i]], merged)?);
                group_by_cols.push(merged.any());
            }
            group_by_cols.push(merged.any());

            (group_by_cols, ops)
        };

        let mut aggregates = Vec::with_capacity(batch1.aggregations.len());
        for (&(ileft, aggregator), &(iright, _)) in batch1.aggregations.iter().zip(batch2.aggregations.iter()) {
            let left = left[ileft].i64()?;
            let right = right[iright].i64()?;
            let aggregated = executor.buffer_i64("aggregated");
            executor.push(VecOperator::merge_aggregate(ops,
                                                       left,
                                                       right,
                                                       aggregated,
                                                       aggregator));
            aggregates.push((aggregated.any(), aggregator));
        }

        let mut results = executor.prepare_no_columns();
        executor.run(1, &mut results, batch1.show || batch2.show);

        let (columns, projection, aggregations, _) = results.collect_aliased(&group_by_cols, &aggregates, &[]);
        let result = BatchResult {
            columns,
            projection,
            aggregations,
            order_by: vec![],
            level: batch1.level + 1,
            batch_count: batch1.batch_count + batch2.batch_count,
            show: batch1.show && batch2.show,
            unsafe_referenced_buffers: {
                let mut urb = batch1.unsafe_referenced_buffers;
                urb.extend(batch2.unsafe_referenced_buffers.into_iter());
                urb
            },
        };
        result.validate()?;
        Ok(result)
    } else {
        // No aggregation
        if !batch1.order_by.is_empty() {
            // Sort query
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
                    let merged = query_plan::prepare(
                        query_syntax::merge_keep(TypedBufferRef::from(merge_ops), left[ileft], right[iright]),
                        &mut executor)?;
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

            let (columns, projection, _, order_by) = results.collect_aliased(&projection, &[], &order_by);

            Ok(BatchResult {
                columns,
                projection,
                order_by,
                aggregations: vec![],
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
            ensure!(batch1.projection  == batch2.projection,
                    "Different projections in select batches ({:?}, {:?})",
                    &batch1.projection, &batch2.projection);
            let mut result = Vec::with_capacity(batch1.columns.len());
            let show = batch1.show || batch2.show;
            for (mut col1, col2) in batch1.columns.into_iter().zip(batch2.columns) {
                if show {
                    println!("Merging columns");
                    println!("{}", col1.display());
                    println!("{}", col2.display());
                }
                let count = if col1.len() >= limit { 0 } else {
                    min(col2.len(), limit - col1.len())
                };
                if let Some(newcol) = col1.append_all(&*col2, count) {
                    if show { println!("{}", newcol.display()); }
                    result.push(newcol)
                } else {
                    if show { println!("{}", col1.display()); }
                    result.push(col1)
                }
            }
            Ok(BatchResult {
                columns: result,
                projection: batch1.projection,
                aggregations: vec![],
                order_by: vec![],
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

fn set<'a>(executor: &mut QueryExecutor<'a>,
           name: &'static str,
           vec: BoxedData<'a>) -> TypedBufferRef {
    let buffer = executor.named_buffer(name, vec.get_type());
    let op = VecOperator::constant_vec(vec, buffer.any());
    executor.push(op);
    buffer
}

