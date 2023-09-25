use crate::engine::*;
use crate::errors::QueryError;
use crate::mem_store::column::DataSource;
use std::cmp::min;
use std::collections::HashMap;
use std::result::Result;
use std::sync::Arc;
use std::usize;

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
                return Err(fatal!(
                    "Aggregation exceeds number of columns ({}): {:?}",
                    self.columns.len(),
                    &self.aggregations
                ));
            }
        }

        Ok(())
    }

    pub fn into_columns(self) -> HashMap<String, Arc<dyn DataSource + 'a>> {
        let mut cols = HashMap::<String, Arc<dyn DataSource>>::default();
        let columns = self.columns.into_iter().map(Arc::new).collect::<Vec<_>>();
        for projection in self.projection {
            cols.insert(format!("_cs{}", projection), columns[projection].clone());
        }
        for (i, &(aggregation, _)) in self.aggregations.iter().enumerate() {
            cols.insert(format!("_ca{}", i), columns[aggregation].clone());
        }
        cols
    }
}

pub fn combine<'a>(
    batch1: BatchResult<'a>,
    batch2: BatchResult<'a>,
    limit: usize,
) -> Result<BatchResult<'a>, QueryError> {
    ensure!(
        batch1.projection.len() == batch2.projection.len(),
        "Unequal number of projections in left ({}) and right ({}) batch result.",
        batch1.projection.len(),
        batch2.projection.len(),
    );
    ensure!(
        batch1.order_by.len() == batch2.order_by.len(),
        "Unequal number of order by in left ({}) and right ({}) batch result.",
        batch1.order_by.len(),
        batch2.order_by.len(),
    );
    ensure!(
        batch1.aggregations.len() == batch2.aggregations.len(),
        "Unequal number of aggregations in left ({:?}) and right ({:?}) batch result.",
        batch1.aggregations.len(),
        batch2.aggregations.len(),
    );

    let mut qp = QueryPlanner::default();
    let mut data = Vec::new();

    if !batch1.aggregations.is_empty() {
        // Aggregation query
        let left = batch1
            .columns
            .into_iter()
            .map(|vec| {
                let buffer = qp.constant_vec(data.len(), vec.encoding_type());
                data.push(vec);
                buffer
            })
            .collect::<Vec<_>>();
        let right = batch2
            .columns
            .into_iter()
            .map(|vec| {
                let buffer = qp.constant_vec(data.len(), vec.encoding_type());
                data.push(vec);
                buffer
            })
            .collect::<Vec<_>>();

        let lprojection = batch1.projection;
        let rprojection = batch2.projection;
        let (group_by_cols, ops) = if lprojection.is_empty() {
            let ops = qp
                .constant_vec(data.len(), EncodingType::MergeOp)
                .merge_op()?;
            data.push(Box::new(vec![MergeOp::TakeLeft, MergeOp::MergeRight]));
            (vec![], ops)
        } else if lprojection.len() == 1 {
            let (l, r) = unify_types(&mut qp, left[lprojection[0]], right[rprojection[0]]);
            let (ops, merged) = qp.merge_deduplicate(l, r);
            (vec![merged.any()], ops)
        } else {
            let (l, r) = unify_types(&mut qp, left[lprojection[0]], right[rprojection[0]]);
            let mut partitioning = qp.partition(l, r, limit, false);
            for i in 1..(lprojection.len() - 1) {
                let (l, r) = unify_types(&mut qp, left[lprojection[i]], right[rprojection[i]]);
                partitioning = qp.subpartition(partitioning, l, r, false);
            }

            let last = lprojection.len() - 1;
            let (l, r) = unify_types(&mut qp, left[lprojection[last]], right[rprojection[last]]);
            let (ops, merged) = qp.merge_deduplicate_partitioned(partitioning, l, r);

            let mut group_by_cols = Vec::with_capacity(lprojection.len());
            for i in 0..last {
                let (l, r) = unify_types(&mut qp, left[lprojection[i]], right[rprojection[i]]);
                let merged = qp.merge_drop(ops, l, r);
                group_by_cols.push(merged.any());
            }
            group_by_cols.push(merged.any());

            (group_by_cols, ops)
        };

        let mut aggregates = Vec::with_capacity(batch1.aggregations.len());
        for (&(ileft, aggregator), &(iright, _)) in
            batch1.aggregations.iter().zip(batch2.aggregations.iter())
        {
            let left = left[ileft];
            let right = right[iright];
            let aggregated = qp.merge_aggregate(ops, left, right, aggregator);
            aggregates.push((aggregated.any(), aggregator));
        }

        let mut executor = qp.prepare(data)?;
        let mut results = executor.prepare_no_columns();
        executor.run(1, &mut results, batch1.show || batch2.show)?;

        let (columns, projection, aggregations, _) =
            results.collect_aliased(&group_by_cols, &aggregates, &[]);
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
                urb.extend(batch2.unsafe_referenced_buffers);
                urb
            },
        };
        result.validate()?;
        Ok(result)
    } else {
        // No aggregation
        if !batch1.order_by.is_empty() {
            // Sort query
            let left = batch1
                .columns
                .into_iter()
                .map(|vec| {
                    let buffer = qp.constant_vec(data.len(), vec.encoding_type());
                    data.push(vec);
                    buffer
                })
                .collect::<Vec<_>>();
            let right = batch2
                .columns
                .into_iter()
                .map(|vec| {
                    let buffer = qp.constant_vec(data.len(), vec.encoding_type());
                    data.push(vec);
                    buffer
                })
                .collect::<Vec<_>>();

            let (final_sort_col_index1, final_desc) = *batch1.order_by.last().unwrap();
            let final_sort_col_index2 = batch2.order_by.last().unwrap().0;
            #[allow(clippy::branches_sharing_code)]
            let (merge_ops, merged_final_sort_col) = if batch1.order_by.len() == 1 {
                let (index1, desc) = batch1.order_by[0];
                let (index2, _) = batch2.order_by[0];
                let (left, right) = unify_types(&mut qp, left[index1], right[index2]);
                qp.merge(left, right, limit, desc)
            } else {
                let (first_sort_col_index1, desc) = batch1.order_by[0];
                let (first_sort_col_index2, _) = batch2.order_by[0];
                let (l, r) = unify_types(
                    &mut qp,
                    left[first_sort_col_index1],
                    right[first_sort_col_index2],
                );
                let mut partitioning = qp.partition(l, r, limit, desc);

                for i in 1..(left.len() - 1) {
                    let (index1, desc) = batch1.order_by[i];
                    let (index2, _) = batch1.order_by[i];
                    let (l, r) = unify_types(&mut qp, left[index1], right[index2]);
                    partitioning = qp.subpartition(partitioning, l, r, desc);
                }
                let (l, r) = unify_types(
                    &mut qp,
                    left[final_sort_col_index1],
                    right[final_sort_col_index2],
                );
                qp.merge_partitioned(partitioning, l, r, limit, batch1.order_by.last().unwrap().1)
            };

            let mut projection = Vec::new();
            for (&ileft, &iright) in batch1.projection.iter().zip(batch2.projection.iter()) {
                if ileft == final_sort_col_index1 && iright == final_sort_col_index2 {
                    projection.push(merged_final_sort_col.any());
                } else {
                    let (l, r) = unify_types(&mut qp, left[ileft], right[iright]);
                    let merged = qp.merge_keep(merge_ops, l, r);
                    projection.push(merged.any());
                }
            }
            let mut order_by = vec![];
            for (&(ileft, desc), &(iright, _)) in batch1.order_by[0..batch1.order_by.len() - 1]
                .iter()
                .zip(batch2.order_by.iter())
            {
                let (l, r) = unify_types(&mut qp, left[ileft], right[iright]);
                let merged = qp.merge_keep(merge_ops, l, r);
                order_by.push((merged.any(), desc));
            }
            order_by.push((merged_final_sort_col.any(), final_desc));

            let mut executor = qp.prepare(data)?;
            let mut results = executor.prepare_no_columns();
            executor.run(1, &mut results, batch1.show || batch2.show)?;
            let (columns, projection, _, order_by) =
                results.collect_aliased(&projection, &[], &order_by);

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
                    urb.extend(batch2.unsafe_referenced_buffers);
                    urb
                },
            })
        } else {
            // Select query
            // TODO(#97): make this work for differently aliased columns (need to send through query planner)
            ensure!(
                batch1.projection == batch2.projection,
                "Different projections in select batches ({:?}, {:?})",
                &batch1.projection,
                &batch2.projection
            );
            let mut result = Vec::with_capacity(batch1.columns.len());
            let show = batch1.show || batch2.show;
            for (mut col1, col2) in batch1.columns.into_iter().zip(batch2.columns) {
                if show {
                    println!("Merging columns");
                    println!("{}", col1.display());
                    println!("{}", col2.display());
                }
                let count = if col1.len() >= limit {
                    0
                } else {
                    min(col2.len(), limit - col1.len())
                };
                if let Some(newcol) = col1.append_all(&*col2, count) {
                    if show {
                        println!("{}", newcol.display());
                    }
                    result.push(newcol)
                } else {
                    if show {
                        println!("{}", col1.display());
                    }
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
                    urb.extend(batch2.unsafe_referenced_buffers);
                    urb
                },
            })
        }
    }
}

fn unify_types(
    qp: &mut QueryPlanner,
    mut left: TypedBufferRef,
    mut right: TypedBufferRef,
) -> (TypedBufferRef, TypedBufferRef) {
    let lub = left.tag.least_upper_bound(right.tag);
    if left.tag != lub {
        left = qp.cast(left, lub);
    }
    if right.tag != lub {
        right = qp.cast(right, lub);
    }
    (left, right)
}
