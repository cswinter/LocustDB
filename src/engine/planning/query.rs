use crate::engine::*;
use crate::ingest::raw_val::RawVal;
use crate::mem_store::column::DataSource;
use crate::syntax::expression::*;
use crate::syntax::limit::*;
use std::collections::HashMap;
use std::collections::HashSet;
use std::ops::Range;
use std::sync::Arc;
use std::u64;

#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub expr: Expr,
    pub name: String,
}

/// NormalFormQuery observes the following invariants:
/// - none of the expressions contain aggregation functions
/// - if aggregate.len() > 0 then order_by.len() == 0 and vice versa
#[derive(Debug, Clone)]
pub struct NormalFormQuery {
    // Every projection is an expression that does not contain any aggregation functions
    pub projection: Vec<ColumnInfo>,
    // Every aggregation is an expression with a top level aggregation function
    pub aggregate: Vec<(Aggregator, ColumnInfo)>,
    pub filter: Expr,
    pub order_by: Vec<(Expr, bool)>,
    pub limit: LimitClause,
}

#[derive(Debug, Clone)]
pub struct Query {
    pub select: Vec<ColumnInfo>,
    pub table: String,
    pub filter: Expr,
    pub order_by: Vec<(Expr, bool)>,
    pub limit: LimitClause,
}

#[derive(Debug, Clone)]
pub enum ResultColumn {
    Proj(usize),
    Agg(usize),
}

impl NormalFormQuery {
    #[inline(never)] // produces more useful profiles
    pub fn run<'a>(
        &self,
        columns: &'a HashMap<String, Arc<dyn DataSource>>,
        explain: bool,
        show: bool,
        partition: usize,
        partition_range: Range<usize>,
        batch_size: usize,
    ) -> Result<(BatchResult<'a>, Option<String>), QueryError> {
        let limit = (self.limit.limit + self.limit.offset) as usize;
        let mut planner = QueryPlanner::default();

        let (filter_plan, _) = QueryPlan::compile_expr(
            &self.filter,
            Filter::None,
            columns,
            partition_range.len(),
            &mut planner,
        )?;
        let mut filter = match filter_plan.tag {
            EncodingType::U8 => Filter::U8(filter_plan.u8()?),
            EncodingType::NullableU8 => Filter::NullableU8(filter_plan.nullable_u8()?),
            _ => Filter::None,
        };

        // Sorting
        let mut sort_indices = None;
        for (plan, desc) in self.order_by.iter().rev() {
            let (ranking, _) = query_plan::order_preserving(
                QueryPlan::compile_expr(
                    plan,
                    filter,
                    columns,
                    partition_range.len(),
                    &mut planner,
                )?,
                &mut planner,
            );

            // PERF: better criterion for using top_n
            // PERF: top_n for multiple columns?
            // TODO: efficient PERF top_n for null or constant vec (construct indices of size min(ranking.len(), limit))
            let indices = if limit < partition_range.len() / 2
                && self.order_by.len() == 1
                && !ranking.is_constant()
            {
                let ranking = if ranking.is_nullable() {
                    // TODO: not implemented for all types (e.g. NullableU8). Need to upcast to u64, add corresponding fused types, or add nullable top_n
                    planner.fuse_nulls(ranking)
                } else {
                    ranking
                };
                planner.top_n(ranking, limit, *desc)
            } else {
                // PERF: sort directly if only single column selected
                let indices = sort_indices.unwrap_or_else(|| planner.indices(ranking));
                if ranking.is_constant() {
                    indices
                } else {
                    planner.sort_by(ranking, indices, *desc, true /* stable sort */)
                }
            };
            sort_indices = Some(indices)
        }
        if let Some(sort_indices) = sort_indices {
            filter = match filter {
                Filter::U8(where_true) => {
                    let buffer = planner.null_vec(partition_range.len(), EncodingType::Null);
                    let indices = planner.indices(buffer).into();
                    let filter = planner.filter(indices, where_true);
                    Filter::Indices(planner.select(filter, sort_indices).usize()?)
                }
                Filter::NullableU8(where_true) => {
                    let buffer = planner.null_vec(partition_range.len(), EncodingType::Null);
                    let indices = planner.indices(buffer).into();
                    let filter = planner.nullable_filter(indices, where_true);
                    Filter::Indices(planner.select(filter, sort_indices).usize()?)
                }
                Filter::None => Filter::Indices(sort_indices),
                Filter::Indices(_) => unreachable!(),
            };
        }

        let mut select = Vec::new();
        for col_info in &self.projection {
            let (mut plan, plan_type) = QueryPlan::compile_expr(
                &col_info.expr,
                filter,
                columns,
                partition_range.len(),
                &mut planner,
            )?;
            plan = plan_type.codec.decode(plan, &mut planner);
            // TODO(perf): use more efficient solution than fuse_nulls for nullable columns (mostly requires better support in batch_merging)
            if plan.is_nullable() {
                plan = planner.cast(plan, EncodingType::Val);
            }
            plan = planner.collect(plan, &col_info.name.clone());
            select.push(plan.any());
        }
        let mut order_by = Vec::new();
        for (i, (expr, desc)) in self.order_by.iter().enumerate() {
            let (mut plan, plan_type) = QueryPlan::compile_expr(
                expr,
                filter,
                columns,
                partition_range.len(),
                &mut planner,
            )?;
            plan = plan_type.codec.decode(plan, &mut planner);
            if plan.is_nullable() {
                plan = planner.fuse_nulls(plan);
            }
            plan = planner.collect(plan, &format!("order_by_{}", i));
            order_by.push((plan.any(), *desc));
        }

        for c in columns {
            debug!("{}: {:?}", partition, c);
        }
        let mut executor = planner.prepare(vec![], batch_size, show)?;
        let mut results = executor.prepare(NormalFormQuery::column_data(columns));
        debug!("{:#}", &executor);
        executor.run(partition_range.len(), &mut results, show)?;
        let (columns, projection, _, order_by) = results.collect_aliased(&select, &[], &order_by);

        Ok((
            BatchResult {
                columns,
                projection,
                aggregations: vec![],
                order_by,
                level: 0,
                scanned_range: partition_range,
                batch_count: 1,
                show,
                unsafe_referenced_buffers: results.collect_pinned(),
            },
            if explain {
                Some(format!("{}", executor))
            } else {
                None
            },
        ))
    }

    #[inline(never)] // produces more useful profiles
    pub fn run_aggregate<'a>(
        &self,
        columns: &'a HashMap<String, Arc<dyn DataSource>>,
        explain: bool,
        show: bool,
        partition: usize,
        partition_range: Range<usize>,
        batch_size: usize,
    ) -> Result<(BatchResult<'a>, Option<String>), QueryError> {
        let mut qp = QueryPlanner::default();

        // Filter
        let (filter_plan, filter_type) = QueryPlan::compile_expr(
            &self.filter,
            Filter::None,
            columns,
            partition_range.len(),
            &mut qp,
        )?;
        let filter = match filter_type.encoding_type() {
            EncodingType::U8 => Filter::U8(filter_plan.u8()?),
            EncodingType::NullableU8 => Filter::NullableU8(filter_plan.nullable_u8()?),
            _ => Filter::None,
        };

        // Combine all group by columns into a single decodable grouping key
        let group_by_plan = query_plan::compile_grouping_key(
            &self
                .projection
                .iter()
                .map(|col_info| col_info.expr.clone())
                .collect::<Vec<_>>(),
            filter,
            columns,
            partition_range.len(),
            &mut qp,
        )?;

        // Reduce cardinality of grouping key if necessary and perform grouping
        // PERF: also determine and use is_dense. always true for hashmap, depends on group by columns for raw.
        let (encoded_group_by_column,
            grouping_key,
            is_grouping_key_order_preserving,
            aggregation_cardinality) =
        // PERF: refine criterion
            if group_by_plan.max < 1 << 16 {
                let max_grouping_key_buf = qp.scalar_i64(group_by_plan.max, true);
                (None,
                 group_by_plan.raw_grouping_key,
                 group_by_plan.is_raw_grouping_key_order_preserving,
                 max_grouping_key_buf)
            } else {
                query_plan::prepare_hashmap_grouping(
                    group_by_plan.raw_grouping_key,
                    group_by_plan.decode_plans.len(),
                    group_by_plan.max as usize,
                    &mut qp)?
            };

        // Aggregators
        let mut aggregation_results = Vec::new();
        let mut selector = None;
        let mut selector_index = None;
        for (i, &(aggregator, ref col_info)) in self.aggregate.iter().enumerate() {
            let (plan, plan_type) = QueryPlan::compile_expr(
                &col_info.expr,
                filter,
                columns,
                partition_range.len(),
                &mut qp,
            )?;
            let (aggregate, t) = query_plan::prepare_aggregation(
                plan,
                plan_type,
                grouping_key,
                aggregation_cardinality,
                aggregator,
                &mut qp,
            )?;
            // PERF: if summation column is strictly positive, can use sum as well
            if aggregator == Aggregator::Count && !plan.is_nullable() {
                selector = Some((aggregate, t.encoding_type()));
                selector_index = Some(i)
            }
            aggregation_results.push((aggregator, aggregate, t, plan.is_nullable()))
        }

        // Determine selector
        let selector = match selector {
            None => qp.exists(grouping_key, aggregation_cardinality).into(),
            Some(x) => x.0,
        };

        // Construct (encoded) group by column
        let encoded_group_by_column = match encoded_group_by_column {
            None => qp.nonzero_indices(selector, grouping_key.tag),
            Some(x) => x,
        };
        qp.connect(encoded_group_by_column, group_by_plan.encoded_group_by_placeholder);

        // Compact and decode aggregation results
        let mut aggregation_cols = Vec::new();
        {
            let mut decode_compact = |aggregator: Aggregator,
                                      aggregate: TypedBufferRef,
                                      t: Type,
                                      input_nullable: bool| {
                let compacted = match aggregator {
                    // PERF: if summation column is strictly positive, can use NonzeroCompact
                    Aggregator::SumI64
                    | Aggregator::MaxI64
                    | Aggregator::MinI64
                    | Aggregator::SumF64
                    | Aggregator::MaxF64
                    | Aggregator::MinF64 => qp.compact(aggregate, selector),
                    Aggregator::Count => {
                        if input_nullable {
                            qp.compact(aggregate, selector)
                        } else {
                            qp.nonzero_compact(aggregate)
                        }
                    }
                };
                if t.is_encoded() {
                    Ok(t.codec.decode(compacted, &mut qp))
                } else {
                    Ok(compacted)
                }
            };

            for (i, &(aggregator, aggregate, ref t, input_nullable)) in
                aggregation_results.iter().enumerate()
            {
                if selector_index != Some(i) {
                    let decode_compacted =
                        decode_compact(aggregator, aggregate, t.clone(), input_nullable)?;
                    let aggregator = if aggregate.tag == EncodingType::F64 {
                        match aggregator {
                            Aggregator::SumI64 => Aggregator::SumF64,
                            Aggregator::MaxI64 => Aggregator::MaxF64,
                            Aggregator::MinI64 => Aggregator::MinF64,
                            _ => aggregator,
                        }
                    } else {
                        aggregator
                    };
                    aggregation_cols.push((decode_compacted, aggregator))
                }
            }

            // There is probably a simpler way to do this
            if let Some(i) = selector_index {
                let (aggregator, aggregate, ref t, input_nullable) = aggregation_results[i];
                let selector = decode_compact(aggregator, aggregate, t.clone(), input_nullable)?;
                aggregation_cols.insert(i, (selector, aggregator));
            }
        }

        //  Reconstruct all group by columns from grouping
        let mut grouping_columns = Vec::with_capacity(group_by_plan.decode_plans.len());
        for (decode_plan, _t) in group_by_plan.decode_plans {
            grouping_columns.push(decode_plan);
        }

        // If the grouping is not order preserving, we need to sort all output columns by using the ordering constructed from the decoded group by columns
        // This is necessary to make it possible to efficiently merge with other batch results
        if !is_grouping_key_order_preserving {
            let sort_indices = if group_by_plan.is_raw_grouping_key_order_preserving {
                let indices = qp.indices(encoded_group_by_column);
                qp.sort_by(
                    encoded_group_by_column,
                    indices,
                    false, /* desc */
                    false, /* stable */
                )
            } else {
                if grouping_columns.len() != 1 {
                    bail!(QueryError::NotImplemented,
                        "Grouping key is not order preserving and more than 1 grouping column\nGrouping key type: {:?}\nTODO: PLANNER",
                        &grouping_key.tag)
                }
                let indices = qp.indices(grouping_columns[0]);
                qp.sort_by(
                    grouping_columns[0],
                    indices,
                    false, /* desc */
                    false, /* stable */
                )
            };

            let mut aggregations2 = Vec::new();
            for &(a, aggregator) in &aggregation_cols {
                let plan = qp.select(a, sort_indices);
                aggregations2.push((plan, aggregator));
            }
            aggregation_cols = aggregations2;

            let mut grouping_columns2 = Vec::new();
            for s in &grouping_columns {
                grouping_columns2.push(qp.select(*s, sort_indices));
            }
            grouping_columns = grouping_columns2;
        }

        for (i, plan) in grouping_columns.iter_mut().enumerate() {
            if plan.is_nullable() {
                *plan = qp.fuse_nulls(*plan);
            }
            *plan = qp.collect(*plan, &format!("grouping_{}", i));
        }
        for (i, (plan, _)) in aggregation_cols.iter_mut().enumerate() {
            if plan.is_nullable() {
                *plan = qp.fuse_nulls(*plan);
            }
            *plan = qp.collect(*plan, &format!("aggregation_{}", i));
        }

        for c in columns {
            debug!("{}: {:?}", partition, c);
        }
        let mut executor = qp.prepare(vec![], batch_size, show)?;
        let mut results = executor.prepare(NormalFormQuery::column_data(columns));
        debug!("{:#}", &executor);
        executor.run(partition_range.len(), &mut results, show)?;
        let (columns, projection, aggregations, _) = results.collect_aliased(
            &grouping_columns.iter().map(|s| s.any()).collect::<Vec<_>>(),
            &aggregation_cols
                .iter()
                .map(|&(s, aggregator)| (s.any(), aggregator))
                .collect::<Vec<_>>(),
            &[],
        );

        let batch = BatchResult {
            columns,
            projection,
            aggregations,
            order_by: vec![],
            level: 0,
            scanned_range: partition_range,
            batch_count: 1,
            show,
            unsafe_referenced_buffers: results.collect_pinned(),
        };
        if let Err(err) = batch.validate() {
            warn!("Query result failed validation (partition {}): {}\n{:#}\nGroup By: {:?}\nSelect: {:?}",
                  partition, err, &executor, grouping_columns, aggregation_cols);
            Err(err)
        } else {
            Ok((
                batch,
                if explain {
                    Some(format!("{}", executor))
                } else {
                    None
                },
            ))
        }
    }

    fn column_data(
        columns: &HashMap<String, Arc<dyn DataSource>>,
    ) -> HashMap<String, Vec<&dyn Data>> {
        columns
            .iter()
            .map(|(name, column)| (name.to_string(), column.data_sections()))
            .collect()
    }
}

impl Query {
    pub fn normalize(
        &self,
    ) -> Result<(NormalFormQuery, Option<NormalFormQuery>, Vec<ResultColumn>), QueryError> {
        let mut final_projection = Vec::<ColumnInfo>::new();
        let mut select = Vec::<ColumnInfo>::new();
        let mut aggregate = Vec::new();
        let mut aggregate_colnames = Vec::new();
        let mut select_colnames = Vec::new();
        let mut final_select_ordering = Vec::new();
        for col_info in &self.select {
            let (full_expr, aggregates) = Query::extract_aggregators(
                &col_info.expr,
                &mut aggregate_colnames,
                &col_info.name,
            )?;
            if aggregates.is_empty() {
                final_select_ordering.push(ResultColumn::Proj(select.len()));
                let column_name = format!("_cs{}", select_colnames.len());
                select_colnames.push(column_name.clone());
                select.push(ColumnInfo {
                    expr: full_expr,
                    name: col_info.name.clone(),
                });
                final_projection.push(ColumnInfo {
                    expr: Expr::ColName(column_name),
                    name: col_info.name.clone(),
                });
            } else {
                final_select_ordering.push(ResultColumn::Agg(aggregate.len()));
                aggregate.extend(aggregates);
                final_projection.push(ColumnInfo {
                    expr: full_expr,
                    name: col_info.name.clone(),
                });
            }
        }

        // A projection contains a non-trivial expression that contains an aggregate (e.g. SUM(x)/COUNT(1) or SUM(x) + 4)
        let nontrivial_aggregate_expression = final_projection
            .iter()
            .any(|col_info| !matches!(col_info.expr, Expr::ColName(_)));
        let sort_after_aggregation = !aggregate.is_empty() && !self.order_by.is_empty();
        let require_final_pass = sort_after_aggregation || nontrivial_aggregate_expression;
        Ok(if require_final_pass {
            let mut final_order_by = Vec::new();
            for (expr, desc) in &self.order_by {
                let (full_expr, aggregates) =
                    Query::extract_aggregators(expr, &mut aggregate_colnames, "INTERMEDIARY_COL")?;
                if aggregates.is_empty() {
                    let column_name = format!("_cs{}", select_colnames.len());
                    select_colnames.push(column_name.clone());
                    select.push(ColumnInfo {
                        expr: full_expr,
                        name: column_name.to_string(),
                    });
                    final_order_by.push((Expr::ColName(column_name), *desc));
                } else {
                    aggregate.extend(aggregates);
                    final_order_by.push((full_expr, *desc));
                }
            }
            let final_projection_len = final_projection.len();
            (
                NormalFormQuery {
                    projection: select,
                    aggregate,
                    filter: self.filter.clone(),
                    order_by: vec![],
                    limit: LimitClause {
                        limit: u64::MAX,
                        offset: 0,
                    },
                },
                Some(NormalFormQuery {
                    projection: final_projection,
                    aggregate: vec![],
                    filter: Expr::Const(RawVal::Int(1)),
                    order_by: final_order_by,
                    limit: self.limit.clone(),
                }),
                (0..final_projection_len).map(ResultColumn::Proj).collect(),
            )
        } else {
            (
                NormalFormQuery {
                    projection: select,
                    aggregate,
                    filter: self.filter.clone(),
                    order_by: self.order_by.clone(),
                    limit: self.limit.clone(),
                },
                None,
                final_select_ordering,
            )
        })
    }

    // Finds any aggregator expressions contained within this expression and returns them
    // For example in SELECT a + SUM(b) + COUNT(1) FROM ..., the SUM(b) and COUNT(1) are returned
    pub fn extract_aggregators(
        expr: &Expr,
        column_names: &mut Vec<String>,
        alias: &str,
    ) -> Result<(Expr, Vec<(Aggregator, ColumnInfo)>), QueryError> {
        Ok(match expr {
            Expr::Aggregate(aggregator, expr) => {
                let column_name = format!("_ca{}", column_names.len());
                column_names.push(column_name.clone());
                Query::ensure_no_aggregates(expr)?;
                (
                    Expr::ColName(column_name),
                    vec![(
                        *aggregator,
                        ColumnInfo {
                            expr: *expr.clone(),
                            name: alias.to_string(),
                        },
                    )],
                )
            }
            Expr::Func1(t, expr) => {
                let (expr, aggregates) = Query::extract_aggregators(expr, column_names, alias)?;
                (Expr::Func1(*t, Box::new(expr)), aggregates)
            }
            Expr::Func2(t, expr1, expr2) => {
                let (expr1, mut aggregates1) =
                    Query::extract_aggregators(expr1, column_names, alias)?;
                let (expr2, aggregates2) = Query::extract_aggregators(expr2, column_names, alias)?;
                aggregates1.extend(aggregates2);
                (
                    Expr::Func2(*t, Box::new(expr1), Box::new(expr2)),
                    aggregates1,
                )
            }
            Expr::Const(_) | Expr::ColName(_) => (expr.clone(), vec![]),
        })
    }

    pub fn ensure_no_aggregates(expr: &Expr) -> Result<(), QueryError> {
        match expr {
            Expr::Aggregate(_, _) => {
                bail!(QueryError::TypeError, "Nested aggregates found.")
            }
            Expr::Func1(_, expr) => {
                Query::ensure_no_aggregates(expr)?;
            }
            Expr::Func2(_, expr1, expr2) => {
                Query::ensure_no_aggregates(expr1)?;
                Query::ensure_no_aggregates(expr2)?;
            }
            Expr::Const(_) | Expr::ColName(_) => (),
        };
        Ok(())
    }

    pub fn is_select_star(&self) -> bool {
        if self.select.len() == 1 {
            matches!(self.select[0].expr, Expr::ColName(ref colname) if colname == "*")
        } else {
            false
        }
    }

    pub fn find_referenced_cols(&self) -> HashSet<String> {
        let mut colnames = HashSet::new();
        for col_info in &self.select {
            col_info.expr.add_colnames(&mut colnames);
        }
        for expr in &self.order_by {
            expr.0.add_colnames(&mut colnames);
        }
        self.filter.add_colnames(&mut colnames);
        colnames
    }

    pub fn read_column(table: &str, column: &str) -> Query {
        Query {
            select: vec![ColumnInfo {
                expr: Expr::ColName(column.to_string()),
                name: "col".to_string(),
            }],
            table: table.to_string(),
            filter: Expr::Const(RawVal::Int(1)),
            order_by: vec![],
            limit: LimitClause {
                limit: u64::MAX,
                offset: 0,
            },
        }
    }
}
