use std::collections::HashMap;
use std::collections::HashSet;
use std::iter::Iterator;
use std::sync::Arc;

use ::QueryError;
use engine::*;
use engine::aggregator::*;
use engine::batch_merging::*;
use engine::query_plan::QueryPlan;
use engine::query_plan;
use engine::types::EncodingType;
use engine::types::Type;
use ingest::raw_val::RawVal;
use mem_store::column::Column;
use syntax::expression::*;
use syntax::limit::*;


#[derive(Debug, Clone)]
pub struct Query {
    pub select: Vec<Expr>,
    pub table: String,
    pub filter: Expr,
    pub aggregate: Vec<(Aggregator, Expr)>,
    pub order_by: Option<String>,
    pub order_desc: bool,
    pub limit: LimitClause,
    pub order_by_index: Option<usize>,
}

impl Query {
    #[inline(never)] // produces more useful profiles
    pub fn run<'a>(&self, columns: &'a HashMap<String, Arc<Column>>, explain: bool, show: bool, partition: usize)
                   -> Result<(BatchResult<'a>, Option<String>), QueryError> {
        let limit = (self.limit.limit + self.limit.offset) as usize;
        let len = columns.iter().next().unwrap().1.len();
        let mut executor = QueryExecutor::default();

        let (filter_plan, filter_type) = QueryPlan::create_query_plan(&self.filter, Filter::None, columns)?;
        let mut filter = match filter_type.encoding_type() {
            EncodingType::BitVec => {
                let mut compiled_filter = query_plan::prepare(filter_plan, &mut executor);
                Filter::BitVec(compiled_filter.u8())
            }
            _ => Filter::None,
        };

        let mut select = Vec::new();
        if let Some(index) = self.order_by_index {
            let (plan, plan_t) = query_plan::order_preserving(
                QueryPlan::create_query_plan(&self.select[index], filter, columns)?);
            // TODO(clemens): Reuse sort_column for result
            let sort_column = query_plan::prepare(plan.clone(), &mut executor);
            // TODO(clemens): better criterion
            let sort_indices = if limit < len / 2 {
                query_plan::prepare(
                    QueryPlan::TopN(
                        Box::new(QueryPlan::ReadBuffer(sort_column)),
                        plan_t.encoding_type(), limit, self.order_desc),
                    &mut executor)
            } else {
                // TODO(clemens): Optimization: sort directly if only single column selected
                query_plan::prepare(
                    QueryPlan::SortIndices(
                        Box::new(QueryPlan::ReadBuffer(sort_column)),
                        self.order_desc),
                    &mut executor)
            };
            filter = Filter::Indices(sort_indices.usize());
        }
        for expr in &self.select {
            let (mut plan, plan_type) = QueryPlan::create_query_plan(expr, filter, columns)?;
            if let Some(codec) = plan_type.codec {
                plan = *codec.decode(Box::new(plan));
            }
            select.push(query_plan::prepare_no_alias(plan, &mut executor));
        }

        for c in columns {
            debug!("{}: {:?}", partition, c);
        }
        let mut results = executor.prepare(Query::column_data(columns));
        debug!("{:#}", &executor);
        executor.run(columns.iter().next().unwrap().1.len(), &mut results, show);
        let select = select.into_iter().map(|i| results.collect(i.any())).collect();

        Ok(
            (BatchResult {
                group_by: None,
                sort_by: self.order_by_index,
                select,
                desc: self.order_desc,
                aggregators: Vec::with_capacity(0),
                level: 0,
                batch_count: 1,
                show,
                unsafe_referenced_buffers: results.collect_pinned(),
            },
             if explain { Some(format!("{}", executor)) } else { None }))
    }

    #[inline(never)] // produces more useful profiles
    pub fn run_aggregate<'a>(&self,
                             columns: &'a HashMap<String, Arc<Column>>,
                             explain: bool,
                             show: bool,
                             partition: usize)
                             -> Result<(BatchResult<'a>, Option<String>), QueryError> {
        trace_start!("run_aggregate");

        let mut executor = QueryExecutor::default();

        // Filter
        let (filter_plan, filter_type) = QueryPlan::create_query_plan(&self.filter, Filter::None, columns)?;
        let filter = match filter_type.encoding_type() {
            EncodingType::BitVec => {
                let compiled_filter = query_plan::prepare(filter_plan, &mut executor);
                Filter::BitVec(compiled_filter.u8())
            }
            _ => Filter::None,
        };

        // Combine all group by columns into a single decodable grouping key
        let ((grouping_key_plan, raw_grouping_key_type),
            max_grouping_key,
            decode_plans) =
            query_plan::compile_grouping_key(&self.select, filter, columns)?;
        let raw_grouping_key = query_plan::prepare(grouping_key_plan, &mut executor);

        // Reduce cardinality of grouping key if necessary and perform grouping
        // TODO(clemens): also determine and use is_dense. always true for hashmap, depends on group by columns for raw.
        let (encoded_group_by_column,
            grouping_key,
            grouping_key_type,
            aggregation_cardinality) =
        // TODO(clemens): refine criterion
            if max_grouping_key < 1 << 16 && raw_grouping_key_type.is_positive_integer() {
                let max_grouping_key_buf = query_plan::prepare(
                    QueryPlan::Constant(RawVal::Int(max_grouping_key), true), &mut executor);
                (None,
                 raw_grouping_key,
                 raw_grouping_key_type.clone(),
                 max_grouping_key_buf.const_i64())
            } else {
                query_plan::prepare_hashmap_grouping(
                    raw_grouping_key,
                    max_grouping_key as usize,
                    &mut executor)
            };

        // Aggregators
        let mut aggregation_results = Vec::new();
        let mut selector = None;
        let mut selector_index = None;
        for (i, &(aggregator, ref expr)) in self.aggregate.iter().enumerate() {
            let (plan, plan_type) = QueryPlan::create_query_plan(expr, filter, columns)?;
            let (aggregate, t) = query_plan::prepare_aggregation(
                plan,
                plan_type,
                grouping_key,
                aggregation_cardinality,
                aggregator,
                &mut executor)?;
            // TODO(clemens): if summation column is strictly positive, can use sum as well
            if aggregator == Aggregator::Count {
                selector = Some((aggregate, t.encoding_type()));
                selector_index = Some(i)
            }
            aggregation_results.push((aggregator, aggregate, t))
        }

        // Determine selector
        let (selector, selector_type) = selector.unwrap_or_else(|| {
            let s = query_plan::prepare(
                QueryPlan::Exists(
                    Box::new(QueryPlan::ReadBuffer(grouping_key)),
                    grouping_key_type.encoding_type(),
                    Box::new(QueryPlan::ReadBuffer(aggregation_cardinality.tagged()))),
                &mut executor);
            (s, EncodingType::U8)
        });

        // Construct (encoded) group by column
        let encoded_group_by_column = encoded_group_by_column.unwrap_or_else(|| {
            query_plan::prepare(
                QueryPlan::NonzeroIndices(
                    Box::new(QueryPlan::ReadBuffer(selector)),
                    selector_type,
                    grouping_key_type.encoding_type()),
                &mut executor)
        });
        executor.set_encoded_group_by(encoded_group_by_column);

        // Compact and decode aggregation results
        let mut select = Vec::new();
        {
            let mut decode_compact = |aggregator: Aggregator,
                                      aggregate: TypedBufferRef,
                                      t: Type,
                                      select: &mut Vec<TypedBufferRef>| {
                let compacted = match aggregator {
                    // TODO(clemens): if summation column is strictly positive, can use NonzeroCompact
                    Aggregator::Sum => query_plan::prepare(
                        QueryPlan::Compact(
                            Box::new(QueryPlan::ReadBuffer(aggregate)), t.encoding_type(),
                            Box::new(QueryPlan::ReadBuffer(selector)), selector_type),
                        &mut executor),
                    Aggregator::Count => query_plan::prepare(
                        QueryPlan::NonzeroCompact(Box::new(QueryPlan::ReadBuffer(aggregate)), t.encoding_type()),
                        &mut executor),
                };
                if t.is_encoded() {
                    let decoded = query_plan::prepare(
                        *t.codec.clone().unwrap().decode(Box::new(QueryPlan::ReadBuffer(compacted))),
                        &mut executor);
                    select.push(decoded);
                } else {
                    select.push(compacted);
                }
            };

            for (i, &(aggregator, aggregate, ref t)) in aggregation_results.iter().enumerate() {
                if selector_index != Some(i) {
                    decode_compact(aggregator, aggregate, t.clone(), &mut select);
                }
            }

            // TODO(clemens): is there a simpler way to do this?
            if let Some(i) = selector_index {
                let (aggregator, aggregate, ref t) = aggregation_results[i];
                decode_compact(aggregator, aggregate, t.clone(), &mut select);
                let last = select.pop().unwrap();
                select.insert(i, last);
            }
        }

        //  Reconstruct all group by columns from grouping
        let mut grouping_columns = Vec::with_capacity(decode_plans.len());
        for (decode_plan, _t) in decode_plans {
            let decoded = query_plan::prepare_no_alias(decode_plan.clone(), &mut executor);
            grouping_columns.push(decoded);
        }

        // If the grouping is not order preserving, we need to sort all output columns by using the ordering constructed from the decoded group by columns
        // This is necessary to make it possible to efficiently merge with other batch results
        if !grouping_key_type.is_order_preserving() {
            let sort_indices = if raw_grouping_key_type.is_order_preserving() {
                query_plan::prepare(
                    QueryPlan::SortIndices(
                        Box::new(QueryPlan::ReadBuffer(encoded_group_by_column)),
                        false),
                    &mut executor)
            } else {
                if grouping_columns.len() != 1 {
                    bail!(QueryError::NotImplemented,
                        "Grouping key is not order preserving and more than 1 grouping column\nGrouping key type: {:?}\n{}",
                        &grouping_key_type,
                        &executor)
                }
                query_plan::prepare(
                    QueryPlan::SortIndices(
                        Box::new(QueryPlan::ReadBuffer(grouping_columns[0])),
                        false),
                    &mut executor)
            };

            select = select.iter().map(|s| {
                query_plan::prepare_no_alias(
                    QueryPlan::Select(
                        Box::new(QueryPlan::ReadBuffer(*s)),
                        Box::new(QueryPlan::ReadBuffer(sort_indices)),
                    ),
                    &mut executor)
            }).collect();
            grouping_columns = grouping_columns.iter().map(|s| {
                query_plan::prepare_no_alias(
                    QueryPlan::Select(
                        Box::new(QueryPlan::ReadBuffer(*s)),
                        Box::new(QueryPlan::ReadBuffer(sort_indices)),
                    ),
                    &mut executor)
            }).collect();
        }

        for c in columns {
            debug!("{}: {:?}", partition, c);
        }
        let mut results = executor.prepare(Query::column_data(columns));
        debug!("{:#}", &executor);
        executor.run(columns.iter().next().unwrap().1.len(), &mut results, show);
        let select_cols = select.iter().map(|i| results.collect(i.any())).collect();
        let group_by_cols = grouping_columns.iter().map(|i| results.collect(i.any())).collect();

        let batch = BatchResult {
            group_by: Some(group_by_cols),
            sort_by: None,
            select: select_cols,
            desc: self.order_desc,
            aggregators: self.aggregate.iter().map(|x| x.0).collect(),
            level: 0,
            batch_count: 1,
            show,
            unsafe_referenced_buffers: results.collect_pinned(),
        };
        if let Err(err) = batch.validate() {
            warn!("Query result failed validation (partition {}): {}\n{:#}\nGroup By: {:?}\nSelect: {:?}",
                  partition, err, &executor, grouping_columns, select);
            Err(err)
        } else {
            Ok((
                batch,
                if explain { Some(format!("{}", executor)) } else { None }
            ))
        }
    }

    pub fn is_select_star(&self) -> bool {
        if self.select.len() == 1 {
            match self.select[0] {
                Expr::ColName(ref colname) if colname == "*" => true,
                _ => false,
            }
        } else {
            false
        }
    }

    pub fn result_column_names(&self) -> Vec<String> {
        let mut anon_columns = -1;
        let select_cols = self.select
            .iter()
            .map(|expr| match *expr {
                Expr::ColName(ref name) => name.clone(),
                _ => {
                    anon_columns += 1;
                    format!("col_{}", anon_columns)
                }
            });
        let mut anon_aggregates = -1;
        let aggregate_cols = self.aggregate
            .iter()
            .map(|&(agg, _)| {
                anon_aggregates += 1;
                match agg {
                    Aggregator::Count => format!("count_{}", anon_aggregates),
                    Aggregator::Sum => format!("sum_{}", anon_aggregates),
                }
            });

        select_cols.chain(aggregate_cols).collect()
    }

    pub fn find_referenced_cols(&self) -> HashSet<String> {
        let mut colnames = HashSet::new();
        for expr in &self.select {
            expr.add_colnames(&mut colnames);
        }
        self.filter.add_colnames(&mut colnames);
        for &(_, ref expr) in &self.aggregate {
            expr.add_colnames(&mut colnames);
        }
        colnames
    }

    fn column_data<'a>(columns: &'a HashMap<String, Arc<Column>>)
                       -> HashMap<String, Vec<&'a AnyVec<'a>>> {
        columns.iter()
            .map(|(name, column)| (name.to_string(), column.data_sections()))
            .collect()
    }
}


