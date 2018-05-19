use std::collections::HashMap;
use std::collections::HashSet;
use std::iter::Iterator;

use ::QueryError;
use engine::aggregator::*;
use engine::batch_merging::*;
use engine::filter::Filter;
use engine::query_plan::{QueryPlan, QueryExecutor};
use engine::query_plan;
use engine::types::EncodingType;
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
    pub fn run<'a>(&self, columns: &HashMap<&'a str, &'a Column>) -> Result<BatchResult<'a>, QueryError> {
        let mut executor = QueryExecutor::default();

        let (filter_plan, filter_type) = QueryPlan::create_query_plan(&self.filter, columns)?;
        match filter_type.encoding_type() {
            EncodingType::BitVec => {
                let mut compiled_filter = query_plan::prepare(filter_plan, &mut executor);
                executor.set_filter(Filter::BitVec(compiled_filter));
            }
            _ => {}
        }

        let mut select = Vec::new();
        if let Some(index) = self.order_by_index {
            // TODO(clemens): Reuse sort_column for result
            // TODO(clemens): Optimization: sort directly if only single column selected
            let (plan, _) = query_plan::order_preserving(
                QueryPlan::create_query_plan(&self.select[index], columns)?);
            let sort_column = query_plan::prepare(plan.clone(), &mut executor);
            let sort_indices = query_plan::prepare(
                QueryPlan::SortIndices(
                    Box::new(QueryPlan::ReadBuffer(sort_column)),
                    self.order_desc),
                &mut executor);
            executor.new_stage();
            executor.set_filter(Filter::Indices(sort_indices));
        }
        for expr in &self.select {
            let (mut plan, plan_type) = QueryPlan::create_query_plan(expr, columns)?;
            if let Some(codec) = plan_type.codec {
                plan = QueryPlan::DecodeWith(Box::new(plan), codec);
            }
            select.push(query_plan::prepare(plan, &mut executor));
        }

        //println!("{}", &executor);
        let mut results = executor.run();
        let select = select.into_iter().map(|i| results.collect(i)).collect();

        Ok(BatchResult {
            group_by: None,
            sort_by: self.order_by_index,
            select,
            aggregators: Vec::with_capacity(0),
            level: 0,
            batch_count: 1,
        })
    }

    #[inline(never)] // produces more useful profiles
    pub fn run_aggregate<'a>(&self, columns: &HashMap<&'a str, &'a Column>) -> Result<BatchResult<'a>, QueryError> {
        trace_start!("run_aggregate");

        let mut executor = QueryExecutor::default();

        let (filter_plan, filter_type) = QueryPlan::create_query_plan(&self.filter, columns)?;
        match filter_type.encoding_type() {
            EncodingType::BitVec => {
                let mut compiled_filter = query_plan::prepare(filter_plan, &mut executor);
                executor.set_filter(Filter::BitVec(compiled_filter));
            }
            _ => {}
        }

        let (grouping_key_plan, grouping_key_type, max_grouping_key, decode_plans) =
            QueryPlan::compile_grouping_key(&self.select, columns)?;
        let raw_grouping_key = query_plan::prepare(grouping_key_plan, &mut executor);

        let (encoded_group_by_column, grouping_key, _aggregation_cardinality) =
        // TODO(clemens): refine criterion
        // TODO(clemens): can often collect group_by from non-zero positions in aggregation result
            if max_grouping_key < 1 << 16 && grouping_key_type.is_positive_integer() {
                let max_grouping_key_buf = executor.new_buffer();
                (query_plan::prepare_unique(
                    raw_grouping_key,
                    grouping_key_type.encoding_type(),
                    max_grouping_key as usize,
                    &mut executor),
                 raw_grouping_key,
                 max_grouping_key_buf)
            } else {
                query_plan::prepare_hashmap_grouping(
                    raw_grouping_key,
                    grouping_key_type.encoding_type(),
                    max_grouping_key as usize,
                    &mut executor)
            };

        executor.set_encoded_group_by(encoded_group_by_column);
        // TODO(clemens): fix for multiple groups
        // let groups = groups.order_preserving();

        let mut aggregation_results = Vec::new();
        for &(aggregator, ref expr) in &self.aggregate {
            trace_start!("aggregator {:?}", aggregator);
            let (plan, plan_type) = QueryPlan::create_query_plan(expr, columns)?;
            // TODO(clemens): Use more precise aggregation_cardinality instead of max_grouping_key
            let mut aggregate = query_plan::prepare_aggregation(
                plan,
                plan_type,
                grouping_key,
                grouping_key_type.encoding_type(),
                max_grouping_key as usize,
                aggregator,
                &mut executor)?;
            aggregation_results.push(aggregate)
            // TODO(clemens): renable
            // result.push(compiled.execute().index_decode(&grouping_sort_indices));
        }
        let mut select = Vec::new();
        for (aggregate, t) in aggregation_results {
            if t.is_encoded() {
                let decoded = query_plan::prepare(
                    QueryPlan::DecodeWith(
                        Box::new(QueryPlan::ReadBuffer(aggregate)),
                        t.codec.unwrap()), &mut executor);
                select.push(decoded);
            } else {
                select.push(aggregate);
            }
        }

        trace_replace!("decode grouping_key");
        let mut grouping_columns = Vec::with_capacity(decode_plans.len());
        for decode_plan in decode_plans {
            let decoded = query_plan::prepare(decode_plan.clone(), &mut executor);
            // TODO(clemens): renable
            // .index_decode(&grouping_sort_indices);
            grouping_columns.push(decoded);
        }

        // TODO(clemens): sort if not already in sort order
        /*let mut grouping_sort_indices = QueryPlan::create_query_plan(
            QueryPlan::SortIndices(
                Box::new(QueryPlan::ReadBuffer(group_by_column)),
                (self.limit.limit + self.limit.offset) as usize,
                false);
        executor.new_stage();
        executor.set_filter(Filter::Indices(sort_indices));*/

        let mut results = executor.run();
        let select_cols = select.iter().map(|&i| results.collect(i)).collect();
        let group_by_cols = grouping_columns.iter().map(|&i| results.collect(i)).collect();

        trace_replace!("final decode");
        let batch = BatchResult {
            group_by: Some(group_by_cols),
            sort_by: None,
            select: select_cols,
            aggregators: self.aggregate.iter().map(|x| x.0).collect(),
            level: 0,
            batch_count: 1,
        };
        if let Err(err) = batch.validate() {
            error!("Query result failed validation: {}\n{}\nGroup By:{:?}\nSelect: Write {:?}", err, &executor, grouping_columns, select);
            Err(err)
        } else {
            Ok(batch)
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
}


