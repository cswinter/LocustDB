use std::collections::HashMap;
use std::collections::HashSet;
use std::iter::Iterator;
use std::rc::Rc;

use engine::aggregation_operator::*;
use engine::aggregator::*;
use engine::batch_merging::*;
use engine::filter::Filter;
use engine::query_plan::QueryPlan;
use engine::query_plan;
use engine::typed_vec::TypedVec;
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
    pub fn run<'a>(&self, columns: &HashMap<&'a str, &'a Column>) -> BatchResult<'a> {
        let (filter_plan, _) = QueryPlan::create_query_plan(&self.filter, columns, Filter::None);
        // println!("filter: {:?}", filter_plan);
        // TODO(clemens): type check
        let mut compiled_filter = query_plan::prepare(filter_plan);
        let mut filter = match compiled_filter.execute() {
            TypedVec::Boolean(b) => Filter::BitVec(Rc::new(b)),
            _ => Filter::None,
        };

        let mut result = Vec::new();
        if let Some(index) = self.order_by_index {
            // TODO(clemens): Reuse sort_column for result
            // TODO(clemens): Optimization: sort directly if only single column selected
            let (plan, _) = QueryPlan::create_query_plan(&self.select[index], columns, filter.clone());
            let mut compiled = query_plan::prepare(plan);
            let sort_column = compiled.execute().order_preserving();
            let mut sort_indices = match filter {
                Filter::BitVec(vec) => vec.iter()
                    .enumerate()
                    .filter(|x| x.1)
                    .map(|x| x.0)
                    .collect(),
                Filter::None => (0..sort_column.len()).collect(),
                _ => panic!("surely this will never happen :)"),
            };
            if self.order_desc {
                sort_column.sort_indices_desc(&mut sort_indices);
            } else {
                sort_column.sort_indices_asc(&mut sort_indices);
            }
            sort_indices.truncate((self.limit.limit + self.limit.offset) as usize);
            filter = Filter::Indices(Rc::new(sort_indices));
        }
        for expr in &self.select {
            let (plan, _) = QueryPlan::create_query_plan(expr, columns, filter.clone());
            //println!("select: {:?}", plan);
            let mut compiled = query_plan::prepare(plan);
            result.push(compiled.execute().decode());
        }

        BatchResult {
            group_by: None,
            sort_by: self.order_by_index,
            select: result,
            aggregators: Vec::with_capacity(0),
            level: 0,
            batch_count: 1,
        }
    }

    #[inline(never)] // produces more useful profiles
    pub fn run_aggregate<'a>(&self, columns: &HashMap<&'a str, &'a Column>) -> BatchResult<'a> {
        trace_start!("run_aggregate");
        trace_start!("filter");
        let (filter_plan, _) = QueryPlan::create_query_plan(&self.filter, columns, Filter::None);

        // TODO(clemens): type check
        let mut compiled_filter = query_plan::prepare(filter_plan);

        let filter = match compiled_filter.execute() {
            TypedVec::Boolean(b) => Filter::BitVec(Rc::new(b)),
            _ => Filter::None,
        };

        trace_replace!("grouping_key");
        let (grouping_key_plan, _) = QueryPlan::compile_grouping_key(&self.select, columns, filter.clone());
        let mut compiled_gk = query_plan::prepare(grouping_key_plan);
        let grouping_key = compiled_gk.execute();
        let (grouping, max_index, groups) = grouping(grouping_key);
        let groups = groups.order_preserving();

        trace_replace!("group_ordering");
        let mut grouping_sort_indices = (0..groups.len()).collect();
        groups.sort_indices_asc(&mut grouping_sort_indices);

        trace_replace!("aggregate");
        let mut result = Vec::new();
        for &(aggregator, ref expr) in &self.aggregate {
            trace_start!("aggregator {:?}", aggregator);
            let (plan, _) = QueryPlan::create_query_plan(expr, columns, filter.clone());
            let mut compiled = query_plan::prepare_aggregation(plan, &grouping, max_index, aggregator);
            result.push(compiled.execute().index_decode(&grouping_sort_indices));
        }

        trace_replace!("final decode");
        BatchResult {
            group_by: Some(groups.index_decode(&grouping_sort_indices)),
            sort_by: None,
            select: result,
            aggregators: self.aggregate.iter().map(|x| x.0).collect(),
            level: 0,
            batch_count: 1,
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


