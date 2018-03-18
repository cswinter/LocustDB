use std::cmp;
use std::collections::HashMap;
use std::collections::HashSet;
use std::iter::Iterator;
use std::mem;
use std::rc::Rc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use time::precise_time_ns;

use engine::aggregation_operator::*;
use engine::aggregator::*;
use engine::batch_merging::*;
use engine::filter::Filter;
use engine::query_plan;
use engine::typed_vec::TypedVec;
use ingest::raw_val::RawVal;
use mem_store::batch::Batch;
use mem_store::column::Column;
use parser::expression::*;
use parser::limit::*;
use scheduler::*;


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

pub struct QueryTask {
    query: Query,
    batches: Vec<Batch>,
    referenced_cols: HashSet<String>,
    output_colnames: Vec<String>,
    aggregate: Vec<Aggregator>,

    // Lifetime is not actually static, but tied to the lifetime of this struct.
    // There is currently no good way to express this constraint in Rust.
    // Accessing pointers derived from unsafe_state after it has been dropped violates memory safety.
    // TODO(clemens): better encapsulate unsafety using some abstraction such as the refstruct crate.
    unsafe_state: Mutex<QueryState<'static>>,
    batch_index: AtomicUsize,
    sender: SharedSender<QueryResult>,
}

pub struct QueryState<'a> {
    completed_batches: usize,
    partial_results: Vec<BatchResult<'a>>,
    stats: QueryStats,
}

pub struct QueryResult {
    pub colnames: Vec<String>,
    pub rows: Vec<Vec<RawVal>>,
    pub stats: QueryStats,
}

const ENABLE_DETAILED_STATS: bool = false;

#[derive(Debug, Clone)]
pub struct QueryStats {
    pub runtime_ns: u64,
    pub ops: usize,
    start_time: u64,
    breakdown: HashMap<&'static str, u64>,
}

impl QueryStats {
    pub fn new() -> QueryStats {
        QueryStats {
            runtime_ns: 0,
            ops: 0,
            start_time: 0,
            breakdown: HashMap::new(),
        }
    }

    pub fn start(&mut self) {
        if ENABLE_DETAILED_STATS {
            self.start_time = precise_time_ns();
        }
    }

    pub fn record(&mut self, label: &'static str) {
        if ENABLE_DETAILED_STATS {
            let elapsed = precise_time_ns() - self.start_time;
            *self.breakdown.entry(label).or_insert(0) += elapsed;
        }
    }

    pub fn print(&self) {
        println!("Total runtime: {}ns", self.runtime_ns);
        let mut total = 0_u64;
        let mut sorted_breakdown = self.breakdown.iter().collect::<Vec<_>>();
        sorted_breakdown.sort_by_key(|&(l, _)| l);
        for (label, duration) in sorted_breakdown {
            println!("  {}: {}ns ({}%)", label, duration, duration * 100 / self.runtime_ns);
            total += *duration;
        }
        println!("  Unaccounted: {} ({}%)", self.runtime_ns - total, (self.runtime_ns - total) * 100 / self.runtime_ns)
    }
}


impl QueryTask {
    pub fn new(mut query: Query, source: Vec<Batch>, sender: SharedSender<QueryResult>) -> QueryTask {
        let start_time = precise_time_ns();
        let mut stats = QueryStats::new();

        if query.is_select_star() {
            query.select = find_all_cols(&source).into_iter().map(Expr::ColName).collect();
        }

        stats.start();
        let output_colnames = query.result_column_names();
        let mut order_by_index = None;
        if let Some(ref col) = query.order_by {
            for (i, name) in output_colnames.iter().enumerate() {
                if name == col {
                    order_by_index = Some(i);
                }
            }
        }
        query.order_by_index = order_by_index;
        stats.record(&"determine_output_colnames");

        stats.start();
        let referenced_cols = query.find_referenced_cols();
        stats.record(&"find_referenced_cols");

        let aggregate = query.aggregate.iter().map(|&(aggregate, _)| aggregate).collect();

        stats.runtime_ns = precise_time_ns() - start_time;

        QueryTask {
            query: query,
            batches: source,
            referenced_cols: referenced_cols,
            output_colnames: output_colnames,
            aggregate: aggregate,

            unsafe_state: Mutex::new(QueryState {
                partial_results: Vec::new(),
                completed_batches: 0,
                stats: stats,
            }),
            batch_index: AtomicUsize::new(0),
            sender: sender,
        }
    }

    pub fn run(&self) {
        let start_time = precise_time_ns();
        let mut stats = QueryStats::new();

        let mut batch_results = Vec::<BatchResult>::new();
        while let Some(batch) = self.next_batch() {
            let batch = Query::prepare_batch(&self.referenced_cols, batch);
            let mut batch_result = if self.aggregate.len() == 0 {
                self.query.run(&batch, &mut stats)
            } else {
                self.query.run_aggregate(&batch, &mut stats)
            };
            // Merge only with previous batch results of same level to get O(n log n) complexity
            loop {
                if !batch_results.is_empty() && batch_results.last().unwrap().level == batch_result.level {
                    // TODO(clemens): apply limit during combine when possible
                    batch_result = combine(batch_results.pop().unwrap(), batch_result);
                } else { break; }
            }
            batch_results.push(batch_result)
            /*if self.compiled_order_by.is_none() && (max_limit as usize) < combined_results.cols.len() {
                break;
            }*/
        }

        if let Some(result) = QueryTask::combine_results(batch_results) {
            self.push_result(result);
        }
        stats.runtime_ns += precise_time_ns() - start_time;
    }

    fn combine_results(batch_results: Vec<BatchResult>) -> Option<BatchResult> {
        let mut full_result = None;
        for batch_result in batch_results.into_iter() {
            if let Some(partial) = full_result {
                full_result = Some(combine(partial, batch_result));
            } else {
                full_result = Some(batch_result);
            }
        }
        full_result
    }

    fn push_result(&self, result: BatchResult) {
        let mut state = self.unsafe_state.lock().unwrap();
        state.completed_batches += result.batch_count;
        unsafe {
            let result = mem::transmute::<_, BatchResult<'static>>(result);
            state.partial_results.push(result);
        }
        if state.completed_batches == self.batches.len() {
            let mut owned_results = Vec::with_capacity(0);
            mem::swap(&mut owned_results, &mut state.partial_results);
            // TODO(clemens): Handle empty table
            let full_result = QueryTask::combine_results(owned_results).unwrap();
            let final_result = self.convert_to_output_format(full_result, &mut state.stats);
            self.sender.send(final_result);
        }
    }

    fn next_batch(&self) -> Option<&Batch> {
        let index = self.batch_index.fetch_add(1, Ordering::SeqCst);
        self.batches.get(index)
    }

    fn convert_to_output_format(&self,
                                full_result: BatchResult,
                                stats: &mut QueryStats) -> QueryResult {
        let limit = self.query.limit.limit as usize;
        let offset = self.query.limit.offset as usize;
        stats.start();
        let mut result_rows = Vec::new();
        let count = cmp::min(limit, full_result.len() - offset);
        for i in offset..(count + offset) {
            let mut record = Vec::with_capacity(self.output_colnames.len());
            if let Some(ref g) = full_result.group_by {
                record.push(g.get_raw(i));
            }
            for col in full_result.select.iter() {
                record.push(col.get_raw(i));
            }
            result_rows.push(record);
        }
        stats.record(&"limit_collect");
        QueryResult {
            colnames: self.output_colnames.clone(),
            rows: result_rows,
            stats: stats.clone(),
        }
    }
}

impl Task for QueryTask {
    fn execute(&self) { self.run(); }
    fn completed(&self) -> bool {
        let batch_index = self.batch_index.load(Ordering::SeqCst);
        batch_index >= self.batches.len()
    }
    fn multithreaded(&self) -> bool { true }
}

impl Query {
    fn prepare_batch<'a>(referenced_cols: &'a HashSet<String>, source: &'a Batch) -> HashMap<&'a str, &'a Column> {
        source.cols().iter()
            .filter(|col| referenced_cols.contains(col.name()))
            .map(|col| (col.name(), col))
            .collect()
    }

    #[inline(never)] // produces more useful profiles
    fn run<'a>(&self, columns: &HashMap<&'a str, &'a Column>, stats: &mut QueryStats) -> BatchResult<'a> {
        stats.start();
        let (filter_plan, _) = self.filter.create_query_plan(columns, Filter::None);
        //println!("filter: {:?}", filter_plan);
        // TODO(clemens): type check
        let mut compiled_filter = query_plan::prepare(filter_plan);
        stats.record(&"compile_filter");

        let mut filter = match compiled_filter.execute(stats) {
            TypedVec::Boolean(b) => Filter::BitVec(Rc::new(b)),
            _ => Filter::None,
        };

        let mut result = Vec::new();
        if let Some(index) = self.order_by_index {
            // TODO(clemens): Reuse sort_column for result
            // TODO(clemens): Optimization: sort directly if only single column selected
            let (plan, _) = self.select[index].create_query_plan(columns, filter.clone());
            let mut compiled = query_plan::prepare(plan);
            let sort_column = compiled.execute(stats).order_preserving();
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
            stats.start();
            let (plan, _) = expr.create_query_plan(columns, filter.clone());
            //println!("select: {:?}", plan);
            let mut compiled = query_plan::prepare(plan);
            stats.record(&"compile_select");
            result.push(compiled.execute(stats).decode());
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
    fn run_aggregate<'a>(&self, columns: &HashMap<&'a str, &'a Column>, stats: &mut QueryStats) -> BatchResult<'a> {
        stats.start();
        let (filter_plan, _) = self.filter.create_query_plan(columns, Filter::None);
        // TODO(clemens): type check
        let mut compiled_filter = query_plan::prepare(filter_plan);
        stats.record(&"compile_filter");

        let filter = match compiled_filter.execute(stats) {
            TypedVec::Boolean(b) => Filter::BitVec(Rc::new(b)),
            _ => Filter::None,
        };

        stats.start();
        let (grouping_key_plan, _) = Expr::compile_grouping_key(&self.select, columns, filter.clone());
        let mut compiled_gk = query_plan::prepare(grouping_key_plan);
        stats.record(&"compile_grouping_key");
        let grouping_key = compiled_gk.execute(stats);
        let (grouping, max_index, groups) = grouping(grouping_key);
        let groups = groups.order_preserving();
        let mut grouping_sort_indices = (0..groups.len()).collect();
        groups.sort_indices_asc(&mut grouping_sort_indices);

        let mut result = Vec::new();
        for &(aggregator, ref expr) in &self.aggregate {
            stats.start();
            let (plan, _) = expr.create_query_plan(columns, filter.clone());
            let mut compiled = query_plan::prepare_aggregation(plan, &grouping, max_index, aggregator);
            stats.record(&"compile_aggregate");
            result.push(compiled.execute(stats).index_decode(&grouping_sort_indices));
        }

        BatchResult {
            group_by: Some(groups.index_decode(&grouping_sort_indices)),
            sort_by: None,
            select: result,
            aggregators: self.aggregate.iter().map(|x| x.0).collect(),
            level: 0,
            batch_count: 1,
        }
    }

    fn is_select_star(&self) -> bool {
        if self.select.len() == 1 {
            match self.select[0] {
                Expr::ColName(ref colname) if **colname == "*".to_string() => true,
                _ => false,
            }
        } else {
            false
        }
    }

    fn result_column_names(&self) -> Vec<String> {
        let mut anon_columns = -1;
        let select_cols = self.select
            .iter()
            .map(|expr| match expr {
                &Expr::ColName(ref name) => name.clone(),
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


    fn find_referenced_cols(&self) -> HashSet<String> {
        let mut colnames = HashSet::new();
        for expr in self.select.iter() {
            expr.add_colnames(&mut colnames);
        }
        self.filter.add_colnames(&mut colnames);
        for &(_, ref expr) in self.aggregate.iter() {
            expr.add_colnames(&mut colnames);
        }
        colnames
    }
}

fn find_all_cols(source: &Vec<Batch>) -> Vec<String> {
    let mut cols = HashSet::new();
    for batch in source {
        for column in batch.cols() {
            cols.insert(column.name().to_string());
        }
    }

    cols.into_iter().collect()
}
