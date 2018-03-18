use std::cmp;
use std::collections::HashMap;
use std::collections::HashSet;
use std::iter::Iterator;
use std::mem;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use engine::aggregator::*;
use engine::batch_merging::*;
use engine::query::Query;
use ingest::raw_val::RawVal;
use mem_store::batch::Batch;
use mem_store::column::Column;
use scheduler::*;
use syntax::expression::*;
use time::precise_time_ns;


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

impl Default for QueryStats {
    fn default() -> QueryStats {
        QueryStats {
            runtime_ns: 0,
            ops: 0,
            start_time: 0,
            breakdown: HashMap::new(),
        }
    }
}


impl QueryTask {
    pub fn new(mut query: Query, source: Vec<Batch>, sender: SharedSender<QueryResult>) -> QueryTask {
        let start_time = precise_time_ns();
        let mut stats = QueryStats::default();

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
        stats.record("determine_output_colnames");

        stats.start();
        let referenced_cols = query.find_referenced_cols();
        stats.record("find_referenced_cols");

        let aggregate = query.aggregate.iter().map(|&(aggregate, _)| aggregate).collect();

        stats.runtime_ns = precise_time_ns() - start_time;

        QueryTask {
            query,
            batches: source,
            referenced_cols,
            output_colnames,
            aggregate,

            unsafe_state: Mutex::new(QueryState {
                partial_results: Vec::new(),
                completed_batches: 0,
                stats,
            }),
            batch_index: AtomicUsize::new(0),
            sender,
        }
    }

    pub fn run(&self) {
        let start_time = precise_time_ns();
        let mut stats = QueryStats::default();

        let mut batch_results = Vec::<BatchResult>::new();
        while let Some(batch) = self.next_batch() {
            let batch = QueryTask::prepare_batch(&self.referenced_cols, batch);
            let mut batch_result = if self.aggregate.is_empty() {
                self.query.run(&batch, &mut stats)
            } else {
                self.query.run_aggregate(&batch, &mut stats)
            };
            // Merge only with previous batch results of same level to get O(n log n) complexity
            loop {
                if !batch_results.is_empty() && batch_results.last().unwrap().level == batch_result.level {
                    batch_result = combine(batch_results.pop().unwrap(), batch_result, self.combined_limit());
                } else { break; }
            }
            batch_results.push(batch_result)
            /*if self.compiled_order_by.is_none() && (max_limit as usize) < combined_results.cols.len() {
                break;
            }*/
        }

        if let Some(result) = QueryTask::combine_results(batch_results, self.combined_limit()) {
            self.push_result(result);
        }
        stats.runtime_ns += precise_time_ns() - start_time;
    }

    fn combine_results(batch_results: Vec<BatchResult>, limit: usize) -> Option<BatchResult> {
        let mut full_result = None;
        for batch_result in batch_results {
            if let Some(partial) = full_result {
                full_result = Some(combine(partial, batch_result, limit));
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
            let full_result = QueryTask::combine_results(owned_results, self.combined_limit()).unwrap();
            let final_result = self.convert_to_output_format(&full_result, &mut state.stats);
            self.sender.send(final_result);
        }
    }

    fn next_batch(&self) -> Option<&Batch> {
        let index = self.batch_index.fetch_add(1, Ordering::SeqCst);
        self.batches.get(index)
    }

    fn convert_to_output_format(&self,
                                full_result: &BatchResult,
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
            for col in &full_result.select {
                record.push(col.get_raw(i));
            }
            result_rows.push(record);
        }
        stats.record("limit_collect");
        QueryResult {
            colnames: self.output_colnames.clone(),
            rows: result_rows,
            stats: stats.clone(),
        }
    }

    fn prepare_batch<'a>(referenced_cols: &'a HashSet<String>, source: &'a Batch) -> HashMap<&'a str, &'a Column> {
        source.cols().iter()
            .filter(|col| referenced_cols.contains(col.name()))
            .map(|col| (col.name(), col))
            .collect()
    }

    fn combined_limit(&self) -> usize {
        (self.query.limit.limit + self.query.limit.offset) as usize
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

fn find_all_cols(source: &[Batch]) -> Vec<String> {
    let mut cols = HashSet::new();
    for batch in source {
        for column in batch.cols() {
            cols.insert(column.name().to_string());
        }
    }

    cols.into_iter().collect()
}
