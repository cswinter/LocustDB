use std::cmp;
use std::collections::HashMap;
use std::collections::HashSet;
use std::iter::Iterator;
use std::mem;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, AtomicBool, Ordering};

use crate::QueryError;
use crate::QueryResult;
use crate::engine::*;
use crate::ingest::raw_val::RawVal;
use crate::mem_store::partition::Partition;
use crate::mem_store::column::DataSource;
use crate::scheduler::*;
use crate::scheduler::disk_read_scheduler::DiskReadScheduler;
use crate::syntax::expression::*;
use time::precise_time_ns;


pub struct QueryTask {
    main_phase: NormalFormQuery,
    final_pass: Option<NormalFormQuery>,
    explain: bool,
    show: Vec<usize>,
    partitions: Vec<Arc<Partition>>,
    referenced_cols: HashSet<String>,
    output_colnames: Vec<String>,
    start_time_ns: u64,
    db: Arc<DiskReadScheduler>,

    // Lifetime is not actually static, but tied to the lifetime of this struct.
    // There is currently no good way to express this constraint in Rust.
    // Accessing pointers derived from unsafe_state after it has been dropped violates memory safety.
    // TODO(#96): better encapsulate unsafety using some abstraction such as the refstruct crate.
    unsafe_state: Mutex<QueryState<'static>>,
    batch_index: AtomicUsize,
    completed: AtomicBool,
    sender: SharedSender<QueryResult>,
}

pub struct QueryState<'a> {
    completed_batches: usize,
    partial_results: Vec<BatchResult<'a>>,
    explains: Vec<String>,
    rows_scanned: usize,
    rows_collected: usize,
    colstacks: Vec<Vec<HashMap<String, Arc<dyn DataSource>>>>,
}

pub struct QueryOutput {
    pub colnames: Vec<String>,
    pub rows: Vec<Vec<RawVal>>,
    pub query_plans: HashMap<String, u32>,
    pub stats: QueryStats,
}


#[derive(Debug, Clone)]
pub struct QueryStats {
    pub runtime_ns: u64,
    pub rows_scanned: usize,
}

impl Default for QueryStats {
    fn default() -> QueryStats {
        QueryStats {
            runtime_ns: 0,
            rows_scanned: 0,
        }
    }
}


impl QueryTask {
    pub fn new(mut query: Query, explain: bool, show: Vec<usize>,
               source: Vec<Arc<Partition>>,
               db: Arc<DiskReadScheduler>,
               sender: SharedSender<QueryResult>) -> Result<QueryTask, QueryError> {
        let start_time_ns = precise_time_ns();
        if query.is_select_star() {
            query.select = find_all_cols(&source).into_iter().map(Expr::ColName).collect();
        }

        let referenced_cols = query.find_referenced_cols();

        let (main_phase, final_pass) = query.normalize()?;
        let output_colnames = match &final_pass {
            Some(final_pass) => final_pass.result_column_names(),
            None => main_phase.result_column_names(),
        };

        Ok(QueryTask {
            main_phase,
            final_pass,
            explain,
            show,
            partitions: source,
            referenced_cols,
            output_colnames,
            start_time_ns,
            db,

            unsafe_state: Mutex::new(QueryState {
                partial_results: Vec::new(),
                completed_batches: 0,
                explains: Vec::new(),
                rows_scanned: 0,
                rows_collected: 0,
                colstacks: Vec::new(),
            }),
            batch_index: AtomicUsize::new(0),
            completed: AtomicBool::new(false),
            sender,
        })
    }

    pub fn run(&self) {
        let mut rows_scanned = 0;
        let mut rows_collected = 0;
        let mut colstack = Vec::new();
        let mut batch_results = Vec::<BatchResult>::new();
        let mut explains = Vec::new();
        while let Some((partition, id)) = self.next_partition() {
            trace_start!("Batch {}", id);
            let show = self.show.iter().any(|&x| x == id);
            let cols = partition.get_cols(&self.referenced_cols, &self.db);
            rows_scanned += cols.iter().next().map_or(0, |c| c.1.len());
            let unsafe_cols = unsafe {
                mem::transmute::<&HashMap<String, Arc<dyn DataSource>>,
                    &'static HashMap<String, Arc<dyn DataSource>>>(&cols)
            };
            let (mut batch_result, explain) = match if self.main_phase.aggregate.is_empty() {
                self.main_phase.run(unsafe_cols, self.explain, show, id, partition.len())
            } else {
                self.main_phase.run_aggregate(unsafe_cols, self.explain, show, id, partition.len())
            } {
                Ok(result) => result,
                Err(error) => {
                    self.fail_with(error);
                    return;
                }
            };
            colstack.push(cols);
            rows_collected += batch_result.len();
            if let Some(explain) = explain {
                explains.push(explain);
            }

            // Merge only with previous batch results of same level to get O(n log n) complexity
            while let Some(br) = batch_results.pop() {
                if br.level == batch_result.level {
                    match combine(br, batch_result, self.combined_limit()) {
                        Ok(result) => batch_result = result,
                        Err(error) => {
                            self.fail_with(error);
                            return;
                        }
                    };
                } else {
                    batch_results.push(br);
                    break;
                }
            }
            batch_results.push(batch_result);

            if self.completed.load(Ordering::SeqCst) {
                return;
            }
            if self.sufficient_rows(rows_collected) {
                break;
            }
        }

        match QueryTask::combine_results(batch_results, self.combined_limit()) {
            Ok(Some(result)) => self.push_result(result, rows_scanned, rows_collected, explains),
            Err(error) => self.fail_with(error),
            _ => {}
        }
        // need to keep colstack alive, otherwise results may reference freed data
        self.push_colstack(colstack);
    }

    fn combine_results(batch_results: Vec<BatchResult>, limit: usize) -> Result<Option<BatchResult>, QueryError> {
        let mut full_result = None;
        for batch_result in batch_results {
            if let Some(partial) = full_result {
                full_result = Some(combine(partial, batch_result, limit)?);
            } else {
                full_result = Some(batch_result);
            }
        }
        Ok(full_result)
    }

    fn push_result(&self, result: BatchResult, rows_scanned: usize, rows_collected: usize, explains: Vec<String>) {
        let mut state = self.unsafe_state.lock().unwrap();
        if self.completed.load(Ordering::SeqCst) { return; }
        state.completed_batches += result.batch_count;
        state.explains.extend(explains);
        state.rows_scanned += rows_scanned;
        state.rows_collected += rows_collected;
        unsafe {
            let result = mem::transmute::<_, BatchResult<'static>>(result);
            state.partial_results.push(result);
        }
        if state.completed_batches == self.partitions.len() || self.sufficient_rows(state.rows_collected) {
            let mut owned_results = Vec::with_capacity(0);
            mem::swap(&mut owned_results, &mut state.partial_results);
            let full_result = match QueryTask::combine_results(owned_results, self.combined_limit()) {
                Ok(result) => result.unwrap(),
                Err(error) => {
                    self.fail_with_no_lock(error);
                    return;
                }
            };
            let final_result = if let Some(final_pass) = &self.final_pass {
                let data_sources = full_result.into_columns();
                let cols = unsafe {
                    mem::transmute::<&HashMap<String, Arc<dyn DataSource>>,
                        &'static HashMap<String, Arc<dyn DataSource>>>(&data_sources)
                };
                let full_result = final_pass.run(cols,
                                                 self.explain,
                                                 !self.show.is_empty(),
                                                 0xdead_beef,
                                                 cols.iter().next().map(|(_, c)| c.len()).unwrap_or(0)).unwrap().0;
                self.convert_to_output_format(&full_result, state.rows_scanned, &state.explains)
            } else {
                self.convert_to_output_format(&full_result, state.rows_scanned, &state.explains)
            };
            self.sender.send(Ok(final_result));
            self.completed.store(true, Ordering::SeqCst);
        }
    }

    fn push_colstack(&self, colstack: Vec<HashMap<String, Arc<dyn DataSource>>>) {
        let mut state = self.unsafe_state.lock().unwrap();
        state.colstacks.push(colstack);
    }

    fn fail_with(&self, error: QueryError) {
        let mut _state = self.unsafe_state.lock().unwrap();
        if self.completed.load(Ordering::SeqCst) { return; }
        self.fail_with_no_lock(error)
    }

    fn fail_with_no_lock(&self, error: QueryError) {
        self.completed.store(true, Ordering::SeqCst);
        self.batch_index.store(self.partitions.len(), Ordering::SeqCst);
        self.sender.send(Err(error));
    }

    fn sufficient_rows(&self, rows_collected: usize) -> bool {
        let unordered_select = self.main_phase.aggregate.is_empty() && self.main_phase.order_by.is_empty();
        unordered_select && self.combined_limit() < rows_collected
    }

    fn next_partition(&self) -> Option<(&Arc<Partition>, usize)> {
        let index = self.batch_index.fetch_add(1, Ordering::SeqCst);
        self.partitions.get(index).map(|b| (b, index))
    }

    fn convert_to_output_format(&self,
                                full_result: &BatchResult,
                                rows_scanned: usize,
                                explains: &[String]) -> QueryOutput {
        let limit = self.main_phase.limit.limit as usize;
        let offset = self.main_phase.limit.offset as usize;
        let mut result_rows = Vec::new();
        let count = cmp::min(limit, full_result.len() - offset);
        for i in offset..(count + offset) {
            let mut record = Vec::with_capacity(self.output_colnames.len());
            // TODO(#99): use column order of original query
            for &j in &full_result.projection {
                record.push(full_result.columns[j].get_raw(i));
            }
            for &(aggregation, _) in &full_result.aggregations {
                record.push(full_result.columns[aggregation].get_raw(i));
            }
            result_rows.push(record);
        }

        let mut query_plans = HashMap::new();
        for plan in explains {
            *query_plans.entry(plan.to_owned()).or_insert(0) += 1
        }

        QueryOutput {
            colnames: self.output_colnames.clone(),
            rows: result_rows,
            query_plans,
            stats: QueryStats {
                runtime_ns: precise_time_ns() - self.start_time_ns,
                rows_scanned,
            },
        }
    }

    fn combined_limit(&self) -> usize {
        (self.main_phase.limit.limit + self.main_phase.limit.offset) as usize
    }
}

impl Task for QueryTask {
    fn execute(&self) { self.run(); }
    fn completed(&self) -> bool {
        let batch_index = self.batch_index.load(Ordering::SeqCst);
        self.completed.load(Ordering::SeqCst) || batch_index >= self.partitions.len()
    }
    fn multithreaded(&self) -> bool { true }
}

fn find_all_cols(source: &[Arc<Partition>]) -> Vec<String> {
    let mut cols = HashSet::new();
    for partition in source {
        for name in partition.col_names() {
            cols.insert(name.to_string());
        }
    }

    cols.into_iter().collect()
}
