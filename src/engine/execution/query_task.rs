use std::cmp;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::iter::Iterator;
use std::mem;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};

use crate::engine::*;
use crate::ingest::raw_val::RawVal;
use crate::mem_store::column::DataSource;
use crate::mem_store::partition::Partition;
use crate::perf_counter::QueryPerfCounter;
use crate::scheduler::disk_read_scheduler::DiskReadScheduler;
use crate::scheduler::*;
use crate::syntax::expression::*;
use crate::QueryError;
use crate::QueryResult;

pub struct QueryTask {
    main_phase: NormalFormQuery,
    final_pass: Option<NormalFormQuery>,
    explain: bool,
    rowformat: bool,
    show: Vec<usize>,
    partitions: Vec<Arc<Partition>>,
    referenced_cols: HashSet<String>,
    output_colnames: Vec<String>,
    start_time: Instant,
    db: Arc<DiskReadScheduler>,
    perf_counter: Arc<QueryPerfCounter>,
    batch_size: usize,

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
    partial_results: BTreeMap<usize, BatchResult<'a>>,
    explains: Vec<String>,
    rows_collected: usize,
    colstacks: Vec<Vec<HashMap<String, Arc<dyn DataSource>>>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryOutput {
    pub colnames: Vec<String>,

    pub rows: Option<Vec<Vec<RawVal>>>,
    pub columns: Vec<(String, BasicTypeColumn)>,

    pub query_plans: HashMap<String, u32>,
    pub stats: QueryStats,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum BasicTypeColumn {
    Int(Vec<i64>),
    Float(Vec<f64>),
    String(Vec<String>),
    Null(usize),
    Mixed(Vec<RawVal>),
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct QueryStats {
    pub runtime_ns: u64,
    pub rows_scanned: u64,
    pub files_opened: u64,
    pub disk_read_bytes: u64,
}

impl QueryTask {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        mut query: Query,
        rowformat: bool,
        explain: bool,
        show: Vec<usize>,
        source: Vec<Arc<Partition>>,
        db: Arc<DiskReadScheduler>,
        sender: SharedSender<QueryResult>,
        batch_size: usize,
    ) -> Result<QueryTask, QueryError> {
        let start_time = Instant::now();
        if query.is_select_star() {
            query.select = find_all_cols(&source)
                .into_iter()
                .map(|name| ColumnInfo {
                    expr: Expr::ColName(name.clone()),
                    name: Some(name),
                })
                .collect();
        }

        let referenced_cols = query.find_referenced_cols();

        let (main_phase, final_pass) = query.normalize()?;
        let output_colnames = match &final_pass {
            Some(final_pass) => final_pass.result_column_names()?,
            None => main_phase.result_column_names()?,
        };

        let task = QueryTask {
            main_phase,
            final_pass,
            explain,
            rowformat,
            show,
            partitions: source,
            referenced_cols,
            output_colnames,
            start_time,
            db,
            perf_counter: Arc::default(),
            batch_size,

            unsafe_state: Mutex::new(QueryState {
                partial_results: BTreeMap::new(),
                completed_batches: 0,
                explains: Vec::new(),
                rows_collected: 0,
                colstacks: Vec::new(),
            }),
            batch_index: AtomicUsize::new(0),
            completed: AtomicBool::new(false),
            sender,
        };

        // If table is empty and there are no partitions we need to return result immediately, otherwise sender is dropped since no threads execute.
        if task.completed() {
            task.sender.send(Ok(QueryOutput {
                colnames: task.output_colnames.clone(),
                rows: Some(vec![]),
                columns: Default::default(),
                query_plans: Default::default(),
                stats: QueryStats {
                    runtime_ns: start_time.elapsed().as_nanos() as u64,
                    rows_scanned: 0,
                    files_opened: 0,
                    disk_read_bytes: 0,
                },
            }));
        }

        Ok(task)
    }

    pub fn run(&self) {
        let mut rows_scanned = 0;
        let mut rows_collected = 0;
        let mut colstack = Vec::new();
        let mut batch_results = BTreeMap::<usize, BatchResult>::new();
        let mut explains = Vec::new();
        while let Some((partition, id)) = self.next_partition() {
            let show = self.show.iter().any(|&x| x == id);
            let cols =
                partition.get_cols(&self.referenced_cols, &self.db, self.perf_counter.as_ref());
            rows_scanned += cols.iter().next().map_or(0, |c| c.1.len());
            let unsafe_cols = unsafe {
                mem::transmute::<
                    &HashMap<String, Arc<dyn DataSource>>,
                    &'static HashMap<String, Arc<dyn DataSource>>,
                >(&cols)
            };
            let (batch_result, explain) = match if self.main_phase.aggregate.is_empty() {
                self.main_phase.run(
                    unsafe_cols,
                    self.explain,
                    show,
                    id,
                    partition.range(),
                    self.batch_size,
                )
            } else {
                self.main_phase.run_aggregate(
                    unsafe_cols,
                    self.explain,
                    show,
                    id,
                    partition.range(),
                    self.batch_size,
                )
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

            batch_results.insert(batch_result.scanned_range.start, batch_result);
            // Merge only with contiguous previous batch results of same level to get O(n log n) complexity and deterministic order.
            // Find any adjacent batch results of same level and merge them
            if let Err(error) = QueryTask::combine_results(
                &mut batch_results,
                self.combined_limit(),
                self.batch_size,
                true,
            ) {
                self.fail_with(error);
                return;
            }
            if self.completed.load(Ordering::SeqCst) {
                return;
            }
            // TODO: abort early if we have selected sufficient number of rows from initial partition
        }

        // TODO: parallelize combining results from different threads
        for (_, result) in batch_results {
            self.push_result(result, rows_scanned, rows_collected, explains.clone());
            rows_scanned = 0;
            rows_collected = 0;
            explains.clear();
        }

        // need to keep colstack alive, otherwise results may reference freed data
        self.push_colstack(colstack);
    }

    fn combine_results(
        batch_results: &mut BTreeMap<usize, BatchResult>,
        combined_limit: usize,
        batch_size: usize,
        require_same_level: bool,
    ) -> Result<(), QueryError> {
        fn eligible_pair(
            batch_results: &BTreeMap<usize, BatchResult>,
            require_same_level: bool,
        ) -> Option<(usize, usize)> {
            let mut iter = batch_results.iter();
            let mut prev = iter.next()?;
            for (offset, curr) in iter {
                if (prev.1.level == curr.level || !require_same_level)
                    && prev.1.scanned_range.end == curr.scanned_range.start
                {
                    return Some((*prev.0, *offset));
                }
                prev = (offset, curr);
            }
            None
        }
        while let Some((key1, key2)) =
            eligible_pair(batch_results, true).or_else(|| if !require_same_level {
                eligible_pair(batch_results, false)
            } else {
                None
            })
        {
            let br1 = batch_results.remove(&key1).unwrap();
            let br2 = batch_results.remove(&key2).unwrap();
            let result = combine(br1, br2, combined_limit, batch_size)?;
            batch_results.insert(key1, result);
        }
        Ok(())
    }

    fn push_result(
        &self,
        result: BatchResult,
        rows_scanned: usize,
        rows_collected: usize,
        explains: Vec<String>,
    ) {
        let mut state = self.unsafe_state.lock().unwrap();
        if self.completed.load(Ordering::SeqCst) {
            return;
        }
        state.completed_batches += result.batch_count;
        state.explains.extend(explains);
        self.perf_counter.scanned(rows_scanned as u64);
        state.rows_collected += rows_collected;

        let result = unsafe { mem::transmute::<_, BatchResult<'static>>(result) };
        state
            .partial_results
            .insert(result.scanned_range.start, result);

        if state.completed_batches == self.partitions.len() {
            let mut owned_results = mem::take(&mut state.partial_results);
            if let Err(error) = QueryTask::combine_results(
                &mut owned_results,
                self.combined_limit(),
                self.batch_size,
                false,
            ) {
                self.fail_with_no_lock(error);
                return;
            };
            if owned_results.len() != 1 {
                let error = fatal!(
                    "Expected exactly one remaining partition. Ranges and levels: {:?}",
                    owned_results
                        .values()
                        .map(|v| (v.scanned_range.clone(), v.level))
                        .collect::<Vec<_>>()
                );
                self.fail_with_no_lock(error);
                return;
            }
            let full_result = owned_results.into_iter().next().unwrap().1;
            let final_result = if let Some(final_pass) = &self.final_pass {
                let data_sources = full_result.into_columns();
                let cols = unsafe {
                    mem::transmute::<
                        &HashMap<String, Arc<dyn DataSource>>,
                        &'static HashMap<String, Arc<dyn DataSource>>,
                    >(&data_sources)
                };
                let full_result = final_pass
                    .run(
                        cols,
                        self.explain,
                        !self.show.is_empty(),
                        0xdead_beef,
                        0..cols.iter().next().map(|(_, c)| c.len()).unwrap_or(0),
                        self.batch_size,
                    )
                    .unwrap()
                    .0;
                self.convert_to_output_format(&full_result, &state.explains)
            } else {
                self.convert_to_output_format(&full_result, &state.explains)
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
        if self.completed.load(Ordering::SeqCst) {
            return;
        }
        self.fail_with_no_lock(error)
    }

    fn fail_with_no_lock(&self, error: QueryError) {
        self.completed.store(true, Ordering::SeqCst);
        self.batch_index
            .store(self.partitions.len(), Ordering::SeqCst);
        self.sender.send(Err(error));
    }

    fn next_partition(&self) -> Option<(&Arc<Partition>, usize)> {
        let index = self.batch_index.fetch_add(1, Ordering::SeqCst);
        self.partitions.get(index).map(|b| (b, index))
    }

    fn convert_to_output_format(
        &self,
        full_result: &BatchResult,
        explains: &[String],
    ) -> QueryOutput {
        let lo = self
            .final_pass
            .as_ref()
            .map(|x| &x.limit)
            .unwrap_or(&self.main_phase.limit);
        let limit = lo.limit as usize;
        let offset = lo.offset as usize;
        let count = cmp::min(limit, full_result.len() - offset);
        full_result.validate().unwrap();

        let mut rows = None;
        if self.rowformat {
            let mut result_rows = Vec::new();
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
            rows = Some(result_rows);
        }

        let mut query_plans = HashMap::new();
        for plan in explains {
            *query_plans.entry(plan.to_owned()).or_insert(0) += 1
        }

        let mut columns = vec![];
        // TODO: this is probably wrong, see above (columns might not all be in the same order as output_colnames or correspond to columns we want to return)
        for (colname, column) in self.output_colnames.iter().zip(full_result.columns.iter()) {
            let column = column.slice_box(offset, offset + count);
            let column = BasicTypeColumn::from_boxed_data(column);
            columns.push((colname.clone(), column));
        }

        QueryOutput {
            colnames: self.output_colnames.clone(),
            rows,
            columns,
            query_plans,
            stats: QueryStats {
                runtime_ns: self.start_time.elapsed().as_nanos() as u64,
                rows_scanned: self.perf_counter.rows_scanned(),
                files_opened: self.perf_counter.files_opened(),
                disk_read_bytes: self.perf_counter.disk_read_bytes(),
            },
        }
    }

    fn combined_limit(&self) -> usize {
        (self.main_phase.limit.limit + self.main_phase.limit.offset) as usize
    }
}

impl Task for QueryTask {
    fn execute(&self) {
        self.run();
    }
    fn completed(&self) -> bool {
        let batch_index = self.batch_index.load(Ordering::SeqCst);
        self.completed.load(Ordering::SeqCst) || batch_index >= self.partitions.len()
    }
    fn multithreaded(&self) -> bool {
        true
    }
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

impl BasicTypeColumn {
    fn from_boxed_data(data: BoxedData) -> BasicTypeColumn {
        match data.get_type() {
            EncodingType::Str => {
                BasicTypeColumn::String(data.cast_ref_str().iter().map(|s| s.to_string()).collect())
            }
            EncodingType::I64 => BasicTypeColumn::Int(data.cast_ref_i64().to_vec()),
            EncodingType::U8 | EncodingType::Bitvec => {
                BasicTypeColumn::Int(data.cast_ref_u8().iter().map(|&i| i as i64).collect())
            }
            EncodingType::U16 => {
                BasicTypeColumn::Int(data.cast_ref_u16().iter().map(|&i| i as i64).collect())
            }
            EncodingType::U32 => {
                BasicTypeColumn::Int(data.cast_ref_u32().iter().map(|&i| i as i64).collect())
            }
            EncodingType::U64 => {
                BasicTypeColumn::Int(data.cast_ref_u64().iter().map(|&i| i as i64).collect())
            }
            EncodingType::USize => {
                BasicTypeColumn::Int(data.cast_ref_usize().iter().map(|&i| i as i64).collect())
            }
            EncodingType::F64 => {
                BasicTypeColumn::Float(data.cast_ref_f64().iter().map(|&f| f.0).collect())
            }
            EncodingType::Null => BasicTypeColumn::Null(data.len()),

            EncodingType::Val
            | EncodingType::NullableStr
            | EncodingType::NullableI64
            | EncodingType::NullableU8
            | EncodingType::NullableU16
            | EncodingType::NullableU32
            | EncodingType::NullableU64
            | EncodingType::NullableF64
            | EncodingType::OptStr
            | EncodingType::OptF64 => {
                let mut vals = vec![];
                for i in 0..data.len() {
                    vals.push(data.get_raw(i));
                }
                BasicTypeColumn::Mixed(vals)
            }
            EncodingType::ScalarI64
            | EncodingType::ScalarStr
            | EncodingType::ScalarString
            | EncodingType::ConstVal
            | EncodingType::ValRows
            | EncodingType::ByteSlices(_)
            | EncodingType::Premerge
            | EncodingType::MergeOp => {
                panic!("Unsupported type {:?}", data.get_type())
            }
        }
    }

    pub fn len(&self) -> usize {
        match self {
            BasicTypeColumn::Int(v) => v.len(),
            BasicTypeColumn::Float(v) => v.len(),
            BasicTypeColumn::String(v) => v.len(),
            BasicTypeColumn::Null(v) => *v,
            BasicTypeColumn::Mixed(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl PartialEq for BasicTypeColumn {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (BasicTypeColumn::Int(a), BasicTypeColumn::Int(b)) => a == b,
            (BasicTypeColumn::Float(a), BasicTypeColumn::Float(b)) => a.len() == b.len() && (0..a.len()).all(|i| OrderedFloat(a[i]) == OrderedFloat(b[i])),
            (BasicTypeColumn::String(a), BasicTypeColumn::String(b)) => a == b,
            (BasicTypeColumn::Null(a), BasicTypeColumn::Null(b)) => a == b,
            (BasicTypeColumn::Mixed(a), BasicTypeColumn::Mixed(b)) => a == b,
            _ => false,
        }
    }
}