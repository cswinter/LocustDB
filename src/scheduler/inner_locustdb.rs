use std::borrow::Cow;
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::ops::Range;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread;
use std::time::{Duration, Instant};
use std::time::{SystemTime, UNIX_EPOCH};
use std::{mem, str};

use datasize::data_size;
use futures::channel::oneshot;
use futures::executor::block_on;
use inner_locustdb::meta_store::PartitionMetadata;
use itertools::Itertools;
use locustdb_serialization::event_buffer::{ColumnBuffer, ColumnData, EventBuffer, TableBuffer};
use threadpool::ThreadPool;

use crate::disk_store::storage::Storage;
use crate::engine::query_task::{BasicTypeColumn, QueryTask};
use crate::engine::Query;
use crate::ingest::colgen::GenTable;
use crate::ingest::input_column::InputColumn;
use crate::ingest::raw_val::RawVal;
use crate::locustdb::Options;
use crate::mem_store::partition::Partition;
use crate::mem_store::table::*;
use crate::observability::{metrics, PerfCounter, SimpleTracer};
use crate::scheduler::disk_read_scheduler::DiskReadScheduler;
use crate::scheduler::*;
use crate::{disk_store::*, QueryError, QueryOutput};
use crate::{mem_store::*, NoopStorage};

use self::meta_store::SubpartitionMetadata;
use self::raw_col::MixedCol;
use self::wal_segment::WalSegment;

pub struct InnerLocustDB {
    tables: RwLock<HashMap<String, Arc<Table>>>,
    lru: Lru,
    disk_read_scheduler: Arc<DiskReadScheduler>,

    storage: Option<Arc<Storage>>,

    wal_size: (Mutex<u64>, Condvar),
    pending_wal_flushes: (Mutex<Vec<mpsc::Sender<()>>>, Condvar),

    opts: Options,

    perf_counter: Arc<PerfCounter>,

    running: AtomicBool,
    idle_queue: Condvar,
    task_queue: Mutex<VecDeque<Arc<dyn Task>>>,

    walflush_threadpool: ThreadPool,
}

impl InnerLocustDB {
    pub fn new(opts: &Options) -> Arc<InnerLocustDB> {
        let lru = Lru::default();
        let perf_counter = Arc::new(PerfCounter::default());
        let (storage, tables, wal_segments, wal_size) = match opts.db_path.clone() {
            Some(path) => {
                let perf_counter = perf_counter.clone();
                let lru = lru.clone();
                std::thread::spawn(move || {
                    let (storage, wal, wal_size) = Storage::new(&path, perf_counter, false);
                    let tables = Table::restore_tables_from_disk(&storage, &lru);

                    (Some(Arc::new(storage)), tables, wal, wal_size)
                })
                .join()
                .unwrap()
            }
            None => (None, HashMap::new(), vec![], 0),
        };
        let disk_read_scheduler = Arc::new(DiskReadScheduler::new(
            storage
                .clone()
                .map(|s| s as Arc<dyn ColumnLoader>)
                .unwrap_or(Arc::new(NoopStorage)),
            lru.clone(),
            opts.read_threads,
            !opts.mem_lz4,
        ));

        let locustdb = Arc::new(InnerLocustDB {
            tables: RwLock::new(tables),
            lru,
            disk_read_scheduler,
            running: AtomicBool::new(true),

            storage,

            wal_size: (Mutex::new(wal_size), Condvar::new()),
            pending_wal_flushes: (Mutex::new(vec![]), Condvar::new()),

            opts: opts.clone(),
            perf_counter,

            idle_queue: Condvar::new(),
            task_queue: Mutex::new(VecDeque::new()),

            walflush_threadpool: ThreadPool::new(opts.wal_flush_compaction_threads),
        });

        InnerLocustDB::start_worker_threads(&locustdb);

        let _ = locustdb.create_if_empty_no_ingest("_meta_tables");
        let mut next_id = None;
        for wal_segment in wal_segments {
            if let Some(id) = next_id {
                assert_eq!(wal_segment.id, id, "WAL segments are not contiguous");
            }
            next_id = Some(wal_segment.id + 1);
            for (table_name, data) in wal_segment.data.into_owned().tables {
                let _ = locustdb.create_if_empty_no_ingest(&table_name);
                let tables = locustdb.tables.read().unwrap();
                let table = tables.get(&table_name).unwrap();
                let rows = data.len;
                // TODO: eliminate conversion
                if !table.columns_names_loaded() {
                    let column_names = locustdb
                        .query_column_names(&table_name)
                        .expect("Failed to query column names");
                    table.init_column_names(column_names.into_iter().collect());
                }
                let columns = data
                    .columns
                    .into_iter()
                    .map(|(k, v)| (k, InputColumn::from_column_data(v.data, rows)))
                    .collect();
                table.ingest_homogeneous(columns);
            }
        }
        locustdb
    }

    pub fn start_worker_threads(locustdb: &Arc<InnerLocustDB>) {
        for _ in 0..locustdb.opts.threads {
            let cloned = locustdb.clone();
            thread::spawn(move || InnerLocustDB::worker_loop(cloned));
        }
        let ldb = locustdb.clone();
        thread::spawn(move || InnerLocustDB::enforce_mem_limit(&ldb));
        let ldb = locustdb.clone();
        thread::spawn(move || ldb.enforce_wal_limit());
        let ldb = locustdb.clone();
        thread::spawn(move || ldb.log_metrics());
    }

    pub fn snapshot(
        &self,
        table: &str,
        column_filter: Option<&[String]>,
    ) -> Option<Vec<Arc<Partition>>> {
        let tables = self.tables.read().unwrap();
        tables.get(table).map(|t| t.snapshot(column_filter))
    }

    pub fn full_snapshot(&self) -> Vec<Vec<Arc<Partition>>> {
        let tables = self.tables.read().unwrap();
        tables.values().map(|t| t.snapshot(None)).collect()
    }

    pub fn stop(&self) {
        // TODO: ensure all pending ingestion tasks are completed and new requests are rejected
        // Acquire task_queue_guard to make sure that there are no threads that have checked self.running but not waited on idle_queue yet.
        info!("Stopping database...");
        self.running.store(false, Ordering::SeqCst);
        let _guard = self.task_queue.lock();
        self.running.store(false, Ordering::SeqCst);
        self.idle_queue.notify_all();
    }

    fn worker_loop(locustdb: Arc<InnerLocustDB>) {
        while locustdb.running.load(Ordering::SeqCst) {
            if let Some(task) = InnerLocustDB::await_task(&locustdb) {
                task.execute();
            }
        }
        drop(locustdb) // Make clippy happy
    }

    fn await_task(ldb: &Arc<InnerLocustDB>) -> Option<Arc<dyn Task>> {
        let mut task_queue = ldb.task_queue.lock().unwrap();
        while task_queue.is_empty() {
            if !ldb.running.load(Ordering::SeqCst) {
                return None;
            }
            task_queue = ldb.idle_queue.wait(task_queue).unwrap();
        }
        while let Some(task) = task_queue.pop_front() {
            if task.completed() {
                continue;
            }
            if task.multithreaded() {
                task_queue.push_front(task.clone());
            }
            if !task_queue.is_empty() {
                ldb.idle_queue.notify_one();
            }
            return Some(task);
        }
        None
    }

    pub fn schedule<T: Task + 'static>(&self, task: T) {
        // This function may be entered by event loop thread so it's important it always returns quickly.
        // Since the task queue locks are never held for long, we should be fine.
        let mut task_queue = self.task_queue.lock().unwrap();
        task_queue.push_back(Arc::new(task));
        self.idle_queue.notify_one();
    }

    fn ingest_single(&self, table: &str, row: Vec<(String, RawVal)>) {
        self.create_if_empty(table);
        let tables = self.tables.read().unwrap();
        tables.get(table).unwrap().ingest(row)
    }

    pub fn ingest_efficient(&self, mut events: EventBuffer) {
        let (wal_size, wal_condvar) = &self.wal_size;
        // Holding wal lock ensures single-threaded ingestion
        let mut wal_size = wal_size.lock().unwrap();
        while *wal_size > self.opts.max_wal_size_bytes {
            log::warn!("wal size limit exceeded, blocking ingestion");
            wal_size = wal_condvar.wait(wal_size).unwrap();
        }

        let mut _meta_tables_rows = vec![];
        let mut _new_column_rows = vec![];
        for (table, table_buffer) in &events.tables {
            // Ensure table and corresponding _meta_columns table exists
            if let Some(row) = self.create_if_empty_no_ingest(table) {
                _meta_tables_rows.push(row);
            }
            let meta_columns_table = format!("_meta_columns_{}", table);
            if let Some(row) = self.create_if_empty_no_ingest(&meta_columns_table) {
                _meta_tables_rows.push(row);
            }
            // Ensure columns are loaded for the table
            let table = self.tables.read().unwrap()[table].clone();
            if !table.columns_names_loaded() {
                let column_names = self
                    .query_column_names(table.name())
                    .expect("Failed to query column names");
                table.init_column_names(column_names.into_iter().collect());
            }
            let new_column_names =
                table.new_column_names(table_buffer.columns.keys().map(|s| s.as_str()));
            if !new_column_names.is_empty() {
                _new_column_rows.push((meta_columns_table.clone(), new_column_names));
            }
        }
        if !_meta_tables_rows.is_empty() {
            let (timestamps, names): (Vec<_>, Vec<_>) = _meta_tables_rows.into_iter().unzip();
            let len = timestamps.len() as u64;
            let mut columns = HashMap::new();
            columns.insert(
                "timestamp".to_string(),
                ColumnBuffer {
                    data: ColumnData::I64(timestamps),
                },
            );
            columns.insert(
                "name".to_string(),
                ColumnBuffer {
                    data: ColumnData::String(names),
                },
            );
            let meta_tables_buffer = TableBuffer { len, columns };
            events
                .tables
                .insert("_meta_tables".to_string(), meta_tables_buffer);
        }
        for (table, names) in _new_column_rows {
            let len = names.len() as u64;
            let columns = HashMap::from([(
                "column_name".to_string(),
                ColumnBuffer {
                    data: ColumnData::String(names),
                },
            )]);
            let meta_columns_buffer = TableBuffer { len, columns };
            events.tables.insert(table, meta_columns_buffer);
        }

        let bytes_written_join_handle = self.storage.as_ref().map(|storage| {
            let events = events.clone();
            let storage = storage.clone();
            thread::spawn(move || {
                storage.persist_wal_segment(WalSegment {
                    id: 0,
                    data: Cow::Borrowed(&events),
                })
            })
        });
        for (table, data) in events.tables {
            let tables = self.tables.read().unwrap();
            let table = tables.get(&table).unwrap();
            let rows = data.len;
            // TODO: eliminate conversion
            let columns = data
                .columns
                .into_iter()
                .map(|(k, v)| (k, InputColumn::from_column_data(v.data, rows)))
                .collect();
            table.ingest_homogeneous(columns);
        }

        if let Some(jh) = bytes_written_join_handle {
            let bytes_written = jh.join().unwrap();
            *wal_size += bytes_written;
        }
        wal_condvar.notify_all();
    }

    /// Creates new partition from currently open buffer in each table, persists partitions to disk, and deletes WAL.
    /// There is a single WAL flush thread that is responsible for flushing the WAL and creating new partitions, so
    /// this function is never called concurrently.
    fn wal_flush(self: &Arc<InnerLocustDB>) {
        log::info!("Commencing WAL flush");
        let mut tracer = SimpleTracer::new();
        let span_wal_flush = tracer.start_span("wal_flush");

        // Acquire wal_size lock to block creation of new WAL segments and modifications of open buffers,
        // record the range of unflushed WAL entries, freeze table buffers, and reset WAL size.
        // After this block, ingestion is unblocked again.
        let span_freeze_buffers = tracer.start_span("freeze_buffers");
        let tables;
        let unflushed_wal_ids;
        {
            let (wal_size, wal_condvar) = &self.wal_size;
            let mut wal_size = wal_size.lock().unwrap();
            unflushed_wal_ids = self
                .storage
                .as_ref()
                .map(|s| s.unflushed_wal_ids())
                .unwrap_or(0..0);
            tables = self
                .tables
                .read()
                .unwrap()
                .values()
                .cloned()
                .collect::<Vec<_>>();
            for table in &tables {
                table.freeze_buffer();
            }
            *wal_size = 0;
            wal_condvar.notify_all();
        }
        tracer.end_span(span_freeze_buffers);

        // Iterate over all tables and create new partitions from frozen buffers.
        let span_batching = tracer.start_span("batching");
        let (tx, rx) = mpsc::channel();
        let table_count = tables.len();
        for table in tables {
            let this = self.clone();
            let tx = tx.clone();
            self.walflush_threadpool.execute(move || {
                tx.send(this.flush_table_buffer(table)).unwrap();
            });
        }
        let mut new_partitions = Vec::new();
        let mut compactions = Vec::new();
        for (new_partition, maybe_compaction) in rx.iter().take(table_count) {
            if let Some((metadata, subpartitions)) = new_partition {
                new_partitions.push((metadata, subpartitions));
            }
            if let Some(compaction) = maybe_compaction {
                compactions.push(compaction);
            }
        }
        tracer.end_span(span_batching);

        // Persist new partitions
        if let Some(s) = self.storage.as_ref() {
            s.persist_partitions(new_partitions, &mut tracer);
        }

        // Write new segments from compactions to storage and apply compaction in-memory
        let span_compaction = tracer.start_span("compaction");
        let (tx, rx) = mpsc::channel();
        let num_compactions = compactions.len();
        for (table, id, range, parts) in compactions {
            let tx = tx.clone();
            let this = self.clone();
            self.walflush_threadpool.execute(move || {
                let to_delete = this.compact(table, id, range, &parts);
                tx.send(to_delete).unwrap();
            });
        }
        let partitions_to_delete = rx.iter().take(num_compactions).flatten().collect();
        tracer.end_span(span_compaction);

        // Update metastore and clean up orphaned partitions and WAL segments
        if let Some(storage) = self.storage.as_ref() {
            storage.persist_metastore(unflushed_wal_ids.end, &mut tracer);
            storage.delete_orphaned_partitions(partitions_to_delete, &mut tracer);
            storage.delete_wal_segments(unflushed_wal_ids, &mut tracer);
        }

        tracer.end_span(span_wal_flush);

        log::info!("Completed WAL flush\n{}", tracer.summary());
    }

    pub(crate) fn schedule_query_column_names(
        &self,
        table: &str,
    ) -> Result<oneshot::Receiver<Result<QueryOutput, QueryError>>, QueryError> {
        let meta_table = format!("_meta_columns_{}", table);
        let query = Query::read_column(&meta_table, "column_name");
        let data = self
            .tables
            .read()
            .unwrap()
            .get(&meta_table)
            .ok_or_else(|| {
                fatal!(
                    "Table column name meta table {} not found for table {}",
                    meta_table,
                    table
                )
            })?
            .snapshot(None);
        let (sender, receiver) = oneshot::channel();
        let query_task = QueryTask::new(
            query,
            false,
            false,
            vec![],
            data.clone(),
            self.disk_read_scheduler().clone(),
            SharedSender::new(sender),
            self.opts.batch_size,
            None,
        )
        .unwrap();
        self.schedule(query_task);
        Ok(receiver)
    }

    fn query_column_names(&self, table: &str) -> Result<Vec<String>, QueryError> {
        let receiver = self.schedule_query_column_names(table)?;
        let mut result = block_on(receiver).unwrap()?;
        assert!(result.columns.len() == 1, "Expected 1 column");
        let column_names = match result.columns.pop().unwrap() {
            (_, BasicTypeColumn::String(names)) => Ok(names.into_iter().collect()),
            _ => Err(fatal!(
                "Expected single string column in meta columns table for {}",
                table
            )),
        };
        column_names
    }

    /// Triggers a WAL flush and blocks until it is complete.
    pub fn trigger_wal_flush(&self) {
        let receiver = {
            let (sender, receiver) = mpsc::channel();
            let mut pending_wal_flushes = self.pending_wal_flushes.0.lock().unwrap();
            pending_wal_flushes.push(sender);
            self.pending_wal_flushes.1.notify_all();
            receiver
        };
        // Have to ensure that lock guard is dropped before waiting on receiver
        receiver.recv().unwrap()
    }

    #[allow(clippy::type_complexity)]
    #[must_use]
    pub fn flush_table_buffer(
        &self,
        table: Arc<Table>,
    ) -> (
        Option<(PartitionMetadata, Vec<Vec<Arc<Column>>>)>,
        Option<(Arc<Table>, u64, Range<usize>, Vec<u64>)>,
    ) {
        let mut new_partition = None;
        let mut maybe_compaction = None;

        if let Some(partition) = table.batch() {
            let columns: Vec<_> = partition
                .clone_column_handles()
                .into_iter()
                .map(|c| c.try_get().as_ref().unwrap().clone())
                .sorted_by(|a, b| a.name().cmp(b.name()))
                .collect();
            let (metadata, subpartitions) = subpartition(&self.opts, columns);
            let mut subpartitions_by_last_column = BTreeMap::new();
            for (i, subpartition) in metadata.iter().enumerate() {
                subpartitions_by_last_column.insert(subpartition.last_column.clone(), i);
            }
            let partition_metadata = PartitionMetadata {
                id: partition.id,
                tablename: table.name().to_string(),
                len: partition.len(),
                offset: partition.range().start,
                subpartitions: metadata,
                subpartitions_by_last_column,
            };
            new_partition = Some((partition_metadata, subpartitions));
        }

        if let Some((range, parts)) = table.plan_compaction(self.opts.partition_combine_factor) {
            maybe_compaction = Some((table.clone(), table.next_partition_id(), range, parts));
        }

        (new_partition, maybe_compaction)
    }

    fn compact(
        &self,
        table: Arc<Table>,
        id: PartitionID,
        range: Range<usize>,
        parts: &[u64],
    ) -> Option<(String, Vec<(u64, String)>)> {
        // get table, create new merged partition/sub-partitions (not registered with table)
        // - get names of all columns
        // - run query for each column, construct Column
        // - create subpartitions
        let colnames = table.column_names();
        let data = table.snapshot_parts(parts);
        let mut columns = Vec::with_capacity(colnames.len());
        for column in &colnames {
            let query = Query::read_column(table.name(), column);
            let (sender, receiver) = oneshot::channel();
            let query_task = QueryTask::new(
                query,
                false,
                false,
                vec![],
                data.clone(),
                self.disk_read_scheduler().clone(),
                SharedSender::new(sender),
                self.opts.batch_size,
                None,
            )
            .unwrap();
            self.schedule(query_task);
            let result = block_on(receiver).unwrap().unwrap();
            let mut column_builder = MixedCol::default();
            let column_data = result.columns.into_iter().next().unwrap().1;
            match column_data {
                BasicTypeColumn::Int(ints) => column_builder.push_ints(ints),
                BasicTypeColumn::Float(floats) => column_builder.push_floats(floats),
                BasicTypeColumn::String(strings) => column_builder.push_strings(strings),
                BasicTypeColumn::Null(count) => column_builder.push_nulls(count),
                BasicTypeColumn::Mixed(raws) => {
                    raws.into_iter().for_each(|r| column_builder.push(r))
                }
            }
            assert_eq!(
                range.len(),
                column_builder.len(),
                "range={range:?}, column_builder.len() = {}, table = {},  column = {column}",
                column_builder.len(),
                table.name(),
            );
            columns.push(column_builder.finalize(column));
        }
        let (metadata, subpartitions) = subpartition(&self.opts, columns.clone());

        // replace old partitions with new partition
        table.compact(id, range.start, columns, parts);

        // write new subpartitions to disk and update in-memory metastore
        self.storage.as_ref().map(|s| {
            let to_delete = s.prepare_compact(
                table.name(),
                id,
                metadata,
                subpartitions,
                parts,
                range.start,
            );
            (table.name().to_string(), to_delete)
        })
    }

    pub fn restore(&self, id: PartitionID, column: Column) {
        let column = Arc::new(column);
        for table in self.tables.read().unwrap().values() {
            table.restore(id, &column);
        }
    }

    #[allow(dead_code)]
    fn ingest_homogeneous(&self, table: &str, columns: HashMap<String, InputColumn>) {
        self.create_if_empty(table);
        let tables = self.tables.read().unwrap();
        tables.get(table).unwrap().ingest_homogeneous(columns)
    }

    #[allow(dead_code)]
    fn ingest_heterogeneous(&self, table: &str, columns: HashMap<String, Vec<RawVal>>) {
        self.create_if_empty(table);
        let tables = self.tables.read().unwrap();
        tables.get(table).unwrap().ingest_heterogeneous(columns)
    }

    pub fn drop_pending_tasks(&self) {
        let mut task_queue = self.task_queue.lock().unwrap();
        task_queue.clear();
    }

    pub fn mem_tree(&self, depth: usize, table: Option<String>) -> Vec<MemTreeTable> {
        let tables = self.tables.read().unwrap();
        tables
            .values()
            .filter(|t| table.as_ref().map(|name| name == t.name()).unwrap_or(true))
            .map(|table| table.mem_tree(depth))
            .collect()
    }

    pub fn stats(&self) -> Vec<TableStats> {
        let tables = self.tables.read().unwrap();
        tables.values().map(|table| table.stats()).collect()
    }

    pub fn gen_partition(&self, opts: &GenTable, p: u64) {
        opts.gen(self, p);
    }

    #[must_use]
    fn create_if_empty_no_ingest(&self, table: &str) -> Option<(i64, String)> {
        let exists = {
            let tables = self.tables.read().unwrap();
            tables.contains_key(table)
        };
        if !exists {
            {
                let mut tables = self.tables.write().unwrap();
                tables.insert(
                    table.to_string(),
                    Arc::new(Table::new(table, self.lru.clone(), Some(HashSet::new()))),
                );
            }
            Some((
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs() as i64,
                table.to_string(),
            ))
        } else {
            None
        }
    }

    fn create_if_empty(&self, table: &str) {
        let exists = {
            let tables = self.tables.read().unwrap();
            tables.contains_key(table)
        };
        if !exists {
            {
                let mut tables = self.tables.write().unwrap();
                tables.insert(
                    table.to_string(),
                    Arc::new(Table::new(table, self.lru.clone(), Some(HashSet::new()))),
                );
            }
            self.ingest_single(
                "_meta_tables",
                vec![
                    (
                        "timestamp".to_string(),
                        RawVal::Int(
                            SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap()
                                .as_secs() as i64,
                        ),
                    ),
                    ("name".to_string(), RawVal::Str(table.to_string())),
                ],
            );
        }
    }

    fn enforce_mem_limit(ldb: &Arc<InnerLocustDB>) {
        while ldb.running.load(Ordering::SeqCst) {
            let mut mem_usage_bytes: usize = {
                let tables = ldb.tables.read().unwrap();
                tables
                    .values()
                    .map(|table| table.heap_size_of_children())
                    .sum()
            };
            let mem_usage_metastore = match ldb.storage.as_ref() {
                Some(storage) => data_size(&*storage.meta_store().read().unwrap()),
                None => 0,
            };
            metrics::META_STORE_BYTES.set(mem_usage_metastore as f64);

            metrics::COLUMN_CACHE_BYTES.set(mem_usage_bytes as f64);
            metrics::COLUMN_CACHE_UTILIZATION.set(
                (mem_usage_bytes + mem_usage_metastore) as f64
                    / ldb.opts.mem_size_limit_tables as f64,
            );

            if (mem_usage_bytes + mem_usage_metastore) > ldb.opts.mem_size_limit_tables {
                info!(
                    "Evicting. mem_usage_bytes = {} mem_usage_metastore = {}",
                    mem_usage_bytes, mem_usage_metastore
                );
                while mem_usage_bytes > ldb.opts.mem_size_limit_tables {
                    match ldb.lru.evict() {
                        Some(victim) => {
                            let tables = ldb.tables.read().unwrap();
                            mem_usage_bytes -= tables[&victim.table].evict(&victim);
                        }
                        None => {
                            if ldb.opts.mem_size_limit_tables > 0 {
                                warn!(
                                    "Table memory usage is {} but failed to find column to evict!",
                                    mem_usage_bytes
                                );
                            }
                            break;
                        }
                    }
                }
                info!("mem_usage_bytes = {}", mem_usage_bytes);
            }
            thread::sleep(Duration::from_millis(1000));
        }
    }

    fn enforce_wal_limit(self: Arc<InnerLocustDB>) {
        let (wal_size, _) = &self.wal_size;
        let (pending_wal_flushes_mutex, pending_wal_flushes_condvar) = &self.pending_wal_flushes;
        while self.running.load(Ordering::SeqCst) {
            let wal_size = { *wal_size.lock().unwrap() };
            let pending_wal_flushes = mem::take(&mut *pending_wal_flushes_mutex.lock().unwrap());
            if wal_size > self.opts.max_wal_size_bytes || !pending_wal_flushes.is_empty() {
                self.wal_flush();
                for sender in pending_wal_flushes {
                    let _ = sender.send(());
                }
            } else {
                let pending_wal_flushes = pending_wal_flushes_mutex.lock().unwrap();
                if pending_wal_flushes.is_empty() {
                    let _ = pending_wal_flushes_condvar
                        .wait_timeout_while(
                            pending_wal_flushes,
                            Duration::from_millis(1000),
                            |pending_wal_flushes| {
                                pending_wal_flushes.is_empty()
                                    && self.running.load(Ordering::SeqCst)
                            },
                        )
                        .unwrap();
                }
            }
        }
        for sender in pending_wal_flushes_mutex.lock().unwrap().drain(..) {
            sender.send(()).unwrap();
        }
    }

    fn log_metrics(self: Arc<InnerLocustDB>) {
        let mut last_log_time = Instant::now() - Duration::from_secs(self.opts.metrics_interval);
        let mut last_value = HashMap::new();
        let metrics_table_name = match self.opts.metrics_table_name.clone() {
            Some(name) => name,
            None => return,
        };
        while self.running.load(Ordering::SeqCst) {
            if last_log_time.elapsed() >= Duration::from_secs(self.opts.metrics_interval) {
                last_log_time = Instant::now();
                metrics::WAL_SIZE_BYTES.set(*self.wal_size.0.lock().unwrap() as f64);
                metrics::WAL_UTILIZATION.set(
                    *self.wal_size.0.lock().unwrap() as f64 / self.opts.max_wal_size_bytes as f64,
                );

                let metrics = prometheus::gather();
                let timestamp = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs() as i64;
                let mut columns = HashMap::new();
                columns.insert(
                    "timestamp".to_string(),
                    ColumnBuffer {
                        data: ColumnData::I64(vec![timestamp]),
                    },
                );
                self.log_table_stats();

                for metric_family in metrics {
                    for metric in metric_family.get_metric() {
                        let value = if let Some(gauge) = metric.get_gauge().as_ref() {
                            gauge.value()
                        } else if let Some(counter) = metric.get_counter().as_ref() {
                            counter.value()
                        } else {
                            continue;
                        };
                        columns.insert(
                            metric_family.name().to_string(),
                            ColumnBuffer {
                                data: ColumnData::Dense(vec![value]),
                            },
                        );
                        match last_value.entry(metric_family.name().to_string()) {
                            Entry::Occupied(mut last_value) => {
                                let rate =
                                    (value - last_value.get()) / self.opts.metrics_interval as f64;
                                columns.insert(
                                    format!("rate.{}", metric_family.name()),
                                    ColumnBuffer {
                                        data: ColumnData::Dense(vec![rate]),
                                    },
                                );
                                last_value.insert(value);
                            }
                            Entry::Vacant(last_value) => {
                                last_value.insert(value);
                            }
                        }
                    }
                }

                let table_buffer = TableBuffer { len: 1, columns };
                let event_buffer = EventBuffer {
                    tables: HashMap::from([(metrics_table_name.clone(), table_buffer)]),
                };
                self.ingest_efficient(event_buffer);
            }

            thread::sleep(Duration::from_millis(250));
        }
    }

    pub fn opts(&self) -> &Options {
        &self.opts
    }

    pub fn disk_read_scheduler(&self) -> &Arc<DiskReadScheduler> {
        &self.disk_read_scheduler
    }

    pub fn perf_counter(&self) -> &PerfCounter {
        self.perf_counter.as_ref()
    }

    pub(crate) fn evict_cache(&self) -> usize {
        let tables = self.tables.read().unwrap();
        let mut bytes_evicted = 0;
        while let Some(victim) = self.lru.evict() {
            bytes_evicted += tables[&victim.table].evict(&victim);
        }
        bytes_evicted
    }

    fn log_table_stats(&self) {
        let mut total_rows = 0;
        let mut total_values = 0;
        let mut total_partition_columns = 0;
        let mut total_bytes = 0;
        let mut table_count = 0;
        let mut total_partitions = 0;
        for table in self.tables.read().unwrap().values() {
            table_count += 1;
            for partition in table.partitions.read().unwrap().values() {
                total_partitions += 1;

                let len = partition.range().len();
                total_rows += len;
                total_partition_columns += partition.col_handle_count();
                total_bytes += partition.total_size_bytes();
                total_values += partition.col_handle_count() * len;
            }
        }
        metrics::PARTITION_COUNT.set(total_partitions as f64);
        metrics::PARTITION_COLUMN_COUNT.set(total_partition_columns as f64);
        metrics::PARTITION_VALUES.set(total_values as f64);
        metrics::DATABASE_SIZE_BYTES.set(total_bytes as f64);
        metrics::TABLE_COUNT.set(table_count as f64);
        metrics::ROW_COUNT.set(total_rows as f64);
    }
}

impl Drop for InnerLocustDB {
    fn drop(&mut self) {
        info!("Stopped");
    }
}

#[derive(Default)]
struct PartitionBuilder {
    subpartition_metadata: Vec<(Vec<String>, u64)>,
    subpartitions: Vec<Vec<Arc<Column>>>,
    subpartition: Vec<Arc<Column>>,
    bytes: u64,
}

fn subpartition(
    opts: &Options,
    columns: Vec<Arc<Column>>,
) -> (Vec<SubpartitionMetadata>, Vec<Vec<Arc<Column>>>) {
    let mut acc = PartitionBuilder::default();
    fn create_subpartition(acc: &mut PartitionBuilder) {
        acc.subpartition_metadata.push((
            acc.subpartition
                .iter()
                .map(|c| c.name().to_string())
                .collect(),
            acc.bytes,
        ));
        acc.subpartitions.push(mem::take(&mut acc.subpartition));
        acc.bytes = 0;
    }

    let mut last_column = "".to_string();
    for column in columns {
        if column.name() > last_column.as_str() {
            last_column = column.name().to_string();
        }
        let size_bytes = column.heap_size_of_children() as u64;
        if acc.bytes + size_bytes > opts.max_partition_size_bytes {
            create_subpartition(&mut acc);
        }
        acc.subpartition.push(column);
        acc.bytes += size_bytes;
    }
    create_subpartition(&mut acc);

    let subpartition_metadata = if acc.subpartitions.len() == 1 {
        vec![SubpartitionMetadata {
            subpartition_key: "all".to_string(),
            size_bytes: acc.subpartition_metadata[0].1,
            last_column,
        }]
    } else {
        acc.subpartition_metadata
            .iter()
            .map(|(column_names, size)| {
                let mut last_column = column_names[0].to_string();
                for col in column_names {
                    if *col > last_column {
                        last_column = col.to_string();
                    }
                }
                let subpartition_key = if is_filesystem_safe(&last_column) {
                    format!("upto_{}", last_column)
                } else {
                    use sha2::{Digest, Sha256};
                    let mut hasher = Sha256::new();
                    hasher.update(last_column.as_bytes());
                    format!("{:x}", hasher.finalize())
                };
                SubpartitionMetadata {
                    subpartition_key,
                    size_bytes: *size,
                    last_column,
                }
            })
            .collect()
    };
    (subpartition_metadata, acc.subpartitions)
}

fn is_filesystem_safe(column_name: &str) -> bool {
    column_name.len() <= 64
        && column_name
            .chars()
            .all(|c| (c.is_alphanumeric() && c.is_lowercase()) || c == '_')
}
