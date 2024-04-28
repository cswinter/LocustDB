use std::borrow::Cow;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread;
use std::time::{Duration, Instant};
use std::time::{SystemTime, UNIX_EPOCH};
use std::{mem, str};

use futures::channel::oneshot;
use futures::executor::block_on;
use inner_locustdb::meta_store::PartitionMetadata;
use itertools::Itertools;
use locustdb_serialization::event_buffer::{ColumnBuffer, ColumnData, EventBuffer, TableBuffer};

use crate::disk_store::storage::Storage;
use crate::disk_store::*;
use crate::engine::query_task::{BasicTypeColumn, QueryTask};
use crate::engine::Query;
use crate::ingest::colgen::GenTable;
use crate::ingest::input_column::InputColumn;
use crate::ingest::raw_val::RawVal;
use crate::locustdb::Options;
use crate::mem_store::partition::Partition;
use crate::mem_store::table::*;
use crate::perf_counter::PerfCounter;
use crate::scheduler::disk_read_scheduler::DiskReadScheduler;
use crate::scheduler::*;
use crate::{mem_store::*, NoopStorage};

use self::meta_store::SubpartitionMetadata;
use self::raw_col::MixedCol;
use self::wal_segment::WalSegment;

pub struct InnerLocustDB {
    tables: RwLock<HashMap<String, Table>>,
    lru: Lru,
    disk_read_scheduler: Arc<DiskReadScheduler>,

    storage: Option<Arc<Storage>>,

    wal_size: (Mutex<u64>, Condvar),

    opts: Options,

    perf_counter: Arc<PerfCounter>,

    running: AtomicBool,
    idle_queue: Condvar,
    task_queue: Mutex<VecDeque<Arc<dyn Task>>>,
}

impl InnerLocustDB {
    pub fn new(opts: &Options) -> InnerLocustDB {
        let lru = Lru::default();
        let perf_counter = Arc::new(PerfCounter::default());
        let (storage, tables) = match opts.db_path.clone() {
            Some(path) => {
                let perf_counter = perf_counter.clone();
                let lru = lru.clone();
                std::thread::spawn(move || {
                    let (storage, wal) = Storage::new(&path, perf_counter, false);
                    let tables = Table::restore_tables_from_disk(&storage, wal, &lru);
                    (Some(Arc::new(storage)), tables)
                })
                .join()
                .unwrap()
            }
            None => (None, HashMap::new()),
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

        let locustdb = InnerLocustDB {
            tables: RwLock::new(tables),
            lru,
            disk_read_scheduler,
            running: AtomicBool::new(true),

            storage,

            // TODO: doesn't take into account size of existing wal after restart
            wal_size: (Mutex::new(0), Condvar::new()),

            opts: opts.clone(),
            perf_counter,

            idle_queue: Condvar::new(),
            task_queue: Mutex::new(VecDeque::new()),
        };
        let _ = locustdb.create_if_empty_no_ingest("_meta_tables");
        locustdb
    }

    pub fn start_worker_threads(locustdb: &Arc<InnerLocustDB>) {
        for _ in 0..locustdb.opts.threads {
            let cloned = locustdb.clone();
            thread::spawn(move || InnerLocustDB::worker_loop(cloned));
        }
        let cloned = locustdb.clone();
        thread::spawn(move || InnerLocustDB::enforce_mem_limit(&cloned));
        let cloned = locustdb.clone();
        thread::spawn(move || InnerLocustDB::enforce_wal_limit(&cloned));
    }

    pub fn snapshot(&self, table: &str) -> Option<Vec<Arc<Partition>>> {
        let tables = self.tables.read().unwrap();
        tables.get(table).map(|t| t.snapshot())
    }

    pub fn full_snapshot(&self) -> Vec<Vec<Arc<Partition>>> {
        let tables = self.tables.read().unwrap();
        tables.values().map(|t| t.snapshot()).collect()
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

    pub fn ingest_single(&self, table: &str, row: Vec<(String, RawVal)>) {
        self.create_if_empty(table);
        let tables = self.tables.read().unwrap();
        tables.get(table).unwrap().ingest(row)
    }

    pub fn ingest_efficient(&self, mut events: EventBuffer) {
        let (wal_size, wal_condvar) = &self.wal_size;
        let mut wal_size = wal_size.lock().unwrap();
        while *wal_size > self.opts.max_wal_size_bytes {
            log::warn!("wal size limit exceeded, blocking ingestion");
            wal_size = wal_condvar.wait(wal_size).unwrap();
        }

        let mut _meta_tables_rows = vec![];
        for table in events.tables.keys() {
            if let Some(row) = self.create_if_empty_no_ingest(table) {
                _meta_tables_rows.push(row);
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
        // TODO: code duplicated in Table::restore_tables_from_disk
        for (table, data) in events.tables {
            let tables = self.tables.read().unwrap();
            let table = tables.get(&table).unwrap();
            let rows = data.len;
            // TODO: eliminate conversion
            let columns = data
                .columns
                .into_iter()
                .map(|(k, v)| {
                    let col = match v.data {
                        ColumnData::Dense(data) => {
                            if (data.len() as u64) < rows {
                                InputColumn::NullableFloat(
                                    rows,
                                    data.into_iter()
                                        .enumerate()
                                        .map(|(i, v)| (i as u64, v))
                                        .collect(),
                                )
                            } else {
                                InputColumn::Float(data)
                            }
                        }
                        ColumnData::Sparse(data) => InputColumn::NullableFloat(rows, data),
                        ColumnData::I64(data) => {
                            if (data.len() as u64) < rows {
                                InputColumn::NullableInt(
                                    rows,
                                    data.into_iter()
                                        .enumerate()
                                        .map(|(i, v)| (i as u64, v))
                                        .collect(),
                                )
                            } else {
                                InputColumn::Int(data)
                            }
                        }
                        ColumnData::String(data) => {
                            assert!(data.len() == rows as usize);
                            InputColumn::Str(data)
                        }
                        ColumnData::Empty => InputColumn::Null(rows as usize),
                        ColumnData::SparseI64(data) => InputColumn::NullableInt(rows, data),
                    };
                    (k, col)
                })
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
    pub(crate) fn wal_flush(&self) {
        let start_time = Instant::now();
        let tables = self.tables.read().unwrap();
        let mut new_partitions = Vec::new();
        let mut compactions = Vec::new();
        for table in tables.values() {
            if let Some(partition) = table.batch() {
                let columns: Vec<_> = partition
                    .col_handles()
                    .map(|c| c.try_get().as_ref().unwrap().clone())
                    .sorted_by(|a, b| a.name().cmp(b.name()));
                let (metadata, subpartitions) = subpartition(&self.opts, columns);
                let column_name_to_subpartition_index = subpartitions
                    .iter()
                    .enumerate()
                    .flat_map(|(i, subpartition)| {
                        subpartition
                            .iter()
                            .map(move |column| (column.name().to_string(), i))
                    })
                    .collect();
                let partition_metadata = PartitionMetadata {
                    id: partition.id,
                    tablename: table.name().to_string(),
                    len: partition.len(),
                    offset: partition.range().start,
                    subpartitions: metadata,
                    column_name_to_subpartition_index,
                };
                new_partitions.push((partition_metadata, subpartitions));
            }

            if let Some(compaction) = table.plan_compaction(self.opts.partition_combine_factor) {
                compactions.push((table.name(), table.next_partition_id(), compaction));
            }
        }

        if let Some(s) = self.storage.as_ref() {
            s.persist_partitions_delete_wal(new_partitions)
        }

        for (table, id, (range, parts)) in compactions {
            // get table, create new merged partition/sub-partitions (not registered with table)
            // - get names of all columns
            // - run query for each column, construct Column
            // - create subpartitions
            let colnames = tables[table].column_names(&parts);
            let mut columns = Vec::with_capacity(colnames.len());
            let data = tables[table].snapshot_parts(&parts);
            for column in &colnames {
                let query = Query::read_column(table, column);
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
                    "range={range:?}, column_builder.len() = {}, table = {table},  column = {column}",
                    column_builder.len(),

                );
                columns.push(column_builder.finalize(column));
            }
            let (metadata, subpartitions) = subpartition(&self.opts, columns.clone());
            // write subpartitions to disk, update metastore unlinking old partitions, delete old partitions
            if let Some(storage) = self.storage.as_ref() {
                storage.compact(table, id, metadata, subpartitions, &parts, range.start);
            }

            // replace old partitions with new partition
            tables[table].compact(id, range.start, columns, &parts);
        }

        log::info!("Performed wal flush in {:?}", start_time.elapsed());
    }

    pub fn restore(&self, id: PartitionID, column: Column) {
        let column = Arc::new(column);
        for table in self.tables.read().unwrap().values() {
            table.restore(id, &column);
        }
    }

    #[allow(dead_code)]
    pub fn ingest_homogeneous(&self, table: &str, columns: HashMap<String, InputColumn>) {
        self.create_if_empty(table);
        let tables = self.tables.read().unwrap();
        tables.get(table).unwrap().ingest_homogeneous(columns)
    }

    #[allow(dead_code)]
    pub fn ingest_heterogeneous(&self, table: &str, columns: HashMap<String, Vec<RawVal>>) {
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
                tables.insert(table.to_string(), Table::new(table, self.lru.clone()));
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
                tables.insert(table.to_string(), Table::new(table, self.lru.clone()));
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
            if mem_usage_bytes > ldb.opts.mem_size_limit_tables {
                info!("Evicting. mem_usage_bytes = {}", mem_usage_bytes);
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

    fn enforce_wal_limit(&self) {
        let (wal_size, wal_condvar) = &self.wal_size;
        let mut wal_size = wal_size.lock().unwrap();
        while self.running.load(Ordering::SeqCst) {
            if *wal_size < self.opts.max_wal_size_bytes {
                (wal_size, _) = wal_condvar
                    .wait_timeout(wal_size, Duration::from_secs(1))
                    .unwrap();
            } else {
                self.wal_flush();
                *wal_size = 0;
            }
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

    pub fn search_column_names(&self, table: &str, column: &str) -> Vec<String> {
        let tables = self.tables.read().unwrap();
        tables
            .get(table)
            .map_or(vec![], |t| t.search_column_names(column))
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

    for column in columns {
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
        }]
    } else {
        acc.subpartition_metadata
            .iter()
            .map(|(column_names, size)| {
                let first_col = column_names.iter().next().unwrap();
                let is_column_name_filesystem_safe = first_col.len() <= 64
                    && first_col
                        .chars()
                        .all(|c| (c.is_alphanumeric() && c.is_lowercase()) || c == '_');
                let subpartition_key = if column_names.len() == 1 && is_column_name_filesystem_safe
                {
                    format!("x{}", first_col)
                } else {
                    use sha2::{Digest, Sha256};
                    let mut hasher = Sha256::new();
                    for col in column_names {
                        hasher.update(col);
                    }
                    format!("{:x}", hasher.finalize())
                };
                SubpartitionMetadata {
                    subpartition_key,
                    size_bytes: *size,
                }
            })
            .collect()
    };
    (subpartition_metadata, acc.subpartitions)
}
