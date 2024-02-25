use std::borrow::Cow;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{mem, str};

use itertools::Itertools;

use crate::disk_store::interface::*;
use crate::disk_store::v2::{Storage, StorageV2, WALSegment};
use crate::ingest::colgen::GenTable;
use crate::ingest::input_column::InputColumn;
use crate::ingest::raw_val::RawVal;
use crate::locustdb::Options;
use crate::logging_client::ColumnData;
use crate::logging_client::EventBuffer;
use crate::mem_store::partition::Partition;
use crate::mem_store::table::*;
use crate::perf_counter::PerfCounter;
use crate::scheduler::disk_read_scheduler::DiskReadScheduler;
use crate::scheduler::*;
use crate::{mem_store::*, NoopStorage};

use self::partition::ColumnLocator;

pub struct InnerLocustDB {
    tables: RwLock<HashMap<String, Table>>,
    lru: Lru,
    disk_read_scheduler: Arc<DiskReadScheduler>,

    storage_v2: Option<Arc<StorageV2>>,
    wal_size: AtomicU64,

    opts: Options,

    perf_counter: Arc<PerfCounter>,

    next_partition_id: AtomicUsize,
    running: AtomicBool,
    idle_queue: Condvar,
    task_queue: Mutex<VecDeque<Arc<dyn Task>>>,
}

impl InnerLocustDB {
    pub fn new(opts: &Options) -> InnerLocustDB {
        let lru = Lru::default();
        let perf_counter = Arc::new(PerfCounter::default());
        let storage_v2 = opts.db_v2_path.as_ref().map(|path| {
            let (storage, wal) = StorageV2::new(path, perf_counter.clone(), false);
            (Arc::new(storage), wal)
        });
        let (storage_v2, existing_tables) = match storage_v2 {
            Some((storage, wal_segments)) => {
                let tables = Table::restore_tables_from_disk(&storage, wal_segments, &lru);
                (Some(storage), tables)
            }
            None => (None, HashMap::new()),
        };
        let max_pid = existing_tables
            .values()
            .map(|t| t.max_partition_id())
            .max()
            .unwrap_or(0);
        let disk_read_scheduler = Arc::new(DiskReadScheduler::new(
            storage_v2
                .clone()
                .map(|s| s as Arc<dyn ColumnLoader>)
                .unwrap_or(Arc::new(NoopStorage)),
            lru.clone(),
            opts.read_threads,
            !opts.mem_lz4,
        ));

        InnerLocustDB {
            tables: RwLock::new(existing_tables),
            lru,
            disk_read_scheduler,
            running: AtomicBool::new(true),

            storage_v2,
            // TODO: doesn't take into account size of existing wal after restart
            wal_size: AtomicU64::new(0),

            opts: opts.clone(),
            perf_counter,

            next_partition_id: AtomicUsize::new(max_pid as usize + 1),
            idle_queue: Condvar::new(),
            task_queue: Mutex::new(VecDeque::new()),
        }
    }

    pub fn start_worker_threads(locustdb: &Arc<InnerLocustDB>) {
        for _ in 0..locustdb.opts.threads {
            let cloned = locustdb.clone();
            thread::spawn(move || InnerLocustDB::worker_loop(cloned));
        }
        let cloned = locustdb.clone();
        thread::spawn(move || InnerLocustDB::enforce_mem_limit(&cloned));
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
        // Acquire task_queue_guard to make sure that there are no threads that have checked self.running but not waited on idle_queue yet.
        info!("Stopping database...");
        let _guard = self.task_queue.lock();
        self.running.store(false, Ordering::SeqCst);
        self.idle_queue.notify_all();
        self.wal_flush();
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

    pub fn store_partition(&self, tablename: &str, partition: Vec<Arc<Column>>) {
        self.create_if_empty(tablename);
        let tables = self.tables.read().unwrap();
        let table = tables.get(tablename).unwrap();
        let pid = self.next_partition_id.fetch_add(1, Ordering::SeqCst) as u64;
        if self.storage_v2.is_some() {
            todo!();
        }
        let (new_partition, keys) = Partition::new(table.name(), pid, partition, self.lru.clone());
        table.load_partition(new_partition);
        for (id, column) in keys {
            self.lru.put(ColumnLocator::new(table.name(), id, &column));
        }
    }

    pub fn ingest_single(&self, table: &str, row: Vec<(String, RawVal)>) {
        self.create_if_empty(table);
        let tables = self.tables.read().unwrap();
        tables.get(table).unwrap().ingest(row)
    }

    pub fn ingest_efficient(&self, events: EventBuffer) {
        if let Some(storage) = &self.storage_v2 {
            let bytes_written = storage.persist_wal_segment(WALSegment {
                id: 0,
                data: Cow::Borrowed(&events),
            });
            self.wal_size.fetch_add(bytes_written, Ordering::SeqCst);
        }
        for (table, data) in events.tables {
            self.create_if_empty(&table);
            let tables = self.tables.read().unwrap();
            let table = tables.get(&table).unwrap();
            // TODO: eliminate conversion
            let columns = data
                .columns
                .into_iter()
                .map(|(k, v)| {
                    let col = match v.data {
                        ColumnData::Dense(data) => InputColumn::Float(data),
                        ColumnData::Sparse(_) => {
                            todo!("INGESTION OF SPARSE VALUES NOT IMPLEMENTED")
                        }
                    };
                    (k, col)
                })
                .collect();
            table.ingest_homogeneous(columns);
        }
        if self.wal_size.load(Ordering::SeqCst) > self.opts.max_wal_size_bytes {
            self.wal_flush();
        }
    }

    pub(crate) fn wal_flush(&self) {
        // TODO: race conditions, need careful locking here
        self.wal_size.store(0, Ordering::SeqCst);
        let mut new_partitions = Vec::new();
        let tables = self.tables.write().unwrap();
        for table in tables.values() {
            if let Some(partition) = table.batch() {
                let columns: Vec<_> = partition
                    .col_handles()
                    .map(|c| c.try_get().as_ref().unwrap().clone())
                    .sorted_by(|a, b| a.name().cmp(b.name()));

                #[derive(Default)]
                struct PartitionBuilder {
                    subpartition_metadata: Vec<SubpartitionMeatadata>,
                    subpartitions: Vec<Vec<Arc<Column>>>,
                    subpartition: Vec<Arc<Column>>,
                    bytes: u64,
                }

                let mut acc = PartitionBuilder::default();
                fn push(acc: &mut PartitionBuilder, subpartition_key: String) {
                    acc.subpartition_metadata.push(SubpartitionMeatadata {
                        column_names: acc
                            .subpartition
                            .iter()
                            .map(|c| c.name().to_string())
                            .collect(),
                        size_bytes: acc.bytes,
                        subpartition_key,
                    });
                    acc.subpartitions.push(mem::take(&mut acc.subpartition));
                    acc.bytes = 0;
                }

                for column in columns {
                    let size_bytes = column.heap_size_of_children() as u64;
                    if acc.bytes + size_bytes > self.opts.max_partition_size_bytes {
                        push(&mut acc, column.name().to_string());
                    }
                    acc.subpartition.push(column);
                    acc.bytes += size_bytes;
                }

                let subpartition_key = if acc.subpartitions.is_empty() {
                    "all".to_string()
                } else if acc.subpartition.len() == 1 {
                    // TODO: sanitize
                    acc.subpartition[0].name().to_string()
                } else {
                    use sha2::{Digest, Sha256};
                    let mut hasher = Sha256::new();
                    for col in &acc.subpartition {
                        hasher.update(col.name());
                    }
                    format!("{:x}", hasher.finalize())
                };
                push(&mut acc, subpartition_key);

                let partition_metadata = PartitionMetadata {
                    id: partition.id,
                    tablename: table.name().to_string(),
                    len: partition.len(),
                    subpartitions: acc.subpartition_metadata,
                };
                new_partitions.push((partition_metadata, acc.subpartitions));
            }
        }

        self.storage_v2
            .as_ref()
            .unwrap()
            .persist_partitions_delete_wal(new_partitions)
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

    pub fn mem_tree(&self, depth: usize) -> Vec<MemTreeTable> {
        let tables = self.tables.read().unwrap();
        tables.values().map(|table| table.mem_tree(depth)).collect()
    }

    pub fn stats(&self) -> Vec<TableStats> {
        let tables = self.tables.read().unwrap();
        tables.values().map(|table| table.stats()).collect()
    }

    pub fn gen_partition(&self, opts: &GenTable, p: u64) {
        opts.gen(self, p);
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

    pub fn max_partition_id(&self) -> u64 {
        self.next_partition_id.load(Ordering::SeqCst) as u64
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
}

impl Drop for InnerLocustDB {
    fn drop(&mut self) {
        info!("Stopped");
    }
}
