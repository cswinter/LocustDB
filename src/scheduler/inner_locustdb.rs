use std::collections::{HashMap, VecDeque};
use std::mem;
use std::str;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread;
use std::time::Duration;

use futures_core::*;
use futures_channel::oneshot;
use heapsize::HeapSizeOf;
use time;

use disk_store::interface::*;
use ingest::input_column::InputColumn;
use ingest::raw_val::RawVal;
use mem_store::partition::Partition;
use mem_store::table::*;
use mem_store::*;
use scheduler::*;
use trace::*;
use locustdb::Options;


pub struct InnerLocustDB {
    tables: RwLock<HashMap<String, Table>>,
    lru: LRU,
    pub storage: Arc<DiskStore>,

    opts: Options,

    next_partition_id: AtomicUsize,
    running: AtomicBool,
    idle_queue: Condvar,
    task_queue: Mutex<VecDeque<Arc<TaskState>>>,
}

struct TaskState {
    trace_builder: RwLock<Option<TraceBuilder>>,
    trace_sender: SharedSender<Trace>,
    task: Box<Task>,
}

impl Drop for TaskState {
    fn drop(&mut self) {
        let trace_builder = mem::replace(&mut *self.trace_builder.write().unwrap(), None);
        self.trace_sender.send(trace_builder.map(|tb| tb.finalize()).unwrap());
    }
}

impl InnerLocustDB {
    pub fn new(storage: Arc<DiskStore>, opts: &Options) -> InnerLocustDB {
        let lru = LRU::default();
        let existing_tables = Table::load_table_metadata(1 << 20, storage.as_ref(), &lru);
        let max_pid = existing_tables.iter().map(|(_, t)| t.max_partition_id()).max().unwrap_or(0);

        InnerLocustDB {
            tables: RwLock::new(existing_tables),
            lru,
            storage,
            running: AtomicBool::new(true),

            opts: opts.clone(),

            next_partition_id: AtomicUsize::new(max_pid as usize + 1),
            idle_queue: Condvar::new(),
            task_queue: Mutex::new(VecDeque::new()),
        }
    }

    pub fn start_worker_threads(locustdb: &Arc<InnerLocustDB>) {
        for id in 0..locustdb.opts.threads {
            let cloned = locustdb.clone();
            thread::spawn(move || InnerLocustDB::worker_loop(cloned, id));
        }
        let cloned = locustdb.clone();
        thread::spawn(move || InnerLocustDB::enforce_mem_limit(cloned));
    }

    pub fn snapshot(&self, table: &str) -> Option<Vec<Arc<Partition>>> {
        let tables = self.tables.read().unwrap();
        tables.get(table).map(|t| t.snapshot())
    }

    pub fn stop(&self) {
        // Acquire task_queue_guard to make sure that there are no threads that have checked self.running but not waited on idle_queue yet.
        info!("Stopping database...");
        let _ = self.task_queue.lock();
        self.running.store(false, Ordering::SeqCst);
        self.idle_queue.notify_all();
    }

    fn worker_loop(locustdb: Arc<InnerLocustDB>, thread_id: usize) {
        while locustdb.running.load(Ordering::SeqCst) {
            if let Some(task) = InnerLocustDB::await_task(locustdb.clone()) {
                if let Some(ref tb) = *task.trace_builder.read().unwrap() {
                    tb.activate();
                }
                {
                    trace_start!("Worker thread {}", thread_id);
                    task.task.execute();
                }
                if let Some(ref mut tb) = *task.trace_builder.write().unwrap() {
                    tb.collect();
                }
            }
        }
        drop(locustdb) // Make clippy happy
    }

    fn await_task(ldb: Arc<InnerLocustDB>) -> Option<Arc<TaskState>> {
        let mut task_queue = ldb.task_queue.lock().unwrap();
        while task_queue.is_empty() {
            if !ldb.running.load(Ordering::SeqCst) { return None; }
            task_queue = ldb.idle_queue.wait(task_queue).unwrap();
        }
        while let Some(task) = task_queue.pop_front() {
            if task.task.completed() {
                continue;
            }
            if task.task.multithreaded() {
                task_queue.push_front(task.clone());
            }
            if !task_queue.is_empty() {
                ldb.idle_queue.notify_one();
            }
            return Some(task);
        };
        None
    }

    pub fn schedule<T: Task + 'static>(&self, task: T) -> impl Future<Item=Trace, Error=oneshot::Canceled> {
        // This function may be entered by event loop thread so it's important it always returns quickly.
        // Since the task queue locks are never held for long, we should be fine.
        let trace_builder = RwLock::new(Some(start_toplevel("schedule")));
        let (trace_sender, trace_receiver) = oneshot::channel();
        let mut task_queue = self.task_queue.lock().unwrap();
        task_queue.push_back(Arc::new(TaskState {
            trace_sender: SharedSender::new(trace_sender),
            trace_builder,
            task: Box::new(task),
        }));
        self.idle_queue.notify_one();
        trace_receiver
    }


    pub fn store_partition(&self, tablename: &str, partition: Vec<Arc<Column>>) {
        self.create_if_empty(tablename);
        let tables = self.tables.read().unwrap();
        let table = tables.get(tablename).unwrap();
        let pid = self.next_partition_id.fetch_add(1, Ordering::SeqCst) as u64;
        self.storage.store_partition(pid, tablename, &partition);
        table.load_partition(Partition::new(pid, partition, self.lru.clone()));
    }

    pub fn ingest(&self, table: &str, row: Vec<(String, RawVal)>) {
        self.create_if_empty(table);
        let tables = self.tables.read().unwrap();
        tables.get(table).unwrap().ingest(row)
    }

    pub fn restore(&self, id: PartitionID, column: Column) {
        let column = Arc::new(column);
        for table in self.tables.read().unwrap().values() {
            table.restore(id, column.clone());
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
        tables.values().map(|table| { table.mem_tree(depth) }).collect()
    }

    pub fn stats(&self) -> Vec<TableStats> {
        let tables = self.tables.read().unwrap();
        tables.values().map(|table| table.stats()).collect()
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
                    Table::new(1 << 20, table, self.lru.clone()));
            }
            self.ingest("_meta_tables", vec![
                ("timestamp".to_string(), RawVal::Int(time::now().to_timespec().sec)),
                ("name".to_string(), RawVal::Str(table.to_string())),
            ]);
        }
    }

    fn enforce_mem_limit(ldb: Arc<InnerLocustDB>) {
        while ldb.running.load(Ordering::SeqCst) {
            let mut mem_usage_bytes: usize = {
                let tables = ldb.tables.read().unwrap();
                tables.values().map(|table| table.heap_size_of_children()).sum()
            };
            if mem_usage_bytes > ldb.opts.mem_size_limit_tables {
                info!("Evicting. mem_usage_bytes = {}", mem_usage_bytes);
                while mem_usage_bytes > ldb.opts.mem_size_limit_tables {
                    match ldb.lru.evict() {
                        Some(victim) => {
                            let tables = ldb.tables.read().unwrap();
                            for t in tables.values() {
                                mem_usage_bytes -= t.evict(&victim);
                            }
                        }
                        None => {
                            warn!("Failed to find column to evict!");
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
}

impl Drop for InnerLocustDB {
    fn drop(&mut self) {
        info!("Stopped");
    }
}

