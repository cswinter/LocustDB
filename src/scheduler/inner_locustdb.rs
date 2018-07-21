use std::collections::{HashMap, VecDeque};
use std::mem;
use std::str;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread;

use disk_store::interface::*;
use futures_core::*;
use futures_channel::oneshot;
use ingest::input_column::InputColumn;
use ingest::raw_val::RawVal;
use mem_store::batch::Partition;
use mem_store::table::*;
use mem_store::*;
use num_cpus;
use scheduler::*;
use time;
use trace::*;


pub struct InnerLocustDB {
    tables: RwLock<HashMap<String, Table>>,
    storage: Box<DiskStore>,

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
    pub fn new(storage: Box<DiskStore>, restore_tabledata: bool) -> InnerLocustDB {
        let existing_tables = if restore_tabledata {
            Table::load_table_metadata(1 << 20, storage.as_ref())
        } else {
            HashMap::new()
        };

        InnerLocustDB {
            tables: RwLock::new(existing_tables),
            storage,
            running: AtomicBool::new(true),
            idle_queue: Condvar::new(),
            task_queue: Mutex::new(VecDeque::new()),
        }
    }

    pub fn start_worker_threads(locustdb: &Arc<InnerLocustDB>, threads: Option<usize>) {
        for id in 0..threads.unwrap_or(num_cpus::get()) {
            let cloned = locustdb.clone();
            thread::spawn(move || InnerLocustDB::worker_loop(cloned, id));
        }
    }

    pub fn snapshot(&self, table: &str) -> Option<Vec<Arc<Partition>>> {
        let tables = self.tables.read().unwrap();
        tables.get(table).map(|t| t.snapshot())
    }

    pub fn stop(&self) {
        // Acquire task_queue_guard to make sure that there are no threads that have checked self.running but not waited on idle_queue yet.
        let _ = self.task_queue.lock();
        self.running.store(false, Ordering::SeqCst);
        self.idle_queue.notify_all();
    }

    fn worker_loop(locustdb: Arc<InnerLocustDB>, thread_id: usize) {
        while locustdb.running.load(Ordering::SeqCst) {
            if let Some(task) = locustdb.await_task() {
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

    fn await_task(&self) -> Option<Arc<TaskState>> {
        let mut task_queue = self.task_queue.lock().unwrap();
        while task_queue.is_empty() {
            if !self.running.load(Ordering::SeqCst) { return None; }
            task_queue = self.idle_queue.wait(task_queue).unwrap();
        }
        while let Some(task) = task_queue.pop_front() {
            if task.task.completed() {
                continue;
            }
            if task.task.multithreaded() {
                task_queue.push_front(task.clone());
            }
            if !task_queue.is_empty() {
                self.idle_queue.notify_one();
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

    pub fn store_partitions(&self, tablename: &str, partitions: Vec<Vec<Arc<Column>>>) {
        // TODO(clemens): pid needs to be unique across all invocations, compactions and restore from DB
        let mut pid = 0;
        self.create_if_empty(tablename);
        let tables = self.tables.read().unwrap();
        let table = tables.get(tablename).unwrap();
        for partition in partitions {
            self.storage.store_partition(pid, tablename, &partition);
            table.load_batch(Partition::new(pid, partition));
            pid += 1;
        }
    }

    pub fn ingest(&self, table: &str, row: Vec<(String, RawVal)>) {
        self.create_if_empty(table);
        let tables = self.tables.read().unwrap();
        tables.get(table).unwrap().ingest(row)
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
                    Table::new(1 << 20, table));
            }
            self.ingest("_meta_tables", vec![
                ("timestamp".to_string(), RawVal::Int(time::now().to_timespec().sec)),
                ("name".to_string(), RawVal::Str(table.to_string())),
            ]);
        }
    }
}

