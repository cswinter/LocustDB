use std::collections::{HashMap, VecDeque};
use std::str;
use std::sync::{Arc, Mutex, RwLock, Condvar};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use disk_store::db::*;
use ingest::input_column::InputColumn;
use ingest::raw_val::RawVal;
use mem_store::batch::Batch;
use mem_store::table::*;
use num_cpus;
use scheduler::*;
use time;


pub struct InnerRuba {
    tables: RwLock<HashMap<String, Table>>,
    storage: Box<DB>,

    running: AtomicBool,
    idle_queue: Condvar,
    task_queue: Mutex<VecDeque<Arc<Task>>>,
}

impl InnerRuba {
    pub fn new(storage: Box<DB>, restore_tabledata: bool) -> InnerRuba {
        let existing_tables = if restore_tabledata {
            Table::load_table_metadata(20_000, storage.as_ref())
        } else {
            Table::restore_from_db(20_000, storage.as_ref())
        };

        InnerRuba {
            tables: RwLock::new(existing_tables),
            storage,
            running: AtomicBool::new(true),
            idle_queue: Condvar::new(),
            task_queue: Mutex::new(VecDeque::new()),
        }
    }

    pub fn start_worker_threads(ruba: &Arc<InnerRuba>) {
        for _ in 0..num_cpus::get() {
            let cloned = ruba.clone();
            thread::spawn(move || InnerRuba::worker_loop(cloned));
        }
    }

    pub fn snapshot(&self, table: &str) -> Option<Vec<Batch>> {
        let tables = self.tables.read().unwrap();
        tables.get(table).map(|t| t.snapshot())
    }

    pub fn stop(&self) {
        // Acquire task_queue_guard to make sure that there are no threads that have checked self.running but not waited on idle_queue yet.
        let _ = self.task_queue.lock();
        self.running.store(false, Ordering::SeqCst);
        self.idle_queue.notify_all();
    }

    fn worker_loop(ruba: Arc<InnerRuba>) {
        while ruba.running.load(Ordering::SeqCst) {
            if let Some(task) = ruba.await_task() {
                task.execute();
            }
        }
        drop(ruba) // Make clippy happy
    }

    fn await_task(&self) -> Option<Arc<Task>> {
        let mut task_queue = self.task_queue.lock().unwrap();
        while task_queue.is_empty() {
            if !self.running.load(Ordering::SeqCst) { return None; }
            task_queue = self.idle_queue.wait(task_queue).unwrap();
        }
        while let Some(task) = task_queue.pop_front() {
            if task.completed() {
                continue;
            }
            if task.multithreaded() {
                task_queue.push_front(task.clone());
            }
            if !task_queue.is_empty() {
                self.idle_queue.notify_one();
            }
            return Some(task);
        };
        None
    }

    pub fn schedule(&self, task: Arc<Task>) {
        // This function may be entered by event loop thread so it's important it always returns quickly.
        // Since the task queue locks are never held for long, we should be fine.
        let mut task_queue = self.task_queue.lock().unwrap();
        task_queue.push_back(task);
        self.idle_queue.notify_one();
    }

    pub fn load_batches(&self, table: &str, batches: Vec<Batch>) {
        self.create_if_empty(table);
        let tables = self.tables.read().unwrap();
        let table = tables.get(table).unwrap();
        for batch in batches {
            table.load_batch(batch);
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
                tables.insert(table.to_string(), Table::new(10_000, table, Metadata { batch_count: 0, name: table.to_string() }));
            }
            self.ingest("_meta_tables", vec![
                ("timestamp".to_string(), RawVal::Int(time::now().to_timespec().sec)),
                ("name".to_string(), RawVal::Str(table.to_string())),
            ]);
        }
    }
}

