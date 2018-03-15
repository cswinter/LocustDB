#![feature(conservative_impl_trait)]
#[macro_use]
extern crate nom;
#[macro_use]
extern crate serde_derive;

extern crate bincode;
extern crate heapsize;
extern crate itertools;
extern crate num;
extern crate regex;
extern crate time;
extern crate seahash;
extern crate serde;
extern crate bit_vec;
extern crate num_cpus;
extern crate futures;
extern crate futures_channel;
// extern crate tempdir;

pub mod parser;
pub mod mem_store;
pub mod query_engine;

mod engine;
mod util;
mod value;
mod expression;
mod aggregator;
mod limit;
mod task;
mod shared_sender;


use heapsize::HeapSizeOf;
use mem_store::batch::Batch;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, RwLock, Condvar};
use std::ops::DerefMut;
use std::str;
use std::thread;
use futures_channel::oneshot;
use futures::*;
use mem_store::ingest::RawCol;
use mem_store::csv_loader::CSVIngestionTask;
use query_engine::{Query, QueryResult};
// use rocksdb::{DB, Options, WriteBatch, IteratorMode, Direction};
// use tempdir::TempDir;

pub use mem_store::ingest::RawVal as InputValue;
use mem_store::ingest::RawVal;

pub enum InputColumn {
    Int(Vec<i64>),
    Str(Vec<String>),
    Null(usize),
}

pub struct Ruba {
    tables: RwLock<HashMap<String, Table>>,
    idle_queue: (Mutex<bool>, Condvar),
    task_queue: RwLock<VecDeque<Arc<task::Task>>>,
    storage: Box<DB>,
}


impl Ruba {
    pub fn memory_only() -> Arc<Ruba> {
        Ruba::new(Box::new(NoopStorage), false)
    }

    pub fn new(storage: Box<DB>, load_tabledata: bool) -> Arc<Ruba> {
        let existing_tables =
            if load_tabledata {
                Table::restore_from_db(20_000, storage.as_ref())
            } else {
                Table::load_table_metadata(20_000, storage.as_ref())
            };

        let ruba = Arc::new(Ruba {
            tables: RwLock::new(existing_tables),
            idle_queue: (Mutex::new(false), Condvar::new()),
            task_queue: RwLock::new(VecDeque::new()),
            storage: storage,
        });
        for _ in 0..num_cpus::get() {
            let cloned = ruba.clone();
            thread::spawn(move || Ruba::worker_loop(cloned));
        }

        return ruba;
    }

    pub fn run_query(&self, query: &str) -> Result<QueryResult, String> {
        match parser::parse_query(query.as_bytes()) {
            nom::IResult::Done(_remaining, query) => {
                let tables = self.tables.read().unwrap();
                // TODO(clemens): extend query language with from clause
                match tables.get(&query.table) {
                    Some(table) => table.run_query(query),
                    None => Err(format!("Table `{}` not found!", query.table).to_string()),
                }
            }
            err => Err(format!("Failed to parse query! {:?}", err).to_string()),
        }
    }

    pub fn load_table_data(&self) {
        let tables = self.tables.read().unwrap();
        for (_, table) in tables.iter() {
            table.load_table_data(self.storage.as_ref());
            println!("Finished loading {}", &table.name);
        }
        println!("Finished loading all table data!");
    }

    pub fn load_csv(ruba: Arc<Ruba>,
                    path: &str,
                    table_name: &str,
                    chunk_size: usize) -> impl Future<Item=(), Error=oneshot::Canceled> {
        let (sender, receiver) = oneshot::channel();
        let task = CSVIngestionTask::new(
            path.to_string(),
            table_name.to_string(),
            chunk_size,
            ruba.clone(),
            shared_sender::SharedSender::new(sender));
        ruba.schedule(Arc::new(task));
        receiver
    }

    pub ( crate ) fn load_batches(&self, table: &str, batches: Vec<Batch>) {
        self.create_if_empty(table);
        let tables = self.tables.read().unwrap();
        let table = tables.get(table).unwrap();
        for batch in batches.into_iter() {
            table.load_batch(batch);
        }
    }

    pub fn ingest(&self, table: &str, row: Vec<(String, InputValue)>) {
        self.create_if_empty(table);
        let tables = self.tables.read().unwrap();
        tables.get(table).unwrap().ingest(row)
    }


    pub fn ingest_homogeneous(&self, table: &str, columns: HashMap<String, InputColumn>) {
        self.create_if_empty(table);
        let tables = self.tables.read().unwrap();
        tables.get(table).unwrap().ingest_homogeneous(columns)
    }

    pub fn ingest_heterogeneous(&self, table: &str, columns: HashMap<String, Vec<InputValue>>) {
        self.create_if_empty(table);
        let tables = self.tables.read().unwrap();
        tables.get(table).unwrap().ingest_heterogeneous(columns)
    }

    pub fn stats(&self) -> Stats {
        let tables = self.tables.read().unwrap();
        Stats {
            tables: tables.values().map(|table| table.stats()).collect()
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
                tables.insert(table.to_string(), Table::new(10_000, table, Metadata { batch_count: 0, name: table.to_string() }));
            }
            self.ingest("_meta_tables", vec![
                ("timestamp".to_string(), RawVal::Int(time::now().to_timespec().sec)),
                ("name".to_string(), RawVal::Str(table.to_string())),
            ]);
        }
    }

    fn worker_loop(ruba: Arc<Ruba>) {
        loop {
            if let Some(task) = ruba.await_task() {
                task.execute();
            }
        }
    }

    fn await_task(&self) -> Option<Arc<task::Task>> {
        let &(ref lock, ref cvar) = &self.idle_queue;
        let mut task_available = lock.lock().unwrap();
        while !*task_available {
            task_available = cvar.wait(task_available).unwrap();
        }
        let mut task_queue_guard = self.task_queue.write().unwrap();
        let task_queue = task_queue_guard.deref_mut();
        while let Some(task) = task_queue.pop_front() {
            if task.completed() { continue; }
            if task.multithreaded() {
                task_queue.push_front(task.clone());
            }
            *task_available = task_queue.len() > 0;
            if *task_available {
                cvar.notify_one();
            }
            return Some(task);
        }
        None
    }

    fn schedule(&self, task: Arc<task::Task>) {
        let &(ref lock, ref cvar) = &self.idle_queue;
        let mut task_available = lock.lock().unwrap();
        let mut task_queue_guard = self.task_queue.write().unwrap();
        let task_queue = task_queue_guard.deref_mut();
        task_queue.push_back(task);
        *task_available = true;
        cvar.notify_one();
    }
}

pub struct Table {
    name: String,
    batch_size: usize,
    #[allow(dead_code)]
    metadata: RwLock<Metadata>,
    batches: RwLock<Vec<Batch>>,
    buffer: Mutex<Buffer>,
}

impl Table {
    pub fn new(batch_size: usize, name: &str, metadata: Metadata) -> Table {
        Table {
            name: name.to_string(),
            batch_size: batch_size_override(batch_size, name),
            batches: RwLock::new(Vec::new()),
            buffer: Mutex::new(Buffer::new()),
            metadata: RwLock::new(metadata),
        }
    }

    pub fn restore_from_db(batch_size: usize, storage: &DB) -> HashMap<String, Table> {
        let mut tables = Table::load_table_metadata(batch_size, storage);
        // populate tables
        for (_, table) in tables.iter_mut() {
            table.load_table_data(storage);
        }

        tables
    }


    pub fn load_table_metadata(batch_size: usize, storage: &DB) -> HashMap<String, Table> {
        let mut tables = HashMap::new();
        for md in storage.metadata() {
            tables.insert(md.name.to_string(), Table::new(batch_size, &md.name, md.clone()));
        }
        tables

        // load metadata
        /*let metadata_prefix = "METADATA$".as_bytes();
        let iter = storage.iterator(IteratorMode::From(metadata_prefix, Direction::Forward));
        for (key, val) in iter {
            if !key.starts_with(metadata_prefix) { break; }
            let table_name = str::from_utf8(&key[metadata_prefix.len()..]).unwrap();
            let metadata = deserialize::<Metadata>(&val).unwrap();
        }*/
    }

    pub fn load_table_data(&self, storage: &DB) {
        /*let tabledata_prefix = format!("BATCHES${}", &self.name);
        let iter = self.storage.iterator(IteratorMode::From(tabledata_prefix.as_bytes(), Direction::Forward));
        for (key, val) in iter {
            if !key.starts_with(tabledata_prefix.as_bytes()) { break; }
            let batch = deserialize::<Buffer>(&val).unwrap();
            self.load_batch(batch);
        }*/
        for buffer in storage.data(&self.name) {
            self.load_buffer(buffer);
        }
    }

    pub fn ingest(&self, row: Vec<(String, InputValue)>) {
        let mut buffer = self.buffer.lock().unwrap();
        let buffer_length = buffer.length;
        for (name, input_val) in row {
            let buffered_col = buffer.buffer.entry(name).or_insert_with(|| RawCol::with_nulls(buffer_length));
            buffered_col.push(input_val);
        }
        buffer.length += 1;
        buffer.extend_to_largest();
        self.batch_if_needed(&mut buffer);
    }

    pub fn ingest_homogeneous(&self, columns: HashMap<String, InputColumn>) {
        let mut buffer = self.buffer.lock().unwrap();
        let buffer_length = buffer.length;
        let mut new_length = 0;
        for (name, input_col) in columns {
            let buffered_col = buffer.buffer.entry(name).or_insert_with(|| RawCol::with_nulls(buffer_length));
            match input_col {
                InputColumn::Int(vec) => buffered_col.push_ints(vec),
                InputColumn::Str(vec) => buffered_col.push_strings(vec),
                InputColumn::Null(c) => buffered_col.push_nulls(c),
            }
            new_length = std::cmp::max(new_length, buffered_col.len())
        }
        buffer.length = new_length;
        buffer.extend_to_largest();
        self.batch_if_needed(buffer.deref_mut());
    }

    pub fn ingest_heterogeneous(&self, columns: HashMap<String, Vec<InputValue>>) {
        let mut buffer = self.buffer.lock().unwrap();
        let buffer_length = buffer.length;
        let mut new_length = 0;
        for (name, input_vals) in columns {
            let buffered_col = buffer.buffer.entry(name).or_insert_with(|| RawCol::with_nulls(buffer_length));
            for input_val in input_vals {
                buffered_col.push(input_val);
            }
            new_length = std::cmp::max(new_length, buffered_col.len())
        }
        buffer.length = new_length;
        buffer.extend_to_largest();
        self.batch_if_needed(&mut buffer);
    }


    pub fn run_query(&self, query: Query) -> Result<QueryResult, String> {
        self.batch_if_nonzero();
        let batches = self.batches.read().unwrap();
        let mut compiled_query = query.compile(&batches);
        let result = compiled_query.run();
        Ok(result)
    }

    pub fn load_batch(&self, batch: Batch) {
        let mut batches = self.batches.write().unwrap();
        batches.push(batch);
        // println!("Loaded batch for {}", self.name)
    }

    fn batch_if_needed(&self, buffer: &mut Buffer) {
        if buffer.length < self.batch_size { return; }
        self.batch(buffer);
    }

    fn batch_if_nonzero(&self) {
        let mut buffer = self.buffer.lock().unwrap();
        if buffer.length == 0 { return; }
        self.batch(&mut buffer);
    }

    fn batch(&self, buffer: &mut Buffer) {
        // let length = buffer.length;
        let buffer = std::mem::replace(buffer, Buffer::new());
        self.persist_batch(&buffer);
        let new_batch = Batch::new(buffer.buffer.into_iter()
            .map(|(name, raw_col)| (name, raw_col.finalize()))
            .collect());

        let mut batches = self.batches.write().unwrap();
        batches.push(new_batch);

        // println!("Created size {} batch for {}!", length, self.name);
    }

    fn load_buffer(&self, buffer: Buffer) {
        let new_batch = Batch::new(buffer.buffer.into_iter()
            .map(|(name, raw_col)| (name, raw_col.finalize()))
            .collect());
        self.load_batch(new_batch);
    }

    fn persist_batch(&self, _batch: &Buffer) {
        /*let encoded = serialize(batch, Infinite).unwrap();
        let mut metadata = self.metadata.write().unwrap();
        metadata.batch_count = metadata.batch_count + 1;

        let mut transaction = WriteBatch::default();
        transaction.put(format!("METADATA${}", self.name).as_bytes(), &serialize(metadata.deref(), Infinite).unwrap());
        transaction.put(format!("BATCHES${}{}", self.name, metadata.batch_count).as_bytes(), &encoded);
        self.storage.write(transaction);*/
    }

    pub fn stats(&self) -> TableStats {
        let buffer = self.buffer.lock().unwrap();
        let batches = self.batches.read().unwrap();
        TableStats {
            name: self.name.clone(),
            batches: batches.len() as u64,
            batches_bytes: batches.heap_size_of_children() as u64,
            buffer_length: buffer.length as u64,
            buffer_bytes: buffer.heap_size_of_children() as u64,
        }
    }
}

fn batch_size_override(batch_size: usize, tablename: &str) -> usize {
    if tablename == "_meta_tables" { 1 } else if tablename == "_meta_queries" { 10 } else { batch_size }
}

impl HeapSizeOf for Table {
    fn heap_size_of_children(&self) -> usize {
        let batches_size = {
            let batches = self.batches.read().unwrap();
            batches.heap_size_of_children()
        };
        let buffer_size = {
            let buffer = self.buffer.lock().unwrap();
            buffer.heap_size_of_children()
        };
        batches_size + buffer_size
    }
}

pub trait DB: Sync + Send + 'static {
    fn metadata(&self) -> Vec<&Metadata>;
    fn data(&self, table_name: &str) -> Vec<Buffer>;
}

struct NoopStorage;

impl DB for NoopStorage {
    fn metadata(&self) -> Vec<&Metadata> { Vec::new() }
    fn data(&self, _: &str) -> Vec<Buffer> { Vec::new() }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Metadata {
    pub name: String,
    pub batch_count: u64,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct Buffer {
    buffer: HashMap<String, RawCol>,
    length: usize,
}

impl Buffer {
    fn new() -> Buffer {
        Buffer {
            buffer: HashMap::new(),
            length: 0,
        }
    }

    fn extend_to_largest(&mut self) {
        let target_length = self.length;
        for buffered_col in self.buffer.values_mut() {
            let col_length = buffered_col.len();
            if col_length < target_length {
                buffered_col.push_nulls(target_length - col_length)
            }
        }
    }
}

impl HeapSizeOf for Buffer {
    fn heap_size_of_children(&self) -> usize {
        self.buffer.heap_size_of_children()
    }
}

#[derive(Debug)]
pub struct Stats {
    pub tables: Vec<TableStats>,
}

#[derive(Debug)]
pub struct TableStats {
    pub name: String,
    pub batches: u64,
    pub batches_bytes: u64,
    pub buffer_length: u64,
    pub buffer_bytes: u64,
}


/*#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage() {
        let tmpdir = TempDir::new("ruba_test_db").unwrap().into_path();
        {
            let ruba = Ruba::new(&tmpdir.to_str().unwrap(), false);
            for i in 0..10000 {
                ruba.ingest("default", vec![("timestamp".to_string(), mem_store::ingest::RawVal::Int(i))]);
            }
        }

        let ruba = Arc::new(Ruba::new(&tmpdir.to_str().unwrap(), false));
        let ruba_clone = ruba.clone();
        thread::spawn(move || { ruba_clone.load_table_data() });
        thread::sleep(stdtime::Duration::from_millis(1000));
        let result = ruba.run_query("select sum(timestamp) from default limit 10000;").unwrap();
        assert_eq!(result.stats.rows_scanned, 10000);
    }


    #[test]
    fn test_select_start_query() {
        let tmpdir = TempDir::new("ruba_test_db").unwrap().into_path();
        let ruba = Ruba::new(&tmpdir.to_str().unwrap(), false);
        for i in 0..10000 {
            ruba.ingest("requests", vec![
                ("timestamp".to_string(), mem_store::ingest::RawVal::Int(i)),
                ("path".to_string(), mem_store::ingest::RawVal::Str("/test".to_string())),
            ]);
        }
        for i in 0..10000 {
            ruba.ingest("requests", vec![
                ("timestamp".to_string(), mem_store::ingest::RawVal::Int(i)),
                ("code".to_string(), mem_store::ingest::RawVal::Int(420)),
            ]);
        }
        
        let result = ruba.run_query("select * from requests limit 10;").unwrap();
        assert_eq!(result.colnames.len(), 3);
        assert_eq!(result.rows.len(), 10);
        assert_eq!(result.rows[0].len(), 3)
    }

    #[test]
    fn test_batch_before_query() {
        let tmpdir = TempDir::new("ruba_test_db").unwrap().into_path();
        let ruba = Ruba::new(&tmpdir.to_str().unwrap(), false);
        for i in 0..100 {
            ruba.ingest("requests", vec![
                ("timestamp".to_string(), mem_store::ingest::RawVal::Int(i)),
                ("path".to_string(), mem_store::ingest::RawVal::Str("/test".to_string())),
            ]);
        }
        let result = ruba.run_query("select * from requests;").unwrap();
        assert_eq!(result.stats.rows_scanned, 100);
    }

    #[test]
    fn test_meta_tables() {
        let tmpdir = TempDir::new("ruba_test_db").unwrap().into_path();
        let ruba = Ruba::new(&tmpdir.to_str().unwrap(), false);
        for i in 0..100 {
            ruba.ingest("requests", vec![
                ("timestamp".to_string(), mem_store::ingest::RawVal::Int(i)),
                ("path".to_string(), mem_store::ingest::RawVal::Str("/test".to_string())),
            ]);
        }
        let result = ruba.run_query("select * from _meta_tables;").unwrap();
        assert_eq!(result.stats.rows_scanned, 2);
    }
}*/
