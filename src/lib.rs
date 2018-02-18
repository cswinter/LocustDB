#[macro_use] extern crate nom;
#[macro_use] extern crate serde_derive;

extern crate bincode;
extern crate heapsize;
extern crate itertools;
extern crate num;
extern crate regex;
extern crate time;
extern crate seahash;
extern crate serde;
extern crate bit_vec;
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


use heapsize::HeapSizeOf;
use mem_store::batch::Batch;
use std::collections::HashMap;
use std::sync::{Mutex, RwLock};
use std::ops::DerefMut;
use std::str;
use mem_store::ingest::RawCol;
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

pub struct Ruba<T: DB + Clone> {
    tables: RwLock<HashMap<String, Table>>,
    storage: Box<T>,
}

impl <T: DB + Clone> Ruba<T> {
    pub fn new(storage: Box<T>, load_tabledata: bool) -> Ruba<T> {
        let existing_tables =
            if load_tabledata { Table::restore_from_db(20_000, &storage) }
            else { Table::load_table_metadata(20_000, &storage) };

        Ruba {
            tables: RwLock::new(existing_tables),
            storage: storage,
        }
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
            },
            err => Err(format!("Failed to parse query! {:?}", err).to_string()),
        }
    }

    pub fn load_table_data(&self) {
        let tables = self.tables.read().unwrap();
        for (_, table) in tables.iter() {
            table.load_table_data(&self.storage);
            println!("Finished loading {}", &table.name);
        }
        println!("Finished loading all table data!");
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

    pub fn restore_from_db<T: DB + Clone>(batch_size: usize, storage: &Box<T>) -> HashMap<String, Table> {
        let mut tables = Table::load_table_metadata(batch_size, storage);
        // populate tables
        for (_, table) in tables.iter_mut() {
            table.load_table_data(storage);
        }

        tables
    }


    pub fn load_table_metadata<T: DB + Clone>(batch_size: usize, storage: &Box<T>) -> HashMap<String, Table> {
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

    pub fn load_table_data<T: DB + Clone>(&self, storage: &Box<T>) {
        /*let tabledata_prefix = format!("BATCHES${}", &self.name);
        let iter = self.storage.iterator(IteratorMode::From(tabledata_prefix.as_bytes(), Direction::Forward));
        for (key, val) in iter {
            if !key.starts_with(tabledata_prefix.as_bytes()) { break; }
            let batch = deserialize::<Buffer>(&val).unwrap();
            self.load_batch(batch);
        }*/
        for buffer in storage.data(&self.name) {
            self.load_batch(buffer);
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

    fn batch_if_needed(&self, buffer: &mut Buffer) {
        if buffer.length < self.batch_size { return }
        self.batch(buffer);
    }

    fn batch_if_nonzero(&self) {
        let mut buffer = self.buffer.lock().unwrap();
        if buffer.length == 0 { return }
        self.batch(&mut buffer);
    }

    fn batch(&self, buffer: &mut Buffer) {
        let length = buffer.length;
        let buffer = std::mem::replace(buffer, Buffer::new());
        self.persist_batch(&buffer);
        let new_batch = Batch::new(buffer.buffer.into_iter()
                                   .map(|(name, raw_col)| (name, raw_col.finalize()))
                                   .collect());

        let mut batches = self.batches.write().unwrap();
        batches.push(new_batch);

        println!("Created size {} batch for {}!", length, self.name);
    }

    fn load_batch(&self, buffer: Buffer) {
        let new_batch = Batch::new(buffer.buffer.into_iter()
                                   .map(|(name, raw_col)| (name, raw_col.finalize()))
                                   .collect());

        let mut batches = self.batches.write().unwrap();
        batches.push(new_batch);
        println!("Loaded batch for {}", self.name)
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
    if tablename == "_meta_tables" { 1 }
    else if tablename == "_meta_queries" { 10 }
    else { batch_size }
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

pub trait DB {
    fn metadata(&self) -> Vec<&Metadata>;
    fn data(&self, table_name: &str) -> Vec<Buffer>;
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
