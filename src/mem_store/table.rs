use std::collections::HashMap;
use std::ops::DerefMut;
use std::str;
use std::sync::{Mutex, RwLock};
use std;

use disk_store::db::DB;
use heapsize::HeapSizeOf;
use ingest::buffer::Buffer;
use ingest::input_column::InputColumn;
use ingest::raw_val::RawVal;
use mem_store::batch::Batch;


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

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn snapshot(&self) -> Vec<Batch> {
        let batches = self.batches.read().unwrap();
        batches.clone()
    }

    pub fn restore_from_db(batch_size: usize, storage: &DB) -> HashMap<String, Table> {
        let mut tables = Table::load_table_metadata(batch_size, storage);
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
    }

    pub fn load_table_data(&self, storage: &DB) {
        for buffer in storage.data(&self.name) {
            self.load_buffer(buffer);
        }
    }

    pub fn ingest(&self, row: Vec<(String, RawVal)>) {
        let mut buffer = self.buffer.lock().unwrap();
        buffer.push_row(row);
        self.batch_if_needed(buffer.deref_mut());
    }

    pub fn ingest_homogeneous(&self, columns: HashMap<String, InputColumn>) {
        let mut buffer = self.buffer.lock().unwrap();
        buffer.push_typed_cols(columns);
    }

    pub fn ingest_heterogeneous(&self, columns: HashMap<String, Vec<RawVal>>) {
        let mut buffer = self.buffer.lock().unwrap();
        buffer.push_untyped_cols(columns);
        self.batch_if_needed(&mut buffer);
    }

    pub fn load_batch(&self, batch: Batch) {
        let mut batches = self.batches.write().unwrap();
        batches.push(batch);
    }

    fn batch_if_needed(&self, buffer: &mut Buffer) {
        if buffer.len() < self.batch_size { return; }
        self.batch(buffer);
    }

    fn batch(&self, buffer: &mut Buffer) {
        let buffer = std::mem::replace(buffer, Buffer::new());
        self.persist_batch(&buffer);
        let new_batch = buffer.into();
        let mut batches = self.batches.write().unwrap();
        batches.push(new_batch);
    }

    fn load_buffer(&self, buffer: Buffer) {
        self.load_batch(buffer.into());
    }

    fn persist_batch(&self, _batch: &Buffer) {}

    #[allow(dead_code)]
    pub fn stats(&self) -> TableStats {
        let buffer = self.buffer.lock().unwrap();
        let batches = self.batches.read().unwrap();
        TableStats {
            name: self.name().to_string(),
            batches: batches.len() as u64,
            batches_bytes: batches.heap_size_of_children() as u64,
            buffer_length: buffer.len() as u64,
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

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Metadata {
    pub name: String,
    pub batch_count: u64,
}

#[derive(Debug)]
pub struct TableStats {
    pub name: String,
    pub batches: u64,
    pub batches_bytes: u64,
    pub buffer_length: u64,
    pub buffer_bytes: u64,
}


