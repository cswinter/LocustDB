use std::collections::HashMap;
use std::mem;
use std::ops::DerefMut;
use std::str;
use std::sync::Arc;
use std::sync::{Mutex, RwLock};

use disk_store::interface::DiskStore;
use disk_store::interface::PartitionID;
use heapsize::HeapSizeOf;
use ingest::buffer::Buffer;
use ingest::input_column::InputColumn;
use ingest::raw_val::RawVal;
use mem_store::batch::Partition;


pub struct Table {
    name: String,
    batch_size: usize,
    partitions: RwLock<Vec<Arc<Partition>>>,
    buffer: Mutex<Buffer>,
}

impl Table {
    pub fn new(batch_size: usize, name: &str) -> Table {
        Table {
            name: name.to_string(),
            batch_size: batch_size_override(batch_size, name),
            partitions: RwLock::new(Vec::new()),
            buffer: Mutex::new(Buffer::default()),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn snapshot(&self) -> Vec<Arc<Partition>> {
        let batches = self.partitions.read().unwrap();
        batches.clone()
    }

    pub fn load_table_metadata(batch_size: usize, storage: &DiskStore) -> HashMap<String, Table> {
        let mut tables = HashMap::new();
        for md in storage.load_metadata() {
            let table = tables
                .entry(md.tablename.clone())
                .or_insert(Table::new(batch_size, &md.tablename));
            table.insert_nonresident_partition(md.id, &md.columns);
        }
        tables
    }

    pub fn insert_nonresident_partition(&self, id: PartitionID, columns: &[String]) {
        let mut partitions = self.partitions.write().unwrap();
        partitions.push(Arc::new(Partition::nonresident(id, columns)));
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

    pub fn load_batch(&self, partition: Partition) {
        let mut batches = self.partitions.write().unwrap();
        batches.push(Arc::new(partition));
    }

    fn batch_if_needed(&self, buffer: &mut Buffer) {
        if buffer.len() < self.batch_size { return; }
        self.batch(buffer);
    }

    fn batch(&self, buffer: &mut Buffer) {
        let buffer = mem::replace(buffer, Buffer::default());
        self.persist_batch(&buffer);
        // TODO(clemens): get unique partition ID
        let new_partition = Partition::from_buffer(0, buffer);
        let mut partitions = self.partitions.write().unwrap();
        partitions.push(Arc::new(new_partition));
    }

    /*fn load_buffer(&self, buffer: Buffer) {
        self.load_batch(buffer.into());
    }*/

    fn persist_batch(&self, _batch: &Buffer) {}

    pub fn stats(&self) -> TableStats {
        let partitions = self.snapshot();
        let size_per_column = Table::size_per_column(&partitions);
        let buffer = self.buffer.lock().unwrap();
        TableStats {
            name: self.name().to_string(),
            // TODO(clemens): fix
            rows: 0,// batches.iter().map(|b| b.cols().get(0).map_or(0, |c| c.len())).sum(),
            batches: partitions.len(),
            batches_bytes: partitions.heap_size_of_children(),
            buffer_length: buffer.len(),
            buffer_bytes: buffer.heap_size_of_children(),
            size_per_column,
        }
    }

    fn size_per_column(_batches: &[Arc<Partition>]) -> Vec<(String, usize)> {
        // TODO(clemens): fix
        vec![]
        /*let mut sizes: HashMap<&str, usize> = HashMap::default();
        for batch in batches {
            for col in batch.cols() {
                let heapsize = col.heap_size_of_children();
                *sizes.entry(col.name()).or_insert(0) += heapsize;
            }
        }
        sizes.iter().map(|(name, size)| (name.to_string(), *size)).collect()*/
    }
}

fn batch_size_override(batch_size: usize, tablename: &str) -> usize {
    if tablename == "_meta_tables" { 1 } else if tablename == "_meta_queries" { 10 } else { batch_size }
}

impl HeapSizeOf for Table {
    fn heap_size_of_children(&self) -> usize {
        let batches_size = {
            let batches = self.partitions.read().unwrap();
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
    pub rows: usize,
    pub batches: usize,
    pub batches_bytes: usize,
    pub buffer_length: usize,
    pub buffer_bytes: usize,
    pub size_per_column: Vec<(String, usize)>,
}


