use std::collections::HashMap;
use std::mem;
use std::ops::DerefMut;
use std::str;
use std::sync::Arc;
use std::sync::{Mutex, RwLock};

use disk_store::interface::*;
use heapsize::HeapSizeOf;
use ingest::buffer::Buffer;
use ingest::input_column::InputColumn;
use ingest::raw_val::RawVal;
use mem_store::partition::{Partition, ColumnKey};
use mem_store::*;


pub struct Table {
    name: String,
    batch_size: usize,
    partitions: RwLock<HashMap<PartitionID, Arc<Partition>>>,
    buffer: Mutex<Buffer>,
    lru: LRU,
}

impl Table {
    pub fn new(batch_size: usize, name: &str, lru: LRU) -> Table {
        Table {
            name: name.to_string(),
            batch_size: batch_size_override(batch_size, name),
            partitions: RwLock::new(HashMap::new()),
            buffer: Mutex::new(Buffer::default()),
            lru,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn snapshot(&self) -> Vec<Arc<Partition>> {
        let partitions = self.partitions.read().unwrap();
        partitions.values().cloned().collect()
    }

    pub fn load_table_metadata(batch_size: usize, storage: &DiskStore, lru: &LRU) -> HashMap<String, Table> {
        let mut tables = HashMap::new();
        for md in storage.load_metadata() {
            let table = tables
                .entry(md.tablename.clone())
                .or_insert_with(|| Table::new(batch_size, &md.tablename, lru.clone()));
            table.insert_nonresident_partition(&md);
        }
        tables
    }

    pub fn restore(&self, id: PartitionID, col: &Arc<Column>) {
        let partitions = self.partitions.read().unwrap();
        partitions[&id].restore(&col);
    }

    pub fn evict(&self, key: &ColumnKey) -> usize {
        let partitions = self.partitions.read().unwrap();
        partitions.get(&key.0).map(|p| p.evict(&key.1)).unwrap_or(0)
    }

    pub fn insert_nonresident_partition(&self, md: &PartitionMetadata) {
        let partition = Arc::new(Partition::nonresident(md.id, md.len, &md.columns, self.lru.clone()));
        let mut partitions = self.partitions.write().unwrap();
        partitions.insert(md.id, partition);
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

    pub fn load_partition(&self, partition: Partition) {
        let mut partitions = self.partitions.write().unwrap();
        partitions.insert(partition.id(), Arc::new(partition));
    }

    fn batch_if_needed(&self, buffer: &mut Buffer) {
        if buffer.len() < self.batch_size { return; }
        self.batch(buffer);
    }

    fn batch(&self, buffer: &mut Buffer) {
        let buffer = mem::replace(buffer, Buffer::default());
        self.persist_batch(&buffer);
        // TODO(clemens): get unique partition ID
        let (new_partition, keys) = Partition::from_buffer(0, buffer, self.lru.clone());
        let mut partitions = self.partitions.write().unwrap();
        partitions.insert(new_partition.id(), Arc::new(new_partition));
        for key in keys { self.lru.put(key); }
    }

    /*fn load_buffer(&self, buffer: Buffer) {
        self.load_batch(buffer.into());
    }*/

    fn persist_batch(&self, _batch: &Buffer) {}

    pub fn mem_tree(&self, depth: usize) -> MemTreeTable {
        assert!(depth > 0);
        let mut tree = MemTreeTable {
            name: self.name().to_string(),
            rows: 0,
            fully_resident: true,
            size_bytes: 0,
            columns: HashMap::default(),
        };
        let partitions = self.snapshot();
        for partition in partitions {
            partition.mem_tree(&mut tree.columns, if depth == 1 { 1 } else { depth - 1 });
            tree.rows += partition.len();
        }
        tree.aggregate();
        if depth == 1 {
            tree.columns = HashMap::default();
        }
        tree
    }

    pub fn stats(&self) -> TableStats {
        let partitions = self.snapshot();
        let size_per_column = Table::size_per_column(&partitions);
        let buffer = self.buffer.lock().unwrap();
        TableStats {
            name: self.name().to_string(),
            rows: partitions.iter().map(|p| p.len()).sum(),
            batches: partitions.len(),
            batches_bytes: partitions.heap_size_of_children(),
            buffer_length: buffer.len(),
            buffer_bytes: buffer.heap_size_of_children(),
            size_per_column,
        }
    }

    pub fn max_partition_id(&self) -> u64 {
        let partitions = self.partitions.read().unwrap();
        partitions.keys().max().cloned().unwrap_or(0)
    }

    fn size_per_column(partitions: &[Arc<Partition>]) -> Vec<(String, usize)> {
        let mut sizes: HashMap<String, usize> = HashMap::default();
        for partition in partitions {
            for (colname, size) in partition.heap_size_per_column() {
                *sizes.entry(colname).or_insert(0) += size;
            }
        }
        sizes.iter().map(|(name, size)| (name.to_string(), *size)).collect()
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

#[derive(PartialEq, Debug, Clone)]
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


