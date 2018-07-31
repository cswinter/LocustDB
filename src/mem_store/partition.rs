use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;

use heapsize::HeapSizeOf;
use mem_store::*;
use disk_store::interface::*;
use ingest::buffer::Buffer;


pub type ColumnKey = (PartitionID, String);

pub struct Partition {
    id: PartitionID,
    len: usize,
    cols: Vec<(ColumnKey, Mutex<ColumnHandle>)>,
    lru: LRU,
}

#[derive(HeapSizeOf)]
pub enum ColumnHandle {
    NonResident,
    Resident(Arc<Column>),
}

impl ColumnHandle {
    fn non_resident(&self) -> bool {
        match &self {
            ColumnHandle::NonResident => true,
            _ => false,
        }
    }
}

impl Partition {
    pub fn new(id: PartitionID, cols: Vec<Arc<Column>>, lru: LRU) -> Partition {
        Partition {
            id,
            len: cols[0].len(),
            cols: cols.into_iter()
                .map(|c| {
                    let key = (id, c.name().to_string());
                    lru.put(&key);
                    (key, Mutex::new(ColumnHandle::Resident(c)))
                })
                .collect(),
            lru,
        }
    }

    pub fn nonresident(id: PartitionID, len: usize, cols: &[String], lru: LRU) -> Partition {
        Partition {
            id,
            len,
            cols: cols.iter()
                .map(|name| ((id, name.to_string()), Mutex::new(ColumnHandle::NonResident)))
                .collect(),
            lru,
        }
    }

    pub fn from_buffer(id: PartitionID, buffer: Buffer, lru: LRU) -> Partition {
        Partition::new(
            id,
            buffer.buffer.into_iter()
                .map(|(name, raw_col)| raw_col.finalize(&name))
                .collect(),
            lru)
    }

    pub fn get_cols(&self, referenced_cols: &HashSet<String>, db: &DiskStore) -> HashMap<String, Arc<Column>> {
        let mut columns = HashMap::new();
        for (key, handle) in &self.cols {
            if referenced_cols.contains(&key.1) {
                let mut handle = handle.lock().unwrap();
                let column = match *handle {
                    ColumnHandle::NonResident => {
                        self.lru.put(&key);
                        Arc::new(db.load_column(key.0, &key.1))
                    }
                    ColumnHandle::Resident(ref column) => {
                        self.lru.touch(&key);
                        column.clone()
                    }
                };
                *handle = ColumnHandle::Resident(column.clone());
                columns.insert(key.1.to_string(), column);
            }
        }
        columns
    }

    pub fn col_names(&self) -> Vec<String> {
        let mut names = Vec::new();
        for (key, _) in &self.cols {
            names.push(key.1.to_string());
        }
        names
    }

    pub fn restore(&self, col: Arc<Column>) {
        for (key, c) in &self.cols {
            if key.1 == col.name() {
                let mut handle = c.lock().unwrap();
                if handle.non_resident() {
                    self.lru.put(&key);
                }
                *handle = ColumnHandle::Resident(col.clone());
            }
        }
    }

    pub fn evict(&self, col: &str) -> usize {
        for (key, c) in &self.cols {
            let mut handle = c.lock().unwrap();
            if key.1 == col {
                let mem_size = handle.heap_size_of_children();
                *handle = ColumnHandle::NonResident;
                return mem_size;
            }
        }
        0
    }

    pub fn id(&self) -> u64 { self.id }
    pub fn len(&self) -> usize { self.len }

    pub fn mem_tree(&self, coltrees: &mut HashMap<String, MemTreeColumn>, depth: usize) {
        if depth == 0 { return; }
        for ((_, name), col) in &self.cols {
            let col = col.lock().unwrap();
            let mut coltree = coltrees.entry(name.to_string())
                .or_insert(MemTreeColumn {
                    name: name.to_string(),
                    size_bytes: 0,
                    size_percentage: 0.0,
                    rows: 0,
                    rows_percentage: 0.0,
                    encodings: HashMap::default(),
                });
            if let ColumnHandle::Resident(ref col) = *col {
                col.mem_tree(&mut coltree, depth);
            }
        }
    }

    pub fn heap_size_per_column(&self) -> Vec<(String, usize)> {
        self.cols.iter()
            .map(|((_, name), c)| {
                let c = c.lock().unwrap();
                (name.to_string(), c.heap_size_of_children())
            })
            .collect()
    }
}

impl HeapSizeOf for Partition {
    fn heap_size_of_children(&self) -> usize {
        self.cols.iter().map(|(_, c)| c.lock().unwrap().heap_size_of_children()).sum()
    }
}