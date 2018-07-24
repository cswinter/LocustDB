use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;

use heapsize::HeapSizeOf;
use mem_store::*;
use disk_store::interface::*;
use ingest::buffer::Buffer;


pub struct Partition {
    id: PartitionID,
    len: usize,
    cols: Vec<Mutex<ColumnHandle>>,
}

#[derive(HeapSizeOf)]
pub enum ColumnHandle {
    NonResident(String),
    Resident(Arc<Column>),
}

impl ColumnHandle {
    fn name(&self) -> &str {
        match self {
            ColumnHandle::NonResident(ref name) => name,
            ColumnHandle::Resident(ref column) => column.name(),
        }
    }
}

impl Partition {
    pub fn new(id: PartitionID, cols: Vec<Arc<Column>>) -> Partition {
        Partition {
            id,
            len: cols[0].len(),
            cols: cols.into_iter()
                .map(|c| Mutex::new(ColumnHandle::Resident(c)))
                .collect(),
        }
    }

    pub fn nonresident(id: PartitionID, len: usize, cols: &[String]) -> Partition {
        Partition {
            id,
            len,
            cols: cols.iter()
                .map(|name| Mutex::new(ColumnHandle::NonResident(name.to_string())))
                .collect(),
        }
    }

    pub fn from_buffer(id: PartitionID, buffer: Buffer) -> Partition {
        Partition::new(
            id,
            buffer.buffer.into_iter()
                .map(|(name, raw_col)| raw_col.finalize(&name))
                .collect())
    }

    pub fn get_cols(&self, referenced_cols: &HashSet<String>, db: &DiskStore) -> HashMap<String, Arc<Column>> {
        let mut columns = HashMap::new();
        for handle in &self.cols {
            let mut handle = handle.lock().unwrap();
            if referenced_cols.contains(handle.name()) {
                let column = match *handle {
                    ColumnHandle::NonResident(ref name) => Arc::new(db.load_column(self.id, name)),
                    ColumnHandle::Resident(ref column) => column.clone(),
                };
                *handle = ColumnHandle::Resident(column.clone());
                columns.insert(handle.name().to_string(), column);
            }
        }
        columns
    }

    pub fn col_names(&self) -> Vec<String> {
        let mut names = Vec::new();
        for handle in &self.cols {
            let mut handle = handle.lock().unwrap();
            names.push(handle.name().to_string());
        }
        names
    }

    pub fn id(&self) -> u64 { self.id }
    pub fn len(&self) -> usize { self.len }

    pub fn heap_size_per_column(&self) -> Vec<(String, usize)> {
        self.cols.iter()
            .map(|c| {
                let c = c.lock().unwrap();
                (c.name().to_string(), c.heap_size_of_children())
            })
            .collect()
    }
}

impl HeapSizeOf for Partition {
    fn heap_size_of_children(&self) -> usize {
        self.cols.iter().map(|c| c.lock().unwrap().heap_size_of_children()).sum()
    }
}