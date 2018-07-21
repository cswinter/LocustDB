use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;

use heapsize::HeapSizeOf;
use mem_store::*;
use disk_store::interface::PartitionID;
use ingest::buffer::Buffer;


pub struct Partition {
    id: PartitionID,
    cols: Vec<Mutex<ColumnHandle>>,
}

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
            cols: cols.into_iter()
                .map(|c| Mutex::new(ColumnHandle::Resident(c)))
                .collect(),
        }
    }

    pub fn nonresident(id: PartitionID, cols: &[String]) -> Partition {
        Partition {
            id,
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

    pub fn get_cols(&self, referenced_cols: &HashSet<String>) -> HashMap<String, Arc<Column>> {
        let mut columns = HashMap::new();
        for handle in &self.cols {
            let mut handle = handle.lock().unwrap();
            if referenced_cols.contains(handle.name()) {
                let column = match *handle {
                    ColumnHandle::NonResident(ref _name) => {
                        panic!("non resident!");
                    }
                    ColumnHandle::Resident(ref column) => column.clone(),
                };
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
}

impl HeapSizeOf for Partition {
    fn heap_size_of_children(&self) -> usize {
        // TODO(clemens): fix
        // self.cols.heap_size_of_children()
        0
    }
}

impl HeapSizeOf for ColumnHandle {
    fn heap_size_of_children(&self) -> usize {
        match self {
            ColumnHandle::NonResident(ref name) => name.heap_size_of_children(),
            ColumnHandle::Resident(ref col) => col.heap_size_of_children(),
        }
    }
}