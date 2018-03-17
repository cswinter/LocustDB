use std::sync::Arc;

use heapsize::HeapSizeOf;
use mem_store::column::{Column, ColumnData};

#[derive(Clone)]
pub struct Batch {
    cols: Arc<Vec<Column>>,
}


impl Batch {
    pub fn new(cols: Vec<(String, Box<ColumnData>)>) -> Batch {
        let mut mem_store = Vec::new();
        for (name, col) in cols {
            mem_store.push(Column::new(name, col));
        }
        Batch { cols: Arc::new(mem_store) }
    }

    pub fn cols(&self) -> &Vec<Column> {
        self.cols.as_ref()
    }
}

impl From<Vec<Column>> for Batch {
    fn from(cols: Vec<Column>) -> Batch {
        Batch { cols: Arc::new(cols) }
    }
}


impl HeapSizeOf for Batch {
    fn heap_size_of_children(&self) -> usize {
        println!("HEAP SIZE BATCH");
        self.cols.heap_size_of_children()
    }
}
