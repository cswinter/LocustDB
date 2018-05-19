use std::sync::Arc;

use heapsize::HeapSizeOf;
use mem_store::*;

#[derive(Clone)]
pub struct Batch {
    cols: Arc<Vec<Box<Column>>>,
}


impl Batch {
    pub fn new(cols: Vec<Box<Column>>) -> Batch {
        Batch { cols: Arc::new(cols) }
    }

    pub fn cols(&self) -> &Vec<Box<Column>> {
        self.cols.as_ref()
    }
}

impl From<Vec<Box<Column>>> for Batch {
    fn from(cols: Vec<Box<Column>>) -> Batch {
        Batch { cols: Arc::new(cols) }
    }
}


impl HeapSizeOf for Batch {
    fn heap_size_of_children(&self) -> usize {
        self.cols.heap_size_of_children()
    }
}
