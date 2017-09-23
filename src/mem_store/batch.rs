use heapsize::HeapSizeOf;
use mem_store::column::{Column, ColumnData};

pub struct Batch {
    pub cols: Vec<Column>,
}


impl Batch {
    pub fn new(cols: Vec<(String, Box<ColumnData>)>) -> Batch {
        let mut mem_store = Vec::new();
        for (name, col) in cols {
            mem_store.push(Column::new(name, col));
        }
        Batch { cols: mem_store }
    }
}


impl HeapSizeOf for Batch {
    fn heap_size_of_children(&self) -> usize {
        println!("HEAP SIZE BATCH");
        self.cols.heap_size_of_children()
    }
}
