use mem_store::column::*;
use value::Val;
use std::iter;
use heapsize::HeapSizeOf;


pub struct NullColumn {
    length: usize,
}

impl NullColumn {
    pub fn new(length: usize) -> NullColumn {
        NullColumn { length: length }
    }
}

impl ColumnData for NullColumn {
    fn iter<'a>(&'a self) -> ColIter<'a> {
        let iter = iter::repeat(Val::Null).take(self.length);
        ColIter::new(iter)
    }
}

impl HeapSizeOf for NullColumn {
    fn heap_size_of_children(&self) -> usize {
        0
    }
}
