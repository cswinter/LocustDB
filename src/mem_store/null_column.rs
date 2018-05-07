use bit_vec::BitVec;
use engine::typed_vec::{BoxedVec, TypedVec};
use engine::types::*;
use heapsize::HeapSizeOf;
use mem_store::column::*;


pub struct NullColumn {
    length: usize,
}

impl NullColumn {
    pub fn new(length: usize) -> NullColumn {
        NullColumn { length }
    }
}

impl ColumnData for NullColumn {
    fn collect_decoded(&self) -> BoxedVec {
        TypedVec::empty(self.length)
    }

    fn filter_decode(&self, filter: &BitVec) -> BoxedVec {
        TypedVec::empty(filter.iter().filter(|b| *b).count())
    }

    fn index_decode(&self, indices: &[usize]) -> BoxedVec {
        TypedVec::empty(indices.len())
    }

    fn basic_type(&self) -> BasicType { BasicType::Null }

    fn len(&self) -> usize { self.length }
}

impl HeapSizeOf for NullColumn {
    fn heap_size_of_children(&self) -> usize {
        0
    }
}
