use bit_vec::BitVec;
use engine::typed_vec::TypedVec;
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
    fn collect_decoded(&self) -> TypedVec {
        TypedVec::Empty(self.length)
    }

    fn filter_decode(&self, filter: &BitVec) -> TypedVec {
        TypedVec::Empty(filter.iter().filter(|b| *b).count())
    }

    fn index_decode(&self, indices: &[usize]) -> TypedVec {
        TypedVec::Empty(indices.len())
    }

    fn basic_type(&self) -> BasicType { BasicType::Null }

    fn len(&self) -> usize { self.length }
}

impl HeapSizeOf for NullColumn {
    fn heap_size_of_children(&self) -> usize {
        0
    }
}
