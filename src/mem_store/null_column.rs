use bit_vec::BitVec;
use mem_store::column::*;
use heapsize::HeapSizeOf;
use engine::types::Type;
use engine::typed_vec::TypedVec;


pub struct NullColumn {
    length: usize,
}

impl NullColumn {
    pub fn new(length: usize) -> NullColumn {
        NullColumn { length: length }
    }
}

impl ColumnData for NullColumn {
    fn collect_decoded(&self) -> TypedVec {
        TypedVec::Empty
    }

    fn filter_decode(&self, _: &BitVec) -> TypedVec {
        TypedVec::Empty
    }

    fn index_decode(&self, _: &Vec<usize>) -> TypedVec {
        TypedVec::Empty
    }

    fn decoded_type(&self) -> Type { Type::Null }
}

impl HeapSizeOf for NullColumn {
    fn heap_size_of_children(&self) -> usize {
        0
    }
}
