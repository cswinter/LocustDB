use mem_store::column::*;
use value::Val;
use std::iter;
use heapsize::HeapSizeOf;
use engine::types::Type;

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

    fn dump_untyped<'a>(&'a self, count: usize, offset: usize, buffer: &mut Vec<Val<'a>>) {
        // TODO(clemens): respect length field
        for _ in offset..(offset + count) {
            buffer.push(Val::Null);
        }
    }

    fn decoded_type(&self) -> Type { Type::Null }
}

impl HeapSizeOf for NullColumn {
    fn heap_size_of_children(&self) -> usize {
        0
    }
}
