use mem_store::column::*;
use mem_store::ingest::RawVal;
use value::Val;
use heapsize::HeapSizeOf;


struct MixedColumn {
    values: Vec<RawVal>,
}

impl MixedColumn {
    fn new(mut values: Vec<RawVal>) -> MixedColumn {
        values.shrink_to_fit();
        MixedColumn { values: values }
    }
}

impl ColumnData for MixedColumn {
    fn iter(&self) -> ColIter {
        let iter = self.values.iter().map(|val| val.to_val());
        ColIter::new(iter)
    }
}

impl RawVal {
    pub fn to_val<'a>(&'a self) -> Val<'a> {
        match self {
            &RawVal::Null => Val::Null,
            &RawVal::Int(i) => Val::Integer(i),
            &RawVal::Str(ref string) => Val::Str(string),
        }
    }
}

impl HeapSizeOf for RawVal {
    fn heap_size_of_children(&self) -> usize {
        match self {
            &RawVal::Null | &RawVal::Int(_) => 0,
            &RawVal::Str(ref r) => r.heap_size_of_children(),
        }
    }
}

impl HeapSizeOf for MixedColumn {
    fn heap_size_of_children(&self) -> usize {
        self.values.heap_size_of_children()
    }
}
