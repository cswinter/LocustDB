use bit_vec::BitVec;
use mem_store::column::*;
use mem_store::ingest::RawVal;
use value::Val;
use heapsize::HeapSizeOf;
use engine::types::Type;
use engine::typed_vec::TypedVec;


/*
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
    fn collect_decoded<'a>(&'a self, filter: &Option<BitVec>) -> TypedVec {
        let mut result = Vec::with_capacity(self.values.len());
        match filter {
            &None => {
                for val in self.values.iter() {
                    result.push(val.to_val());
                }
            }
            &Some(ref bv) => {
                for (val, selected) in self.values.iter().zip(bv) {
                    if selected {
                        result.push(val.to_val());
                    }
                }
            }
        }
        TypedVec::Mixed(result)
    }

    fn decoded_type(&self) -> Type { Type::Val }
}

impl HeapSizeOf for MixedColumn {
    fn heap_size_of_children(&self) -> usize {
        self.values.heap_size_of_children()
    }
}
*/
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


