// use bit_vec::BitVec;
// use mem_store::column::*;
use ingest::raw_val::RawVal;
use mem_store::value::Val;
// use engine::types::Type;
// use engine::typed_vec::TypedVec;


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
*/

impl RawVal {
    pub fn to_val(&self) -> Val {
        match *self {
            RawVal::Null => Val::Null,
            RawVal::Int(i) => Val::Integer(i),
            RawVal::Str(ref string) => Val::Str(string),
        }
    }
}

