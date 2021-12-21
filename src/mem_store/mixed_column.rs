use crate::ingest::raw_val::RawVal;
use crate::mem_store::value::Val;

impl RawVal {
    pub fn to_val(&self) -> Val {
        match *self {
            RawVal::Null => Val::Null,
            RawVal::Int(i) => Val::Integer(i),
            RawVal::Float(f) => Val::Float(f),
            RawVal::Str(ref string) => Val::Str(string),
        }
    }
}
