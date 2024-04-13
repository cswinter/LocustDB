use crate::ingest::raw_val::RawVal;
use crate::mem_store::value::Val;

impl RawVal {
    pub fn to_val(&self) -> Val {
        match *self {
            RawVal::Null => Val::Null,
            RawVal::Int(i) => Val::Integer(i),
            RawVal::Str(ref string) => Val::Str(string),
            RawVal::Float(f) => Val::Float(f),
        }
    }

    pub fn to_static_val(&self) -> Val<'static> {
        match *self {
            RawVal::Null => Val::Null,
            RawVal::Int(i) => Val::Integer(i),
            RawVal::Float(f) => Val::Float(f),
            RawVal::Str(_) => panic!("Can't convert RawVal::Str to Val::Str + 'static"),
        }
    }
}

