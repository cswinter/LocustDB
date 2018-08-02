use std::fmt;
use engine::types::BasicType;


#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Hash, HeapSizeOf)]
pub enum RawVal {
    Int(i64),
    Str(String),
    Null,
}

impl RawVal {
    pub fn get_type(&self) -> BasicType {
        match *self {
            RawVal::Int(_) => BasicType::Integer,
            RawVal::Str(_) => BasicType::String,
            RawVal::Null => BasicType::Null,
        }
    }
}

impl fmt::Display for RawVal {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            RawVal::Null => write!(f, "null"),
            RawVal::Int(i) => write!(f, "{}", i),
            RawVal::Str(ref s) => write!(f, "\"{}\"", s),
        }
    }
}

