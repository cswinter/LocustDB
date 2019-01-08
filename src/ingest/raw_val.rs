use std::fmt;
use std::mem;
use engine::data_types::BasicType;


#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Hash)]
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

    pub fn heap_size_of_children(&self) -> usize {
        match *self {
            RawVal::Int(_) => mem::size_of::<i64>(),
            RawVal::Str(ref s) => s.capacity() * mem::size_of::<u8>(),
            RawVal::Null => mem::size_of::<RawVal>()
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

pub mod syntax {
    pub use super::RawVal::{Int, Null};

    #[allow(non_snake_case)]
    pub fn Str(s: &str) -> super::RawVal { super::RawVal::Str(s.to_string()) }
}