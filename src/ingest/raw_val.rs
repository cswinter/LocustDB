use std::fmt;
use std::mem;

use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};

use crate::engine::data_types::BasicType;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Hash, Serialize, Deserialize)]
pub enum RawVal {
    Int(i64),
    Float(OrderedFloat<f64>),
    Str(String),
    Null,
}

impl RawVal {
    pub fn get_type(&self) -> BasicType {
        match *self {
            RawVal::Int(_) => BasicType::Integer,
            RawVal::Str(_) => BasicType::String,
            RawVal::Null => BasicType::Null,
            RawVal::Float(_) => BasicType::Float,
        }
    }

    pub fn heap_size_of_children(&self) -> usize {
        match *self {
            RawVal::Int(_) => 0,
            RawVal::Str(ref s) => s.capacity() * mem::size_of::<u8>(),
            RawVal::Null => 0,
            RawVal::Float(_) => 0,
        }
    }
}

impl fmt::Display for RawVal {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            RawVal::Null => write!(f, "null"),
            RawVal::Int(i) => write!(f, "{}", i),
            RawVal::Str(ref s) => write!(f, "\"{}\"", s),
            RawVal::Float(x) => write!(f, "{:e}", x),
        }
    }
}

pub mod syntax {
    pub use super::RawVal::{Int, Null, Float};

    #[allow(non_snake_case)]
    pub fn Str(s: &str) -> super::RawVal {
        super::RawVal::Str(s.to_string())
    }
}
