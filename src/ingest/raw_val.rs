use std::fmt;
use std::hash::Hash;
use std::mem;

use serde::{Deserialize, Serialize};

use crate::engine::data_types::BasicType;

use super::float::FloatOrd;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Hash, Serialize, Deserialize)]
pub enum RawVal {
    Int(i64),
    Float(FloatOrd<f64>),
    Str(String),
    Null,
}

impl RawVal {
    pub fn get_type(&self) -> BasicType {
        match *self {
            RawVal::Int(_) => BasicType::Integer,
            RawVal::Float(_) => BasicType::Float,
            RawVal::Str(_) => BasicType::String,
            RawVal::Null => BasicType::Null,
        }
    }

    pub fn heap_size_of_children(&self) -> usize {
        match *self {
            RawVal::Int(_) => 0,
            RawVal::Float(_) => 0,
            RawVal::Str(ref s) => s.capacity() * mem::size_of::<u8>(),
            RawVal::Null => 0,
        }
    }
}

impl fmt::Display for RawVal {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            RawVal::Null => write!(f, "null"),
            RawVal::Int(i) => write!(f, "{}", i),
            RawVal::Float(FloatOrd(x)) => write!(f, "{}", x),
            RawVal::Str(ref s) => write!(f, "\"{}\"", s),
        }
    }
}

pub mod syntax {
    use crate::FloatOrd;

    pub use super::RawVal::{Int, Null};

    #[allow(non_snake_case)]
    pub fn Str(s: &str) -> super::RawVal {
        super::RawVal::Str(s.to_string())
    }

    #[allow(non_snake_case)]
    pub fn Float(f: FloatOrd<f64>) -> super::RawVal {
        super::RawVal::Float(f)
    }
}
