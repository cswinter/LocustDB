use std::fmt;
use std::mem;

use datasize::DataSize;
use locustdb_serialization::api::AnyVal;
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
    pub use super::RawVal::{Int, Null};

    #[allow(non_snake_case)]
    pub fn Str(s: &str) -> super::RawVal {
        super::RawVal::Str(s.to_string())
    }

    #[allow(non_snake_case)]
    pub fn Float(x: f64) -> super::RawVal {
        super::RawVal::Float(x.into())
    }
}

impl From<f64> for RawVal {
    fn from(val: f64) -> Self {
        RawVal::Float(OrderedFloat(val))
    }
}

impl From<String> for RawVal {
    fn from(val: String) -> Self {
        RawVal::Str(val)
    }
}

impl From<()> for RawVal {
    fn from(_: ()) -> Self {
        RawVal::Null
    }
}

impl<T: Into<RawVal>> From<Option<T>> for RawVal {
    fn from(val: Option<T>) -> Self {
        match val {
            Some(val) => val.into(),
            None => RawVal::Null,
        }
    }
}

impl From<&str> for RawVal {
    fn from(val: &str) -> RawVal {
        RawVal::Str(val.to_string())
    }
}

impl From<i64> for RawVal {
    fn from(val: i64) -> RawVal {
        RawVal::Int(val)
    }
}

impl From<AnyVal> for RawVal {
    fn from(val: AnyVal) -> Self {
        match val {
            AnyVal::Int(i) => RawVal::Int(i),
            AnyVal::Float(f) => RawVal::Float(OrderedFloat(f)),
            AnyVal::Str(s) => RawVal::Str(s),
            AnyVal::Null => RawVal::Null,
        }
    }
}

impl From<RawVal> for AnyVal {
    fn from(val: RawVal) -> Self {
        match val {
            RawVal::Int(i) => AnyVal::Int(i),
            RawVal::Float(f) => AnyVal::Float(f.0),
            RawVal::Str(s) => AnyVal::Str(s),
            RawVal::Null => AnyVal::Null,
        }
    }
}

impl DataSize for RawVal {
    const IS_DYNAMIC: bool = true;
    const STATIC_HEAP_SIZE: usize = 0;
    fn estimate_heap_size(&self) -> usize {
        match *self {
            RawVal::Str(ref s) => s.capacity() * mem::size_of::<u8>(),
            RawVal::Int(_) | RawVal::Float(_) | RawVal::Null => 0,
        }
    }
}
