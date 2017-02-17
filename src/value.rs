use std::rc::Rc;
use std::fmt;
use heapsize::HeapSizeOf;
use std::convert::From;

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub enum ValueType<'a> {
    Null,
    Bool(bool),
    Timestamp(u64),
    Integer(i64),
    Str(&'a str),
    Set(Rc<Vec<String>>),
}

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub enum InpVal {
    Null,
    Timestamp(u64),
    Integer(i64),
    Str(Rc<String>),
    Set(Rc<Vec<String>>),
}

pub type InpRecordType<'a> = Vec<(&'a str, InpVal)>;

impl InpVal {
    pub fn to_val<'a>(&'a self) -> ValueType<'a> {
        match self {
            &InpVal::Null => ValueType::Null,
            &InpVal::Timestamp(t) => ValueType::Timestamp(t),
            &InpVal::Integer(i) => ValueType::Integer(i),
            &InpVal::Str(ref string) => ValueType::Str(string),
            &InpVal::Set(ref set) => ValueType::Set(set.clone()),
        }
    }
}

impl<'a> fmt::Display for ValueType<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &ValueType::Null => write!(f, "null"),
            &ValueType::Bool(b) => write!(f, "{}", b),
            &ValueType::Timestamp(t) => write!(f, "t{}", t),
            &ValueType::Integer(i) => write!(f, "{}", i),
            &ValueType::Str(ref s) => write!(f, "\"{}\"", s),
            &ValueType::Set(ref vec) => write!(f, "{:?}", vec),
        }
    }
}

impl<'a> HeapSizeOf for ValueType<'a> {
    fn heap_size_of_children(&self) -> usize {
        use ValueType::*;
        match self {
            &Null | &Bool(_) | &Timestamp(_) | &Integer(_) => 0,
            &Str(ref r) => r.heap_size_of_children(),
            &Set(ref r) => r.heap_size_of_children(),
        }
    }
}

impl HeapSizeOf for InpVal {
    fn heap_size_of_children(&self) -> usize {
        use self::InpVal::*;
        match self {
            &Null | &Timestamp(_) | &Integer(_) => 0,
            &Str(ref r) => r.heap_size_of_children(),
            &Set(ref r) => r.heap_size_of_children(),
        }
    }
}

impl<'a> From<()> for ValueType<'a> {
    fn from(_: ()) -> ValueType<'a> {
        ValueType::Null
    }
}

impl<'a> From<bool> for ValueType<'a> {
    fn from(b: bool) -> ValueType<'a> {
        ValueType::Bool(b)
    }
}

impl<'a> From<u64> for ValueType<'a> {
    fn from(t: u64) -> ValueType<'a> {
        ValueType::Timestamp(t)
    }
}

impl<'a> From<i64> for ValueType<'a> {
    fn from(t: i64) -> ValueType<'a> {
        ValueType::Integer(t)
    }
}

impl<'a> From<&'a str> for ValueType<'a> {
    fn from(s: &'a str) -> ValueType<'a> {
        ValueType::Str(s)
    }
}

impl<'a> From<Vec<String>> for ValueType<'a> {
    fn from(s: Vec<String>) -> ValueType<'a> {
        ValueType::Set(Rc::new(s))
    }
}

impl<'a, T> From<Option<T>> for ValueType<'a> where ValueType<'a>: From<T> {
    fn from(o: Option<T>) -> ValueType<'a> {
        match o {
            None => ValueType::Null,
            Some(v) => ValueType::from(v)
        }
    }
}
