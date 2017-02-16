use std::rc::Rc;
use std::fmt;
use heapsize::HeapSizeOf;

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

pub type RecordType<'a> = Vec<(String, ValueType<'a>)>;
pub type InpRecordType = Vec<(String, InpVal)>;

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
