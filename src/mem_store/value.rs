use std::fmt;
use heapsize::HeapSizeOf;
use std::convert::From;
use ingest::raw_val::RawVal;

#[derive(Debug, PartialEq, Eq, Ord, PartialOrd, Clone, Hash)]
pub enum Val<'a> {
    Null,
    Bool(bool),
    Integer(i64),
    Str(&'a str),
}


impl<'a> fmt::Display for Val<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &Val::Null => write!(f, "null"),
            &Val::Bool(b) => write!(f, "{}", b),
            &Val::Integer(i) => write!(f, "{}", i),
            &Val::Str(ref s) => write!(f, "\"{}\"", s),
        }
    }
}

impl<'a> HeapSizeOf for Val<'a> {
    fn heap_size_of_children(&self) -> usize {
        use self::Val::*;
        match self {
            &Null | &Bool(_) | &Integer(_) => 0,
            &Str(ref r) => r.heap_size_of_children(),
        }
    }
}

impl<'a> From<()> for Val<'a> {
    fn from(_: ()) -> Val<'a> {
        Val::Null
    }
}

impl<'a> From<bool> for Val<'a> {
    fn from(b: bool) -> Val<'a> {
        Val::Bool(b)
    }
}

impl<'a> From<i64> for Val<'a> {
    fn from(t: i64) -> Val<'a> {
        Val::Integer(t)
    }
}

impl<'a> From<&'a str> for Val<'a> {
    fn from(s: &'a str) -> Val<'a> {
        Val::Str(s)
    }
}

impl<'a, T> From<Option<T>> for Val<'a>
    where Val<'a>: From<T>
{
    fn from(o: Option<T>) -> Val<'a> {
        match o {
            None => Val::Null,
            Some(v) => Val::from(v),
        }
    }
}

impl<'a, 'b> From<&'a Val<'b>> for RawVal {
    fn from(val: &Val) -> RawVal {
        match val {
            &Val::Integer(b) => RawVal::Int(b),
            &Val::Str(s) => RawVal::Str(s.to_string()),
            &Val::Null => RawVal::Null,
            &Val::Bool(_) => RawVal::Null,
        }
    }
}
