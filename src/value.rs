use std::collections::HashSet;
use std::rc::Rc;

#[derive(Debug, PartialEq, Clone)]
pub enum ValueType {
    Null,
    Bool(bool),
    Timestamp(u64),
    Integer(i64),
    Float(f64),
    String(Rc<String>),
    Set(Rc<HashSet<String>>)
}

pub type RecordType = Vec<(String, ValueType)>;
