use std::collections::HashSet;

#[derive(Debug, PartialEq)]
pub enum ValueType {
    Null,
    Bool(bool),
    String(String),
    Timestamp(u64),
    Integer(i64),
    Float(f64),
    Set(HashSet<String>)
}

pub type RecordType = Vec<(String, ValueType)>;
