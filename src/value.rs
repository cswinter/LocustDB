use std::collections::HashSet;

#[derive(Debug)]
pub enum ValueType {
    Null,
    String(String),
    Timestamp(u64),
    Integer(i64),
    Float(f64),
    Set(HashSet<String>)
}
