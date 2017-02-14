use std::collections::HashSet;
use std::rc::Rc;

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub enum ValueType {
    Null,
    Bool(bool),
    Timestamp(u64),
    Integer(i64),
    String(Rc<String>),
    Set(Rc<Vec<String>>)
}

pub type RecordType = Vec<(String, ValueType)>;
