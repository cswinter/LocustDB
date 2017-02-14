use std::rc::Rc;

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub enum ValueType {
    Null,
    Bool(bool),
    Timestamp(u64),
    Integer(i64),
    Str(Rc<String>),
    Set(Rc<Vec<String>>)
}

pub type RecordType = Vec<(String, ValueType)>;
