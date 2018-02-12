use bit_vec::BitVec;

use value::Val;
use mem_store::ingest::RawVal;


pub enum TypedVec<'a> {
    String(Vec<&'a str>),
    Integer(Vec<i64>),
    Mixed(Vec<Val<'a>>),
    Boolean(BitVec),
    Constant(RawVal),
    Empty,
}

impl<'a> TypedVec<'a> {
    pub fn len(&self) -> usize {
        match self {
            &TypedVec::String(ref v) => v.len(),
            &TypedVec::Integer(ref v) => v.len(),
            &TypedVec::Mixed(ref v) => v.len(),
            &TypedVec::Boolean(ref v) => v.len(),
            &TypedVec::Empty => 0,
            &TypedVec::Constant(_) => panic!(" cannot get length of constant"),
        }
    }

    pub fn get_raw(&self, i: usize) -> RawVal {
        match self {
            &TypedVec::String(ref v) => RawVal::Str(v[i].to_string()),
            &TypedVec::Integer(ref v) => RawVal::Int(v[i]),
            &TypedVec::Mixed(ref v) => RawVal::from(&v[i]),
            &TypedVec::Boolean(_) => panic!("Boolean(BitVec).get_raw()"),
            &TypedVec::Empty => RawVal::Null,
            &TypedVec::Constant(ref r) => r.clone(),
        }
    }
}

