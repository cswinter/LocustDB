use std::iter::repeat;
use std::ops::BitOr;
use std::sync::Arc;
use std::mem;

use ingest::raw_val::RawVal;
use mem_store::*;
use mem_store::column_builder::*;

// Can eliminate this? Used by in-memory buffer.
#[derive(PartialEq, Debug)]
pub struct MixedCol {
    types: ColType,
    data: Vec<RawVal>,
}

impl MixedCol {
    pub fn with_nulls(count: usize) -> MixedCol {
        let mut c = Self::default();
        c.push_nulls(count);
        c
    }

    pub fn push(&mut self, elem: RawVal) {
        self.types = self.types | ColType::determine(&elem);
        self.data.push(elem);
    }

    pub fn push_ints(&mut self, ints: Vec<i64>) {
        self.types = self.types | ColType::int();
        self.data.extend(ints.into_iter().map(RawVal::Int));
    }

    pub fn push_strings(&mut self, strs: Vec<String>) {
        self.types = self.types | ColType::string();
        self.data.extend(strs.into_iter().map(RawVal::Str));
    }

    pub fn push_nulls(&mut self, count: usize) {
        self.types = self.types | ColType::null();
        self.data.extend(repeat(RawVal::Null).take(count));
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn finalize(self, name: &str) -> Arc<Column> {
        if self.types.contains_string {
            let mut builder = StringColBuilder::default();
            for v in self.data {
                match v {
                    RawVal::Str(s) => builder.push(&s),
                    RawVal::Int(i) => builder.push(&i.to_string()),
                    RawVal::Null => builder.push(&""),
                }
            }
            ColumnBuilder::<String>::finalize(builder, name, None)
        } else if self.types.contains_int {
            let mut builder = IntColBuilder::default();
            for v in self.data {
                match v {
                    RawVal::Str(_) => panic!("Unexpected string in int column!"),
                    RawVal::Int(i) => builder.push(&Some(i)),
                    RawVal::Null => builder.push(&None),
                }
            }
            builder.finalize(name, None)
        } else {
            Arc::new(Column::null(name, self.data.len()))
        }
    }

    pub fn heap_size_of_children(&self) -> usize {
        let data_size = self.data.iter().map(|v| v.heap_size_of_children()).sum::<usize>()
            + self.data.capacity() * mem::size_of::<RawVal>();
        let type_size = mem::size_of::<ColType>();

        data_size + type_size
    }
}

impl Default for MixedCol {
    fn default() -> MixedCol {
        MixedCol {
            types: ColType::nothing(),
            data: Vec::new(),
        }
    }
}

#[derive(PartialEq, Debug, Copy, Clone)]
struct ColType {
    contains_string: bool,
    contains_int: bool,
    contains_null: bool,
}

impl ColType {
    fn new(string: bool, int: bool, null: bool) -> ColType {
        ColType { contains_string: string, contains_int: int, contains_null: null }
    }

    fn string() -> ColType {
        ColType::new(true, false, false)
    }

    fn int() -> ColType {
        ColType::new(false, true, false)
    }

    fn null() -> ColType {
        ColType::new(false, false, true)
    }

    fn nothing() -> ColType {
        ColType::new(false, false, false)
    }

    fn determine(v: &RawVal) -> ColType {
        match *v {
            RawVal::Null => ColType::null(),
            RawVal::Str(_) => ColType::string(),
            RawVal::Int(_) => ColType::int()
        }
    }
}

impl BitOr for ColType {
    type Output = Self;
    fn bitor(self, rhs: ColType) -> Self::Output {
        ColType {
            contains_string: self.contains_string | rhs.contains_string,
            contains_int: self.contains_int | rhs.contains_int,
            contains_null: self.contains_null | rhs.contains_null,
        }
    }
}


impl<'a> From<&'a str> for RawVal {
    fn from(val: &str) -> RawVal { RawVal::Str(val.to_string()) }
}

impl From<i64> for RawVal {
    fn from(val: i64) -> RawVal { RawVal::Int(val) }
}
