use heapsize::HeapSizeOf;
use mem_store::column::ColumnData;
use mem_store::column_builder::*;
use mem_store::null_column::NullColumn;
use std::ops::BitOr;
use std::iter::repeat;
use std::fmt;

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone, Hash)]
pub enum RawVal {
    Int(i64),
    Str(String),
    Null,
}

impl fmt::Display for RawVal {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &RawVal::Null => write!(f, "null"),
            &RawVal::Int(i) => write!(f, "{}", i),
            &RawVal::Str(ref s) => write!(f, "\"{}\"", s),
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct RawCol {
    types: ColType,
    data: Vec<RawVal>,
}

impl RawCol {
    pub fn new() -> RawCol {
        RawCol {
            types: ColType::nothing(),
            data: Vec::new(),
        }
    }

    pub fn with_nulls(count: usize) -> RawCol {
        let mut c = Self::new();
        c.push_nulls(count);
        c
    }

    pub fn push(&mut self, elem: RawVal) {
        self.types = self.types | ColType::determine(&elem);
        self.data.push(elem);
    }

    pub fn push_ints(&mut self, ints: Vec<i64>) {
        self.types = self.types | ColType::int();
        self.data.extend(ints.into_iter().map(|i| RawVal::Int(i)));
    }

    pub fn push_strings(&mut self, strs: Vec<String>) {
        self.types = self.types | ColType::string();
        self.data.extend(strs.into_iter().map(|s| RawVal::Str(s)));
    }

    pub fn push_nulls(&mut self, count: usize) {
        self.types = self.types | ColType::null();
        self.data.extend(repeat(RawVal::Null).take(count));
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn finalize(self) -> Box<ColumnData> {
        if self.types.contains_string {
            let mut builder = StringColBuilder::new();
            for v in self.data {
                match v {
                    RawVal::Str(s) => builder.push(&s),
                    RawVal::Int(i) => builder.push(&i.to_string()),
                    RawVal::Null => builder.push(""),
                }
            }
            builder.finalize()
        } else if self.types.contains_int {
           let mut builder = IntColBuilder::new();
           for v in self.data {
                match v {
                    RawVal::Str(_) => panic!("Unexpected string in int column!"),
                    RawVal::Int(i) => builder.push(&i),
                    RawVal::Null => builder.push(&0),
                }
           }
            builder.finalize()
        } else {
            Box::new(NullColumn::new(self.data.len()))
        }
    }
}

impl HeapSizeOf for RawCol {
    fn heap_size_of_children(&self) -> usize {
        self.data.heap_size_of_children()
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Copy, Clone)]
struct ColType {
    contains_string: bool,
    contains_int: bool,
    contains_null: bool,
}

impl ColType {
    fn new(string: bool, int: bool, null: bool) -> ColType {
        ColType {  contains_string: string, contains_int: int, contains_null: null }
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
        match v {
            &RawVal::Null => ColType::null(),
            &RawVal::Str(_) => ColType::string(),
            &RawVal::Int(_) => ColType::int()
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
