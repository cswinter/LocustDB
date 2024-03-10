use std::iter::repeat;
use std::mem;
use std::ops::BitOr;
use std::sync::Arc;

use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};

use crate::ingest::raw_val::RawVal;
use crate::mem_store::column_builder::*;
use crate::mem_store::*;

// Can eliminate this? Used by in-memory buffer.
#[derive(PartialEq, Debug, Clone, Serialize, Deserialize)]
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

    pub fn push_floats(&mut self, floats: Vec<f64>) {
        self.types = self.types | ColType::float();
        self.data.extend(floats.into_iter().map(|f| RawVal::Float(OrderedFloat(f))));
    }

    pub fn push_strings(&mut self, strs: Vec<String>) {
        self.types = self.types | ColType::string();
        self.data.extend(strs.into_iter().map(RawVal::Str));
    }

    pub fn push_nulls(&mut self, count: usize) {
        if count > 0 {
            self.types = self.types | ColType::null();
            self.data.extend(repeat(RawVal::Null).take(count));
        }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn finalize(self, name: &str) -> Arc<Column> {
        let present =  if self.types.contains_null {
            let mut present = vec![0u8; (self.data.len() + 7) / 8];
            for (i, v) in self.data.iter().enumerate() {
                if *v != RawVal::Null {
                    present[i / 8] |= 1 << (i % 8);
                }
            }
            Some(present)
        } else {
            None
        };
        if self.types.contains_string {
            let mut builder = StringColBuilder::default();
            for v in self.data {
                match v {
                    RawVal::Str(s) => builder.push(&s),
                    RawVal::Int(i) => builder.push(&i.to_string()),
                    RawVal::Null => builder.push(&""),
                    RawVal::Float(f) => builder.push(&f.to_string()),
                }
            }
            ColumnBuilder::<String>::finalize(builder, name, present)
        } else if self.types.contains_float {
            let mut builder = FloatColBuilder::default();
            for v in self.data {
                match v {
                    RawVal::Str(_) => panic!("Unexpected string in float column!"),
                    RawVal::Int(i) => builder.push(&Some(i as f64)),
                    RawVal::Null => builder.push(&None),
                    RawVal::Float(f) => builder.push(&Some(f.into_inner())),
                }
            }
            builder.finalize(name, present)
        } else if self.types.contains_int {
            let mut builder = IntColBuilder::default();
            for v in self.data {
                match v {
                    RawVal::Str(_) => panic!("Unexpected string in int column!"),
                    RawVal::Int(i) => builder.push(&Some(i)),
                    RawVal::Null => builder.push(&None),
                    RawVal::Float(_) => todo!("Unexpected float in int column!"),
                }
            }
            builder.finalize(name, present)
        } else {
            Arc::new(Column::null(name, self.data.len()))
        }
    }

    pub fn heap_size_of_children(&self) -> usize {
        let data_size = self
            .data
            .iter()
            .map(|v| v.heap_size_of_children())
            .sum::<usize>()
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

#[derive(PartialEq, Debug, Copy, Clone, Serialize, Deserialize)]
struct ColType {
    contains_string: bool,
    contains_int: bool,
    contains_float: bool,
    contains_null: bool,
}

impl ColType {
    fn new(string: bool, int: bool, float: bool, null: bool) -> ColType {
        ColType {
            contains_string: string,
            contains_int: int,
            contains_float: float,
            contains_null: null,
        }
    }

    fn string() -> ColType {
        ColType::new(true, false, false, false)
    }

    fn int() -> ColType {
        ColType::new(false, true, false, false)
    }

    fn float() -> ColType {
        ColType::new(false, false, true, false)
    }

    fn null() -> ColType {
        ColType::new(false, false, false, true)
    }

    fn nothing() -> ColType {
        ColType::new(false, false, false, false)
    }

    fn determine(v: &RawVal) -> ColType {
        match *v {
            RawVal::Null => ColType::null(),
            RawVal::Str(_) => ColType::string(),
            RawVal::Int(_) => ColType::int(),
            RawVal::Float(_) => ColType::float(),
        }
    }
}

impl BitOr for ColType {
    type Output = Self;
    fn bitor(self, rhs: ColType) -> Self::Output {
        ColType {
            contains_string: self.contains_string | rhs.contains_string,
            contains_int: self.contains_int | rhs.contains_int,
            contains_float: self.contains_float | rhs.contains_float,
            contains_null: self.contains_null | rhs.contains_null,
        }
    }
}