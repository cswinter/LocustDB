use std::cmp;
use std::sync::Arc;

use datasize::DataSize;
use ordered_float::OrderedFloat;

use crate::bitvec::*;
use crate::ingest::raw_val::RawVal;
use crate::mem_store::column::*;
use crate::mem_store::integers::*;
use crate::mem_store::strings::*;
use crate::stringpack::*;

use super::floats::FloatColumn;

#[derive(Default, Clone, Debug, DataSize)]
pub struct ColumnBuffer {
    buffer: TypedBuffer,
    length: usize,
    present: Option<Vec<u8>>,
}

impl ColumnBuffer {
    pub fn null(length: usize) -> Self {
        ColumnBuffer {
            buffer: TypedBuffer::Empty,
            length,
            present: None,
        }
    }

    pub fn push_val(&mut self, elem: RawVal) {
        match elem {
            RawVal::Int(elem) => self.push_ints([elem], None),
            RawVal::Float(ordered_float) => self.push_floats([ordered_float], None),
            RawVal::Str(s) => self.push_strings([s.as_str()], None),
            RawVal::Null => self.push_nulls(1),
        }
    }

    pub fn push_ints<I: IntoIterator<Item = i64>>(&mut self, elems: I, present: Option<&[u8]>) {
        let mut count = 0;
        match &mut self.buffer {
            TypedBuffer::Empty => {
                let mut buffer = IntColBuffer::default();
                if self.len() > 0 {
                    self.init_present();
                }
                for _ in 0..self.len() {
                    buffer.push(0);
                }
                for i in elems {
                    buffer.push(i);
                    count += 1;
                }
                self.buffer = TypedBuffer::Int(buffer);
            }
            TypedBuffer::Int(buffer) => {
                for i in elems {
                    buffer.push(i);
                    count += 1;
                }
            }
            TypedBuffer::Mixed(buffer) => {
                for i in elems {
                    buffer.push(RawVal::Int(i));
                    count += 1;
                }
            }
            TypedBuffer::Float(buffer) => {
                // TODO: conversion is potentially lossy, convert into mixed column if necessary
                for i in elems {
                    buffer.push(i as f64);
                    count += 1;
                }
            }
            TypedBuffer::String(buffer) => {
                let mut buffer = MixedColBuffer {
                    data: buffer
                        .values
                        .iter()
                        .map(|s| RawVal::Str(s.to_string()))
                        .collect(),
                };
                for i in elems {
                    buffer.push(RawVal::Int(i));
                    count += 1;
                }
                self.buffer = TypedBuffer::Mixed(buffer);
            }
        }
        self.push_present(present, count);
        self.length += count;
    }

    pub fn push_floats<I: IntoIterator<Item = OrderedFloat<f64>>>(
        &mut self,
        elems: I,
        present: Option<&[u8]>,
    ) {
        let mut count = 0;
        match &mut self.buffer {
            TypedBuffer::Empty => {
                if self.len() > 0 {
                    self.init_present();
                }
                let mut buffer = FloatColBuffer::default();
                for _ in 0..self.len() {
                    buffer.push(0.0);
                }
                for f in elems {
                    buffer.push(f.0);
                    count += 1;
                }
                self.buffer = TypedBuffer::Float(buffer);
            }
            TypedBuffer::Float(buffer) => {
                for f in elems {
                    buffer.push(f.0);
                    count += 1;
                }
            }
            TypedBuffer::Int(buffer) => {
                // TODO: conversion is potentially lossy, convert into mixed column if necessary
                let mut float_buffer = FloatColBuffer::default();
                for i in buffer.data.iter() {
                    float_buffer.push(*i as f64);
                }
                for f in elems {
                    float_buffer.push(f.0);
                    count += 1;
                }
                self.buffer = TypedBuffer::Float(float_buffer);
            }
            TypedBuffer::String(buffer) => {
                let mut buffer = MixedColBuffer {
                    data: buffer
                        .values
                        .iter()
                        .map(|s| RawVal::Str(s.to_string()))
                        .collect(),
                };
                for f in elems {
                    buffer.push(RawVal::Float(f));
                    count += 1;
                }
                self.buffer = TypedBuffer::Mixed(buffer);
            }
            TypedBuffer::Mixed(buffer) => {
                for f in elems {
                    buffer.push(RawVal::Float(f));
                    count += 1;
                }
            }
        }
        self.push_present(present, count);
        self.length += count;
    }

    pub fn push_strings<'a, I: IntoIterator<Item = &'a str>>(
        &mut self,
        elems: I,
        present: Option<&[u8]>,
    ) {
        let mut count = 0;
        match &mut self.buffer {
            TypedBuffer::Empty => {
                if self.len() > 0 {
                    self.init_present();
                }
                let mut buffer = StringColBuffer::default();
                for _ in 0..self.len() {
                    buffer.push("");
                }
                for s in elems {
                    buffer.push(s);
                    count += 1;
                }
                self.buffer = TypedBuffer::String(buffer);
            }
            TypedBuffer::String(buffer) => {
                for s in elems {
                    buffer.push(s);
                    count += 1;
                }
            }
            TypedBuffer::Int(buffer) => {
                let mut buffer = MixedColBuffer {
                    data: buffer
                        .data
                        .iter()
                        .map(|s| RawVal::Str(s.to_string()))
                        .collect(),
                };
                for s in elems {
                    buffer.push(RawVal::Str(s.to_string()));
                    count += 1;
                }
                self.buffer = TypedBuffer::Mixed(buffer);
            }
            TypedBuffer::Float(buffer) => {
                let mut buffer = MixedColBuffer {
                    data: buffer
                        .data
                        .iter()
                        .map(|s| RawVal::Str(s.to_string()))
                        .collect(),
                };
                for s in elems {
                    buffer.push(RawVal::Str(s.to_string()));
                    count += 1;
                }
                self.buffer = TypedBuffer::Mixed(buffer);
            }
            TypedBuffer::Mixed(buffer) => {
                for s in elems {
                    buffer.push(RawVal::Str(s.to_string()));
                    count += 1;
                }
            }
        }
        self.push_present(present, count);
        self.length += count;
    }

    fn push_present(&mut self, new_present: Option<&[u8]>, count: usize) {
        if let Some(all_present) = self.present.as_mut() {
            if let Some(new_present) = new_present {
                for i in 0..count {
                    if BitVec::is_set(new_present, i) {
                        BitVecMut::set(all_present, self.length + i);
                    }
                }
            } else {
                for i in 0..count {
                    BitVecMut::set(all_present, self.length + i);
                }
            }
        }
    }

    fn init_present(&mut self) {
        assert!(self.present.is_none());
        match self.buffer {
            TypedBuffer::Empty => {
                let present = vec![0; self.length / 8];
                self.present = Some(present);
            }
            _ => {
                let mut present = vec![0; self.length / 8];
                for i in 0..self.length {
                    BitVecMut::set(&mut present, i);
                }
                self.present = Some(present);
            }
        }
    }

    pub fn push_nulls(&mut self, count: usize) {
        match &mut self.buffer {
            TypedBuffer::Empty => {}
            buffer => {
                if self.present.is_none() {
                    let mut present = vec![0xff; self.length / 8];
                    for i in ((self.length / 8) * 8)..self.length {
                        BitVecMut::set(&mut present, i);
                    }
                    self.present = Some(present);
                }
                match buffer {
                    TypedBuffer::Int(buffer) => {
                        for _ in 0..count {
                            buffer.push(0);
                        }
                    }
                    TypedBuffer::Float(buffer) => {
                        for _ in 0..count {
                            buffer.push(0.0);
                        }
                    }
                    TypedBuffer::Mixed(buffer) => {
                        for _ in 0..count {
                            buffer.push(RawVal::Null);
                        }
                    }
                    TypedBuffer::String(buffer) => {
                        for _ in 0..count {
                            buffer.push("");
                        }
                    }
                    TypedBuffer::Empty => {}
                }
            }
        }
        self.length += count;
    }

    pub fn finalize(self, name: &str) -> Arc<Column> {
        match self.buffer {
            TypedBuffer::Empty => Arc::new(Column::null(name, self.length)),
            TypedBuffer::Int(buffer) => buffer.finalize(name, self.present),
            TypedBuffer::Float(buffer) => buffer.finalize(name, self.present),
            TypedBuffer::String(buffer) => buffer.finalize(name, self.present),
            TypedBuffer::Mixed(buffer) => buffer.finalize(name, self.present),
        }
    }

    pub fn len(&self) -> usize {
        self.length
    }
}

#[derive(Default, Clone, Debug, DataSize)]
enum TypedBuffer {
    #[default]
    Empty,
    String(StringColBuffer),
    Int(IntColBuffer),
    Float(FloatColBuffer),
    Mixed(MixedColBuffer),
}

#[derive(Clone, Debug, DataSize)]
struct StringColBuffer {
    values: IndexedPackedStrings,
    lhex: bool,
    uhex: bool,
    string_bytes: usize,
}

impl Default for StringColBuffer {
    fn default() -> StringColBuffer {
        StringColBuffer {
            values: IndexedPackedStrings::default(),
            lhex: true,
            uhex: true,
            string_bytes: 0,
        }
    }
}

impl StringColBuffer {
    fn push(&mut self, elem: &str) {
        self.lhex = self.lhex && is_lowercase_hex(elem);
        self.uhex = self.uhex && is_uppercase_hex(elem);
        self.string_bytes += elem.len();
        self.values.push(elem);
    }

    fn finalize(self, name: &str, present: Option<Vec<u8>>) -> Arc<Column> {
        fast_build_string_column(
            name,
            self.values.iter(),
            self.values.len(),
            self.lhex,
            self.uhex,
            self.string_bytes,
            present,
        )
    }
}

#[derive(Clone, Debug, DataSize)]
struct IntColBuffer {
    data: Vec<i64>,
    min: i64,
    max: i64,
    increasing: u64,
    allow_delta_encode: bool,
    last: i64,
}

impl Default for IntColBuffer {
    fn default() -> IntColBuffer {
        IntColBuffer {
            data: Vec::new(),
            min: i64::MAX,
            max: i64::MIN,
            increasing: 0,
            allow_delta_encode: true,
            last: i64::MIN,
        }
    }
}

impl IntColBuffer {
    fn push(&mut self, elem: i64) {
        // PERF: can set arbitrary values for null to help compression (extend from last/previous value)
        self.min = cmp::min(elem, self.min);
        self.max = cmp::max(elem, self.max);
        if elem > self.last {
            self.increasing += 1;
        } else if elem.checked_sub(self.last).is_none() {
            self.allow_delta_encode = false;
        };
        self.last = elem;
        self.data.push(elem);
    }

    fn finalize(self, name: &str, present: Option<Vec<u8>>) -> Arc<Column> {
        // PERF: heuristic for deciding delta encoding could probably be improved
        let delta_encode =
            self.allow_delta_encode && (self.increasing * 10 > self.data.len() as u64 * 9);
        IntegerColumn::new_boxed(name, self.data, self.min, self.max, delta_encode, present)
    }
}

#[derive(Default, Clone, Debug, DataSize)]
struct FloatColBuffer {
    data: Vec<f64>,
}

impl FloatColBuffer {
    #[inline]
    fn push(&mut self, elem: f64) {
        self.data.push(elem);
    }

    fn finalize(self, name: &str, present: Option<Vec<u8>>) -> Arc<Column> {
        FloatColumn::new_boxed(
            name,
            unsafe { std::mem::transmute::<Vec<f64>, Vec<OrderedFloat<f64>>>(self.data) },
            present,
        )
    }
}

#[derive(Default, Clone, Debug, DataSize)]
struct MixedColBuffer {
    data: Vec<RawVal>,
}

impl MixedColBuffer {
    fn push(&mut self, elem: RawVal) {
        self.data.push(elem);
    }

    fn finalize(self, name: &str, present: Option<Vec<u8>>) -> Arc<Column> {
        // TODO: allow for mixed columns
        let mut string_col = StringColBuffer::default();
        for elem in self.data {
            match elem {
                RawVal::Str(s) => string_col.push(&s),
                RawVal::Int(i) => string_col.push(&i.to_string()),
                RawVal::Float(f) => string_col.push(&f.to_string()),
                RawVal::Null => {}
            }
        }
        string_col.finalize(name, present)
    }
}

fn is_lowercase_hex(string: &str) -> bool {
    string.len() & 1 == 0
        && string.chars().all(|c| {
            c == '0'
                || c == '1'
                || c == '2'
                || c == '3'
                || c == '4'
                || c == '5'
                || c == '6'
                || c == '7'
                || c == '8'
                || c == '9'
                || c == 'a'
                || c == 'b'
                || c == 'c'
                || c == 'd'
                || c == 'e'
                || c == 'f'
        })
}

fn is_uppercase_hex(string: &str) -> bool {
    string.len() & 1 == 0
        && string.chars().all(|c| {
            c == '0'
                || c == '1'
                || c == '2'
                || c == '3'
                || c == '4'
                || c == '5'
                || c == '6'
                || c == '7'
                || c == '8'
                || c == '9'
                || c == 'A'
                || c == 'B'
                || c == 'C'
                || c == 'D'
                || c == 'E'
                || c == 'F'
        })
}
