use datasize::DataSize;
use ordered_float::OrderedFloat;

use crate::ingest::input_column::InputColumn;
use crate::ingest::raw_val::RawVal;
use crate::mem_store::column_buffer::ColumnBuffer;
use std::cmp;
use std::collections::HashMap;

#[derive(Debug, Clone, Default, DataSize)]
pub struct Buffer {
    pub buffer: HashMap<String, ColumnBuffer>,
    pub length: usize,
}

impl Buffer {
    pub fn push_row(&mut self, row: Vec<(String, RawVal)>) {
        let len = self.len();
        for (name, input_val) in row {
            let buffered_col = self
                .buffer
                .entry(name)
                .or_insert_with(|| ColumnBuffer::null(len));
            buffered_col.push_val(input_val);
        }
        self.length += 1;
        self.extend_to_largest();
    }

    pub fn push_typed_cols(&mut self, columns: HashMap<String, InputColumn>) {
        let len = self.len();
        let mut new_length = 0;
        for (name, input_col) in columns {
            let buffered_col = self
                .buffer
                .entry(name)
                .or_insert_with(|| ColumnBuffer::null(len));
            match input_col {
                InputColumn::Int(vec) => buffered_col.push_ints(vec, None),
                InputColumn::Str(vec) => {
                    buffered_col.push_strings(vec.iter().map(|s| s.as_str()), None)
                }
                InputColumn::Float(vec) => {
                    buffered_col.push_floats(vec.into_iter().map(OrderedFloat), None)
                }
                InputColumn::Null(c) => buffered_col.push_nulls(c),
                InputColumn::Mixed(vec) => {
                    for val in vec {
                        buffered_col.push_val(val);
                    }
                }
                InputColumn::NullableFloat(c, data) => {
                    let mut next_i = 0;
                    for (i, f) in data {
                        buffered_col.push_nulls((i - next_i) as usize);
                        buffered_col.push_val(RawVal::Float(OrderedFloat(f)));
                        next_i = i + 1;
                    }
                    buffered_col.push_nulls((c - next_i) as usize);
                }
                InputColumn::NullableInt(c, data) => {
                    let mut next_i = 0;
                    for (i, f) in data {
                        buffered_col.push_nulls((i - next_i) as usize);
                        buffered_col.push_val(RawVal::Int(f));
                        next_i = i + 1;
                    }
                    buffered_col.push_nulls((c - next_i) as usize);
                }
            }
            assert!(buffered_col.len() > self.length);
            assert!(new_length == 0 || new_length == buffered_col.len());
            new_length = cmp::max(new_length, buffered_col.len())
        }
        assert!(new_length > self.length);
        self.length = new_length;
        self.extend_to_largest();
    }

    pub fn push_untyped_cols(&mut self, columns: HashMap<String, Vec<RawVal>>) {
        let mut new_length = 0;
        for (name, input_vals) in columns {
            let buffered_col = self.buffer.entry(name).or_default();
            for input_val in input_vals {
                buffered_col.push_val(input_val);
            }
            new_length = cmp::max(new_length, buffered_col.len())
        }
        self.length = new_length;
        self.extend_to_largest();
    }

    fn extend_to_largest(&mut self) {
        let target_length = self.length;
        for buffered_col in self.buffer.values_mut() {
            let col_length = buffered_col.len();
            if col_length < target_length {
                buffered_col.push_nulls(target_length - col_length)
            }
        }
    }

    pub fn len(&self) -> usize {
        self.length
    }

    pub fn filter(&self, columns: &[String]) -> Buffer {
        let mut columns: HashMap<_, _> = columns
            .iter()
            .filter_map(|name| self.buffer.get(name).map(|col| (name.clone(), col.clone())))
            .collect();
        // Need at least one column to have a length
        if columns.is_empty() {
            let (key, val) = self.buffer.iter().next().unwrap();
            columns.insert(key.clone(), val.clone());
        }
        Buffer {
            buffer: columns,
            length: self.length,
        }
    }
}
