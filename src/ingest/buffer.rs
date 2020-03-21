use std::collections::HashMap;
use crate::mem_store::raw_col::MixedCol;
use crate::ingest::raw_val::RawVal;
use crate::ingest::input_column::InputColumn;
use std::cmp;

#[derive(PartialEq, Debug)]
pub struct Buffer {
    pub buffer: HashMap<String, MixedCol>,
    pub length: usize,
}

impl Default for Buffer {
    fn default() -> Buffer {
        Buffer {
            buffer: HashMap::new(),
            length: 0,
        }
    }
}

impl Buffer {
    pub fn push_row(&mut self, row: Vec<(String, RawVal)>) {
        let len = self.len();
        for (name, input_val) in row {
            let buffered_col = self.buffer.entry(name)
                .or_insert_with(|| MixedCol::with_nulls(len));
            buffered_col.push(input_val);
        }
        self.length += 1;
        self.extend_to_largest();
    }

    pub fn push_typed_cols(&mut self, columns: HashMap<String, InputColumn>) {
        let len = self.len();
        let mut new_length = 0;
        for (name, input_col) in columns {
            let buffered_col = self.buffer.entry(name)
                .or_insert_with(|| MixedCol::with_nulls(len));
            match input_col {
                InputColumn::Int(vec) => buffered_col.push_ints(vec),
                InputColumn::Str(vec) => buffered_col.push_strings(vec),
                InputColumn::Null(c) => buffered_col.push_nulls(c),
            }
            new_length = cmp::max(new_length, buffered_col.len())
        }
        self.length = new_length;
        self.extend_to_largest();
    }

    pub fn push_untyped_cols(&mut self, columns: HashMap<String, Vec<RawVal>>) {
        let len = self.len();
        let mut new_length = 0;
        for (name, input_vals) in columns {
            let buffered_col = self.buffer.entry(name)
                .or_insert_with(|| MixedCol::with_nulls(len));
            for input_val in input_vals {
                buffered_col.push(input_val);
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

    pub fn heap_size_of_children(&self) -> usize {
        self.buffer.iter().map(|(_, v)| {
            // Currently does not take into account the memory of String.
            v.heap_size_of_children()
        }).sum()
    }
}

