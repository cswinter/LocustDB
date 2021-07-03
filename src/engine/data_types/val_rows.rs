use std::cmp::min;
use std::fmt;
use std::fmt::Write;

use itertools::Itertools;

use crate::engine::data_types::*;
use crate::ingest::raw_val::RawVal;
use crate::mem_store::value::Val;

#[derive(Debug, Clone)]
pub struct ValRows<'a> {
    pub row_len: usize,
    pub data: Vec<Val<'a>>,
}

impl<'a> ValRows<'a> {
    pub fn new(row_len: usize) -> ValRows<'a> {
        ValRows {
            row_len,
            data: Vec::default(),
        }
    }

    pub fn with_capacity(row_len: usize, rows: usize) -> ValRows<'a> {
        ValRows {
            row_len,
            data: Vec::with_capacity(row_len * rows),
        }
    }

    pub fn row(&self, i: usize) -> &[Val<'a>] {
        &self.data[i * self.row_len..(i + 1) * self.row_len]
    }
}

impl<'a> Data<'a> for ValRows<'a> {
    fn len(&self) -> usize {
        self.data.len() / self.row_len
    }
    fn get_raw(&self, _i: usize) -> RawVal {
        panic!("{}", self.type_error("get_raw"))
    }
    fn get_type(&self) -> EncodingType {
        EncodingType::ValRows
    }

    fn append_all(&mut self, _other: &dyn Data<'a>, _count: usize) -> Option<BoxedData<'a>> {
        panic!("{}", self.type_error("append_all"))
    }

    fn slice_box<'b>(&'b self, _from: usize, _to: usize) -> BoxedData<'b>
    where
        'a: 'b,
    {
        panic!("{}", self.type_error("slice_box"))
        // let to = min(to, self.len());
        // Box::new(&self[self.row_len * from..self.row_len * to])
    }

    fn type_error(&self, func_name: &str) -> String {
        format!("ValRows.{}", func_name)
    }

    fn display(&self) -> String {
        format!("ValRows[{}]{}", self.row_len, display_vals(&self.data, 120))
    }

    // fn cast_ref_byte_slices(&self) -> &ByteSlices<'a> { self }
    fn cast_ref_mut_val_rows(&mut self) -> &mut ValRows<'a> {
        self
    }
}

pub fn display_vals(vals: &[Val], max_chars: usize) -> String {
    let mut length = vals.len();
    loop {
        let result = _display_vals(vals, length);
        if result.len() < max_chars {
            break;
        }
        length = min(length - 1, max_chars * length / result.len());
        if length < 3 {
            return _display_vals(vals, 2);
        }
    }
    if length == vals.len() {
        return _display_vals(vals, vals.len());
    }
    for l in length..max_chars {
        if _display_vals(vals, l).len() > max_chars {
            return _display_vals(vals, l - 1);
        }
    }
    "display_vals error!".to_owned()
}

fn _display_vals(slice: &[Val], max: usize) -> String {
    let mut result = String::new();
    write!(result, "[").unwrap();
    write!(result, "[").unwrap();
    write!(
        result,
        "{}",
        slice[..max].iter().map(|x| format!("{:?}", x)).join(", ")
    )
    .unwrap();
    if max < slice.len() {
        write!(result, ", ...] ({} more)", slice.len() - max).unwrap();
    } else {
        write!(result, "]").unwrap();
    }
    result
}

impl<'a> fmt::Display for ValRows<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", display_vals(&self.data, 120))
    }
}
