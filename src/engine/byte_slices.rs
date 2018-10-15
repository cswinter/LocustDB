use std::cmp::min;
use std::fmt::Write;
use std::fmt;

use hex;
use itertools::Itertools;

use engine::typed_vec::*;
use engine::types::*;
use ingest::raw_val::RawVal;


#[derive(HeapSizeOf, Debug)]
pub struct ByteSlices<'a> {
    pub row_len: usize,
    pub data: Vec<&'a [u8]>,
}

impl<'a> ByteSlices<'a> {
    pub fn new(row_len: usize) -> ByteSlices<'a> {
        ByteSlices { row_len, data: Vec::default() }
    }

    pub fn with_capacity(row_len: usize, rows: usize) -> ByteSlices<'a> {
        ByteSlices { row_len, data: Vec::with_capacity(row_len * rows) }
    }

    pub fn row(&self, i: usize) -> &[&'a [u8]] {
        &self.data[i * self.row_len..(i + 1) * self.row_len]
    }
}

impl<'a> AnyVec<'a> for ByteSlices<'a> {
    fn len(&self) -> usize { self.data.len() / self.row_len }
    fn get_raw(&self, _i: usize) -> RawVal { panic!(self.type_error("get_raw")) }
    fn get_type(&self) -> EncodingType { EncodingType::ByteSlices(self.row_len) }

    fn sort_indices_desc(&self, indices: &mut Vec<usize>) {
        indices.sort_unstable_by(|i, j| self.row(*i).cmp(&self.row(*j)).reverse());
    }

    fn sort_indices_asc(&self, indices: &mut Vec<usize>) {
        indices.sort_unstable_by_key(|i| self.row(*i));
    }

    fn append_all(&mut self, _other: &AnyVec<'a>, _count: usize) -> Option<BoxedVec<'a>> {
        panic!(self.type_error("append_all"))
    }

    fn slice_box<'b>(&'b self, _from: usize, _to: usize) -> BoxedVec<'b> where 'a: 'b {
        panic!(self.type_error("slice_box"))
        // let to = min(to, self.len());
        // Box::new(&self[self.row_len * from..self.row_len * to])
    }

    fn type_error(&self, func_name: &str) -> String { format!("RawByteSlices.{}", func_name) }

    fn display(&self) -> String {
        format!("ByteSlices[{}]{}", self.row_len, display_byte_slices(&self.data, 120))
    }

    fn cast_ref_byte_slices(&self) -> &ByteSlices<'a> { self }
    fn cast_ref_mut_byte_slices(&mut self) -> &mut ByteSlices<'a> { self }
}

pub fn display_byte_slices(slice: &[&[u8]], max_chars: usize) -> String {
    let mut length = slice.len();
    loop {
        let result = _display_slice(slice, length);
        if result.len() < max_chars { break; }
        length = min(length - 1, max_chars * length / result.len());
        if length < 3 {
            return _display_slice(slice, 2);
        }
    }
    if length == slice.len() {
        return _display_slice(slice, slice.len());
    }
    for l in length..max_chars {
        if _display_slice(slice, l).len() > max_chars {
            return _display_slice(slice, l - 1);
        }
    }
    "display_slice error!".to_owned()
}

fn _display_slice(slice: &[&[u8]], max: usize) -> String {
    let mut result = String::new();
    write!(result, "[").unwrap();
    write!(result,
           "{}",
           slice[..max]
               .iter()
               .map(|x| format!("0x{}", hex::encode(x)))
               .join(", ")
    ).unwrap();
    if max < slice.len() {
        write!(result, ", ...] ({} more)", slice.len() - max).unwrap();
    } else {
        write!(result, "]").unwrap();
    }
    result
}

impl<'a> fmt::Display for ByteSlices<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", display_byte_slices(&self.data, 120))
    }
}