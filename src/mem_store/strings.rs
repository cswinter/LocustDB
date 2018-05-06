use bit_vec::BitVec;
use ingest::raw_val::RawVal;
use mem_store::column::{ColumnData, ColumnCodec};
use mem_store::column_builder::UniqueValues;
use mem_store::point_codec::PointCodec;
use heapsize::HeapSizeOf;
use std::collections::hash_set::HashSet;
use std::collections::HashMap;
use std::rc::Rc;
use std::str;
use std::{u8, u16};
use engine::types::*;
use engine::typed_vec::TypedVec;


pub const MAX_UNIQUE_STRINGS: usize = 10000;

pub fn build_string_column(values: &[Option<Rc<String>>],
                           unique_values: UniqueValues<Option<Rc<String>>>)
                           -> Box<ColumnData> {
    if let Some(u) = unique_values.get_values() {
        Box::new(DictEncodedStrings::from_strings(values, u))
    } else {
        Box::new(StringPacker::from_strings(values))
    }
}

struct StringPacker {
    data: Vec<u8>,
}

// TODO(clemens): encode using variable size length + special value to represent null
impl StringPacker {
    pub fn new() -> StringPacker {
        StringPacker { data: Vec::new() }
    }

    pub fn from_strings(strings: &[Option<Rc<String>>]) -> StringPacker {
        let mut sp = StringPacker::new();
        for string in strings {
            match *string {
                Some(ref string) => sp.push(string),
                None => sp.push(""),
            }
        }
        sp.shrink_to_fit();
        sp
    }

    pub fn push(&mut self, string: &str) {
        for &byte in string.as_bytes().iter() {
            self.data.push(byte);
        }
        self.data.push(0);
    }

    pub fn shrink_to_fit(&mut self) {
        self.data.shrink_to_fit();
    }

    pub fn iter(&self) -> StringPackerIterator {
        StringPackerIterator {
            data: &self.data,
            curr_index: 0,
        }
    }
}

impl ColumnData for StringPacker {
    fn collect_decoded(&self) -> TypedVec {
        TypedVec::String(self.iter().collect())
    }

    fn filter_decode<'a>(&'a self, filter: &BitVec) -> TypedVec {
        let mut result = Vec::new();
        for (s, select) in self.iter().zip(filter.iter()) {
            if select {
                result.push(s);
            }
        }
        TypedVec::String(result)
    }

    fn index_decode<'a>(&'a self, filter: &[usize]) -> TypedVec {
        let decoded = self.iter().collect::<Vec<_>>();
        let mut result = Vec::with_capacity(filter.len());
        for &i in filter {
            result.push(decoded[i]);
        }
        TypedVec::String(result)
    }

    fn basic_type(&self) -> BasicType { BasicType::String }

    fn len(&self) -> usize { self.iter().count() } // FIXME(clemens): O(n)
}

impl HeapSizeOf for StringPacker {
    fn heap_size_of_children(&self) -> usize {
        self.data.heap_size_of_children()
    }
}

pub struct StringPackerIterator<'a> {
    data: &'a Vec<u8>,
    curr_index: usize,
}

impl<'a> Iterator for StringPackerIterator<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<&'a str> {
        if self.curr_index >= self.data.len() {
            return None;
        }

        let mut index = self.curr_index;
        while self.data[index] != 0 {
            index += 1;
        }
        let result = unsafe { str::from_utf8_unchecked(&self.data[self.curr_index..index]) };
        self.curr_index = index + 1;
        Some(result)
    }
}

struct DictEncodedStrings {
    mapping: Vec<Option<String>>,
    encoded_values: Vec<u16>,
}

impl DictEncodedStrings {
    pub fn from_strings(strings: &[Option<Rc<String>>],
                        unique_values: HashSet<Option<Rc<String>>>)
                        -> DictEncodedStrings {
        assert!(unique_values.len() <= u16::MAX as usize);

        let mut mapping: Vec<Option<String>> =
            unique_values.into_iter().map(|o| o.map(|s| s.as_str().to_owned())).collect();
        mapping.sort();
        let encoded_values: Vec<u16> = {
            let reverse_mapping: HashMap<Option<&String>, u16> =
                mapping.iter().map(Option::as_ref).zip(0..).collect();
            strings.iter().map(|o| reverse_mapping[&o.as_ref().map(|x| &**x)]).collect()
        };

        DictEncodedStrings {
            mapping,
            encoded_values,
        }
    }
}

impl ColumnData for DictEncodedStrings {
    fn collect_decoded(&self) -> TypedVec {
        self.decode(&self.encoded_values)
    }

    fn filter_decode(&self, filter: &BitVec) -> TypedVec {
        let mut result = Vec::<&str>::with_capacity(self.encoded_values.len());
        for (encoded_value, selected) in self.encoded_values.iter().zip(filter) {
            if selected {
                result.push(self.mapping[*encoded_value as usize].as_ref().unwrap());
            }
        }
        TypedVec::String(result)
    }

    fn index_decode(&self, filter: &[usize]) -> TypedVec {
        PointCodec::index_decode(self, &self.encoded_values, filter)
    }

    fn basic_type(&self) -> BasicType { BasicType::String }
    fn to_codec(&self) -> Option<&ColumnCodec> { Some(self as &ColumnCodec) }
    fn len(&self) -> usize { self.encoded_values.len() }
}

impl PointCodec<u16> for DictEncodedStrings {
    fn decode(&self, data: &[u16]) -> TypedVec {
        let mut result = Vec::<&str>::with_capacity(self.encoded_values.len());
        for encoded_value in data {
            result.push(self.mapping[*encoded_value as usize].as_ref().unwrap());
        }
        TypedVec::String(result)
    }

    fn index_decode(&self, data: &[u16], filter: &[usize]) -> TypedVec {
        let mut result = Vec::<&str>::with_capacity(filter.len());
        for &i in filter {
            let encoded_value = data[i];
            result.push(self.mapping[encoded_value as usize].as_ref().unwrap());
        }
        TypedVec::String(result)
    }

    fn to_raw(&self, elem: u16) -> RawVal {
        RawVal::Str(self.mapping[elem as usize].as_ref().unwrap().to_string())
    }

    fn max_cardinality(&self) -> usize { self.mapping.len() }
}

impl ColumnCodec for DictEncodedStrings {
    fn get_encoded(&self) -> TypedVec {
        TypedVec::BorrowedEncodedU16(&self.encoded_values, self as &PointCodec<u16>)
    }

    fn unwrap_decode<'a>(&'a self, data: &TypedVec<'a>) -> TypedVec<'a> {
        self.decode(data.cast_ref_u16().0)
    }

    fn filter_encoded(&self, filter: &BitVec) -> TypedVec {
        let mut result = Vec::with_capacity(self.encoded_values.len());
        for (encoded_value, selected) in self.encoded_values.iter().zip(filter) {
            if selected {
                result.push(*encoded_value);
            }
        }
        TypedVec::EncodedU16(result, self as &PointCodec<u16>)
    }

    fn index_encoded(&self, filter: &[usize]) -> TypedVec {
        let mut result = Vec::with_capacity(filter.len());
        for &i in filter {
            result.push(self.encoded_values[i]);
        }
        TypedVec::EncodedU16(result, self as &PointCodec<u16>)
    }

    fn encoding_type(&self) -> EncodingType { EncodingType::U16 }

    fn encode_str(&self, s: &str) -> RawVal {
        for (i, val) in self.mapping.iter().enumerate() {
            if val.as_ref().unwrap() == s {
                return RawVal::Int(i as i64);
            }
        }
        RawVal::Int(-1)
    }

    fn is_summation_preserving(&self) -> bool { false }
    fn is_order_preserving(&self) -> bool { true }
    fn is_positive_integer(&self) -> bool { true }
    fn encoding_range(&self) -> Option<(i64, i64)> { Some((0, self.mapping.len() as i64)) }
}

impl HeapSizeOf for DictEncodedStrings {
    fn heap_size_of_children(&self) -> usize {
        self.mapping.heap_size_of_children() + self.encoded_values.heap_size_of_children()
    }
}
