use std::collections::hash_set::HashSet;
use std::collections::HashMap;
use std::fmt;
use std::rc::Rc;
use std::str;
use std::{u8, u16};

use heapsize::HeapSizeOf;

use engine::typed_vec::{BoxedVec, TypedVec};
use engine::types::*;
use ingest::raw_val::RawVal;
use mem_store::*;
use mem_store::column_builder::UniqueValues;


pub const MAX_UNIQUE_STRINGS: usize = 10000;

pub fn build_string_column(name: &str,
                           values: &[Option<Rc<String>>],
                           unique_values: UniqueValues<Option<Rc<String>>>)
                           -> Box<Column> {
    if let Some(u) = unique_values.get_values() {
        let (indices, dictionary) = DictEncodedStrings::construct_dictionary(values, u);
        Column::encoded(name, indices, DictionaryEncoding { mapping: dictionary })
    } else {
        Box::new(StringPacker::from_strings(name.to_owned(), values))
    }
}

struct StringPacker {
    name: String,
    count: usize,
    data: Vec<u8>,
}

// TODO(clemens): encode using variable size length + special value to represent null
impl StringPacker {
    pub fn from_strings(name: String, strings: &[Option<Rc<String>>]) -> StringPacker {
        let mut sp = StringPacker { name, count: strings.len(), data: Vec::new() };
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

impl Column for StringPacker {
    fn name(&self) -> &str { &self.name }
    fn len(&self) -> usize { self.count }
    fn get_encoded(&self) -> Option<BoxedVec> { None }
    fn decode(&self) -> Option<BoxedVec> {
        Some(TypedVec::owned(self.iter().collect()))
    }
    fn codec(&self) -> Option<Codec> { None }
    fn encoding_type(&self) -> EncodingType { EncodingType::U8 }
    fn basic_type(&self) -> BasicType { BasicType::String }
}

impl fmt::Debug for StringPacker {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "<{}, StringPacked>", &self.name)
    }
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
    pub fn construct_dictionary(strings: &[Option<Rc<String>>],
                                unique_values: HashSet<Option<Rc<String>>>)
                                -> (Vec<u16>, Vec<String>) {
        assert!(unique_values.len() <= u16::MAX as usize);
        // TODO(clemens): handle null values
        let mut mapping: Vec<String> =
            unique_values.into_iter().map(|o| o.unwrap().to_string()).collect();
        mapping.sort();
        let encoded_values: Vec<u16> = {
            let reverse_mapping: HashMap<&String, u16> =
                mapping.iter().zip(0..).collect();
            strings.iter().map(|o| reverse_mapping[o.clone().unwrap().as_ref()]).collect()
        };

        (encoded_values, mapping)
    }
}

struct DictionaryEncoding {
    mapping: Vec<String>,
}

impl<'a> ColumnCodec<'a> for &'a DictionaryEncoding {
    fn unwrap_decode<'b>(&self, data: &TypedVec<'b>) -> BoxedVec<'b> where 'a: 'b {
        let data = data.cast_ref_u16();
        let mut result = Vec::<&str>::with_capacity(data.len());
        for encoded_value in data {
            result.push(self.mapping[*encoded_value as usize].as_ref());
        }
        TypedVec::owned(result)
    }

    fn encode_str(&self, s: &str) -> RawVal {
        // TODO(clemens): use binary search!
        for (i, val) in self.mapping.iter().enumerate() {
            if val == s {
                return RawVal::Int(i as i64);
            }
        }
        RawVal::Int(-1)
    }

    fn is_summation_preserving(&self) -> bool { false }
    fn is_order_preserving(&self) -> bool { true }
    fn is_positive_integer(&self) -> bool { true }
    fn encoding_range(&self) -> Option<(i64, i64)> { Some((0, self.mapping.len() as i64)) }
    fn encoding_type(&self) -> EncodingType { EncodingType::U16 }
    fn decoded_type(&self) -> BasicType { BasicType::String }
}

impl fmt::Debug for DictionaryEncoding {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "StringDictionary({})", self.mapping.len())
    }
}

impl HeapSizeOf for DictionaryEncoding {
    fn heap_size_of_children(&self) -> usize {
        self.mapping.heap_size_of_children()
    }
}
