use bit_vec::BitVec;
use value::Val;
use mem_store::column::{ColumnData, ColIter};
use mem_store::column_builder::UniqueValues;
use heapsize::HeapSizeOf;
use std::collections::hash_set::HashSet;
use std::collections::HashMap;
use std::rc::Rc;
use std::str;
use std::{u8, u16};
use engine::types::Type;
use engine::typed_vec::TypedVec;


pub const MAX_UNIQUE_STRINGS: usize = 10000;

pub fn build_string_column(values: Vec<Option<Rc<String>>>,
                           unique_values: UniqueValues<Option<Rc<String>>>)
                           -> Box<ColumnData> {
    if let Some(u) = unique_values.get_values() {
        Box::new(DictEncodedStrings::from_strings(&values, u))
    } else {
        Box::new(StringPacker::from_strings(&values))
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

    pub fn from_strings(strings: &Vec<Option<Rc<String>>>) -> StringPacker {
        let mut sp = StringPacker::new();
        for string in strings {
            match string {
                &Some(ref string) => sp.push(string),
                &None => sp.push(""),
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
    fn iter<'a>(&'a self) -> ColIter<'a> {
        let iter = self.iter().map(|s| Val::Str(s));
        ColIter::new(iter)
    }

    fn collect_decoded<'a>(&'a self, filter: &Option<BitVec>) -> TypedVec {
        // TODO(clemens): at length method to StringPacker
        match filter {
            &None => TypedVec::String(self.iter().collect()),
            &Some(ref bv) => {
                let mut result = Vec::new();
                for (s, select) in self.iter().zip(bv.iter()) {
                    if select {
                        result.push(s);
                    }
                }
                TypedVec::String(result)
            }
        }

    }

    fn decoded_type(&self) -> Type { Type::String }
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
    pub fn from_strings(strings: &Vec<Option<Rc<String>>>,
                        unique_values: HashSet<Option<Rc<String>>>)
                        -> DictEncodedStrings {
        assert!(unique_values.len() <= u16::MAX as usize);

        let mapping: Vec<Option<String>> =
            unique_values.into_iter().map(|o| o.map(|s| s.as_str().to_owned())).collect();
        let encoded_values: Vec<u16> = {
            let reverse_mapping: HashMap<Option<&String>, u16> =
                mapping.iter().map(Option::as_ref).zip(0..).collect();
            strings.iter().map(|o| reverse_mapping[&o.as_ref().map(|x| &**x)]).collect()
        };

        // println!("\tMapping: {}MB; values: {}MB",
        //          mapping.heap_size_of_children() as f64 / 1024f64 / 1024f64,
        //          encoded_values.heap_size_of_children() as f64 / 1024f64 / 1024f64);

        DictEncodedStrings {
            mapping: mapping,
            encoded_values: encoded_values,
        }
    }
}

pub struct DictEncodedStringsIterator<'a> {
    data: &'a DictEncodedStrings,
    i: usize,
}

impl<'a> Iterator for DictEncodedStringsIterator<'a> {
    type Item = Option<&'a str>;

    fn next(&mut self) -> Option<Option<&'a str>> {
        if self.i >= self.data.encoded_values.len() {
            return None;
        }
        let encoded_value = &self.data.encoded_values[self.i];
        let value = &self.data.mapping[*encoded_value as usize];

        self.i += 1;
        Some(value.as_ref().map(|s| &**s))
    }
}

impl ColumnData for DictEncodedStrings {
    fn iter<'a>(&'a self) -> ColIter<'a> {
        let iter = DictEncodedStringsIterator { data: self, i: 0 }.map(Val::from);
        ColIter::new(iter)
    }

    fn collect_decoded<'a>(&'a self, filter: &Option<BitVec>) -> TypedVec {
        // TODO(clemens): optimize for filter?
        let mut result = Vec::<&'a str>::with_capacity(self.encoded_values.len());
        match filter {
            &None => {
                for encoded_value in self.encoded_values.iter() {
                    result.push(self.mapping[*encoded_value as usize].as_ref().unwrap());
                }
            }
            &Some(ref bv) => {
                for i in 0..bv.len() {
                    if bv.get(i) == Some(true) {
                        let encoded_value = self.encoded_values[i];
                        result.push(self.mapping[encoded_value as usize].as_ref().unwrap());
                    }
                }
            }
        }
        TypedVec::String(result)
    }

    fn decoded_type(&self) -> Type { Type::String }
}

impl HeapSizeOf for DictEncodedStrings {
    fn heap_size_of_children(&self) -> usize {
        self.mapping.heap_size_of_children() + self.encoded_values.heap_size_of_children()
    }
}
