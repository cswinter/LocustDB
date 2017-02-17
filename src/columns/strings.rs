use value::ValueType;
use columns::{ColumnData, ColIter};
use heapsize::HeapSizeOf;
use std::rc::Rc;
use std::str;

pub struct StringColumn {
    values: StringPacker,
}

impl StringColumn {
    pub fn new(values: Vec<Option<Rc<String>>>) -> StringColumn {
        StringColumn {
            values: StringPacker::from_strings(&values),
        }
    }
}

impl ColumnData for StringColumn {
    fn iter<'a>(&'a self) -> ColIter<'a> {
        let iter = self.values.iter().map(|s| ValueType::Str(s));
        ColIter{iter: Box::new(iter)}
    }
}

impl HeapSizeOf for StringColumn {
    fn heap_size_of_children(&self) -> usize {
        self.values.heap_size_of_children()
    }
}

struct StringPacker {
    data: Vec<u8>,
}


// TODO: encode using variable size length + special value to represent null
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
        StringPackerIterator { data: &self.data, curr_index: 0 }
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
        if self.curr_index >= self.data.len() { return None }

        let mut index = self.curr_index;
        while self.data[index] != 0 {
            index += 1;
        }
        let result = unsafe {
            str::from_utf8_unchecked(&self.data[self.curr_index..index])
        };
        self.curr_index = index + 1;
        Some(result)
    }
}
