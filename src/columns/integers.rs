use value::ValueType;
use columns::{Column, ColIter};
use heapsize::HeapSizeOf;
use std::{u8, u16, u32, i64};

pub struct IntegerColumn {
    name: String,
    values: Vec<i64>
}

impl IntegerColumn {
    pub fn new(name: String, mut values: Vec<i64>, min: i64, max: i64) -> Box<Column> {
        if max - min <= u8::MAX as i64 {
            Box::new(IntegerColumn1B::new(name, values, min))
        } else if max - min <= u16::MAX as i64 {
            Box::new(IntegerColumn2B::new(name, values, min))
        } else if max - min <= u32::MAX as i64 {
            Box::new(IntegerColumn4B::new(name, values, min))
        } else {
            values.shrink_to_fit();
            Box::new(IntegerColumn {
                name: name,
                values: values,
            })
        }
    }
}

impl Column for IntegerColumn {
    fn get_name(&self) -> &str {
        &self.name
    }

    fn iter<'a>(&'a self) -> ColIter<'a> {
        let iter = self.values.iter().map(|&i| ValueType::Integer(i));
        ColIter{iter: Box::new(iter)}
    }
}


struct IntegerColumn4B { name: String, values: Vec<u32>, offset: i64, }
struct IntegerColumn2B { name: String, values: Vec<u16>, offset: i64, }
struct IntegerColumn1B { name: String, values: Vec<u8>, offset: i64, }

impl IntegerColumn4B {
    fn new(name: String, values: Vec<i64>, offset: i64) -> IntegerColumn4B {
        let mut encoded_vals = Vec::with_capacity(values.len());
        for v in values {
            encoded_vals.push((v - offset) as u32);
        }
        IntegerColumn4B { name: name, values: encoded_vals, offset: offset, }
    }
}

impl IntegerColumn2B {
    fn new(name: String, values: Vec<i64>, offset: i64) -> IntegerColumn2B {
        let mut encoded_vals = Vec::with_capacity(values.len());
        for v in values {
            encoded_vals.push((v - offset) as u16);
        }
        IntegerColumn2B { name: name, values: encoded_vals, offset: offset, }
    }
}

impl IntegerColumn1B {
    fn new(name: String, values: Vec<i64>, offset: i64) -> IntegerColumn1B {
        let mut encoded_vals = Vec::with_capacity(values.len());
        for v in values {
            encoded_vals.push((v - offset) as u8);
        }
        IntegerColumn1B { name: name, values: encoded_vals, offset: offset, }
    }
}

impl Column for IntegerColumn4B {
    fn get_name(&self) -> &str { &self.name }
    
    fn iter<'a>(&'a self) -> ColIter<'a> {
        let offset = self.offset;
        let iter = self.values.iter().map(move |i| ValueType::Integer(*i as i64 + offset));
        ColIter { iter: Box::new(iter) }
    }
}

impl Column for IntegerColumn2B {
    fn get_name(&self) -> &str { &self.name }
    
    fn iter<'a>(&'a self) -> ColIter<'a> {
        let offset = self.offset;
        let iter = self.values.iter().map(move |i| ValueType::Integer(*i as i64 + offset));
        ColIter { iter: Box::new(iter) }
    }
}

impl Column for IntegerColumn1B {
    fn get_name(&self) -> &str { &self.name }
    
    fn iter<'a>(&'a self) -> ColIter<'a> {
        let offset = self.offset;
        let iter = self.values.iter().map(move |i| ValueType::Integer(*i as i64 + offset));
        ColIter { iter: Box::new(iter) }
    }
}


impl HeapSizeOf for IntegerColumn {
    fn heap_size_of_children(&self) -> usize {
        self.name.heap_size_of_children() + self.values.heap_size_of_children()
    }
}

impl HeapSizeOf for IntegerColumn4B {
    fn heap_size_of_children(&self) -> usize {
        self.name.heap_size_of_children() + self.values.heap_size_of_children()
    }
}

impl HeapSizeOf for IntegerColumn2B {
    fn heap_size_of_children(&self) -> usize {
        self.name.heap_size_of_children() + self.values.heap_size_of_children()
    }
}

impl HeapSizeOf for IntegerColumn1B {
    fn heap_size_of_children(&self) -> usize {
        self.name.heap_size_of_children() + self.values.heap_size_of_children()
    }
}

