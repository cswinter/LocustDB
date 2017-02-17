use value::ValueType;
use columns::{Column, ColIter};
use heapsize::HeapSizeOf;
use std::{u8, u16, u32, i64};
use num::traits::NumCast;

pub struct IntegerColumn {
    name: String,
    values: Vec<i64>
}

impl IntegerColumn {
    pub fn new(name: String, mut values: Vec<i64>, min: i64, max: i64) -> Box<Column> {
        if max - min <= u8::MAX as i64 {
            Box::new(IntegerOffsetColumn::<u8>::new(name, values, min))
        } else if max - min <= u16::MAX as i64 {
            Box::new(IntegerOffsetColumn::<u16>::new(name, values, min))
        } else if max - min <= u32::MAX as i64 {
            Box::new(IntegerOffsetColumn::<u32>::new(name, values, min))
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

trait IntLike : NumCast + HeapSizeOf {  }
impl IntLike for u8 {}
impl IntLike for u16 {}
impl IntLike for u32 {}

struct IntegerOffsetColumn<T: IntLike> {
    name: String,
    values: Vec<T>,
    offset: i64,
}

impl<T: IntLike> IntegerOffsetColumn<T> {
    fn new(name: String, values: Vec<i64>, offset: i64) -> IntegerOffsetColumn<T> {
        let mut encoded_vals = Vec::with_capacity(values.len());
        for v in values {
            encoded_vals.push(T::from(v - offset).unwrap());
        }
        IntegerOffsetColumn { name: name, values: encoded_vals, offset: offset, }
    }
}

impl<T: IntLike> Column for IntegerOffsetColumn<T> {
    fn get_name(&self) -> &str { &self.name }
    
    fn iter<'a>(&'a self) -> ColIter<'a> {
        let offset = self.offset;
        let iter = self.values.iter().map(move |i| ValueType::Integer(i.to_i64().unwrap() + offset));
        ColIter { iter: Box::new(iter) }
    }
}

impl HeapSizeOf for IntegerColumn {
    fn heap_size_of_children(&self) -> usize {
        self.name.heap_size_of_children() + self.values.heap_size_of_children()
    }
}

impl<T: IntLike> HeapSizeOf for IntegerOffsetColumn<T> {
    fn heap_size_of_children(&self) -> usize {
        self.name.heap_size_of_children() + self.values.heap_size_of_children()
    }
}
