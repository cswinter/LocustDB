use bit_vec::BitVec;
use heapsize::HeapSizeOf;
use value::Val;
use engine::types::Type;
use std::fmt;


pub struct Column {
    name: String,
    data: Box<ColumnData>,
}

impl Column {
    pub fn get_name(&self) -> &str {
        &self.name
    }

    pub fn iter<'a>(&'a self) -> ColIter<'a> {
        self.data.iter()
    }

    pub fn dump_untyped<'a>(&'a self, count: usize, offset: usize, buffer: &mut Vec<Val<'a>>) {
        self.data.dump_untyped(count, offset, buffer);
    }

    pub fn collect_str<'a>(&'a self, count: usize, offset: usize, filter: &Option<BitVec>, buffer: &mut Vec<&'a str>) {
        self.data.collect_str(count, offset, filter, buffer);
    }

    pub fn collect_int<'a>(&'a self, count: usize, offset: usize, filter: &Option<BitVec>, buffer: &mut Vec<i64>) {
        self.data.collect_int(count, offset, filter, buffer);
    }

    pub fn decoded_type(&self) -> Type {
        self.data.decoded_type()
    }

    pub fn new(name: String, data: Box<ColumnData>) -> Column {
        Column {
            name: name,
            data: data,
        }
    }
}

impl fmt::Debug for Column {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "<{}>", &self.name)
    }
}

impl HeapSizeOf for Column {
    fn heap_size_of_children(&self) -> usize {
        self.name.heap_size_of_children() + self.data.heap_size_of_children()
    }
}

pub trait ColumnData: HeapSizeOf + Send + Sync {
    fn iter<'a>(&'a self) -> ColIter<'a>;
    fn dump_untyped<'a>(&'a self, count: usize, offset: usize, buffer: &mut Vec<Val<'a>>);

    #[allow(unused_variables)]
    fn collect_str<'a>(&'a self, count: usize, offset: usize, filter: &Option<BitVec>, buffer: &mut Vec<&'a str>) {
        panic!("Not supported");
    }

    #[allow(unused_variables)]
    fn collect_int<'a>(&'a self, count: usize, offset: usize, filter: &Option<BitVec>, buffer: &mut Vec<i64>) {
        panic!("Not supported");
    }

    fn decoded_type(&self) -> Type;
}

pub struct ColIter<'a> {
    iter: Box<Iterator<Item=Val<'a>> + 'a>,
}

impl<'a> ColIter<'a> {
    pub fn new<T: Iterator<Item=Val<'a>> + 'a>(iter: T) -> ColIter<'a> {
        ColIter { iter: Box::new(iter) }
    }
}

impl<'a> Iterator for ColIter<'a> {
    type Item = Val<'a>;

    fn next(&mut self) -> Option<Val<'a>> {
        self.iter.next()
    }
}
