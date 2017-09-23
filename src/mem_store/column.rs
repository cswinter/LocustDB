use heapsize::HeapSizeOf;
use value::Val;

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

    pub fn new(name: String, data: Box<ColumnData>) -> Column {
        Column {
            name: name,
            data: data,
        }
    }
}

impl HeapSizeOf for Column {
    fn heap_size_of_children(&self) -> usize {
        self.name.heap_size_of_children() + self.data.heap_size_of_children()
    }
}

pub trait ColumnData: HeapSizeOf + Send + Sync {
    fn iter<'a>(&'a self) -> ColIter<'a>;
}

pub struct ColIter<'a> {
    iter: Box<Iterator<Item = Val<'a>> + 'a>,
}

impl<'a> ColIter<'a> {
    pub fn new<T: Iterator<Item = Val<'a>> + 'a>(iter: T) -> ColIter<'a> {
        ColIter { iter: Box::new(iter) }
    }
}

impl<'a> Iterator for ColIter<'a> {
    type Item = Val<'a>;

    fn next(&mut self) -> Option<Val<'a>> {
        self.iter.next()
    }
}
