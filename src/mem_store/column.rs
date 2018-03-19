use bit_vec::BitVec;
use heapsize::HeapSizeOf;
use engine::types::*;
use std::fmt;
use engine::typed_vec::TypedVec;
use ingest::raw_val::RawVal;


pub struct Column {
    name: String,
    data: Box<ColumnData>,
}

impl Column {
    pub fn new(name: String, data: Box<ColumnData>) -> Column {
        Column {
            name,
            data,
        }
    }

    pub fn name(&self) -> &str { &self.name }
    pub fn len(&self) -> usize { self.data().len() }
    pub fn data(&self) -> &ColumnData { self.data.as_ref() }
}


impl HeapSizeOf for Column {
    fn heap_size_of_children(&self) -> usize {
        self.name.heap_size_of_children() + self.data.heap_size_of_children()
    }
}

pub trait ColumnData: HeapSizeOf + Send + Sync {
    fn collect_decoded(&self) -> TypedVec;
    fn filter_decode(&self, filter: &BitVec) -> TypedVec;
    fn index_decode(&self, filter: &[usize]) -> TypedVec;
    fn basic_type(&self) -> BasicType;
    fn to_codec(&self) -> Option<&ColumnCodec> { None }
    fn len(&self) -> usize;

    fn full_type(&self) -> Type {
        Type::new(self.basic_type(), self.to_codec())
    }
}

impl<'a> fmt::Debug for &'a ColumnData {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "<{:?}>", &self.basic_type())
    }
}


pub trait ColumnCodec: ColumnData {
    fn get_encoded(&self) -> TypedVec;
    fn filter_encoded(&self, filter: &BitVec) -> TypedVec;
    fn index_encoded(&self, filter: &[usize]) -> TypedVec;
    fn encoding_type(&self) -> EncodingType;

    fn encode_str(&self, _: &str) -> RawVal {
        panic!("encode_str not supported")
    }
}

impl<'a> fmt::Debug for &'a ColumnCodec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "<{:?}, {:?}>", &self.encoding_type(), &self.basic_type())
    }
}
