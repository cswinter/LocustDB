use bit_vec::BitVec;
use heapsize::HeapSizeOf;
use engine::types::Type;
use std::fmt;
use engine::typed_vec::TypedVec;


pub struct Column {
    name: String,
    data: Box<ColumnData>,
}

impl Column {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn data(&self) -> &ColumnData { self.data.as_ref() }

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
    fn collect_decoded(&self) -> TypedVec;
    fn filter_decode(&self, filter: &BitVec) -> TypedVec;
    fn index_decode(&self, filter: &Vec<usize>) -> TypedVec;
    fn decoded_type(&self) -> Type;
    fn to_codec(&self) -> Option<&ColumnCodec> { None }
}

impl<'a> fmt::Debug for &'a ColumnData {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "<{:?}>", &self.decoded_type())
    }
}


pub trait ColumnCodec: ColumnData {
    fn get_encoded(&self) -> TypedVec;
    fn filter_encoded(&self, filter: &BitVec) -> TypedVec;
    fn index_encoded(&self, filter: &Vec<usize>) -> TypedVec;
    fn encoded_type(&self) -> Type;
    fn ref_encoded_type(&self) -> Type;
}

impl<'a> fmt::Debug for &'a ColumnCodec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "<{:?}, {:?}>", &self.encoded_type(), &self.decoded_type())
    }
}
