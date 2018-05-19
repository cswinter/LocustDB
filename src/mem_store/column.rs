use std::fmt;
use std::sync::Arc;
// use std::mem;

use heapsize::HeapSizeOf;
use engine::types::*;
use engine::typed_vec::{BoxedVec, TypedVec};
use ingest::raw_val::RawVal;

pub trait Column: HeapSizeOf + fmt::Debug + Send + Sync {
    fn name(&self) -> &str;
    fn len(&self) -> usize;
    fn get_encoded(&self) -> Option<BoxedVec>;
    fn decode(&self) -> Option<BoxedVec>;
    fn codec(&self) -> Option<Codec>;
    fn basic_type(&self) -> BasicType;
    fn encoding_type(&self) -> EncodingType;
    fn full_type(&self) -> Type {
        Type::new(self.basic_type(), self.codec())
    }
}

impl Column {
    pub fn plain<T: TypedVec<'static> + HeapSizeOf + 'static>(name: &str, data: T) -> Box<Column> {
        Box::new(PlainColumn { name: name.to_owned(), data })
    }

    pub fn encoded<T, C>(name: &str, data: T, codec: C) -> Box<Column>
        where T: TypedVec<'static> + HeapSizeOf + Sync + Send + 'static,
              C: HeapSizeOf + Sync + Send + 'static,
              for<'a> &'a C: ColumnCodec<'a> {
        Box::new(PlainEncodedColumn { name: name.to_owned(), data, codec: codec })
    }
}

pub struct PlainEncodedColumn<T: 'static, C: 'static> {
    name: String,
    data: T,
    codec: C,
}

impl<T, C> Column for PlainEncodedColumn<T, C>
    where T: TypedVec<'static> + HeapSizeOf + Sync + Send + 'static,
          C: HeapSizeOf + Sync + Send + 'static,
          for<'a> &'a C: ColumnCodec<'a> {
    fn name(&self) -> &str { &self.name }
    fn len(&self) -> usize { self.data.len() }
    fn get_encoded<'b>(&'b self) -> Option<BoxedVec<'b>> { Some(self.data.ref_box()) }
    fn decode(&self) -> Option<BoxedVec> { None }
    fn codec(&self) -> Option<Codec> { Some(Arc::new(&self.codec)) }
    fn basic_type(&self) -> BasicType { (&self.codec).decoded_type() }
    fn encoding_type(&self) -> EncodingType { self.data.get_type() }
}

impl<T, C> fmt::Debug for PlainEncodedColumn<T, C>
    where T: TypedVec<'static> + HeapSizeOf + Sync + Send + 'static,
          C: HeapSizeOf + Sync + Send + 'static,
          for<'a> &'a C: ColumnCodec<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "<{:?}, {:?}>", &self.name, self.encoding_type())
    }
}

impl<T, C> HeapSizeOf for PlainEncodedColumn<T, C>
    where T: TypedVec<'static> + HeapSizeOf + Sync + Send + 'static,
          C: HeapSizeOf + Sync + Send + 'static,
          for<'a> &'a C: ColumnCodec<'a> {
    fn heap_size_of_children(&self) -> usize {
        self.name.heap_size_of_children() + self.data.heap_size_of_children() + self.codec.heap_size_of_children()
    }
}

pub struct PlainColumn<T> {
    name: String,
    data: T,
}

impl<T: TypedVec<'static> + HeapSizeOf> Column for PlainColumn<T> {
    fn name(&self) -> &str { &self.name }
    fn len(&self) -> usize { self.data.len() }
    fn get_encoded(&self) -> Option<BoxedVec> { Some(self.data.ref_box()) }
    fn decode(&self) -> Option<BoxedVec> { None }
    fn codec(&self) -> Option<Codec> { None }
    fn basic_type(&self) -> BasicType { self.data.get_type().cast_to_basic() }
    fn encoding_type(&self) -> EncodingType { self.data.get_type() }
}

impl<T: TypedVec<'static>> fmt::Debug for PlainColumn<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "<{:?}, {:?}>", &self.name, &self.data.get_type())
    }
}

impl<T: HeapSizeOf> HeapSizeOf for PlainColumn<T> {
    fn heap_size_of_children(&self) -> usize {
        self.name.heap_size_of_children() + self.data.heap_size_of_children()
    }
}

pub type Codec<'a> = Arc<ColumnCodec<'a> + 'a>;

pub trait ColumnCodec<'a>: fmt::Debug {
    fn unwrap_decode<'b>(&self, data: &TypedVec<'b>) -> BoxedVec<'b> where 'a: 'b;
    fn encoding_type(&self) -> EncodingType;
    fn decoded_type(&self) -> BasicType;
    fn is_summation_preserving(&self) -> bool;
    fn is_order_preserving(&self) -> bool;
    fn is_positive_integer(&self) -> bool;
    fn encoding_range(&self) -> Option<(i64, i64)>;

    fn encode_str(&self, _: &str) -> RawVal {
        panic!("encode_str not supported")
    }

    fn encode_int(&self, _: i64) -> RawVal {
        panic!("encode_str not supported")
    }
}

impl<'a, T> ColumnCodec<'a> for &'a T where T: ColumnCodec<'static> {
    fn unwrap_decode<'b>(&self, data: &TypedVec<'b>) -> BoxedVec<'b> where 'a: 'b { (*self).unwrap_decode(data) }
    fn encoding_type(&self) -> EncodingType { (*self).encoding_type() }
    fn decoded_type(&self) -> BasicType { (*self).decoded_type() }
    fn is_summation_preserving(&self) -> bool { (*self).is_summation_preserving() }
    fn is_order_preserving(&self) -> bool { (*self).is_order_preserving() }
    fn is_positive_integer(&self) -> bool { (*self).is_positive_integer() }
    fn encoding_range(&self) -> Option<(i64, i64)> { (*self).encoding_range() }
    fn encode_str(&self, s: &str) -> RawVal { (*self).encode_str(s) }
    fn encode_int(&self, i: i64) -> RawVal { (*self).encode_int(i) }
}