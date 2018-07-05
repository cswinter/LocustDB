use std::fmt;
use std::sync::Arc;
// use std::mem;

use heapsize::HeapSizeOf;
use engine::types::*;
use engine::typed_vec::{BoxedVec, AnyVec};
use ingest::raw_val::RawVal;

pub trait Column: HeapSizeOf + fmt::Debug + Send + Sync {
    fn name(&self) -> &str;
    fn len(&self) -> usize;
    fn get_encoded(&self, from: usize, to: usize) -> Option<BoxedVec>;
    fn decode(&self) -> BoxedVec;
    fn codec(&self) -> Option<Codec>;
    fn basic_type(&self) -> BasicType;
    fn encoding_type(&self) -> EncodingType;
    fn range(&self) -> Option<(i64, i64)>;
    fn full_type(&self) -> Type {
        Type::new(self.basic_type(), self.codec())
    }
}

impl Column {
    pub fn plain<T: AnyVec<'static> + HeapSizeOf + 'static>(name: &str, data: T, range: Option<(i64, i64)>) -> Box<Column> {
        Box::new(PlainColumn { name: name.to_owned(), data, range })
    }

    pub fn encoded<T, C>(name: &str, data: T, codec: C, range: Option<(i64, i64)>) -> Box<Column>
        where T: AnyVec<'static> + HeapSizeOf + Sync + Send + 'static,
              C: HeapSizeOf + Sync + Send + 'static,
              for<'a> &'a C: ColumnCodec<'a> {
        Box::new(PlainEncodedColumn { name: name.to_owned(), data, codec, range })
    }
}

pub struct PlainEncodedColumn<T: 'static, C: 'static> {
    name: String,
    data: T,
    codec: C,
    range: Option<(i64, i64)>,
}

impl<T, C> Column for PlainEncodedColumn<T, C>
    where T: AnyVec<'static> + HeapSizeOf + Sync + Send + 'static,
          C: HeapSizeOf + Sync + Send + 'static,
          for<'a> &'a C: ColumnCodec<'a> {
    fn name(&self) -> &str { &self.name }
    fn len(&self) -> usize { self.data.len() }
    fn get_encoded<'b>(&'b self, from: usize, to: usize) -> Option<BoxedVec<'b>> { Some(self.data.slice_box(from, to)) }
    fn decode(&self) -> BoxedVec { panic!("PlainEncodedColumn{:?}.decode()", &self) }
    fn codec(&self) -> Option<Codec> { Some(Arc::new(&self.codec)) }
    fn basic_type(&self) -> BasicType { (&self.codec).decoded_type() }
    fn encoding_type(&self) -> EncodingType { self.data.get_type() }
    fn range(&self) -> Option<(i64, i64)> { self.range }
}

impl<T, C> fmt::Debug for PlainEncodedColumn<T, C>
    where T: AnyVec<'static> + HeapSizeOf + Sync + Send + 'static,
          C: HeapSizeOf + Sync + Send + 'static,
          for<'a> &'a C: ColumnCodec<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "[{}; {:?}; {:?}]", &self.name, self.encoding_type(), &self.codec)
    }
}

impl<T, C> HeapSizeOf for PlainEncodedColumn<T, C>
    where T: AnyVec<'static> + HeapSizeOf + Sync + Send + 'static,
          C: HeapSizeOf + Sync + Send + 'static,
          for<'a> &'a C: ColumnCodec<'a> {
    fn heap_size_of_children(&self) -> usize {
        self.name.heap_size_of_children() + self.data.heap_size_of_children() + self.codec.heap_size_of_children()
    }
}

pub struct PlainColumn<T> {
    name: String,
    data: T,
    range: Option<(i64, i64)>,
}

impl<T: AnyVec<'static> + HeapSizeOf> Column for PlainColumn<T> {
    fn name(&self) -> &str { &self.name }
    fn len(&self) -> usize { self.data.len() }
    fn get_encoded(&self, from: usize, to: usize) -> Option<BoxedVec> { Some(self.data.slice_box(from, to)) }
    fn decode(&self) -> BoxedVec { panic!("PlainColumn{:?}.decode()", self) }
    fn codec(&self) -> Option<Codec> { None }
    fn basic_type(&self) -> BasicType { self.data.get_type().cast_to_basic() }
    fn encoding_type(&self) -> EncodingType { self.data.get_type() }
    fn range(&self) -> Option<(i64, i64)> { self.range }
}

impl<T: AnyVec<'static>> fmt::Debug for PlainColumn<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "[{}; {:?}]", &self.name, &self.data.get_type())
    }
}

impl<T: HeapSizeOf> HeapSizeOf for PlainColumn<T> {
    fn heap_size_of_children(&self) -> usize {
        self.name.heap_size_of_children() + self.data.heap_size_of_children()
    }
}

pub type Codec<'a> = Arc<ColumnCodec<'a> + 'a>;

pub trait ColumnCodec<'a>: fmt::Debug {
    fn unwrap_decode<'b>(&self, data: &AnyVec<'b>, buffer: &mut AnyVec<'b>) where 'a: 'b;
    fn encoding_type(&self) -> EncodingType;
    fn decoded_type(&self) -> BasicType;
    fn is_summation_preserving(&self) -> bool;
    fn is_order_preserving(&self) -> bool;
    fn is_positive_integer(&self) -> bool;
    fn decode_range(&self, range: (i64, i64)) -> Option<(i64, i64)>;

    fn encode_str(&self, _: &str) -> RawVal {
        panic!("encode_str not supported")
    }

    fn encode_int(&self, _: i64) -> RawVal {
        panic!("encode_str not supported")
    }
}

impl<'a, T> ColumnCodec<'a> for &'a T where T: ColumnCodec<'static> {
    fn unwrap_decode<'b>(&self, data: &AnyVec<'b>, buffer: &mut AnyVec<'b>) where 'a: 'b {
        (*self).unwrap_decode(data, buffer)
    }
    fn encoding_type(&self) -> EncodingType { (*self).encoding_type() }
    fn decoded_type(&self) -> BasicType { (*self).decoded_type() }
    fn is_summation_preserving(&self) -> bool { (*self).is_summation_preserving() }
    fn is_order_preserving(&self) -> bool { (*self).is_order_preserving() }
    fn is_positive_integer(&self) -> bool { (*self).is_positive_integer() }
    fn decode_range(&self, range: (i64, i64)) -> Option<(i64, i64)> { (*self).decode_range(range) }
    fn encode_str(&self, s: &str) -> RawVal { (*self).encode_str(s) }
    fn encode_int(&self, i: i64) -> RawVal { (*self).encode_int(i) }
}