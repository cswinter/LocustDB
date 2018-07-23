use heapsize::HeapSizeOf;

use mem_store::*;
use engine::typed_vec::AnyVec;
use engine::types::*;

#[derive(Debug, Serialize, Deserialize)]
pub struct Column {
    name: String,
    len: usize,
    range: Option<(i64, i64)>,
    codec: Codec,
    data: Vec<DataSection>,
}

impl Column {
    pub fn new(name: &str,
               len: usize,
               range: Option<(i64, i64)>,
               mut codec: Codec,
               data: Vec<DataSection>) -> Column {
        codec.set_column_name(name);
        Column {
            name: name.to_string(),
            len,
            range,
            codec,
            data,
        }
    }

    pub fn null(name: &str, len: usize) -> Column {
        Column {
            name: name.to_string(),
            len,
            range: None,
            codec: Codec::identity(EncodingType::Null),
            data: vec![DataSection::Null(len)],
        }
    }

    pub fn name(&self) -> &str { &self.name }
    pub fn len(&self) -> usize { self.len }
    pub fn codec(&self) -> Codec { self.codec.clone() }
    pub fn basic_type(&self) -> BasicType { self.codec.decoded_type() }
    pub fn encoding_type(&self) -> EncodingType { self.codec.encoding_type() }
    pub fn range(&self) -> Option<(i64, i64)> { self.range }
    pub fn full_type(&self) -> Type {
        Type::new(self.basic_type(), Some(self.codec()))
    }
    pub fn data_sections(&self) -> Vec<&AnyVec> {
        self.data.iter().map(|d| d.to_any_vec()).collect()
    }
}

impl HeapSizeOf for Column {
    fn heap_size_of_children(&self) -> usize {
        self.name.heap_size_of_children() + self.data.heap_size_of_children()
    }
}


#[derive(Debug, Serialize, Deserialize)]
pub enum DataSection {
    U8(Vec<u8>),
    U16(Vec<u16>),
    U32(Vec<u32>),
    I64(Vec<i64>),
    // TODO(clemens): remove
    String(Vec<String>),
    Null(usize),
}

impl DataSection {
    pub fn to_any_vec(&self) -> &AnyVec {
        match self {
            DataSection::U8(ref x) => x,
            DataSection::U16(ref x) => x,
            DataSection::U32(ref x) => x,
            DataSection::I64(ref x) => x,
            DataSection::String(ref x) => x,
            DataSection::Null(ref x) => x,
        }
    }
}

impl HeapSizeOf for DataSection {
    fn heap_size_of_children(&self) -> usize {
        match self {
            DataSection::U8(ref x) => x.heap_size_of_children(),
            DataSection::U16(ref x) => x.heap_size_of_children(),
            DataSection::U32(ref x) => x.heap_size_of_children(),
            DataSection::I64(ref x) => x.heap_size_of_children(),
            DataSection::Null(_) => 0,
            DataSection::String(ref x) => x.heap_size_of_children(),
        }
    }
}

