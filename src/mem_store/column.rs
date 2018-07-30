use mem_store::*;
use engine::typed_vec::AnyVec;
use engine::types::*;
use mem_store::lz4;

use heapsize::HeapSizeOf;


#[derive(Debug, Serialize, Deserialize, HeapSizeOf)]
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
            codec: Codec::identity(BasicType::Null),
            data: vec![DataSection::Null(len)],
        }
    }

    pub fn lz4_encode(&mut self) {
        if cfg!(feature = "enable_lz4") {
            let (encoded, worth_it) = self.data[0].lz4_encode();
            if worth_it {
                self.codec = self.codec.with_lz4(self.data[0].len());
                self.data[0] = encoded;
            }
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

    pub fn mem_tree(&self, tree: &mut MemTreeColumn, depth: usize) {
        if depth == 0 { return; }
        let size_bytes = self.heap_size_of_children();
        tree.size_bytes += size_bytes;
        tree.rows += self.len;
        if depth > 1 {
            let signature = self.codec().signature().to_string();
            let codec_tree = tree.encodings
                .entry(signature.clone())
                .or_insert(MemTreeEncoding::default());
            codec_tree.codec = signature;
            codec_tree.size_bytes += size_bytes;
            codec_tree.rows += self.len;
            if depth > 2 && self.data.len() > 1 {
                for (i, d) in self.data.iter().enumerate() {
                    if codec_tree.sections.len() == i {
                        codec_tree.sections.push(MemTreeSection {
                            id: i,
                            size_bytes: 0,
                        });
                    }
                    codec_tree.sections[i].size_bytes += d.heap_size_of_children();
                }
            }
        }
    }

    pub fn shrink_to_fit_ish(&mut self) {
        for d in &mut self.data {
            d.shrink_to_fit_ish();
        }
    }
}


#[derive(Debug, Serialize, Deserialize)]
pub enum DataSection {
    U8(Vec<u8>),
    U16(Vec<u16>),
    U32(Vec<u32>),
    U64(Vec<u64>),
    I64(Vec<i64>),
    Null(usize),
}

impl DataSection {
    pub fn to_any_vec(&self) -> &AnyVec {
        match self {
            DataSection::U8(ref x) => x,
            DataSection::U16(ref x) => x,
            DataSection::U32(ref x) => x,
            DataSection::U64(ref x) => x,
            DataSection::I64(ref x) => x,
            DataSection::Null(ref x) => x,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            DataSection::U8(ref x) => x.len(),
            DataSection::U16(ref x) => x.len(),
            DataSection::U32(ref x) => x.len(),
            DataSection::U64(ref x) => x.len(),
            DataSection::I64(ref x) => x.len(),
            DataSection::Null(ref x) => *x,
        }
    }

    pub fn capacity(&self) -> usize {
        match self {
            DataSection::U8(ref x) => x.capacity(),
            DataSection::U16(ref x) => x.capacity(),
            DataSection::U32(ref x) => x.capacity(),
            DataSection::U64(ref x) => x.capacity(),
            DataSection::I64(ref x) => x.capacity(),
            DataSection::Null(ref x) => *x,
        }
    }

    pub fn lz4_encode(&self) -> (DataSection, bool) {
        let min_reduction = 90;
        match self {
            DataSection::U8(ref x) => {
                let mut encoded = unsafe { lz4::encode(&x) };
                encoded.shrink_to_fit();
                let len = encoded.len();
                (DataSection::U8(encoded), len * 100 < x.len() * min_reduction)
            }
            DataSection::U16(ref x) => {
                let mut encoded = unsafe { lz4::encode(&x) };
                encoded.shrink_to_fit();
                let len = encoded.len();
                (DataSection::U8(encoded), len * 100 < x.len() * 2 * min_reduction)
            }
            DataSection::U32(ref x) => {
                let mut encoded = unsafe { lz4::encode(&x) };
                encoded.shrink_to_fit();
                let len = encoded.len();
                (DataSection::U8(encoded), len * 100 < x.len() * 4 * min_reduction)
            }
            DataSection::U64(ref x) => {
                let mut encoded = unsafe { lz4::encode(&x) };
                encoded.shrink_to_fit();
                let len = encoded.len();
                (DataSection::U8(encoded), len * 100 < x.len() * 8 * min_reduction)
            }
            DataSection::I64(ref x) => {
                let mut encoded = unsafe { lz4::encode(&x) };
                encoded.shrink_to_fit();
                let len = encoded.len();
                (DataSection::U8(encoded), len * 100 < x.len() * 8 * min_reduction)
            }
            DataSection::Null(ref x) => (DataSection::Null(*x), false)
        }
    }

    pub fn shrink_to_fit_ish(&mut self) {
        if self.capacity() / 10 > self.len() / 9 {
            match self {
                DataSection::U8(ref mut x) => x.shrink_to_fit(),
                DataSection::U16(ref mut x) => x.shrink_to_fit(),
                DataSection::U32(ref mut x) => x.shrink_to_fit(),
                DataSection::U64(ref mut x) => x.shrink_to_fit(),
                DataSection::I64(ref mut x) => x.shrink_to_fit(),
                DataSection::Null(_) => {}
            }
        }
    }
}

impl HeapSizeOf for DataSection {
    fn heap_size_of_children(&self) -> usize {
        match self {
            DataSection::U8(ref x) => x.heap_size_of_children(),
            DataSection::U16(ref x) => x.heap_size_of_children(),
            DataSection::U32(ref x) => x.heap_size_of_children(),
            DataSection::U64(ref x) => x.heap_size_of_children(),
            DataSection::I64(ref x) => x.heap_size_of_children(),
            DataSection::Null(_) => 0,
        }
    }
}

impl From<Vec<u8>> for DataSection {
    fn from(vec: Vec<u8>) -> Self {
        assert_eq!(vec.len(), vec.capacity());
        DataSection::U8(vec)
    }
}

impl From<Vec<u16>> for DataSection {
    fn from(vec: Vec<u16>) -> Self {
        assert_eq!(vec.len(), vec.capacity());
        DataSection::U16(vec)
    }
}

impl From<Vec<u32>> for DataSection {
    fn from(vec: Vec<u32>) -> Self {
        assert_eq!(vec.len(), vec.capacity());
        DataSection::U32(vec)
    }
}

impl From<Vec<u64>> for DataSection {
    fn from(vec: Vec<u64>) -> Self {
        assert_eq!(vec.len(), vec.capacity());
        DataSection::U64(vec)
    }
}

impl From<Vec<i64>> for DataSection {
    fn from(vec: Vec<i64>) -> Self {
        assert_eq!(vec.len(), vec.capacity());
        DataSection::I64(vec)
    }
}

