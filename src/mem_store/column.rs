use std::fmt;
use std::mem;
use std::sync::Arc;

use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};

use crate::engine::data_types::*;
use crate::mem_store::*;

#[derive(Serialize, Deserialize)]
pub struct Column {
    name: String,
    len: usize,
    range: Option<(i64, i64)>,
    codec: Codec,
    data: Vec<DataSection>,
}

pub trait DataSource: fmt::Debug + Sync + Send {
    fn encoding_type(&self) -> EncodingType;
    fn range(&self) -> Option<(i64, i64)>;
    fn codec(&self) -> Codec;
    fn len(&self) -> usize;
    fn data_sections(&self) -> Vec<&dyn Data>;
    fn full_type(&self) -> Type;
}

impl<T: DataSource> DataSource for Arc<T> {
    fn encoding_type(&self) -> EncodingType {
        (**self).encoding_type()
    }
    fn range(&self) -> Option<(i64, i64)> {
        (**self).range()
    }
    fn codec(&self) -> Codec {
        (**self).codec()
    }
    fn len(&self) -> usize {
        (**self).len()
    }
    fn data_sections(&self) -> Vec<&dyn Data> {
        (**self).data_sections()
    }
    fn full_type(&self) -> Type {
        (**self).full_type()
    }
}

impl DataSource for Column {
    fn encoding_type(&self) -> EncodingType {
        self.codec.encoding_type()
    }
    fn range(&self) -> Option<(i64, i64)> {
        self.range
    }
    fn codec(&self) -> Codec {
        self.codec.clone()
    }
    fn len(&self) -> usize {
        self.len
    }
    fn data_sections(&self) -> Vec<&dyn Data> {
        // TODO(#96): fix unsafety
        unsafe {
            mem::transmute::<Vec<&dyn Data>, Vec<&dyn Data>>(
                self.data.iter().map(|d| d.to_any_vec()).collect(),
            )
        }
    }
    fn full_type(&self) -> Type {
        Type::new(self.basic_type(), Some(self.codec()))
    }
}

impl Column {
    pub fn new(
        name: &str,
        len: usize,
        range: Option<(i64, i64)>,
        codec: Vec<CodecOp>,
        data: Vec<DataSection>,
    ) -> Column {
        let mut codec = if codec.is_empty() {
            Codec::identity(data[0].encoding_type().cast_to_basic())
        } else {
            Codec::new(codec, data.iter().map(DataSection::encoding_type).collect())
        };
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
        let (encoded, worth_it) = self.data[0].lz4_encode();
        if worth_it {
            self.codec = self.codec.with_lz4(self.data[0].len());
            self.data[0] = encoded;
        }
    }

    pub fn lz4_decode(&mut self) {
        if let Some(CodecOp::LZ4(decoded_type, _)) = self.codec.ops().first().copied() {
            trace!("lz4_decode before: {:?}", self);
            self.codec = self.codec.without_lz4();
            self.data[0] = self.data[0].lz4_decode(decoded_type, self.len);
            trace!("lz4_decode after: {:?}", self);
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn data(&self) -> &[DataSection] {
        &self.data
    }
    pub fn basic_type(&self) -> BasicType {
        self.codec.decoded_type()
    }
    pub fn section_encoding_type(&self, section: usize) -> EncodingType {
        self.data[section].encoding_type()
    }

    pub fn heap_size_of_children(&self) -> usize {
        self.data
            .iter()
            .map(|section| section.heap_size_of_children())
            .sum()
    }

    pub fn mem_tree(&self, tree: &mut MemTreeColumn, depth: usize) {
        if depth == 0 {
            return;
        }
        let size_bytes = self.heap_size_of_children();
        tree.size_bytes += size_bytes;
        tree.rows += self.len;
        if depth > 1 {
            let signature = self.codec().signature(false);
            let codec_tree = tree.encodings.entry(signature.clone()).or_default();
            codec_tree.codec = signature;
            codec_tree.size_bytes += size_bytes;
            codec_tree.rows += self.len;
            if depth > 2 {
                for (i, d) in self.data.iter().enumerate() {
                    if codec_tree.sections.len() == i {
                        codec_tree.sections.push(MemTreeSection {
                            id: i,
                            size_bytes: 0,
                            datatype: format!("{:?}", d.encoding_type()),
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

impl fmt::Debug for Column {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "name={}, len={}, minmax={:?}, codec={:#}, codec.section_types={:?}, sections(type,len)={:?}",
               &self.name,
               self.len(),
               self.range,
               self.codec.signature(true),
            self.codec.section_types(),
               self.data.iter().map(|d| (d.encoding_type(), d.len())).collect::<Vec<_>>())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum DataSection {
    U8(Vec<u8>),
    U16(Vec<u16>),
    U32(Vec<u32>),
    U64(Vec<u64>),
    I64(Vec<i64>),
    F64(Vec<OrderedFloat<f64>>),
    Null(usize),
    Bitvec(Vec<u8>),
    LZ4 {
        decoded_bytes: usize,
        bytes_per_element: usize,
        data: Vec<u8>,
    },
}

impl DataSection {
    pub fn to_any_vec(&self) -> &dyn Data {
        match self {
            DataSection::U8(ref x) => x,
            DataSection::U16(ref x) => x,
            DataSection::U32(ref x) => x,
            DataSection::U64(ref x) => x,
            DataSection::I64(ref x) => x,
            DataSection::F64(ref x) => x,
            DataSection::Null(ref x) => x,
            DataSection::Bitvec(ref x) => x,
            DataSection::LZ4 { data, .. } => data,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            DataSection::U8(ref x) => x.len(),
            DataSection::U16(ref x) => x.len(),
            DataSection::U32(ref x) => x.len(),
            DataSection::U64(ref x) => x.len(),
            DataSection::I64(ref x) => x.len(),
            DataSection::F64(ref x) => x.len(),
            DataSection::Null(ref x) => *x,
            DataSection::Bitvec(ref x) => x.len(),
            DataSection::LZ4 { data, .. } => data.len(),
        }
    }

    pub fn capacity(&self) -> usize {
        match self {
            DataSection::U8(ref x) => x.capacity(),
            DataSection::U16(ref x) => x.capacity(),
            DataSection::U32(ref x) => x.capacity(),
            DataSection::U64(ref x) => x.capacity(),
            DataSection::I64(ref x) => x.capacity(),
            DataSection::F64(ref x) => x.capacity(),
            DataSection::Null(ref x) => *x,
            DataSection::Bitvec(ref x) => x.capacity(),
            DataSection::LZ4 { data, .. } => data.capacity(),
        }
    }

    pub fn encoding_type(&self) -> EncodingType {
        match self {
            DataSection::U8(_) => EncodingType::U8,
            DataSection::U16(_) => EncodingType::U16,
            DataSection::U32(_) => EncodingType::U32,
            DataSection::U64(_) => EncodingType::U64,
            DataSection::I64(_) => EncodingType::I64,
            DataSection::F64(_) => EncodingType::F64,
            DataSection::Null(_) => EncodingType::Null,
            DataSection::Bitvec(_) => EncodingType::Bitvec,
            DataSection::LZ4 { .. } => EncodingType::U8,
        }
    }

    pub fn lz4_encode(&self) -> (DataSection, bool) {
        let min_reduction = 90;
        match self {
            DataSection::U8(ref x) | DataSection::Bitvec(ref x) => {
                let mut encoded = lz4::encode(x);
                encoded.shrink_to_fit();
                let len = encoded.len();
                (
                    DataSection::LZ4 {
                        data: encoded,
                        decoded_bytes: x.len(),
                        bytes_per_element: 1,
                    },
                    len * 100 < x.len() * min_reduction,
                )
            }
            DataSection::U16(ref x) => {
                let mut encoded = lz4::encode(x);
                encoded.shrink_to_fit();
                let len = encoded.len();
                (
                    DataSection::LZ4 {
                        data: encoded,
                        decoded_bytes: x.len() * 2,
                        bytes_per_element: 2,
                    },
                    len * 100 < x.len() * 2 * min_reduction,
                )
            }
            DataSection::U32(ref x) => {
                let mut encoded = lz4::encode(x);
                encoded.shrink_to_fit();
                let len = encoded.len();
                (
                    DataSection::LZ4 {
                        data: encoded,
                        decoded_bytes: x.len() * 4,
                        bytes_per_element: 4,
                    },
                    len * 100 < x.len() * 4 * min_reduction,
                )
            }
            DataSection::U64(ref x) => {
                let mut encoded = lz4::encode(x);
                encoded.shrink_to_fit();
                let len = encoded.len();
                (
                    DataSection::LZ4 {
                        data: encoded,
                        decoded_bytes: x.len() * 8,
                        bytes_per_element: 8,
                    },
                    len * 100 < x.len() * 8 * min_reduction,
                )
            }
            DataSection::I64(ref x) => {
                let mut encoded = lz4::encode(x);
                encoded.shrink_to_fit();
                let len = encoded.len();
                (
                    DataSection::LZ4 {
                        data: encoded,
                        decoded_bytes: x.len() * 8,
                        bytes_per_element: 8,
                    },
                    len * 100 < x.len() * 8 * min_reduction,
                )
            }
            DataSection::F64(ref x) => {
                let mut encoded = lz4::encode(x);
                encoded.shrink_to_fit();
                let len = encoded.len();
                (
                    DataSection::LZ4 {
                        data: encoded,
                        decoded_bytes: x.len() * 8,
                        bytes_per_element: 8,
                    },
                    len * 100 < x.len() * 8 * min_reduction,
                )
            }
            DataSection::Null(ref x) => (DataSection::Null(*x), false),
            DataSection::LZ4 { .. } => panic!("Trying to lz4 encode lz4 data section"),
        }
    }

    pub fn lz4_decode(&self, decoded_type: EncodingType, len: usize) -> DataSection {
        match self {
            // This code can be removed, only for backwards compatibility with small region of commits (all LZ4 encoded data sections use LZ4 variant now)
            DataSection::U8(encoded) => match decoded_type {
                EncodingType::U8 => {
                    let mut decoded = vec![];
                    let mut decoder = lz4::decoder(encoded);
                    std::io::copy(&mut decoder, &mut decoded).unwrap();
                    DataSection::U8(decoded)
                }
                EncodingType::U16 => {
                    let mut decoded = vec![0; len];
                    lz4::decode::<u16>(&mut lz4::decoder(encoded), &mut decoded);
                    DataSection::U16(decoded)
                }
                EncodingType::U32 => {
                    let mut decoded = vec![0; len];
                    lz4::decode::<u32>(&mut lz4::decoder(encoded), &mut decoded);
                    DataSection::U32(decoded)
                }
                EncodingType::U64 => {
                    let mut decoded = vec![0; len];
                    lz4::decode::<u64>(&mut lz4::decoder(encoded), &mut decoded);
                    DataSection::U64(decoded)
                }
                EncodingType::I64 => {
                    let mut decoded = vec![0; len];
                    lz4::decode::<i64>(&mut lz4::decoder(encoded), &mut decoded);
                    DataSection::I64(decoded)
                }
                t => panic!("Unexpected type {:?} for lz4 decode", t),
            },
            DataSection::LZ4 {
                decoded_bytes,
                data,
                bytes_per_element,
            } => {
                match decoded_type {
                    EncodingType::U8 => {
                        let mut decoded = vec![0; *decoded_bytes / *bytes_per_element];
                        lz4::decode::<u8>(&mut lz4::decoder(data), &mut decoded);
                        DataSection::U8(decoded)
                    }
                    EncodingType::U16 => {
                        let mut decoded = vec![0; *decoded_bytes / *bytes_per_element];
                        lz4::decode::<u16>(&mut lz4::decoder(data), &mut decoded);
                        DataSection::U16(decoded)
                    }
                    EncodingType::U32 => {
                        let mut decoded = vec![0; *decoded_bytes / *bytes_per_element];
                        lz4::decode::<u32>(&mut lz4::decoder(data), &mut decoded);
                        DataSection::U32(decoded)
                    }
                    EncodingType::U64 => {
                        let mut decoded = vec![0; *decoded_bytes / *bytes_per_element];
                        lz4::decode::<u64>(&mut lz4::decoder(data), &mut decoded);
                        DataSection::U64(decoded)
                    }
                    EncodingType::I64 => {
                        let mut decoded = vec![0; *decoded_bytes / *bytes_per_element];
                        lz4::decode::<i64>(&mut lz4::decoder(data), &mut decoded);
                        DataSection::I64(decoded)
                    }
                    t => panic!("Unexpected type {:?} for lz4 decode", t),
                }
            }
            _ => panic!("Trying to lz4 encode non u8/non lz4 data section"),
        }
    }

    pub fn shrink_to_fit_ish(&mut self) {
        if self.capacity() / 10 > self.len() / 9 {
            match self {
                DataSection::U8(ref mut x) | DataSection::Bitvec(ref mut x) => x.shrink_to_fit(),
                DataSection::U16(ref mut x) => x.shrink_to_fit(),
                DataSection::U32(ref mut x) => x.shrink_to_fit(),
                DataSection::U64(ref mut x) => x.shrink_to_fit(),
                DataSection::I64(ref mut x) => x.shrink_to_fit(),
                DataSection::F64(ref mut x) => x.shrink_to_fit(),
                DataSection::Null(_) => {}
                DataSection::LZ4 { data, .. } => data.shrink_to_fit(),
            }
        }
    }

    pub fn heap_size_of_children(&self) -> usize {
        match self {
            DataSection::U8(ref x) | DataSection::Bitvec(ref x) => {
                x.capacity() * mem::size_of::<u8>()
            }
            DataSection::U16(ref x) => x.capacity() * mem::size_of::<u16>(),
            DataSection::U32(ref x) => x.capacity() * mem::size_of::<u32>(),
            DataSection::U64(ref x) => x.capacity() * mem::size_of::<u64>(),
            DataSection::I64(ref x) => x.capacity() * mem::size_of::<i64>(),
            DataSection::F64(ref x) => x.capacity() * mem::size_of::<OrderedFloat<f64>>(),
            DataSection::Null(_) => 0,
            DataSection::LZ4 { data, .. } => data.capacity() * mem::size_of::<u8>(),
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

impl From<Vec<OrderedFloat<f64>>> for DataSection {
    fn from(vec: Vec<OrderedFloat<f64>>) -> Self {
        assert_eq!(vec.len(), vec.capacity());
        DataSection::F64(vec)
    }
}
