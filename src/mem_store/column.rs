use pco::standalone::{simple_decompress, simpler_compress};
use pco::DEFAULT_COMPRESSION_LEVEL;
use std::fmt;
use std::mem;
use std::sync::Arc;

use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};

use crate::engine::data_types::*;
use crate::mem_store::*;
use crate::stringpack::StringPackerIterator;

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
    fn data_sections<'a>(&'a self) -> Vec<&'a dyn Data<'a>>;
    fn full_type(&self) -> Type;

    fn decode<'a>(&'a self) -> BoxedData<'a> {
        decode(&self.codec(), &self.data_sections())
    }
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
        Type::new(self.basic_type(), self.codec())
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

    pub fn lz4_or_pco_encode(&mut self) {
        let (lz4_encoded, lz4_ratio) = self.data[0].lz4_encode();
        let (pco_encoded, pco_ratio, is_fp32) = self.data[0].pco_encode();
        if lz4_ratio < pco_ratio && lz4_ratio < 0.9 {
            self.codec = self.codec.with_lz4(self.data[0].len());
            self.data[0] = lz4_encoded;
        } else if pco_ratio < 0.9 {
            self.codec = self.codec.with_pco(self.data[0].len(), is_fp32);
            self.data[0] = pco_encoded;
        }
    }

    pub fn lz4_or_pco_decode(&mut self) {
        if let Some(CodecOp::LZ4(decoded_type, _)) = self.codec.ops().first().copied() {
            trace!("lz4_decode before: {:?}", self);
            self.codec = self.codec.without_lz4();
            self.data[0] = self.data[0].lz4_decode(decoded_type, self.len);
            trace!("lz4_decode after: {:?}", self);
        }
        if let Some(CodecOp::Pco(decoded_type, length, ..)) = self.codec.ops().first().copied() {
            trace!("lz4_decode before: {:?}", self);
            self.codec = self.codec.without_pco();
            self.data[0] = self.data[0].pco_decode(decoded_type, length);
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
    Pco {
        decoded_bytes: usize,
        bytes_per_element: usize,
        data: Vec<u8>,
        is_fp32: bool,
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
            DataSection::Pco { data, .. } => data,
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
            DataSection::Pco { data, .. } => data.len(),
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
            DataSection::Pco { data, .. } => data.capacity(),
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
            DataSection::Pco { .. } => EncodingType::U8,
        }
    }

    pub fn lz4_encode(&self) -> (DataSection, f64) {
        let (mut encoded, bytes_per_element) = match self {
            DataSection::U8(ref x) | DataSection::Bitvec(ref x) => (lz4::encode(x), 1),
            DataSection::U16(ref x) => (lz4::encode(x), 2),
            DataSection::U32(ref x) => (lz4::encode(x), 4),
            DataSection::U64(ref x) => (lz4::encode(x), 8),
            DataSection::I64(ref x) => (lz4::encode(x), 8),
            DataSection::F64(ref x) => (lz4::encode(x), 8),
            DataSection::Null(ref x) => return (DataSection::Null(*x), 1.0),
            DataSection::LZ4 { .. } => panic!("Trying to lz4 encode lz4 data section"),
            DataSection::Pco { .. } => panic!("Trying to lz4 encode pco data section"),
        };
        encoded.shrink_to_fit();
        let ratio = encoded.len() as f64 / (self.len() * bytes_per_element) as f64;
        let encoded_data = DataSection::LZ4 {
            data: encoded,
            decoded_bytes: self.len() * bytes_per_element,
            bytes_per_element,
        };
        (encoded_data, ratio)
    }

    pub fn pco_encode(&self) -> (DataSection, f64, bool) {
        let (mut encoded, bytes_per_element, is_fp32) = match self {
            DataSection::U8(ref x) | DataSection::Bitvec(ref x) => {
                let data_u32 = x.iter().map(|&v| v as u32).collect::<Vec<u32>>();
                (
                    simpler_compress(&data_u32, DEFAULT_COMPRESSION_LEVEL).unwrap(),
                    1,
                    false,
                )
            }
            DataSection::U16(ref x) => {
                let data_u32 = x.iter().map(|&v| v as u32).collect::<Vec<u32>>();
                (
                    simpler_compress(&data_u32, DEFAULT_COMPRESSION_LEVEL).unwrap(),
                    2,
                    false,
                )
            }
            DataSection::U32(ref x) => (
                simpler_compress(x, DEFAULT_COMPRESSION_LEVEL).unwrap(),
                4,
                false,
            ),
            DataSection::U64(ref x) => (
                simpler_compress(x, DEFAULT_COMPRESSION_LEVEL).unwrap(),
                8,
                false,
            ),
            DataSection::I64(ref x) => (
                simpler_compress(x, DEFAULT_COMPRESSION_LEVEL).unwrap(),
                8,
                false,
            ),
            DataSection::F64(ref x) => {
                if x.iter().all(|f| (f.0 as f32) as f64 == f.0) {
                    (
                        simpler_compress(
                            &x.iter().map(|f| f.0 as f32).collect::<Vec<f32>>(),
                            DEFAULT_COMPRESSION_LEVEL,
                        )
                        .unwrap(),
                        8,
                        true,
                    )
                } else {
                    (
                        simpler_compress(
                            unsafe { std::mem::transmute::<&Vec<OrderedFloat<f64>>, &Vec<f64>>(x) },
                            DEFAULT_COMPRESSION_LEVEL,
                        )
                        .unwrap(),
                        8,
                        false,
                    )
                }
            }
            DataSection::Null(ref x) => return (DataSection::Null(*x), 1.0, false),
            DataSection::LZ4 { .. } => panic!("Trying to pco encode lz4 data section"),
            DataSection::Pco { .. } => panic!("Trying to pco encode pco data section"),
        };
        encoded.shrink_to_fit();
        let ratio = encoded.len() as f64 / (self.len() * bytes_per_element) as f64;
        let encoded_data = DataSection::Pco {
            data: encoded,
            decoded_bytes: self.len() * bytes_per_element,
            bytes_per_element,
            is_fp32,
        };
        (encoded_data, ratio, is_fp32)
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
                EncodingType::F64 => {
                    let mut decoded = vec![OrderedFloat(0.0); len];
                    lz4::decode::<OrderedFloat<f64>>(&mut lz4::decoder(encoded), &mut decoded);
                    DataSection::F64(decoded)
                }
                t => panic!("Unexpected type {:?} for lz4 decode", t),
            },
            DataSection::LZ4 {
                decoded_bytes,
                data,
                bytes_per_element,
            } => match decoded_type {
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
                EncodingType::F64 => {
                    let mut decoded = vec![OrderedFloat(0.0); *decoded_bytes / *bytes_per_element];
                    lz4::decode::<OrderedFloat<f64>>(&mut lz4::decoder(data), &mut decoded);
                    DataSection::F64(decoded)
                }
                t => panic!("Unexpected type {:?} for lz4 decode", t),
            },
            _ => panic!("Trying to lz4 decode non u8/non lz4 data section"),
        }
    }

    pub fn pco_decode(&self, decoded_type: EncodingType, length: usize) -> DataSection {
        match self {
            DataSection::Pco { data, is_fp32, .. } => match decoded_type {
                EncodingType::U8 => DataSection::U8(
                    simple_decompress::<u32>(data)
                        .unwrap()
                        .into_iter()
                        .map(|v| v as u8)
                        .collect(),
                ),
                EncodingType::U16 => DataSection::U16(
                    simple_decompress::<u32>(data)
                        .unwrap()
                        .into_iter()
                        .map(|v| v as u16)
                        .collect(),
                ),
                EncodingType::U32 => DataSection::U32(simple_decompress(data).unwrap()),
                EncodingType::U64 => DataSection::U64(simple_decompress(data).unwrap()),
                EncodingType::I64 => DataSection::I64(simple_decompress(data).unwrap()),
                EncodingType::F64 if *is_fp32 => match simple_decompress::<f32>(data) {
                    Ok(decompressed) => DataSection::F64(
                        decompressed
                            .into_iter()
                            .map(|v| OrderedFloat(v as f64))
                            .collect(),
                    ),
                    Err(e) => {
                        log::error!("Error decompressing PCO f32 data section: {:?}", e);
                        log::error!("PCO data section (hex): {:02x?}", data);
                        DataSection::F64(vec![OrderedFloat(0.0); length])
                    }
                },
                EncodingType::F64 if !is_fp32 => DataSection::F64(unsafe {
                    std::mem::transmute::<Vec<f64>, Vec<of64>>(
                        simple_decompress::<f64>(data).unwrap(),
                    )
                }),
                t => panic!("Unexpected type {:?} for pco decode", t),
            },
            _ => panic!("Trying to pco decode non pco data section"),
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
                DataSection::Pco { data, .. } => data.shrink_to_fit(),
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
            DataSection::Pco { data, .. } => data.capacity() * mem::size_of::<u8>(),
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

fn decode<'a>(codec: &Codec, sections: &[&'a dyn Data<'a>]) -> BoxedData<'a> {
    let mut section_stack: Vec<BoxedData<'a>> = vec![sections[0].slice_box(0, sections[0].len())];
    for codec_op in codec.ops() {
        let arg0 = section_stack.first().unwrap();
        let decoded = match codec_op {
            CodecOp::Nullable => {
                let present = section_stack.pop().unwrap();
                let mut data = section_stack.pop().unwrap();
                data.make_nullable(present.cast_ref_u8())
            }
            CodecOp::Add(encoding_type, value) => match encoding_type {
                EncodingType::U8 => Box::new(
                    arg0.cast_ref_u8()
                        .iter()
                        .map(|&v| v as i64 + value)
                        .collect::<Vec<_>>(),
                ),
                EncodingType::U16 => Box::new(
                    arg0.cast_ref_u16()
                        .iter()
                        .map(|&v| v as i64 + value)
                        .collect::<Vec<_>>(),
                ),
                EncodingType::U32 => Box::new(
                    arg0.cast_ref_u32()
                        .iter()
                        .map(|&v| v as i64 + value)
                        .collect::<Vec<_>>(),
                ),
                _ => panic!(
                    "Unsupported encoding type for CodecOp::Add: {:?}",
                    encoding_type
                ),
            },
            CodecOp::Delta(encoding_type) => match encoding_type {
                EncodingType::U8 => {
                    let mut decoded = Vec::with_capacity(arg0.len());
                    let deltas = arg0.cast_ref_u8();
                    let mut current = 0;
                    for delta in deltas {
                        current += *delta as i64;
                        decoded.push(current);
                    }
                    Box::new(decoded)
                }
                EncodingType::U16 => {
                    let mut decoded = Vec::with_capacity(arg0.len());
                    let deltas = arg0.cast_ref_u16();
                    let mut current = 0;
                    for delta in deltas {
                        current += *delta as i64;
                        decoded.push(current);
                    }
                    Box::new(decoded)
                }
                EncodingType::U32 => {
                    let mut decoded = Vec::with_capacity(arg0.len());
                    let deltas = arg0.cast_ref_u32();
                    let mut current = 0;
                    for delta in deltas {
                        current += *delta as i64;
                        decoded.push(current);
                    }
                    Box::new(decoded)
                }
                EncodingType::I64 => {
                    let mut decoded = Vec::with_capacity(arg0.len());
                    let deltas = arg0.cast_ref_i64();
                    let mut current = 0;
                    for delta in deltas {
                        current += *delta;
                        decoded.push(current);
                    }
                    Box::new(decoded)
                }
                _ => panic!(
                    "Unsupported encoding type for CodecOp::Delta: {:?}",
                    encoding_type
                ),
            },
            CodecOp::ToI64(encoding_type) => match encoding_type {
                EncodingType::U8 => Box::new(
                    arg0.cast_ref_u8()
                        .iter()
                        .map(|&v| v as i64)
                        .collect::<Vec<_>>(),
                ),
                EncodingType::U16 => Box::new(
                    arg0.cast_ref_u16()
                        .iter()
                        .map(|&v| v as i64)
                        .collect::<Vec<_>>(),
                ),
                EncodingType::U32 => Box::new(
                    arg0.cast_ref_u32()
                        .iter()
                        .map(|&v| v as i64)
                        .collect::<Vec<_>>(),
                ),
                _ => panic!(
                    "Unsupported encoding type for CodecOp::ToI64: {:?}",
                    encoding_type
                ),
            },
            CodecOp::PushDataSection(index) => {
                let data_section = sections[*index].slice_box(0, sections[*index].len());
                section_stack.push(data_section);
                continue;
            }
            CodecOp::DictLookup(encoding_type) => {
                let dict_data = section_stack.pop().unwrap();
                // TODO: make lifetimes work out
                let dict_data =
                    unsafe { std::mem::transmute::<&[u8], &[u8]>(dict_data.cast_ref_u8()) };
                let string_ranges = section_stack.pop().unwrap();
                let string_ranges = string_ranges.cast_ref_u64();
                let indices: Vec<usize> = match encoding_type {
                    EncodingType::U8 => section_stack
                        .pop()
                        .unwrap()
                        .cast_ref_u8()
                        .iter()
                        .map(|i| *i as usize)
                        .collect(),
                    EncodingType::U16 => section_stack
                        .pop()
                        .unwrap()
                        .cast_ref_u16()
                        .iter()
                        .map(|i| *i as usize)
                        .collect(),
                    EncodingType::U32 => section_stack
                        .pop()
                        .unwrap()
                        .cast_ref_u32()
                        .iter()
                        .map(|i| *i as usize)
                        .collect(),
                    EncodingType::I64 => section_stack
                        .pop()
                        .unwrap()
                        .cast_ref_i64()
                        .iter()
                        .map(|i| *i as usize)
                        .collect(),
                    _ => panic!(
                        "Unexpected encoding type for CodecOp::DictLookup: {:?}",
                        encoding_type
                    ),
                };
                let mut output = Vec::new();
                for i in indices.iter() {
                    let offset_len = string_ranges[*i];
                    let offset = (offset_len >> 24) as usize;
                    let len = (offset_len & 0x00ff_ffff) as usize;
                    let string =
                        unsafe { str::from_utf8_unchecked(&dict_data[offset..(offset + len)]) };
                    output.push(string);
                }
                Box::new(output) as BoxedData
            }
            CodecOp::LZ4(encoding_type, count) => match encoding_type {
                EncodingType::U8 => {
                    let mut decoded = vec![0; *count];
                    lz4::decode::<u8>(&mut lz4::decoder(arg0.cast_ref_u8()), &mut decoded);
                    Box::new(decoded) as BoxedData
                }
                EncodingType::I64 => {
                    let mut decoded = vec![0; *count];
                    lz4::decode::<i64>(&mut lz4::decoder(arg0.cast_ref_u8()), &mut decoded);
                    Box::new(decoded)
                }
                EncodingType::F64 => {
                    let mut decoded = vec![0.0; *count];
                    lz4::decode::<f64>(&mut lz4::decoder(arg0.cast_ref_u8()), &mut decoded);
                    Box::new(vec_f64_to_vec_of64(decoded))
                }
                other => panic!("Unsupported encoding type for CodecOp::LZ4: {:?}", other),
            },
            CodecOp::Pco(encoding_type, _, is_fp32) => {
                let encoded_data = arg0.cast_ref_u8();
                match encoding_type {
                    EncodingType::U8 => {
                        let decoded = simple_decompress::<u32>(encoded_data).unwrap();
                        Box::new(decoded.iter().map(|&x| x as u8).collect::<Vec<_>>()) as BoxedData
                    }
                    EncodingType::U16 => {
                        let decoded = simple_decompress::<u32>(encoded_data).unwrap();
                        Box::new(decoded.iter().map(|&x| x as u16).collect::<Vec<_>>())
                    }
                    EncodingType::U32 => {
                        let decoded = simple_decompress::<u32>(encoded_data).unwrap();
                        Box::new(decoded)
                    }
                    EncodingType::U64 => {
                        let decoded = simple_decompress::<u64>(encoded_data).unwrap();
                        Box::new(decoded)
                    }
                    EncodingType::I64 => {
                        let decoded = simple_decompress::<i64>(encoded_data).unwrap();
                        Box::new(decoded)
                    }
                    EncodingType::F64 => {
                        if *is_fp32 {
                            let decoded = simple_decompress::<f32>(encoded_data).unwrap();
                            Box::new(
                                decoded
                                    .iter()
                                    .map(|&v| OrderedFloat(v as f64))
                                    .collect::<Vec<_>>(),
                            )
                        } else {
                            let decoded = simple_decompress::<f64>(encoded_data).unwrap();
                            Box::new(decoded.iter().map(|&v| OrderedFloat(v)).collect::<Vec<_>>())
                        }
                    }
                    encoding_type => panic!(
                        "Unsupported encoding type for CodecOp::Pco: {:?}",
                        encoding_type
                    ),
                }
            }
            CodecOp::UnpackStrings => {
                let mut output = Vec::new();
                let packed: &'a [u8] = sections[0].cast_ref_u8();
                let iterator = unsafe { StringPackerIterator::from_slice(packed) };
                for str in iterator {
                    output.push(str);
                }
                Box::new(output) as BoxedData
            }
            CodecOp::UnhexpackStrings(_, _) => todo!(),
            CodecOp::Unknown => todo!(),
        };
        section_stack.pop();
        section_stack.push(decoded);
    }

    section_stack.pop().unwrap()
}
