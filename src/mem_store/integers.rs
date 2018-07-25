use std::{u16, u32, u8};
use std::convert::From;
use std::env;
use std::sync::Arc;

use engine::*;
use engine::types::*;
use mem_store::*;

pub struct IntegerColumn;

impl IntegerColumn {
    pub fn new_boxed(name: &str, mut values: Vec<i64>, min: i64, max: i64) -> Arc<Column> {
        let len = values.len();
        // TODO(clemens): remove, this was just a hack to vary memory bandwidth for benchmarks
        let min_width = env::var_os("LOCUSTDB_MIN_WIDTH")
            .map(|x| x.to_str().unwrap().parse::<u8>().unwrap()).unwrap_or(0);
        let compressed_range = Some((0, max - min));
        let original_range = Some((min, max));
        let (range, codec, data) = if min >= 0 && max <= From::from(u8::MAX) && min_width < 2 {
            (original_range,
             integer_cast_codec(EncodingType::U8),
             DataSection::U8(IntegerColumn::encode::<u8>(values, 0)))
        } else if max - min <= From::from(u8::MAX) && min_width < 2 {
            (compressed_range,
             integer_offset_codec(EncodingType::U8, min),
             DataSection::U8(IntegerColumn::encode::<u8>(values, min)))
        } else if min >= 0 && max <= From::from(u16::MAX) && min_width < 3 {
            (original_range,
             integer_cast_codec(EncodingType::U16),
             DataSection::U16(IntegerColumn::encode::<u16>(values, 0)))
        } else if max - min <= From::from(u16::MAX) && min_width < 3 {
            (compressed_range,
             integer_offset_codec(EncodingType::U16, min),
             DataSection::U16(IntegerColumn::encode::<u16>(values, min)))
        } else if min >= 0 && max <= From::from(u32::MAX) && min_width < 5 {
            (original_range,
             integer_cast_codec(EncodingType::U32),
             DataSection::U32(IntegerColumn::encode::<u32>(values, 0)))
        } else if max - min <= From::from(u32::MAX) && min_width < 5 {
            (compressed_range,
             integer_offset_codec(EncodingType::U32, min),
             DataSection::U32(IntegerColumn::encode::<u32>(values, min)))
        } else {
            values.shrink_to_fit();
            return Arc::new(Column::new(
                name,
                values.len(),
                original_range,
                Codec::identity(EncodingType::I64),
                vec![DataSection::I64(values)]));
        };
        Arc::new(Column::new(
            name,
            len,
            range,
            codec,
            vec![data]))
    }


    pub fn encode<T: GenericIntVec<T>>(values: Vec<i64>, offset: i64) -> Vec<T> {
        let mut encoded_vals = Vec::with_capacity(values.len());
        for v in values {
            encoded_vals.push(T::from(v - offset).unwrap());
        }
        encoded_vals
    }
}

pub fn integer_offset_codec(t: EncodingType, offset: i64) -> Codec {
    Codec::new(vec![CodecOp::Add(t, offset)])
}

pub fn integer_cast_codec(t: EncodingType) -> Codec {
    Codec::new(vec![CodecOp::ToI64(t)])
}

