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
        // TODO(clemens): remove, this was just a hack to vary memory bandwidth for benchmarks
        let min_width = env::var_os("LOCUSTDB_MIN_WIDTH")
            .map(|x| x.to_str().unwrap().parse::<u8>().unwrap()).unwrap_or(0);
        let original_range = Some((min, max));
        if min >= 0 && max <= From::from(u8::MAX) && min_width < 2 {
            IntegerColumn::create_col::<u8>(name, values, 0, min, max, EncodingType::U8)
        } else if max - min <= From::from(u8::MAX) && min_width < 2 {
            IntegerColumn::create_col::<u8>(name, values, min, min, max, EncodingType::U8)
        } else if min >= 0 && max <= From::from(u16::MAX) && min_width < 3 {
            IntegerColumn::create_col::<u16>(name, values, 0, min, max, EncodingType::U16)
        } else if max - min <= From::from(u16::MAX) && min_width < 3 {
            IntegerColumn::create_col::<u16>(name, values, min, min, max, EncodingType::U16)
        } else if min >= 0 && max <= From::from(u32::MAX) && min_width < 5 {
            IntegerColumn::create_col::<u32>(name, values, 0, min, max, EncodingType::U32)
        } else if max - min <= From::from(u32::MAX) && min_width < 5 {
            IntegerColumn::create_col::<u32>(name, values, min, min, max, EncodingType::U32)
        } else {
            values.shrink_to_fit();
            if cfg!(feature = "enable_lz4") {
                Arc::new(Column::new(
                    name,
                    values.len(),
                    original_range,
                    Codec::lz4(EncodingType::I64),
                    vec![DataSection::U8(unsafe { lz4::encode(&values) })]))
            } else {
                Arc::new(Column::new(
                    name,
                    values.len(),
                    original_range,
                    Codec::identity(BasicType::Integer),
                    vec![DataSection::I64(values)]))
            }
        }
    }

    pub fn create_col<T>(name: &str, values: Vec<i64>, offset: i64, min: i64, max: i64, t: EncodingType) -> Arc<Column>
        where T: GenericIntVec<T>, Vec<T>: Into<DataSection> {
        let values = IntegerColumn::encode::<T>(values, offset);
        let len = values.len();
        let codec = if offset == 0 {
            Codec::integer_cast(t)
        } else {
            Codec::integer_offset(t, offset)
        };

        #[cfg(feature = "enable_lz4")]
        let values = unsafe { lz4::encode(&values) };
        #[cfg(feature = "enable_lz4")]
        let codec = codec.with_lz4();

        Arc::new(Column::new(
            name,
            len,
            Some((min - offset, max - offset)),
            codec,
            vec![values.into()]))
    }

    pub fn encode<T: GenericIntVec<T>>(values: Vec<i64>, offset: i64) -> Vec<T> {
        let mut encoded_vals = Vec::with_capacity(values.len());
        for v in values {
            encoded_vals.push(T::from(v - offset).unwrap());
        }
        encoded_vals
    }
}

