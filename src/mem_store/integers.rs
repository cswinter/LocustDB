use crate::engine::data_types::*;
use crate::mem_store::*;
use std::{u16, u32, u8};
use std::convert::From;
use std::sync::Arc;

pub struct IntegerColumn;

impl IntegerColumn {
    pub fn new_boxed(name: &str,
                     mut values: Vec<i64>,
                     mut min: i64,
                     mut max: i64,
                     delta_encode: bool,
                     null: Option<Vec<u8>>) -> Arc<Column> {
        let null = null.map(|mut n| {
            n.shrink_to_fit();
            n
        });
        let original_range = Some((min, max));
        let min0 = min;
        let max0 = max;
        if delta_encode && !values.is_empty() {
            let mut previous = values[0];
            max = previous;
            min = previous;
            for curr in &mut values[1..] {
                let tmp = *curr;
                *curr -= previous;
                previous = tmp;
                if max < *curr { max = *curr }
                if min > *curr { min = *curr }
            }
        }
        let interval = if min < 0 && max > 0 {
            max as u64 + (-min) as u64
        } else {
            (max - min) as u64
        };
        let mut column = if min >= 0 && max <= From::from(u8::MAX) {
            IntegerColumn::create_col::<u8>(name, values, 0, min0, max0, delta_encode, null, EncodingType::U8)
        } else if interval <= From::from(u8::MAX) {
            IntegerColumn::create_col::<u8>(name, values, min, min0, max0, delta_encode, null, EncodingType::U8)
        } else if min >= 0 && max <= From::from(u16::MAX) {
            IntegerColumn::create_col::<u16>(name, values, 0, min0, max0, delta_encode, null, EncodingType::U16)
        } else if interval <= From::from(u16::MAX) {
            IntegerColumn::create_col::<u16>(name, values, min, min0, max0, delta_encode, null, EncodingType::U16)
        } else if min >= 0 && max <= From::from(u32::MAX) {
            IntegerColumn::create_col::<u32>(name, values, 0, min0, max0, delta_encode, null, EncodingType::U32)
        } else if interval <= From::from(u32::MAX) {
            IntegerColumn::create_col::<u32>(name, values, min, min0, max0, delta_encode, null, EncodingType::U32)
        } else {
            values.shrink_to_fit();
            match null {
                Some(present) => if delta_encode {
                    Column::new(
                        name,
                        values.len(),
                        original_range,
                        vec![CodecOp::Delta(EncodingType::I64), CodecOp::PushDataSection(1), CodecOp::Nullable],
                        vec![values.into(), present.into()])
                } else {
                    Column::new(
                        name,
                        values.len(),
                        original_range,
                        vec![CodecOp::PushDataSection(1), CodecOp::Nullable],
                        vec![values.into(), present.into()])
                }
                None => if delta_encode {
                    Column::new(
                        name,
                        values.len(),
                        original_range,
                        vec![CodecOp::Delta(EncodingType::I64)],
                        vec![DataSection::I64(values)])
                } else {
                    Column::new(
                        name,
                        values.len(),
                        original_range,
                        vec![],
                        vec![DataSection::I64(values)])
                }
            }
        };
        column.lz4_encode();
        Arc::new(column)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create_col<T>(name: &str,
                         values: Vec<i64>,
                         offset: i64,
                         min: i64, max: i64,
                         delta_encode: bool,
                         null_map: Option<Vec<u8>>,
                         t: EncodingType) -> Column
        where T: GenericIntVec<T>, Vec<T>: Into<DataSection> {
        let values = IntegerColumn::encode::<T>(values, offset);
        let len = values.len();
        let codec = if null_map.is_some() {
            match (offset == 0, delta_encode) {
                (true, true) => vec![CodecOp::Delta(t), CodecOp::PushDataSection(1), CodecOp::Nullable],
                (true, false) => vec![CodecOp::PushDataSection(1), CodecOp::Nullable, CodecOp::ToI64(t)],
                (false, true) => vec![CodecOp::Add(t, offset), CodecOp::Delta(EncodingType::I64), CodecOp::PushDataSection(1), CodecOp::Nullable],
                (false, false) => vec![CodecOp::PushDataSection(1), CodecOp::Nullable, CodecOp::Add(t, offset)],
            }
        } else {
            match (offset == 0, delta_encode) {
                (true, true) => vec![CodecOp::Delta(t)],
                (true, false) => vec![CodecOp::ToI64(t)],
                (false, true) => vec![CodecOp::Add(t, offset), CodecOp::Delta(EncodingType::I64)],
                (false, false) => vec![CodecOp::Add(t, offset)],
            }
        };

        Column::new(
            name,
            len,
            Some((min - offset, max - offset)),
            codec,
            if let Some(present) = null_map { vec![values.into(), present.into()] } else { vec![values.into()] })
    }

    pub fn encode<T: GenericIntVec<T>>(values: Vec<i64>, offset: i64) -> Vec<T> {
        let mut encoded_vals = Vec::with_capacity(values.len());
        for v in values {
            let encoded_val = T::from(v - offset);
            if encoded_val.is_none() {
                unreachable!("{} {}", v, offset);
            }
            encoded_vals.push(encoded_val.unwrap());
        }
        encoded_vals
    }
}
