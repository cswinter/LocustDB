use ordered_float::OrderedFloat;

use crate::mem_store::*;
use std::sync::Arc;

pub struct FloatColumn;

impl FloatColumn {
    pub fn new_boxed(name: &str, mut values: Vec<OrderedFloat<f64>>, null: Option<Vec<u8>>) -> Arc<Column> {
        let null = null.map(|mut n| {
            n.shrink_to_fit();
            n
        });
        values.shrink_to_fit();
        let mut column = match null {
            Some(present) => Column::new(
                name,
                values.len(),
                None,
                vec![CodecOp::PushDataSection(1), CodecOp::Nullable],
                vec![values.into(), DataSection::Bitvec(present)],
            ),
            None => Column::new(
                name,
                values.len(),
                None,
                vec![],
                vec![DataSection::F64(values)],
            ),
        };
        column.lz4_encode();
        Arc::new(column)
    }
}