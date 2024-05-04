use ordered_float::OrderedFloat;

use crate::mem_store::*;
use std::sync::Arc;
use crate::bitvec::BitVec;

pub struct FloatColumn;

impl FloatColumn {
    pub fn new_boxed(name: &str, mut values: Vec<OrderedFloat<f64>>, null: Option<Vec<u8>>) -> Arc<Column> {
        let null = null.map(|mut n| {
            n.shrink_to_fit();
            n
        });
        values.shrink_to_fit();
        let mut column = match null {
            Some(present) => {
                // Values for null entries are arbitrary, replace them with values that give high compression
                let mut last_value = OrderedFloat(0.0);
                for (i, value) in values.iter_mut().enumerate() {
                    if !present.is_set(i) {
                        *value = last_value;
                    } else {
                        last_value = *value;
                    }
                }
                Column::new(
                    name,
                    values.len(),
                    None,
                    vec![CodecOp::PushDataSection(1), CodecOp::Nullable],
                    vec![values.into(), DataSection::Bitvec(present)],
                )
            },
            None => Column::new(
                name,
                values.len(),
                None,
                vec![],
                vec![DataSection::F64(values)],
            ),
        };
        column.lz4_or_pco_encode();
        Arc::new(column)
    }
}