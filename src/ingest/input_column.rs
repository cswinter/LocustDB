use locustdb_serialization::event_buffer::ColumnData;

use crate::Value;

pub enum InputColumn {
    Int(Vec<i64>),
    Float(Vec<f64>),
    NullableFloat(u64, Vec<(u64, f64)>),
    NullableInt(u64, Vec<(u64, i64)>),
    Str(Vec<String>),
    Null(usize),
    Mixed(Vec<Value>),
}

impl InputColumn {
    pub fn from_column_data(column_data: ColumnData, rows: u64) -> Self {
        match column_data {
            ColumnData::Dense(data) => {
                if (data.len() as u64) < rows {
                    InputColumn::NullableFloat(
                        rows,
                        data.into_iter()
                            .enumerate()
                            .map(|(i, v)| (i as u64, v))
                            .collect(),
                    )
                } else {
                    InputColumn::Float(data)
                }
            }
            ColumnData::Sparse(data) => InputColumn::NullableFloat(rows, data),
            ColumnData::I64(data) => {
                if (data.len() as u64) < rows {
                    InputColumn::NullableInt(
                        rows,
                        data.into_iter()
                            .enumerate()
                            .map(|(i, v)| (i as u64, v))
                            .collect(),
                    )
                } else {
                    InputColumn::Int(data)
                }
            }
            ColumnData::String(data) => {
                assert!(
                    (data.len() as u64) == rows,
                    "rows: {}, data.len(): {}",
                    rows,
                    data.len()
                );
                InputColumn::Str(data)
            }
            ColumnData::Empty => InputColumn::Null(rows as usize),
            ColumnData::SparseI64(data) => InputColumn::NullableInt(rows, data),
            ColumnData::Mixed(data) => {
                InputColumn::Mixed(data.into_iter().map(|v| v.into()).collect())
            }
        }
    }
}
