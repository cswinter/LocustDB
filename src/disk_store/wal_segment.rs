use std::borrow::Cow;
use std::collections::HashMap;

use capnp::serialize_packed;

use crate::logging_client::{ColumnBuffer, ColumnData, EventBuffer, TableBuffer};

use super::serialization::wal_segment_capnp;

pub struct WalSegment<'a> {
    pub id: u64,
    pub data: Cow<'a, EventBuffer>,
}

impl<'a> WalSegment<'a> {
    pub fn serialize(&self) -> Vec<u8> {
        let mut builder = capnp::message::Builder::new_default();
        let mut wal_segment = builder.init_root::<wal_segment_capnp::wal_segment::Builder>();
        wal_segment.set_id(self.id);

        assert!(self.data.tables.len() < std::u32::MAX as usize);
        let mut data = wal_segment
            .reborrow()
            .init_data(self.data.tables.len() as u32);
        for (i, (name, table)) in self.data.tables.iter().enumerate() {
            let mut table_builder = data.reborrow().get(i as u32);
            table_builder.set_name(name);
            let mut columns = table_builder
                .reborrow()
                .init_columns(table.columns.len() as u32);
            for (j, (colname, column)) in table.columns.iter().enumerate() {
                let mut column_builder = columns.reborrow().get(j as u32);
                column_builder.set_name(colname);
                match &column.data {
                    ColumnData::Dense(f64s) => {
                        column_builder.get_data().set_f64(&f64s[..]).unwrap();
                    }
                    ColumnData::Sparse(sparse) => {
                        let mut sparse_builder = column_builder.get_data().init_sparse_f64();
                        assert!(sparse.len() < std::u32::MAX as usize);
                        let (indices, values): (Vec<_>, Vec<_>) = sparse.iter().cloned().unzip();
                        sparse_builder.reborrow().set_indices(&indices[..]).unwrap();
                        sparse_builder.reborrow().set_values(&values[..]).unwrap();
                    }
                }
            }
        }

        let mut buf = Vec::new();
        serialize_packed::write_message(&mut buf, &builder).unwrap();
        buf
    }

    pub fn deserialize(data: &[u8]) -> capnp::Result<WalSegment<'static>> {
        let message_reader =
            serialize_packed::read_message(data, capnp::message::ReaderOptions::new()).unwrap();
        let wal_segment = message_reader.get_root::<wal_segment_capnp::wal_segment::Reader>()?;
        let id = wal_segment.get_id();
        let mut tables = HashMap::<String, TableBuffer>::new();
        for table in wal_segment.get_data()?.iter() {
            let name = table.get_name()?.to_string().unwrap();
            let len = table.get_len();
            let mut columns = HashMap::new();
            for column in table.get_columns()?.iter() {
                let colname = column.get_name()?.to_string().unwrap();
                let data = column.get_data();
                use wal_segment_capnp::column::data::Which;
                let data = match data.which()? {
                    Which::F64(f64s) => ColumnData::Dense(f64s?.iter().collect()),
                    Which::SparseF64(sparse) => {
                        let indices = sparse.get_indices()?;
                        let values = sparse.get_values()?;
                        ColumnData::Sparse(
                            indices
                                .iter()
                                .zip(values.iter())
                                .collect(),
                        )
                    }
                };
                columns.insert(colname, ColumnBuffer { data });
            }
            tables.insert(name, TableBuffer { len, columns });
        }

        Ok(WalSegment {
            id,
            data: Cow::Owned(EventBuffer { tables }),
        })
    }
}
