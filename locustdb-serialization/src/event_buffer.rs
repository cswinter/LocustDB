use std::collections::HashMap;

use crate::wal_segment_capnp::{self, table_segment_list};

#[derive(Default, Clone)]
pub struct EventBuffer {
    pub tables: HashMap<String, TableBuffer>,
}

#[derive(Default, Clone, Debug)]
pub struct TableBuffer {
    pub len: u64,
    pub columns: HashMap<String, ColumnBuffer>,
}

#[derive(Default, Clone, Debug)]
pub struct ColumnBuffer {
    pub data: ColumnData,
}

#[derive(Clone, Debug)]
pub enum ColumnData {
    Dense(Vec<f64>),
    Sparse(Vec<(u64, f64)>),
}

impl ColumnData {
    pub fn len(&self) -> usize {
        match self {
            ColumnData::Dense(data) => data.len(),
            ColumnData::Sparse(data) => data.len(),
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        match self {
            ColumnData::Dense(data) => data.is_empty(),
            ColumnData::Sparse(data) => data.is_empty(),
        }
    }
}

impl ColumnBuffer {
    pub fn push(&mut self, value: f64, len: u64) {
        match &mut self.data {
            ColumnData::Dense(data) => {
                if data.len() as u64 == len {
                    data.push(value)
                } else {
                    let mut sparse_data: Vec<(u64, f64)> = data
                        .drain(..)
                        .enumerate()
                        .map(|(i, v)| (i as u64, v))
                        .collect();
                    sparse_data.push((len, value));
                    self.data = ColumnData::Sparse(sparse_data);
                }
            }
            ColumnData::Sparse(data) => data.push((len, value)),
        }
    }
}

impl Default for ColumnData {
    fn default() -> Self {
        ColumnData::Dense(Vec::new())
    }
}

impl EventBuffer {
    pub fn serialize(&self) -> Vec<u8> {
        let mut builder = capnp::message::Builder::new_default();
        let mut table_segment_list = builder.init_root::<wal_segment_capnp::table_segment_list::Builder>();
        self.serialize_builder(&mut table_segment_list);
        let mut buf = Vec::new();
        capnp::serialize_packed::write_message(&mut buf, &builder).unwrap();
        buf
    }

    pub fn serialize_builder(&self, table_segment_list: &mut table_segment_list::Builder) {
        assert!(self.tables.len() < std::u32::MAX as usize);
        let mut data = table_segment_list
            .reborrow()
            .init_data(self.tables.len() as u32);
        for (i, (name, table)) in self.tables.iter().enumerate() {
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
    }

    pub fn deserialize(data: &[u8]) -> capnp::Result<Self> {
        let message_reader =
            capnp::serialize_packed::read_message(data, capnp::message::ReaderOptions::new())?;
        let table_segment_list = message_reader.get_root::<wal_segment_capnp::table_segment_list::Reader>()?;
        let data = EventBuffer::deserialize_reader(table_segment_list)?;
        Ok(EventBuffer {
            tables: data.tables,
        })
    }

    pub fn deserialize_reader(data: table_segment_list::Reader) -> capnp::Result<Self> {
        let mut tables = HashMap::<String, TableBuffer>::new();
        for table in data.get_data()?.iter() {
            let name = table.get_name()?.to_string().unwrap();
            let len = table.get_len();
            let mut columns = HashMap::new();
            for column in table.get_columns()?.iter() {
                let colname = column.get_name()?.to_string().unwrap();
                let data = column.get_data();
                use crate::wal_segment_capnp::column::data::Which;
                let data = match data.which()? {
                    Which::F64(f64s) => ColumnData::Dense(f64s?.iter().collect()),
                    Which::SparseF64(sparse) => {
                        let indices = sparse.get_indices()?;
                        let values = sparse.get_values()?;
                        ColumnData::Sparse(indices.iter().zip(values.iter()).collect())
                    }
                };
                columns.insert(colname, ColumnBuffer { data });
            }
            tables.insert(name, TableBuffer { len, columns });
        }
        Ok(EventBuffer {
            tables,
        })
    }
}
