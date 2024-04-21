use capnp::serialize_packed;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

use crate::api_capnp::{self, query_response};

#[derive(Serialize, Deserialize, Debug)]
pub struct ColumnNameRequest {
    pub tables: Vec<String>,
    pub pattern: Option<String>,
    pub offset: Option<usize>,
    pub limit: Option<usize>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ColumnNameResponse {
    pub columns: Vec<String>,
    pub offset: usize,
    pub len: usize,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct QueryRequest {
    pub query: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MultiQueryRequest {
    pub queries: Vec<String>,
    pub encoding_opts: Option<EncodingOpts>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct QueryResponse {
    pub columns: HashMap<String, Column>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct EncodingOpts {
    pub xor_float_compression: bool,
    pub mantissa: Option<u32>,
    pub full_precision_cols: HashSet<String>,
}

pub struct MultiQueryResponse {
    pub responses: Vec<QueryResponse>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Column {
    Float(Vec<f64>),
    Int(Vec<i64>),
    String(Vec<String>),
    Mixed(Vec<AnyVal>),
    Null(usize),

    Xor(Vec<u8>),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum AnyVal {
    Int(i64),
    Float(f64),
    Str(String),
    Null,
}

impl Column {
    pub fn size_bytes(&self) -> usize {
        let heapsize = match self {
            Column::Float(xs) => xs.len() * std::mem::size_of::<f64>(),
            Column::Int(xs) => xs.len() * std::mem::size_of::<i64>(),
            Column::String(xs) => xs.iter().map(|s| s.len()).sum(),
            Column::Mixed(xs) => xs
                .iter()
                .map(|m| match m {
                    AnyVal::Str(s) => s.len() + std::mem::size_of::<AnyVal>(),
                    _ => std::mem::size_of::<AnyVal>(),
                })
                .sum(),
            Column::Null(_) => 0,
            Column::Xor(xs) => xs.len(),
        };
        heapsize + std::mem::size_of::<Column>()
    }

    pub fn len(&self) -> usize {
        match self {
            Column::Float(xs) => xs.len(),
            Column::Int(xs) => xs.len(),
            Column::String(xs) => xs.len(),
            Column::Mixed(xs) => xs.len(),
            Column::Null(n) => *n,
            Column::Xor(_) => panic!("len() not implemented for xor compressed columns"),
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl MultiQueryResponse {
    pub fn deserialize(data: &[u8]) -> capnp::Result<MultiQueryResponse> {
        let message_reader =
            serialize_packed::read_message(data, capnp::message::ReaderOptions::new())?;
        let multi_query_response =
            message_reader.get_root::<api_capnp::multi_query_response::Reader>()?;
        let mut responses = Vec::new();
        for query_response in multi_query_response.get_responses()?.iter() {
            responses.push(QueryResponse::deserialize_reader(query_response)?);
        }
        Ok(MultiQueryResponse { responses })
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut builder = capnp::message::Builder::new_default();
        let multi_query_response = builder.init_root::<api_capnp::multi_query_response::Builder>();
        let mut responses = multi_query_response.init_responses(self.responses.len() as u32);
        for (i, response) in self.responses.iter().enumerate() {
            let mut response_builder = responses.reborrow().get(i as u32);
            response.serialize_builder(&mut response_builder);
        }
        let mut buf = Vec::new();
        serialize_packed::write_message(&mut buf, &builder).unwrap();
        buf
    }
}

impl QueryResponse {
    pub fn deserialize(data: &[u8]) -> capnp::Result<QueryResponse> {
        let message_reader =
            serialize_packed::read_message(data, capnp::message::ReaderOptions::new()).unwrap();
        let query_response = message_reader.get_root::<api_capnp::query_response::Reader>()?;
        QueryResponse::deserialize_reader(query_response)
    }

    pub fn deserialize_reader(
        query_response: query_response::Reader,
    ) -> capnp::Result<QueryResponse> {
        let mut columns = HashMap::new();
        for column in query_response.get_columns()?.iter() {
            let name = column.get_name()?.to_string().unwrap();
            use api_capnp::column::data::Which;
            let column = match column.get_data().which()? {
                Which::F64(xs) => Column::Float(xs?.iter().collect()),
                Which::I64(xs) => Column::Int(xs?.iter().collect()),
                Which::String(xs) => {
                    let mut strings = Vec::new();
                    for s in xs?.iter() {
                        strings.push(s?.to_string().unwrap());
                    }
                    Column::String(strings)
                }
                Which::Mixed(xs) => {
                    let mut mixed = Vec::new();
                    for x in xs?.iter() {
                        use api_capnp::any_val::Which;
                        let x = match x.which()? {
                            Which::I64(x) => AnyVal::Int(x),
                            Which::F64(x) => AnyVal::Float(x),
                            Which::String(x) => AnyVal::Str(x?.to_string().unwrap()),
                            Which::Null(()) => AnyVal::Null,
                        };
                        mixed.push(x);
                    }
                    Column::Mixed(mixed)
                }
                Which::Null(xs) => Column::Null(xs as usize),
                Which::XorF64(xs) => Column::Xor(xs?.to_vec()),
            };
            columns.insert(name, column);
        }
        Ok(QueryResponse { columns })
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut builder = capnp::message::Builder::new_default();
        let mut query_response = builder.init_root::<query_response::Builder>();
        self.serialize_builder(&mut query_response);
        let mut buf = Vec::new();
        serialize_packed::write_message(&mut buf, &builder).unwrap();
        buf
    }

    pub fn serialize_builder(&self, query_response: &mut query_response::Builder) {
        let mut columns = query_response
            .reborrow()
            .init_columns(self.columns.len() as u32);
        for (i, (name, column)) in self.columns.iter().enumerate() {
            let mut column_builder = columns.reborrow().get(i as u32);
            column_builder.set_name(name);
            match column {
                Column::Float(xs) => column_builder
                    .reborrow()
                    .init_data()
                    .set_f64(&xs[..])
                    .unwrap(),
                Column::Int(xs) => column_builder
                    .reborrow()
                    .init_data()
                    .set_i64(&xs[..])
                    .unwrap(),
                Column::String(xs) => {
                    let mut strings = column_builder
                        .reborrow()
                        .init_data()
                        .init_string(xs.len() as u32);
                    for (i, s) in xs.iter().enumerate() {
                        strings.set(i as u32, s);
                    }
                }
                Column::Mixed(xs) => {
                    let mut mixed = column_builder
                        .reborrow()
                        .init_data()
                        .init_mixed(xs.len() as u32);
                    for (i, x) in xs.iter().enumerate() {
                        let mut x_builder = mixed.reborrow().get(i as u32);
                        match x {
                            AnyVal::Int(x) => x_builder.set_i64(*x),
                            AnyVal::Float(x) => x_builder.set_f64(*x),
                            AnyVal::Str(x) => x_builder.set_string(x),
                            AnyVal::Null => x_builder.set_null(()),
                        }
                    }
                }
                Column::Null(n) => column_builder
                    .reborrow()
                    .init_data()
                    .set_null(*n as u64),
                Column::Xor(xs) => column_builder
                    .reborrow()
                    .init_data()
                    .set_xor_f64(&xs[..]),
            };
        }
    }
}
