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
            let (name, column) = Column::deserialize_reader(column)?;
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
            column.serialize_builder(name, &mut column_builder);
        }
    }
}

impl Column {
    #[cfg(test)]
    fn serialize(&self) -> Vec<u8> {
        let mut builder = capnp::message::Builder::new_default();
        let mut column = builder.init_root::<api_capnp::column::Builder>();
        self.serialize_builder("", &mut column);
        let mut buf = Vec::new();
        serialize_packed::write_message(&mut buf, &builder).unwrap();
        buf
    }

    fn serialize_builder(&self, name: &str, column_builder: &mut api_capnp::column::Builder) {
        column_builder.set_name(name);
        match self {
            Column::Float(xs) => column_builder
                .reborrow()
                .init_data()
                .set_f64(&xs[..])
                .unwrap(),
            Column::Int(xs) => {
                let delta_stats = determine_delta_compressability(&xs[..]);
                if delta_stats.min_delta == delta_stats.max_delta
                    && delta_stats.max_delta <= i64::MAX as i128
                    && delta_stats.max_delta >= i64::MIN as i128
                {
                    let mut range = column_builder.reborrow().init_data().init_range();
                    range.set_start(xs[0]);
                    range.set_len(xs.len() as u64);
                    range.set_step(delta_stats.min_delta as i64);
                } else if delta_stats.min_delta >= i8::MIN as i128
                    && delta_stats.max_delta <= i8::MAX as i128
                {
                    let mut delta_encoded = column_builder
                        .reborrow()
                        .init_data()
                        .init_delta_encoded_i8();
                    delta_encoded.set_first(xs[0]);
                    delta_encoded.set_data(&delta_encode(xs)[..]).unwrap();
                } else if delta_stats.min_delta_delta >= i8::MIN as i128
                    && delta_stats.max_delta_delta <= i8::MAX as i128
                {
                    let mut double_delta_encoded = column_builder
                        .reborrow()
                        .init_data()
                        .init_double_delta_encoded_i8();
                    double_delta_encoded.set_first(xs[0]);
                    double_delta_encoded.set_second(xs[1]);
                    double_delta_encoded
                        .set_data(&double_delta_encode(xs)[..])
                        .unwrap();
                } else if delta_stats.min_delta >= i16::MIN as i128
                    && delta_stats.max_delta <= i16::MAX as i128
                {
                    let mut delta_encoded = column_builder
                        .reborrow()
                        .init_data()
                        .init_delta_encoded_i16();
                    delta_encoded.set_first(xs[0]);
                    delta_encoded.set_data(&delta_encode(xs)[..]).unwrap();
                } else if delta_stats.min_delta_delta >= i16::MIN as i128
                    && delta_stats.max_delta_delta <= i16::MAX as i128
                {
                    let mut double_delta_encoded = column_builder
                        .reborrow()
                        .init_data()
                        .init_double_delta_encoded_i16();
                    double_delta_encoded.set_first(xs[0]);
                    double_delta_encoded.set_second(xs[1]);
                    double_delta_encoded
                        .set_data(&double_delta_encode(xs)[..])
                        .unwrap();
                } else if delta_stats.min_delta >= i32::MIN as i128
                    && delta_stats.max_delta <= i32::MAX as i128
                {
                    let mut delta_encoded = column_builder
                        .reborrow()
                        .init_data()
                        .init_delta_encoded_i32();
                    delta_encoded.set_first(xs[0]);
                    delta_encoded.set_data(&delta_encode(xs)[..]).unwrap();
                } else if delta_stats.min_delta_delta >= i32::MIN as i128
                    && delta_stats.max_delta_delta <= i32::MAX as i128
                {
                    let mut double_delta_encoded = column_builder
                        .reborrow()
                        .init_data()
                        .init_double_delta_encoded_i32();
                    double_delta_encoded.set_first(xs[0]);
                    double_delta_encoded.set_second(xs[1]);
                    double_delta_encoded
                        .set_data(&double_delta_encode(xs)[..])
                        .unwrap();
                } else {
                    column_builder
                        .reborrow()
                        .init_data()
                        .set_i64(&xs[..])
                        .unwrap()
                }
            }
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
            Column::Null(n) => column_builder.reborrow().init_data().set_null(*n as u64),
            Column::Xor(xs) => column_builder.reborrow().init_data().set_xor_f64(&xs[..]),
        };
    }

    #[cfg(test)]
    fn deserialize(data: &[u8]) -> capnp::Result<(String, Column)> {
        let message_reader =
            serialize_packed::read_message(data, capnp::message::ReaderOptions::new()).unwrap();
        let column = message_reader.get_root::<api_capnp::column::Reader>()?;
        Column::deserialize_reader(column)
    }

    fn deserialize_reader(column: api_capnp::column::Reader) -> capnp::Result<(String, Column)> {
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
            Which::DeltaEncodedI8(xs) => {
                let first = xs.get_first();
                let data = xs.get_data()?;
                let mut decoded = Vec::with_capacity(data.len() as usize + 1);
                decoded.push(first);
                let mut last = first;
                for i in data {
                    last += i as i64;
                    decoded.push(last);
                }
                Column::Int(decoded)
            }
            Which::DeltaEncodedI16(xs) => {
                let first = xs.get_first();
                let data = xs.get_data()?;
                let mut decoded = Vec::with_capacity(data.len() as usize + 1);
                decoded.push(first);
                let mut last = first;
                for i in data {
                    last += i as i64;
                    decoded.push(last);
                }
                Column::Int(decoded)
            }
            Which::DeltaEncodedI32(xs) => {
                let first = xs.get_first();
                let data = xs.get_data()?;
                let mut decoded = Vec::with_capacity(data.len() as usize + 1);
                decoded.push(first);
                let mut last = first;
                for i in data {
                    last += i as i64;
                    decoded.push(last);
                }
                Column::Int(decoded)
            }
            Which::DoubleDeltaEncodedI8(xs) => {
                let first = xs.get_first();
                let second = xs.get_second();
                let data = xs.get_data()?;
                let mut decoded = Vec::with_capacity(data.len() as usize + 2);
                decoded.push(first);
                decoded.push(second);
                let mut last = second;
                let mut last_delta = second - first;
                for i in data {
                    last_delta += i as i64;
                    last += last_delta;
                    decoded.push(last);
                }
                Column::Int(decoded)
            }
            Which::DoubleDeltaEncodedI16(xs) => {
                let first = xs.get_first();
                let second = xs.get_second();
                let data = xs.get_data()?;
                let mut decoded = Vec::with_capacity(data.len() as usize + 2);
                decoded.push(first);
                decoded.push(second);
                let mut last = second;
                let mut last_delta = second - first;
                for i in data {
                    last_delta += i as i64;
                    last += last_delta;
                    decoded.push(last);
                }
                Column::Int(decoded)
            }
            Which::DoubleDeltaEncodedI32(xs) => {
                let first = xs.get_first();
                let second = xs.get_second();
                let data = xs.get_data()?;
                let mut decoded = Vec::with_capacity(data.len() as usize + 2);
                decoded.push(first);
                decoded.push(second);
                let mut last = second;
                let mut last_delta = second - first;
                for i in data {
                    last_delta += i as i64;
                    last += last_delta;
                    decoded.push(last);
                }
                Column::Int(decoded)
            }
            Which::Range(xs) => {
                let start = xs.get_start();
                let len = xs.get_len() as usize;
                let step = xs.get_step();
                let decoded = (0..len).map(|i| start + i as i64 * step).collect();
                Column::Int(decoded)
            }
        };
        Ok((name, column))
    }
}

#[derive(Debug)]
struct DeltaStats {
    min_delta: i128,
    max_delta: i128,
    min_delta_delta: i128,
    max_delta_delta: i128,
}

fn determine_delta_compressability(ints: &[i64]) -> DeltaStats {
    let mut min_delta = i128::MAX;
    let mut max_delta = i128::MIN;
    let mut min_delta_delta = i128::MAX;
    let mut max_delta_delta = i128::MIN;

    if ints.len() < 2 {
        return DeltaStats {
            min_delta,
            max_delta,
            min_delta_delta,
            max_delta_delta,
        };
    }

    let mut previous = ints[1];
    let mut previous_delta = (ints[1] - ints[0]) as i128;
    for curr in &ints[2..] {
        let delta = (*curr - previous) as i128;
        min_delta = min_delta.min(delta);
        max_delta = max_delta.max(delta);
        let delta_delta = delta - previous_delta;
        min_delta_delta = min_delta_delta.min(delta_delta);
        max_delta_delta = max_delta_delta.max(delta_delta);
        previous = *curr;
        previous_delta = delta;
    }

    DeltaStats {
        min_delta,
        max_delta,
        min_delta_delta,
        max_delta_delta,
    }
}

fn delta_encode<T>(ints: &[i64]) -> Vec<T>
where
    T: TryFrom<i64>,
    <T as TryFrom<i64>>::Error: std::fmt::Debug,
{
    let mut encoded = Vec::with_capacity(ints.len());
    let mut previous = ints[0];
    for curr in &ints[1..] {
        let delta = *curr - previous;
        encoded.push(T::try_from(delta).unwrap());
        previous = *curr;
    }
    encoded
}

fn double_delta_encode<T>(ints: &[i64]) -> Vec<T>
where
    T: TryFrom<i64>,
    <T as TryFrom<i64>>::Error: std::fmt::Debug,
{
    let mut encoded = Vec::with_capacity(ints.len());
    let mut previous = ints[1];
    let mut previous_delta = ints[1] - ints[0];
    for curr in &ints[2..] {
        let delta = curr - previous;
        let delta_delta = delta - previous_delta;
        encoded.push(T::try_from(delta_delta).unwrap());
        previous = *curr;
        previous_delta = delta;
    }
    encoded
}

pub mod any_val_syntax {
    pub fn vf64<F>(x: F) -> super::AnyVal
    where
        F: TryInto<f64>,
        <F as TryInto<f64>>::Error: std::fmt::Debug,
    {
        super::AnyVal::Float(x.try_into().unwrap())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    fn test_compression(ints: Vec<i64>, max_bytes: usize) {
        let column = Column::Int(ints.clone());
        let serialized = column.serialize();
        assert!(serialized.len() < max_bytes, "took {} bytes", serialized.len());
        let (_, deserialized) = Column::deserialize(&serialized).unwrap();
        match deserialized {
            Column::Int(xs) => assert_eq!(ints, xs),
            _ => panic!("expected int column"),
        }
    }

    #[test]
    fn test_range_compression() {
        test_compression(
            (0..1024)
                .map(|i| -1231429 + i * 241248124)
                .collect::<Vec<_>>(),
            64,
        );
    }

    #[test]
    fn test_i8_delta_compression() {
        test_compression(
            (0..1024)
                .map(|i| -1231429 + i * 32 - i % 32)
                .collect::<Vec<_>>(),
            64 + 1024,
        );
    }

    #[test]
    fn test_i16_delta_compression() {
        test_compression(
            (0..1024)
                .map(|i| -1231429 + i * 512 - (i * 7) % 1024)
                .collect::<Vec<_>>(),
            64 + 2048,
        );
    }

    #[test]
    fn test_i8_delta_delta_compression() {
        test_compression(
            (0..1024)
                .map(|i| -1231429 + i * 3812384134 - (i * 7) % 32)
                .collect::<Vec<_>>(),
            64 + 1024,
        );
    }
}
