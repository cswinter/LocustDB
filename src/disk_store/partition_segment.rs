use capnp::serialize_packed;
use locustdb_serialization::partition_segment_capnp;
use ordered_float::OrderedFloat;

use crate::engine::EncodingType;
use crate::mem_store::{CodecOp, Column, DataSection, DataSource};

pub struct PartitionSegment {
    pub columns: Vec<Column>,
}

impl PartitionSegment {
    pub fn serialize(cols: &[&Column]) -> Vec<u8> {
        let mut builder = capnp::message::Builder::new_default();
        let partition = builder.init_root::<partition_segment_capnp::partition_segment::Builder>();

        let mut columns = partition.init_columns(cols.len() as u32);
        for (i, col) in cols.iter().enumerate() {
            let mut column = columns.reborrow().get(i as u32);
            column.set_name(col.name());
            column.set_len(col.len() as u64);
            {
                let mut range = column.reborrow().init_range();
                match col.range() {
                    None => range.set_empty(()),
                    Some((start, end)) => {
                        let mut range = range.reborrow().init_range();
                        range.set_start(start);
                        range.set_end(end);
                    }
                }
            }
            {
                let mut codec = column.reborrow().init_codec(col.codec().ops().len() as u32);
                for (i, &op) in col.codec().ops().iter().enumerate() {
                    let mut capnp_op = codec.reborrow().get(i as u32);
                    match op {
                        CodecOp::Nullable => capnp_op.set_nullable(()),
                        CodecOp::Add(t, amount) => {
                            let mut add = capnp_op.init_add();
                            add.set_type(encoding_type_to_capnp(t));
                            add.set_amount(amount);
                        }
                        CodecOp::Delta(t) => capnp_op.set_delta(encoding_type_to_capnp(t)),
                        CodecOp::ToI64(t) => capnp_op.set_to_i64(encoding_type_to_capnp(t)),
                        CodecOp::PushDataSection(section) => {
                            capnp_op.set_push_data_section(section as u64)
                        }
                        CodecOp::DictLookup(t) => {
                            capnp_op.set_dict_lookup(encoding_type_to_capnp(t))
                        }
                        CodecOp::LZ4(t, decoded_length) => {
                            let mut lz4 = capnp_op.init_lz4();
                            lz4.set_type(encoding_type_to_capnp(t));
                            lz4.set_len_decoded(decoded_length as u64);
                        }
                        CodecOp::UnpackStrings => capnp_op.set_unpack_strings(()),
                        CodecOp::UnhexpackStrings(uppercase, total_bytes) => {
                            let mut uhps = capnp_op.init_unhexpack_strings();
                            uhps.set_uppercase(uppercase);
                            uhps.set_total_bytes(total_bytes as u64);
                        }
                        CodecOp::Unknown => panic!("Trying to serialize CodecOp::Unkown"),
                    }
                }
            }
            {
                let mut data_sections = column.reborrow().init_data(col.data().len() as u32);
                for (i, section) in col.data().iter().enumerate() {
                    let mut ds = data_sections.reborrow().get(i as u32);
                    match section {
                        DataSection::U8(x) => ds.set_u8(&x[..]).unwrap(),
                        DataSection::U16(x) => ds.set_u16(&x[..]).unwrap(),
                        DataSection::U32(x) => ds.set_u32(&x[..]).unwrap(),
                        DataSection::U64(x) => ds.set_u64(&x[..]).unwrap(),
                        DataSection::I64(x) => ds.set_i64(&x[..]).unwrap(),
                        DataSection::F64(x) => {
                            assert!(x.len() < std::u32::MAX as usize);
                            let mut builder = ds.init_f64(x.len() as u32);
                            for (i, f) in x.iter().enumerate() {
                                builder.set(i as u32, f.0);
                            }
                        }
                        DataSection::Null(count) => ds.set_null(*count as u64),
                        DataSection::Bitvec(x) => ds.set_bitvec(&x[..]).unwrap(),
                        DataSection::LZ4 { decoded_bytes, bytes_per_element, data } => {
                            let mut lz4 = ds.init_lz4();
                            lz4.set_decoded_bytes(*decoded_bytes as u64);
                            lz4.set_bytes_per_element(*bytes_per_element as u64);
                            lz4.set_data(&data[..]).unwrap();
                        }
                    }
                }
            }
        }

        let mut buf = Vec::new();
        serialize_packed::write_message(&mut buf, &builder).unwrap();
        buf
    }

    pub fn deserialize(data: &[u8]) -> capnp::Result<PartitionSegment> {
        let message_reader =
            serialize_packed::read_message(data, capnp::message::ReaderOptions::new()).unwrap();
        let partition_segment =
            message_reader.get_root::<partition_segment_capnp::partition_segment::Reader>()?;
        let mut columns = Vec::new();
        for column in partition_segment.get_columns()?.iter() {
            let name = column.get_name()?.to_string().unwrap();
            let len = column.get_len();
            use partition_segment_capnp::column::range::Which;
            let range = match column.get_range().which()? {
                Which::Empty(()) => None,
                Which::Range(range) => {
                    let range = range?;
                    let min = range.reborrow().get_start();
                    let max = range.get_end();
                    Some((min, max))
                }
            };

            let codec = column
                .get_codec()
                .unwrap()
                .iter()
                .map(|op| {
                    use partition_segment_capnp::codec_op::Which::*;
                    match op.which().unwrap() {
                        Nullable(_) => CodecOp::Nullable,
                        Add(add) => {
                            let add = add.unwrap();
                            CodecOp::Add(
                                deserialize_type(add.get_type().unwrap()),
                                add.get_amount(),
                            )
                        }
                        Delta(delta) => CodecOp::Delta(deserialize_type(delta.unwrap())),
                        ToI64(toi64) => CodecOp::ToI64(deserialize_type(toi64.unwrap())),
                        PushDataSection(section) => CodecOp::PushDataSection(section as usize),
                        DictLookup(t) => CodecOp::DictLookup(deserialize_type(t.unwrap())),
                        Lz4(lz4) => {
                            let lz4 = lz4.unwrap();
                            CodecOp::LZ4(
                                deserialize_type(lz4.get_type().unwrap()),
                                lz4.get_len_decoded() as usize,
                            )
                        }
                        UnpackStrings(_) => CodecOp::UnpackStrings,
                        UnhexpackStrings(uhps) => {
                            let uhps = uhps.unwrap();
                            CodecOp::UnhexpackStrings(
                                uhps.get_uppercase(),
                                uhps.get_total_bytes() as usize,
                            )
                        }
                    }
                })
                .collect::<Vec<_>>();

            let data_sections = column
                .get_data()
                .unwrap()
                .iter()
                .map(|d| {
                    use partition_segment_capnp::data_section::Which::*;
                    match d.which().unwrap() {
                        U8(data) => {
                            let data = data.unwrap();
                            let mut buffer = Vec::with_capacity(data.len() as usize);
                            buffer.extend(data);
                            DataSection::U8(buffer)
                        }
                        U16(data) => {
                            let data = data.unwrap();
                            let mut buffer = Vec::with_capacity(data.len() as usize);
                            buffer.extend(data);
                            DataSection::U16(buffer)
                        }
                        U32(data) => {
                            let data = data.unwrap();
                            let mut buffer = Vec::with_capacity(data.len() as usize);
                            buffer.extend(data);
                            DataSection::U32(buffer)
                        }
                        U64(data) => {
                            let data = data.unwrap();
                            let mut buffer = Vec::with_capacity(data.len() as usize);
                            buffer.extend(data);
                            DataSection::U64(buffer)
                        }
                        I64(data) => {
                            let data = data.unwrap();
                            let mut buffer = Vec::with_capacity(data.len() as usize);
                            buffer.extend(data);
                            DataSection::I64(buffer)
                        }
                        F64(data) => {
                            let data = data.unwrap();
                            let mut buffer = Vec::with_capacity(data.len() as usize);
                            buffer.extend(data.iter().map(OrderedFloat));
                            DataSection::F64(buffer)
                        }
                        Null(count) => DataSection::Null(count as usize),
                        Bitvec(data) => {
                            let data = data.unwrap();
                            let mut buffer = Vec::with_capacity(data.len() as usize);
                            buffer.extend(data);
                            DataSection::Bitvec(buffer)
                        }
                        Lz4(lz4) => {
                            let data = lz4.get_data().unwrap();
                            let mut buffer = Vec::with_capacity(data.len() as usize);
                            buffer.extend(data);
                            DataSection::LZ4 {
                                decoded_bytes: lz4.get_decoded_bytes() as usize,
                                bytes_per_element: lz4.get_bytes_per_element() as usize,
                                data: buffer,
                            }
                        }
                    }
                })
                .collect::<Vec<_>>();

            columns.push(Column::new(
                &name,
                len as usize,
                range,
                codec,
                data_sections,
            ));
        }

        Ok(PartitionSegment { columns })
    }
}

fn deserialize_type(t: partition_segment_capnp::EncodingType) -> EncodingType {
    use partition_segment_capnp::EncodingType::*;
    match t {
        U8 => EncodingType::U8,
        U16 => EncodingType::U16,
        U32 => EncodingType::U32,
        U64 => EncodingType::U64,
        I64 => EncodingType::I64,
        Null => EncodingType::Null,
        F64 => EncodingType::F64,
        Bitvec => EncodingType::Bitvec,
    }
}

fn encoding_type_to_capnp(t: EncodingType) -> partition_segment_capnp::EncodingType {
    use partition_segment_capnp::EncodingType::*;
    match t {
        EncodingType::U8 => U8,
        EncodingType::U16 => U16,
        EncodingType::U32 => U32,
        EncodingType::U64 => U64,
        EncodingType::I64 => I64,
        EncodingType::Null => Null,
        EncodingType::F64 => F64,
        EncodingType::Bitvec => Bitvec,
        _ => panic!("Trying to encode unsupported type {:?}", t),
    }
}
