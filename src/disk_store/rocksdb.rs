extern crate rocksdb;
extern crate capnp;

use std::sync::Arc;

use byteorder::{ByteOrder, BigEndian};
use self::rocksdb::*;
use storage_format_capnp::*;
use capnp::{serialize, Word, message};

use disk_store::interface::*;
use mem_store::column::{Column, DataSection};
use scheduler::inner_locustdb::InnerLocustDB;
use mem_store::codec::CodecOp;
use engine::types::EncodingType as Type;


pub struct RocksDB {
    db: DB,
}

impl RocksDB {
    pub fn new(path: &str) -> RocksDB {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);

        let mut partitions_options = Options::default();
        if cfg!(feature = "enable_lz4") {
            partitions_options.set_compression_type(DBCompressionType::None);
        }

        let db = DB::open_cf_descriptors(&options, path, vec![
            ColumnFamilyDescriptor::new("metadata", Options::default()),
            ColumnFamilyDescriptor::new("partitions", partitions_options),
        ]).unwrap();
        RocksDB { db }
    }

    fn metadata(&self) -> ColumnFamily {
        self.db.cf_handle("metadata").unwrap()
    }

    fn partitions(&self) -> ColumnFamily {
        self.db.cf_handle("partitions").unwrap()
    }
}

impl DiskStore for RocksDB {
    fn load_metadata(&self) -> Vec<PartitionMetadata> {
        let mut metadata = Vec::new();
        let iter = self.db.iterator_cf(self.metadata(), IteratorMode::Start).unwrap();
        for (key, value) in iter {
            let partition_id = BigEndian::read_u64(&key) as PartitionID;
            metadata.push(deserialize_meta_data(&value, partition_id))
        }
        metadata
    }

    fn load_column(&self, partition: PartitionID, column_name: &str) -> Column {
        let data = self.db.get_cf(self.partitions(), &column_key(partition, column_name)).unwrap().unwrap();
        deserialize_column(&data)
    }

    fn bulk_load(&self, ldb: &InnerLocustDB, start: PartitionID, end: PartitionID) {
        let mut key = [0; 8];
        BigEndian::write_u64(&mut key, start as u64);
        let iterator = self.db
            .iterator_cf(self.partitions(), IteratorMode::From(&key, Direction::Forward))
            .unwrap();
        for (key, value) in iterator {
            let id = BigEndian::read_u64(&key);
            if id >= end as u64 { break; }
            ldb.restore(id, deserialize_column(&value));
        }
    }

    fn store_partition(&self, partition: PartitionID, tablename: &str, columns: &Vec<Arc<Column>>) {
        let mut tx = WriteBatch::default();
        let mut key = [0; 8];
        BigEndian::write_u64(&mut key, partition as u64);
        let md = serialize_meta_data(tablename, columns);
        tx.put_cf(self.metadata(), &key, &md).unwrap();
        for column in columns {
            let key = column_key(partition, column.name());
            let data = serialize_column(column.as_ref());
            tx.put_cf(self.partitions(), &key, &data).unwrap();
        }

        self.db.write(tx).unwrap();
    }
}

fn column_key(id: PartitionID, column_name: &str) -> Vec<u8> {
    let mut key = vec![0; 8];
    BigEndian::write_u64(&mut key, id as u64);
    key.push('.' as u8);
    key.extend(column_name.as_bytes());
    key
}

fn deserialize_column(data: &[u8]) -> Column {
    let message_reader = serialize::read_message_from_words(
        Word::bytes_to_words(&data),
        message::ReaderOptions::new()).unwrap();
    let column = message_reader.get_root::<column::Reader>().unwrap();

    let name = column.get_name().unwrap();
    let len = column.get_len() as usize;
    let range = match column.get_range().which().unwrap() {
        column::range::Which::Empty(_) => None,
        column::range::Which::Range(range) => {
            let range = range.unwrap();
            Some((range.get_start(), range.get_end()))
        }
    };

    let codec = column.get_codec().unwrap().iter().map(|op| {
        use storage_format_capnp::codec_op::Which::*;
        match op.which().unwrap() {
            Add(add) => {
                let add = add.unwrap();
                CodecOp::Add(deserialize_type(add.get_type().unwrap()), add.get_amount())
            }
            Delta(delta) => CodecOp::Delta(deserialize_type(delta.unwrap())),
            ToI64(toi64) => CodecOp::ToI64(deserialize_type(toi64.unwrap())),
            PushDataSection(section) => CodecOp::PushDataSection(section as usize),
            DictLookup(t) => CodecOp::DictLookup(deserialize_type(t.unwrap())),
            Lz4(lz4) => {
                let lz4 = lz4.unwrap();
                CodecOp::LZ4(deserialize_type(lz4.get_type().unwrap()), lz4.get_len_decoded() as usize)
            }
            UnpackStrings(_) => CodecOp::UnpackStrings,
            UnhexpackStrings(uhps) => {
                let uhps = uhps.unwrap();
                CodecOp::UnhexpackStrings(uhps.get_uppercase(), uhps.get_total_bytes() as usize)
            }
        }
    }).collect::<Vec<_>>();

    let data_sections = column.get_data().unwrap().iter().map(|d| {
        use storage_format_capnp::data_section::Which::*;
        match d.which().unwrap() {
            U8(data) => {
                let data = data.unwrap();
                let mut buffer = Vec::with_capacity(data.len() as usize);
                buffer.extend(data);
                DataSection::U8(buffer)
            },
            U16(data) => {
                let data = data.unwrap();
                let mut buffer = Vec::with_capacity(data.len() as usize);
                buffer.extend(data);
                DataSection::U16(buffer)
            },
            U32(data) => {
                let data = data.unwrap();
                let mut buffer = Vec::with_capacity(data.len() as usize);
                buffer.extend(data);
                DataSection::U32(buffer)
            },
            U64(data) => {
                let data = data.unwrap();
                let mut buffer = Vec::with_capacity(data.len() as usize);
                buffer.extend(data);
                DataSection::U64(buffer)
            },
            I64(data) => {
                let data = data.unwrap();
                let mut buffer = Vec::with_capacity(data.len() as usize);
                buffer.extend(data);
                DataSection::I64(buffer)
            },
            Null(count) => DataSection::Null(count as usize),
        }
    }).collect::<Vec<_>>();

    Column::new(name, len, range, codec, data_sections)
}

fn deserialize_type(t: EncodingType) -> Type {
    use self::EncodingType::*;
    match t {
        U8 => Type::U8,
        U16 => Type::U16,
        U32 => Type::U32,
        U64 => Type::U64,
        I64 => Type::I64,
        Null => Type::Null,
    }
}

fn deserialize_meta_data(data: &[u8], partition_id: PartitionID) -> PartitionMetadata {
    let message_reader = serialize::read_message_from_words(
        Word::bytes_to_words(data),
        message::ReaderOptions::new()).unwrap();
    let meta_data = message_reader.get_root::<meta_data::Reader>().unwrap();
    PartitionMetadata {
        id: partition_id,
        len: meta_data.get_len() as usize,
        tablename: meta_data.get_tablename().unwrap().to_string(),
        columns: meta_data.get_columns().unwrap().iter().map(|c| c.unwrap().to_string()).collect(),
    }
}

fn serialize_meta_data(tablename: &str, columns: &[Arc<Column>]) -> Vec<u8> {
    let mut builder = capnp::message::Builder::new_default();
    {
        let mut meta_data = builder.init_root::<meta_data::Builder>();
        meta_data.set_len(columns[0].len() as u64);
        meta_data.set_tablename(tablename);
        {
            let mut col_names = meta_data.reborrow().init_columns(columns.len() as u32);
            for (i, column) in columns.iter().enumerate() {
                col_names.set(i as u32, column.name());
            }
        }
    }
    let mut buffer = Vec::new();
    capnp::serialize::write_message(&mut buffer, &builder).unwrap();
    buffer
}

fn serialize_column(col: &Column) -> Vec<u8> {
    let mut builder = capnp::message::Builder::new_default();
    {
        let mut column = builder.init_root::<column::Builder>();
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
                    CodecOp::Add(t, amount) => {
                        let mut add = capnp_op.init_add();
                        add.set_type(encoding_type_to_capnp(t));
                        add.set_amount(amount);
                    }
                    CodecOp::Delta(t) => capnp_op.set_delta(encoding_type_to_capnp(t)),
                    CodecOp::ToI64(t) => capnp_op.set_to_i64(encoding_type_to_capnp(t)),
                    CodecOp::PushDataSection(section) => capnp_op.set_push_data_section(section as u64),
                    CodecOp::DictLookup(t) => capnp_op.set_dict_lookup(encoding_type_to_capnp(t)),
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
                    DataSection::U8(x) => {
                        let mut builder = ds.init_u8(x.len() as u32);
                        populate_primitive_list(&mut builder, x);
                    }
                    DataSection::U16(x) => {
                        let mut builder = ds.init_u16(x.len() as u32);
                        populate_primitive_list(&mut builder, x);
                    }
                    DataSection::U32(x) => {
                        let mut builder = ds.init_u32(x.len() as u32);
                        populate_primitive_list(&mut builder, x);
                    }
                    DataSection::U64(x) => {
                        let mut builder = ds.init_u64(x.len() as u32);
                        populate_primitive_list(&mut builder, x);
                    }
                    DataSection::I64(x) => {
                        let mut builder = ds.init_i64(x.len() as u32);
                        populate_primitive_list(&mut builder, x);
                    }
                    DataSection::Null(count) => ds.set_null(*count as u64),
                }
            }
        }
    }
    let mut buffer = Vec::new();
    capnp::serialize::write_message(&mut buffer, &builder).unwrap();
    buffer
}

fn encoding_type_to_capnp(t: Type) -> EncodingType {
    match t {
        Type::U8 => EncodingType::U8,
        Type::U16 => EncodingType::U16,
        Type::U32 => EncodingType::U32,
        Type::U64 => EncodingType::U64,
        Type::I64 => EncodingType::I64,
        Type::Null => EncodingType::Null,
        _ => panic!("Trying to encode unsupported type {:?}", t)
    }
}

fn populate_primitive_list<'a, T>(builder: &mut capnp::primitive_list::Builder<'a, T>, values: &[T])
    where T: capnp::private::layout::PrimitiveElement + Copy {
    for (i, &x) in values.iter().enumerate() {
        builder.set(i as u32, x);
    }
}
