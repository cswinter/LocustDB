use capnp::serialize_packed;
use locustdb_serialization::{dbmeta_capnp, default_reader_options};
use lz4_flex::block::{compress_prepend_size, decompress_size_prepended};
use pco::standalone::{simple_decompress, simpler_compress};
use pco::DEFAULT_COMPRESSION_LEVEL;
use std::collections::{HashMap, HashSet};

type TableName = String;
type PartitionID = u64;

#[derive(Clone)]
pub struct MetaStore {
    pub next_wal_id: u64,
    pub partitions: HashMap<TableName, HashMap<PartitionID, PartitionMetadata>>,
}

#[derive(Clone, Debug)]
pub struct PartitionMetadata {
    pub id: PartitionID,
    pub tablename: String,
    pub offset: usize,
    pub len: usize,
    pub subpartitions: Vec<SubpartitionMetadata>,
    pub column_name_to_subpartition_index: HashMap<String, usize>,
}

#[derive(Clone, Debug)]
pub struct SubpartitionMetadata {
    pub size_bytes: u64,
    pub subpartition_key: String,
}

impl PartitionMetadata {
    pub fn subpartition_key(&self, column_name: &str) -> String {
        let subpartition_index = self.column_name_to_subpartition_index[column_name];
        self.subpartitions[subpartition_index]
            .subpartition_key
            .clone()
    }
}

impl MetaStore {
    pub fn serialize(&self) -> Vec<u8> {
        let mut builder = ::capnp::message::Builder::new_default();
        let mut dbmeta = builder.init_root::<dbmeta_capnp::d_b_meta::Builder>();
        dbmeta.set_next_wal_id(self.next_wal_id);

        let total_partitions = self.partitions.values().map(|x| x.len()).sum::<usize>();
        assert!(total_partitions < std::u32::MAX as usize);
        let mut i = 0;

        let unique_strings = self
            .partitions
            .values()
            .flat_map(|x| x.values())
            .flat_map(|x| x.column_name_to_subpartition_index.keys())
            .collect::<HashSet<_>>();
        let mut sorted_strings = unique_strings.iter().cloned().collect::<Vec<_>>();
        sorted_strings.sort();
        let mut string_bytes: Vec<u8> = Vec::new();
        let mut lens = Vec::new();
        for string in &sorted_strings {
            assert!(string.len() <= std::u16::MAX as usize);
            lens.push(string.len() as u16);
            string_bytes.extend(string.as_bytes());
        }
        dbmeta.reborrow().set_compressed_strings(&compress_prepend_size(&string_bytes));
        dbmeta.reborrow().set_lengths_compressed_strings(&lens[..]).unwrap();
        assert!(sorted_strings.len() < std::u32::MAX as usize);
        let column_name_to_id = sorted_strings
            .iter()
            .cloned()
            .enumerate()
            .map(|(i, s)| (s, i as u64))
            .collect::<HashMap<_, _>>();

        let mut partitions_builder = dbmeta.reborrow().init_partitions(total_partitions as u32);
        for table in self.partitions.values() {
            for partition in table.values() {
                let mut subpartition_index_to_column_names =
                    vec![Vec::new(); partition.subpartitions.len()];
                for (column_name, subpartition_index) in
                    &partition.column_name_to_subpartition_index
                {
                    let column_id = column_name_to_id[column_name];
                    subpartition_index_to_column_names[*subpartition_index].push(column_id);
                }
                let mut partition_builder = partitions_builder.reborrow().get(i);
                partition_builder.set_id(partition.id);
                partition_builder.set_tablename(&partition.tablename);
                partition_builder.set_offset(partition.offset as u64);
                partition_builder.set_len(partition.len as u64);
                assert!(partition.subpartitions.len() < std::u32::MAX as usize);
                let mut subpartitions_builder =
                    partition_builder.init_subpartitions(partition.subpartitions.len() as u32);
                for (i, subpartition) in partition.subpartitions.iter().enumerate() {
                    let mut subpartition_builder = subpartitions_builder.reborrow().get(i as u32);
                    subpartition_builder.set_size_bytes(subpartition.size_bytes);
                    subpartition_builder.set_subpartition_key(&subpartition.subpartition_key);
                    let subpartition_column_ids_sorted = itertools::Itertools::sorted(subpartition_index_to_column_names[i].iter().cloned());
                    let all_column_ids_compressed = simpler_compress(&subpartition_column_ids_sorted, DEFAULT_COMPRESSION_LEVEL).unwrap();
                    subpartition_builder.set_compressed_interned_columns(&all_column_ids_compressed[..]);
                }
                i += 1;
            }
        }
        let mut strings_builder = dbmeta.init_strings(column_name_to_id.len() as u32);
        for (column_name, column_id) in column_name_to_id {
            strings_builder.set(column_id as u32, column_name);
        }

        let mut buf = Vec::new();
        serialize_packed::write_message(&mut buf, &builder).unwrap();
        buf
    }

    pub fn deserialize(data: &[u8]) -> capnp::Result<MetaStore> {
        let message_reader =
            serialize_packed::read_message(&mut &data[..], default_reader_options())?;
        let dbmeta = message_reader.get_root::<dbmeta_capnp::d_b_meta::Reader>()?;
        let next_wal_id = dbmeta.get_next_wal_id();

        // v1
        let mut strings = Vec::new();
        for string in dbmeta.get_strings()? {
            strings.push(string?.to_string().unwrap());
        }
        // v2
        let compressed_strs = dbmeta.get_compressed_strings()?;
        if !compressed_strs.is_empty() {
            let decompressed = decompress_size_prepended(compressed_strs).unwrap();
            let mut i = 0;
            for len in dbmeta.get_lengths_compressed_strings()? {
                let len = len as usize;
                strings.push(String::from_utf8(decompressed[i..i + len].to_vec()).unwrap());
                i += len;
            }
        }

        let mut partitions = HashMap::<TableName, HashMap<PartitionID, PartitionMetadata>>::new();
        for partition in dbmeta.get_partitions()? {
            let id = partition.get_id();
            let tablename = partition.get_tablename()?.to_string().unwrap();
            let offset = partition.get_offset() as usize;
            let len = partition.get_len() as usize;
            let mut subpartitions = Vec::new();
            let mut column_name_to_subpartition_index = HashMap::new();
            for (i, subpartition) in partition.get_subpartitions()?.iter().enumerate() {
                let size_bytes = subpartition.get_size_bytes();
                let subpartition_key = subpartition.get_subpartition_key()?.to_string().unwrap();
                subpartitions.push(SubpartitionMetadata {
                    size_bytes,
                    subpartition_key,
                });
                // v0
                for column in subpartition.get_columns()? {
                    let column = column?.to_string().unwrap();
                    column_name_to_subpartition_index.insert(column, i);
                }
                // v1
                for column_string_id in subpartition.get_interned_columns()? {
                    let column = strings[column_string_id as usize].clone();
                    column_name_to_subpartition_index.insert(column, i);
                }
                // v2
                let compressed_interned_columns = subpartition.get_compressed_interned_columns()?;
                if !compressed_interned_columns.is_empty() {
                    let interned_columns = simple_decompress::<u64>(compressed_interned_columns).unwrap();
                    for column_id in interned_columns {
                        let column = strings[column_id as usize].clone();
                        column_name_to_subpartition_index.insert(column, i);
                    }
                }
            }
            let partition = PartitionMetadata {
                id,
                tablename: tablename.clone(),
                offset,
                len,
                subpartitions,
                column_name_to_subpartition_index,
            };
            partitions
                .entry(tablename)
                .or_default()
                .insert(id, partition);
        }

        Ok(MetaStore {
            next_wal_id,
            partitions,
        })
    }
}
