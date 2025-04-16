use capnp::serialize_packed;
use datasize::DataSize;
use locustdb_serialization::{dbmeta_capnp, default_reader_options};
use lz4_flex::block::decompress_size_prepended;
use pco::standalone::simple_decompress;
use std::collections::{BTreeMap, HashMap};
use std::ops::Range;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use crate::observability::SimpleTracer;

type TableName = String;
type PartitionID = u64;

#[derive(Clone, DataSize, Default, Debug)]
pub struct MetaStore {
    // ID for the next WAL segment to be written
    next_wal_id: u64,
    // ID of the earliest WAL segment that has not been flushed into partitions (this WAL segment may not exist yet)
    earliest_unflushed_wal_id: u64,
    /// Maps each table to it's set of partitions.
    /// Each partition is a contigous subset of rows in the table.
    partitions: HashMap<TableName, HashMap<PartitionID, PartitionMetadata>>,
}

#[derive(Clone, DataSize, Debug)]
pub struct MetaStoreDirectory {
    // ID for the next WAL segment to be written
    pub next_wal_id: u64,
    // ID of the earliest WAL segment that has not been flushed into partitions (this WAL segment may not exist yet)
    pub earliest_uncommited_wal_id: u64,
    // IDs of metastore partition segments
    pub partitions: Vec<u64>,
}

/// Metadata for a partition of a table.
/// A partition is a contigous subset of rows in the table.
/// A partition may be split into subpartitions, each of which holds a subset of the columns of the partition.
#[derive(Clone, Debug, DataSize)]
pub struct PartitionMetadata {
    pub id: PartitionID,
    pub tablename: String,
    pub offset: usize,
    pub len: usize,
    pub subpartitions: Vec<SubpartitionMetadata>,
    // Maps the last column name in each subpartition to the corresponding index in `subpartitions`
    pub subpartitions_by_last_column: BTreeMap<String, usize>,
}

#[derive(Clone, Debug, DataSize)]
pub struct SubpartitionMetadata {
    pub size_bytes: u64,
    pub subpartition_key: String,
    pub last_column: String,
    #[data_size(with = atomic_bool_size)]
    pub loaded: Arc<AtomicBool>,
}

fn atomic_bool_size(_: &Arc<AtomicBool>) -> usize {
    std::mem::size_of::<AtomicBool>()
}

impl PartitionMetadata {
    pub fn subpartition_key(&self, column_name: &str) -> Option<String> {
        let (_, subpartition_index) = self
            .subpartitions_by_last_column
            .lower_bound(std::ops::Bound::Included(column_name))
            .peek_next()?;
        Some(
            self.subpartitions[*subpartition_index]
                .subpartition_key
                .clone(),
        )
    }

    pub fn subpartition_has_been_loaded(&self, column_name: &str) -> bool {
        let subpartition_index = match self
            .subpartitions_by_last_column
            .lower_bound(std::ops::Bound::Included(column_name))
            .peek_next()
        {
            Some((_, subpartition_index)) => subpartition_index,
            None => return true,
        };
        self.subpartitions[*subpartition_index]
            .loaded
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    pub fn mark_subpartition_as_loaded(&self, column_name: &str) {
        if let Some((_, subpartition_index)) = self
            .subpartitions_by_last_column
            .lower_bound(std::ops::Bound::Included(column_name))
            .peek_next()
        {
            self.subpartitions[*subpartition_index]
                .loaded
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }
}

impl MetaStore {
    pub fn earliest_uncommited_wal_id(&self) -> u64 {
        self.earliest_unflushed_wal_id
    }

    pub fn register_wal_segment(&mut self, wal_id: u64) {
        self.next_wal_id = self.next_wal_id.max(wal_id + 1);
    }

    pub fn unflushed_wal_ids(&self) -> Range<u64> {
        self.earliest_unflushed_wal_id..self.next_wal_id
    }

    pub fn advance_earliest_unflushed_wal_id(&mut self, wal_id: u64) {
        self.earliest_unflushed_wal_id = wal_id;
    }

    pub fn next_wal_id(&self) -> u64 {
        self.next_wal_id
    }

    /// Iterate over all partitions in the metastore. (used once on startup to restore tables)
    pub fn partitions(&self) -> impl Iterator<Item = &PartitionMetadata> {
        self.partitions.values().flat_map(|x| x.values())
    }

    pub fn partitions_for_table(
        &self,
        table_name: &str,
    ) -> impl Iterator<Item = &PartitionMetadata> {
        self.partitions.get(table_name).unwrap().values()
    }

    pub fn subpartition_key(
        &self,
        table_name: &str,
        partition: PartitionID,
        column_name: &str,
    ) -> Option<String> {
        self.partitions[table_name][&partition].subpartition_key(column_name)
    }

    pub fn subpartition_has_been_loaded(
        &self,
        table_name: &str,
        partition: PartitionID,
        column_name: &str,
    ) -> bool {
        self.partitions[table_name][&partition].subpartition_has_been_loaded(column_name)
    }

    pub fn mark_subpartition_as_loaded(
        &mut self,
        table_name: &str,
        partition: PartitionID,
        column_name: &str,
    ) {
        self.partitions[table_name][&partition].mark_subpartition_as_loaded(column_name);
    }

    pub fn add_wal_segment(&mut self) -> u64 {
        let wal_id = self.next_wal_id;
        self.next_wal_id += 1;
        wal_id
    }

    pub fn insert_partition(&mut self, partition: PartitionMetadata) {
        self.partitions
            .entry(partition.tablename.clone())
            .or_default()
            .insert(partition.id, partition);
    }

    pub fn delete_partitions(
        &mut self,
        table: &str,
        old_partitions: &[PartitionID],
    ) -> Vec<(u64, String)> {
        let all_partitions = self.partitions.get_mut(table).unwrap();
        old_partitions
            .iter()
            .map(|id| all_partitions.remove(id).unwrap())
            .flat_map(|partition| {
                let id = partition.id;
                partition
                    .subpartitions
                    .iter()
                    .map(move |sb| (id, (*sb.subpartition_key).to_string()))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
    }

    pub fn serialize(&self, tracer: &mut SimpleTracer) -> Vec<u8> {
        let span_serialize = tracer.start_span("serialize_metastore");

        let mut builder = ::capnp::message::Builder::new_default();
        let mut dbmeta = builder.init_root::<dbmeta_capnp::d_b_meta::Builder>();
        dbmeta.set_next_wal_id(self.earliest_unflushed_wal_id);

        let total_partitions = self.partitions.values().map(|x| x.len()).sum::<usize>();
        assert!(total_partitions < u32::MAX as usize);
        let mut i = 0;

        // Serialize partitions
        let span_partition_serialization = tracer.start_span("partition_serialization");
        let mut partitions_builder = dbmeta.reborrow().init_partitions(total_partitions as u32);
        for table in self.partitions.values() {
            for partition in table.values() {
                let mut partition_builder = partitions_builder.reborrow().get(i);
                partition_builder.set_id(partition.id);
                partition_builder.set_tablename(&partition.tablename);
                partition_builder.set_offset(partition.offset as u64);
                partition_builder.set_len(partition.len as u64);

                assert!(partition.subpartitions.len() < u32::MAX as usize);
                let mut subpartitions_builder =
                    partition_builder.init_subpartitions(partition.subpartitions.len() as u32);
                for (i, subpartition) in partition.subpartitions.iter().enumerate() {
                    let mut subpartition_builder = subpartitions_builder.reborrow().get(i as u32);
                    subpartition_builder.set_size_bytes(subpartition.size_bytes);
                    subpartition_builder.set_subpartition_key(&subpartition.subpartition_key);
                    subpartition_builder.set_last_column(&subpartition.last_column);
                }
                i += 1;
            }
        }
        tracer.end_span(span_partition_serialization);

        // Write out the capnproto message
        let span_message_serialization = tracer.start_span("message_serialization");
        let mut buf = Vec::new();
        serialize_packed::write_message(&mut buf, &builder).unwrap();
        tracer.end_span(span_message_serialization);
        tracer.annotate("table_count", self.partitions.len());
        tracer.annotate("partition_count", total_partitions);
        tracer.annotate("unique_column_count", 0);
        tracer.annotate("column_count", -1);
        tracer.annotate("total_bytes", buf.len());
        tracer.annotate("column_names_bytes", 0);
        tracer.annotate("compressed_column_names_bytes", 0);
        tracer.annotate("column_name_lengths_bytes", 0);
        tracer.end_span(span_serialize);

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
            let mut subpartitions_by_last_column = BTreeMap::new();
            for subpartition in partition.get_subpartitions()?.iter() {
                let size_bytes = subpartition.get_size_bytes();
                let subpartition_key = subpartition.get_subpartition_key()?.to_string().unwrap();

                let mut last_column = "".to_string();
                // v0
                for column in subpartition.get_columns()? {
                    let column = column?.to_string().unwrap();
                    if column > last_column {
                        last_column = column;
                    }
                }
                // v1
                for column_string_id in subpartition.get_interned_columns()? {
                    let column = strings[column_string_id as usize].clone();
                    if column > last_column {
                        last_column = column;
                    }
                }
                // v2
                let compressed_interned_columns = subpartition.get_compressed_interned_columns()?;
                if !compressed_interned_columns.is_empty() {
                    let interned_columns =
                        simple_decompress::<u64>(compressed_interned_columns).unwrap();
                    for column_id in interned_columns {
                        let column = strings[column_id as usize].clone();
                        if column > last_column {
                            last_column = column;
                        }
                    }
                }
                // v3
                let explicit_last_column = subpartition.get_last_column()?.to_string().unwrap();
                if !explicit_last_column.is_empty() {
                    last_column = explicit_last_column;
                }

                subpartitions_by_last_column.insert(last_column.clone(), subpartitions.len());

                subpartitions.push(SubpartitionMetadata {
                    size_bytes,
                    subpartition_key,
                    last_column,
                    loaded: Arc::new(AtomicBool::new(false)),
                });
            }
            let partition = PartitionMetadata {
                id,
                tablename: tablename.clone(),
                offset,
                len,
                subpartitions,
                subpartitions_by_last_column,
            };
            partitions
                .entry(tablename)
                .or_default()
                .insert(id, partition);
        }

        Ok(MetaStore {
            next_wal_id,
            earliest_unflushed_wal_id: next_wal_id,
            partitions,
        })
    }
}
