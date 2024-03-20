use std::borrow::Cow;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};

use super::file_writer::{BlobWriter, FileBlobWriter};
use super::{ColumnLoader, PartitionMetadata, SubpartitionMetadata};
use crate::logging_client::EventBuffer;
use crate::mem_store::{Column, DataSource};
use crate::perf_counter::{PerfCounter, QueryPerfCounter};

#[derive(Serialize, Deserialize)]
pub struct WALSegment<'a> {
    pub id: u64,
    pub data: Cow<'a, EventBuffer>,
}

type TableName = String;

#[derive(Serialize, Deserialize, Clone)]
pub struct MetaStore {
    pub next_wal_id: u64,
    pub partitions: HashMap<TableName, HashMap<PartitionID, PartitionMetadata>>,
}

type PartitionID = u64;

impl ColumnLoader for Storage {
    fn load_column(
        &self,
        table_name: &str,
        partition: PartitionID,
        column_name: &str,
        perf_counter: &QueryPerfCounter,
    ) -> Vec<Column> {
        Storage::load_column(self, partition, table_name, column_name, perf_counter)
    }

    fn load_column_range(
        &self,
        _start: PartitionID,
        _end: PartitionID,
        _column_name: &str,
        _ldb: &crate::scheduler::InnerLocustDB,
    ) {
        todo!()
    }
}

pub struct Storage {
    wal_dir: PathBuf,
    meta_db_path: PathBuf,
    tables_path: PathBuf,
    meta_store: Arc<RwLock<MetaStore>>,
    writer: Box<dyn BlobWriter + Send + Sync + 'static>,
    perf_counter: Arc<PerfCounter>,
}

impl Storage {
    pub fn new(
        path: &Path,
        perf_counter: Arc<PerfCounter>,
        readonly: bool,
    ) -> (Storage, Vec<WALSegment>) {
        let meta_db_path = path.join("meta");
        let wal_dir = path.join("wal");
        let tables_path = path.join("tables");
        let writer = Box::new(FileBlobWriter::new());
        let (meta_store, wal_segments) = Storage::recover(
            &writer,
            &meta_db_path,
            &wal_dir,
            readonly,
            perf_counter.as_ref(),
        );
        let meta_store = Arc::new(RwLock::new(meta_store));
        (
            Storage {
                wal_dir,
                meta_db_path,
                tables_path,
                meta_store,
                writer,
                perf_counter,
            },
            wal_segments,
        )
    }

    fn recover(
        writer: &FileBlobWriter,
        meta_db_path: &Path,
        wal_dir: &Path,
        readonly: bool,
        perf_counter: &PerfCounter,
    ) -> (MetaStore, Vec<WALSegment<'static>>) {
        let mut meta_store: MetaStore = if writer.exists(meta_db_path).unwrap() {
            let data = writer.load(meta_db_path).unwrap();
            perf_counter.disk_read_meta_store(data.len() as u64);
            bincode::deserialize(&data).unwrap()
        } else {
            MetaStore {
                next_wal_id: 0,
                partitions: HashMap::new(),
            }
        };

        let mut wal_segments = Vec::new();
        let next_wal_id = meta_store.next_wal_id;
        log::info!("Recovering from wal checkpoint {}", next_wal_id);
        for wal_file in writer.list(wal_dir).unwrap() {
            let wal_data = writer.load(&wal_file).unwrap();
            perf_counter.disk_read_wal(wal_data.len() as u64);
            let wal_segment: WALSegment = bincode::deserialize(&wal_data).unwrap();
            log::info!(
                "Found wal segment {} with id {} and {} rows in {} tables",
                wal_file.display(),
                wal_segment.id,
                wal_segment.data.tables.values().map(|t| t.len).sum::<u64>(),
                wal_segment.data.tables.len(),
            );
            if wal_segment.id < next_wal_id {
                if !readonly {
                    writer.delete(&wal_file).unwrap();
                    log::info!("Deleting wal segment {}", wal_file.display());
                } else {
                    log::info!("Skipping wal segment {}", wal_file.display());
                }
            } else {
                wal_segments.push(wal_segment);
                meta_store.next_wal_id = meta_store
                    .next_wal_id
                    .max(wal_segments.last().unwrap().id + 1);
            }
        }

        wal_segments.sort_by_key(|s| s.id);

        (meta_store, wal_segments)
    }

    fn write_metastore(&self, meta_store: &MetaStore) {
        let data = bincode::serialize(meta_store).unwrap();
        self.perf_counter.disk_write_meta_store(data.len() as u64);
        self.writer.store(&self.meta_db_path, &data).unwrap();
    }

    fn write_subpartitions(
        &self,
        partition: &PartitionMetadata,
        subpartition_cols: Vec<Vec<Arc<Column>>>,
    ) {
        for (metadata, cols) in partition.subpartitions.iter().zip(subpartition_cols) {
            let table_dir = self.tables_path.join(&partition.tablename);
            let cols = cols.iter().map(|col| &**col).collect::<Vec<_>>();
            let data = bincode::serialize(&cols).unwrap();
            self.perf_counter
                .new_partition_file_write(data.len() as u64);
            self.writer
                .store(
                    &table_dir.join(partition_filename(partition.id, &metadata.subpartition_key)),
                    &data,
                )
                .unwrap();
        }
    }

    pub fn meta_store(&self) -> &RwLock<MetaStore> {
        &self.meta_store
    }

    pub fn persist_wal_segment(&self, mut segment: WALSegment) -> u64 {
        {
            let mut meta_store = self.meta_store.write().unwrap();
            segment.id = meta_store.next_wal_id;
            meta_store.next_wal_id += 1;
        }
        let path = self.wal_dir.join(format!("{}.wal", segment.id));
        let data = bincode::serialize(&segment).unwrap();
        self.perf_counter.disk_write_wal(data.len() as u64);
        self.writer.store(&path, &data).unwrap();
        data.len() as u64
    }

    pub fn persist_partitions_delete_wal(
        &self,
        partitions: Vec<(PartitionMetadata, Vec<Vec<Arc<Column>>>)>,
    ) {
        // Lock meta store
        let mut meta_store = self.meta_store.write().unwrap();

        // Write out new partition files
        for (partition, subpartition_cols) in partitions {
            self.write_subpartitions(&partition, subpartition_cols);
            meta_store
                .partitions
                .entry(partition.tablename.clone())
                .or_default()
                .insert(partition.id, partition);
        }

        // Atomically overwrite meta store file
        self.write_metastore(&meta_store);

        // Delete WAL files
        for file in self.writer.list(&self.wal_dir).unwrap() {
            self.writer.delete(&file).unwrap();
        }
    }

    // Combine set of partitions into single new partition.
    pub fn compact(
        &self,
        table: &str,
        id: PartitionID,
        metadata: Vec<SubpartitionMetadata>,
        subpartitions: Vec<Vec<Arc<Column>>>,
        old_partitions: &[PartitionID],
        offset: usize,
    ) {
        log::debug!(
            "compacting {} parititions into {} for table {}",
            old_partitions.len(),
            id,
            table
        );

        let column_name_to_subpartition_index = subpartitions
            .iter()
            .enumerate()
            .flat_map(|(i, cols)| {
                cols.iter()
                    .map(|col| col.name().to_string())
                    .map(move |name| (name, i))
            })
            .collect();
        // Persist new partition files
        let partition = PartitionMetadata {
            id,
            tablename: table.to_string(),
            len: subpartitions[0][0].len(),
            offset,
            subpartitions: metadata,
            column_name_to_subpartition_index,
        };
        self.write_subpartitions(&partition, subpartitions);

        // Atomically update metastore
        let mut meta_store = self.meta_store.write().unwrap();
        let all_partitions = meta_store.partitions.get_mut(table).unwrap();
        let to_delete: Vec<(u64, String)> = old_partitions
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
            .collect::<Vec<_>>();
        meta_store
            .partitions
            .get_mut(table)
            .unwrap()
            .insert(partition.id, partition);
        drop(meta_store);
        let meta_store = self.meta_store.read().unwrap();
        self.write_metastore(&meta_store);

        // Delete old partition files
        let table_dir = self.tables_path.join(table);
        for (id, key) in to_delete {
            let path = table_dir.join(partition_filename(id, &key));
            self.writer.delete(&path).unwrap();
        }
    }

    pub fn load_column(
        &self,
        partition: PartitionID,
        table_name: &str,
        column_name: &str,
        perf_counter: &QueryPerfCounter,
    ) -> Vec<Column> {
        let subpartition_key = self.meta_store.read().unwrap().partitions[table_name][&partition]
            .subpartition_key(column_name);
        let path = self
            .tables_path
            .join(table_name)
            .join(partition_filename(partition, &subpartition_key));
        let data = self.writer.load(&path).unwrap();
        self.perf_counter.disk_read_partition(data.len() as u64);
        perf_counter.disk_read(data.len() as u64);
        bincode::deserialize(&data).unwrap()
    }
}

fn partition_filename(id: PartitionID, subpartition_key: &str) -> String {
    format!("{:05}_{}.part", id, subpartition_key)
}
