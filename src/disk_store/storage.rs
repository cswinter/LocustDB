use std::collections::HashMap;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use super::azure_writer::AzureBlobWriter;
use super::file_writer::{BlobWriter, FileBlobWriter, VersionedChecksummedBlobWriter};
use super::gcs_writer::GCSBlobWriter;
use super::meta_store::{MetaStore, PartitionMetadata, SubpartitionMetadata};
use super::partition_segment::PartitionSegment;
use super::wal_segment::WalSegment;
use super::{ColumnLoader, PartitionID};
use crate::mem_store::{Column, DataSource};
use crate::observability::{PerfCounter, QueryPerfCounter, SimpleTracer};

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
    ) -> (Storage, Vec<WalSegment<'static>>, u64) {
        let (writer, path): (Box<dyn BlobWriter + Send + Sync + 'static>, PathBuf) =
            if path.starts_with("gs://") {
                let components = path.components().collect::<Vec<_>>();
                if components.len() < 2 {
                    panic!("Invalid GCS path: {:?}", path);
                }
                let bucket = components[1]
                    .as_os_str()
                    .to_str()
                    .expect("Invalid GCS path");
                // create new path that omits the first two components
                let path = components[2..]
                    .iter()
                    .map(|c| c.as_os_str())
                    .collect::<PathBuf>();
                (
                    Box::new(GCSBlobWriter::new(bucket.to_string()).unwrap()),
                    path,
                )
            } else if path.starts_with("az://") {
                let components = path.components().collect::<Vec<_>>();
                if components.len() < 3 {
                    panic!("Invalid Azure path: {:?}", path);
                }
                let account = components[1]
                    .as_os_str()
                    .to_str()
                    .expect("Invalid Azure path");
                let container = components[2]
                    .as_os_str()
                    .to_str()
                    .expect("Invalid Azure path");
                // create new path that omits the first three components
                let path = components[3..]
                    .iter()
                    .map(|c| c.as_os_str())
                    .collect::<PathBuf>();
                (
                    Box::new(AzureBlobWriter::new(account, container).unwrap()),
                    path,
                )
            } else {
                (Box::new(FileBlobWriter::new()), path.to_owned())
            };
        let writer = Box::new(VersionedChecksummedBlobWriter::new(writer));
        let meta_db_path = path.join("meta");
        let wal_dir = path.join("wal");
        let tables_path = path.join("tables");
        let (meta_store, wal_segments, wal_size) = Storage::recover(
            writer.as_ref(),
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
            wal_size,
        )
    }

    fn recover(
        writer: &dyn BlobWriter,
        meta_db_path: &Path,
        wal_dir: &Path,
        readonly: bool,
        perf_counter: &PerfCounter,
    ) -> (MetaStore, Vec<WalSegment<'static>>, u64) {
        let mut meta_store: MetaStore = if writer.exists(meta_db_path).unwrap() {
            let data = writer.load(meta_db_path).unwrap();
            perf_counter.disk_read_meta_store(data.len() as u64);
            MetaStore::deserialize(&data).unwrap()
        } else {
            MetaStore {
                next_wal_id: 0,
                earliest_uncommited_wal_id: 0,
                partitions: HashMap::new(),
            }
        };

        let mut wal_segments = Vec::new();
        let earliest_uncommited_wal_id = meta_store.earliest_uncommited_wal_id;
        log::info!(
            "Recovering from wal checkpoint {}",
            earliest_uncommited_wal_id
        );
        let wal_files = writer.list(wal_dir).unwrap();
        log::info!("Found {} wal segments", wal_files.len());
        let mut wal_size = 0;
        for wal_file in wal_files {
            let wal_data = writer.load(&wal_file).unwrap();
            perf_counter.disk_read_wal(wal_data.len() as u64);
            let wal_segment = WalSegment::deserialize(&wal_data).unwrap();
            log::info!(
                "Found wal segment {} with id {} and {} rows in {} tables",
                wal_file.display(),
                wal_segment.id,
                wal_segment.data.tables.values().map(|t| t.len).sum::<u64>(),
                wal_segment.data.tables.len(),
            );
            if wal_segment.id < earliest_uncommited_wal_id {
                if readonly {
                    log::info!("Skipping wal segment {}", wal_file.display());
                } else {
                    writer.delete(&wal_file).unwrap();
                    log::info!("Deleting wal segment {}", wal_file.display());
                }
            } else {
                wal_segments.push(wal_segment);
                meta_store.next_wal_id = meta_store
                    .next_wal_id
                    .max(wal_segments.last().unwrap().id + 1);
                wal_size += wal_data.len() as u64;
            }
        }

        wal_segments.sort_by_key(|s| s.id);

        (meta_store, wal_segments, wal_size)
    }

    fn write_metastore(&self, meta_store: &MetaStore, tracer: &mut SimpleTracer) {
        let span_write_metastore = tracer.start_span("write_metastore");
        let data = meta_store.serialize(tracer);
        self.perf_counter.disk_write_meta_store(data.len() as u64);
        self.writer.store(&self.meta_db_path, &data).unwrap();
        tracer.end_span(span_write_metastore);
    }

    fn write_subpartitions(
        &self,
        partition: &PartitionMetadata,
        subpartition_cols: Vec<Vec<Arc<Column>>>,
        is_compaction: bool,
    ) {
        for (metadata, cols) in partition.subpartitions.iter().zip(subpartition_cols) {
            let table_dir = self
                .tables_path
                .join(sanitize_table_name(&partition.tablename));
            let cols = cols.iter().map(|col| &**col).collect::<Vec<_>>();
            let data = PartitionSegment::serialize(&cols[..]);
            if is_compaction {
                self.perf_counter.disk_write_compaction(data.len() as u64);
            } else {
                self.perf_counter
                    .new_partition_file_write(data.len() as u64);
            };
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

    pub fn persist_wal_segment(&self, mut segment: WalSegment) -> u64 {
        {
            let mut meta_store = self.meta_store.write().unwrap();
            segment.id = meta_store.next_wal_id;
            meta_store.next_wal_id += 1;
        }
        let path = self.wal_dir.join(format!("{}.wal", segment.id));
        let data = segment.serialize();
        self.perf_counter.disk_write_wal(data.len() as u64);
        self.writer.store(&path, &data).unwrap();
        data.len() as u64
    }

    pub fn uncommited_wal_ids(&self) -> Range<u64> {
        let meta_store = self.meta_store.read().unwrap();
        meta_store.earliest_uncommited_wal_id..meta_store.next_wal_id
    }

    pub fn persist_partitions(
        &self,
        partitions: Vec<(PartitionMetadata, Vec<Vec<Arc<Column>>>)>,
        tracer: &mut SimpleTracer,
    ) {
        let span_persist_partitions = tracer.start_span("persist_partitions");

        // Write out new partition files
        for (partition, subpartition_cols) in partitions {
            let span_write_subpartitions = tracer.start_span("write_subpartitions");
            self.write_subpartitions(&partition, subpartition_cols, false);
            tracer.end_span(span_write_subpartitions);

            let span_lock_meta_store = tracer.start_span("lock_meta_store");
            let mut meta_store = self.meta_store.write().unwrap();
            meta_store
                .partitions
                .entry(partition.tablename.clone())
                .or_default()
                .insert(partition.id, partition);
            tracer.end_span(span_lock_meta_store);
        }

        tracer.end_span(span_persist_partitions);
    }

    /// Delete WAL segments with ids in the given range.
    pub fn delete_wal_segments(&self, ids: Range<u64>, tracer: &mut SimpleTracer) {
        let span_delete_wal_segments = tracer.start_span("delete_wal_segments");
        for id in ids {
            let path = self.wal_dir.join(format!("{}.wal", id));
            self.writer.delete(&path).unwrap();
        }
        tracer.end_span(span_delete_wal_segments);
    }

    // Combine set of partitions into single new partition.
    pub fn prepare_compact(
        &self,
        table: &str,
        id: PartitionID,
        metadata: Vec<SubpartitionMetadata>,
        subpartitions: Vec<Vec<Arc<Column>>>,
        old_partitions: &[PartitionID],
        offset: usize,
    ) -> Vec<(u64, String)> {
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
        self.write_subpartitions(&partition, subpartitions, true);

        // Update metastore
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

        to_delete
    }

    pub fn delete_orphaned_partitions(
        &self,
        to_delete: Vec<(String, Vec<(u64, String)>)>,
        tracer: &mut SimpleTracer,
    ) {
        // Delete old partition files
        let span_delete_orphaned_partitions = tracer.start_span("delete_orphaned_partitions");
        for (table, to_delete) in &to_delete {
            for (id, key) in to_delete {
                let table_dir = self.tables_path.join(sanitize_table_name(table));
                let path = table_dir.join(partition_filename(*id, key));
                self.writer.delete(&path).unwrap();
            }
        }
        tracer.end_span(span_delete_orphaned_partitions);
    }

    pub fn persist_metastore(&self, earliest_uncommited_wal_id: u64, tracer: &mut SimpleTracer) {
        let span_persist_metastore = tracer.start_span("persist_metastore");
        let span_clone_meta_store = tracer.start_span("clone_meta_store");
        {
            self.meta_store.write().unwrap().earliest_uncommited_wal_id =
                earliest_uncommited_wal_id;
        }
        let meta_store = { self.meta_store.read().unwrap().clone() };
        tracer.end_span(span_clone_meta_store);

        self.write_metastore(&meta_store, tracer);
        tracer.end_span(span_persist_metastore);
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
            .join(sanitize_table_name(table_name))
            .join(partition_filename(partition, &subpartition_key));
        let data = self.writer.load(&path).unwrap();
        self.perf_counter.disk_read_partition(data.len() as u64);
        perf_counter.disk_read(data.len() as u64);
        PartitionSegment::deserialize(&data).unwrap().columns
    }
}

fn partition_filename(id: PartitionID, subpartition_key: &str) -> String {
    format!("{:05}_{}.part", id, subpartition_key)
}

/// Sanitize table name to ensure valid file name:
/// - converts to lowercase
/// - removes any characters that are not alphanumeric, underscore, hyphen, or dot
/// - strip leading dashes and dots
/// - truncates to max 255-64-2=189 characters
/// - if name was modified, prepend underscore and append hash of original name
fn sanitize_table_name(table_name: &str) -> String {
    let mut name = table_name.to_lowercase();
    name.retain(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.');
    name = name.trim_start_matches(['-', '.']).to_string();
    if name.len() > 189 {
        name = name[..189].to_string();
    }
    if name != table_name {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(table_name.as_bytes());
        name = format!("-{}-{:x}", name, hasher.finalize());
    }
    name
}
