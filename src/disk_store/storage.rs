use std::collections::BTreeMap;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc, RwLock};

use threadpool::ThreadPool;

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
    ) -> Option<Vec<Column>> {
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

    fn partition_has_been_loaded(&self, table: &str, partition: PartitionID, column: &str) -> bool {
        Storage::partition_has_been_loaded(self, table, partition, column)
    }

    fn mark_subpartition_as_loaded(&self, table: &str, partition: PartitionID, column: &str) {
        Storage::mark_subpartition_as_loaded(self, table, partition, column);
    }
}

pub struct Storage {
    wal_dir: PathBuf,
    meta_db_path: PathBuf,
    tables_path: PathBuf,
    meta_store: Arc<RwLock<MetaStore>>,
    writer: Arc<dyn BlobWriter + Send + Sync + 'static>,
    perf_counter: Arc<PerfCounter>,

    io_threadpool: Option<ThreadPool>,
}

impl Storage {
    pub fn new(
        path: &Path,
        perf_counter: Arc<PerfCounter>,
        readonly: bool,
        io_threads: usize,
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
        let writer = Arc::new(VersionedChecksummedBlobWriter::new(writer));
        let meta_db_path = path.join("meta");
        let wal_dir = path.join("wal");
        let tables_path = path.join("tables");
        let (meta_store, wal_segments, wal_size) = Storage::recover(
            writer.clone(),
            &meta_db_path,
            &wal_dir,
            readonly,
            perf_counter.clone(),
            io_threads,
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
                io_threadpool: if io_threads > 1 {
                    Some(ThreadPool::new(io_threads))
                } else {
                    None
                },
            },
            wal_segments,
            wal_size,
        )
    }

    fn recover(
        writer: Arc<dyn BlobWriter>,
        meta_db_path: &Path,
        wal_dir: &Path,
        readonly: bool,
        perf_counter: Arc<PerfCounter>,
        io_threads: usize,
    ) -> (MetaStore, Vec<WalSegment<'static>>, u64) {
        let mut meta_store: MetaStore = if writer.exists(meta_db_path).unwrap() {
            let data = writer.load(meta_db_path).unwrap();
            perf_counter.disk_read_meta_store(data.len() as u64);
            MetaStore::deserialize(&data).unwrap()
        } else {
            MetaStore::default()
        };

        let earliest_uncommited_wal_id = meta_store.earliest_uncommited_wal_id();
        log::info!(
            "Recovering from wal checkpoint {}",
            earliest_uncommited_wal_id
        );
        let wal_files = writer.list(wal_dir).unwrap();
        let num_wal_files = wal_files.len();
        log::info!("Found {} wal segments", wal_files.len());

        let threadpool = ThreadPool::new(io_threads.min(wal_files.len()));
        let (tx, rx) = mpsc::channel();
        for wal_file in wal_files {
            let tx = tx.clone();
            let writer = writer.clone();
            let perf_counter = perf_counter.clone();
            threadpool.execute(move || {
                let wal_data = writer.load(&wal_file).unwrap();
                perf_counter.disk_read_wal(wal_data.len() as u64);
                let wal_segment = WalSegment::deserialize(&wal_data).unwrap();
                log::info!(
                    "Found wal segment {} with id {} and {} rows in {} tables",
                    wal_file.display(),
                    wal_segment.id,
                    wal_segment
                        .data
                        .tables
                        .values()
                        .map(|t| t.len())
                        .sum::<usize>(),
                    wal_segment.data.tables.len(),
                );
                tx.send((wal_file, wal_segment, wal_data.len() as u64)).unwrap();
            });
        }

        let mut wal_size = 0;
        let mut wal_segments = Vec::new();
        for (path, wal_segment, size) in rx.iter().take(num_wal_files) {
                if wal_segment.id < earliest_uncommited_wal_id {
                    if readonly {
                        log::info!("Skipping wal segment {}", path.display());
                    } else {
                        writer.delete(&path).unwrap();
                        log::info!("Deleting wal segment {}", path.display());
                    }
                } else {
                    meta_store.register_wal_segment(wal_segment.id);
            wal_size += size;
            wal_segments.push(wal_segment);
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
            segment.id = meta_store.add_wal_segment();
        }
        let path = self.wal_dir.join(format!("{}.wal", segment.id));
        let data = segment.serialize();
        self.perf_counter.disk_write_wal(data.len() as u64);
        self.writer.store(&path, &data).unwrap();
        data.len() as u64
    }

    pub fn unflushed_wal_ids(&self) -> Range<u64> {
        self.meta_store.read().unwrap().unflushed_wal_ids()
    }

    pub fn persist_partitions(
        self: &Arc<Storage>,
        partitions: Vec<(PartitionMetadata, Vec<Vec<Arc<Column>>>)>,
        tracer: &mut SimpleTracer,
    ) {
        let span_persist_partitions = tracer.start_span("persist_partitions");
        let mut partition_count = 0;
        let mut partition_bytes = 0;
        if let Some(io_threadpool) = &self.io_threadpool {
            let (tx, rx) = mpsc::channel();
            let span_spawn_tasks = tracer.start_span("spawn_tasks");
            for (partition, subpartitions) in partitions {
                partition_count += 1;
                partition_bytes += subpartitions
                    .iter()
                    .flat_map(|s| s.iter())
                    .map(|c| c.heap_size_of_children())
                    .sum::<usize>();
                let tx = tx.clone();
                let storage = self.clone();
                io_threadpool.execute(move || {
                    storage.write_subpartitions(&partition, subpartitions, false);
                    let mut meta_store = storage.meta_store.write().unwrap();
                    meta_store.insert_partition(partition);
                    tx.send(()).unwrap();
                });
            }
            tracer.end_span(span_spawn_tasks);

            let span_wait_for_tasks = tracer.start_span("wait_for_tasks");
            for _ in rx.iter().take(partition_count) {
                // Wait for all partitions to be persisted
            }
            tracer.end_span(span_wait_for_tasks);
        } else {
            // Write out new partition files
            for (partition, subpartition_cols) in partitions {
                let span_write_subpartitions = tracer.start_span("write_subpartitions");
                self.write_subpartitions(&partition, subpartition_cols, false);
                tracer.end_span(span_write_subpartitions);

                let span_lock_meta_store = tracer.start_span("lock_meta_store");
                let mut meta_store = self.meta_store.write().unwrap();
                meta_store.insert_partition(partition);
                tracer.end_span(span_lock_meta_store);
            }
        }

        tracer.annotate("partition_count", partition_count);
        tracer.annotate("partition_bytes", partition_bytes);
        tracer.end_span(span_persist_partitions);
    }

    pub fn persist_partition(
        &self,
        partition: PartitionMetadata,
        subpartition_cols: Vec<Vec<Arc<Column>>>,
    ) {
        self.write_subpartitions(&partition, subpartition_cols, false);
        let mut meta_store = self.meta_store.write().unwrap();
        meta_store.insert_partition(partition);
    }

    /// Delete WAL segments with ids in the given range.
    pub fn delete_wal_segments(self: &Arc<Storage>, ids: Range<u64>, tracer: &mut SimpleTracer) {
        let span_delete_wal_segments = tracer.start_span("delete_wal_segments");

        if let Some(io_threadpool) = &self.io_threadpool {
            let count = ids.end - ids.start;
            let (tx, rx) = mpsc::channel();
            for id in ids {
                let tx = tx.clone();
                let storage = self.clone();
                io_threadpool.execute(move || {
                    let path = storage.wal_dir.join(format!("{}.wal", id));
                    storage.writer.delete(&path).unwrap();
                    tx.send(()).unwrap();
                });
            }
            for _ in rx.iter().take(count as usize) {
                // Wait for all WAL segments to be deleted
            }
        } else {
            for id in ids {
                let path = self.wal_dir.join(format!("{}.wal", id));
                self.writer.delete(&path).unwrap();
            }
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
            "compacting {} partitions into {} for table {}",
            old_partitions.len(),
            id,
            table
        );

        let mut subpartitions_by_last_column = BTreeMap::new();
        for (i, subpartition) in metadata.iter().enumerate() {
            subpartitions_by_last_column.insert(subpartition.last_column.clone(), i);
        }

        // Persist new partition files
        let partition = PartitionMetadata {
            id,
            tablename: table.to_string(),
            len: subpartitions[0][0].len(),
            offset,
            subpartitions: metadata,
            subpartitions_by_last_column,
        };
        self.write_subpartitions(&partition, subpartitions, true);

        // Update metastore
        let mut meta_store = self.meta_store.write().unwrap();
        let to_delete = meta_store.delete_partitions(table, old_partitions);
        meta_store.insert_partition(partition);

        to_delete
    }

    pub fn delete_orphaned_partitions(
        self: &Arc<Storage>,
        to_delete: Vec<(String, Vec<(u64, String)>)>,
        tracer: &mut SimpleTracer,
    ) {
        // Delete old partition files
        let span_delete_orphaned_partitions = tracer.start_span("delete_orphaned_partitions");
        let mut table_count = 0;
        let mut partition_count = 0;

        if let Some(io_threadpool) = &self.io_threadpool {
            let (tx, rx) = mpsc::channel();
            let span_spawn_tasks = tracer.start_span("spawn_tasks");
            for (table, to_delete) in to_delete {
                table_count += 1;
                for (id, key) in to_delete {
                    partition_count += 1;
                    let tx = tx.clone();
                    let storage = self.clone();
                    let table = table.clone();
                    io_threadpool.execute(move || {
                        let table_dir = storage.tables_path.join(sanitize_table_name(&table));
                        let path = table_dir.join(partition_filename(id, &key));
                        storage.writer.delete(&path).unwrap();
                        tx.send(()).unwrap();
                    });
                }
            }
            tracer.end_span(span_spawn_tasks);

            let span_wait_for_tasks = tracer.start_span("wait_for_tasks");
            for _ in rx.iter().take(partition_count) {
                // Wait for all partitions to be deleted
            }
            tracer.end_span(span_wait_for_tasks);
        } else {
            for (table, to_delete) in &to_delete {
                table_count += 1;
                for (id, key) in to_delete {
                    partition_count += 1;
                    let table_dir = self.tables_path.join(sanitize_table_name(table));
                    let path = table_dir.join(partition_filename(*id, key));
                    self.writer.delete(&path).unwrap();
                }
            }
        }
        tracer.annotate("table_count", table_count);
        tracer.annotate("partition_count", partition_count);
        tracer.end_span(span_delete_orphaned_partitions);
    }

    pub fn persist_metastore(&self, earliest_uncommited_wal_id: u64, tracer: &mut SimpleTracer) {
        let span_persist_metastore = tracer.start_span("persist_metastore");
        let span_clone_meta_store = tracer.start_span("clone_meta_store");
        {
            self.meta_store
                .write()
                .unwrap()
                .advance_earliest_unflushed_wal_id(earliest_uncommited_wal_id);
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
    ) -> Option<Vec<Column>> {
        let subpartition_key =
            self.meta_store
                .read()
                .unwrap()
                .subpartition_key(table_name, partition, column_name)?;
        let path = self
            .tables_path
            .join(sanitize_table_name(table_name))
            .join(partition_filename(partition, &subpartition_key));
        let data = self.writer.load(&path).unwrap();
        self.perf_counter.disk_read_partition(data.len() as u64);
        perf_counter.disk_read(data.len() as u64);
        Some(PartitionSegment::deserialize(&data).unwrap().columns)
    }

    /// Checks whether a subpartition has been loaded before.
    /// This implies that existing columns have handles, and columns without handles do not exist.
    pub fn partition_has_been_loaded(
        &self,
        table: &str,
        partition: PartitionID,
        column: &str,
    ) -> bool {
        self.meta_store
            .read()
            .unwrap()
            .subpartition_has_been_loaded(table, partition, column)
    }

    pub fn mark_subpartition_as_loaded(&self, table: &str, partition: PartitionID, column: &str) {
        self.meta_store
            .write()
            .unwrap()
            .mark_subpartition_as_loaded(table, partition, column);
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
