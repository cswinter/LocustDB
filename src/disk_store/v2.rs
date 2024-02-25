use std::borrow::Cow;
use std::error::Error;
use std::fs::{create_dir_all, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use super::interface::{ColumnLoader, PartitionMetadata};
use crate::logging_client::EventBuffer;
use crate::mem_store::Column;
use crate::perf_counter::{PerfCounter, QueryPerfCounter};

#[derive(Serialize, Deserialize)]
pub struct WALSegment<'a> {
    pub id: u64,
    pub data: Cow<'a, EventBuffer>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct MetaStore {
    pub next_wal_id: u64,
    pub partitions: Vec<PartitionMetadata>,
}

type PartitionID = u64;

pub trait Storage: Send + Sync + 'static {
    /// Persists a WAL segment and returns the number of bytes written
    fn persist_wal_segment(&self, segment: WALSegment) -> u64;
    fn persist_partitions_delete_wal(
        &self,
        partitions: Vec<(PartitionMetadata, Vec<Vec<Arc<Column>>>)>,
    );
    fn meta_store(&self) -> &Mutex<MetaStore>;
    fn load_column(
        &self,
        partition: PartitionID,
        table_name: &str,
        column_name: &str,
        perf_counter: &QueryPerfCounter,
    ) -> Vec<Column>;
}

impl ColumnLoader for StorageV2 {
    fn load_column(
        &self,
        table_name: &str,
        partition: super::interface::PartitionID,
        column_name: &str,
        perf_counter: &QueryPerfCounter,
    ) -> Vec<Column> {
        Storage::load_column(self, partition, table_name, column_name, perf_counter)
    }

    fn load_column_range(
        &self,
        _start: super::interface::PartitionID,
        _end: super::interface::PartitionID,
        _column_name: &str,
        _ldb: &crate::scheduler::InnerLocustDB,
    ) {
        todo!()
    }
}

trait BlobWriter {
    fn store(&self, path: &Path, data: &[u8])
        -> Result<(), Box<dyn Error + Send + Sync + 'static>>;
    fn load(&self, path: &Path) -> Result<Vec<u8>, Box<dyn Error + Send + Sync + 'static>>;
    fn delete(&self, path: &Path) -> Result<(), Box<dyn Error + Send + Sync + 'static>>;
    fn rename(&self, src: &Path, dst: &Path) -> Result<(), Box<dyn Error + Send + Sync + 'static>>;
    /// Returns absolute paths of files in the directory
    fn list(&self, path: &Path) -> Result<Vec<PathBuf>, Box<dyn Error + Send + Sync + 'static>>;
    fn exists(&self, path: &Path) -> Result<bool, Box<dyn Error + Send + Sync + 'static>>;
}

pub struct StorageV2 {
    wal_dir: PathBuf,
    meta_db_path: PathBuf,
    new_meta_db_path: PathBuf,
    tables_path: PathBuf,
    meta_store: Arc<Mutex<MetaStore>>,
    writer: Box<dyn BlobWriter + Send + Sync + 'static>,
    perf_counter: Arc<PerfCounter>,
}

impl StorageV2 {
    pub fn new(
        path: &Path,
        perf_counter: Arc<PerfCounter>,
        readonly: bool,
    ) -> (StorageV2, Vec<WALSegment>) {
        let meta_db_path = path.join("meta");
        let new_meta_db_path = path.join("meta_new");
        let wal_dir = path.join("wal");
        let tables_path = path.join("tables");
        let writer = Box::new(FileBlobWriter::new());
        let (meta_store, wal_segments) = StorageV2::recover(
            &writer,
            &meta_db_path,
            &new_meta_db_path,
            &wal_dir,
            readonly,
            perf_counter.as_ref(),
        );
        let meta_store = Arc::new(Mutex::new(meta_store));
        (
            StorageV2 {
                wal_dir,
                meta_db_path,
                new_meta_db_path,
                tables_path,
                meta_store,
                writer,
                perf_counter,
            },
            wal_segments,
        )
    }

    fn recover<'a>(
        writer: &FileBlobWriter,
        mut meta_db_path: &'a Path,
        new_meta_db_path: &'a Path,
        wal_dir: &Path,
        readonly: bool,
        perf_counter: &PerfCounter,
    ) -> (MetaStore, Vec<WALSegment<'static>>) {
        // If db crashed during wal flush, might have new state at new_meta_db_path and potentially
        // old state at meta_db_path. If both exist, delete old state and rename new state to old.
        if writer.exists(new_meta_db_path).unwrap() {
            log::info!(
                "Found new unfinalized meta db at {}",
                new_meta_db_path.display()
            );
            if writer.exists(meta_db_path).unwrap() {
                log::info!("Found old meta db at {}, deleting", meta_db_path.display());
                if !readonly {
                    writer.delete(meta_db_path).unwrap();
                }
            }
            log::info!("Renaming new meta db to {}", meta_db_path.display());
            if readonly {
                meta_db_path = new_meta_db_path;
            } else {
                writer.rename(new_meta_db_path, meta_db_path).unwrap();
            }
        }

        let mut meta_store: MetaStore = if writer.exists(meta_db_path).unwrap() {
            let data = writer.load(meta_db_path).unwrap();
            perf_counter.disk_read_meta_store(data.len() as u64);
            bincode::deserialize(&data).unwrap()
        } else {
            MetaStore {
                next_wal_id: 0,
                partitions: Vec::new(),
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
                "Found wal segment {} with id {}",
                wal_file.display(),
                wal_segment.id
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
                meta_store.next_wal_id =
                    meta_store.next_wal_id.max(wal_segments.last().unwrap().id);
            }
        }

        (meta_store, wal_segments)
    }
}

impl Storage for StorageV2 {
    fn meta_store(&self) -> &Mutex<MetaStore> {
        &self.meta_store
    }

    fn persist_wal_segment(&self, mut segment: WALSegment) -> u64 {
        {
            let mut meta_store = self.meta_store.lock().unwrap();
            segment.id = meta_store.next_wal_id;
            meta_store.next_wal_id += 1;
        }
        let path = self.wal_dir.join(format!("{}.wal", segment.id));
        let data = bincode::serialize(&segment).unwrap();
        self.perf_counter.disk_write_wal(data.len() as u64);
        self.writer.store(&path, &data).unwrap();
        data.len() as u64
    }

    fn persist_partitions_delete_wal(
        &self,
        partitions: Vec<(PartitionMetadata, Vec<Vec<Arc<Column>>>)>,
    ) {
        // Lock meta store
        let mut meta_store = self.meta_store.lock().unwrap();

        // Write out new partition files
        for (partition, subpartition_cols) in partitions {
            for (metadata, cols) in partition.subpartitions.iter().zip(subpartition_cols) {
                // TODO: column names might not be valid for filesystem (too long, disallowed characters). use cleaned prefix + hashcode for column names that are long/have special chars?
                let table_dir = self.tables_path.join(&partition.tablename);
                let cols = cols.iter().map(|col| &**col).collect::<Vec<_>>();
                let data = bincode::serialize(&cols).unwrap();
                self.perf_counter
                    .new_partition_file_write(data.len() as u64);
                self.writer
                    .store(
                        &table_dir
                            .join(partition_filename(partition.id, &metadata.subpartition_key)),
                        &data,
                    )
                    .unwrap();
            }
            meta_store.partitions.push(partition);
        }

        // Atomically overwrite meta store file
        let data = bincode::serialize(&*meta_store).unwrap();
        self.perf_counter.disk_write_meta_store(data.len() as u64);
        self.writer.store(&self.new_meta_db_path, &data).unwrap();
        if self.writer.exists(&self.meta_db_path).unwrap() {
            self.writer.delete(&self.meta_db_path).unwrap();
        }
        self.writer
            .rename(&self.new_meta_db_path, &self.meta_db_path)
            .unwrap();

        // Delete WAL files
        for file in self.writer.list(&self.wal_dir).unwrap() {
            self.writer.delete(&file).unwrap();
        }
    }

    fn load_column(
        &self,
        partition: PartitionID,
        table_name: &str,
        column_name: &str,
        perf_counter: &QueryPerfCounter,
    ) -> Vec<Column> {
        // TODO: efficient access
        let subpartition_key = self
            .meta_store()
            .lock()
            .unwrap()
            .partitions
            .iter()
            .find(|p| p.id == partition && p.tablename == table_name)
            .unwrap()
            .subpartitions
            .iter()
            .find(|sp| sp.column_names.contains(&column_name.to_string()))
            .unwrap()
            .subpartition_key
            .clone();
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

struct FileBlobWriter;

impl FileBlobWriter {
    fn new() -> FileBlobWriter {
        FileBlobWriter
    }
}

impl BlobWriter for FileBlobWriter {
    fn store(
        &self,
        path: &Path,
        data: &[u8],
    ) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        // Create the directory if it doesn't exist
        if let Some(parent) = path.parent() {
            create_dir_all(parent)?;
        }

        // Write the data to the file
        let mut file = File::create(path)?;
        file.write_all(data)?;

        Ok(())
    }

    fn load(&self, path: &Path) -> Result<Vec<u8>, Box<dyn Error + Send + Sync + 'static>> {
        let mut file = File::open(path)?;
        let mut data = Vec::new();
        file.read_to_end(&mut data)?;
        Ok(data)
    }

    fn delete(&self, path: &Path) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        std::fs::remove_file(path)?;
        Ok(())
    }

    fn rename(&self, src: &Path, dst: &Path) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        std::fs::rename(src, dst)?;
        Ok(())
    }

    fn list(&self, path: &Path) -> Result<Vec<PathBuf>, Box<dyn Error + Send + Sync + 'static>> {
        let mut entries = Vec::new();
        match path.read_dir() {
            Ok(paths) => {
                for entry in paths {
                    let entry = entry?;
                    let path = entry.path();
                    if path.is_file() {
                        entries.push(path);
                    }
                }
                Ok(entries)
            }
            Err(err) => {
                if err.kind() == std::io::ErrorKind::NotFound {
                    Ok(entries)
                } else {
                    Err(Box::new(err))
                }
            }
        }
    }

    fn exists(&self, path: &Path) -> Result<bool, Box<dyn Error + Send + Sync + 'static>> {
        Ok(path.exists())
    }
}

fn partition_filename(id: PartitionID, subpartition_key: &str) -> String {
    format!("{:05}_{}.part", id, subpartition_key)
}
