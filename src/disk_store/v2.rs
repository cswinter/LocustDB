use std::error::Error;
use std::fs::{create_dir_all, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use super::interface::PartitionMetadata;
use crate::ingest::raw_val::RawVal;
use crate::mem_store::Column;

type Row = Vec<(String, RawVal)>;

#[derive(Serialize, Deserialize)]
pub struct WALSegment {
    pub id: u64,
    // TODO: inefficient format
    // [(TableName, [Rows])]
    pub data: Vec<(String, Vec<Row>)>,
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
    fn persist_partitions_delete_wal(&self, partitions: Vec<(PartitionMetadata, Vec<Arc<Column>>)>);
    fn meta_store(&self) -> &Mutex<MetaStore>;
    fn load_column(&self, partition: PartitionID, table_name: &str, column_name: &str) -> Column;
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
}

impl StorageV2 {
    pub fn new(path: &Path, readonly: bool) -> (StorageV2, Vec<WALSegment>) {
        let meta_db_path = path.join("meta");
        let new_meta_db_path = path.join("meta_new");
        let wal_dir = path.join("wal");
        let tables_path = path.join("tables");
        let writer = Box::new(FileBlobWriter::new());
        let (meta_store, wal_segments) = StorageV2::recover(&writer, &meta_db_path, &new_meta_db_path, &wal_dir, readonly);
        let meta_store = Arc::new(Mutex::new(meta_store));
        (StorageV2 {
            wal_dir,
            meta_db_path,
            new_meta_db_path,
            tables_path,
            meta_store,
            writer,
        }, wal_segments)
    }

    fn recover<'a>(writer: &FileBlobWriter, mut meta_db_path: &'a Path, new_meta_db_path: &'a Path, wal_dir: &Path, readonly: bool) -> (MetaStore, Vec<WALSegment>) {
        // If db crashed during wal flush, might have new state at new_meta_db_path and potentially
        // old state at meta_db_path. If both exist, delete old state and rename new state to old.
        if writer.exists(new_meta_db_path).unwrap() {
            log::info!("Found new unfinalized meta db at {}", new_meta_db_path.display());
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
                writer
                    .rename(new_meta_db_path, meta_db_path)
                    .unwrap();
            }
        }

        let mut meta_store: MetaStore = if writer.exists(meta_db_path).unwrap() {
            let data = writer.load(meta_db_path).unwrap();
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
        self.writer.store(&path, &data).unwrap();
        data.len() as u64
    }

    fn persist_partitions_delete_wal(
        &self,
        partitions: Vec<(PartitionMetadata, Vec<Arc<Column>>)>,
    ) {
        let mut meta_store = self.meta_store.lock().unwrap();
        for (metadata, cols) in partitions {
            for col in cols {
                // TODO: column names might not be valid for filesystem (too long, disallowed characters). use cleaned prefix + hashcode for column names that are long/have special chars?
                let table_dir = self.tables_path.join(&metadata.tablename);
                self.writer
                    .store(
                        &table_dir.join(format!("{}_{}.part", metadata.id, col.name())),
                        &bincode::serialize(&*col).unwrap(),
                    )
                    .unwrap();
            }
            meta_store.partitions.push(metadata);
        }
        self.writer
            .store(
                &self.new_meta_db_path,
                &bincode::serialize(&*meta_store).unwrap(),
            )
            .unwrap();
        if self.writer.exists(&self.meta_db_path).unwrap() {
            self.writer.delete(&self.meta_db_path).unwrap();
        }
        self.writer
            .rename(&self.new_meta_db_path, &self.meta_db_path)
            .unwrap();
        for file in self.writer.list(&self.wal_dir).unwrap() {
            self.writer.delete(&file).unwrap();
        }
    }

    fn load_column(&self, partition: PartitionID, table_name: &str, column_name: &str) -> Column {
        let path = self
            .tables_path
            .join(table_name)
            .join(format!("{}_{}.part", partition, column_name));
        let data = self.writer.load(&path).unwrap();
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
