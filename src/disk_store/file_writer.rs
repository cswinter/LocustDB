use std::error::Error;
use std::fs::{create_dir_all, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

pub trait BlobWriter: Send + Sync {
    fn store(&self, path: &Path, data: &[u8])
        -> Result<(), Box<dyn Error + Send + Sync + 'static>>;
    fn load(&self, path: &Path) -> Result<Vec<u8>, Box<dyn Error + Send + Sync + 'static>>;
    fn delete(&self, path: &Path) -> Result<(), Box<dyn Error + Send + Sync + 'static>>;
    /// Returns absolute paths of files in the directory
    fn list(&self, path: &Path) -> Result<Vec<PathBuf>, Box<dyn Error + Send + Sync + 'static>>;
    fn exists(&self, path: &Path) -> Result<bool, Box<dyn Error + Send + Sync + 'static>>;
}

pub struct VersionedChecksummedBlobWriter {
    writer: Box<dyn BlobWriter>,
}

impl VersionedChecksummedBlobWriter {
    pub fn new(writer: Box<dyn BlobWriter>) -> VersionedChecksummedBlobWriter {
        VersionedChecksummedBlobWriter { writer }
    }
}

impl BlobWriter for VersionedChecksummedBlobWriter {
    fn store(
        &self,
        path: &Path,
        data: &[u8],
    ) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        let mut wrapped_data = Vec::<u8>::with_capacity(8 + 8 + 256 + data.len());
        // Version number
        wrapped_data.extend(0u64.to_be_bytes().iter());
        // Data length
        wrapped_data.extend(data.len().to_be_bytes().iter());
        // Checksum
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(data);
        let checksum = hasher.finalize();
        wrapped_data.extend(checksum.iter());

        self.writer.store(path, &wrapped_data)
    }

    fn load(&self, path: &Path) -> Result<Vec<u8>, Box<dyn Error + Send + Sync + 'static>> {
        let data = self.writer.load(path)?;
        if data.len() < 8 + 8 + 256 {
            return Err(format!("Invalid data length for {:?}: {}", path, data.len()).into());
        }
        let version = u64::from_be_bytes([
            data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
        ]);
        if version != 0 {
            return Err(format!("Invalid version number for {:?}: {}", path, version).into());
        }
        let data_len = usize::from_be_bytes([
            data[8], data[9], data[10], data[11], data[12], data[13], data[14], data[15],
        ]);
        if data.len() != 8 + 8 + 256 + data_len {
            return Err(format!("Invalid data length for {:?}: {}", path, data.len()).into());
        }
        let checksum = &data[16..16 + 256];
        let actual_checksum = {
            use sha2::{Digest, Sha256};
            let mut hasher = Sha256::new();
            hasher.update(&data[16 + 256..]);
            hasher.finalize()
        };
        if checksum != actual_checksum.as_slice() {
            let mut checksum_hex = String::new();
            for byte in checksum {
                checksum_hex.push_str(&format!("{:02x}", byte));
            }
            return Err(format!(
                "Checksum mismatch for {:?}: expected {}, actual {:0512x}",
                path, checksum_hex, actual_checksum
            )
            .into());
        }
        Ok(data[16 + 256..].to_vec())
    }

    fn delete(&self, path: &Path) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        self.writer.delete(path)
    }

    fn list(&self, path: &Path) -> Result<Vec<PathBuf>, Box<dyn Error + Send + Sync + 'static>> {
        self.writer.list(path)
    }

    fn exists(&self, path: &Path) -> Result<bool, Box<dyn Error + Send + Sync + 'static>> {
        self.writer.exists(path)
    }
}

pub struct FileBlobWriter;

impl FileBlobWriter {
    pub fn new() -> FileBlobWriter {
        FileBlobWriter
    }
}

impl BlobWriter for FileBlobWriter {
    /// Atomically writes the data to the file at the given path
    fn store(
        &self,
        path: &Path,
        data: &[u8],
    ) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        // Create the directory if it doesn't exist
        if let Some(parent) = path.parent() {
            create_dir_all(parent)?;
        }

        // Write the data to a temporary file and then rename it to the target path
        let tmp_path = path.with_extension(".INCOMPLETE");
        let mut file = File::create(&tmp_path)?;
        file.write_all(data)?;
        file.sync_all()?;
        std::fs::rename(tmp_path, path).map_err(|e| format!("Failed to rename file: {}", e))?;

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
