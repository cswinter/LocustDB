use std::error::Error;
use std::fs::{create_dir_all, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};


pub trait BlobWriter {
    fn store(&self, path: &Path, data: &[u8])
        -> Result<(), Box<dyn Error + Send + Sync + 'static>>;
    fn load(&self, path: &Path) -> Result<Vec<u8>, Box<dyn Error + Send + Sync + 'static>>;
    fn delete(&self, path: &Path) -> Result<(), Box<dyn Error + Send + Sync + 'static>>;
    fn rename(&self, src: &Path, dst: &Path) -> Result<(), Box<dyn Error + Send + Sync + 'static>>;
    /// Returns absolute paths of files in the directory
    fn list(&self, path: &Path) -> Result<Vec<PathBuf>, Box<dyn Error + Send + Sync + 'static>>;
    fn exists(&self, path: &Path) -> Result<bool, Box<dyn Error + Send + Sync + 'static>>;
}

pub struct FileBlobWriter;

impl FileBlobWriter {
    pub fn new() -> FileBlobWriter {
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