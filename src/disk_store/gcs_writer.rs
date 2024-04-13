use std::error::Error;
use std::path::{Path, PathBuf};
use futures::executor::block_on;

use google_cloud_storage::client::Client;
use google_cloud_storage::client::ClientConfig;
use google_cloud_storage::http::objects::delete::DeleteObjectRequest;
use google_cloud_storage::http::objects::download::Range;
use google_cloud_storage::http::objects::get::GetObjectRequest;
use google_cloud_storage::http::objects::list::ListObjectsRequest;
use google_cloud_storage::http::objects::upload::{Media, UploadObjectRequest, UploadType};
use reqwest::StatusCode;

use super::file_writer::BlobWriter;

pub struct GCSBlobWriter {
    client: Client,
    bucket: String,
}

impl GCSBlobWriter {
    pub fn new(bucket: String) -> Result<GCSBlobWriter, Box<dyn Error>> {
        let config = block_on(ClientConfig::default().with_auth())?;
        Ok(GCSBlobWriter {
            client: Client::new(config),
            bucket,
        })
    }
}

impl BlobWriter for GCSBlobWriter {
    fn store(
        &self,
        path: &Path,
        data: &[u8],
    ) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        let upload_type = UploadType::Simple(Media::new(path.to_string_lossy().to_string()));
        block_on(self.client.upload_object(
            &UploadObjectRequest {
                bucket: self.bucket.clone(),
                ..Default::default()
            },
            data.to_vec(),
            &upload_type,
        ))?;
        Ok(())
    }

    fn load(&self, path: &Path) -> Result<Vec<u8>, Box<dyn Error + Send + Sync + 'static>> {
        block_on(self.client.download_object(
            &GetObjectRequest {
                bucket: self.bucket.to_string(),
                object: path.to_string_lossy().to_string(),
                ..Default::default()
            },
            &Range::default(),
        ))
        .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync + 'static>)
    }

    fn delete(&self, path: &Path) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        block_on(self.client.delete_object(&DeleteObjectRequest {
            bucket: self.bucket.to_string(),
            object: path.to_string_lossy().to_string(),
            ..Default::default()
        }))
        .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync + 'static>)
    }

    fn list(&self, path: &Path) -> Result<Vec<PathBuf>, Box<dyn Error + Send + Sync + 'static>> {
        block_on(self.client.list_objects(&ListObjectsRequest {
            bucket: self.bucket.to_string(),
            prefix: Some(path.to_string_lossy().to_string()),
            ..Default::default()
        }))
        .map(|objects| {
            objects
                .items
                .unwrap_or_default()
                .iter()
                .map(|object| PathBuf::from(object.name.as_str()))
                .collect()
        })
        .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync + 'static>)
    }

    fn exists(&self, path: &Path) -> Result<bool, Box<dyn Error + Send + Sync + 'static>> {
        block_on(self.client.get_object(&GetObjectRequest {
            bucket: self.bucket.to_string(),
            object: path.to_string_lossy().to_string(),
            ..Default::default()
        }))
        .map(|_| true)
        .or_else(|e| match e {
            google_cloud_storage::http::Error::Response(err) if err.code == 404 => Ok(false),
            google_cloud_storage::http::Error::HttpClient(err)
                if err.status() == Some(StatusCode::NOT_FOUND) =>
            {
                Ok(false)
            }
            _ => Err(Box::new(e) as Box<dyn Error + Send + Sync + 'static>),
        })
    }
}
