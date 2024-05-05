use azure_core::StatusCode;
use std::error::Error;
use std::path::{Path, PathBuf};

use azure_identity::DefaultAzureCredential;
use azure_storage::{prelude::*, ErrorKind};
use azure_storage_blobs::prelude::*;
use futures::stream::StreamExt;
use std::sync::Arc;

use super::file_writer::BlobWriter;
use super::RT;

pub struct AzureBlobWriter {
    account: String,
    container: String,
    storage_credentials: StorageCredentials,
}

impl AzureBlobWriter {
    pub fn new(account: &str, container: &str) -> Result<AzureBlobWriter, Box<dyn Error>> {
        let creds = Arc::new(DefaultAzureCredential::default());
        let storage_credentials = StorageCredentials::token_credential(creds);
        Ok(AzureBlobWriter {
            account: account.to_string(),
            container: container.to_string(),
            storage_credentials,
        })
    }

    fn blob_client(&self, path: &Path) -> BlobClient {
        let string_path = path.to_string_lossy().to_string();
        ClientBuilder::new(self.account.clone(), self.storage_credentials.clone())
            .blob_client(&self.container, string_path)
    }

    fn container_client(&self) -> ContainerClient {
        ClientBuilder::new(self.account.clone(), self.storage_credentials.clone())
            .container_client(&self.container)
    }
}

impl BlobWriter for AzureBlobWriter {
    fn store(
        &self,
        path: &Path,
        data: &[u8],
    ) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        log::debug!("Storing blob in Azure: {:?} {} B", path, data.len());
        let client = self.blob_client(path);
        log::trace!("got blob client {:?}", client);
        let data = data.to_vec();
        RT.block_on(
            client
                .put_block_blob(data)
                .content_type("application/octet-stream")
                .into_future(),
        )?;
        Ok(())
    }

    fn load(&self, path: &Path) -> Result<Vec<u8>, Box<dyn Error + Send + Sync + 'static>> {
        log::debug!("Loading blob from Azure: {:?}", path);
        // The stream is composed of individual calls to the get blob endpoint
        let client = self.blob_client(path);
        let mut result = Vec::new();
        let mut stream = client.get().into_stream();
        while let Some(value) = RT.block_on(stream.next()) {
            let mut body = value?.data;
            // For each response, we stream the body instead of collecting it all
            // into one large allocation.
            while let Some(value) = RT.block_on(body.next()) {
                let value = value?;
                result.extend(&value);
            }
        }
        Ok(result)
    }

    fn delete(&self, path: &Path) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        log::debug!("Deleting blob from Azure: {:?}", path);
        RT.block_on(self.blob_client(path).delete().into_future())?;
        Ok(())
    }

    fn list(&self, path: &Path) -> Result<Vec<PathBuf>, Box<dyn Error + Send + Sync + 'static>> {
        log::debug!("Listing blobs from Azure: {:?}", path);
        let mut prefix = path.to_string_lossy().to_string();
        if !prefix.ends_with('/') {
            prefix.push('/');
        }
        let mut stream = self.container_client()
                .list_blobs()
                .prefix(prefix)
                .delimiter("/")
                .into_stream();
        let mut paths = Vec::new();
        while let Some(value) = RT.block_on(stream.next()) {
            let page = value?;
            log::debug!("Got page of blobs: {:?}", page.blobs);
            for path in page.blobs.blobs() {
                paths.push(PathBuf::from(path.name.as_str()));
            }
        }
        Ok(paths)
    }

    fn exists(&self, path: &Path) -> Result<bool, Box<dyn Error + Send + Sync + 'static>> {
        log::debug!("Checking if blob exists in Azure: {:?}", path);
        match RT.block_on(self.blob_client(path).get_metadata().into_future()) {
            Err(err) => match err.kind() {
                ErrorKind::HttpResponse { status, .. } if *status == StatusCode::NotFound => {
                    Ok(false)
                }
                _ => Err(Box::new(err)),
            },
            Ok(_) => Ok(true),
        }
    }
}
