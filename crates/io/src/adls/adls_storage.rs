use azure_core::lro::location;
use azure_storage_blobs::prelude::{BlobServiceClient, ContainerClient};
use bytes::Bytes;
use futures::StreamExt as _;

use crate::{
    adls::{adls_error::parse_error, AdlsLocation},
    BatchDeleteResult, DeleteBatchFatalError, DeleteError, IOError, InvalidLocationError,
    LakekeeperStorage, Location, ReadError, WriteError,
};

const DEFAULT_CHUNK_SIZE: u64 = 0x1000 * 0x1000 * 10; // 10 MiB

#[derive(Debug, Clone)]
pub struct AdlsStorage {
    client: BlobServiceClient,
}

impl AdlsStorage {
    async fn get_container_client(
        &self,
        location: &AdlsLocation,
    ) -> Result<ContainerClient, InvalidLocationError> {
        if self.client.account() != location.account_name() {
            return Err(InvalidLocationError {
                reason: format!(
                    "Location account name `{}` does not match storage account `{}`",
                    location.account_name(),
                    self.client.account()
                ),
                location: location.to_string(),
            });
        }

        Ok(self.client.container_client(location.filesystem()))
    }
}

impl AdlsStorage {
    pub fn new(client: BlobServiceClient) -> Self {
        Self { client }
    }

    pub fn client(&self) -> &BlobServiceClient {
        &self.client
    }
}

impl LakekeeperStorage for AdlsStorage {
    async fn delete(&self, path: impl AsRef<str>) -> Result<(), DeleteError> {
        todo!()
    }

    async fn delete_batch(
        &self,
        paths: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Result<BatchDeleteResult, DeleteBatchFatalError> {
        todo!()
    }

    async fn write(&self, path: impl AsRef<str>, bytes: Bytes) -> Result<(), WriteError> {
        todo!()
    }

    async fn read(&self, path: impl AsRef<str>) -> Result<Bytes, ReadError> {
        let path = path.as_ref();
        let adls_location = AdlsLocation::try_from_str(path, true)?;

        // Get the container/filesystem name and the blob path (key)
        let container_name = adls_location.filesystem().to_string();
        let blob_path = if adls_location.key().is_empty() {
            String::new()
        } else {
            adls_location.key().join("/")
        };

        // Get container client from service client
        let container_client = self.client.container_client(&container_name);

        // Get blob client from container client
        let blob_client = container_client.blob_client(&blob_path);

        // Get the blob
        let mut stream = blob_client
            .get()
            .chunk_size(DEFAULT_CHUNK_SIZE)
            .into_stream();
        let mut blob = Vec::<u8>::new();
        while let Some(value) = stream.next().await {
            // TODO RETRY?!
            let data = value
                .map_err(|e| parse_error(e, path.as_ref()))?
                .data
                .collect()
                .await
                .map_err(|e| parse_error(e, path.as_ref()))?;
            blob.extend(&data);
        }

        Ok(Bytes::from(blob))
    }

    async fn list(
        &self,
        path: impl AsRef<str> + Send,
        page_size: Option<usize>,
    ) -> Result<futures::stream::BoxStream<'_, Result<Vec<Location>, IOError>>, InvalidLocationError>
    {
        todo!()
    }
}
