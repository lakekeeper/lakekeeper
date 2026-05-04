use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use iceberg::io::{FileMetadata, Storage, StorageConfig, StorageFactory};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{BoxStream, DeleteBatchError, DeleteError, LakekeeperStorage, ReadError, WriteError};

impl From<ReadError> for iceberg::Error {
    fn from(value: ReadError) -> Self {
        iceberg::Error::new(
            iceberg::ErrorKind::Unexpected,
            format!("Read error {value}"),
        )
        .with_source(value)
    }
}
impl From<WriteError> for iceberg::Error {
    fn from(value: WriteError) -> Self {
        iceberg::Error::new(
            iceberg::ErrorKind::Unexpected,
            format!("Write error {value}"),
        )
        .with_source(value)
    }
}

impl From<DeleteError> for iceberg::Error {
    fn from(value: DeleteError) -> Self {
        iceberg::Error::new(
            iceberg::ErrorKind::Unexpected,
            format!("Delete error {value}"),
        )
        .with_source(value)
    }
}

impl From<DeleteBatchError> for iceberg::Error {
    fn from(value: DeleteBatchError) -> Self {
        iceberg::Error::new(
            iceberg::ErrorKind::Unexpected,
            format!("Delete stream error {value}"),
        )
        .with_source(value)
    }
}

#[derive(Debug, Clone)]
pub struct IcebergStorageBridge {
    lakekeeper_io: Arc<dyn LakekeeperStorage>,
}

impl IcebergStorageBridge {
    #[must_use]
    pub fn new(lakekeeper_io: Arc<dyn LakekeeperStorage>) -> Self {
        Self { lakekeeper_io }
    }
}

impl Serialize for IcebergStorageBridge {
    fn serialize<S: Serializer>(&self, _serializer: S) -> Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom(
            "IcebergStorageBridge cannot be serialized",
        ))
    }
}

impl<'de> Deserialize<'de> for IcebergStorageBridge {
    fn deserialize<D: Deserializer<'de>>(_deserializer: D) -> Result<Self, D::Error> {
        Err(serde::de::Error::custom(
            "IcebergStorageBridge cannot be deserialized",
        ))
    }
}

#[async_trait]
#[typetag::serde]
impl Storage for IcebergStorageBridge {
    async fn exists(&self, path: &str) -> iceberg::Result<bool> {
        self.lakekeeper_io.exists(path).await.map_err(Into::into)
    }

    async fn metadata(&self, path: &str) -> iceberg::Result<FileMetadata> {
        let info = self.lakekeeper_io.metadata(path).await?;
        let size = info.size().ok_or_else(|| {
            iceberg::Error::new(
                iceberg::ErrorKind::Unexpected,
                format!("Backend did not report size for {path}"),
            )
        })?;
        Ok(FileMetadata { size })
    }

    async fn read(&self, path: &str) -> iceberg::Result<bytes::Bytes> {
        self.lakekeeper_io.read(path).await.map_err(Into::into)
    }

    async fn reader(&self, path: &str) -> iceberg::Result<Box<dyn iceberg::io::FileRead>> {
        Ok(Box::new(IcebergFileRead {
            lakekeeper_io: self.lakekeeper_io.clone(),
            path: path.to_string(),
        }))
    }

    async fn write(&self, path: &str, bs: bytes::Bytes) -> iceberg::Result<()> {
        self.lakekeeper_io.write(path, bs).await.map_err(Into::into)
    }

    async fn writer(&self, path: &str) -> iceberg::Result<Box<dyn iceberg::io::FileWrite>> {
        let inner = self.lakekeeper_io.writer(path).await?;
        Ok(Box::new(IcebergFileWrite { inner }))
    }

    async fn delete(&self, path: &str) -> iceberg::Result<()> {
        self.lakekeeper_io.delete(path).await.map_err(Into::into)
    }

    async fn delete_prefix(&self, path: &str) -> iceberg::Result<()> {
        self.lakekeeper_io
            .remove_all(path)
            .await
            .map_err(Into::into)
    }

    async fn delete_stream(&self, paths: BoxStream<'static, String>) -> iceberg::Result<()> {
        let paths = paths.collect::<Vec<_>>().await;
        self.lakekeeper_io
            .delete_batch(&paths)
            .await
            .map_err(Into::into)
    }

    fn new_input(&self, path: &str) -> iceberg::Result<iceberg::io::InputFile> {
        Ok(iceberg::io::InputFile::new(
            Arc::new(self.clone()),
            path.to_string(),
        ))
    }

    fn new_output(&self, path: &str) -> iceberg::Result<iceberg::io::OutputFile> {
        Ok(iceberg::io::OutputFile::new(
            Arc::new(self.clone()),
            path.to_string(),
        ))
    }
}

#[derive(Debug)]
pub(crate) struct IcebergFileRead {
    lakekeeper_io: Arc<dyn LakekeeperStorage>,
    path: String,
}

#[async_trait]
impl iceberg::io::FileRead for IcebergFileRead {
    async fn read(&self, range: std::ops::Range<u64>) -> iceberg::Result<bytes::Bytes> {
        self.lakekeeper_io
            .read_range(&self.path, range)
            .await
            .map_err(Into::into)
    }
}

/// Streaming file writer.
///
/// This trait exists solely to satisfy the iceberg-rust
/// `iceberg::io::FileWrite` contract via `IcebergFileWrite`; it is not a
/// general-purpose internal API.
///
/// **Closing is mandatory.** Dropping a writer without first calling
/// `close` may leave incomplete multipart or resumable uploads on the
/// backend. These linger until a server-side lifecycle policy reaps them
/// (and may incur storage charges for parts in the meantime). Currently
/// there is no stable `AsyncDrop`.
#[async_trait::async_trait]
pub trait LakekeeperFileWrite: std::fmt::Debug + Send + Sync + 'static {
    /// Append bytes to the file. Implementations may buffer locally and
    /// only contact the backend once an internal threshold is reached.
    async fn write(&mut self, bytes: Bytes) -> Result<(), WriteError>;

    /// Finalise the file. Calling `close` on an already-closed writer
    /// returns an error.
    async fn close(&mut self) -> Result<(), WriteError>;
}

#[derive(Debug)]
pub(crate) struct IcebergFileWrite {
    inner: Box<dyn LakekeeperFileWrite>,
}

#[async_trait]
impl iceberg::io::FileWrite for IcebergFileWrite {
    async fn write(&mut self, bs: bytes::Bytes) -> iceberg::Result<()> {
        self.inner.write(bs).await.map_err(Into::into)
    }

    async fn close(&mut self) -> iceberg::Result<()> {
        self.inner.close().await.map_err(Into::into)
    }
}

#[derive(Debug)]
pub struct IcebergStorageBridgeFactory {
    bridge: Arc<IcebergStorageBridge>,
}

impl IcebergStorageBridgeFactory {
    #[must_use]
    pub fn new(bridge: Arc<IcebergStorageBridge>) -> Self {
        Self { bridge }
    }
}

#[typetag::serde]
impl StorageFactory for IcebergStorageBridgeFactory {
    fn build(&self, _config: &StorageConfig) -> iceberg::Result<Arc<dyn Storage>> {
        Ok(self.bridge.clone())
    }
}

impl Serialize for IcebergStorageBridgeFactory {
    fn serialize<S: Serializer>(&self, _serializer: S) -> Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom(
            "IcebergStorageBridgeFactory cannot be serialized",
        ))
    }
}

impl<'de> Deserialize<'de> for IcebergStorageBridgeFactory {
    fn deserialize<D: Deserializer<'de>>(_deserializer: D) -> Result<Self, D::Error> {
        Err(serde::de::Error::custom(
            "LakekeeperIoIcebergBrideStorageFactory cannot be deserialized",
        ))
    }
}
