use futures::stream::BoxStream;
use iceberg::spec::TableMetadata;
use iceberg_ext::catalog::rest::IcebergErrorResponse;
use lakekeeper_io::{
    DeleteError, IOError, InvalidLocationError, LakekeeperStorage, Location, ReadError, WriteError,
};
use serde::Serialize;

use super::compression_codec::CompressionCodec;
use crate::api::{ErrorModel, Result};

pub(crate) async fn write_file(
    io: &impl LakekeeperStorage,
    location: &Location,
    data: impl Serialize,
    compression_codec: CompressionCodec,
) -> Result<(), IOErrorExt> {
    tracing::debug!("Writing file to {}", location);
    let buf = serde_json::to_vec(&data).map_err(IOErrorExt::Serialization)?;
    let metadata_bytes = compression_codec.compress(buf).await?;

    io.write(location.as_str(), metadata_bytes.into())
        .await
        .map_err(Into::into)
}

pub(crate) async fn delete_file(
    io: &impl LakekeeperStorage,
    location: &Location,
) -> Result<(), IOErrorExt> {
    io.delete(location.as_str()).await.map_err(Into::into)
}

pub(crate) async fn read_file(
    io: &impl LakekeeperStorage,
    file: &Location,
    compression_codec: CompressionCodec,
) -> Result<Vec<u8>, IOErrorExt> {
    let content: Vec<_> = io.read(file.as_str()).await.map(Into::into)?;

    if matches!(compression_codec, CompressionCodec::None) {
        Ok(content)
    } else {
        let content = compression_codec
            .decompress(content)
            .await
            .map_err(|e| IOErrorExt::FileDecompression(Box::new(e)))?;
        tracing::debug!("Read file {} with codec {:?}", file, compression_codec);
        Ok(content)
    }
}

pub(crate) async fn read_metadata_file(
    io: &impl LakekeeperStorage,
    file: &Location,
) -> Result<TableMetadata, IOErrorExt> {
    let compression_codec = if file.as_str().ends_with(".gz.metadata.json") {
        CompressionCodec::Gzip
    } else {
        CompressionCodec::None
    };

    let content = read_file(io, file, compression_codec).await?;
    match tokio::task::spawn_blocking(move || {
        serde_json::from_slice(&content).map_err(IOErrorExt::Deserialization)
    })
    .await
    {
        Ok(result) => result,
        Err(e) => Err(IOErrorExt::FileDecompression(Box::new(e))),
    }
}

pub(crate) async fn remove_all(
    io: &impl LakekeeperStorage,
    location: &Location,
) -> Result<(), IOErrorExt> {
    io.remove_all(location.as_str()).await.map_err(Into::into)
}

pub(crate) async fn list_location<'a>(
    io: &'a impl LakekeeperStorage,
    location: &'a Location,
    page_size: Option<usize>,
) -> Result<BoxStream<'a, std::result::Result<Vec<Location>, IOError>>, InvalidLocationError> {
    tracing::debug!("Listing location: {}", location);
    let entries = io.list(location, page_size).await?;
    Ok(entries)
}

#[derive(thiserror::Error, Debug, strum::IntoStaticStr)]
pub enum IOErrorExt {
    #[error("Failed to serialize data: {0}")]
    Serialization(#[source] serde_json::Error),
    #[error("Failed to deserialize table metadata: {0}")]
    Deserialization(#[source] serde_json::Error),
    #[error("Failed to finish compressing file: {0}")]
    FileCompression(#[source] Box<dyn std::error::Error + Sync + Send + 'static>),
    #[error("Failed to finish decompressing file: {0}")]
    FileDecompression(#[source] Box<dyn std::error::Error + Sync + Send + 'static>),
    #[error("Invalid file location: {0}")]
    InvalidLocation(#[from] InvalidLocationError),
    #[error("{0}")]
    IOError(#[from] IOError),
}

impl From<WriteError> for IOErrorExt {
    fn from(value: WriteError) -> Self {
        match value {
            WriteError::IOError(e) => e.into(),
            WriteError::InvalidLocation(e) => e.into(),
        }
    }
}

impl From<DeleteError> for IOErrorExt {
    fn from(value: DeleteError) -> Self {
        match value {
            DeleteError::IOError(e) => e.into(),
            DeleteError::InvalidLocation(e) => e.into(),
        }
    }
}

impl From<ReadError> for IOErrorExt {
    fn from(value: ReadError) -> Self {
        match value {
            ReadError::IOError(e) => e.into(),
            ReadError::InvalidLocation(e) => e.into(),
        }
    }
}

impl IOErrorExt {
    pub fn to_type(&self) -> &'static str {
        self.into()
    }
}

impl From<IOErrorExt> for IcebergErrorResponse {
    fn from(value: IOErrorExt) -> Self {
        let typ = value.to_type();
        let boxed = Box::new(value);
        let message = boxed.to_string();

        tracing::info!(?boxed, "IO Error: {message}");

        match boxed.as_ref() {
            IOErrorExt::FileDecompression(_) => {
                ErrorModel::failed_dependency(message, typ, Some(boxed)).into()
            }
            IOErrorExt::FileCompression(_) | IOErrorExt::Serialization(_) => {
                ErrorModel::internal(message, typ, Some(boxed)).into()
            }
            IOErrorExt::Deserialization(_)
            | IOErrorExt::InvalidLocation(_)
            | IOErrorExt::IOError(_) => {
                ErrorModel::bad_request(message.to_string(), typ, Some(boxed)).into()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use needs_env_var::needs_env_var;

    use super::*;
    use crate::service::storage::{StorageCredential, StorageProfile};

    #[allow(dead_code)]
    async fn test_list_and_remove_all(cred: StorageCredential, profile: StorageProfile) {
        async fn list_simple(io: &impl LakekeeperStorage, location: &Location) -> Vec<Location> {
            let list = list_location(io, location, Some(10)).await.unwrap();

            list.collect::<Vec<_>>()
                .await
                .into_iter()
                .map(Result::unwrap)
                .next()
                .unwrap_or_default()
        }

        let mut location = profile.base_location().unwrap();
        location.without_trailing_slash();

        let folder_1 = location.clone().push("folder").clone();
        let file_1 = folder_1.clone().push("file1").clone();
        let folder_2 = location.clone().push("folder-2").clone();
        let file_2 = folder_2.clone().push("file2").clone();

        let data_1 = serde_json::json!({"file": "1"});
        let data_2 = serde_json::json!({"file": "2"});

        let io = profile.file_io(Some(&cred)).await.unwrap();
        write_file(&io, &file_1, data_1, CompressionCodec::Gzip)
            .await
            .unwrap();
        write_file(&io, &file_2, data_2, CompressionCodec::Gzip)
            .await
            .unwrap();

        // Test list - when we list folder 1, we should not see anything related to folder-2
        let list_f1 = list_simple(&io, &folder_1).await;
        // Assert that one of the items contains file1
        assert!(list_f1.iter().any(|entry| entry.as_str().contains("file1")));
        // Assert that "folder-2" is nowhere in the list
        assert!(!list_f1
            .iter()
            .any(|entry| entry.as_str().contains("folder-2")));

        // List full location - we should see both folders
        let list = list_simple(&io, &location).await;
        assert!(list
            .iter()
            .any(|entry| entry.as_str().contains("folder/file1")));
        assert!(list
            .iter()
            .any(|entry| entry.as_str().contains("folder-2/file2")));

        // Remove folder 1 - file 2 should still be here:
        remove_all(&io, &folder_1).await.unwrap();
        assert!(read_file(&io, &file_2, CompressionCodec::Gzip)
            .await
            .is_ok());

        let list = list_simple(&io, &location).await;
        // Assert that "folder/" / file1 is gone
        assert!(!list.iter().any(|entry| entry.as_str().contains("file1")));
        // and that "folder-2/" / file2 is still here
        assert!(list
            .iter()
            .any(|entry| entry.as_str().contains("folder-2/file2")));

        // Listing location 1 should return an empty list
        let folder_1_list = list_simple(&io, &folder_1).await;
        assert!(
            folder_1_list.is_empty(),
            "Folder 1 should be empty, got: {folder_1_list:?}"
        );

        // Cleanup
        remove_all(&io, &folder_2).await.unwrap();
    }

    #[needs_env_var(TEST_AWS = 1)]
    pub(crate) mod aws {
        use super::*;
        use crate::service::storage::{
            s3::test::aws::get_storage_profile, StorageCredential, StorageProfile,
        };

        #[tokio::test]
        async fn test_remove_all_s3() {
            let (profile, cred) = get_storage_profile();
            let cred: StorageCredential = cred.into();
            let profile: StorageProfile = profile.into();

            test_list_and_remove_all(cred, profile).await;
        }
    }

    #[needs_env_var(TEST_AZURE = 1)]
    pub(crate) mod az {
        use super::*;
        use crate::service::storage::{
            az::test::azure_tests::{azure_profile, client_creds},
            StorageCredential, StorageProfile,
        };

        #[tokio::test]
        async fn test_remove_all_az() {
            let cred: StorageCredential = client_creds().into();
            let profile: StorageProfile = azure_profile().into();

            test_list_and_remove_all(cred, profile).await;
        }
    }

    #[needs_env_var(TEST_GCS = 1)]
    pub(crate) mod gcs {
        use super::*;
        use crate::service::storage::{
            gcs::test::cloud_tests::get_storage_profile, StorageCredential, StorageProfile,
        };

        #[tokio::test]
        async fn test_remove_all_gcs() {
            let (profile, cred) = get_storage_profile();
            let cred: StorageCredential = cred.into();
            let profile: StorageProfile = profile.into();

            test_list_and_remove_all(cred, profile).await;
        }
    }
}
