use futures::{stream::BoxStream, StreamExt};
use iceberg::{io::FileIO, spec::TableMetadata};
use iceberg_ext::{catalog::rest::IcebergErrorResponse, configs::Location};
use serde::Serialize;

use super::compression_codec::CompressionCodec;
use crate::{
    api::{ErrorModel, Result},
    retry::retry_fn,
    service::storage::az::reduce_scheme_string as reduce_azure_scheme,
};

fn normalize_location(location: &Location) -> String {
    if location.as_str().starts_with("abfs") {
        reduce_azure_scheme(location.as_str(), false)
    } else if location.scheme().starts_with("s3") {
        if location.scheme() == "s3" {
            location.to_string()
        } else {
            let mut location = location.clone();
            location.set_scheme_mut("s3");
            location.to_string()
        }
    } else {
        location.to_string()
    }
}

pub(crate) async fn write_metadata_file(
    metadata_location: &Location,
    metadata: impl Serialize,
    compression_codec: CompressionCodec,
    file_io: &FileIO,
) -> Result<(), IoError> {
    let metadata_location = normalize_location(metadata_location);
    tracing::debug!("Writing metadata file to {}", metadata_location);

    let metadata_file = file_io
        .new_output(metadata_location)
        .map_err(IoError::FileCreation)?;

    let buf = serde_json::to_vec(&metadata).map_err(IoError::Serialization)?;

    let metadata_bytes = compression_codec.compress(buf).await?;

    retry_fn(|| async {
        metadata_file
            .write(metadata_bytes.clone().into())
            .await
            .map_err(IoError::FileWriterCreation)
    })
    .await
}

pub(crate) async fn delete_file(file_io: &FileIO, location: &Location) -> Result<(), IoError> {
    let location = normalize_location(location);

    retry_fn(|| async {
        file_io
            .clone()
            .delete(location.clone())
            .await
            .map_err(IoError::FileDelete)
    })
    .await
}

pub(crate) async fn read_file(file_io: &FileIO, file: &Location) -> Result<Vec<u8>, IoError> {
    let file = normalize_location(file);

    let content: Vec<_> = retry_fn(|| async {
        // InputFile isn't clone hence it's here
        file_io
            .clone()
            .new_input(file.clone())
            .map_err(IoError::FileInput)?
            .read()
            .await
            .map_err(|e| IoError::FileRead(Box::new(e)))
            .map(Into::into)
    })
    .await?;

    if file.as_str().ends_with(".gz.metadata.json") {
        let codec = CompressionCodec::Gzip;
        let content = codec.decompress(content).await?;
        Ok(content)
    } else {
        Ok(content)
    }
}

pub(crate) async fn read_metadata_file(
    file_io: &FileIO,
    file: &Location,
) -> Result<TableMetadata, IoError> {
    let content = read_file(file_io, file).await?;
    match tokio::task::spawn_blocking(move || {
        serde_json::from_slice(&content).map_err(IoError::TableMetadataDeserialization)
    })
    .await
    {
        Ok(result) => result,
        Err(e) => Err(IoError::FileDecompression(Box::new(e))),
    }
}

pub(crate) async fn remove_all(file_io: &FileIO, location: &Location) -> Result<(), IoError> {
    let location = normalize_location(location.clone().with_trailing_slash());

    retry_fn(|| async {
        file_io
            .clone()
            .remove_all(location.clone())
            .await
            .map_err(IoError::FileRemoveAll)
    })
    .await
}

pub(crate) const DEFAULT_LIST_LOCATION_PAGE_SIZE: usize = 1000;

pub(crate) async fn list_location<'a>(
    file_io: &'a FileIO,
    location: &'a Location,
    page_size: Option<usize>,
) -> Result<BoxStream<'a, std::result::Result<Vec<String>, IoError>>, IoError> {
    let location = normalize_location(location);
    let location = format!("{}/", location.trim_end_matches('/'));
    tracing::debug!("Listing location: {}", location);
    let size = page_size.unwrap_or(DEFAULT_LIST_LOCATION_PAGE_SIZE);

    let entries = retry_fn(|| async {
        file_io
            .list_paginated(location.clone().as_str(), true, size)
            .await
            .map_err(|e| {
                tracing::warn!(?e, "Failed to list files in location. Retry three times...");
                IoError::List(e)
            })
    })
    .await?
    .map(|res| match res {
        Ok(entries) => Ok(entries
            .into_iter()
            .map(|it| it.path().to_string())
            .collect()),
        Err(e) => Err(IoError::List(e)),
    });
    Ok(entries.boxed())
}

#[derive(thiserror::Error, Debug, strum::IntoStaticStr)]
pub enum IoError {
    #[error("Failed to create file. Please check the storage credentials.")]
    FileCreation(#[source] iceberg::Error),
    #[error("Failed to read file. Please check the storage credentials.")]
    FileInput(#[source] iceberg::Error),
    #[error("Failed to create file writer. Please check the storage credentials.")]
    FileWriterCreation(#[source] iceberg::Error),
    #[error("Failed to serialize data.")]
    Serialization(#[source] serde_json::Error),
    #[error("Failed to deserialize table metadata.")]
    TableMetadataDeserialization(#[source] serde_json::Error),
    #[error("Failed to write table metadata to compressed buffer.")]
    Write(#[source] iceberg::Error),
    #[error("Failed to finish compressing file.")]
    FileCompression(#[source] Box<dyn std::error::Error + Sync + Send + 'static>),
    #[error("Failed to finish decompressing file.")]
    FileDecompression(#[source] Box<dyn std::error::Error + Sync + Send + 'static>),
    #[error("Failed to write file. Please check the storage credentials.")]
    FileWrite(#[source] Box<dyn std::error::Error + Sync + Send + 'static>),
    #[error("Failed to read file. Please check the storage credentials.")]
    FileRead(#[source] Box<dyn std::error::Error + Sync + Send + 'static>),
    #[error("Failed to close file. Please check the storage credentials.")]
    FileClose(#[source] iceberg::Error),
    #[error("Failed to delete file. Please check the storage credentials.")]
    FileDelete(#[source] iceberg::Error),
    #[error("Failed to remove all files in location. Please check the storage credentials.")]
    FileRemoveAll(#[source] iceberg::Error),
    #[error("Failed to list files in location. Please check the storage credentials.")]
    List(#[source] iceberg::Error),
}

impl IoError {
    pub fn to_type(&self) -> &'static str {
        self.into()
    }
}

impl From<IoError> for IcebergErrorResponse {
    fn from(value: IoError) -> Self {
        let typ = value.to_type();
        let boxed = Box::new(value);
        let message = boxed.to_string();

        match boxed.as_ref() {
            IoError::FileRead(_)
            | IoError::FileInput(_)
            | IoError::FileDelete(_)
            | IoError::FileRemoveAll(_)
            | IoError::FileClose(_)
            | IoError::FileWrite(_)
            | IoError::FileWriterCreation(_)
            | IoError::FileCreation(_)
            | IoError::FileDecompression(_)
            | IoError::List(_) => ErrorModel::failed_dependency(message, typ, Some(boxed)).into(),
            IoError::FileCompression(_) | IoError::Write(_) | IoError::Serialization(_) => {
                ErrorModel::internal(message, typ, Some(boxed)).into()
            }
            IoError::TableMetadataDeserialization(e) => {
                ErrorModel::bad_request(format!("{message} {e}"), typ, Some(boxed)).into()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use needs_env_var::needs_env_var;

    use super::*;
    use crate::service::storage::{StorageCredential, StorageProfile};

    async fn test_remove_all(cred: StorageCredential, profile: StorageProfile) {
        async fn list_simple(file_io: &FileIO, location: &Location) -> Option<Vec<String>> {
            let list = list_location(file_io, location, Some(10)).await.unwrap();

            list.collect::<Vec<_>>()
                .await
                .into_iter()
                .map(Result::unwrap)
                .next()
        }

        let location = profile.base_location().unwrap();

        let folder_1 = location.clone().push("folder").clone();
        let file_1 = folder_1.clone().push("file1").clone();
        let folder_2 = location.clone().push("folder-2").clone();
        let file_2 = folder_2.clone().push("file2").clone();

        let data_1 = serde_json::json!({"file": "1"});
        let data_2 = serde_json::json!({"file": "2"});

        let file_io = profile.file_io(Some(&cred)).unwrap();
        write_metadata_file(&file_1, data_1, CompressionCodec::Gzip, &file_io)
            .await
            .unwrap();
        write_metadata_file(&file_2, data_2, CompressionCodec::Gzip, &file_io)
            .await
            .unwrap();

        // Test list - when we list folder 1, we should not see anything related to folder-2
        let list_f1 = list_simple(&file_io, &folder_1).await.unwrap();
        // Assert that one of the items contains file1
        assert!(list_f1.iter().any(|entry| entry.contains("file1")));
        // Assert that "folder-2" is nowhere in the list
        assert!(!list_f1.iter().any(|entry| entry.contains("folder-2")));

        // List full location - we should see both folders
        let list = list_simple(&file_io, &location).await.unwrap();
        assert!(list.iter().any(|entry| entry.contains("folder/file1")));
        assert!(list.iter().any(|entry| entry.contains("folder-2/file2")));

        // Remove folder 1 - file 2 should still be here:
        remove_all(&file_io, &folder_1).await.unwrap();
        assert!(read_file(&file_io, &file_2).await.is_ok());

        let list = list_simple(&file_io, &location).await.unwrap();
        // Assert that "folder/" / file1 is gone
        assert!(!list.iter().any(|entry| entry.contains("file1")));
        // and that "folder-2/" / file2 is still here
        assert!(list.iter().any(|entry| entry.contains("folder-2/file2")));

        // Listing location 1 should return an empty list
        assert!(list_simple(&file_io, &folder_1).await.is_none());

        // Cleanup
        remove_all(&file_io, &folder_2).await.unwrap();
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

            test_remove_all(cred, profile).await;
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

            test_remove_all(cred, profile).await;
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

            test_remove_all(cred, profile).await;
        }
    }
}
