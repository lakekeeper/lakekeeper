use super::compression_codec::CompressionCodec;
use crate::{
    api::{ErrorModel, Result},
    retry::retry_fn,
    service::storage::az::{
        reduce_scheme_string as reduce_azure_scheme,
        ALTERNATIVE_PROTOCOLS as AZURE_ALTERNATIVE_PROTOCOLS,
    },
};
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use hdfs_native_object_store::HdfsObjectStore;
use iceberg::{io::FileIO as IcebergFileIO, spec::TableMetadata};
use iceberg_ext::{catalog::rest::IcebergErrorResponse, configs::Location};
use object_store::{ObjectStore, PutPayload};
use serde::Serialize;

use super::compression_codec::CompressionCodec;
use crate::{
    api::{ErrorModel, Result},
    retry::retry_fn,
    service::storage::az::reduce_scheme_string as reduce_azure_scheme,
};

#[derive(Debug, Clone)]
pub enum LakekeeperFileIO {
    IcebergFileIO(IcebergFileIO),
    HdfsNative(HdfsObjectStore),
}

impl LakekeeperFileIO {
    pub async fn write_metadata_file(
        &self,
        metadata_location: &Location,
        metadata: impl Serialize,
        compression_codec: CompressionCodec,
    ) -> Result<(), IoError> {
        let metadata_location = normalize_location(metadata_location);

        let buf = serde_json::to_vec(&metadata).map_err(IoError::Serialization)?;
        let metadata_bytes = compression_codec.compress(buf).await?;

        match self {
            LakekeeperFileIO::IcebergFileIO(file_io) => {
                retry_fn(|| async {
                    let metadata_file = file_io
                        .new_output(&metadata_location)
                        .map_err(IoError::FileCreation)?;
                    metadata_file
                        .write(metadata_bytes.clone().into())
                        .await
                        .map_err(IoError::FileWriterCreation)
                })
                .await
            }
            LakekeeperFileIO::HdfsNative(hdfs) => hdfs
                .put(
                    &object_store::path::Path::from(metadata_location.clone()),
                    PutPayload::from(metadata_bytes),
                )
                .await
                .map_err(|e| IoError::FileWrite(Box::new(e)))
                .map(|_| ()),
        }
        .inspect_err(|e| {
            tracing::info!(
                ?e,
                "Failed to delete table metadata from `{metadata_location}`"
            )
        })?;
        Ok(())
    }

    pub async fn read_file(&self, file: &Location) -> Result<Vec<u8>, IoError> {
        let file = normalize_location(file);
        match self {
            LakekeeperFileIO::IcebergFileIO(file_io) => {
                retry_fn(|| async {
                    // InputFile isn't clone
                    file_io
                        .clone()
                        .new_input(&file)
                        .map_err(IoError::FileInput)?
                        .read()
                        .await
                        .map_err(|e| IoError::FileRead(Box::new(e)))
                        .map(Into::into)
                })
                .await
            }
            LakekeeperFileIO::HdfsNative(hdfs) => {
                let content = hdfs
                    .get(&object_store::path::Path::from(file.clone()))
                    .await
                    .map_err(|e| IoError::FileRead(Box::new(e)))?;
                content
                    .bytes()
                    .await
                    .map_err(|e| IoError::FileRead(Box::new(e)))
                    .map(|pl| pl.to_vec())
            }
        }
        .inspect_err(|e| tracing::info!(?e, "Failed to read file `{file}`"))
    }

    pub async fn read_metadata_file(&self, file: &Location) -> Result<TableMetadata, IoError> {
        let content = self.read_file(file).await?;

        let content = if file.as_str().ends_with(".gz.metadata.json") {
            let codec = CompressionCodec::Gzip;
            let content = codec.decompress(content).await?;
            content
        } else {
            content
        };

        tokio::task::spawn_blocking(move || {
            serde_json::from_slice(&content).map_err(IoError::TableMetadataDeserialization)
        })
        .await
        .unwrap_or_else(|e| Err(IoError::JoinError(e)))
    }

    pub async fn delete_all(&self, location: &Location) -> Result<(), IoError> {
        let location = normalize_location(location.clone().with_trailing_slash());

        match self {
            LakekeeperFileIO::IcebergFileIO(file_io) => {
                retry_fn(|| async {
                    file_io
                        .clone()
                        .remove_all(&location)
                        .await
                        .map_err(IoError::FileRemoveAll)
                })
                .await
            }
            LakekeeperFileIO::HdfsNative(hdfs) => {
                let files = hdfs
                    .list(Some(&object_store::path::Path::from(location.clone())))
                    .map(|p| p.map(|p| p.location));
                let delete_stream = hdfs.delete_stream(files.boxed());
                let v = delete_stream.try_collect::<Vec<_>>().await;
                match v {
                    Ok(_) => Ok(()),
                    Err(e) => Err(IoError::FileDelete(Box::new(e))),
                }
            }
        }
        .inspect_err(|e| tracing::info!(?e, "Failed to delete all files in location `{location}`"))
    }

    pub async fn delete_file(&self, location: &Location) -> Result<(), IoError> {
        let location = normalize_location(&location);
        match self {
            LakekeeperFileIO::IcebergFileIO(file_io) => {
                retry_fn(|| async {
                    file_io
                        .clone()
                        .delete(&location)
                        .await
                        .map_err(|e| IoError::FileDelete(Box::new(e)))
                })
                .await
            }
            LakekeeperFileIO::HdfsNative(hdfs) => hdfs
                .delete(&object_store::path::Path::from(location.clone()))
                .await
                .map_err(|e| IoError::FileDelete(Box::new(e))),
        }
        .inspect_err(|e| tracing::info!(?e, "Failed to delete file `{location}`"))
    }

    pub async fn list_location<'a>(
        &'a self,
        location: &'a Location,
        page_size: Option<usize>,
    ) -> Result<BoxStream<'a, std::result::Result<Vec<String>, IoError>>, IoError> {
        let location = normalize_location(location.clone().with_trailing_slash());

        match self {
            LakekeeperFileIO::IcebergFileIO(file_io) => {
                let size = page_size.unwrap_or(DEFAULT_LIST_LOCATION_PAGE_SIZE);
                let entries = retry_fn(|| async {
                    file_io
                        .list_paginated(location.clone().as_str(), true, size)
                        .await
                        .map_err(|e| IoError::List(Box::new(e)))
                })
                .await?
                .map(|res| match res {
                    Ok(entries) => Ok(entries
                        .into_iter()
                        .map(|it| it.path().to_string())
                        .collect()),
                    Err(e) => Err(IoError::List(Box::new(e))),
                });
                Ok(entries.boxed())
            }
            LakekeeperFileIO::HdfsNative(hdfs) => Ok(hdfs
                .list(Some(&object_store::path::Path::from(location)))
                .map(|p| p.map(|p| p.location.to_string()))
                .chunks(page_size.unwrap_or(DEFAULT_LIST_LOCATION_PAGE_SIZE))
                .map(|c| {
                    c.into_iter()
                        .collect::<Result<Vec<String>, _>>()
                        .map_err(|e| IoError::List(Box::new(e)))
                })
                .boxed()),
        }
    }
}

fn normalize_location(location: &Location) -> String {
    if location.as_str().starts_with("abfs")
        || AZURE_ALTERNATIVE_PROTOCOLS
            .iter()
            .any(|p| location.as_str().starts_with(p))
    {
        reduce_azure_scheme(location.as_str(), false)
    } else if location.scheme().starts_with("hdfs") {
        let mut prefix = String::from(location.scheme());
        prefix.push_str("://");
        if let Some(host) = location.url().host() {
            prefix.push_str(&host.to_string());
        }
        if let Some(port) = location.url().port() {
            prefix.push_str(&format!(":{port}"));
        }
        location
            .as_str()
            .strip_prefix(&prefix)
            .map_or_else(|| location.as_str().to_string(), ToString::to_string)
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

pub(crate) const DEFAULT_LIST_LOCATION_PAGE_SIZE: usize = 1000;

#[derive(thiserror::Error, Debug, strum::IntoStaticStr)]
pub enum IoError {
    #[error("Failed to create file. Please check the storage credentials: {}", .0)]
    FileCreation(#[source] iceberg::Error),
    #[error("Failed to read file. Please check the storage credentials: {}", .0)]
    FileInput(#[source] iceberg::Error),
    #[error("Failed to create file writer. Please check the storage credentials: {}", .0)]
    FileWriterCreation(#[source] iceberg::Error),
    #[error("Failed to serialize data.")]
    Serialization(#[source] serde_json::Error),
    #[error("Failed to deserialize table metadata")]
    TableMetadataDeserialization(#[source] serde_json::Error),
    #[error("Failed to write table metadata to compressed buffer: {}", .0)]
    Write(#[source] iceberg::Error),
    #[error("Failed to finish compressing file")]
    FileCompression(#[source] Box<dyn std::error::Error + Sync + Send + 'static>),
    #[error("Failed to finish decompressing file")]
    FileDecompression(#[source] Box<dyn std::error::Error + Sync + Send + 'static>),
    #[error("Failed to write file. Please check the storage credentials.")]
    FileWrite(#[source] Box<dyn std::error::Error + Sync + Send + 'static>),
    #[error("Failed to read file. Please check the storage credentials.")]
    FileRead(#[source] Box<dyn std::error::Error + Sync + Send + 'static>),
    #[error("Failed to close file. Please check the storage credentials: {}", .0)]
    FileClose(#[source] iceberg::Error),
    #[error("Failed to delete file. Please check the storage credentials: {}", .0)]
    FileDelete(#[source] iceberg::Error),
    #[error(
        "Failed to remove all files in location. Please check the storage credentials: {}", .0
    )]
    FileRemoveAll(#[source] iceberg::Error),
    #[error("Failed to join thread.")]
    JoinError(#[source] tokio::task::JoinError),
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
            IoError::FileCompression(_)
            | IoError::Write(_)
            | IoError::Serialization(_)
            | IoError::JoinError(_) => ErrorModel::internal(message, typ, Some(boxed)).into(),
            IoError::TableMetadataDeserialization(e) => {
                ErrorModel::bad_request(format!("{message} {e}"), typ, Some(boxed)).into()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use iceberg_ext::configs::ParseFromStr;
    use needs_env_var::needs_env_var;

    use super::*;
    use crate::service::storage::{StorageCredential, StorageProfile};

    #[test]
    fn test_with_trailing_slash_persists_in_normalization() {
        let loc = Location::parse_value("s3://bucket/folder/").unwrap();
        let loc = normalize_location(&loc);
        assert_eq!("s3://bucket/folder/", loc);
    }

    #[allow(dead_code)]
    async fn test_remove_all(cred: StorageCredential, profile: StorageProfile) {
        async fn list_simple(
            file_io: &LakekeeperFileIO,
            location: &Location,
        ) -> Option<Vec<String>> {
            let list = file_io.list_location(location, Some(10)).await.unwrap();

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

        let file_io = profile.file_io(Some(&cred)).await.unwrap();
        file_io
            .write_metadata_file(&file_1, data_1, CompressionCodec::Gzip)
            .await
            .unwrap();
        file_io
            .write_metadata_file(&file_2, data_2, CompressionCodec::Gzip)
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
        file_io.delete_all(&folder_1).await.unwrap();
        assert!(file_io.read_file(&file_2).await.is_ok());

        let list = list_simple(&file_io, &location).await.unwrap();
        // Assert that "folder/" / file1 is gone
        assert!(!list.iter().any(|entry| entry.contains("file1")));
        // and that "folder-2/" / file2 is still here
        assert!(list.iter().any(|entry| entry.contains("folder-2/file2")));

        // Listing location 1 should return an empty list
        assert!(list_simple(&file_io, &folder_1).await.is_none());

        // Cleanup
        file_io.delete_all(&folder_2).await.unwrap();
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

    #[test]
    fn test_normalize_hdfs() {
        let loc = "hdfs://namenode:8020/user/hdfs/019625f2-668e-7173-bb4d-65ace9b475f1/019625f2-668e-7173-bb4d-65bb757efa94/metadata/test";
        let loc = Location::parse_value(loc).unwrap();
        let loc = normalize_location(&loc);
        assert_eq!("/user/hdfs/019625f2-668e-7173-bb4d-65ace9b475f1/019625f2-668e-7173-bb4d-65bb757efa94/metadata/test", loc);
    }
}
