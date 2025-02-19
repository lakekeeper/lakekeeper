use futures::{stream::BoxStream, StreamExt};
use iceberg::{io::FileIO, spec::TableMetadata};
use iceberg_ext::{catalog::rest::IcebergErrorResponse, configs::Location};
use serde::Serialize;

use super::compression_codec::CompressionCodec;
use crate::{
    api::{ErrorModel, Result},
    retry::retry_fn,
    service::storage::path_utils,
};

pub(crate) async fn write_metadata_file(
    metadata_location: &Location,
    metadata: impl Serialize,
    compression_codec: CompressionCodec,
    file_io: &FileIO,
) -> Result<(), IoError> {
    let metadata_location = metadata_location.as_str();
    let metadata_location = if metadata_location.starts_with("abfs") {
        path_utils::reduce_scheme_string(metadata_location, false)
    } else {
        metadata_location.to_string()
    };
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
    let location = location.as_str();
    let location = if location.starts_with("abfs") {
        path_utils::reduce_scheme_string(location, false)
    } else {
        location.to_string()
    };

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
    let file = file.as_str();
    let file = if file.starts_with("abfs") {
        path_utils::reduce_scheme_string(file, false)
    } else {
        file.to_string()
    };

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
    let location = location.as_str();
    let location = if location.starts_with("abfs") {
        path_utils::reduce_scheme_string(location, false)
    } else {
        location.to_string()
    };

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
    let location = path_utils::reduce_scheme_string(location.as_str(), false);
    tracing::debug!("Listing location: {}", location);
    let location = format!("{}/", location.trim_end_matches('/'));
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
mod tests {}
