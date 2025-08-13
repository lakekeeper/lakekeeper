use std::{collections::HashMap, num::NonZeroU32, str::FromStr, sync::atomic::AtomicU64};

use crate::{
    adls::{adls_error::parse_error, AdlsLocation},
    calculate_ranges, delete_not_found_is_ok,
    error::ErrorKind,
    safe_usize_to_i64, validate_file_size, BatchDeleteError, BatchDeleteResult,
    DeleteBatchFatalError, DeleteError, IOError, InvalidLocationError, LakekeeperStorage, Location,
    ReadError, WriteError,
};
use azure_core::prelude::Range;
use azure_storage::CloudLocation;
use azure_storage_datalake::prelude::{
    DataLakeClient, DirectoryClient, FileClient, FileSystemClient,
};
use bytes::Bytes;
use futures::{stream::FuturesUnordered, StreamExt as _};

#[derive(Debug, Clone)]
pub struct AdlsStorage {
    data_lake_client: DataLakeClient,
    cloud_location: CloudLocation,
}

const MAX_BYTES_PER_REQUEST: usize = 7 * 1024 * 1024;
const DEFAULT_BYTES_PER_REQUEST: usize = 4 * 1024 * 1024;

impl AdlsStorage {
    /// Returns a [`FileSystemClient`] for the Azure Storage account.
    ///
    /// # Errors
    /// - If the specified account in the location does not match the location's account name.
    pub fn get_filesystem_client(
        &self,
        location: &AdlsLocation,
    ) -> Result<FileSystemClient, InvalidLocationError> {
        if self.cloud_location.account() != location.account_name() {
            return Err(InvalidLocationError {
                reason: format!(
                    "Location account name `{}` does not match storage account `{}`",
                    location.account_name(),
                    self.cloud_location.account()
                ),
                location: location.to_string(),
            });
        }

        // Get the container client for the filesystem
        let container_client = self
            .data_lake_client
            .file_system_client(location.filesystem());
        Ok(container_client)
    }

    /// Returns a [`FileClient`] for the Azure Storage account.
    ///
    /// # Errors
    /// - If the filesystem client cannot be retrieved or initialized.
    pub fn get_file_client(
        &self,
        location: &AdlsLocation,
    ) -> Result<FileClient, InvalidLocationError> {
        let filesystem_client = self.get_filesystem_client(location)?;
        Ok(filesystem_client.into_file_client(location.blob_name()))
    }

    /// Returns a [`DirectoryClient`] for the Azure Storage account.
    ///
    /// # Errors
    /// - If the filesystem client cannot be retrieved or initialized.
    pub fn get_directory_client(
        &self,
        location: &AdlsLocation,
    ) -> Result<DirectoryClient, InvalidLocationError> {
        let filesystem_client = self.get_filesystem_client(location)?;
        Ok(filesystem_client.into_directory_client(location.blob_name()))
    }
}

impl AdlsStorage {
    #[must_use]
    pub fn new(client: DataLakeClient, cloud_location: CloudLocation) -> Self {
        Self {
            data_lake_client: client,
            cloud_location,
        }
    }

    #[must_use]
    pub fn client(&self) -> &DataLakeClient {
        &self.data_lake_client
    }
}

impl LakekeeperStorage for AdlsStorage {
    async fn delete(&self, path: impl AsRef<str>) -> Result<(), DeleteError> {
        let path = path.as_ref();
        let adls_location = AdlsLocation::try_from_str(path, true)?;

        // Get the container/filesystem name and the blob path (key)
        require_key(&adls_location)?;
        // Get container client from service client
        let client = self.get_file_client(&adls_location)?;

        let mut delete_response = client.delete().into_stream();
        while let Some(result) = delete_response.next().await {
            let result = result.map_err(|e| parse_error(e, path)).map(|_| ());
            let result = delete_not_found_is_ok(result);
            if let Err(e) = result {
                return Err(e.into());
            }
        }

        // Check if deletion was successful
        Ok(())
    }

    // ToDo: Switch to BlobBatch delete once supported by rust SDK
    async fn delete_batch(
        &self,
        paths: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Result<BatchDeleteResult, DeleteBatchFatalError> {
        // Group paths by account and filesystem
        let grouped_paths = group_paths_by_container(paths)?;

        // Create futures for parallel deletion
        let mut delete_futures = FuturesUnordered::new();
        let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(1000));

        // Create delete operations for each path
        for ((_account, _filesystem), paths) in &grouped_paths {
            if paths.is_empty() {
                continue; // Skip empty groups
            }
            let filesystem_client = self.get_filesystem_client(&paths[0])?;

            for path in paths {
                let file_client = filesystem_client.get_file_client(path.blob_name());
                let mut deletion_stream = file_client.delete().into_stream();

                let _permit = semaphore.acquire().await.map_err(|e| {
                    DeleteBatchFatalError::IOError(IOError::new(
                        ErrorKind::Unexpected,
                        format!("Failed to acquire semaphore permit for deletion: {e}"),
                        path.location().to_string(),
                    ))
                })?;

                delete_futures.push(async move {
                    let mut last_err = None;
                    while let Some(result) = deletion_stream.next().await {
                        let result = result
                            .map_err(|e| parse_error(e, path.location().as_str()))
                            .map(|_| ());
                        let result = delete_not_found_is_ok(result);
                        if let Err(e) = result {
                            last_err = Some(e);
                        }
                    }

                    if let Some(e) = last_err {
                        (path, Some(e))
                    } else {
                        (path, None)
                    }
                });
            }
        }

        // Process all futures as they complete
        let counter = AtomicU64::new(0);
        let mut successful_paths = Vec::new();
        let mut errors = Vec::new();

        while let Some(result) = delete_futures.next().await {
            // Increment the counter for each processed operation
            let batch_index = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            match result {
                (path, None) => {
                    // Successful deletion
                    successful_paths.push(path);
                }
                (location, Some(error)) => {
                    // Individual deletion error
                    if batch_index == 0 {
                        // If this is the first batch and it failed, return early
                        return Err(DeleteBatchFatalError::IOError(error));
                    }

                    // Otherwise, track the error but continue processing
                    let batch_error = BatchDeleteError::new(
                        location.location().to_string(),
                        Some(error.kind().to_string()),
                        error.to_string(),
                    );
                    errors.push(batch_error);
                }
            }
        }

        // Return the result based on whether we had any errors
        if errors.is_empty() {
            Ok(BatchDeleteResult::AllSuccessful())
        } else {
            let successful_paths: Vec<String> = successful_paths
                .into_iter()
                .map(|p| p.location().to_string())
                .collect();
            Ok(BatchDeleteResult::PartialFailure {
                successful_paths,
                errors,
            })
        }
    }

    async fn write(&self, path: impl AsRef<str>, bytes: Bytes) -> Result<(), WriteError> {
        let path = path.as_ref();
        let adls_location = AdlsLocation::try_from_str(path, true)?;

        // Get the container/filesystem name and the blob path (key)
        require_key(&adls_location)?;

        let client = self.get_file_client(&adls_location)?;

        let _create_result = client
            .create()
            .await
            .map_err(|e| WriteError::IOError(parse_error(e, path)))?;

        let file_length = safe_usize_to_i64(bytes.len(), path)?;

        // If the data is small enough, upload in a single request
        if bytes.len() <= MAX_BYTES_PER_REQUEST {
            client
                .append(0, bytes)
                .await
                .map_err(|e| WriteError::IOError(parse_error(e, path)))?;
        } else {
            // Split data into chunks and upload concurrently
            let chunks: Vec<_> = bytes
                .chunks(DEFAULT_BYTES_PER_REQUEST)
                .enumerate()
                .map(|(i, chunk)| {
                    let offset = i64::try_from(i * DEFAULT_BYTES_PER_REQUEST).unwrap_or(i64::MAX);
                    (offset, Bytes::copy_from_slice(chunk))
                })
                .collect();

            // Create futures for concurrent uploads with a limit of 10 parallel requests
            let mut upload_futures = FuturesUnordered::new();
            let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(10));

            for (offset, chunk) in chunks {
                let client = client.clone();
                let path = path.to_string();
                let semaphore = semaphore.clone();

                let future = async move {
                    let _permit = semaphore.acquire().await.map_err(|e| {
                        WriteError::IOError(IOError::new(
                            ErrorKind::Unexpected,
                            format!(
                                "Failed to acquire semaphore permit for chunk at offset {offset}: {e}",
                            ),
                            path.clone(),
                        ))
                    })?;

                    client
                        .append(offset, chunk)
                        .await
                        .map_err(|e| WriteError::IOError(parse_error(e, &path)))
                };

                upload_futures.push(future);
            }

            // Wait for all uploads to complete
            while let Some(result) = upload_futures.next().await {
                result?;
            }
        }

        client
            .flush(file_length)
            .close(true)
            .await
            .map_err(|e| WriteError::IOError(parse_error(e, path)))?;

        Ok(())
    }

    async fn read_single(&self, path: impl AsRef<str>) -> Result<Bytes, ReadError> {
        let path = path.as_ref();
        let adls_location = AdlsLocation::try_from_str(path, true)?;

        // Get the container/filesystem name and the blob path (key)
        require_key(&adls_location)?;

        let client = self.get_file_client(&adls_location)?;

        let read_file_response = client.read().await.map_err(|e| {
            ReadError::IOError(
                parse_error(e, path).with_context("Failed to read ADLS file in single request."),
            )
        })?;

        Ok(read_file_response.data)
    }

    async fn read(&self, path: impl AsRef<str> + Send) -> Result<Bytes, ReadError> {
        let path = path.as_ref();
        let adls_location = AdlsLocation::try_from_str(path, true)?;

        // Get the container/filesystem name and the blob path (key)
        require_key(&adls_location)?;

        let client = self.get_file_client(&adls_location)?;
        let status = client.get_properties().await.map_err(|e| {
            ReadError::IOError(parse_error(e, path).with_context("Failed to get ADLS file status"))
        })?;

        let Some(content_length) = status.content_length else {
            return self.read_single(path).await;
        };

        let file_size = validate_file_size(content_length, adls_location.location().as_str())?;

        if file_size < MAX_BYTES_PER_REQUEST {
            // If the file is small enough, read it in a single request
            return self.read_single(path).await;
        }

        let download_futures = FuturesUnordered::new();
        let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(10));

        let chunks = calculate_ranges(file_size, DEFAULT_BYTES_PER_REQUEST);

        for (chunk_index, (start, end)) in chunks.into_iter().enumerate() {
            let semaphore = semaphore.clone();
            let client = client.clone();
            let path = path.to_string();

            let future = async move {
                let _permit = semaphore.acquire().await.map_err(|_| {
                    ReadError::IOError(IOError::new(
                        ErrorKind::Unexpected,
                        format!("Failed to acquire semaphore permit for ADLS download chunk {chunk_index}"),
                        path.clone(),
                    ))
                })?;

                let chunk_data = client
                    .read()
                    .range(Range::new(start as u64, (end + 1) as u64))
                    .await
                    .map_err(|e| {
                        ReadError::IOError(parse_error(e, &path).with_context(format!(
                            "Failed to download chunk {chunk_index} (bytes {start}-{end})"
                        )))
                    })?;

                if chunk_data.last_modified != status.last_modified {
                    return Err(ReadError::IOError(IOError::new(
                        ErrorKind::Unexpected,
                        format!(
                            "File was modified during multi-part download: expected last modified time {}, got {}",
                            status.last_modified,
                            chunk_data.last_modified
                        ),
                        path.clone(),
                    )));
                }

                Ok::<(usize, Bytes), ReadError>((chunk_index, chunk_data.data))
            };

            download_futures.push(future);
        }

        // Use the shared utility function to assemble chunks
        let bytes =
            crate::assemble_chunks(download_futures, file_size, DEFAULT_BYTES_PER_REQUEST).await?;

        Ok(bytes)
    }

    async fn list(
        &self,
        path: impl AsRef<str> + Send,
        page_size: Option<usize>,
    ) -> Result<futures::stream::BoxStream<'_, Result<Vec<Location>, IOError>>, InvalidLocationError>
    {
        let path = format!("{}/", path.as_ref().trim_end_matches('/'));
        let adls_location = AdlsLocation::try_from_str(&path, true)?;
        let base_location = adls_location.location().clone();

        let client = self.get_filesystem_client(&adls_location)?;

        let mut list_op = client.list_paths().directory(adls_location.blob_name());

        // Set maximum results per page if requested.
        // By default, ADLS returns 5000 items per page.
        if let Some(size) = page_size {
            // Convert to NonZeroU32, ensuring it's at least 1
            if let Some(max_results) = NonZeroU32::new(u32::try_from(size).unwrap_or(u32::MAX)) {
                list_op = list_op.max_results(max_results);
            }
        }

        let list_stream = list_op.into_stream();

        let stream = list_stream.map(move |result| {
            let base_location = base_location.clone();
            result
                .map_err(|e| parse_error(e, path.as_str()))
                .map(|page| {
                    let locations = page
                        .paths
                        .iter()
                        .filter_map(|path| {
                            // Create a location from account, filesystem and blob name
                            let path_name = if path.is_directory {
                                format!("{}/", path.name.trim_end_matches('/'))
                            } else {
                                path.name.clone()
                            };
                            let full_path = format!(
                                "{}://{}/{}",
                                base_location.scheme(),
                                base_location.authority_with_host(),
                                path_name
                            );
                            Location::from_str(&full_path).ok()
                        })
                        .collect::<Vec<_>>();
                    locations
                })
        });

        Ok(stream.boxed())
    }

    async fn remove_all(&self, path: impl AsRef<str>) -> Result<(), DeleteError> {
        let path = path.as_ref().trim_end_matches('/');
        let adls_location = AdlsLocation::try_from_str(path, true)?;

        // Get the container/filesystem name and the blob path (key)
        require_key(&adls_location)?;

        let client = self.get_file_client(&adls_location)?;
        let mut delete_stream = client.delete().recursive(true).into_stream();

        while let Some(result) = delete_stream.next().await {
            if let Some(err) = result.err() {
                return Err(DeleteError::IOError(parse_error(err, path)));
            }
        }

        Ok(())
    }
}

fn require_key(adls_location: &AdlsLocation) -> Result<(), InvalidLocationError> {
    if adls_location.blob_name().is_empty() || adls_location.blob_name() == "/" {
        return Err(InvalidLocationError {
            reason: "Operation requires a path inside the ADLS Filesystem".to_string(),
            location: adls_location.to_string(),
        });
    }
    Ok(())
}

/// Groups paths by account and filesystem (container).
///
/// Returns a `HashMap` with keys as `(account_name, filesystem)` tuples and values as
/// vectors of `(blob_path, original_path)` tuples.
fn group_paths_by_container(
    paths: impl IntoIterator<Item = impl AsRef<str>>,
) -> Result<HashMap<(String, String), Vec<AdlsLocation>>, InvalidLocationError> {
    let mut grouped_paths: HashMap<(String, String), Vec<AdlsLocation>> = HashMap::new();

    for p in paths {
        let path = p.as_ref();
        let adls_location = AdlsLocation::try_from_str(path, true)?;

        // Make sure we have a key (blob path)
        require_key(&adls_location)?;

        let account = adls_location.account_name().to_string();
        let filesystem = adls_location.filesystem().to_string();

        grouped_paths
            .entry((account, filesystem))
            .or_default()
            .push(adls_location);
    }

    Ok(grouped_paths)
}
