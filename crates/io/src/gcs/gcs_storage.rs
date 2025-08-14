use std::{
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
};

use bytes::Bytes;
use futures::{
    stream::{self, FuturesUnordered},
    StreamExt as _,
};
use google_cloud_storage::{
    client::Client,
    http::{
        objects::{
            delete::DeleteObjectRequest,
            download::Range,
            list::ListObjectsRequest,
            upload::{Media, UploadObjectRequest, UploadType},
            Object,
        },
        resumable_upload_client::{ChunkSize, UploadStatus},
    },
};

use crate::{
    calculate_ranges, delete_not_found_is_ok,
    gcs::{gcs_error::parse_error, GcsLocation},
    safe_usize_to_i32, safe_usize_to_i64, validate_file_size, BatchDeleteError, BatchDeleteResult,
    DeleteBatchFatalError, DeleteError, ErrorKind, IOError, InvalidLocationError,
    LakekeeperStorage, Location, ReadError, WriteError,
};

const MAX_BYTES_PER_REQUEST: usize = 25 * 1024 * 1024;
const DEFAULT_BYTES_PER_REQUEST: usize = 16 * 1024 * 1024;

#[derive(Clone)]
pub struct GcsStorage {
    client: Client,
}

impl std::fmt::Debug for GcsStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GCSStorage")
            .field("client", &"<redacted>") // Does not implement Debug
            .finish()
    }
}

impl GcsStorage {
    /// Create a new `GCSStorage` instance with the provided client.
    #[must_use]
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    /// Get the underlying GCS client.
    #[must_use]
    pub fn client(&self) -> &Client {
        &self.client
    }
}

impl LakekeeperStorage for GcsStorage {
    async fn delete(&self, path: impl AsRef<str>) -> Result<(), DeleteError> {
        let path = path.as_ref();
        let location = GcsLocation::try_from_str(path)?;

        let delete_request = DeleteObjectRequest {
            bucket: location.bucket_name().to_string(),
            object: location.object_name(),
            ..Default::default()
        };

        let result = self
            .client
            .delete_object(&delete_request)
            .await
            .map_err(|e| parse_error(e, location.as_str()));

        delete_not_found_is_ok(result)?;

        Ok(())
    }

    // ToDo: Switch to BlobBatch delete once supported by rust SDK.
    async fn delete_batch(
        &self,
        paths: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Result<BatchDeleteResult, DeleteBatchFatalError> {
        // Create futures for parallel deletion
        let mut delete_futures = FuturesUnordered::new();
        let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(1000));

        // Create delete operations for each path
        for path in paths {
            let path = path.as_ref();
            let location = GcsLocation::try_from_str(path)?;

            let semaphore = semaphore.clone();
            let client = self.client.clone();

            let future = async move {
                let _permit = semaphore.acquire().await.map_err(|e| {
                    DeleteBatchFatalError::IOError(IOError::new(
                        ErrorKind::Unexpected,
                        format!("Failed to acquire semaphore permit for GCS deletion: {e}"),
                        location.as_str().to_string(),
                    ))
                })?;

                let delete_request = DeleteObjectRequest {
                    bucket: location.bucket_name().to_string(),
                    object: location.object_name(),
                    ..Default::default()
                };

                let result = client
                    .delete_object(&delete_request)
                    .await
                    .map_err(|e| parse_error(e, location.as_str()));

                // Convert 404 (not found) to success for idempotent behavior
                let result = delete_not_found_is_ok(result);

                Ok::<(GcsLocation, Option<IOError>), DeleteBatchFatalError>((
                    location,
                    result.err(),
                ))
            };

            delete_futures.push(future);
        }

        // Process all futures as they complete
        let counter = AtomicU64::new(0);
        let mut successful_paths = Vec::new();
        let mut errors = Vec::new();

        while let Some(result) = delete_futures.next().await {
            let (location, error_opt) = result?;

            // Increment the counter for each processed operation
            let batch_index = counter.fetch_add(1, Ordering::Relaxed);

            match error_opt {
                None => {
                    // Successful deletion
                    successful_paths.push(location.as_str().to_string());
                }
                Some(error) => {
                    // Individual deletion error
                    if batch_index == 0 {
                        // If this is the first batch and it failed, return early
                        return Err(DeleteBatchFatalError::IOError(error));
                    }

                    // Otherwise, track the error but continue processing
                    let batch_error = BatchDeleteError::new(
                        location.as_str().to_string(),
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
            Ok(BatchDeleteResult::PartialFailure {
                successful_paths,
                errors,
            })
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn write(&self, path: impl AsRef<str>, bytes: Bytes) -> Result<(), WriteError> {
        let path = path.as_ref();
        let location = GcsLocation::try_from_str(path)?;

        let upload_request = UploadObjectRequest {
            bucket: location.bucket_name().to_string(),
            ..Default::default()
        };

        if bytes.len() < MAX_BYTES_PER_REQUEST {
            let mut media = Media::new(location.object_name().to_string());
            media.content_length = Some(bytes.len() as u64);
            let upload_type = UploadType::Simple(media);

            self.client
                .upload_object(&upload_request, bytes, &upload_type)
                .await
                .map(|_| ())
                .map_err(|e| {
                    parse_error(e, location.as_str())
                        .with_context("Failed to upload single part object.")
                        .into()
                })
        } else {
            let chunks: Vec<_> = bytes
                .chunks(DEFAULT_BYTES_PER_REQUEST)
                .enumerate()
                .map(|(i, chunk)| {
                    let offset = i * DEFAULT_BYTES_PER_REQUEST;
                    (offset, Bytes::copy_from_slice(chunk))
                })
                .collect();

            let upload_type = UploadType::Multipart(Box::new(Object {
                name: location.object_name(),
                bucket: location.bucket_name().to_string(),
                size: safe_usize_to_i64(bytes.len(), location.as_str())?,
                ..Default::default()
            }));
            let upload_client = self
                .client
                .prepare_resumable_upload(&upload_request, &upload_type)
                .await
                .map_err(|e| {
                    parse_error(e, location.as_str())
                        .with_context("Failed to prepare resumable upload.")
                })?;

            let mut upload_futures = FuturesUnordered::new();
            let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(1));

            for (offset, chunk) in chunks {
                let semaphore = semaphore.clone();
                let path = path.to_string();
                let upload_client_cloned = upload_client.clone();
                let len_bytes = bytes.len() as u64;

                let future = async move {
                    let _permit = semaphore.acquire().await.map_err(|e| {
                            WriteError::IOError(IOError::new(
                                ErrorKind::Unexpected,
                                format!(
                                "Semaphore closed unexpectedly for GCS download chunk at offset {offset}: {e}",
                            ),
                                path.clone(),
                            ))
                        })?;

                    let chunk_size = ChunkSize::new(
                        offset as u64,
                        offset as u64 + chunk.len() as u64 - 1,
                        Some(len_bytes),
                    );
                    upload_client_cloned
                        .upload_multiple_chunk(chunk, &chunk_size)
                        .await
                        .map_err(|e| {
                            WriteError::IOError(
                                parse_error(e, &path).with_context(format!(
                                    "Failed to upload chunk at offset {offset}"
                                )),
                            )
                        })
                };

                upload_futures.push(future);
            }

            // Wait for all uploads to complete
            while let Some(result) = upload_futures.next().await {
                let _status = result?;
            }

            let status =
                upload_client
                    .status(Some(bytes.len() as u64))
                    .await
                    .map_err(|e| {
                        WriteError::IOError(parse_error(e, location.as_str()).with_context(
                            "Failed to get upload status after uploading all chunks.",
                        ))
                    })?;

            match status {
                UploadStatus::Ok(_) => {}
                UploadStatus::ResumeIncomplete(i) => {
                    return Err(WriteError::IOError(IOError::new(
                            ErrorKind::Unexpected,
                            format!(
                                "Multipart upload should be completed, but returned status is `ResumeIncomplete` with uploaded range {i:?}"
                            ),
                            location.as_str().to_string(),
                        )));
                }
                UploadStatus::NotStarted => {
                    return Err(WriteError::IOError(IOError::new(
                        ErrorKind::Unexpected,
                        "Multipart upload should be completed, but returned status is `NotStarted`"
                            .to_string(),
                        location.as_str().to_string(),
                    )));
                }
            }

            Ok(())
        }
    }

    async fn read_single(&self, path: impl AsRef<str> + Send) -> Result<Bytes, ReadError> {
        let path = path.as_ref();
        let location = GcsLocation::try_from_str(path)?;

        let request = google_cloud_storage::http::objects::get::GetObjectRequest {
            bucket: location.bucket_name().to_string(),
            object: location.object_name(),
            ..Default::default()
        };

        let range = Range::default();
        let data = self
            .client
            .download_object(&request, &range)
            .await
            .map_err(|e| {
                ReadError::IOError(
                    parse_error(e, location.as_str())
                        .with_context("Failed to download full object."),
                )
            })?;

        Ok(bytes::Bytes::from(data))
    }

    async fn read(&self, path: impl AsRef<str>) -> Result<Bytes, ReadError> {
        let path = path.as_ref();
        let location = GcsLocation::try_from_str(path)?;

        let mut request = google_cloud_storage::http::objects::get::GetObjectRequest {
            bucket: location.bucket_name().to_string(),
            object: location.object_name(),
            ..Default::default()
        };

        let status = self.client.get_object(&request).await.map_err(|e| {
            ReadError::IOError(
                parse_error(e, location.as_str())
                    .with_context("Failed to get metadata about the object."),
            )
        })?;

        let file_size = validate_file_size(status.size, location.as_str())?;

        if file_size < MAX_BYTES_PER_REQUEST {
            let range = Range::default();
            // If the object is small enough, we can read it in one go
            let data = self
                .client
                .download_object(&request, &range)
                .await
                .map_err(|e| {
                    ReadError::IOError(
                        parse_error(e, location.as_str())
                            .with_context("Failed to download full object."),
                    )
                })?;

            return Ok(bytes::Bytes::from(data));
        }

        request.generation = Some(status.generation);

        let download_futures = FuturesUnordered::new();
        let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(10));

        // Calculate the chunks, starting from 0 up to the size of the object in DEFAULT_BYTES_PER_REQUEST chunks
        let file_size = validate_file_size(status.size, location.as_str())?;
        let chunks = calculate_ranges(file_size, DEFAULT_BYTES_PER_REQUEST);
        for (chunk_index, (start, end)) in chunks.into_iter().enumerate() {
            let semaphore = semaphore.clone();
            let client = self.client.clone();
            let request = request.clone();
            let path = path.to_string();

            let future = async move {
                let _permit = semaphore.acquire().await.map_err(|e| {
                    ReadError::IOError(IOError::new(
                        ErrorKind::Unexpected,
                        format!("Semaphore closed unexpectedly for GCS download chunk {chunk_index}: {e}"),
                        path.clone(),
                    ))
                })?;

                let range = Range(Some(start as u64), Some(end as u64));

                let chunk_data = client
                    .download_object(&request, &range)
                    .await
                    .map_err(|e| {
                        ReadError::IOError(parse_error(e, &path).with_context(format!(
                            "Failed to download chunk {chunk_index} (bytes {start}-{end})"
                        )))
                    })?;

                Ok::<(usize, Bytes), ReadError>((chunk_index, Bytes::from(chunk_data)))
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
        let path = path.as_ref();
        let location = GcsLocation::try_from_str(path)?;

        // Ensure the path ends with '/' for proper prefix matching
        let prefix = format!("{}/", location.object_name().trim_end_matches('/'));

        let list_request = ListObjectsRequest {
            bucket: location.bucket_name().to_string(),
            prefix: Some(prefix),
            max_results: page_size.and_then(|size| safe_usize_to_i32(size, location.as_str()).ok()),
            ..Default::default()
        };

        let client = self.client.clone();
        let bucket_name = location.bucket_name().to_string();

        let stream = stream::try_unfold(
            (Some(list_request), false), // (request, is_done)
            move |(request_opt, is_done)| {
                let client = client.clone();
                let bucket_name = bucket_name.clone();

                async move {
                    let Some(request) = request_opt else {
                        return Ok(None); // No more requests to process
                    };

                    if is_done {
                        return Ok(None);
                    }

                    let response = client
                        .list_objects(&request)
                        .await
                        .map_err(|e| parse_error(e, &bucket_name))?;

                    // Convert GCS objects to Location objects
                    let locations: Vec<Location> = response
                        .items
                        .unwrap_or_default()
                        .into_iter()
                        .map(|object| {
                            let gcs_path = format!("gs://{}/{}", bucket_name, object.name);
                            Location::from_str(&gcs_path).map_err(|e| {
                                IOError::new(
                                    ErrorKind::Unexpected,
                                    format!(
                                        "Failed to parse GCS object path returned from list: {e}",
                                    ),
                                    gcs_path,
                                )
                            })
                        })
                        .collect::<Result<_, _>>()?;

                    // Prepare next request if there's a next page
                    let next_state = if let Some(next_page_token) = response.next_page_token {
                        let mut next_request = request;
                        next_request.page_token = Some(next_page_token);
                        (Some(next_request), false)
                    } else {
                        (None, true) // No more pages
                    };

                    Ok(Some((locations, next_state)))
                }
            },
        );

        Ok(stream.boxed())
    }
}
