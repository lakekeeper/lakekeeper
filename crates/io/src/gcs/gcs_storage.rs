use std::{
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
};

use bytes::Bytes;
use futures::{stream, StreamExt as _};
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
use tokio;

use crate::{
    calculate_ranges, delete_not_found_is_ok, execute_with_parallelism,
    gcs::{gcs_error::parse_error, GcsLocation},
    safe_usize_to_i32, safe_usize_to_i64, validate_file_size, DeleteBatchError, DeleteError,
    ErrorKind, IOError, InvalidLocationError, LakekeeperStorage, Location, ReadError, WriteError,
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
    ) -> Result<(), DeleteBatchError> {
        // Create futures for parallel deletion
        let delete_futures: Vec<_> = paths
            .into_iter()
            .map(|path| {
                let path = path.as_ref();
                let location = GcsLocation::try_from_str(path)?;
                let client = self.client.clone();

                let future = async move {
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

                    Ok::<(GcsLocation, Option<IOError>), DeleteBatchError>((location, result.err()))
                };

                Ok::<_, DeleteBatchError>(future)
            })
            .collect::<Result<Vec<_>, _>>()?;

        let completed_batches = AtomicU64::new(0);
        let total_batches = delete_futures.len();

        let delete_stream = execute_with_parallelism(delete_futures, 100).map(|result| {
            result
                .map_err(|join_err| {
                    DeleteBatchError::IOError(IOError::new(
                        ErrorKind::Unexpected,
                        format!("Task join error during batch delete: {join_err}"),
                        "batch_operation".to_string(),
                    ))
                })
                .and_then(|inner_result| inner_result)
        });
        tokio::pin!(delete_stream);

        while let Some(result) = delete_stream.next().await {
            let completed_batch = completed_batches.fetch_add(1, Ordering::Relaxed);
            let (_location, error_opt) = result?;

            match error_opt {
                None => {}
                Some(error) => {
                    return Err(DeleteBatchError::IOError(error.with_context(format!(
                        "Delete batch {completed_batch} out of {total_batches} failed",
                    ))));
                }
            }
        }

        Ok(())
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

            let upload_futures: Vec<_> = chunks
                .into_iter()
                .map(|(offset, chunk)| {
                    let path = path.to_string();
                    let upload_client_cloned = upload_client.clone();
                    let len_bytes = bytes.len() as u64;

                    async move {
                        let chunk_size = ChunkSize::new(
                            offset as u64,
                            offset as u64 + chunk.len() as u64 - 1,
                            Some(len_bytes),
                        );
                        upload_client_cloned
                            .upload_multiple_chunk(chunk, &chunk_size)
                            .await
                            .map_err(|e| {
                                WriteError::IOError(parse_error(e, &path).with_context(format!(
                                    "Failed to upload chunk at offset {offset}"
                                )))
                            })
                    }
                })
                .collect();

            // Wait for all uploads to complete
            let upload_stream = execute_with_parallelism(upload_futures, 1).map(|result| {
                result.map_err(|join_err| {
                    WriteError::IOError(IOError::new(
                        ErrorKind::Unexpected,
                        format!("Task join error during multipart upload: {join_err}"),
                        "multipart_upload".to_string(),
                    ))
                })
            });
            tokio::pin!(upload_stream);

            while let Some(result) = upload_stream.next().await {
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

        // Calculate the chunks, starting from 0 up to the size of the object in DEFAULT_BYTES_PER_REQUEST chunks
        let file_size = validate_file_size(status.size, location.as_str())?;
        let chunks = calculate_ranges(file_size, DEFAULT_BYTES_PER_REQUEST);

        let download_futures: Vec<_> = chunks
            .into_iter()
            .enumerate()
            .map(|(chunk_index, (start, end))| {
                let client = self.client.clone();
                let request = request.clone();
                let path = path.to_string();

                async move {
                    let range = Range(Some(start as u64), Some(end as u64));

                    let chunk_data =
                        client
                            .download_object(&request, &range)
                            .await
                            .map_err(|e| {
                                ReadError::IOError(parse_error(e, &path).with_context(format!(
                                    "Failed to download chunk {chunk_index} (bytes {start}-{end})"
                                )))
                            })?;

                    Ok::<(usize, Bytes), ReadError>((chunk_index, Bytes::from(chunk_data)))
                }
            })
            .collect();

        let download_stream = execute_with_parallelism(download_futures, 10).map(|result| {
            result
                .map_err(|join_err| {
                    ReadError::IOError(IOError::new(
                        ErrorKind::Unexpected,
                        format!("Task join error during parallel download: {join_err}"),
                        "parallel_download".to_string(),
                    ))
                })
                .and_then(|inner_result| inner_result)
        });
        tokio::pin!(download_stream);

        // Use the shared utility function to assemble chunks
        let bytes =
            crate::assemble_chunks(download_stream, file_size, DEFAULT_BYTES_PER_REQUEST).await?;

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
