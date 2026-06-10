use std::{
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use bytes::{Bytes, BytesMut};
use futures::{StreamExt as _, stream};
use google_cloud_storage::{
    client::Client,
    http::{
        objects::{
            Object,
            delete::DeleteObjectRequest,
            download::Range,
            get::GetObjectRequest,
            list::ListObjectsRequest,
            upload::{Media, UploadObjectRequest, UploadType},
        },
        resumable_upload_client::{ChunkSize, ResumableUploadClient, UploadStatus},
    },
};

use crate::{
    DeleteBatchError, DeleteError, ErrorKind, FileInfo, IOError, InvalidLocationError,
    LakekeeperFileWrite, LakekeeperStorage, Location, ReadError, WriteError,
    delete_not_found_is_ok, execute_with_parallelism,
    gcs::{GcsLocation, gcs_error::parse_error},
    safe_usize_to_i32, safe_usize_to_i64, validate_file_size,
};

const MAX_BYTES_PER_REQUEST: usize = 25 * 1024 * 1024;
const DEFAULT_BYTES_PER_REQUEST: usize = 16 * 1024 * 1024;
/// Upper bound on best-effort cleanup work spawned from `Drop`. The cancel
/// future is dropped on elapse; we still log the timeout so an orphaned
/// resumable session is observable.
const DROP_CANCEL_DURATION: Duration = Duration::from_secs(10);

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

#[async_trait::async_trait]
impl LakekeeperStorage for GcsStorage {
    async fn delete(&self, path: &str) -> Result<(), DeleteError> {
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
    async fn delete_batch(&self, paths: &[String]) -> Result<(), DeleteBatchError> {
        // Create futures for parallel deletion
        let delete_futures: Vec<_> = paths
            .iter()
            .map(|path| {
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

        let delete_stream = execute_with_parallelism(delete_futures, 16).map(|result| {
            result
                .map_err(|join_err| {
                    DeleteBatchError::IOError(
                        IOError::new_without_location(
                            ErrorKind::Unexpected,
                            format!("Task join error during batch delete: {join_err}"),
                        )
                        .with_context("GCS batch delete"),
                    )
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

    async fn write(&self, path: &str, bytes: Bytes) -> Result<(), WriteError> {
        let location = GcsLocation::try_from_str(path)?;

        let total_bytes = bytes.len();
        if total_bytes < MAX_BYTES_PER_REQUEST {
            return upload_simple(&self.client, &location, bytes).await;
        }

        let upload_client = prepare_resumable(
            &self.client,
            &location,
            safe_usize_to_i64(total_bytes, location.as_str())?,
        )
        .await?;
        // GCS resumable upload sessions require chunks to be committed in strict offset order: the server tracks the last-committed offset
        // and rejects PUTs that arrive ahead of it.
        // If chunk-upload throughput on this path is ever shown by measurement to be a real bottleneck, we should consider GCS parallel composite uploads:
        // upload each chunk as a temporary object in parallel, then issue `compose_object` to concatenate them into the final object and
        // delete the temporaries. That is a substantial redesign and should be carefully considered.
        // Zero-copy chunking: `bytes.slice(range)` produces an owned
        // refcounted view handed off to each `upload_chunk` call.
        let total_chunks = total_bytes.div_ceil(DEFAULT_BYTES_PER_REQUEST);
        let mut first_error: Option<WriteError> = None;
        for (chunk_index, range) in crate::chunk_ranges(total_bytes, DEFAULT_BYTES_PER_REQUEST) {
            // Per GCS protocol only last chunk writes total_size
            let is_final_chunk = chunk_index + 1 == total_chunks;
            let total_size = if is_final_chunk {
                Some(total_bytes as u64)
            } else {
                None
            };
            let offset = range.start as u64;
            let chunk = bytes.slice(range);
            if let Err(e) = upload_chunk(&upload_client, &location, offset, chunk, total_size)
                .await
                .map_err(|e| match e {
                    WriteError::IOError(io) => WriteError::IOError(
                        io.with_context(format!("Multipart upload chunk {chunk_index}")),
                    ),
                    other @ WriteError::InvalidLocation(_) => other,
                })
            {
                first_error = Some(e);
                break;
            }
        }
        if let Some(err) = first_error {
            cancel_resumable_logged_infallible(
                upload_client,
                &location,
                "sequential multipart write failed",
            )
            .await;
            return Err(err);
        }

        match verify_resumable_complete(&upload_client, &location, total_bytes as u64).await {
            Ok(()) => Ok(()),
            Err(e) => {
                cancel_resumable_logged_infallible(
                    upload_client,
                    &location,
                    "verify_resumable_complete failed after sequential write",
                )
                .await;
                Err(e)
            }
        }
    }

    async fn writer(&self, path: &str) -> Result<Box<dyn LakekeeperFileWrite>, WriteError> {
        let location = GcsLocation::try_from_str(path)?;
        Ok(Box::new(GcsFileWrite {
            client: self.client.clone(),
            location,
            state: GcsWriterState::Buffering(BytesMut::new()),
        }))
    }

    async fn metadata(&self, path: &str) -> Result<FileInfo, ReadError> {
        let location = GcsLocation::try_from_str(path)?;
        let head_response = head(&self.client, &location).await?;
        let size = crate::size_to_u64(head_response.size, location.as_str());
        let last_modified = head_response
            .updated
            .as_ref()
            .and_then(parse_offsetdatetime);

        Ok(FileInfo::new(
            last_modified,
            location.location().clone(),
            size,
        ))
    }

    async fn read_single(&self, path: &str) -> Result<Bytes, ReadError> {
        let location = GcsLocation::try_from_str(path)?;
        let request = build_get_object_request(&location);

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

    async fn read(&self, path: &str) -> Result<Bytes, ReadError> {
        let gcs_location = GcsLocation::try_from_str(path)?;

        let head_response = head(&self.client, &gcs_location).await?;
        let file_size = validate_file_size(head_response.size, gcs_location.as_str())?;

        if file_size == 0 {
            return Ok(Bytes::new());
        }

        if file_size < MAX_BYTES_PER_REQUEST {
            // If the file is small enough, read it in a single request
            let request = build_get_object_request(&gcs_location);
            return fetch_range(&self.client, &request, None..None).await;
        }

        parallel_chunked_read_with_fixed_generation(
            &self.client,
            &gcs_location,
            0,
            file_size,
            Some(head_response.generation),
        )
        .await
    }

    async fn read_range(
        &self,
        path: &str,
        range: std::ops::Range<u64>,
    ) -> Result<Bytes, ReadError> {
        let gcs_location = GcsLocation::try_from_str(path)?;
        if range.end < range.start {
            return Err(ReadError::IOError(IOError::new(
                ErrorKind::ConditionNotMatch,
                format!(
                    "Invalid range: start ({}) > end ({})",
                    range.start, range.end
                ),
                gcs_location.as_str().into(),
            )));
        }
        if range.is_empty() {
            return Ok(Bytes::new());
        }

        let range_size_u64 = range.end - range.start;
        let range_size = usize::try_from(range_size_u64).map_err(|_| {
            ReadError::IOError(IOError::new(
                ErrorKind::ConditionNotMatch,
                format!("Range size {range_size_u64} too large for this platform"),
                path.to_string(),
            ))
        })?;

        if range_size <= MAX_BYTES_PER_REQUEST {
            let request = build_get_object_request(&gcs_location);
            return fetch_range(&self.client, &request, Some(range.start)..Some(range.end)).await;
        }

        let head_response = head(&self.client, &gcs_location).await?;
        parallel_chunked_read_with_fixed_generation(
            &self.client,
            &gcs_location,
            range.start,
            range_size,
            Some(head_response.generation),
        )
        .await
    }

    async fn list(
        &self,
        path: &str,
        page_size: Option<usize>,
    ) -> Result<futures::stream::BoxStream<'_, Result<Vec<FileInfo>, IOError>>, InvalidLocationError>
    {
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
                    let file_infos: Vec<FileInfo> = response
                        .items
                        .unwrap_or_default()
                        .into_iter()
                        .map(try_parse_file_info(&bucket_name))
                        .collect::<Result<_, _>>()?;

                    // Prepare next request if there's a next page
                    let next_state = if let Some(next_page_token) = response.next_page_token {
                        let mut next_request = request;
                        next_request.page_token = Some(next_page_token);
                        (Some(next_request), false)
                    } else {
                        (None, true) // No more pages
                    };

                    Ok(Some((file_infos, next_state)))
                }
            },
        );

        Ok(stream.boxed())
    }
}

/// Convert a `time::OffsetDateTime` to a `chrono::DateTime<Utc>`, preserving
/// nanosecond precision. Returns `None` if the seconds are out of range.
fn parse_offsetdatetime(t: &time::OffsetDateTime) -> Option<chrono::DateTime<chrono::Utc>> {
    chrono::DateTime::from_timestamp(t.unix_timestamp(), t.nanosecond())
}

fn try_parse_file_info(bucket_name: &str) -> impl FnMut(Object) -> Result<FileInfo, IOError> {
    move |object| {
        let gcs_path = format!("gs://{}/{}", bucket_name, object.name);
        let location = Location::from_str(&gcs_path).map_err(|e| {
            IOError::new(
                ErrorKind::Unexpected,
                format!("Failed to parse GCS object path returned from list: {e}"),
                gcs_path.clone(),
            )
        })?;
        let last_modified = object.updated.as_ref().and_then(parse_offsetdatetime);
        let size = crate::size_to_u64(object.size, &gcs_path);
        Ok(FileInfo::new(last_modified, location, size))
    }
}

/// Fetch range of bytes with `range`
///
/// - Some(n)..Some(m) -> bytes `[n, m)`
/// - None..Some(n) -> last `m-1` bytes
/// - Some(n)..None -> bytes `[n..end_of_file]`
/// - None..None -> whole file
///
/// Checks on range are defered to `client`
async fn fetch_range(
    client: &Client,
    request: &GetObjectRequest,
    range: std::ops::Range<Option<u64>>,
) -> Result<Bytes, ReadError> {
    // `download_object`s' `range` is inclusive, so subtract 1
    let r = Range(range.start, range.end.map(|end| end - 1));
    let data = client.download_object(request, &r).await.map_err(|e| {
        ReadError::IOError(
            parse_error(e, &request.object).with_context("Failed to download byte range."),
        )
    })?;
    Ok(Bytes::from(data))
}

async fn head(client: &Client, location: &GcsLocation) -> Result<Object, ReadError> {
    let request = build_get_object_request(location);
    client.get_object(&request).await.map_err(|e| {
        ReadError::IOError(
            parse_error(e, location.as_str())
                .with_context("Failed to get metadata about the object."),
        )
    })
}

fn build_get_object_request(location: &GcsLocation) -> GetObjectRequest {
    GetObjectRequest {
        bucket: location.bucket_name().to_string(),
        object: location.object_name(),
        ..Default::default()
    }
}

async fn parallel_chunked_read_with_fixed_generation(
    client: &Client,
    gcs_location: &GcsLocation,
    range_start: u64,
    range_size: usize,
    generation: Option<i64>,
) -> Result<Bytes, ReadError> {
    if range_size == 0 {
        return Ok(Bytes::new());
    }

    let mut request = build_get_object_request(gcs_location);
    request.generation = generation;

    let client = client.clone();
    crate::parallel_chunked_read(
        range_size,
        DEFAULT_BYTES_PER_REQUEST,
        10,
        gcs_location.as_str(),
        move |rel_start, rel_end, chunk_index| {
            let client = client.clone();
            let request = request.clone();
            let abs_start = range_start + rel_start as u64;
            let abs_end = range_start + rel_end as u64 + 1;
            async move {
                let chunk = fetch_range(&client, &request, Some(abs_start)..Some(abs_end))
                    .await
                    .map_err(|e| match e {
                        ReadError::IOError(io) => ReadError::IOError(io.with_context(format!(
                            "Failed to download chunk {chunk_index} (bytes {abs_start}-{abs_end})"
                        ))),
                        invalid_location_error @ ReadError::InvalidLocation(_) => {
                            invalid_location_error
                        }
                    })?;
                Ok((chunk_index, chunk))
            }
        },
    )
    .await
}

/// Upload a small object in a single request.
async fn upload_simple(
    client: &Client,
    location: &GcsLocation,
    bytes: Bytes,
) -> Result<(), WriteError> {
    let upload_request = UploadObjectRequest {
        bucket: location.bucket_name().to_string(),
        ..Default::default()
    };
    let mut media = Media::new(location.object_name().clone());
    media.content_length = Some(bytes.len() as u64);
    let upload_type = UploadType::Simple(media);
    client
        .upload_object(&upload_request, bytes, &upload_type)
        .await
        .map(|_| ())
        .map_err(|e| {
            parse_error(e, location.as_str())
                .with_context("Failed to upload single part object.")
                .into()
        })
}

/// Initiate a resumable upload session for the given object.
///
/// `total_size_hint` is the size the caller expects the final object to
/// be. For streaming writers the size may not be known up-front; pass `0`
/// (the GCS API ignores this value when individual chunks specify their
/// own totals).
async fn prepare_resumable(
    client: &Client,
    location: &GcsLocation,
    total_size_hint: i64,
) -> Result<ResumableUploadClient, WriteError> {
    let upload_request = UploadObjectRequest {
        bucket: location.bucket_name().to_string(),
        ..Default::default()
    };
    let upload_type = UploadType::Multipart(Box::new(Object {
        name: location.object_name(),
        bucket: location.bucket_name().to_string(),
        size: total_size_hint,
        ..Default::default()
    }));
    client
        .prepare_resumable_upload(&upload_request, &upload_type)
        .await
        .map_err(|e| {
            parse_error(e, location.as_str())
                .with_context("Failed to prepare resumable upload.")
                .into()
        })
}

/// Upload a single chunk of a resumable upload.
///
/// `total_size` is `Some(total)` for the final chunk and `None` for any
/// preceding chunk; the GCS protocol uses this to mark the upload as
/// complete.
async fn upload_chunk(
    upload_client: &ResumableUploadClient,
    location: &GcsLocation,
    offset: u64,
    chunk: Bytes,
    total_size: Option<u64>,
) -> Result<(), WriteError> {
    let chunk_len = chunk.len() as u64;
    // This is a defensive check against future refactoring. Currently `chunk_length` cannot be 0.
    if chunk_len == 0 {
        return Err(WriteError::IOError(IOError::new(
            ErrorKind::ConditionNotMatch,
            "Internal invariant violated: calculated chunk length is 0",
            location.to_string(),
        )));
    }
    let chunk_size = ChunkSize::new(offset, offset + chunk_len - 1, total_size);
    upload_client
        .upload_multiple_chunk(chunk, &chunk_size)
        .await
        .map(|_| ())
        .map_err(|e| {
            WriteError::IOError(parse_error(e, location.as_str()).with_context(format!(
                "Failed to upload chunk (bytes {offset}-{end})",
                end = offset + chunk_len
            )))
        })
}

/// Best-effort resumable-upload cancel that swallows the cancel error after
/// logging it.
///
/// Use on cleanup paths where the caller propagates a different (original)
/// error and the cancel failure is unactionable: one-shot bulk write, a
/// `verify_resumable_complete` failure, or `Drop`. The `context` field is
/// included in the warn log to disambiguate which cancel site triggered the
/// cleanup. Consumes the `upload_client` because `ResumableUploadClient::cancel`
/// takes `self`.
async fn cancel_resumable_logged_infallible(
    upload_client: ResumableUploadClient,
    location: &GcsLocation,
    context: &str,
) {
    if let Err(e) = upload_client.cancel().await {
        tracing::warn!(
            location = %location,
            error = ?e,
            context = %context,
            "Failed to cancel GCS resumable upload session. Incomplete upload may exist in target location until the session expires.",
        );
    }
}

/// Issue a final status query against the resumable upload session and
/// surface anything other than `UploadStatus::Ok` as an error.
async fn verify_resumable_complete(
    upload_client: &ResumableUploadClient,
    location: &GcsLocation,
    total_bytes: u64,
) -> Result<(), WriteError> {
    let status = upload_client.status(Some(total_bytes)).await.map_err(|e| {
        WriteError::IOError(
            parse_error(e, location.as_str())
                .with_context("Failed to get upload status after uploading all chunks."),
        )
    })?;
    match status {
        UploadStatus::Ok(_) => Ok(()),
        UploadStatus::ResumeIncomplete(i) => Err(WriteError::IOError(IOError::new(
            ErrorKind::Unexpected,
            format!(
                "Multipart upload should be completed, but returned status is `ResumeIncomplete` with uploaded range {i:?}"
            ),
            location.to_string(),
        ))),
        UploadStatus::NotStarted => Err(WriteError::IOError(IOError::new(
            ErrorKind::Unexpected,
            "Multipart upload should be completed, but returned status is `NotStarted`".to_string(),
            location.to_string(),
        ))),
    }
}

/// Streaming writer for GCS.
///
/// Buffers bytes locally; promotes to a resumable upload session once
/// `MAX_BYTES_PER_REQUEST` has accumulated. Each chunk flush uses
/// `DEFAULT_BYTES_PER_REQUEST` (a multiple of 256 `KiB` as required by the
/// GCS resumable protocol; the final chunk has no minimum).
///
/// Zero-copy invariant: incoming `Bytes` are appended into a local
/// `BytesMut` (one copy, unavoidable to span multiple `write` calls);
/// each chunk is then handed off to `upload_chunk` zero-copy via
/// `BytesMut::split_to(N).freeze()`.
pub(crate) struct GcsFileWrite {
    client: Client,
    location: GcsLocation,
    state: GcsWriterState,
}

impl std::fmt::Debug for GcsFileWrite {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GcsFileWrite")
            .field("location", &self.location)
            .field("state", &self.state)
            .finish_non_exhaustive()
    }
}

enum GcsWriterState {
    Buffering(BytesMut),
    Resumable {
        upload_client: ResumableUploadClient,
        offset: u64,
        buffer: BytesMut,
    },
    Closed,
    Aborted,
    AbortFailed,
}

impl std::fmt::Debug for GcsWriterState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GcsWriterState::Buffering(buffer) => f
                .debug_tuple("Buffering")
                .field(&format_args!("{} bytes", buffer.len()))
                .finish(),
            GcsWriterState::Resumable { offset, buffer, .. } => f
                .debug_struct("Resumable")
                .field("offset", offset)
                .field("buffered_bytes", &buffer.len())
                .finish(),
            GcsWriterState::Closed => f.write_str("Closed"),
            GcsWriterState::Aborted => f.write_str("Aborted"),
            GcsWriterState::AbortFailed => f.write_str("AbortFailed"),
        }
    }
}

#[async_trait::async_trait]
impl LakekeeperFileWrite for GcsFileWrite {
    async fn write(&mut self, bytes_in: Bytes) -> Result<(), WriteError> {
        match &mut self.state {
            GcsWriterState::Closed => {
                return Err(WriteError::IOError(IOError::new(
                    ErrorKind::ConditionNotMatch,
                    "Cannot write to closed writer",
                    self.location.to_string(),
                )));
            }
            GcsWriterState::Aborted => {
                return Err(WriteError::IOError(IOError::new(
                    ErrorKind::ConditionNotMatch,
                    "Cannot write to aborted writer",
                    self.location.to_string(),
                )));
            }
            GcsWriterState::AbortFailed => {
                return Err(WriteError::IOError(IOError::new(
                    ErrorKind::ConditionNotMatch,
                    "Cannot write to writer that failed to abort",
                    self.location.to_string(),
                )));
            }
            GcsWriterState::Buffering(buffer) => {
                buffer.extend_from_slice(&bytes_in);
                if buffer.len() < MAX_BYTES_PER_REQUEST {
                    return Ok(());
                }
                let upload_client = prepare_resumable(&self.client, &self.location, 0).await?;
                let buffer = std::mem::take(buffer);
                self.state = GcsWriterState::Resumable {
                    upload_client,
                    offset: 0,
                    buffer,
                };
                self.flush_resumable_buffer().await?;
            }
            GcsWriterState::Resumable { buffer, .. } => {
                buffer.extend_from_slice(&bytes_in);
                self.flush_resumable_buffer().await?;
            }
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<(), WriteError> {
        let state = std::mem::replace(&mut self.state, GcsWriterState::Closed);
        match state {
            GcsWriterState::Closed | GcsWriterState::Aborted | GcsWriterState::AbortFailed => {
                Err(WriteError::IOError(IOError::new(
                    ErrorKind::ConditionNotMatch,
                    "Writer already closed or aborted",
                    self.location.to_string(),
                )))
            }
            GcsWriterState::Buffering(buffer) => {
                upload_simple(&self.client, &self.location, buffer.freeze()).await
            }
            GcsWriterState::Resumable {
                upload_client,
                offset,
                buffer,
            } => {
                // Upload any remaining tail bytes, then finalize via
                // `verify_resumable_complete`. When the buffer is empty
                // only the finalizing status query is needed. On any
                // failure the resumable session is best-effort cancelled
                // so it does not linger until the GCS-side expiry.
                let total_bytes = offset + buffer.len() as u64;
                if !buffer.is_empty()
                    && let Err(upload_error) = upload_chunk(
                        &upload_client,
                        &self.location,
                        offset,
                        buffer.freeze(),
                        Some(total_bytes),
                    )
                    .await
                {
                    cancel_resumable_logged_infallible(
                        upload_client,
                        &self.location,
                        "final chunk upload failed during close",
                    )
                    .await;
                    return Err(upload_error);
                }
                if let Err(verify_error) =
                    verify_resumable_complete(&upload_client, &self.location, total_bytes).await
                {
                    cancel_resumable_logged_infallible(
                        upload_client,
                        &self.location,
                        "verify_resumable_complete failed during close",
                    )
                    .await;
                    return Err(verify_error);
                }
                Ok(())
            }
        }
    }
}

impl GcsFileWrite {
    /// Flush every full `DEFAULT_BYTES_PER_REQUEST`-sized chunk the
    /// resumable buffer can produce. Any tail remains buffered and is
    /// flushed by `close` as the finalising chunk.
    async fn flush_resumable_buffer(&mut self) -> Result<(), WriteError> {
        loop {
            // Re-borrow each iteration so the error path can `mem::replace`
            // `self.state` and consume the upload client for cancellation.
            let GcsWriterState::Resumable {
                upload_client,
                offset,
                buffer,
            } = &mut self.state
            else {
                return Ok(());
            };
            if buffer.len() < DEFAULT_BYTES_PER_REQUEST {
                return Ok(());
            }
            let chunk = buffer.split_to(DEFAULT_BYTES_PER_REQUEST).freeze();
            let chunk_offset = *offset;
            let chunk_len = chunk.len() as u64;
            match upload_chunk(upload_client, &self.location, chunk_offset, chunk, None).await {
                Ok(()) => *offset += chunk_len,
                Err(e) => {
                    if let GcsWriterState::Resumable { upload_client, .. } =
                        std::mem::replace(&mut self.state, GcsWriterState::Aborted)
                    {
                        cancel_resumable_logged_infallible(
                            upload_client,
                            &self.location,
                            "buffered chunk upload failed",
                        )
                        .await;
                    }
                    return Err(e);
                }
            }
        }
    }
}

impl Drop for GcsFileWrite {
    fn drop(&mut self) {
        let state = std::mem::replace(&mut self.state, GcsWriterState::Aborted);
        let GcsWriterState::Resumable { upload_client, .. } = state else {
            // Buffering: nothing on GCS yet.
            // Closed / Aborted / AbortFailed: terminal states, no action.
            return;
        };

        // `Handle::current()` panics when called
        // outside a tokio runtime (runtime already shut down) or
        // race with shutdown
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            self.state = GcsWriterState::AbortFailed;
            tracing::warn!(
                location = %self.location,
                "GcsFileWrite dropped without closing outside runtime, upload session cannot be canceled. Incomplete file may exist in target location."
            );
            return;
        };

        // Once stable `std::future::AsyncDrop`
        // (https://doc.rust-lang.org/std/future/trait.AsyncDrop.html —
        // currently nightly-only and experimental) exists, we can change this
        // to `.cancel().await` without `spawn`. The bounded `DROP_CANCEL_DURATION`
        // protects against a stuck cancel call holding the spawned task on a
        // shutting-down runtime; the elapse path is logged so an orphaned
        // resumable session is observable.
        let location = self.location.clone();
        handle.spawn(async move {
            if tokio::time::timeout(
                DROP_CANCEL_DURATION,
                cancel_resumable_logged_infallible(upload_client, &location, "writer dropped without closing"),
            )
            .await
            .is_err()
            {
                tracing::warn!(
                    location = %location,
                    timeout = ?DROP_CANCEL_DURATION,
                    "Best-effort cancel of un-closed GCS resumable upload timed out. Incomplete upload may exist in target location until session expiry.",
                );
            }
        });
    }
}
