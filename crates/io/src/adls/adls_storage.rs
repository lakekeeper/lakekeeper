use std::{
    collections::HashMap,
    num::NonZeroU32,
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use azure_storage::CloudLocation;
use azure_storage_datalake::prelude::{
    DataLakeClient, DirectoryClient, FileClient, FileSystemClient, GetFileResponse,
    HeadPathResponse, Path,
};
use bytes::{Bytes, BytesMut};
use chrono::DateTime;
use futures::StreamExt as _;

use crate::{
    DeleteBatchError, DeleteError, ErrorKind, FileInfo, IOError, InvalidLocationError,
    LakekeeperFileWrite, LakekeeperStorage, Location, ReadError, WriteError,
    adls::{AdlsLocation, adls_error::parse_error},
    delete_not_found_is_ok, execute_with_parallelism, safe_usize_to_i64, validate_file_size,
};

#[derive(Debug, Clone)]
pub struct AdlsStorage {
    data_lake_client: DataLakeClient,
    cloud_location: CloudLocation,
}

const MAX_BYTES_PER_REQUEST: usize = 7 * 1024 * 1024;
const DEFAULT_BYTES_PER_REQUEST: usize = 4 * 1024 * 1024;
/// Upper bound on best-effort cleanup work spawned from `Drop`. The delete
/// future is dropped on elapse; we still log the timeout so a partial file
/// left behind is observable.
const DROP_CANCEL_DURATION: Duration = Duration::from_secs(10);

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
            return Err(InvalidLocationError::new(
                location.to_string(),
                format!(
                    "Location account name `{}` does not match storage account `{}`",
                    location.account_name(),
                    self.cloud_location.account()
                ),
            ));
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

#[async_trait::async_trait]
impl LakekeeperStorage for AdlsStorage {
    async fn delete(&self, path: &str) -> Result<(), DeleteError> {
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

    async fn delete_batch(&self, paths: &[String]) -> Result<(), DeleteBatchError> {
        // Group paths by account and filesystem
        let grouped_paths = group_paths_by_container(paths)?;

        // Create futures for parallel deletion
        let mut delete_futures = Vec::new();

        // Create delete operations for each path
        for ((_account, _filesystem), paths) in grouped_paths {
            if paths.is_empty() {
                continue; // Skip empty groups
            }
            let filesystem_client = self.get_filesystem_client(&paths[0])?;

            for path in paths {
                let file_client = filesystem_client.get_file_client(path.blob_name());
                let mut deletion_stream = file_client.delete().into_stream();

                let future = async move {
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
                        Ok::<(AdlsLocation, Option<IOError>), DeleteBatchError>((path, Some(e)))
                    } else {
                        Ok((path, None))
                    }
                };

                delete_futures.push(future);
            }
        }

        let completed_batches = AtomicU64::new(0);
        let total_batches = delete_futures.len();

        let delete_stream = execute_with_parallelism(delete_futures, 16).map(|result| {
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
            match result? {
                (_path, None) => {}
                (_location, Some(error)) => {
                    return Err(DeleteBatchError::IOError(error.with_context(format!(
                        "Delete batch {completed_batch} out of {total_batches} failed",
                    ))));
                }
            }
        }

        Ok(())
    }

    async fn write(&self, path: &str, bytes: Bytes) -> Result<(), WriteError> {
        let adls_location = AdlsLocation::try_from_str(path, true)?;
        require_key(&adls_location)?;
        let client = self.get_file_client(&adls_location)?;
        let file_length = safe_usize_to_i64(bytes.len(), path)?;

        create_file(&client, path).await?;

        if bytes.len() <= MAX_BYTES_PER_REQUEST {
            if let Err(e) = append_chunk(&client, 0, bytes, path).await {
                delete_partial_file_logged_infallible(
                    &client,
                    path,
                    "single-chunk append failed during bulk write",
                )
                .await;
                return Err(e);
            }
        } else {
            // Zero-copy chunking: `bytes.slice(range)` produces an owned
            // refcounted view that can be moved into per-chunk futures.
            let upload_futures = crate::chunk_ranges(bytes.len(), DEFAULT_BYTES_PER_REQUEST)
                .map(|(chunk_index, range)| {
                    let raw_offset = range.start;
                    let offset = i64::try_from(raw_offset).map_err(|_| {
                        WriteError::IOError(IOError::new(
                            ErrorKind::ConditionNotMatch,
                            format!(
                                "Calculated offset for write exceeds i64 limit: {raw_offset} > {}",
                                i64::MAX
                            ),
                            path.to_string(),
                        ))
                    })?;
                    let chunk = bytes.slice(range);
                    let client = client.clone();
                    let path = path.to_string();
                    Ok(async move {
                        append_chunk(&client, offset, chunk, &path)
                            .await
                            .map_err(|e| match e {
                                WriteError::IOError(io) => {
                                    WriteError::IOError(io.with_context(format!(
                                        "Multipart upload chunk {chunk_index}"
                                    )))
                                }
                                other @ WriteError::InvalidLocation(_) => other,
                            })
                    })
                })
                .collect::<Result<Vec<_>, WriteError>>()?;

            let path_for_join_err = path.to_string();
            let upload_stream = execute_with_parallelism(upload_futures, 10).map(move |result| {
                let path_for_err = path_for_join_err.clone();
                result.map_err(move |join_err| {
                    WriteError::IOError(IOError::new(
                        ErrorKind::Unexpected,
                        format!("Task join error during multipart upload: {join_err}"),
                        path_for_err,
                    ))
                })
            });
            tokio::pin!(upload_stream);

            // Drain the result stream even after the first failure so that any
            // already-spawned upload tasks finish (or fail) before we delete
            // the partial file. Keep the earliest error to surface.
            let mut first_error: Option<WriteError> = None;
            while let Some(result) = upload_stream.next().await {
                match result {
                    Err(write_err) | Ok(Err(write_err)) if first_error.is_none() => {
                        first_error = Some(write_err);
                    }
                    _ => {
                        // Already errored or success — drop result.
                    }
                }
            }
            if let Some(err) = first_error {
                delete_partial_file_logged_infallible(
                    &client,
                    path,
                    "parallel multipart write failed",
                )
                .await;
                return Err(err);
            }
        }

        if let Err(e) = flush_close(&client, file_length, path).await {
            delete_partial_file_logged_infallible(
                &client,
                path,
                "flush_close failed after bulk write",
            )
            .await;
            return Err(e);
        }
        Ok(())
    }

    async fn writer(&self, path: &str) -> Result<Box<dyn LakekeeperFileWrite>, WriteError> {
        let adls_location = AdlsLocation::try_from_str(path, true)?;
        require_key(&adls_location)?;
        let client = self.get_file_client(&adls_location)?;
        create_file(&client, path).await?;
        Ok(Box::new(AdlsFileWrite {
            client,
            path: path.to_string(),
            offset: 0,
            buffer: BytesMut::new(),
            state: AdlsWriterState::Active,
        }))
    }

    async fn read_single(&self, path: &str) -> Result<Bytes, ReadError> {
        let adls_location = AdlsLocation::try_from_str(path, true)?;

        // Get the container/filesystem name and the blob path (key)
        require_key(&adls_location)?;

        let client = self.get_file_client(&adls_location)?;

        client.read().await.map(|gfr| gfr.data).map_err(|e| {
            ReadError::IOError(
                parse_error(e, path).with_context("Failed to read ADLS file in single request."),
            )
        })
    }

    async fn metadata(&self, path: &str) -> Result<FileInfo, ReadError> {
        let adls_location = AdlsLocation::try_from_str(path, true)?;
        require_key(&adls_location)?;
        let client = self.get_file_client(&adls_location)?;
        let head_response = head(&client, &adls_location).await?;
        let size = head_response
            .content_length
            .and_then(|cl| crate::size_to_u64(cl, adls_location.location().as_str()));
        let last_modified = parse_offsetdatetime(&head_response.last_modified);
        Ok(FileInfo::new(
            last_modified,
            adls_location.location().clone(),
            size,
        ))
    }

    async fn read(&self, path: &str) -> Result<Bytes, ReadError> {
        let adls_location = AdlsLocation::try_from_str(path, true)?;
        require_key(&adls_location)?;
        let client = self.get_file_client(&adls_location)?;

        let head_response = head(&client, &adls_location).await?;
        let Some(content_length) = head_response.content_length else {
            // If we do not get content_length, we cannot read in chunks,
            // so read the file in one request. We can use `fetch_range`
            // with `u64::MAX`, which is set by client if no range is provided
            return fetch_range(&client, 0..u64::MAX, adls_location)
                .await
                .map(|gfr| gfr.data);
        };
        let file_size = validate_file_size(content_length, adls_location.location().as_str())?;

        if file_size == 0 {
            return Ok(Bytes::new());
        }

        if file_size < MAX_BYTES_PER_REQUEST {
            // If the file is small enough, read it in a single request
            return fetch_range(&client, 0..file_size as u64, adls_location)
                .await
                .map(|gfr| gfr.data);
        }

        parallel_chunked_read_with_integrity(
            &client,
            path,
            0,
            file_size,
            head_response.last_modified,
            adls_location,
        )
        .await
    }

    async fn read_range(
        &self,
        path: &str,
        range: std::ops::Range<u64>,
    ) -> Result<Bytes, ReadError> {
        let adls_location = AdlsLocation::try_from_str(path, true)?;
        require_key(&adls_location)?;
        if range.end < range.start {
            return Err(ReadError::IOError(IOError::new(
                ErrorKind::ConditionNotMatch,
                format!(
                    "Invalid range: start ({}) > end ({})",
                    range.start, range.end
                ),
                path.to_string(),
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

        let client = self.get_file_client(&adls_location)?;

        if range_size <= MAX_BYTES_PER_REQUEST {
            return fetch_range(&client, range, adls_location)
                .await
                .map(|gfr| gfr.data);
        }

        let head_response = head(&client, &adls_location).await?;
        parallel_chunked_read_with_integrity(
            &client,
            path,
            range.start,
            range_size,
            head_response.last_modified,
            adls_location,
        )
        .await
    }

    async fn list(
        &self,
        path: &str,
        page_size: Option<usize>,
    ) -> Result<futures::stream::BoxStream<'_, Result<Vec<FileInfo>, IOError>>, InvalidLocationError>
    {
        let path = format!("{}/", path.trim_end_matches('/'));
        let adls_location = AdlsLocation::try_from_str(&path, true)
            .map_err(|e| e.with_context("List Operation failed"))?;
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
            let result = result.map_err(|e| {
                parse_error(e, path.as_str()).with_context("Failed to list ADLS path")
            });
            if let Err(err) = &result
                && err.kind() == ErrorKind::NotFound
            {
                return Ok(vec![]); // Return empty list if path does not exist
            }
            result.map(|page| {
                page.paths
                    .iter()
                    .filter_map(try_parse_file_info(&base_location))
                    .collect::<Vec<_>>()
            })
        });

        Ok(stream.boxed())
    }

    /// Native ADLS Gen2 recursive delete via `DELETE` + `recursive=true`.
    ///
    /// The trailing slash is stripped before parsing: the ADLS API
    /// accepts either form for a path, and `recursive=true` is what signals
    /// directory semantics. For file paths the `recursive` flag is ignored
    /// server-side, so the same call safely handles both files and directories.
    ///
    /// `NotFound` responses are treated as success, matching the idempotent
    /// semantics of `delete` and `delete_batch` on this backend — removing an
    /// already-absent prefix is a no-op, not an error.
    async fn remove_all(&self, path: &str) -> Result<(), DeleteError> {
        let path = path.trim_end_matches('/');
        let adls_location = AdlsLocation::try_from_str(path, true)?;

        // Get the container/filesystem name and the blob path (key)
        require_key(&adls_location)?;

        let client = self.get_file_client(&adls_location)?;
        let mut delete_stream = client.delete().recursive(true).into_stream();

        while let Some(result) = delete_stream.next().await {
            let result = result.map(|_| ()).map_err(|e| parse_error(e, path));
            delete_not_found_is_ok(result).map_err(DeleteError::IOError)?;
        }

        Ok(())
    }
}

/// Convert a `time::OffsetDateTime` to a `chrono::DateTime<Utc>`, preserving
/// nanosecond precision. Returns `None` if the seconds are out of range.
fn parse_offsetdatetime(t: &time::OffsetDateTime) -> Option<DateTime<chrono::Utc>> {
    DateTime::from_timestamp(t.unix_timestamp(), t.nanosecond())
}

fn try_parse_file_info(base_location: &Location) -> impl FnMut(&Path) -> Option<FileInfo> {
    |path| {
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
        let location = Location::from_str(&full_path).ok()?;
        let last_modified = parse_offsetdatetime(&path.last_modified);
        let size = if path.is_directory {
            None
        } else {
            crate::size_to_u64(path.content_length, &full_path)
        };
        Some(FileInfo::new(last_modified, location, size))
    }
}

fn require_key(adls_location: &AdlsLocation) -> Result<(), InvalidLocationError> {
    if adls_location.blob_name().is_empty() || adls_location.blob_name() == "/" {
        return Err(InvalidLocationError::new(
            adls_location.to_string(),
            "Operation requires a path inside the ADLS Filesystem".to_string(),
        ));
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
async fn head(client: &FileClient, location: &AdlsLocation) -> Result<HeadPathResponse, ReadError> {
    client.get_properties().await.map_err(|e| {
        ReadError::IOError(
            parse_error(e, location.location().as_str())
                .with_context("Failed to get ADLS file status"),
        )
    })
}

async fn fetch_range(
    client: &FileClient,
    range: std::ops::Range<u64>,
    adls_location: AdlsLocation,
) -> Result<GetFileResponse, ReadError> {
    client.read().range(range).await.map_err(|e| {
        ReadError::IOError(
            parse_error(e, &adls_location.to_string())
                .with_context("Failed to download byte range."),
        )
    })
}

/// Run a parallel-chunked download over `[range_start, range_start + range_size)`
/// and verify each chunk's `last_modified` against `head_last_modified`. A
/// mismatch is surfaced as an error so concurrent overwrites cannot
/// silently produce a corrupt download.
async fn parallel_chunked_read_with_integrity(
    client: &FileClient,
    error_context: &str,
    range_start: u64,
    range_size: usize,
    head_last_modified: time::OffsetDateTime,
    adls_location: AdlsLocation,
) -> Result<Bytes, ReadError> {
    if range_size == 0 {
        return Ok(Bytes::new());
    }
    let client = client.clone();
    crate::parallel_chunked_read(
        range_size,
        DEFAULT_BYTES_PER_REQUEST,
        10,
        error_context,
        move |rel_start, rel_end, chunk_index| {
            let client = client.clone();
            let abs_start = range_start + rel_start as u64;
            let abs_end = range_start + rel_end as u64 + 1;
            let adls_location = adls_location.clone();
            async move {
                let response = fetch_range(&client, abs_start..abs_end, adls_location.clone())
                    .await
                    .map_err(|e| match e {
                        ReadError::IOError(io) => ReadError::IOError(io.with_context(format!(
                            "Failed to download chunk {chunk_index} (bytes {abs_start}-{abs_end})"
                        ))),
                        invalid_location_error @ ReadError::InvalidLocation(_) => invalid_location_error,
                    })?;
                if response.last_modified != head_last_modified {
                    return Err(ReadError::IOError(IOError::new(
                        ErrorKind::Unexpected,
                        format!(
                            "File was modified during chunked parallel download: expected last modified time {}, got {}",
                            head_last_modified, response.last_modified
                        ),
                        adls_location.to_string(),
                    )));
                }
                Ok((chunk_index, response.data))
            }
        },
    )
        .await
}

/// Create the empty target file. ADLS requires an explicit `create` before
/// any append/flush.
async fn create_file(client: &FileClient, path: &str) -> Result<(), WriteError> {
    client.create().await.map(|_| ()).map_err(|e| {
        WriteError::IOError(parse_error(e, path).with_context("Failed to create ADLS file."))
    })
}

/// Append a chunk at the given byte offset.
async fn append_chunk(
    client: &FileClient,
    offset: i64,
    chunk: Bytes,
    path: &str,
) -> Result<(), WriteError> {
    let chunk_len = chunk.len();
    client.append(offset, chunk).await.map(|_| ()).map_err(|e| {
        WriteError::IOError(parse_error(e, path).with_context(format!(
            "Failed to upload chunk (bytes {offset}-{end})",
            end = offset.saturating_add_unsigned(chunk_len as u64)
        )))
    })
}

/// Finalise the file by flushing and closing it.
async fn flush_close(client: &FileClient, file_length: i64, path: &str) -> Result<(), WriteError> {
    client
        .flush(file_length)
        .close(true)
        .await
        .map(|_| ())
        .map_err(|e| {
            WriteError::IOError(parse_error(e, path).with_context(format!(
                "Failed to flush and close ADLS file (length {file_length})"
            )))
        })
}

/// Delete the target file as part of cleanup after a failed write.
///
/// Note on Azure auto-cleanup: the underlying Block Blob layer documents that
/// uncommitted blocks (and zero-byte blobs created only via uncommitted blocks)
/// are discarded one week after the last successful block upload. The ADLS
/// Gen2 (DFS) docs do not explicitly state the same guarantee for HNS path
/// objects, but they do not contradict it either. So in our current usage —
/// where `flush` is only ever issued at `close` time — a partial file would
/// most likely be garbage-collected by the underlying layer regardless.
///
/// We still delete explicitly because:
///   1. A 0-byte / unflushed file can linger for up to a week (Block Blob
///      documented behaviour) — long enough to confuse list operations,
///      monitoring, and any reader expecting only completed files.
///   2. lakekeeper-io's contract is that callers only ever observe finished
///      files; a partial file is never inspected or recovered.
///   3. If the writer is ever changed to issue intermediate flushes between
///      appends (e.g. for memory pressure or progress checkpointing), the
///      Block-Blob uncommitted-block GC stops covering the leak — the bytes
///      become *committed* and persist indefinitely. Cleaning up explicitly
///      now avoids that future foot-gun.
async fn delete_file(client: &FileClient, path: &str) -> Result<(), WriteError> {
    // The Azure SDK's `IntoFuture` impl for `DeletePathBuilder` is broken: the
    // generated `fn into_future(self) { Self::into_future(self) }` recurses
    // into itself because no inherent `into_future` body is provided by
    // `operations/path_delete.rs`. Awaiting `client.delete()` directly blows
    // the stack during `IntoFuture::into_future`. The `into_stream()` API is
    // unaffected; consume its single page here to match the rest of this
    // module's delete usage.
    let mut delete_stream = client.delete().into_stream();
    while let Some(result) = delete_stream.next().await {
        result.map(|_| ()).map_err(|e| {
            WriteError::IOError(
                parse_error(e, path).with_context("Failed to delete partial ADLS file."),
            )
        })?;
    }
    Ok(())
}

/// Best-effort variant of [`delete_file`] that swallows the delete error after
/// logging it.
///
/// Use on cleanup paths where the caller propagates a different (original)
/// error and the delete failure is unactionable: `close`, bulk write, `Drop`.
/// The `context` field is included in the warn log to disambiguate which
/// cleanup site triggered the deletion.
async fn delete_partial_file_logged_infallible(client: &FileClient, path: &str, context: &str) {
    if let Err(e) = delete_file(client, path).await {
        tracing::warn!(
            path = %path,
            error = ?e,
            context = %context,
            "Failed to delete partial ADLS file. The file may persist in target \
             location until manually removed or until the underlying \
             uncommitted-block GC runs.",
        );
    }
}

/// Streaming writer for ADLS.
///
/// The target file is created up-front by `writer`. Each `write` buffers
/// locally and flushes append calls once `DEFAULT_BYTES_PER_REQUEST` bytes
/// have accumulated. `close` flushes any remaining buffered bytes and
/// finalises the file.
///
/// Zero-copy invariant: incoming `Bytes` are appended into a local
/// `BytesMut` (one copy, unavoidable to span multiple `write` calls);
/// each chunk is then handed off to `append_chunk` zero-copy via
/// `BytesMut::split_to(N).freeze()`.
pub(crate) struct AdlsFileWrite {
    client: FileClient,
    path: String,
    offset: i64,
    buffer: BytesMut,
    state: AdlsWriterState,
}

enum AdlsWriterState {
    Active,
    Closed,
    Aborted,
    AbortFailed,
}

impl std::fmt::Debug for AdlsFileWrite {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdlsFileWrite")
            .field("path", &self.path)
            .field("offset", &self.offset)
            .field("buffered_bytes", &self.buffer.len())
            .field("state", &self.state)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for AdlsWriterState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Active => f.write_str("Active"),
            Self::Closed => f.write_str("Closed"),
            Self::Aborted => f.write_str("Aborted"),
            Self::AbortFailed => f.write_str("AbortFailed"),
        }
    }
}

#[async_trait::async_trait]
impl LakekeeperFileWrite for AdlsFileWrite {
    async fn write(&mut self, bytes_in: Bytes) -> Result<(), WriteError> {
        match self.state {
            AdlsWriterState::Closed => {
                return Err(WriteError::IOError(IOError::new(
                    ErrorKind::ConditionNotMatch,
                    "Cannot write to closed writer",
                    self.path.clone(),
                )));
            }
            AdlsWriterState::Aborted => {
                return Err(WriteError::IOError(IOError::new(
                    ErrorKind::ConditionNotMatch,
                    "Cannot write to aborted writer",
                    self.path.clone(),
                )));
            }
            AdlsWriterState::AbortFailed => {
                return Err(WriteError::IOError(IOError::new(
                    ErrorKind::ConditionNotMatch,
                    "Cannot write to writer that failed to abort",
                    self.path.clone(),
                )));
            }
            AdlsWriterState::Active => {}
        }
        self.buffer.extend_from_slice(&bytes_in);
        while self.buffer.len() >= DEFAULT_BYTES_PER_REQUEST {
            let chunk = self.buffer.split_to(DEFAULT_BYTES_PER_REQUEST).freeze();
            let chunk_len =
                safe_usize_to_i64(chunk.len(), self.path.clone()).map_err(WriteError::IOError)?;
            if let Err(append_error) =
                append_chunk(&self.client, self.offset, chunk, &self.path).await
            {
                // Always surface the original append error. The cleanup
                // delete is best-effort; its failure is logged and
                // reflected in `state` for `Drop`, but never masks
                // `append_error`.
                match delete_file(&self.client, &self.path).await {
                    Ok(()) => self.state = AdlsWriterState::Aborted,
                    Err(delete_error) => {
                        self.state = AdlsWriterState::AbortFailed;
                        tracing::warn!(
                            path = %self.path,
                            error = ?delete_error,
                            "Failed to delete partial ADLS file after streaming append error; \
                             partial file may exist at target location. \
                             Original append error is being returned.",
                        );
                    }
                }
                return Err(append_error);
            }
            self.offset += chunk_len;
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<(), WriteError> {
        let prev_state = std::mem::replace(&mut self.state, AdlsWriterState::Closed);
        match prev_state {
            AdlsWriterState::Closed | AdlsWriterState::Aborted | AdlsWriterState::AbortFailed => {
                return Err(WriteError::IOError(IOError::new(
                    ErrorKind::ConditionNotMatch,
                    "Writer already closed or aborted",
                    self.path.clone(),
                )));
            }
            AdlsWriterState::Active => {}
        }
        if !self.buffer.is_empty() {
            let chunk = self.buffer.split().freeze();
            let chunk_len = match safe_usize_to_i64(chunk.len(), self.path.clone()) {
                Ok(v) => v,
                Err(e) => {
                    delete_partial_file_logged_infallible(
                        &self.client,
                        &self.path,
                        "buffer-size conversion failed during close",
                    )
                    .await;
                    return Err(WriteError::IOError(e));
                }
            };
            if let Err(e) = append_chunk(&self.client, self.offset, chunk, &self.path).await {
                delete_partial_file_logged_infallible(
                    &self.client,
                    &self.path,
                    "tail append failed during close",
                )
                .await;
                return Err(e);
            }
            self.offset += chunk_len;
        }
        if let Err(e) = flush_close(&self.client, self.offset, &self.path).await {
            delete_partial_file_logged_infallible(
                &self.client,
                &self.path,
                "flush_close failed during close",
            )
            .await;
            return Err(e);
        }
        Ok(())
    }
}

impl Drop for AdlsFileWrite {
    fn drop(&mut self) {
        let prev_state = std::mem::replace(&mut self.state, AdlsWriterState::Aborted);
        if !matches!(prev_state, AdlsWriterState::Active) {
            // Closed / Aborted / AbortFailed: terminal states, no action.
            return;
        }

        // `Handle::current()` panics when called outside a tokio runtime
        // (runtime already shut down) or racing with shutdown.
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            self.state = AdlsWriterState::AbortFailed;
            tracing::warn!(
                path = %self.path,
                "AdlsFileWrite dropped without closing outside runtime, partial file cannot be deleted. Incomplete file may exist in target location.",
            );
            return;
        };

        // Once stable `std::future::AsyncDrop` exists we could change this to
        // `.delete_file(...).await` without `spawn`. The bounded
        // `DROP_CANCEL_DURATION` protects against a stuck delete call holding
        // the spawned task on a shutting-down runtime; the elapse path is
        // logged so a partial file left behind is observable.
        let client = self.client.clone();
        let path = self.path.clone();
        handle.spawn(async move {
            if tokio::time::timeout(
                DROP_CANCEL_DURATION,
                delete_partial_file_logged_infallible(
                    &client,
                    &path,
                    "writer dropped without closing",
                ),
            )
            .await
            .is_err()
            {
                tracing::warn!(
                    path = %path,
                    timeout = ?DROP_CANCEL_DURATION,
                    "Best-effort delete of partial ADLS file timed out. Partial file may persist until manually removed or until uncommitted-block GC runs.",
                );
            }
        });
    }
}
