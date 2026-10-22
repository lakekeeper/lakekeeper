use std::{
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use bytes::{Bytes, BytesMut};
use futures::{StreamExt as _, TryStreamExt as _};
use google_cloud_gax::{
    options::RequestOptionsBuilder as _, paginator::Paginator as _,
    retry_policy::RetryPolicyExt as _,
};
use google_cloud_storage::{
    client::{Storage, StorageControl},
    model::Object,
    model_ext::ReadRange,
    retry_policy::RetryableErrors,
};

use crate::{
    DeleteBatchError, DeleteError, ErrorKind, FileInfo, IOError, InvalidLocationError,
    LakekeeperFileWrite, LakekeeperStorage, Location, ObjectRead, ReadError, WriteError,
    delete_not_found_is_ok, execute_with_parallelism,
    gcs::{
        CHUNK_SIZE, ControlClient, GcsLocation, REQUEST_TIMEOUT, RESUMABLE_UPLOAD_THRESHOLD,
        UPLOAD_RETRY_TIME_LIMIT, gcs_error::parse_error, gcs_writer::GcsFileWrite,
    },
    safe_usize_to_i32, validate_file_size,
};

/// Reads up to this size are sent as a single request.
const MAX_BYTES_PER_READ: usize = RESUMABLE_UPLOAD_THRESHOLD;
/// Concurrent ranged requests of one parallel read.
const READ_PARALLELISM: usize = 10;
/// Concurrent delete requests of one batch delete.
const DELETE_PARALLELISM: usize = 16;

/// Bucket name in the form the SDK expects.
pub(crate) fn bucket_resource(location: &GcsLocation) -> String {
    format!("projects/_/buckets/{}", location.bucket_name())
}

/// Fails `operation` with [`ErrorKind::RequestTimeout`] if it does not finish within `limit`.
async fn within<T>(
    limit: Duration,
    location: &GcsLocation,
    operation: impl Future<Output = Result<T, IOError>>,
) -> Result<T, IOError> {
    tokio::time::timeout(limit, operation)
        .await
        .unwrap_or_else(|_| {
            Err(IOError::new(
                ErrorKind::RequestTimeout,
                format!("GCS request did not finish within {limit:?}"),
                location.to_string(),
            ))
        })
}

/// Deletes one object. Owned arguments keep the returned future free of borrows, so
/// it satisfies the `'static` bound of [`execute_with_parallelism`] and the caller
/// can build the futures lazily.
async fn delete_one(
    control: Arc<StorageControl>,
    path: String,
) -> Result<(GcsLocation, Option<IOError>), DeleteBatchError> {
    let location = GcsLocation::try_from_str(&path)?;
    let result = delete_object(&control, &location).await;
    // Convert 404 (not found) to success for idempotent behavior
    let result = delete_not_found_is_ok(result);
    Ok((location, result.err()))
}

async fn delete_object(control: &StorageControl, location: &GcsLocation) -> Result<(), IOError> {
    within(REQUEST_TIMEOUT, location, async {
        control
            .delete_object()
            .set_bucket(bucket_resource(location))
            .set_object(location.object_name())
            .with_idempotency(true)
            .send()
            .await
            .map_err(|e| parse_error(e, location.as_str()))
    })
    .await
}

/// GCS storage: object data through the HTTP API, object metadata through gRPC.
#[derive(Clone)]
pub struct GcsStorage {
    storage: Storage,
    control: Arc<ControlClient>,
}

impl std::fmt::Debug for GcsStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GcsStorage").finish_non_exhaustive()
    }
}

impl GcsStorage {
    pub(crate) fn new(storage: Storage, control: ControlClient) -> Self {
        Self {
            storage,
            control: Arc::new(control),
        }
    }

    async fn control(&self) -> Arc<StorageControl> {
        self.control.get().await
    }
}

#[async_trait::async_trait]
impl LakekeeperStorage for GcsStorage {
    async fn delete(&self, path: &str) -> Result<(), DeleteError> {
        let location = GcsLocation::try_from_str(path)?;
        delete_not_found_is_ok(delete_object(&*self.control().await, &location).await)?;
        Ok(())
    }

    async fn delete_batch(&self, paths: &[String]) -> Result<(), DeleteBatchError> {
        // Validate every location first so an invalid path fails the batch while all
        // objects are still present.
        for path in paths {
            GcsLocation::try_from_str(path)?;
        }

        // Futures stay lazy: `execute_with_parallelism` pulls them one at a time, so
        // only `parallelism` of them exist at once. Collecting them all costs ~2 KiB
        // per object, which reaches gigabytes for a table with millions of files.
        // `cloned()` hands the closure an owned path, so each future borrows nothing
        // and meets the `'static` bound of `execute_with_parallelism`. Clippy sees
        // only that `delete_one` reads the path through a reference.
        let control = self.control().await;
        #[allow(clippy::redundant_iter_cloned)]
        let delete_futures = paths
            .iter()
            .cloned()
            .map(move |path| delete_one(control.clone(), path));

        let completed_batches = AtomicU64::new(0);
        let total_batches = paths.len();

        let delete_stream =
            execute_with_parallelism(delete_futures, DELETE_PARALLELISM).map(|result| {
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
        upload_bytes(&self.storage, &location, bytes).await
    }

    async fn writer(&self, path: &str) -> Result<Box<dyn LakekeeperFileWrite>, WriteError> {
        let location = GcsLocation::try_from_str(path)?;
        Ok(Box::new(GcsFileWrite::new(self.storage.clone(), location)))
    }

    async fn metadata(&self, path: &str) -> Result<FileInfo, ReadError> {
        let location = GcsLocation::try_from_str(path)?;
        let object = head(&*self.control().await, &location).await?;
        Ok(file_info(
            &object,
            location.location().clone(),
            location.as_str(),
        ))
    }

    async fn read_single(&self, path: &str) -> Result<Bytes, ReadError> {
        let location = GcsLocation::try_from_str(path)?;
        fetch_range(&self.storage, &location, FetchRange::whole(None, None))
            .await
            .map_err(|e| ReadError::IOError(e.with_context("Failed to download full object.")))
    }

    async fn read(&self, path: &str) -> Result<ObjectRead, ReadError> {
        let gcs_location = GcsLocation::try_from_str(path)?;

        let object = head(&*self.control().await, &gcs_location).await?;
        let file_size = validate_file_size(object.size, gcs_location.as_str())?;

        // The `head` above already carries the object metadata, so it is
        // surfaced here; a second request is not needed.
        let info = file_info(
            &object,
            gcs_location.location().clone(),
            gcs_location.as_str(),
        );

        let bytes = if file_size == 0 {
            Bytes::new()
        } else if file_size < MAX_BYTES_PER_READ {
            fetch_range(
                &self.storage,
                &gcs_location,
                FetchRange::whole(Some(object.generation), Some(file_size)),
            )
            .await
            .map_err(|e| ReadError::IOError(e.with_context("Failed to download object.")))?
        } else {
            parallel_chunked_read_with_fixed_generation(
                &self.storage,
                &gcs_location,
                0,
                file_size,
                object.generation,
            )
            .await?
        };

        Ok(ObjectRead { bytes, info })
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

        if range_size <= MAX_BYTES_PER_READ {
            return fetch_range(
                &self.storage,
                &gcs_location,
                FetchRange::segment(range.start, range_size, None),
            )
            .await
            .map_err(|e| ReadError::IOError(e.with_context("Failed to download byte range.")));
        }

        let object = head(&*self.control().await, &gcs_location).await?;
        parallel_chunked_read_with_fixed_generation(
            &self.storage,
            &gcs_location,
            range.start,
            range_size,
            object.generation,
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

        let mut request = self
            .control()
            .await
            .list_objects()
            .set_parent(bucket_resource(&location))
            .set_prefix(prefix)
            .with_idempotency(true);
        if let Some(page_size) =
            page_size.and_then(|size| safe_usize_to_i32(size, location.as_str()).ok())
        {
            request = request.set_page_size(page_size);
        }

        let bucket_name = location.bucket_name().to_string();
        let pages = futures::stream::unfold(request.by_page(), {
            let location = location.clone();
            move |mut pages| {
                let location = location.clone();
                async move {
                    let page = within(REQUEST_TIMEOUT, &location, async {
                        pages
                            .next()
                            .await
                            .transpose()
                            .map_err(|e| parse_error(e, location.bucket_name()))
                    })
                    .await
                    .transpose()?;
                    Some((page, pages))
                }
            }
        });
        let stream = pages.and_then(move |page| {
            let file_infos = page
                .objects
                .iter()
                .map(|object| try_parse_file_info(&bucket_name, object))
                .collect::<Result<Vec<_>, _>>();
            futures::future::ready(file_infos)
        });

        Ok(stream.boxed())
    }
}

/// Uploads `bytes` as one object. Below [`RESUMABLE_UPLOAD_THRESHOLD`] this is a single
/// request, above it a resumable upload.
pub(crate) async fn upload_bytes(
    storage: &Storage,
    location: &GcsLocation,
    bytes: Bytes,
) -> Result<(), WriteError> {
    let resumable = bytes.len() >= RESUMABLE_UPLOAD_THRESHOLD;
    let mut request = storage
        .write_object(bucket_resource(location), location.object_name(), bytes)
        // Objects are written to unique paths, so repeating an upload is safe.
        .with_idempotency(true);
    if resumable {
        // All chunks of a resumable upload share one retry budget, so it is time-based.
        request =
            request.with_retry_policy(RetryableErrors.with_time_limit(UPLOAD_RETRY_TIME_LIMIT));
    }
    let upload = request.send_unbuffered();
    Box::pin(upload).await.map(|_| ()).map_err(|e| {
        parse_error(e, location.as_str())
            .with_context("Failed to upload object.")
            .into()
    })
}

fn last_modified(object: &Object) -> Option<chrono::DateTime<chrono::Utc>> {
    let update_time = object.update_time.as_ref()?;
    chrono::DateTime::from_timestamp(update_time.seconds(), update_time.nanos().try_into().ok()?)
}

fn file_info(object: &Object, location: Location, location_str: &str) -> FileInfo {
    FileInfo::new(
        last_modified(object),
        location,
        crate::size_to_u64(object.size, location_str),
    )
    .with_e_tag(Some(object.etag.clone()))
}

fn try_parse_file_info(bucket_name: &str, object: &Object) -> Result<FileInfo, IOError> {
    let gcs_path = format!("gs://{}/{}", bucket_name, object.name);
    let location = Location::from_str(&gcs_path).map_err(|e| {
        IOError::new(
            ErrorKind::Unexpected,
            format!("Failed to parse GCS object path returned from list: {e}"),
            gcs_path.clone(),
        )
    })?;
    let size = crate::size_to_u64(object.size, &gcs_path);
    Ok(FileInfo::new(last_modified(object), location, size))
}

/// Part of an object to download.
struct FetchRange {
    range: ReadRange,
    /// Generation of the object to read.
    generation: Option<i64>,
    /// Number of bytes expected, if known.
    expected_len: Option<usize>,
    /// Whole objects stored with `Content-Encoding: gzip` are returned decompressed.
    decompress: bool,
}

impl FetchRange {
    fn whole(generation: Option<i64>, expected_len: Option<usize>) -> Self {
        Self {
            range: ReadRange::all(),
            generation,
            expected_len,
            decompress: true,
        }
    }

    fn segment(offset: u64, len: usize, generation: Option<i64>) -> Self {
        Self {
            range: ReadRange::segment(offset, len as u64),
            generation,
            expected_len: Some(len),
            decompress: false,
        }
    }
}

/// Downloads part of an object.
async fn fetch_range(
    storage: &Storage,
    location: &GcsLocation,
    fetch: FetchRange,
) -> Result<Bytes, IOError> {
    let FetchRange {
        range,
        generation,
        expected_len,
        decompress,
    } = fetch;
    let mut request = storage
        .read_object(bucket_resource(location), location.object_name())
        .set_read_range(range);
    if decompress {
        request = request.with_automatic_decompression(true);
    }
    if let Some(generation) = generation {
        request = request.set_generation(generation);
    }
    let parse = |e| parse_error(e, location.as_str());
    within(REQUEST_TIMEOUT, location, async {
        let mut response = Box::pin(request.send()).await.map_err(parse)?;
        let mut buffer = BytesMut::with_capacity(expected_len.unwrap_or_default());
        while let Some(chunk) = response.next().await {
            buffer.extend_from_slice(&chunk.map_err(parse)?);
        }
        Ok(buffer.freeze())
    })
    .await
}

async fn head(control: &StorageControl, location: &GcsLocation) -> Result<Object, ReadError> {
    within(REQUEST_TIMEOUT, location, async {
        control
            .get_object()
            .set_bucket(bucket_resource(location))
            .set_object(location.object_name())
            .with_idempotency(true)
            .send()
            .await
            .map_err(|e| parse_error(e, location.as_str()))
    })
    .await
    .map_err(|e| ReadError::IOError(e.with_context("Failed to get metadata about the object.")))
}

async fn parallel_chunked_read_with_fixed_generation(
    storage: &Storage,
    gcs_location: &GcsLocation,
    range_start: u64,
    range_size: usize,
    generation: i64,
) -> Result<Bytes, ReadError> {
    if range_size == 0 {
        return Ok(Bytes::new());
    }

    let storage = storage.clone();
    let location = gcs_location.clone();
    crate::parallel_chunked_read(
        range_size,
        CHUNK_SIZE,
        READ_PARALLELISM,
        gcs_location.as_str(),
        move |rel_start, rel_end, chunk_index| {
            let storage = storage.clone();
            let location = location.clone();
            let abs_start = range_start + rel_start as u64;
            let length = rel_end - rel_start + 1;
            async move {
                let chunk = fetch_range(
                    &storage,
                    &location,
                    FetchRange::segment(abs_start, length, Some(generation)),
                )
                .await
                .map_err(|e| {
                    ReadError::IOError(e.with_context(format!(
                        "Failed to download chunk {chunk_index} (bytes {abs_start}-{})",
                        abs_start + length as u64
                    )))
                })?;
                Ok((chunk_index, chunk))
            }
        },
    )
    .await
}
