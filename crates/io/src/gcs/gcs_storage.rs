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
    model::{ListObjectsResponse, Object},
    model_ext::ReadRange,
    retry_policy::RetryableErrors,
};

use crate::{
    DeleteBatchError, DeleteError, ErrorKind, FileInfo, IOError, InvalidLocationError,
    LakekeeperFileWrite, LakekeeperStorage, Location, ObjectRead, ObjectVersion,
    ObjectVersionEntry, ReadError, SoftDeletedVersion, WriteError, WriteOutcome,
    delete_not_found_is_ok, execute_with_parallelism,
    gcs::{
        CHUNK_SIZE, ControlClient, GcsLocation, REQUEST_TIMEOUT, RESUMABLE_UPLOAD_THRESHOLD,
        UPLOAD_RETRY_TIME_LIMIT, UPLOAD_TIMEOUT, gcs_error::parse_error, gcs_writer::GcsFileWrite,
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
pub(crate) async fn within<T>(
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

    async fn write(&self, path: &str, bytes: Bytes) -> Result<WriteOutcome, WriteError> {
        let location = GcsLocation::try_from_str(path)?;
        upload_bytes(&self.storage, &location, bytes).await
    }

    async fn writer(&self, path: &str) -> Result<Box<dyn LakekeeperFileWrite>, WriteError> {
        let location = GcsLocation::try_from_str(path)?;
        Ok(Box::new(GcsFileWrite::new(self.storage.clone(), location)))
    }

    async fn metadata(&self, path: &str) -> Result<FileInfo, ReadError> {
        let location = GcsLocation::try_from_str(path)?;
        let object = head(&*self.control().await, &location, None).await?;
        Ok(file_info(&object, location.location().clone()))
    }

    async fn read_single(&self, path: &str) -> Result<Bytes, ReadError> {
        let location = GcsLocation::try_from_str(path)?;
        fetch_range(&self.storage, &location, FetchRange::whole(None, None))
            .await
            .map_err(|e| ReadError::IOError(e.with_context("Failed to download full object.")))
    }

    async fn read(&self, path: &str) -> Result<ObjectRead, ReadError> {
        let gcs_location = GcsLocation::try_from_str(path)?;
        self.read_object(&gcs_location, None).await
    }

    async fn read_range(
        &self,
        path: &str,
        range: std::ops::Range<u64>,
    ) -> Result<Bytes, ReadError> {
        let gcs_location = GcsLocation::try_from_str(path)?;
        self.read_object_range(&gcs_location, None, range).await
    }

    async fn read_version(&self, path: &str, version: &str) -> Result<ObjectRead, ReadError> {
        let gcs_location = GcsLocation::try_from_str(path)?;
        let generation = parse_generation(version, &gcs_location)?;
        self.read_object(&gcs_location, Some(generation)).await
    }

    async fn metadata_version(&self, path: &str, version: &str) -> Result<FileInfo, ReadError> {
        let location = GcsLocation::try_from_str(path)?;
        let generation = parse_generation(version, &location)?;
        let object = head(&*self.control().await, &location, Some(generation)).await?;
        Ok(file_info(&object, location.location().clone()))
    }

    async fn read_range_version(
        &self,
        path: &str,
        version: &str,
        range: std::ops::Range<u64>,
    ) -> Result<Bytes, ReadError> {
        let gcs_location = GcsLocation::try_from_str(path)?;
        let generation = parse_generation(version, &gcs_location)?;
        self.read_object_range(&gcs_location, Some(generation), range)
            .await
    }

    /// Lists every generation the bucket keeps: noncurrent ones exist only on a bucket
    /// with object versioning. [`LakekeeperStorage::list_soft_deleted`] lists soft-deleted
    /// generations. GCS has no delete markers.
    async fn list_versions(
        &self,
        path: &str,
        page_size: Option<usize>,
    ) -> Result<futures::stream::BoxStream<'_, Result<Vec<ObjectVersionEntry>, IOError>>, ReadError>
    {
        let location = GcsLocation::try_from_str(path)?;
        let bucket_name = location.bucket_name().to_string();
        let pages = self
            .list_pages(&location, page_size, Listing::Versions)
            .await
            .map(move |page| {
                page?
                    .objects
                    .iter()
                    .map(|object| version_entry(&bucket_name, object))
                    .collect::<Result<Vec<_>, IOError>>()
            });
        Ok(newest_first(pages))
    }

    /// Fails on a bucket without a soft-delete policy.
    async fn list_soft_deleted(
        &self,
        path: &str,
        page_size: Option<usize>,
    ) -> Result<futures::stream::BoxStream<'_, Result<Vec<SoftDeletedVersion>, IOError>>, ReadError>
    {
        let location = GcsLocation::try_from_str(path)?;
        let bucket_name = location.bucket_name().to_string();
        let pages = self
            .list_pages(&location, page_size, Listing::SoftDeleted)
            .await
            .map(move |page| {
                page?
                    .objects
                    .iter()
                    .map(|object| soft_deleted_version(&bucket_name, object))
                    .collect::<Result<Vec<_>, IOError>>()
            });
        Ok(pages.boxed())
    }

    async fn list(
        &self,
        path: &str,
        page_size: Option<usize>,
    ) -> Result<futures::stream::BoxStream<'_, Result<Vec<FileInfo>, IOError>>, InvalidLocationError>
    {
        let location = GcsLocation::try_from_str(path)?;
        let bucket_name = location.bucket_name().to_string();
        let stream = self
            .list_pages(&location, page_size, Listing::Live)
            .await
            .and_then(move |page| {
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

/// The objects a listing returns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Listing {
    /// Live objects.
    Live,
    /// Every generation the bucket keeps, live and noncurrent.
    Versions,
    /// Soft-deleted generations only.
    SoftDeleted,
}

impl GcsStorage {
    /// Pages of the objects under `location` that `listing` selects.
    async fn list_pages(
        &self,
        location: &GcsLocation,
        page_size: Option<usize>,
        listing: Listing,
    ) -> impl futures::Stream<Item = Result<ListObjectsResponse, IOError>> + Send + 'static {
        // Ensure the path ends with '/' for proper prefix matching
        let prefix = format!("{}/", location.object_name().trim_end_matches('/'));

        let mut request = self
            .control()
            .await
            .list_objects()
            .set_parent(bucket_resource(location))
            .set_prefix(prefix)
            .set_versions(listing == Listing::Versions)
            .set_soft_deleted(listing == Listing::SoftDeleted)
            .with_idempotency(true);
        if let Some(page_size) =
            page_size.and_then(|size| safe_usize_to_i32(size, location.as_str()).ok())
        {
            request = request.set_page_size(page_size);
        }

        futures::stream::unfold(request.by_page(), {
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
        })
    }

    /// Reads the current object, or the generation `generation`.
    async fn read_object(
        &self,
        gcs_location: &GcsLocation,
        generation: Option<i64>,
    ) -> Result<ObjectRead, ReadError> {
        let object = head(&*self.control().await, gcs_location, generation).await?;
        let file_size = validate_file_size(object.size, gcs_location.as_str())?;

        // The `head` above already carries the object metadata, so it is
        // surfaced here; a second request is not needed. Every read below is
        // pinned to its generation, so the metadata describes the bytes.
        let info = file_info(&object, gcs_location.location().clone());

        let bytes = if file_size == 0 {
            Bytes::new()
        } else if reads_in_one_request(&object, file_size) {
            fetch_range(
                &self.storage,
                gcs_location,
                FetchRange::whole(Some(object.generation), Some(file_size)),
            )
            .await
            .map_err(|e| ReadError::IOError(e.with_context("Failed to download object.")))?
        } else {
            parallel_chunked_read_with_fixed_generation(
                &self.storage,
                gcs_location,
                0,
                file_size,
                object.generation,
            )
            .await?
        };

        Ok(ObjectRead { bytes, info })
    }

    /// Reads a byte range of the current object, or of the generation `generation`.
    async fn read_object_range(
        &self,
        gcs_location: &GcsLocation,
        generation: Option<i64>,
        range: std::ops::Range<u64>,
    ) -> Result<Bytes, ReadError> {
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
                gcs_location.to_string(),
            ))
        })?;

        if range_size <= MAX_BYTES_PER_READ {
            return fetch_range(
                &self.storage,
                gcs_location,
                FetchRange::segment(range.start, range_size, generation),
            )
            .await
            .map_err(|e| ReadError::IOError(e.with_context("Failed to download byte range.")));
        }

        let generation = match generation {
            Some(generation) => generation,
            None => {
                head(&*self.control().await, gcs_location, None)
                    .await?
                    .generation
            }
        };
        parallel_chunked_read_with_fixed_generation(
            &self.storage,
            gcs_location,
            range.start,
            range_size,
            generation,
        )
        .await
    }
}

/// Uploads `bytes` as one object. Below [`RESUMABLE_UPLOAD_THRESHOLD`] this is a single
/// request limited to [`REQUEST_TIMEOUT`], above it a resumable upload limited to
/// [`UPLOAD_TIMEOUT`].
pub(crate) async fn upload_bytes(
    storage: &Storage,
    location: &GcsLocation,
    bytes: Bytes,
) -> Result<WriteOutcome, WriteError> {
    let resumable = bytes.len() >= RESUMABLE_UPLOAD_THRESHOLD;
    let mut request = storage
        .write_object(bucket_resource(location), location.object_name(), bytes)
        // Objects are written to unique paths, so repeating an upload is safe.
        .with_idempotency(true);
    let mut limit = REQUEST_TIMEOUT;
    if resumable {
        // All chunks of a resumable upload share one retry budget, so it is time-based.
        request =
            request.with_retry_policy(RetryableErrors.with_time_limit(UPLOAD_RETRY_TIME_LIMIT));
        limit = UPLOAD_TIMEOUT;
    }
    within(limit, location, async {
        Box::pin(request.send_unbuffered())
            .await
            .map(|object| write_outcome(&object))
            .map_err(|e| parse_error(e, location.as_str()))
    })
    .await
    .map_err(|e| e.with_context("Failed to upload object.").into())
}

/// Small objects are read in one request, large ones in parallel ranges. Objects stored with
/// `Content-Encoding: gzip` are always read in one request: only a whole read returns them
/// decompressed.
fn reads_in_one_request(object: &Object, file_size: usize) -> bool {
    file_size < MAX_BYTES_PER_READ || object.content_encoding.eq_ignore_ascii_case("gzip")
}

/// When the generation was created, as S3 reports `LastModified` for a version. The update
/// time also moves with metadata changes, such as a storage class change by lifecycle, so
/// it serves only where the creation time is missing.
fn last_modified(object: &Object) -> Option<chrono::DateTime<chrono::Utc>> {
    let time = object
        .finalize_time
        .as_ref()
        .or(object.update_time.as_ref())?;
    utc(time.seconds(), time.nanos())
}

fn utc(seconds: i64, nanos: i32) -> Option<chrono::DateTime<chrono::Utc>> {
    chrono::DateTime::from_timestamp(seconds, nanos.try_into().ok()?)
}

/// The [`FileInfo`] of an object resource. Its version is the generation.
fn file_info(object: &Object, location: Location) -> FileInfo {
    let size = crate::size_to_u64(object.size, location.as_str());
    FileInfo::new(last_modified(object), location, size)
        .with_e_tag(object_e_tag(object))
        .with_version(generation_version(object.generation))
}

/// The version of a generation. GCS numbers generations from 1; a response without one,
/// as an emulator may send, reports generation 0, which is no version.
fn generation_version(generation: i64) -> Option<String> {
    (generation > 0).then(|| generation.to_string())
}

/// The version of an object a listing returned, which always has a generation.
fn listed_version(object: &Object, location: &Location) -> Result<String, IOError> {
    generation_version(object.generation).ok_or_else(|| {
        IOError::new(
            ErrorKind::Unexpected,
            format!(
                "GCS listed an object without a generation (generation {})",
                object.generation
            ),
            location.to_string(),
        )
    })
}

fn try_parse_file_info(bucket_name: &str, object: &Object) -> Result<FileInfo, IOError> {
    Ok(file_info(object, listed_location(bucket_name, object)?))
}

/// The location of an object a list request returned.
fn listed_location(bucket_name: &str, object: &Object) -> Result<Location, IOError> {
    let gcs_path = format!("gs://{}/{}", bucket_name, object.name);
    Location::from_str(&gcs_path).map_err(|e| {
        IOError::new(
            ErrorKind::Unexpected,
            format!("Failed to parse GCS object path returned from list: {e}"),
            gcs_path.clone(),
        )
    })
}

/// The `ETag` of an object resource; the SDK reports a missing one as empty.
fn object_e_tag(object: &Object) -> Option<String> {
    Some(object.etag.clone()).filter(|e_tag| !e_tag.is_empty())
}

pub(crate) fn write_outcome(object: &Object) -> WriteOutcome {
    WriteOutcome::builder()
        .e_tag(object_e_tag(object))
        .version(generation_version(object.generation))
        .build()
}

/// Generations are positive. The API reads generation 0 as the live object, so it is
/// rejected with every other value that is not a generation.
fn parse_generation(version: &str, location: &GcsLocation) -> Result<i64, IOError> {
    match version.parse::<i64>() {
        Ok(generation) if generation > 0 => Ok(generation),
        _ => Err(IOError::new(
            ErrorKind::ConditionNotMatch,
            format!("`{version}` is not a GCS generation"),
            location.to_string(),
        )),
    }
}

fn version_entry(bucket_name: &str, object: &Object) -> Result<ObjectVersionEntry, IOError> {
    let location = listed_location(bucket_name, object)?;
    let version = listed_version(object, &location)?;
    Ok(ObjectVersionEntry::Version(
        ObjectVersion::builder()
            .size(crate::size_to_u64(object.size, location.as_str()))
            .location(location)
            .version(version)
            .e_tag(object_e_tag(object))
            .last_modified(last_modified(object))
            // A noncurrent generation carries the time it stopped being live.
            .is_latest(object.delete_time.is_none())
            .build(),
    ))
}

fn soft_deleted_version(bucket_name: &str, object: &Object) -> Result<SoftDeletedVersion, IOError> {
    let location = listed_location(bucket_name, object)?;
    let version = listed_version(object, &location)?;
    let soft_delete_time = object.soft_delete_time.as_ref();
    let hard_delete_time = object.hard_delete_time.as_ref();
    Ok(SoftDeletedVersion::builder()
        .size(crate::size_to_u64(object.size, location.as_str()))
        .location(location)
        .version(version)
        .e_tag(object_e_tag(object))
        .last_modified(last_modified(object))
        .soft_delete_time(soft_delete_time.and_then(|t| utc(t.seconds(), t.nanos())))
        .hard_delete_time(hard_delete_time.and_then(|t| utc(t.seconds(), t.nanos())))
        .restore_token(object.restore_token.clone())
        .build())
}

/// Reorders pages of generations to newest first within each key. GCS lists the keys in
/// order but documents no order for the generations of a key. The generations of the last
/// key of a page are held back until the next page, which may continue that key.
fn newest_first(
    pages: impl futures::Stream<Item = Result<Vec<ObjectVersionEntry>, IOError>> + Send + 'static,
) -> futures::stream::BoxStream<'static, Result<Vec<ObjectVersionEntry>, IOError>> {
    let state = (pages.boxed(), Vec::new(), false);
    futures::stream::unfold(state, |(mut pages, mut held, done)| async move {
        if done {
            return None;
        }
        loop {
            match pages.next().await {
                Some(Ok(page)) => {
                    held.extend(page);
                    let Some(last_key) = held.last().map(|entry| entry.location().clone()) else {
                        continue;
                    };
                    let split = held
                        .iter()
                        .position(|entry| entry.location() == &last_key)
                        .unwrap_or(held.len());
                    let rest = held.split_off(split);
                    if held.is_empty() {
                        held = rest;
                        continue;
                    }
                    let ready = newest_within_keys(std::mem::replace(&mut held, rest));
                    return Some((Ok(ready), (pages, held, false)));
                }
                Some(Err(error)) => return Some((Err(error), (pages, Vec::new(), true))),
                None if held.is_empty() => return None,
                None => {
                    let ready = newest_within_keys(std::mem::take(&mut held));
                    return Some((Ok(ready), (pages, held, true)));
                }
            }
        }
    })
    .boxed()
}

/// Sorts each run of entries that share a key by generation, newest first, keeping the order
/// of the keys.
fn newest_within_keys(mut entries: Vec<ObjectVersionEntry>) -> Vec<ObjectVersionEntry> {
    let mut start = 0;
    while start < entries.len() {
        let key = entries[start].location().clone();
        let end = entries[start..]
            .iter()
            .position(|entry| entry.location() != &key)
            .map_or(entries.len(), |offset| start + offset);
        // `version_entry` builds every entry from a positive generation.
        entries[start..end].sort_by_key(|entry| {
            std::cmp::Reverse(entry.version().parse::<i64>().unwrap_or_default())
        });
        start = end;
    }
    entries
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

/// Metadata of the live object, or of the generation `generation`.
async fn head(
    control: &StorageControl,
    location: &GcsLocation,
    generation: Option<i64>,
) -> Result<Object, ReadError> {
    let mut request = control
        .get_object()
        .set_bucket(bucket_resource(location))
        .set_object(location.object_name())
        .with_idempotency(true);
    if let Some(generation) = generation {
        request = request.set_generation(generation);
    }
    within(REQUEST_TIMEOUT, location, async {
        request
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

#[cfg(test)]
mod tests {
    use google_cloud_auth::credentials::anonymous;

    use super::*;
    use crate::gcs::{UPLOAD_TIMEOUT, build_clients};

    const PATH: &str = "gs://test-bucket/object";

    /// Storage whose endpoint accepts requests and never answers them.
    async fn unresponsive_storage() -> GcsStorage {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let app = axum::Router::new().fallback(std::future::pending::<()>);
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        build_clients(anonymous::Builder::new().build(), Some(&endpoint))
            .await
            .unwrap()
    }

    /// Asserts that `operation` fails with a timeout after `limit`.
    async fn assert_times_out<T: std::fmt::Debug>(
        limit: Duration,
        operation: impl Future<Output = Result<T, WriteError>>,
    ) {
        let result = tokio::time::timeout(Duration::from_hours(2), operation)
            .await
            .expect("the operation must not hang");
        let Err(WriteError::IOError(error)) = result else {
            panic!("expected a timeout, got {result:?}");
        };
        assert_eq!(error.kind(), ErrorKind::RequestTimeout, "{error:?}");
        assert_eq!(
            error.reason(),
            format!("GCS request did not finish within {limit:?}")
        );
    }

    #[test]
    fn gzip_encoded_objects_are_read_in_one_request() {
        let large = MAX_BYTES_PER_READ;
        assert!(reads_in_one_request(&Object::new(), large - 1));
        assert!(!reads_in_one_request(&Object::new(), large));
        assert!(reads_in_one_request(
            &Object::new().set_content_encoding("gzip"),
            large
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn single_request_upload_times_out() {
        let storage = unresponsive_storage().await;
        assert_times_out(
            REQUEST_TIMEOUT,
            storage.write(PATH, Bytes::from_static(b"data")),
        )
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn resumable_upload_times_out() {
        let storage = unresponsive_storage().await;
        let bytes = Bytes::from(vec![0; RESUMABLE_UPLOAD_THRESHOLD]);
        assert_times_out(UPLOAD_TIMEOUT, storage.write(PATH, bytes)).await;
    }

    #[tokio::test(start_paused = true)]
    async fn streaming_write_times_out_when_upload_stalls() {
        let storage = unresponsive_storage().await;
        let mut writer = storage.writer(PATH).await.unwrap();
        writer
            .write(Bytes::from(vec![0; RESUMABLE_UPLOAD_THRESHOLD]))
            .await
            .unwrap();
        assert_times_out::<()>(UPLOAD_TIMEOUT, async {
            // The stalled upload stops taking data, so a write waits for room.
            loop {
                writer.write(Bytes::from_static(b"data")).await?;
            }
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn streaming_close_times_out_when_upload_stalls() {
        let storage = unresponsive_storage().await;
        let mut writer = storage.writer(PATH).await.unwrap();
        writer
            .write(Bytes::from(vec![0; RESUMABLE_UPLOAD_THRESHOLD]))
            .await
            .unwrap();
        assert_times_out(UPLOAD_TIMEOUT, writer.close()).await;
    }

    fn generation(key: &str, generation: i64, latest: bool) -> ObjectVersionEntry {
        ObjectVersionEntry::Version(
            ObjectVersion::builder()
                .location(Location::from_str(&format!("gs://bucket/p/{key}")).unwrap())
                .version(generation.to_string())
                .is_latest(latest)
                .build(),
        )
    }

    fn summary(pages: &[Vec<ObjectVersionEntry>]) -> Vec<Vec<(String, String)>> {
        pages
            .iter()
            .map(|page| {
                page.iter()
                    .map(|entry| (entry.location().to_string(), entry.version().to_string()))
                    .collect()
            })
            .collect()
    }

    #[tokio::test]
    async fn newest_first_holds_a_key_back_across_page_breaks() {
        // The generations of a key arrive in no particular order.
        let pages = vec![
            Ok(vec![
                generation("a", 1, false),
                generation("a", 2, true),
                generation("b", 3, false),
            ]),
            Ok(vec![generation("b", 5, true)]),
            Ok(vec![generation("b", 4, false), generation("c", 6, true)]),
            Ok(vec![]),
        ];
        let out: Vec<Vec<ObjectVersionEntry>> = newest_first(futures::stream::iter(pages))
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<_, _>>()
            .unwrap();

        let key = |k: &str, g: &str| (format!("gs://bucket/p/{k}"), g.to_string());
        assert_eq!(
            summary(&out),
            vec![
                vec![key("a", "2"), key("a", "1")],
                vec![key("b", "5"), key("b", "4"), key("b", "3")],
                vec![key("c", "6")],
            ]
        );
    }

    #[tokio::test]
    async fn newest_first_ends_the_stream_at_an_error() {
        let pages = vec![
            Ok(vec![generation("a", 1, true), generation("b", 2, true)]),
            Err(IOError::new_without_location(ErrorKind::Unexpected, "boom")),
            Ok(vec![generation("c", 3, true)]),
        ];
        let out: Vec<Result<Vec<ObjectVersionEntry>, IOError>> =
            newest_first(futures::stream::iter(pages)).collect().await;

        assert_eq!(out.len(), 2);
        let first = out[0].as_ref().unwrap();
        assert_eq!(
            summary(std::slice::from_ref(first)),
            vec![vec![("gs://bucket/p/a".to_string(), "1".to_string())]]
        );
        assert_eq!(out[1].as_ref().unwrap_err().kind(), ErrorKind::Unexpected);
    }

    #[test]
    fn an_object_without_a_generation_has_no_version() {
        let location = Location::from_str("gs://bucket/p/a").unwrap();
        let object = Object::new().set_name("p/a");
        assert_eq!(file_info(&object, location).version(), None);
        assert_eq!(write_outcome(&object), WriteOutcome::default());
        let error = version_entry("bucket", &object).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Unexpected);
        assert_eq!(
            error.reason(),
            "GCS listed an object without a generation (generation 0)"
        );
    }

    #[test]
    fn a_generation_is_dated_by_its_creation() {
        let object = |json| serde_json::from_value::<Object>(json).unwrap();
        let created = "2026-10-01T00:00:00Z".parse().unwrap();
        let updated = "2026-10-20T00:00:00Z".parse().unwrap();
        assert_eq!(
            last_modified(&object(serde_json::json!({
                "finalizeTime": "2026-10-01T00:00:00Z",
                "updateTime": "2026-10-20T00:00:00Z",
            }))),
            Some(created)
        );
        assert_eq!(
            last_modified(&object(serde_json::json!({
                "updateTime": "2026-10-20T00:00:00Z",
            }))),
            Some(updated)
        );
    }

    #[test]
    fn a_soft_deleted_generation_keeps_its_restore_token() {
        let object = Object::new()
            .set_name("p/a")
            .set_generation(7)
            .set_restore_token("token");
        let entry = soft_deleted_version("bucket", &object).unwrap();
        assert_eq!(entry.version, "7");
        assert_eq!(entry.restore_token.as_deref(), Some("token"));
    }

    #[test]
    fn only_positive_generations_parse() {
        let location = GcsLocation::try_from_str(PATH).unwrap();
        assert_eq!(parse_generation("1712", &location).unwrap(), 1712);
        for version in ["0", "-1", "null", ""] {
            let error = parse_generation(version, &location).unwrap_err();
            assert_eq!(error.kind(), ErrorKind::ConditionNotMatch, "{version}");
            assert_eq!(
                error.reason(),
                format!("`{version}` is not a GCS generation")
            );
        }
    }
}
