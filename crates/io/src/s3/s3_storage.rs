use std::{collections::HashMap, ops::Range, str::FromStr, time::Duration};

use aws_sdk_s3::{
    operation::head_object::HeadObjectOutput,
    types::{Object, ObjectIdentifier, ServerSideEncryption},
};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::{StreamExt, stream};

use crate::{
    DeleteBatchError, DeleteError, ErrorKind, FileInfo, IOError, InvalidLocationError,
    LakekeeperFileWrite, LakekeeperStorage, Location, ReadError, RetryableError, WriteError,
    execute_with_parallelism,
    s3::{
        S3Location,
        s3_error::{
            parse_aws_sdk_error, parse_batch_delete_error, parse_complete_multipart_upload_error,
            parse_create_multipart_upload_error, parse_delete_error, parse_get_object_error,
            parse_head_object_error, parse_list_objects_v2_error, parse_put_object_error,
            parse_upload_part_error,
        },
    },
    safe_usize_to_i32, validate_file_size,
};

// Convert MB constants to bytes - these will always be safe conversions from u16
const MAX_BYTES_PER_REQUEST: usize = 25 * 1024 * 1024;
const DEFAULT_BYTES_PER_REQUEST: usize = 16 * 1024 * 1024;
const MAX_PARTS_PER_UPLOAD: usize = 10_000; // S3 limit for multipart uploads
const MAX_DELETE_BATCH_SIZE: usize = 1000;
/// Upper bound on best-effort cleanup work spawned from `Drop`. The abort
/// future is dropped on elapse; we still log the timeout so an orphaned
/// multipart upload is observable (S3 lifecycle rules eventually GC it,
/// but storage cost accrues until then).
const DROP_CANCEL_DURATION: Duration = Duration::from_secs(10);

#[derive(Debug, Clone)]
pub struct S3Storage {
    client: aws_sdk_s3::Client,
    aws_kms_key_arn: Option<String>,
}

impl S3Storage {
    #[must_use]
    pub fn new(client: aws_sdk_s3::Client, aws_kms_key_arn: Option<String>) -> Self {
        Self {
            client,
            aws_kms_key_arn,
        }
    }

    #[must_use]
    pub fn client(&self) -> &aws_sdk_s3::Client {
        &self.client
    }

    #[must_use]
    pub fn aws_kms_key_arn(&self) -> Option<&String> {
        self.aws_kms_key_arn.as_ref()
    }
}

#[async_trait::async_trait]
impl LakekeeperStorage for S3Storage {
    async fn delete(&self, path: &str) -> Result<(), DeleteError> {
        let s3_location = S3Location::try_from_str(path, true)?;

        self.client
            .delete_object()
            .bucket(s3_location.bucket_name())
            .key(s3_key_to_str(&s3_location.key()))
            .send()
            .await
            .map_err(|e| parse_delete_error(e, &s3_location))?;

        Ok(())
    }

    async fn delete_batch(&self, paths: &[String]) -> Result<(), DeleteBatchError> {
        let s3_locations: HashMap<String, HashMap<String, String>> = group_paths_by_bucket(paths)?;
        let key_to_path_mapping = build_key_to_path_mapping(&s3_locations);
        let delete_futures = create_delete_futures(&self.client, s3_locations)?;

        process_delete_results(delete_futures, key_to_path_mapping)
            .await
            .map_err(Into::into)
    }

    async fn write(&self, path: &str, bytes: Bytes) -> Result<(), WriteError> {
        let s3_location = S3Location::try_from_str(path, true)?;

        if bytes.len() < MAX_BYTES_PER_REQUEST {
            return put_object_single(
                &self.client,
                &s3_location,
                self.aws_kms_key_arn.as_deref(),
                bytes,
            )
            .await;
        }

        // Large file: parallel multipart upload.
        let file_size = bytes.len();
        let mut chunk_size = DEFAULT_BYTES_PER_REQUEST;
        if file_size.div_ceil(chunk_size) > MAX_PARTS_PER_UPLOAD {
            chunk_size = file_size.div_ceil(MAX_PARTS_PER_UPLOAD);
        }

        let upload_id =
            start_multipart(&self.client, &s3_location, self.aws_kms_key_arn.as_deref()).await?;

        // Zero-copy chunking: `bytes.slice(range)` produces an owned
        // refcounted view that can be moved into per-part futures.
        let upload_futures = crate::chunk_ranges(file_size, chunk_size).map(|(idx, range)| {
            let part_number = idx + 1;
            let chunk = bytes.slice(range);
            let client = self.client.clone();
            let location = s3_location.clone();
            let upload_id = upload_id.clone();
            async move {
                let part_number = safe_usize_to_i32(part_number, location.as_str())
                    .map_err(|e| e.with_context("Too many parts to write"))?;
                let part = upload_part(&client, &location, &upload_id, part_number, chunk).await?;
                Ok::<(i32, aws_sdk_s3::types::CompletedPart), WriteError>((part_number, part))
            }
        });

        let upload_results = execute_with_parallelism(upload_futures, 10);
        tokio::pin!(upload_results);

        // Drain the result stream even after the first failure so that any
        // already-spawned upload tasks finish (or fail) before we abort the
        // upload session. Keep the earliest error to surface to the caller.
        let mut completed_parts: Vec<(i32, aws_sdk_s3::types::CompletedPart)> = Vec::new();
        let mut first_error: Option<WriteError> = None;
        while let Some(result) = upload_results.next().await {
            match result {
                Err(join_err) if first_error.is_none() => {
                    first_error = Some(WriteError::IOError(IOError::new(
                        ErrorKind::Unexpected,
                        format!("Upload task panicked: {join_err}"),
                        s3_location.to_string(),
                    )));
                }
                Ok(Err(write_err)) if first_error.is_none() => {
                    first_error = Some(write_err);
                }
                Ok(Ok((part_number, completed_part))) if first_error.is_none() => {
                    completed_parts.push((part_number, completed_part));
                }
                _ => {
                    // Already errored — drop subsequent results, the upload
                    // session is going to be aborted anyway.
                }
            }
        }
        if let Some(err) = first_error {
            abort_multipart_logged_infallible(
                &self.client,
                &s3_location,
                &upload_id,
                "parallel multipart write failed",
            )
            .await;
            return Err(err);
        }
        completed_parts.sort_by_key(|(part_number, _)| *part_number);
        let completed_parts: Vec<_> = completed_parts
            .into_iter()
            .map(|(_, completed_part)| completed_part)
            .collect();

        if let Err(e) =
            complete_multipart(&self.client, &s3_location, &upload_id, completed_parts).await
        {
            abort_multipart_logged_infallible(
                &self.client,
                &s3_location,
                &upload_id,
                "complete_multipart failed after parallel write",
            )
            .await;
            return Err(e);
        }
        Ok(())
    }

    async fn writer(&self, path: &str) -> Result<Box<dyn LakekeeperFileWrite>, WriteError> {
        let s3_location = S3Location::try_from_str(path, true)?;
        Ok(Box::new(S3FileWrite {
            client: self.client.clone(),
            location: s3_location,
            kms_key_arn: self.aws_kms_key_arn.clone(),
            state: S3WriterState::Buffering(bytes::BytesMut::new()),
        }))
    }

    async fn read(&self, path: &str) -> Result<Bytes, ReadError> {
        let s3_location = S3Location::try_from_str(path, true)?;
        let head_response = head(&self.client, &s3_location).await?;
        let content_length = head_response.content_length().unwrap_or(0);
        let file_size = validate_file_size(content_length, path)?;

        if file_size == 0 {
            return Ok(Bytes::new());
        }

        if file_size < MAX_BYTES_PER_REQUEST {
            // If the file is small enough, read it in a single request
            return fetch_range(&self.client, &s3_location, 0..file_size as u64, None).await;
        }

        let etag = head_response.e_tag().map(ToString::to_string);
        parallel_chunked_read(&self.client, &s3_location, 0, file_size, etag).await
    }

    async fn read_range(&self, path: &str, range: Range<u64>) -> Result<Bytes, ReadError> {
        let s3_location = S3Location::try_from_str(path, true)?;
        if range.end < range.start {
            return Err(ReadError::IOError(IOError::new(
                ErrorKind::ConditionNotMatch,
                format!(
                    "Invalid range: start ({}) > end ({})",
                    range.start, range.end
                ),
                s3_location.to_string(),
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
                s3_location.to_string(),
            ))
        })?;

        if range_size <= MAX_BYTES_PER_REQUEST {
            return fetch_range(&self.client, &s3_location, range, None).await;
        }

        let head_response = head(&self.client, &s3_location).await?;
        let etag = head_response.e_tag().map(ToString::to_string);
        parallel_chunked_read(&self.client, &s3_location, range.start, range_size, etag).await
    }

    async fn read_single(&self, path: &str) -> Result<Bytes, ReadError> {
        let s3_location = S3Location::try_from_str(path, true)?;

        let response = self
            .client
            .get_object()
            .bucket(s3_location.bucket_name())
            .key(s3_key_to_str(&s3_location.key()))
            .send()
            .await
            .map_err(|e| parse_get_object_error(e, &s3_location))?;

        let body = response.body.collect().await.map_err(|e| {
            IOError::new(
                ErrorKind::Unexpected,
                format!("Error in S3 get bytestream: {e}"),
                s3_location.to_string(),
            )
            .set_source(anyhow::anyhow!(e))
        })?;

        Ok(body.into_bytes())
    }

    async fn metadata(&self, path: &str) -> Result<FileInfo, ReadError> {
        let s3_location = S3Location::try_from_str(path, true)?;
        let head_response = head(&self.client, &s3_location).await?;
        let location_str = s3_location.to_string();
        let size = head_response
            .content_length()
            .and_then(|n| crate::size_to_u64(n, &location_str));
        let last_modified = head_response.last_modified().and_then(parse_timestamp);
        Ok(FileInfo::new(
            last_modified,
            s3_location.location().clone(),
            size,
        ))
    }

    async fn list(
        &self,
        path: &str,
        page_size: Option<usize>,
    ) -> Result<futures::stream::BoxStream<'_, Result<Vec<FileInfo>, IOError>>, InvalidLocationError>
    {
        let path = format!("{}/", path.trim_end_matches('/'));
        let s3_location = S3Location::try_from_str(&path, true)?;
        let base_location = s3_location.location().clone();
        let s3_bucket = s3_location.bucket_name().to_string(); // Store the bucket name

        let list_request_template = self
            .client
            .list_objects_v2()
            .bucket(s3_bucket.clone())
            .prefix(s3_key_to_str(&s3_location.key()));

        let stream = stream::unfold(
            (None, false), // (continuation_token, is_done)
            move |(continuation_token, is_done)| {
                let base_location = base_location.clone();
                let list_request = list_request_template.clone();
                let s3_bucket = s3_bucket.clone(); // Clone the bucket name for use in the closure

                async move {
                    if is_done {
                        return None;
                    }

                    let mut list_request = list_request;

                    if let Some(token) = continuation_token {
                        list_request = list_request.continuation_token(token);
                    }

                    if let Some(size) = page_size {
                        list_request =
                            list_request.max_keys(i32::try_from(size).unwrap_or(i32::MAX));
                    }

                    let result = tryhard::retry_fn(|| async {
                        match list_request.clone().send().await {
                            Ok(response) => Ok(Ok(response)),
                            Err(e) => {
                                let error = parse_list_objects_v2_error(e, base_location.as_str());
                                if error.should_retry() {
                                    Err(error)
                                } else {
                                    Ok(Err(error))
                                }
                            }
                        }
                    })
                    .retries(3)
                    .exponential_backoff(std::time::Duration::from_millis(100))
                    .max_delay(std::time::Duration::from_secs(10))
                    .await;

                    match result {
                        Ok(Ok(response)) => {
                            let file_infos = response
                                .contents()
                                .iter()
                                .filter_map(try_parse_file_info(&base_location, &s3_bucket))
                                .collect::<Vec<_>>();

                            let next_continuation_token = response
                                .next_continuation_token()
                                .map(std::string::ToString::to_string);
                            let is_truncated = response.is_truncated().unwrap_or(false);
                            let next_state = (next_continuation_token, !is_truncated);

                            Some((Ok(file_infos), next_state))
                        }
                        // First case: Retryable error occurred but retries didn't resolve it
                        // Second case: Non-retryable error occurred
                        Ok(Err(error)) | Err(error) => Some((Err(error), (None, true))),
                    }
                }
            },
        );

        Ok(stream.boxed())
    }
}

fn try_parse_file_info(
    base_location: &Location,
    s3_bucket: &str,
) -> impl FnMut(&Object) -> Option<FileInfo> {
    move |object| {
        let key = object.key()?;
        let last_modified = object.last_modified().and_then(parse_timestamp);
        let scheme = base_location.scheme();
        let full_path = format!("{scheme}://{s3_bucket}/{key}");
        let location = Location::from_str(&full_path).ok()?;
        let size = object
            .size()
            .and_then(|s| crate::size_to_u64(s, &full_path));
        Some(FileInfo::new(last_modified, location, size))
    }
}

async fn fetch_range(
    client: &aws_sdk_s3::Client,
    location: &S3Location,
    range: std::ops::Range<u64>,
    if_match: Option<&str>,
) -> Result<Bytes, ReadError> {
    let mut request = client
        .get_object()
        .bucket(location.bucket_name())
        .key(s3_key_to_str(&location.key()))
        // range header for s3 client is inclusive
        .range(format!("bytes={}-{}", range.start, range.end - 1));
    if let Some(etag) = if_match {
        request = request.if_match(etag);
    }
    let response = request
        .send()
        .await
        .map_err(|e| parse_get_object_error(e, location))?;

    let body = response.body.collect().await.map_err(|e| {
        IOError::new(
            ErrorKind::Unexpected,
            format!("Error collecting S3 range bytestream: {e}"),
            location.to_string(),
        )
        .set_source(anyhow::anyhow!(e))
    })?;
    Ok(body.into_bytes())
}

async fn head(
    client: &aws_sdk_s3::Client,
    location: &S3Location,
) -> Result<HeadObjectOutput, ReadError> {
    client
        .head_object()
        .bucket(location.bucket_name())
        .key(s3_key_to_str(&location.key()))
        .send()
        .await
        .map_err(|e| ReadError::IOError(parse_head_object_error(e, location)))
}

fn parse_timestamp(timestamp: &aws_smithy_types::DateTime) -> Option<DateTime<Utc>> {
    DateTime::from_timestamp(timestamp.secs(), timestamp.subsec_nanos())
}

/// Run a parallel-chunked download over `[range_start, range_start + range_size)`.
///
/// Each chunk fetch sets `If-Match: <etag>` if present. If etag is set and
/// file is overwritten while download in flight, a `ReadError` is returned.
async fn parallel_chunked_read(
    client: &aws_sdk_s3::Client,
    s3_location: &S3Location,
    range_start: u64,
    range_size: usize,
    if_match: Option<String>,
) -> Result<Bytes, ReadError> {
    if range_size == 0 {
        return Ok(Bytes::new());
    }

    let client = client.clone();
    let location_for_chunks = s3_location.clone();

    crate::parallel_chunked_read(
        range_size,
        DEFAULT_BYTES_PER_REQUEST,
        10,
        s3_location.as_str(),
        move |rel_start, rel_end, chunk_index| {
            let client = client.clone();
            let location = location_for_chunks.clone();
            let if_match = if_match.clone();
            let abs_start = range_start + rel_start as u64;
            let abs_end = range_start + rel_end as u64 + 1;
            async move {
                let chunk =
                    fetch_range(&client, &location, abs_start..abs_end, if_match.as_deref())
                        .await
                        .map_err(|e| {
                            match e {
                        ReadError::IOError(io) => ReadError::IOError(io.with_context(format!(
                            "Failed to download chunk {chunk_index} (bytes {abs_start}-{abs_end})"
                        ))),
                        invalid_location_error @ ReadError::InvalidLocation(_) => {
                            invalid_location_error
                        }
                    }
                        })?;
                Ok((chunk_index, chunk))
            }
        },
    )
    .await
}
/// Upload a small object in a single PUT request.
async fn put_object_single(
    client: &aws_sdk_s3::Client,
    location: &S3Location,
    kms_key_arn: Option<&str>,
    bytes: Bytes,
) -> Result<(), WriteError> {
    let mut put = client
        .put_object()
        .bucket(location.bucket_name())
        .key(s3_key_to_str(&location.key()))
        .body(bytes.into());
    if let Some(arn) = kms_key_arn {
        put = put
            .set_server_side_encryption(Some(ServerSideEncryption::AwsKms))
            .set_ssekms_key_id(Some(arn.to_string()));
    }
    put.send()
        .await
        .map_err(|e| WriteError::IOError(parse_put_object_error(e, location.as_str())))?;
    Ok(())
}

/// Initiate a multipart upload, returning the SDK-issued upload id.
async fn start_multipart(
    client: &aws_sdk_s3::Client,
    location: &S3Location,
    kms_key_arn: Option<&str>,
) -> Result<String, WriteError> {
    let mut create = client
        .create_multipart_upload()
        .bucket(location.bucket_name())
        .key(s3_key_to_str(&location.key()));
    if let Some(arn) = kms_key_arn {
        create = create
            .set_server_side_encryption(Some(ServerSideEncryption::AwsKms))
            .set_ssekms_key_id(Some(arn.to_string()));
    }
    let response = create.send().await.map_err(|e| {
        WriteError::IOError(
            parse_create_multipart_upload_error(e, location.as_str())
                .with_context("Failed to create multipart upload."),
        )
    })?;
    response
        .upload_id()
        .map(ToString::to_string)
        .ok_or_else(|| {
            WriteError::IOError(IOError::new(
                ErrorKind::Unexpected,
                "S3 multipart upload response missing upload_id".to_string(),
                location.to_string(),
            ))
        })
}

/// Upload a single multipart part. Returns the SDK [`CompletedPart`]
/// descriptor expected by `complete_multipart`.
async fn upload_part(
    client: &aws_sdk_s3::Client,
    location: &S3Location,
    upload_id: &str,
    part_number: i32,
    bytes: Bytes,
) -> Result<aws_sdk_s3::types::CompletedPart, WriteError> {
    let chunk_len = bytes.len();
    let response = client
        .upload_part()
        .bucket(location.bucket_name())
        .key(s3_key_to_str(&location.key()))
        .upload_id(upload_id)
        .part_number(part_number)
        .body(bytes.into())
        .send()
        .await
        .map_err(|e| {
            WriteError::IOError(parse_upload_part_error(e, location.as_str()).with_context(
                format!("Failed to upload part {part_number} ({chunk_len} bytes)"),
            ))
        })?;
    let etag = response.e_tag().ok_or_else(|| {
        WriteError::IOError(IOError::new(
            ErrorKind::Unexpected,
            format!("S3 upload part response missing ETag for part {part_number}"),
            location.to_string(),
        ))
    })?;
    Ok(aws_sdk_s3::types::CompletedPart::builder()
        .part_number(part_number)
        .e_tag(etag)
        .build())
}

/// Finalise a multipart upload using the previously uploaded parts.
async fn complete_multipart(
    client: &aws_sdk_s3::Client,
    location: &S3Location,
    upload_id: &str,
    completed_parts: Vec<aws_sdk_s3::types::CompletedPart>,
) -> Result<(), WriteError> {
    let multipart = aws_sdk_s3::types::CompletedMultipartUpload::builder()
        .set_parts(Some(completed_parts))
        .build();
    client
        .complete_multipart_upload()
        .bucket(location.bucket_name())
        .key(s3_key_to_str(&location.key()))
        .upload_id(upload_id)
        .multipart_upload(multipart)
        .send()
        .await
        .map_err(|e| {
            WriteError::IOError(
                parse_complete_multipart_upload_error(e, location.as_str())
                    .with_context("Failed to complete multipart upload."),
            )
        })?;
    Ok(())
}

/// Best-effort multipart abort used on the streaming-writer error path.
async fn abort_multipart(
    client: &aws_sdk_s3::Client,
    location: &S3Location,
    upload_id: &str,
) -> Result<(), WriteError> {
    client
        .abort_multipart_upload()
        .bucket(location.bucket_name())
        .key(s3_key_to_str(&location.key()))
        .upload_id(upload_id)
        .send()
        .await
        .map_err(|e| {
            WriteError::IOError(IOError::new(
                ErrorKind::Unexpected,
                format!(
                    "Failed to abort S3 multipart upload. Partial upload may result in storage cost. {e}"
                ),
                location.to_string(),
            ))
        })?;
    Ok(())
}

/// Best-effort multipart abort that swallows the abort error after logging it.
///
/// Use on cleanup paths where the caller propagates a different (original)
/// error and the abort failure is unactionable: one-shot bulk write, a
/// `complete_multipart` failure, or `Drop`. The `context` field is included in
/// the warn log to disambiguate which abort site triggered the cleanup.
async fn abort_multipart_logged_infallible(
    client: &aws_sdk_s3::Client,
    location: &S3Location,
    upload_id: &str,
    context: &str,
) {
    if let Err(e) = abort_multipart(client, location, upload_id).await {
        tracing::warn!(
            location = %location,
            error = ?e,
            context = %context,
            "Failed to abort S3 multipart upload. Incomplete upload may exist in target location and incur storage cost until bucket lifecycle rules clean it up.",
        );
    }
}

/// Streaming writer for S3.
///
/// Buffers bytes locally; falls back to `PutObject` for files that fit in
/// `MAX_BYTES_PER_REQUEST`, and switches to a multipart upload as soon as
/// more than that has been written. Each part flush uses
/// `DEFAULT_BYTES_PER_REQUEST` (≥ S3's 5 `MiB` minimum, except the final
/// part which has no minimum).
///
/// Zero-copy invariant: incoming `Bytes` are appended into a local
/// `BytesMut` (one copy, unavoidable to span multiple `write` calls);
/// each part is then handed off to `upload_part` zero-copy via
/// `BytesMut::split_to(N).freeze()`.
pub(crate) struct S3FileWrite {
    client: aws_sdk_s3::Client,
    location: S3Location,
    kms_key_arn: Option<String>,
    state: S3WriterState,
}

impl std::fmt::Debug for S3FileWrite {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3FileWrite")
            .field("location", &self.location)
            .field("state", &self.state)
            .finish_non_exhaustive()
    }
}

enum S3WriterState {
    Buffering(bytes::BytesMut),
    Multipart {
        upload_id: String,
        next_part_number: i32,
        completed_parts: Vec<aws_sdk_s3::types::CompletedPart>,
        buffer: bytes::BytesMut,
    },
    Closed,
    Aborted,
    AbortFailed,
}

impl std::fmt::Debug for S3WriterState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Buffering(buffer) => f
                .debug_tuple("Buffering")
                .field(&format_args!("{} bytes", buffer.len()))
                .finish(),
            Self::Multipart {
                next_part_number,
                completed_parts,
                buffer,
                ..
            } => f
                .debug_struct("Multipart")
                .field("next_part_number", next_part_number)
                .field("completed_parts", &completed_parts.len())
                .field("buffered_bytes", &buffer.len())
                .finish_non_exhaustive(),
            Self::Closed => f.write_str("Closed"),
            Self::Aborted => f.write_str("Aborted"),
            Self::AbortFailed => f.write_str("AbortFailed"),
        }
    }
}

#[async_trait::async_trait]
impl LakekeeperFileWrite for S3FileWrite {
    async fn write(&mut self, bytes_in: Bytes) -> Result<(), WriteError> {
        match &mut self.state {
            S3WriterState::Closed => {
                return Err(WriteError::IOError(IOError::new(
                    ErrorKind::ConditionNotMatch,
                    "Cannot write to closed writer",
                    self.location.to_string(),
                )));
            }
            S3WriterState::Aborted => {
                return Err(WriteError::IOError(IOError::new(
                    ErrorKind::ConditionNotMatch,
                    "Cannot write to aborted writer",
                    self.location.to_string(),
                )));
            }
            S3WriterState::AbortFailed => {
                return Err(WriteError::IOError(IOError::new(
                    ErrorKind::ConditionNotMatch,
                    "Cannot write to writer that failed to abort",
                    self.location.to_string(),
                )));
            }
            S3WriterState::Buffering(buffer) => {
                buffer.extend_from_slice(&bytes_in);
                if buffer.len() < MAX_BYTES_PER_REQUEST {
                    return Ok(());
                }
                let upload_id =
                    start_multipart(&self.client, &self.location, self.kms_key_arn.as_deref())
                        .await?;
                let rest = std::mem::take(buffer);
                self.state = S3WriterState::Multipart {
                    upload_id,
                    next_part_number: 1,
                    completed_parts: Vec::new(),
                    buffer: rest,
                };
                self.flush_multipart_buffer().await?;
            }
            S3WriterState::Multipart { buffer, .. } => {
                buffer.extend_from_slice(&bytes_in);
                self.flush_multipart_buffer().await?;
            }
        }
        Ok(())
    }

    /// Closes the streaming write to S3.
    ///
    /// If an error occurred during closing, subsequent calls will only
    /// return generic error. Callers need to inspect `write` errors
    /// or first `close` error, if required.
    async fn close(&mut self) -> Result<(), WriteError> {
        let state = std::mem::replace(&mut self.state, S3WriterState::Closed);
        match state {
            S3WriterState::Closed | S3WriterState::Aborted | S3WriterState::AbortFailed => {
                Err(WriteError::IOError(IOError::new(
                    ErrorKind::ConditionNotMatch,
                    "Writer already closed or aborted",
                    self.location.to_string(),
                )))
            }
            S3WriterState::Buffering(buffer) => {
                put_object_single(
                    &self.client,
                    &self.location,
                    self.kms_key_arn.as_deref(),
                    buffer.freeze(),
                )
                .await
            }
            S3WriterState::Multipart {
                upload_id,
                next_part_number,
                mut completed_parts,
                buffer,
            } => {
                if !buffer.is_empty() {
                    let part = match upload_part(
                        &self.client,
                        &self.location,
                        &upload_id,
                        next_part_number,
                        buffer.freeze(),
                    )
                    .await
                    {
                        Ok(part) => part,
                        Err(upload_error) => {
                            // Always surface the original upload error. The
                            // abort attempt is best-effort; its failure is
                            // logged and reflected in `state` for `Drop`,
                            // but never masks `upload_error`.
                            match abort_multipart(&self.client, &self.location, &upload_id).await {
                                Ok(()) => self.state = S3WriterState::Aborted,
                                Err(abort_error) => {
                                    self.state = S3WriterState::AbortFailed;
                                    tracing::warn!(
                                        location = %self.location,
                                        error = ?abort_error,
                                        "Failed to abort S3 multipart upload after tail upload error during close; \
                                         incomplete upload may exist until S3-side expiry. \
                                         Original upload error is being returned.",
                                    );
                                }
                            }
                            return Err(upload_error);
                        }
                    };
                    // Note: no need to increase next_part_number here,
                    // because state is dropped after `close` finishes
                    // and we can no longer reach the `Multipart` state.
                    // We still need to record `completed_parts` to finish
                    // the the multipart upload.
                    completed_parts.push(part);
                }
                if let Err(e) =
                    complete_multipart(&self.client, &self.location, &upload_id, completed_parts)
                        .await
                {
                    abort_multipart_logged_infallible(
                        &self.client,
                        &self.location,
                        &upload_id,
                        "complete_multipart failed during close",
                    )
                    .await;
                    return Err(e);
                }
                Ok(())
            }
        }
    }
}

impl S3FileWrite {
    /// Flush every full `DEFAULT_BYTES_PER_REQUEST`-sized part the multipart
    /// buffer can produce. Any tail smaller than the part size remains
    /// buffered and is flushed by `close` as the final (size-unconstrained)
    /// part. The caller is responsible for appending new bytes to the
    /// buffer before invoking this method.
    async fn flush_multipart_buffer(&mut self) -> Result<(), WriteError> {
        let S3WriterState::Multipart {
            upload_id,
            next_part_number,
            completed_parts,
            buffer,
        } = &mut self.state
        else {
            return Ok(());
        };

        while buffer.len() >= DEFAULT_BYTES_PER_REQUEST {
            let part_bytes = buffer.split_to(DEFAULT_BYTES_PER_REQUEST).freeze();
            match upload_part(
                &self.client,
                &self.location,
                upload_id,
                *next_part_number,
                part_bytes,
            )
            .await
            {
                Ok(part) => {
                    completed_parts.push(part);
                    *next_part_number += 1;
                }
                Err(upload_error) => {
                    let upload_id = upload_id.clone();
                    // Always surface the original upload error. The abort
                    // attempt is best-effort; its failure is logged and
                    // reflected in `state` for `Drop`, but never masks
                    // `upload_error`.
                    match abort_multipart(&self.client, &self.location, &upload_id).await {
                        Ok(()) => self.state = S3WriterState::Aborted,
                        Err(abort_error) => {
                            self.state = S3WriterState::AbortFailed;
                            tracing::warn!(
                                location = %self.location,
                                error = ?abort_error,
                                "Failed to abort S3 multipart upload after part upload error; \
                                 incomplete upload may exist until S3-side expiry. \
                                 Original upload error is being returned.",
                            );
                        }
                    }
                    return Err(upload_error);
                }
            }
        }
        Ok(())
    }
}

impl Drop for S3FileWrite {
    fn drop(&mut self) {
        let state = std::mem::replace(&mut self.state, S3WriterState::Aborted);
        let S3WriterState::Multipart { upload_id, .. } = state else {
            // Buffering: nothing on S3 yet.
            // Closed / Aborted / AbortFailed: terminal states, no action.
            return;
        };

        //`Handle::current()` panics when called
        // outside a tokio runtime (runtime already shut down) or
        // race with shutdown
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            self.state = S3WriterState::AbortFailed;
            tracing::warn!(
                location = %self.location,
                "S3FileWrite dropped without closing outside runtime, cannot abort multipart upload. Incomplete file may exist in target location."
            );
            return;
        };

        // Once stable `std::future::AsyncDrop`
        // (https://doc.rust-lang.org/std/future/trait.AsyncDrop.html —
        // currently nightly-only and experimental) exists, we can change this
        // to `abort_multipart.await` without `spawn`. The bounded
        // `DROP_CANCEL_DURATION` protects against a stuck abort call holding
        // the spawned task on a shutting-down runtime; the elapse path is
        // logged so an orphaned multipart upload is observable.
        let client = self.client.clone();
        let location = self.location.clone();
        handle.spawn(async move {
            if tokio::time::timeout(
                DROP_CANCEL_DURATION,
                abort_multipart_logged_infallible(
                    &client,
                    &location,
                    &upload_id,
                    "writer dropped without closing",
                ),
            )
            .await
            .is_err()
            {
                tracing::warn!(
                    location = %location,
                    timeout = ?DROP_CANCEL_DURATION,
                    "Best-effort abort of un-closed S3 multipart upload timed out. Incomplete upload may exist until S3 lifecycle GC.",
                );
            }
        });
    }
}

/// Groups paths by S3 bucket and ensures uniqueness of keys per bucket.
/// Returns a map from bucket name to a map of S3 key to original path.
///
/// Output is a `HashMap` where:
/// - Key: Bucket name
/// - Value: A `HashMap` of S3 keys to their original paths
fn group_paths_by_bucket(
    paths: impl IntoIterator<Item = impl AsRef<str>>,
) -> Result<HashMap<String, HashMap<String, String>>, InvalidLocationError> {
    let mut s3_locations: HashMap<String, HashMap<String, String>> = HashMap::new();

    for p in paths {
        let path = p.as_ref();
        let s3_location = S3Location::try_from_str(path, true)?;
        let bucket = s3_location.bucket_name().to_string();
        let key = s3_key_to_str(&s3_location.key());
        s3_locations
            .entry(bucket)
            .or_default()
            .insert(key, path.to_string());
    }

    Ok(s3_locations)
}

/// Builds a global key-to-path mapping
///
/// Input is a `HashMap` where:
/// - Key: Bucket name
/// - Value: A `HashMap` of S3 keys to their original paths
fn build_key_to_path_mapping(
    s3_locations: &HashMap<String, HashMap<String, String>>,
) -> HashMap<String, String> {
    let mut key_to_path_mapping: HashMap<String, String> = HashMap::new();

    for keys in s3_locations.values() {
        for (key, path) in keys {
            key_to_path_mapping.insert(key.clone(), path.clone());
        }
    }

    key_to_path_mapping
}

#[derive(derive_more::From, Debug)]
enum AWSBatchDeleteError {
    SDKError(
        aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::delete_objects::DeleteObjectsError>,
    ),
    IOError(IOError),
}

/// Creates delete futures for batch operations, processing keys in batches of `MAX_DELETE_BATCH_SIZE`.
fn create_delete_futures(
    client: &aws_sdk_s3::Client,
    s3_locations: HashMap<String, HashMap<String, String>>,
) -> Result<
    impl Iterator<
        Item = impl std::future::Future<
            Output = Result<
                aws_sdk_s3::operation::delete_objects::DeleteObjectsOutput,
                AWSBatchDeleteError,
            >,
        > + Send
               + 'static,
    >,
    InvalidLocationError,
> {
    let mut delete_futures = Vec::new();

    for (bucket, keys) in s3_locations {
        // Process keys in batches of MAX_DELETE_BATCH_SIZE
        for key_batch in keys
            .into_iter()
            .collect::<Vec<_>>()
            .chunks(MAX_DELETE_BATCH_SIZE)
        {
            let objects: Vec<ObjectIdentifier> = key_batch
                .iter()
                .map(|key| {
                    ObjectIdentifier::builder()
                        .key(&key.0)
                        .build()
                        .map_err(|e| {
                            InvalidLocationError::new(
                                key.0.clone(),
                                format!("Could not build S3 ObjectIdentifier: {e}"),
                            )
                        })
                })
                .collect::<Result<_, _>>()?;

            let delete = aws_sdk_s3::types::Delete::builder()
                .set_objects(Some(objects))
                .build()
                .map_err(|e| {
                    InvalidLocationError::new(
                        format!("s3://{bucket}"),
                        format!("Could not build S3 Delete: {e}"),
                    )
                })?;

            let bucket_clone = bucket.clone();
            let client_clone = client.clone();
            let delete_future = async move {
                client_clone
                    .delete_objects()
                    .bucket(&bucket_clone)
                    .delete(delete)
                    .send()
                    .await
                    .map_err(AWSBatchDeleteError::SDKError)
            };

            delete_futures.push(delete_future);
        }
    }

    Ok(delete_futures.into_iter())
}

/// Processes delete results and handles errors as they complete.
async fn process_delete_results(
    delete_futures: impl Iterator<
        Item = impl std::future::Future<
            Output = Result<
                aws_sdk_s3::operation::delete_objects::DeleteObjectsOutput,
                AWSBatchDeleteError,
            >,
        > + Send
               + 'static,
    >,
    key_to_path_mapping: HashMap<String, String>,
) -> Result<(), IOError> {
    // Execute delete operations with parallelism limit of 100
    let delete_results = execute_with_parallelism(delete_futures, 100);
    tokio::pin!(delete_results);

    let completed_batches = std::sync::atomic::AtomicU64::new(0);
    let mut total_batches = 0;

    while let Some(result) = delete_results.next().await {
        let completed_batch = completed_batches.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        total_batches += 1;

        // Handle join error
        let aws_result = result.map_err(|e| {
            IOError::new_without_location(
                ErrorKind::Unexpected,
                format!("Delete task panicked: {e}"),
            )
            .with_context("S3 batch delete")
        })?;

        // Increment the counter for each processed batch
        match aws_result {
            Ok(output) => {
                // Check if there were any errors in the delete operation response
                let errors = output.errors();
                let total_errors = errors.len();
                if let Some(error) = errors.first() {
                    let error_key = error.key().map(String::from);
                    let path = error_key
                        .as_ref()
                        .and_then(|key| key_to_path_mapping.get(key))
                        .cloned()
                        .or(error_key)
                        .unwrap_or_else(|| "Unknown".to_string());

                    return Err(
                        parse_aws_sdk_error(error, path.as_str()).with_context(format!(
                            "Delete batch {completed_batch} out of {total_batches} failed with {total_errors} errors"
                        )),
                    );
                }
            }
            Err(e) => {
                // Network or other SDK-level error
                match e {
                    AWSBatchDeleteError::IOError(io_error) => return Err(io_error),
                    AWSBatchDeleteError::SDKError(sdk_error) => {
                        return Err(parse_batch_delete_error(sdk_error).with_context(format!(
                            "Delete batch {completed_batch} out of {total_batches} failed"
                        )));
                    }
                }
            }
        }
    }

    Ok(())
}

fn s3_key_to_str(key: &[&str]) -> String {
    if key.is_empty() {
        return String::new();
    }
    key.join("/")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_s3_key_to_str() {
        // Keys should not start with a slash!
        assert_eq!(s3_key_to_str(&[]), "");
        assert_eq!(s3_key_to_str(&["a"]), "a");
        assert_eq!(s3_key_to_str(&["a", "b"]), "a/b");
        assert_eq!(s3_key_to_str(&["a", ""]), "a/");
    }
}
