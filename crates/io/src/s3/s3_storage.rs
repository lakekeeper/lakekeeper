use std::{collections::HashMap, ops::Range, str::FromStr, sync::Arc, time::Duration};

use aws_sdk_s3::{
    operation::head_object::HeadObjectOutput,
    types::{Object, ObjectIdentifier, ServerSideEncryption},
};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::{StreamExt, stream};

use crate::{
    DeleteBatchError, DeleteError, ErrorKind, FileInfo, IOError, InvalidLocationError,
    LakekeeperFileWrite, LakekeeperStorage, Location, ObjectRead, ReadError, RetryableError,
    WriteError,
    delete_limit::{DeleteEndpoint, DeleteLimit, S3_DELETE_REQUESTS},
    execute_with_parallelism,
    s3::{
        S3Location,
        s3_error::{
            parse_aws_sdk_error, parse_batch_delete_error, parse_complete_multipart_upload_error,
            parse_create_multipart_upload_error, parse_delete_error, parse_get_bucket_policy_error,
            parse_get_object_error, parse_head_object_error, parse_list_objects_v2_error,
            parse_put_object_error, parse_upload_part_error,
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
    /// Names the S3 service for the process-wide [`DeleteLimit`] of its buckets.
    service_scope: String,
}

impl S3Storage {
    /// Batch deletes of buckets with the same name in the same region share one process-wide
    /// limit: the client exposes no endpoint.
    #[must_use]
    pub fn new(client: aws_sdk_s3::Client, aws_kms_key_arn: Option<String>) -> Self {
        let service_scope = client
            .config()
            .region()
            .map_or_else(|| "aws".to_string(), |region| format!("aws-{region}"));
        Self {
            client,
            aws_kms_key_arn,
            service_scope,
        }
    }

    /// Names the S3 service of the client, for the [`DeleteLimit`] of its buckets.
    #[must_use]
    pub(crate) fn with_service_scope(mut self, service_scope: String) -> Self {
        self.service_scope = service_scope;
        self
    }

    #[must_use]
    pub fn client(&self) -> &aws_sdk_s3::Client {
        &self.client
    }

    #[must_use]
    pub fn aws_kms_key_arn(&self) -> Option<&String> {
        self.aws_kms_key_arn.as_ref()
    }

    /// The policy document of `bucket`, or `None` if it has none.
    ///
    /// # Errors
    /// Fails if the policy cannot be read, for example because the credential
    /// is not allowed to.
    pub async fn bucket_policy(&self, bucket: &str) -> Result<Option<String>, IOError> {
        match self.client.get_bucket_policy().bucket(bucket).send().await {
            Ok(output) => Ok(output.policy),
            Err(e) => parse_get_bucket_policy_error(e, bucket),
        }
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
        let buckets = group_paths_by_bucket(paths)?;
        // The keys move into their chunks. A request builds its SDK input once it holds a
        // permit, so at most `S3_DELETE_REQUESTS` inputs per bucket exist at once. Each bucket
        // runs its own requests, so a bucket whose permits are taken does not hold up the others.
        let mut per_bucket = Vec::with_capacity(buckets.len());
        for (bucket, keys) in buckets {
            let limit = DeleteLimit::of(&DeleteEndpoint::S3 {
                scope: &self.service_scope,
                bucket: &bucket,
            });
            let bucket = Arc::<str>::from(bucket);
            let mut keys = keys.into_iter().peekable();
            let mut chunks = Vec::new();
            while keys.peek().is_some() {
                chunks.push(DeleteChunk {
                    bucket: bucket.clone(),
                    limit: limit.clone(),
                    keys: keys.by_ref().take(MAX_DELETE_BATCH_SIZE).collect(),
                });
            }
            let client = self.client.clone();
            let requests = chunks
                .into_iter()
                .map(move |chunk| delete_chunk(client.clone(), chunk));
            per_bucket.push(Box::pin(execute_with_parallelism(
                requests,
                S3_DELETE_REQUESTS,
            )));
        }

        let mut results = stream::select_all(per_bucket);
        while let Some(result) = results.next().await {
            result.map_err(|join_error| {
                IOError::new_without_location(
                    ErrorKind::Unexpected,
                    format!("Delete task panicked: {join_error}"),
                )
                .with_context("S3 batch delete")
            })??;
        }
        Ok(())
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

    async fn read(&self, path: &str) -> Result<ObjectRead, ReadError> {
        let s3_location = S3Location::try_from_str(path, true)?;
        let head_response = head(&self.client, &s3_location).await?;
        let content_length = head_response.content_length().unwrap_or(0);
        let file_size = validate_file_size(content_length, path)?;

        // The `head` above already carries the object metadata, so it is
        // surfaced here; a second request is not needed.
        let location_str = s3_location.to_string();
        let info = FileInfo::new(
            head_response.last_modified().and_then(parse_timestamp),
            s3_location.location().clone(),
            head_response
                .content_length()
                .and_then(|n| crate::size_to_u64(n, &location_str)),
        )
        .with_e_tag(head_response.e_tag().map(ToString::to_string));

        let bytes = if file_size == 0 {
            Bytes::new()
        } else if file_size < MAX_BYTES_PER_REQUEST {
            // If the file is small enough, read it in a single request
            fetch_range(&self.client, &s3_location, 0..file_size as u64, None).await?
        } else {
            let etag = head_response.e_tag().map(ToString::to_string);
            parallel_chunked_read(&self.client, &s3_location, 0, file_size, etag).await?
        };

        Ok(ObjectRead { bytes, info })
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
        Ok(
            FileInfo::new(last_modified, s3_location.location().clone(), size)
                .with_e_tag(head_response.e_tag().map(ToString::to_string)),
        )
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
    put.customize()
        .config_override(crate::s3::transfer_config_override())
        .send()
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
        .customize()
        .config_override(crate::s3::transfer_config_override())
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
        .customize()
        .config_override(crate::s3::transfer_config_override())
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

/// Keys of one bucket, each with the path it was given as, deleted with one `DeleteObjects`
/// request.
struct DeleteChunk {
    bucket: Arc<str>,
    limit: DeleteLimit,
    keys: Vec<(String, String)>,
}

/// Deletes the keys of `chunk` while holding a permit of the bucket's [`DeleteLimit`].
async fn delete_chunk(
    client: aws_sdk_s3::Client,
    chunk: DeleteChunk,
) -> Result<(), DeleteBatchError> {
    let DeleteChunk {
        bucket,
        limit,
        keys,
    } = chunk;
    let output = limit
        .run(async {
            let objects = keys
                .iter()
                .map(|(key, _)| {
                    ObjectIdentifier::builder().key(key).build().map_err(|e| {
                        InvalidLocationError::new(
                            key.clone(),
                            format!("Could not build S3 ObjectIdentifier: {e}"),
                        )
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let delete = aws_sdk_s3::types::Delete::builder()
                .set_objects(Some(objects))
                .build()
                .map_err(|e| {
                    InvalidLocationError::new(
                        format!("s3://{bucket}"),
                        format!("Could not build S3 Delete: {e}"),
                    )
                })?;
            client
                .delete_objects()
                .bucket(bucket.as_ref())
                .delete(delete)
                .send()
                .await
                .map_err(|e| {
                    DeleteBatchError::from(parse_batch_delete_error(e).with_context(format!(
                        "Failed to delete {} keys of bucket {bucket}",
                        keys.len()
                    )))
                })
        })
        .await?;

    // Some S3-compatible stores report a key that does not exist; it counts as deleted.
    let mut errors = output
        .errors()
        .iter()
        .filter(|error| error.code() != Some("NoSuchKey"));
    let Some(error) = errors.next() else {
        return Ok(());
    };
    let failed = 1 + errors.count();
    let key = error.key().unwrap_or_default();
    let path = keys
        .iter()
        .find(|(sent, _)| sent == key)
        .map_or_else(|| format!("s3://{bucket}/{key}"), |(_, path)| path.clone());
    Err(parse_aws_sdk_error(error, &path)
        .with_context(format!(
            "{failed} of {} keys of bucket {bucket} were not deleted",
            keys.len()
        ))
        .into())
}

fn s3_key_to_str(key: &[&str]) -> String {
    if key.is_empty() {
        return String::new();
    }
    key.join("/")
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use super::*;

    #[test]
    fn test_s3_key_to_str() {
        // Keys should not start with a slash!
        assert_eq!(s3_key_to_str(&[]), "");
        assert_eq!(s3_key_to_str(&["a"]), "a");
        assert_eq!(s3_key_to_str(&["a", "b"]), "a/b");
        assert_eq!(s3_key_to_str(&["a", ""]), "a/");
    }

    /// Storage whose endpoint answers every request with `status` and `body`.
    async fn storage_answering(status: &'static str, body: &'static str) -> S3Storage {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let mut request = [0; 4096];
                let _ = socket.read(&mut request).await;
                let response = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: application/xml\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = socket.write_all(response.as_bytes()).await;
            }
        });
        let config = aws_sdk_s3::Config::builder()
            .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
            .region(aws_sdk_s3::config::Region::new("eu01"))
            .endpoint_url(format!("http://{addr}"))
            .force_path_style(true)
            .credentials_provider(aws_sdk_s3::config::Credentials::new(
                "access", "secret", None, None, "test",
            ))
            .retry_config(aws_sdk_s3::config::retry::RetryConfig::disabled())
            .build();
        S3Storage::new(aws_sdk_s3::Client::from_conf(config), None)
    }

    const EMPTY_DELETE_RESULT: &str = r#"<?xml version="1.0" encoding="UTF-8"?><DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></DeleteResult>"#;

    fn delete_result_with_error(key: &str, code: &str) -> String {
        format!(
            r#"<?xml version="1.0" encoding="UTF-8"?><DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Error><Key>{key}</Key><Code>{code}</Code><Message>{code}</Message></Error></DeleteResult>"#
        )
    }

    fn xml(body: String) -> impl axum::response::IntoResponse {
        ([("content-type", "application/xml")], body)
    }

    /// Serves `app` on a local endpoint and returns its URL.
    async fn serve(app: axum::Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        endpoint
    }

    /// Storage of the S3 service at `endpoint`, built the way a warehouse builds it.
    async fn storage_at(endpoint: &str) -> S3Storage {
        let credential = crate::s3::S3Auth::AccessKey(
            crate::s3::S3AccessKeyAuth::builder()
                .aws_access_key_id("access".to_string())
                .aws_secret_access_key("secret".to_string())
                .build(),
        );
        crate::s3::S3Settings::builder()
            .endpoint(Some(endpoint.parse().unwrap()))
            .region("local".to_string())
            .path_style_access(Some(true))
            .build()
            .get_storage_client(Some(&credential))
            .await
    }

    fn paths(bucket: &str, count: usize) -> Vec<String> {
        (0..count)
            .map(|i| format!("s3://{bucket}/key-{i}"))
            .collect()
    }

    #[tokio::test]
    async fn a_key_reported_missing_counts_as_deleted() {
        let app = axum::Router::new()
            .fallback(|| async { xml(delete_result_with_error("missing", "NoSuchKey")) });
        let storage = storage_at(&serve(app).await).await;
        storage
            .delete_batch(&["s3://bucket-a/missing".to_string()])
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn a_key_error_names_the_path_in_its_bucket() {
        // The same key in two buckets; only the second bucket refuses it.
        let app = axum::Router::new().fallback(|uri: axum::http::Uri| async move {
            if uri.path().starts_with("/bucket-b") {
                xml(delete_result_with_error("same/key", "AccessDenied"))
            } else {
                xml(EMPTY_DELETE_RESULT.to_string())
            }
        });
        let storage = storage_at(&serve(app).await).await;
        let error = storage
            .delete_batch(&[
                "s3a://bucket-a/same/key".to_string(),
                "s3://bucket-b/same/key".to_string(),
            ])
            .await
            .unwrap_err();
        let DeleteBatchError::IOError(error) = error else {
            panic!("expected an IO error, got {error:?}");
        };
        assert_eq!(error.kind(), ErrorKind::PermissionDenied);
        assert_eq!(error.location(), Some("s3://bucket-b/same/key"));
    }

    #[tokio::test]
    async fn a_bucket_whose_limit_is_taken_does_not_hold_up_the_other_buckets() {
        let free_requests = Arc::new(AtomicUsize::new(0));
        let app = axum::Router::new().fallback({
            let free_requests = free_requests.clone();
            move |uri: axum::http::Uri| {
                let free_requests = free_requests.clone();
                async move {
                    if uri.path().starts_with("/free") {
                        free_requests.fetch_add(1, Ordering::SeqCst);
                    }
                    xml(EMPTY_DELETE_RESULT.to_string())
                }
            }
        });
        let storage = storage_at(&serve(app).await).await;
        // Each held bucket has as many chunks as one bucket may send at once. The buckets of a
        // batch come in any order, so several held buckets make it likely that one comes first.
        let mut held = Vec::new();
        let mut batch = paths("free", 1);
        for bucket in ["held-a", "held-b", "held-c"] {
            let limit = DeleteLimit::of(&DeleteEndpoint::S3 {
                scope: &storage.service_scope,
                bucket,
            });
            held.push(limit.hold_all().await);
            batch.extend(paths(bucket, S3_DELETE_REQUESTS * MAX_DELETE_BATCH_SIZE));
        }

        let delete = tokio::spawn(async move { storage.delete_batch(&batch).await });
        tokio::time::timeout(Duration::from_secs(5), async {
            while free_requests.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the free bucket waited for the limits of the held buckets");
        delete.abort();
    }

    /// Endpoint that holds every request until its gate has permits, and counts requests.
    struct GatedEndpoint {
        url: String,
        gate: Arc<tokio::sync::Semaphore>,
        in_flight: Arc<AtomicUsize>,
        max_in_flight: Arc<AtomicUsize>,
        requests: Arc<AtomicUsize>,
    }

    impl GatedEndpoint {
        async fn start() -> Self {
            let gate = Arc::new(tokio::sync::Semaphore::new(0));
            let in_flight = Arc::new(AtomicUsize::new(0));
            let max_in_flight = Arc::new(AtomicUsize::new(0));
            let requests = Arc::new(AtomicUsize::new(0));
            let app = axum::Router::new().fallback({
                let (gate, in_flight, max_in_flight, requests) = (
                    gate.clone(),
                    in_flight.clone(),
                    max_in_flight.clone(),
                    requests.clone(),
                );
                move || {
                    let (gate, in_flight, max_in_flight, requests) = (
                        gate.clone(),
                        in_flight.clone(),
                        max_in_flight.clone(),
                        requests.clone(),
                    );
                    async move {
                        let now = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                        max_in_flight.fetch_max(now, Ordering::SeqCst);
                        let _pass = gate.acquire().await.unwrap();
                        in_flight.fetch_sub(1, Ordering::SeqCst);
                        requests.fetch_add(1, Ordering::SeqCst);
                        xml(EMPTY_DELETE_RESULT.to_string())
                    }
                }
            });
            Self {
                url: serve(app).await,
                gate,
                in_flight,
                max_in_flight,
                requests,
            }
        }

        /// Waits until `expected` requests are held, checks that no more arrive, then lets
        /// every request through.
        async fn hold_then_release(&self, expected: usize) {
            tokio::time::timeout(Duration::from_secs(10), async {
                while self.in_flight.load(Ordering::SeqCst) < expected {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("requests reach the endpoint");
            tokio::time::sleep(Duration::from_millis(200)).await;
            assert_eq!(self.in_flight.load(Ordering::SeqCst), expected);
            self.gate.add_permits(1000);
        }
    }

    #[tokio::test]
    async fn batch_deletes_of_one_bucket_share_the_delete_limit() {
        let endpoint = GatedEndpoint::start().await;
        let first = storage_at(&endpoint.url).await;
        let second = storage_at(&endpoint.url).await;
        let paths = paths("shared", 6 * MAX_DELETE_BATCH_SIZE);
        let deletes = async {
            let (a, b) = tokio::join!(first.delete_batch(&paths), second.delete_batch(&paths));
            a.unwrap();
            b.unwrap();
        };
        tokio::join!(deletes, endpoint.hold_then_release(4));
        assert_eq!(endpoint.requests.load(Ordering::SeqCst), 12);
        assert_eq!(endpoint.max_in_flight.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn batch_deletes_of_different_buckets_have_separate_delete_limits() {
        let endpoint = GatedEndpoint::start().await;
        let storage = storage_at(&endpoint.url).await;
        let one = paths("one", 6 * MAX_DELETE_BATCH_SIZE);
        let two = paths("two", 6 * MAX_DELETE_BATCH_SIZE);
        let deletes = async {
            let (a, b) = tokio::join!(storage.delete_batch(&one), storage.delete_batch(&two));
            a.unwrap();
            b.unwrap();
        };
        tokio::join!(deletes, endpoint.hold_then_release(8));
        assert_eq!(endpoint.requests.load(Ordering::SeqCst), 12);
        assert_eq!(endpoint.max_in_flight.load(Ordering::SeqCst), 8);
    }

    #[tokio::test]
    async fn s3_services_have_separate_retry_partitions() {
        let first = storage_at("http://127.0.0.1:1").await;
        let second = storage_at("http://127.0.0.1:2").await;
        let partition = |storage: &S3Storage| {
            storage
                .client()
                .config()
                .retry_partition()
                .map(ToString::to_string)
        };
        assert_eq!(
            partition(&first).as_deref(),
            Some("lakekeeper-s3-http://127.0.0.1:1")
        );
        assert_eq!(
            partition(&second).as_deref(),
            Some("lakekeeper-s3-http://127.0.0.1:2")
        );
    }

    const ASSUME_ROLE_RESPONSE: &str = r#"<AssumeRoleResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><AssumeRoleResult><Credentials><AccessKeyId>test-access-key</AccessKeyId><SecretAccessKey>test-secret-key</SecretAccessKey><SessionToken>test-session-token</SessionToken><Expiration>2099-01-01T00:00:00Z</Expiration></Credentials><AssumedRoleUser><Arn>arn:aws:sts::000000000000:assumed-role/test-role/test-session</Arn><AssumedRoleId>TESTROLEID:test-session</AssumedRoleId></AssumedRoleUser></AssumeRoleResult></AssumeRoleResponse>"#;

    /// Storage that assumes a role through a local STS endpoint answering after `sts_delay`,
    /// and the number of STS requests.
    async fn storage_assuming_role(sts_delay: Duration) -> (S3Storage, Arc<AtomicUsize>) {
        let sts_requests = Arc::new(AtomicUsize::new(0));
        let sts = serve(axum::Router::new().fallback({
            let sts_requests = sts_requests.clone();
            move || {
                sts_requests.fetch_add(1, Ordering::SeqCst);
                async move {
                    tokio::time::sleep(sts_delay).await;
                    xml(ASSUME_ROLE_RESPONSE.to_string())
                }
            }
        }))
        .await;
        let s3 =
            serve(axum::Router::new().fallback(|| async { axum::http::StatusCode::NO_CONTENT }))
                .await;
        let credential = crate::s3::S3Auth::AccessKey(
            crate::s3::S3AccessKeyAuth::builder()
                .aws_access_key_id("access".to_string())
                .aws_secret_access_key("secret".to_string())
                .build(),
        );
        let storage = crate::s3::S3Settings::builder()
            .endpoint(Some(s3.parse().unwrap()))
            .sts_endpoint(Some(sts.parse().unwrap()))
            .assume_role_arn(Some("arn:aws:iam::000000000000:role/test-role".to_string()))
            .region("local".to_string())
            .path_style_access(Some(true))
            .build()
            .get_storage_client(Some(&credential))
            .await;
        (storage, sts_requests)
    }

    #[tokio::test]
    async fn one_storage_reuses_assumed_role_credentials() {
        let (storage, sts_requests) = storage_assuming_role(Duration::ZERO).await;
        storage.delete("s3://bucket/first").await.unwrap();
        storage.delete("s3://bucket/second").await.unwrap();
        assert_eq!(sts_requests.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_slow_role_assumption_still_succeeds() {
        // Longer than the SDK's default limit of 5 seconds for loading credentials.
        let (storage, sts_requests) = storage_assuming_role(Duration::from_secs(6)).await;
        storage.delete("s3://bucket/key").await.unwrap();
        assert_eq!(sts_requests.load(Ordering::SeqCst), 1);
    }

    // A paused clock cannot test the timeout itself: it jumps past the connect timeout
    // while the local connection is still being set up.
    #[tokio::test]
    async fn every_request_attempt_has_a_time_limit() {
        let storage = storage_at("http://127.0.0.1:1").await;
        let timeouts = storage.client().config().timeout_config().cloned();
        assert_eq!(
            timeouts.and_then(|t| t.operation_attempt_timeout()),
            Some(Duration::from_mins(2))
        );
    }

    #[test]
    fn uploads_keep_the_connect_timeout_and_connector_pool() {
        let transfer = &*crate::s3::TRANSFER_TIMEOUT_CONFIG;
        let default = &*crate::s3::TIMEOUT_CONFIG;
        assert_eq!(transfer.connect_timeout(), default.connect_timeout());
        assert_eq!(transfer.read_timeout(), default.read_timeout());
        assert_eq!(
            transfer.operation_attempt_timeout(),
            Some(Duration::from_mins(15))
        );
    }

    #[tokio::test]
    async fn a_single_delete_does_not_wait_for_the_delete_limit() {
        let endpoint =
            serve(axum::Router::new().fallback(|| async { axum::http::StatusCode::NO_CONTENT }))
                .await;
        let storage = storage_at(&endpoint).await;
        let _held = DeleteLimit::of(&DeleteEndpoint::S3 {
            scope: &storage.service_scope,
            bucket: "single-delete",
        })
        .hold_all()
        .await;

        tokio::time::timeout(
            Duration::from_secs(5),
            storage.delete("s3://single-delete/key"),
        )
        .await
        .expect("a single delete waited for the bulk-delete limit")
        .unwrap();
    }

    #[tokio::test]
    async fn the_service_scope_is_the_endpoint_origin() {
        let storage = storage_at("http://127.0.0.1:1").await;
        assert_eq!(storage.service_scope, "http://127.0.0.1:1");
    }

    #[tokio::test]
    async fn a_bucket_without_a_policy_has_none() {
        let storage = storage_answering(
            "404 Not Found",
            "<Error><Code>NoSuchBucketPolicy</Code><Message>The specified bucket does not have a bucket policy.</Message></Error>",
        )
        .await;
        assert_eq!(storage.bucket_policy("my-bucket").await.unwrap(), None);
    }

    #[tokio::test]
    async fn a_bucket_policy_is_returned_verbatim() {
        let storage = storage_answering("200 OK", r#"{"Statement":[]}"#).await;
        assert_eq!(
            storage.bucket_policy("my-bucket").await.unwrap().as_deref(),
            Some(r#"{"Statement":[]}"#)
        );
    }

    #[tokio::test]
    async fn a_refused_policy_read_is_permission_denied() {
        let storage = storage_answering(
            "403 Forbidden",
            "<Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>",
        )
        .await;
        let error = storage.bucket_policy("my-bucket").await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::PermissionDenied, "{error}");
    }
}
