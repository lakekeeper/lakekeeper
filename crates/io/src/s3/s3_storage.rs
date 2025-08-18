use std::{collections::HashMap, str::FromStr};

use aws_sdk_s3::types::{ObjectIdentifier, ServerSideEncryption};
use bytes::Bytes;
use futures::{stream, StreamExt};

use crate::{
    error::{ErrorKind, InvalidLocationError, RetryableError},
    execute_with_parallelism,
    s3::{
        s3_error::{
            parse_aws_sdk_error, parse_batch_delete_error, parse_complete_multipart_upload_error,
            parse_create_multipart_upload_error, parse_delete_error, parse_get_object_error,
            parse_head_object_error, parse_list_objects_v2_error, parse_put_object_error,
            parse_upload_part_error,
        },
        S3Location,
    },
    safe_usize_to_i32, validate_file_size, DeleteBatchError, DeleteError, IOError,
    LakekeeperStorage, Location, ReadError, WriteError,
};

// Convert MB constants to bytes - these will always be safe conversions from u16
const MAX_BYTES_PER_REQUEST: usize = 25 * 1024 * 1024;
const DEFAULT_BYTES_PER_REQUEST: usize = 16 * 1024 * 1024;
const MAX_PARTS_PER_UPLOAD: usize = 10_000; // S3 limit for multipart uploads
const MAX_DELETE_BATCH_SIZE: usize = 1000;

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

impl LakekeeperStorage for S3Storage {
    async fn delete(&self, path: impl AsRef<str>) -> Result<(), DeleteError> {
        let path = path.as_ref();
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

    async fn delete_batch(
        &self,
        paths: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Result<(), DeleteBatchError> {
        let s3_locations: HashMap<String, HashMap<String, String>> = group_paths_by_bucket(paths)?;
        let key_to_path_mapping = build_key_to_path_mapping(&s3_locations);
        let delete_futures = create_delete_futures(&self.client, s3_locations)?;

        process_delete_results(delete_futures, key_to_path_mapping)
            .await
            .map_err(Into::into)
    }

    #[allow(clippy::too_many_lines)]
    async fn write(&self, path: impl AsRef<str>, bytes: Bytes) -> Result<(), WriteError> {
        let path = path.as_ref();
        let s3_location = S3Location::try_from_str(path, true)?;

        if bytes.len() < MAX_BYTES_PER_REQUEST {
            // Small file - use single PUT request
            let mut put_object = self
                .client
                .put_object()
                .bucket(s3_location.bucket_name())
                .key(s3_key_to_str(&s3_location.key()))
                .body(bytes.into());

            if let Some(kms_key_arn) = &self.aws_kms_key_arn {
                put_object = put_object
                    .set_server_side_encryption(Some(ServerSideEncryption::AwsKms))
                    .set_ssekms_key_id(Some(kms_key_arn.clone()));
            }
            put_object.send().await.map_err(|e| {
                WriteError::IOError(parse_put_object_error(e, s3_location.as_str()))
            })?;

            Ok(())
        } else {
            // Large file - use multipart upload
            let file_size = bytes.len();

            // Calculate optimal chunk size to respect MAX_PARTS_PER_UPLOAD constraint
            let mut chunk_size = DEFAULT_BYTES_PER_REQUEST;
            let estimated_parts = file_size.div_ceil(chunk_size);

            if estimated_parts > MAX_PARTS_PER_UPLOAD {
                // Increase chunk size to stay within MAX_PARTS_PER_UPLOAD
                chunk_size = file_size.div_ceil(MAX_PARTS_PER_UPLOAD);
            }

            // Create multipart upload
            let mut create_multipart = self
                .client
                .create_multipart_upload()
                .bucket(s3_location.bucket_name())
                .key(s3_key_to_str(&s3_location.key()));

            if let Some(kms_key_arn) = &self.aws_kms_key_arn {
                create_multipart = create_multipart
                    .set_server_side_encryption(Some(ServerSideEncryption::AwsKms))
                    .set_ssekms_key_id(Some(kms_key_arn.clone()));
            }

            let multipart_response = create_multipart.send().await.map_err(|e| {
                WriteError::IOError(
                    parse_create_multipart_upload_error(e, s3_location.as_str())
                        .with_context("Failed to create multipart upload."),
                )
            })?;

            let upload_id = multipart_response.upload_id().ok_or_else(|| {
                WriteError::IOError(IOError::new(
                    ErrorKind::Unexpected,
                    "S3 multipart upload response missing upload_id".to_string(),
                    s3_location.as_str().to_string(),
                ))
            })?;

            // Create chunks and upload them in parallel
            let chunks: Vec<_> = bytes
                .chunks(chunk_size)
                .enumerate()
                .map(|(i, chunk)| (i + 1, Bytes::copy_from_slice(chunk))) // S3 part numbers start at 1
                .collect();

            // Create upload futures
            let upload_futures = chunks.into_iter().map(|(part_number, chunk_data)| {
                let client = self.client.clone();
                let s3_location = s3_location.clone();
                let upload_id = upload_id.to_string();

                async move {
                    let part_number_i32 = safe_usize_to_i32(part_number, s3_location.as_str())
                        .map_err(|e| e.with_context("Too many parts to write"))?;
                    let upload_part_response = client
                        .upload_part()
                        .bucket(s3_location.bucket_name())
                        .key(s3_key_to_str(&s3_location.key()))
                        .upload_id(&upload_id)
                        .part_number(part_number_i32)
                        .body(chunk_data.into())
                        .send()
                        .await
                        .map_err(|e| {
                            WriteError::IOError(
                                parse_upload_part_error(e, s3_location.as_str())
                                    .with_context(format!("Failed to upload part {part_number}")),
                            )
                        })?;

                    let etag = upload_part_response.e_tag().ok_or_else(|| {
                        WriteError::IOError(IOError::new(
                            ErrorKind::Unexpected,
                            format!("S3 upload part response missing ETag for part {part_number}"),
                            s3_location.as_str().to_string(),
                        ))
                    })?;

                    let completed_part = aws_sdk_s3::types::CompletedPart::builder()
                        .part_number(part_number_i32)
                        .e_tag(etag)
                        .build();

                    Ok::<(i32, aws_sdk_s3::types::CompletedPart), WriteError>((
                        part_number_i32,
                        completed_part,
                    ))
                }
            });

            // Execute uploads with parallelism limit of 10
            let upload_results = execute_with_parallelism(upload_futures, 10);
            tokio::pin!(upload_results);

            // Collect all completed parts
            let mut completed_parts = Vec::new();
            while let Some(result) = upload_results.next().await {
                let join_result = result.map_err(|e| {
                    WriteError::IOError(IOError::new(
                        ErrorKind::Unexpected,
                        format!("Upload task panicked: {e}"),
                        s3_location.as_str().to_string(),
                    ))
                })?;
                let (part_number, completed_part) = join_result?;
                completed_parts.push((part_number, completed_part));
            }

            // Sort parts by part number to ensure correct order
            completed_parts.sort_by_key(|(part_number, _)| *part_number);
            let completed_parts: Vec<_> =
                completed_parts.into_iter().map(|(_, part)| part).collect();

            // Complete the multipart upload
            let completed_multipart_upload = aws_sdk_s3::types::CompletedMultipartUpload::builder()
                .set_parts(Some(completed_parts))
                .build();

            self.client
                .complete_multipart_upload()
                .bucket(s3_location.bucket_name())
                .key(s3_key_to_str(&s3_location.key()))
                .upload_id(upload_id)
                .multipart_upload(completed_multipart_upload)
                .send()
                .await
                .map_err(|e| {
                    WriteError::IOError(
                        parse_complete_multipart_upload_error(e, s3_location.as_str())
                            .with_context("Failed to complete multipart upload."),
                    )
                })?;

            Ok(())
        }
    }

    async fn read(&self, path: impl AsRef<str>) -> Result<Bytes, ReadError> {
        let path = path.as_ref();
        let s3_location = S3Location::try_from_str(path, true)?;

        // First, get object metadata to determine size
        let head_response = self
            .client
            .head_object()
            .bucket(s3_location.bucket_name())
            .key(s3_key_to_str(&s3_location.key()))
            .send()
            .await
            .map_err(|e| parse_head_object_error(e, &s3_location))?;

        let content_length = head_response.content_length().unwrap_or(0);
        let file_size = validate_file_size(content_length, path)?;

        if file_size < MAX_BYTES_PER_REQUEST {
            return self.read_single(path).await;
        }

        // For large files, use parallel chunk downloads
        let chunks = crate::calculate_ranges(file_size, DEFAULT_BYTES_PER_REQUEST);

        let download_futures = chunks.into_iter().enumerate().map(|(chunk_index, (start, end))| {
            let client = self.client.clone();
            let s3_location = s3_location.clone();
            let path = path.to_string();

            async move {
                let range_header = format!("bytes={start}-{end}");
                let response = client
                    .get_object()
                    .bucket(s3_location.bucket_name())
                    .key(s3_key_to_str(&s3_location.key()))
                    .range(range_header)
                    .send()
                    .await
                    .map_err(|e| parse_get_object_error(e, &s3_location))?;

                let chunk_data = response.body.collect().await.map_err(|e| {
                    ReadError::IOError(IOError::new(
                        ErrorKind::Unexpected,
                        format!("Error collecting S3 chunk {chunk_index} bytestream (bytes {start}-{end}): {e}"),
                        path.clone(),
                    ).set_source(anyhow::anyhow!(e)))
                })?;

                Ok::<(usize, Bytes), ReadError>((chunk_index, chunk_data.into_bytes()))
            }
        });

        // Execute downloads with parallelism limit of 10
        let download_results = execute_with_parallelism(download_futures, 10);
        tokio::pin!(download_results);

        // Transform the stream to handle the nested Result and convert JoinError to ReadError
        let flattened_results = download_results.map(|result| {
            result
                .map_err(|join_error| {
                    ReadError::IOError(IOError::new(
                        ErrorKind::Unexpected,
                        format!("Download task panicked: {join_error}"),
                        path.to_string(),
                    ))
                })
                .and_then(|inner| inner)
        });

        // Use the shared utility function to assemble chunks
        let combined_data =
            crate::assemble_chunks(flattened_results, file_size, DEFAULT_BYTES_PER_REQUEST).await?;

        Ok(combined_data)
    }

    async fn read_single(&self, path: impl AsRef<str>) -> Result<Bytes, ReadError> {
        let path = path.as_ref();
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
                s3_location.as_str().to_string(),
            )
            .set_source(anyhow::anyhow!(e))
        })?;

        Ok(body.into_bytes())
    }

    async fn list(
        &self,
        path: impl AsRef<str> + Send,
        page_size: Option<usize>,
    ) -> Result<futures::stream::BoxStream<'_, Result<Vec<Location>, IOError>>, InvalidLocationError>
    {
        let path = format!("{}/", path.as_ref().trim_end_matches('/'));
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
                            let locations = response
                                .contents()
                                .iter()
                                .filter_map(|o| o.key())
                                .map(|key| {
                                    // Create a new location directly using the bucket and key
                                    // to avoid duplicate path components - use the same scheme as the base location
                                    let scheme = base_location.scheme();
                                    let full_path = format!("{scheme}://{s3_bucket}/{key}");
                                    Location::from_str(&full_path)
                                        .unwrap_or_else(|_| base_location.clone())
                                })
                                .collect::<Vec<_>>();

                            let next_continuation_token = response
                                .next_continuation_token()
                                .map(std::string::ToString::to_string);
                            let is_truncated = response.is_truncated().unwrap_or(false);
                            let next_state = (next_continuation_token, !is_truncated);

                            Some((Ok(locations), next_state))
                        }
                        // First case: Retryable error occurred but retries didn't resolve it
                        // Second case: Non-retryable error occured
                        Ok(Err(error)) | Err(error) => Some((Err(error), (None, true))),
                    }
                }
            },
        );

        Ok(stream.boxed())
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
            IOError::new(
                ErrorKind::Unexpected,
                format!("Delete task panicked: {e}"),
                "S3 batch delete".to_string(),
            )
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
