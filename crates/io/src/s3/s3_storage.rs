use std::{collections::HashMap, str::FromStr, sync::atomic::AtomicU64};

use aws_sdk_s3::types::{ObjectIdentifier, ServerSideEncryption};
use bytes::Bytes;
use futures::{
    stream::{self, FuturesUnordered},
    StreamExt,
};

use crate::{
    error::{ErrorKind, InvalidLocationError, RetryableError},
    s3::{
        s3_error::{
            parse_batch_delete_error, parse_delete_error, parse_get_object_error,
            parse_list_objects_v2_error, parse_put_object_error,
        },
        S3Location,
    },
    BatchDeleteError, BatchDeleteResult, DeleteBatchFatalError, DeleteError, IOError,
    LakekeeperStorage, Location, ReadError, WriteError,
};

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
            .key(s3_key_to_str(s3_location.key()))
            .send()
            .await
            .map_err(|e| parse_delete_error(e, &s3_location))?;

        Ok(())
    }

    async fn delete_batch(
        &self,
        paths: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Result<BatchDeleteResult, DeleteBatchFatalError> {
        let s3_locations: HashMap<String, HashMap<String, String>> = group_paths_by_bucket(paths)?;
        let key_to_path_mapping = build_key_to_path_mapping(&s3_locations);
        let delete_futures = create_delete_futures(&self.client, s3_locations)?;

        process_delete_results(delete_futures, key_to_path_mapping)
            .await
            .map_err(Into::into)
    }

    async fn write(&self, path: impl AsRef<str>, bytes: Bytes) -> Result<(), WriteError> {
        let path = path.as_ref();
        let s3_location = S3Location::try_from_str(path, true)?;
        let mut put_object = self
            .client
            .put_object()
            .bucket(s3_location.bucket_name())
            .key(s3_key_to_str(s3_location.key()))
            .body(bytes.into());

        if let Some(kms_key_arn) = &self.aws_kms_key_arn {
            put_object = put_object
                .set_server_side_encryption(Some(ServerSideEncryption::AwsKms))
                .set_ssekms_key_id(Some(kms_key_arn.clone()));
        }
        put_object
            .send()
            .await
            .map_err(|e| parse_put_object_error(e, &s3_location))?;

        Ok(())
    }

    async fn read(&self, path: impl AsRef<str>) -> Result<Bytes, ReadError> {
        let path = path.as_ref();
        let s3_location = S3Location::try_from_str(path, true)?;

        let response = self
            .client
            .get_object()
            .bucket(s3_location.bucket_name())
            .key(s3_key_to_str(s3_location.key()))
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
            .prefix(s3_key_to_str(s3_location.key()));

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
                                    let scheme = base_location.url().scheme();
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
        let key = s3_key_to_str(s3_location.key());
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

/// Creates delete futures for batch operations, processing keys in batches of `MAX_DELETE_BATCH_SIZE`.
fn create_delete_futures(
    client: &aws_sdk_s3::Client,
    s3_locations: HashMap<String, HashMap<String, String>>,
) -> Result<
    FuturesUnordered<
        impl std::future::Future<
                Output = Result<
                    aws_sdk_s3::operation::delete_objects::DeleteObjectsOutput,
                    aws_sdk_s3::error::SdkError<
                        aws_sdk_s3::operation::delete_objects::DeleteObjectsError,
                    >,
                >,
            > + '_,
    >,
    InvalidLocationError,
> {
    let delete_futures = FuturesUnordered::new();

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
                        .map_err(|e| InvalidLocationError {
                            reason: format!("Could not build S3 ObjectIdentifier: {e}"),
                            location: key.0.clone(),
                        })
                })
                .collect::<Result<_, _>>()?;

            let delete_future = client
                .delete_objects()
                .bucket(&bucket)
                .delete(
                    aws_sdk_s3::types::Delete::builder()
                        .set_objects(Some(objects))
                        .build()
                        .map_err(|e| InvalidLocationError {
                            reason: format!("Could not build S3 Delete: {e}"),
                            location: format!("s3://{bucket}"),
                        })?,
                )
                .send();

            delete_futures.push(delete_future);
        }
    }

    Ok(delete_futures)
}

/// Processes delete results and handles errors as they complete.
async fn process_delete_results(
    mut delete_futures: FuturesUnordered<
        impl std::future::Future<
            Output = Result<
                aws_sdk_s3::operation::delete_objects::DeleteObjectsOutput,
                aws_sdk_s3::error::SdkError<
                    aws_sdk_s3::operation::delete_objects::DeleteObjectsError,
                >,
            >,
        >,
    >,
    key_to_path_mapping: HashMap<String, String>,
) -> Result<BatchDeleteResult, IOError> {
    let mut delete_errors = Vec::new();
    let mut sdk_errors = Vec::new();
    let mut successfully_deleted_keys = std::collections::HashSet::new();

    // Process all futures, collecting both individual delete errors and SDK-level errors
    let counter = AtomicU64::new(0);
    while let Some(result) = delete_futures.next().await {
        // Increment the counter for each processed batch
        let batch_index = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        match result {
            Ok(output) => {
                // Track successfully deleted objects
                for deleted_object in output.deleted() {
                    if let Some(key) = deleted_object.key() {
                        successfully_deleted_keys.insert(key.to_string());
                    }
                }

                // Check if there were any errors in the delete operation response
                let errors = output.errors();
                for error in errors {
                    let error_key = error.key().map(String::from);
                    let original_path = error_key
                        .as_ref()
                        .and_then(|key| key_to_path_mapping.get(key))
                        .cloned();

                    let batch_error = BatchDeleteError::new(
                        original_path,
                        error_key,
                        error.code().map(String::from),
                        error.message().map(String::from),
                    );
                    delete_errors.push(batch_error);
                }
            }
            Err(e) => {
                // Network or other SDK-level error - collect but don't return early
                // This allows other batches to continue processing. Exit only if this is the first batch.
                if batch_index == 0 {
                    return Err(parse_batch_delete_error(e));
                }
                sdk_errors.push(e);
            }
        }
    }

    // If we had SDK-level errors, we need to decide how to handle them
    // Since we can't determine which specific keys failed, we treat this as a fatal error
    if !sdk_errors.is_empty() {
        let error_messages: Vec<String> = sdk_errors
            .iter()
            .map(std::string::ToString::to_string)
            .collect();
        return Err(IOError::new_without_location(
            ErrorKind::Unexpected,
            error_messages.join("; ").to_string(),
        ));
    }

    // Convert successfully deleted keys back to original paths
    let successful_paths: Vec<String> = successfully_deleted_keys
        .into_iter()
        .filter_map(|key| key_to_path_mapping.get(&key).cloned())
        .collect();

    if delete_errors.is_empty() {
        Ok(BatchDeleteResult::AllSuccessful(successful_paths))
    } else {
        Ok(BatchDeleteResult::PartialFailure {
            successful_paths,
            errors: delete_errors,
        })
    }
}

fn s3_key_to_str(key: &[String]) -> String {
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
        assert_eq!(s3_key_to_str(&["a".to_string()]), "a");
        assert_eq!(s3_key_to_str(&["a".to_string(), "b".to_string()]), "a/b");
        assert_eq!(s3_key_to_str(&["a".to_string(), String::new()]), "a/");
    }
}
