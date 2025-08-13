#![warn(
    missing_debug_implementations,
    rust_2018_idioms,
    unreachable_pub,
    clippy::pedantic
)]
#![allow(clippy::module_name_repetitions, clippy::large_enum_variant)]
#![forbid(unsafe_code)]

use std::{fmt::Display, future::Future, time::Duration};

mod error;
use bytes::Bytes;
pub use error::{
    BatchDeleteError, DeleteBatchFatalError, DeleteError, ErrorKind, IOError,
    InitializeClientError, InvalidLocationError, ReadError, RetryableError, RetryableErrorKind,
    WriteError,
};
use futures::{
    stream::{BoxStream, FuturesUnordered},
    StreamExt as _,
};
pub use location::Location;
use tokio::task::JoinHandle;
pub use tryhard;
use tryhard::{backoff_strategies::BackoffStrategy, RetryPolicy};

#[cfg(feature = "storage-adls")]
pub mod adls;
#[cfg(feature = "storage-gcs")]
pub mod gcs;
mod location;
#[cfg(feature = "storage-in-memory")]
pub mod memory;
#[cfg(feature = "storage-s3")]
pub mod s3;

pub(crate) fn safe_usize_to_i32(value: usize, context: &str) -> Result<i32, IOError> {
    i32::try_from(value).map_err(|_| {
        IOError::new(
            ErrorKind::ConditionNotMatch,
            format!("Platform usize too large for i32: {value}"),
            context.to_string(),
        )
    })
}

pub(crate) fn safe_usize_to_i64(value: usize, context: &str) -> Result<i64, IOError> {
    i64::try_from(value).map_err(|_| {
        IOError::new(
            ErrorKind::ConditionNotMatch,
            format!("File too large - Platform usize too large for i64: {value}"),
            context.to_string(),
        )
    })
}

pub(crate) fn validate_file_size(size: i64, location: &str) -> Result<usize, IOError> {
    if size < 0 {
        return Err(IOError::new(
            ErrorKind::ConditionNotMatch,
            "File size cannot be negative".to_string(),
            location.to_string(),
        ));
    }

    match usize::try_from(size) {
        Ok(size) => Ok(size),
        Err(_) => Err(IOError::new(
            ErrorKind::ConditionNotMatch,
            format!("File size too large for this platform: {size}"),
            location.to_string(),
        )),
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, strum_macros::Display)]
pub enum OperationType {
    Delete,
    DeleteBatch,
    Read,
    Write,
    List,
}

// #[derive(Debug, Clone)]
// pub struct RefreshingLakekeeperIo<C: StorageCredentialProvider, I: LakekeeperStorage> {
//     credential_provider: Arc<C>,
//     lakekeeper_io: Arc<RwLock<(Arc<I>, std::time::Instant)>>,
//     inner: Arc<RefreshingIo>,
// }

// #[derive(Debug, Clone)]
// struct RefreshingIo {}

#[derive(Debug, Clone, derive_more::From)]
pub enum StorageBackend {
    #[cfg(feature = "storage-s3")]
    S3(crate::s3::S3Storage),
    #[cfg(feature = "storage-in-memory")]
    Memory(crate::memory::MemoryStorage),
    #[cfg(feature = "storage-adls")]
    Adls(crate::adls::AdlsStorage),
    #[cfg(feature = "storage-gcs")]
    Gcs(crate::gcs::GcsStorage),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RetryConfig<B, E>
where
    for<'a> B: BackoffStrategy<'a, E>,
    for<'a> <B as BackoffStrategy<'a, E>>::Output: Into<RetryPolicy>,
    B: Send,
{
    retries: u32,
    backoff_strategy: B,
    max_delay: Option<Duration>,
    phantom: std::marker::PhantomData<E>,
}

impl<B, E> RetryConfig<B, E>
where
    for<'a> B: BackoffStrategy<'a, E>,
    for<'a> <B as BackoffStrategy<'a, E>>::Output: Into<RetryPolicy>,
    B: Send + Clone,
{
    pub fn new(retries: u32, backoff_strategy: B) -> Self {
        Self {
            retries,
            backoff_strategy,
            max_delay: None,
            phantom: std::marker::PhantomData,
        }
    }

    #[must_use]
    pub fn with_max_delay(mut self, max_delay: Duration) -> Self {
        self.max_delay = Some(max_delay);
        self
    }

    pub fn retries(&self) -> u32 {
        self.retries
    }

    pub fn backoff_strategy(&self) -> B {
        self.backoff_strategy.clone()
    }

    pub fn max_delay(&self) -> Option<Duration> {
        self.max_delay
    }
}

pub trait LakekeeperStorage
where
    Self: std::fmt::Debug + Clone + Send + Sync + 'static,
{
    /// Deletes file.
    ///
    /// # Arguments
    ///
    /// * path: It should be *absolute* path starting with scheme string
    fn delete(
        &self,
        path: impl AsRef<str> + Send,
    ) -> impl Future<Output = Result<(), DeleteError>> + Send;

    /// Deletes files in batch.
    ///
    /// # Arguments
    ///
    /// * paths: An iterator of paths to delete, each path should be an absolute path starting with scheme string.
    ///
    /// # Returns
    /// * A future that resolves to a result containing either:
    ///   - `Ok(BatchDeleteResult::AllSuccessful(paths))` if all deletions were successful.
    ///   - `Ok(BatchDeleteResult::PartialFailure { successful_paths, errors })` if some deletions failed.
    ///   - `Err(error)` if the entire batch operation failed (e.g., network error, invalid configuration).
    ///
    /// The `BatchDeleteResult` type forces callers to explicitly handle partial failures,
    /// making it impossible to accidentally ignore errors.
    fn delete_batch(
        &self,
        paths: impl IntoIterator<Item = impl AsRef<str>> + Send,
    ) -> impl Future<Output = Result<BatchDeleteResult, DeleteBatchFatalError>> + Send;

    /// Write the provided data to the specified path.
    fn write(
        &self,
        path: impl AsRef<str> + Send,
        bytes: Bytes,
    ) -> impl Future<Output = Result<(), WriteError>> + Send;

    /// Read a file from the specified path.
    ///
    /// # Arguments
    /// path: It should be an absolute path starting with scheme string.
    fn read(
        &self,
        path: impl AsRef<str> + Send,
    ) -> impl Future<Output = Result<Bytes, ReadError>> + Send;

    /// Read a file from the specified path.
    ///
    /// # Arguments
    /// path: It should be an absolute path starting with scheme string.
    fn read_single(
        &self,
        path: impl AsRef<str> + Send,
    ) -> impl Future<Output = Result<Bytes, ReadError>> + Send;

    /// List files for this prefix.
    /// If the provided location does not end with a slash, the slash will be added automatically.
    /// Retries are handled internally.
    fn list(
        &self,
        path: impl AsRef<str> + Send,
        page_size: Option<usize>,
    ) -> impl Future<
        Output = Result<BoxStream<'_, Result<Vec<Location>, IOError>>, InvalidLocationError>,
    > + Send;

    /// Removes a directory and all its contents.
    /// If the directory doesn't end with a slash, the slash is added automatically.
    fn remove_all(
        &self,
        path: impl AsRef<str> + Send,
    ) -> impl Future<Output = Result<(), DeleteError>> + Send {
        async move {
            let path = path.as_ref();
            let mut delete_futures = FuturesUnordered::new();

            // Use the existing list function to get all objects
            let mut list_stream = self.list(path, None).await?;
            let mut list_failed = Ok(());

            // Process each batch as it arrives from the stream
            while let Some(locations_result) = list_stream.next().await {
                let locations = match locations_result {
                    Ok(locations) => locations,
                    Err(e) => {
                        list_failed = Err(e);
                        break;
                    }
                };

                // Skip empty pages
                if locations.is_empty() {
                    continue;
                }

                // Store the future but don't await yet - allows parallel execution
                let storage = self.clone();
                let delete_future =
                    tokio::spawn(async move { storage.delete_batch(locations).await });
                delete_futures.push(delete_future);
            }

            if let Err(e) = list_failed {
                // Abort non-finished futures if listing failed
                abort_unfinished_batch_delete_futures(delete_futures).await;
                // Return the error from listing
                return Err(e.into());
            }

            // If no objects found, we're done
            if delete_futures.is_empty() {
                return Ok(());
            }

            // Wait for all deletion futures to complete, collecting errors
            let mut return_error = None;

            while let Some(result) = delete_futures.next().await {
                match result {
                    Ok(Ok(BatchDeleteResult::AllSuccessful())) => {}
                    Ok(Ok(BatchDeleteResult::PartialFailure { errors, .. })) => {
                        // Keep track of the first error we encounter
                        if return_error.is_none() && !errors.is_empty() {
                            if let Some(error) = errors.first() {
                                return_error = Some(DeleteError::IOError(IOError::new(
                                    ErrorKind::Unexpected,
                                    format!("Batch delete failed: {error}"),
                                    path.to_string(),
                                )));
                            }
                        }
                    }
                    Ok(Err(e)) => {
                        // Fatal error supersedes PartialErrors
                        return_error = match e {
                            DeleteBatchFatalError::InvalidLocation(e) => Some(e.into()),
                            DeleteBatchFatalError::IOError(e) => Some(e.into()),
                        }
                    }
                    Err(e) => {
                        return_error = Some(DeleteError::IOError(IOError::new(
                            ErrorKind::Unexpected,
                            format!("Failed to join batch deletion task handle: {e}"),
                            path.to_string(),
                        )));
                    }
                }
            }

            // Return the first error if any occurred
            match return_error {
                Some(e) => Err(e),
                None => Ok(()),
            }
        }
    }
}

impl LakekeeperStorage for StorageBackend {
    fn delete(
        &self,
        path: impl AsRef<str> + Send,
    ) -> impl Future<Output = Result<(), DeleteError>> + Send {
        let path = path.as_ref().to_string();
        let storage = self.clone();
        async move {
            match storage {
                #[cfg(feature = "storage-s3")]
                StorageBackend::S3(s3_storage) => s3_storage.delete(path).await,
                #[cfg(feature = "storage-in-memory")]
                StorageBackend::Memory(memory_storage) => memory_storage.delete(path).await,
                #[cfg(feature = "storage-adls")]
                StorageBackend::Adls(adls_storage) => adls_storage.delete(path).await,
                #[cfg(feature = "storage-gcs")]
                StorageBackend::Gcs(gcs_storage) => gcs_storage.delete(path).await,
            }
        }
    }

    fn delete_batch(
        &self,
        paths: impl IntoIterator<Item = impl AsRef<str>> + Send,
    ) -> impl Future<Output = Result<BatchDeleteResult, DeleteBatchFatalError>> + Send {
        let paths: Vec<String> = paths.into_iter().map(|p| p.as_ref().to_string()).collect();
        let storage = self.clone();
        async move {
            match storage {
                #[cfg(feature = "storage-s3")]
                StorageBackend::S3(s3_storage) => s3_storage.delete_batch(paths).await,
                #[cfg(feature = "storage-in-memory")]
                StorageBackend::Memory(memory_storage) => memory_storage.delete_batch(paths).await,
                #[cfg(feature = "storage-adls")]
                StorageBackend::Adls(adls_storage) => adls_storage.delete_batch(paths).await,
                #[cfg(feature = "storage-gcs")]
                StorageBackend::Gcs(gcs_storage) => gcs_storage.delete_batch(paths).await,
            }
        }
    }

    fn write(
        &self,
        path: impl AsRef<str> + Send,
        bytes: Bytes,
    ) -> impl Future<Output = Result<(), WriteError>> + Send {
        let path = path.as_ref().to_string();
        let storage = self.clone();
        async move {
            match storage {
                #[cfg(feature = "storage-s3")]
                StorageBackend::S3(s3_storage) => s3_storage.write(path, bytes).await,
                #[cfg(feature = "storage-in-memory")]
                StorageBackend::Memory(memory_storage) => memory_storage.write(path, bytes).await,
                #[cfg(feature = "storage-adls")]
                StorageBackend::Adls(adls_storage) => adls_storage.write(path, bytes).await,
                #[cfg(feature = "storage-gcs")]
                StorageBackend::Gcs(gcs_storage) => gcs_storage.write(path, bytes).await,
            }
        }
    }

    fn read(
        &self,
        path: impl AsRef<str> + Send,
    ) -> impl Future<Output = Result<Bytes, ReadError>> + Send {
        let path = path.as_ref().to_string();
        let storage = self.clone();
        async move {
            match storage {
                #[cfg(feature = "storage-s3")]
                StorageBackend::S3(s3_storage) => s3_storage.read(path).await,
                #[cfg(feature = "storage-in-memory")]
                StorageBackend::Memory(memory_storage) => memory_storage.read(path).await,
                #[cfg(feature = "storage-adls")]
                StorageBackend::Adls(adls_storage) => adls_storage.read(path).await,
                #[cfg(feature = "storage-gcs")]
                StorageBackend::Gcs(gcs_storage) => gcs_storage.read(path).await,
            }
        }
    }

    fn read_single(
        &self,
        path: impl AsRef<str> + Send,
    ) -> impl Future<Output = Result<Bytes, ReadError>> + Send {
        let path = path.as_ref().to_string();
        let storage = self.clone();
        async move {
            match storage {
                #[cfg(feature = "storage-s3")]
                StorageBackend::S3(s3_storage) => s3_storage.read_single(path).await,
                #[cfg(feature = "storage-in-memory")]
                StorageBackend::Memory(memory_storage) => memory_storage.read_single(path).await,
                #[cfg(feature = "storage-adls")]
                StorageBackend::Adls(adls_storage) => adls_storage.read_single(path).await,
                #[cfg(feature = "storage-gcs")]
                StorageBackend::Gcs(gcs_storage) => gcs_storage.read_single(path).await,
            }
        }
    }

    fn list(
        &self,
        path: impl AsRef<str> + Send,
        page_size: Option<usize>,
    ) -> impl Future<
        Output = Result<BoxStream<'_, Result<Vec<Location>, IOError>>, InvalidLocationError>,
    > + Send {
        let path = path.as_ref().to_string();
        async move {
            match self {
                #[cfg(feature = "storage-s3")]
                StorageBackend::S3(s3_storage) => s3_storage.list(path, page_size).await,
                #[cfg(feature = "storage-in-memory")]
                StorageBackend::Memory(memory_storage) => {
                    memory_storage.list(path, page_size).await
                }
                #[cfg(feature = "storage-adls")]
                StorageBackend::Adls(adls_storage) => adls_storage.list(path, page_size).await,
                #[cfg(feature = "storage-gcs")]
                StorageBackend::Gcs(gcs_storage) => gcs_storage.list(path, page_size).await,
            }
        }
    }

    fn remove_all(
        &self,
        path: impl AsRef<str> + Send,
    ) -> impl Future<Output = Result<(), DeleteError>> + Send {
        let path = path.as_ref().to_string();
        async move {
            match self {
                #[cfg(feature = "storage-s3")]
                StorageBackend::S3(s3_storage) => s3_storage.remove_all(path).await,
                #[cfg(feature = "storage-in-memory")]
                StorageBackend::Memory(memory_storage) => memory_storage.remove_all(path).await,
                #[cfg(feature = "storage-adls")]
                StorageBackend::Adls(adls_storage) => adls_storage.remove_all(path).await,
                #[cfg(feature = "storage-gcs")]
                StorageBackend::Gcs(gcs_storage) => gcs_storage.remove_all(path).await,
            }
        }
    }
}

/// Result of a batch delete operation.
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use = "this `BatchDeleteResult` may be an `PartialFailure` variant, which should be handled"]
pub enum BatchDeleteResult {
    /// All deletions were successful.
    /// Contains the list of successfully deleted paths.
    AllSuccessful(),
    /// Some deletions failed, but some succeeded.
    /// Contains both successful paths and detailed error information.
    /// This variant forces callers to handle partial failures explicitly.
    PartialFailure {
        successful_paths: Vec<String>,
        errors: Vec<BatchDeleteError>,
    },
}

impl BatchDeleteResult {
    /// Returns true if all deletions were successful.
    #[must_use]
    pub fn is_all_successful(&self) -> bool {
        matches!(self, BatchDeleteResult::AllSuccessful())
    }

    /// Returns true if any deletions failed.
    #[must_use]
    pub fn has_failures(&self) -> bool {
        matches!(self, BatchDeleteResult::PartialFailure { .. })
    }

    /// Returns the list of errors, if any.
    #[must_use]
    pub fn errors(&self) -> Option<&[BatchDeleteError]> {
        match self {
            BatchDeleteResult::AllSuccessful() => None,
            BatchDeleteResult::PartialFailure { errors, .. } => Some(errors),
        }
    }

    /// Returns the number of errors, if any.
    #[must_use]
    pub fn error_count(&self) -> usize {
        self.errors().map_or(0, <[error::BatchDeleteError]>::len)
    }
}

async fn abort_unfinished_batch_delete_futures<R, E>(
    delete_futures: FuturesUnordered<JoinHandle<Result<R, E>>>,
) where
    R: Send,
    E: Send + Display + std::error::Error,
{
    for future in &delete_futures {
        if !future.is_finished() {
            future.abort();
        }
    }

    // Await all futures, log any errors except Join Errors where `is_canceled` is true
    for future in delete_futures {
        match future.await {
            Ok(Ok(_)) => {}
            Err(e) => {
                if !e.is_cancelled() {
                    tracing::debug!("Unexpected error while awaiting batch deletion future of an aborted task: {e}");
                }
            }
            Ok(Err(e)) => {
                tracing::debug!("Batch deletion future of an aborted task failed: {e}");
            }
        }
    }
}

/// Helper function to calculate the ranges for reading the object in chunks.
/// Returns start and end (inclusive) indices for each chunk.
#[cfg(any(
    feature = "storage-s3",
    feature = "storage-gcs",
    feature = "storage-adls"
))]
pub(crate) fn calculate_ranges(total_size: usize, chunksize: usize) -> Vec<(usize, usize)> {
    (0..total_size)
        .step_by(chunksize)
        .map(|start| {
            let end = std::cmp::min(start + chunksize - 1, total_size - 1);
            (start, end)
        })
        .collect()
}

/// Helper function to assemble downloaded chunks into a final buffer.
/// Takes a stream of results containing `(chunk_index, chunk_data)` pairs and assembles them
/// into a pre-allocated buffer of the specified total size.
#[cfg(any(
    feature = "storage-s3",
    feature = "storage-gcs",
    feature = "storage-adls"
))]
pub(crate) async fn assemble_chunks<S, E>(
    mut chunk_stream: S,
    total_size: usize,
    chunk_size: usize,
) -> Result<bytes::Bytes, E>
where
    S: futures::StreamExt<Item = Result<(usize, bytes::Bytes), E>> + Unpin,
{
    // Pre-allocate buffer with exact size
    let mut combined_data = vec![0u8; total_size];

    while let Some(result) = chunk_stream.next().await {
        let (chunk_index, chunk_data) = result?;

        // Calculate the offset for this chunk
        let offset = chunk_index * chunk_size;
        let end_offset = std::cmp::min(offset + chunk_data.len(), combined_data.len());

        // Write directly to the pre-allocated buffer
        combined_data[offset..end_offset].copy_from_slice(&chunk_data);
    }

    let bytes = bytes::Bytes::from(combined_data);
    Ok(bytes)
}

#[cfg(any(feature = "storage-gcs", feature = "storage-adls"))]
pub(crate) fn delete_not_found_is_ok(result: Result<(), IOError>) -> Result<(), IOError> {
    match result {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_ranges() {
        let result = calculate_ranges(10, 4);
        assert_eq!(result, vec![(0, 3), (4, 7), (8, 9)]);

        let result = calculate_ranges(3, 4);
        assert_eq!(result, vec![(0, 2)]);

        // Edge cases
        let result = calculate_ranges(0, 4);
        assert_eq!(result, vec![]);

        let result = calculate_ranges(1, 4);
        assert_eq!(result, vec![(0, 0)]);

        // Exact chunk size boundary
        let result = calculate_ranges(8, 4);
        assert_eq!(result, vec![(0, 3), (4, 7)]);

        // Single byte file
        let result = calculate_ranges(1, 1000);
        assert_eq!(result, vec![(0, 0)]);

        // File size exactly equal to chunk size
        let result = calculate_ranges(10, 10);
        assert_eq!(result, vec![(0, 10 - 1)]);

        // Very small chunk size
        let result = calculate_ranges(5, 1);
        assert_eq!(result, vec![(0, 0), (1, 1), (2, 2), (3, 3), (4, 4)]);
    }
}
