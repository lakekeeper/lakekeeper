#![warn(
    missing_debug_implementations,
    rust_2018_idioms,
    unreachable_pub,
    clippy::pedantic
)]
#![allow(clippy::module_name_repetitions, clippy::large_enum_variant)]
#![forbid(unsafe_code)]

use std::{future::Future, sync::Arc, time::Duration};

mod error;
use bytes::Bytes;
pub use error::{
    DeleteBatchError, DeleteError, ErrorKind, IOError, InitializeClientError, InternalError,
    InvalidLocationError, ReadError, RetryableError, RetryableErrorKind, WriteError,
};
use futures::{stream::BoxStream, StreamExt as _};
pub use location::Location;
use tokio::task::JoinSet;
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

#[cfg(any(feature = "storage-s3", feature = "storage-gcs"))]
/// Fallible usize→i32 conversion with additional context for diagnostics.
pub(crate) fn safe_usize_to_i32(value: usize, context: impl Into<String>) -> Result<i32, IOError> {
    i32::try_from(value).map_err(|_| {
        IOError::new(
            ErrorKind::ConditionNotMatch,
            format!("value {value} does not fit into i32"),
            context.into(),
        )
    })
}

#[cfg(any(feature = "storage-adls", feature = "storage-gcs"))]
/// Fallible usize→i64 conversion with contextual diagnostics.
pub(crate) fn safe_usize_to_i64(value: usize, context: impl Into<String>) -> Result<i64, IOError> {
    i64::try_from(value).map_err(|_| {
        IOError::new(
            ErrorKind::ConditionNotMatch, // consider a more specific kind if available
            format!("value {value} does not fit into i64"),
            context.into(),
        )
    })
}

#[cfg(any(
    feature = "storage-adls",
    feature = "storage-gcs",
    feature = "storage-s3"
))]
/// Validate a remote-reported file size (i64), rejecting negative sizes and sizes
/// that do not fit into `usize` on the current platform. The `location` is used
/// for error diagnostics.
pub(crate) fn validate_file_size(size: i64, location: impl Into<String>) -> Result<usize, IOError> {
    if size < 0 {
        return Err(IOError::new(
            ErrorKind::ConditionNotMatch,
            "File size cannot be negative".to_string(),
            location.into(),
        ));
    }

    match usize::try_from(size) {
        Ok(size) => Ok(size),
        Err(_) => Err(IOError::new(
            ErrorKind::ConditionNotMatch,
            format!("File size too large for this platform: {size}"),
            location.into(),
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
    ///   - `Ok(())` if all deletions were successful.
    ///   - `Err(DeleteBatchError)` if any deletion failed.
    fn delete_batch(
        &self,
        paths: impl IntoIterator<Item = impl AsRef<str>> + Send,
    ) -> impl Future<Output = Result<(), DeleteBatchError>> + Send;

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
            let mut join_set = JoinSet::new();

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
                join_set.spawn(async move { storage.delete_batch(locations).await });
            }

            if let Err(e) = list_failed {
                // Abort non-finished futures if listing failed
                abort_unfinished_batch_delete_futures(&mut join_set).await;
                // Return the error from listing
                return Err(e.into());
            }

            // If no objects found, we're done
            if join_set.is_empty() {
                return Ok(());
            }

            // Wait for all deletion futures to complete, collecting errors
            let mut return_error = None;

            while let Some(result) = join_set.join_next().await {
                match result {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        // Fatal error supersedes PartialErrors
                        return_error = match e {
                            DeleteBatchError::InvalidLocation(e) => Some(e.into()),
                            DeleteBatchError::IOError(e) => Some(e.into()),
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
    ) -> impl Future<Output = Result<(), DeleteBatchError>> + Send {
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

// Macro to generate LakekeeperStorage implementations for smart pointer types
macro_rules! impl_lakekeeper_storage_for_smart_pointer {
    ($($wrapper:ty),+ $(,)?) => {
        $(
            impl<T> LakekeeperStorage for $wrapper
            where
                T: LakekeeperStorage,
            {
                fn delete(
                    &self,
                    path: impl AsRef<str> + Send,
                ) -> impl Future<Output = Result<(), DeleteError>> + Send {
                    self.as_ref().delete(path)
                }

                fn delete_batch(
                    &self,
                    paths: impl IntoIterator<Item = impl AsRef<str>> + Send,
                ) -> impl Future<Output = Result<(), DeleteBatchError>> + Send {
                    self.as_ref().delete_batch(paths)
                }

                fn write(
                    &self,
                    path: impl AsRef<str> + Send,
                    bytes: Bytes,
                ) -> impl Future<Output = Result<(), WriteError>> + Send {
                    self.as_ref().write(path, bytes)
                }

                fn read(
                    &self,
                    path: impl AsRef<str> + Send,
                ) -> impl Future<Output = Result<Bytes, ReadError>> + Send {
                    self.as_ref().read(path)
                }

                fn read_single(
                    &self,
                    path: impl AsRef<str> + Send,
                ) -> impl Future<Output = Result<Bytes, ReadError>> + Send {
                    self.as_ref().read_single(path)
                }

                fn list(
                    &self,
                    path: impl AsRef<str> + Send,
                    page_size: Option<usize>,
                ) -> impl Future<
                    Output = Result<BoxStream<'_, Result<Vec<Location>, IOError>>, InvalidLocationError>,
                > + Send {
                    self.as_ref().list(path, page_size)
                }

                fn remove_all(
                    &self,
                    path: impl AsRef<str> + Send,
                ) -> impl Future<Output = Result<(), DeleteError>> + Send {
                    self.as_ref().remove_all(path)
                }
            }
        )+
    };
}

// Generate implementations for Arc<T> and Box<T>
impl_lakekeeper_storage_for_smart_pointer!(Arc<T>, Box<T>);

async fn abort_unfinished_batch_delete_futures(
    join_set: &mut JoinSet<Result<(), DeleteBatchError>>,
) {
    // Abort all running tasks
    join_set.abort_all();

    // Await all futures, log any errors except Join Errors where `is_canceled` is true
    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(Ok(())) => {}
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

/// Executes futures in parallel with a specified parallelism limit using `JoinSet`.
///
/// # Arguments
/// * `futures` - An iterator of futures to execute
/// * `parallelism` - Maximum number of futures to execute concurrently
///
/// # Returns
/// A stream of results from futures as they complete, or ends with a `JoinError`
pub fn execute_with_parallelism<I, F, T>(
    futures: I,
    parallelism: usize,
) -> impl futures::Stream<Item = Result<T, tokio::task::JoinError>>
where
    I: IntoIterator<Item = F>,
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    async_stream::stream! {
        let mut join_set = JoinSet::new();
        let mut futures_iter = futures.into_iter();

        // Initial spawn up to parallelism limit
        for _ in 0..parallelism {
            if let Some(future) = futures_iter.next() {
                join_set.spawn(future);
            } else {
                break;
            }
        }

        // Process completed futures and spawn new ones
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(value) => {
                    yield Ok(value);

                    // Spawn the next future if available
                    if let Some(future) = futures_iter.next() {
                        join_set.spawn(future);
                    }
                }
                Err(join_error) => {
                    // Abort all remaining futures
                    join_set.abort_all();
                    yield Err(join_error);
                    return;
                }
            }
        }
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

    #[tokio::test]
    async fn test_execute_with_parallelism() {
        use std::{
            sync::{
                atomic::{AtomicUsize, Ordering},
                Arc,
            },
            time::Duration,
        };

        // Test basic functionality
        let futures = (0..5).map(|i| async move { i * 2 });
        let results_stream = execute_with_parallelism(futures, 2);
        tokio::pin!(results_stream);

        let mut results = Vec::new();
        while let Some(result) = results_stream.next().await {
            results.push(result);
        }

        // All futures should complete successfully
        assert_eq!(results.len(), 5);
        let mut values: Vec<i32> = results.into_iter().map(|r| r.unwrap()).collect();
        values.sort_unstable(); // Results may not be in order
        assert_eq!(values, vec![0, 2, 4, 6, 8]);

        // Test parallelism limit
        let counter = Arc::new(AtomicUsize::new(0));
        let max_concurrent = Arc::new(AtomicUsize::new(0));

        let futures = (0..10).map(|_| {
            let counter = counter.clone();
            let max_concurrent = max_concurrent.clone();
            async move {
                let current = counter.fetch_add(1, Ordering::SeqCst) + 1;
                let mut max = max_concurrent.load(Ordering::SeqCst);
                while max < current
                    && max_concurrent
                        .compare_exchange_weak(max, current, Ordering::SeqCst, Ordering::SeqCst)
                        .is_err()
                {
                    max = max_concurrent.load(Ordering::SeqCst);
                }

                tokio::time::sleep(Duration::from_millis(10)).await;
                counter.fetch_sub(1, Ordering::SeqCst);
                42
            }
        });

        let results_stream = execute_with_parallelism(futures, 3);
        tokio::pin!(results_stream);

        let mut results = Vec::new();
        while let Some(result) = results_stream.next().await {
            results.push(result);
        }

        assert_eq!(results.len(), 10);
        assert!(results.iter().all(|r| r.as_ref().unwrap() == &42));

        // Verify parallelism was respected (allow some tolerance for timing)
        let max_observed = max_concurrent.load(Ordering::SeqCst);
        assert!(
            max_observed <= 3,
            "Expected max concurrency <= 3, got {max_observed}",
        );
    }
}
