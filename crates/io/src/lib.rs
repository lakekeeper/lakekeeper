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
use chrono::{DateTime, Utc};
pub use error::{
    DeleteBatchError, DeleteError, ErrorKind, IOError, InitializeClientError, InternalError,
    InvalidLocationError, ReadError, RetryableError, RetryableErrorKind, WriteError,
};
use futures::{TryStreamExt as _, stream::BoxStream};
pub use location::{Location, LocationParseError};
pub use tokio;
pub use tryhard;
use tryhard::{RetryPolicy, backoff_strategies::BackoffStrategy};

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

#[derive(Debug, Clone)]
pub struct FileInfo {
    last_modified: Option<DateTime<Utc>>,
    location: Location,
}

impl FileInfo {
    #[must_use]
    pub fn new(last_modified: Option<DateTime<Utc>>, location: Location) -> Self {
        Self {
            last_modified,
            location,
        }
    }

    #[must_use]
    pub fn last_modified(&self) -> Option<DateTime<Utc>> {
        self.last_modified
    }

    #[must_use]
    pub fn location(&self) -> &Location {
        &self.location
    }
}

#[async_trait::async_trait]
pub trait LakekeeperStorage
where
    Self: std::fmt::Debug + Send + Sync + 'static,
{
    /// Deletes file.
    ///
    /// # Arguments
    ///
    /// * path: It should be *absolute* path starting with scheme string
    async fn delete(&self, path: &str) -> Result<(), DeleteError>;

    /// Deletes files in batch.
    ///
    /// # Arguments
    ///
    /// * paths: Absolute paths to delete, each starting with a scheme string.
    ///
    /// # Returns
    /// * A future that resolves to a result containing either:
    ///   - `Ok(())` if all deletions were successful.
    ///   - `Err(DeleteBatchError)` if any deletion failed.
    async fn delete_batch(&self, paths: Vec<String>) -> Result<(), DeleteBatchError>;

    /// Write the provided data to the specified path.
    async fn write(&self, path: &str, bytes: Bytes) -> Result<(), WriteError>;

    /// Read a file from the specified path, possibly in chunks
    ///
    /// # Arguments
    /// path: It should be an absolute path starting with scheme string.
    async fn read(&self, path: &str) -> Result<Bytes, ReadError>;

    /// Read a file from the specified path with a single request.
    ///
    /// # Arguments
    /// path: It should be an absolute path starting with scheme string.
    async fn read_single(&self, path: &str) -> Result<Bytes, ReadError>;

    /// List files for this prefix.
    /// If the provided location does not end with a slash, the slash will be added automatically.
    /// Retries are handled internally.
    async fn list(
        &self,
        path: &str,
        page_size: Option<usize>,
    ) -> Result<BoxStream<'_, Result<Vec<FileInfo>, IOError>>, InvalidLocationError>;

    /// Remove a directory and all of its contents under the given prefix.
    ///
    /// The default implementation streams the listing and issues up to
    /// [`REMOVE_ALL_CONCURRENCY`] concurrent `delete_batch` calls (one per listed
    /// page) on the same task, without spawning. This keeps memory bounded to
    /// roughly `REMOVE_ALL_CONCURRENCY × page_size` paths in flight while
    /// remaining compatible with `&self` dispatch and `dyn LakekeeperStorage`.
    ///
    /// Backends with a native recursive delete (e.g. ADLS Gen2) or direct
    /// in-process state (e.g. the in-memory backend) should override this
    /// method to use the faster path.
    ///
    /// # Error semantics
    ///
    /// On the first error (listing or deleting), this function stops polling
    /// further page-delete futures. In-flight batches in the same concurrent
    /// group may complete or be dropped. Callers must treat `remove_all` as
    /// best-effort and may re-run it to finish cleanup.
    async fn remove_all(&self, path: &str) -> Result<(), DeleteError> {
        let list_stream = self.list(path, None).await?;
        list_stream
            .map_err(DeleteError::from)
            .try_for_each_concurrent(REMOVE_ALL_CONCURRENCY, |page| async move {
                if page.is_empty() {
                    return Ok(());
                }
                let locations: Vec<String> = page
                    .into_iter()
                    .map(|file_info| file_info.location().to_string())
                    .collect();
                self.delete_batch(locations).await.map_err(Into::into)
            })
            .await
    }
}

/// Maximum number of concurrent `delete_batch` calls issued by the default
/// [`LakekeeperStorage::remove_all`] implementation.
pub const REMOVE_ALL_CONCURRENCY: usize = 10;

#[async_trait::async_trait]
impl LakekeeperStorage for StorageBackend {
    async fn delete(&self, path: &str) -> Result<(), DeleteError> {
        match self {
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

    async fn delete_batch(&self, paths: Vec<String>) -> Result<(), DeleteBatchError> {
        match self {
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

    async fn write(&self, path: &str, bytes: Bytes) -> Result<(), WriteError> {
        match self {
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

    async fn read(&self, path: &str) -> Result<Bytes, ReadError> {
        match self {
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

    async fn read_single(&self, path: &str) -> Result<Bytes, ReadError> {
        match self {
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

    async fn list(
        &self,
        path: &str,
        page_size: Option<usize>,
    ) -> Result<BoxStream<'_, Result<Vec<FileInfo>, IOError>>, InvalidLocationError> {
        match self {
            #[cfg(feature = "storage-s3")]
            StorageBackend::S3(s3_storage) => s3_storage.list(path, page_size).await,
            #[cfg(feature = "storage-in-memory")]
            StorageBackend::Memory(memory_storage) => memory_storage.list(path, page_size).await,
            #[cfg(feature = "storage-adls")]
            StorageBackend::Adls(adls_storage) => adls_storage.list(path, page_size).await,
            #[cfg(feature = "storage-gcs")]
            StorageBackend::Gcs(gcs_storage) => gcs_storage.list(path, page_size).await,
        }
    }

    async fn remove_all(&self, path: &str) -> Result<(), DeleteError> {
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

// Unified delegating `LakekeeperStorage` impls.
//
// Each invocation row supplies `($($impl_header:tt)*)` — the full
// `impl … LakekeeperStorage for TYPE [where …]` header, captured as a token
// tree so that generics, where-clauses and the presence or absence of a
// `T: LakekeeperStorage` bound stay flexible.
//
// Every method body uses `&**self` to obtain a reference to the underlying
// implementor:
//   * For `Arc<T>` / `Box<T>` with `T: LakekeeperStorage`, this yields `&T`.
//   * For `Arc<dyn LakekeeperStorage>` / `Box<dyn LakekeeperStorage>`, this
//     yields `&dyn LakekeeperStorage`, which dispatches through the vtable.
// Trait objects do not implement their own trait, so an explicit impl is
// required for the dyn wrappers in addition to the blanket impls.
//
// Delegation expressions are written inside the macro definition (not passed
// in as parameters) so `self` resolves with the method body's hygiene context;
// passing `&**self` through a `tt` parameter captures the call-site hygiene
// of `self`, triggering E0425. Method bodies live inline here for the same
// reason their signatures do: `#[async_trait::async_trait]` generates
// hygienic `'async_trait` lifetimes from the token spans of the `async fn`
// signatures, and tokens originating inside a nested declarative macro
// invocation violate the trait's lifetime bounds (E0195).
//
// Adding a new method on `LakekeeperStorage` therefore requires updating only
// this one place.
macro_rules! impl_lakekeeper_storage_delegating {
    ( $( ( $($impl_header:tt)* ) ; )+ ) => {
        $(
            #[async_trait::async_trait]
            $($impl_header)* {
                async fn delete(&self, path: &str) -> Result<(), DeleteError> {
                    (**self).delete(path).await
                }

                async fn delete_batch(
                    &self,
                    paths: Vec<String>,
                ) -> Result<(), DeleteBatchError> {
                    (**self).delete_batch(paths).await
                }

                async fn write(&self, path: &str, bytes: Bytes) -> Result<(), WriteError> {
                    (**self).write(path, bytes).await
                }

                async fn read(&self, path: &str) -> Result<Bytes, ReadError> {
                    (**self).read(path).await
                }

                async fn read_single(&self, path: &str) -> Result<Bytes, ReadError> {
                    (**self).read_single(path).await
                }

                async fn list(
                    &self,
                    path: &str,
                    page_size: Option<usize>,
                ) -> Result<
                    BoxStream<'_, Result<Vec<FileInfo>, IOError>>,
                    InvalidLocationError,
                > {
                    (**self).list(path, page_size).await
                }

                async fn remove_all(&self, path: &str) -> Result<(), DeleteError> {
                    (**self).remove_all(path).await
                }
            }
        )+
    };
}

impl_lakekeeper_storage_delegating! {
    (impl<T> LakekeeperStorage for Arc<T> where T: LakekeeperStorage) ;
    (impl<T> LakekeeperStorage for Box<T> where T: LakekeeperStorage) ;
    (impl LakekeeperStorage for Arc<dyn LakekeeperStorage>) ;
    (impl LakekeeperStorage for Box<dyn LakekeeperStorage>) ;
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
        let mut join_set = tokio::task::JoinSet::new();
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
    use futures::StreamExt as _;

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
                Arc,
                atomic::{AtomicUsize, Ordering},
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
