use std::{future::Future, sync::Arc, time::Duration};

use futures::stream::{self, Stream, StreamExt};
use iceberg::spec::SnapshotRef;
use lakekeeper_io::{LakekeeperStorage, RetryableError};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};

use crate::operations::{
    error::{CancelledError, InternalError, ReadError, RetryableErrorKind},
    manifest_decode::visit_manifest_list,
    parse_gate::run_parse,
    progress_tracker::ProgressTracker,
    task::ExtendedExponentialBackoff,
};

/// Number of retries of a storage read or delete on transient errors (timeouts,
/// `503 ServiceUnavailable`, `SlowDown` / `TooManyRequests`).
///
/// Combined with exponential backoff (100ms base, 10s cap) this gives a
/// total retry window of ~22s without jitter, capped at 30s by `RETRY_MAX_TOTAL`.
/// Real rate-limit incidents often clear within 10-30s.
const RETRIES: u32 = 8;
/// Minimum delay before the first retry. Grows exponentially and is jittered.
const RETRY_MIN_DELAY: Duration = Duration::from_millis(100);
/// Maximum per-attempt delay.
const RETRY_MAX_DELAY: Duration = Duration::from_secs(10);
/// Hard cap on total retry time per individual operation.
const RETRY_MAX_TOTAL: Duration = Duration::from_secs(30);

/// Manifest lists read at once, further bounded by [`KIB_IN_FLIGHT`]. Reading small lists is
/// bound by storage latency, so many are read at once.
const LIST_READS_IN_FLIGHT: usize = 64;
/// Manifests read at once, further bounded by [`KIB_IN_FLIGHT`].
const MANIFEST_READS_IN_FLIGHT: usize = 64;
/// Bytes of the files one visit has read and not yet handed out as results, in units of 1024
/// bytes. A file larger than the budget waits for all of it and is read alone.
const KIB_IN_FLIGHT: u32 = 64 * 1024;

/// Hands out the storage a reader reads with.
pub trait LakekeeperIoProvider
where
    Self: std::fmt::Debug + Send + Sync,
{
    type LakekeeperIO: LakekeeperStorage + Clone;

    /// The storage to read with. It may refresh credentials, so fetch it again for each attempt.
    fn lakekeeper_io(&self) -> impl Future<Output = Self::LakekeeperIO> + Send;
}

/// Items a loop handles between yields to the runtime.
const ITEMS_PER_YIELD: usize = 4096;

/// Runs `f` on each item and yields to the runtime every [`ITEMS_PER_YIELD`] items. Folding a
/// long manifest list into a map otherwise holds a runtime worker for tens of milliseconds.
pub async fn for_each_yielding<I>(items: impl IntoIterator<Item = I>, mut f: impl FnMut(I)) {
    let _: Result<(), std::convert::Infallible> = try_for_each_yielding(items, |item| {
        f(item);
        Ok(())
    })
    .await;
}

/// [`for_each_yielding`] for a fallible `f`, stopping at the first error.
///
/// # Errors
/// Returns the first error of `f`.
pub async fn try_for_each_yielding<I, E>(
    items: impl IntoIterator<Item = I>,
    mut f: impl FnMut(I) -> Result<(), E>,
) -> Result<(), E> {
    for (i, item) in items.into_iter().enumerate() {
        f(item)?;
        if (i + 1) % ITEMS_PER_YIELD == 0 {
            tokio::task::yield_now().await;
        }
    }
    Ok(())
}

/// Runs a storage operation, retrying errors that should be retried with jittered backoff. Fetches
/// the storage on each attempt, so a retry in a long run picks up refreshed credentials.
///
/// # Errors
/// Returns the first error that should not be retried, or the last one once the retries are used up.
pub async fn with_storage_retries<P, R, E, Fut>(
    provider: &P,
    operation: impl Fn(P::LakekeeperIO) -> Fut,
) -> Result<R, E>
where
    P: LakekeeperIoProvider,
    E: RetryableError + 'static,
    Fut: Future<Output = Result<R, E>>,
{
    tryhard::retry_fn(|| async {
        match operation(provider.lakekeeper_io().await).await {
            Ok(result) => Ok(Ok(result)),
            Err(e) if e.should_retry() => Err(e),
            Err(e) => Ok(Err(e)),
        }
    })
    .retries(RETRIES)
    .custom_backoff(
        ExtendedExponentialBackoff::new(RETRY_MIN_DELAY, RETRY_MAX_DELAY, RETRY_MAX_TOTAL)
            .with_jitter(),
    )
    .await?
}

#[derive(thiserror::Error, Debug)]
pub enum ManifestReaderError {
    #[error("{0}")]
    Cancelled(#[from] CancelledError),
    #[error("{0}")]
    ReadFailed(#[from] ReadError),
}

/// What reading a manifest needs: its path, and the length its manifest list records, which
/// sizes its share of the read budget.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ManifestRef {
    pub path: String,
    pub length: Option<i64>,
}

/// The manifests a manifest list names. A parse function for
/// [`ManifestReader::visit_manifest_lists`].
///
/// # Errors
/// Returns a [`ReadError::ParseError`] if the bytes are not a manifest list.
pub fn manifest_refs_of_list(
    bytes: &[u8],
    snapshot: &SnapshotRef,
) -> Result<Vec<ManifestRef>, ReadError> {
    let mut manifests = Vec::new();
    visit_manifest_list(bytes, snapshot.manifest_list(), |entry| {
        manifests.push(ManifestRef {
            path: entry.manifest_path,
            length: entry.manifest_length,
        });
        Ok(())
    })?;
    Ok(manifests)
}

/// What a parse function returned for one file, and how many bytes the file has.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Read<R> {
    pub value: R,
    /// Bytes read from storage. Can differ from the length a manifest list records.
    pub actual_length: u64,
}

/// Reads manifest lists and manifests and hands their bytes to a parse function.
///
/// Parsing runs on the blocking pool behind the process-wide parse slots, so each visit holds
/// only what its parse function returns. A file's bytes count against the visit's budget until
/// its result is handed out. Reads are retried with jittered backoff; a failed parse is not retried. Results
/// arrive in completion order. When the progress tracker is cancelled, the stream yields
/// [`ManifestReaderError::Cancelled`] for every file not yet read.
#[derive(Debug, Clone)]
pub struct ManifestReader<P: LakekeeperIoProvider + Clone> {
    provider: P,
}

impl<P: LakekeeperIoProvider + Clone + 'static> ManifestReader<P> {
    #[must_use]
    pub fn new(provider: P) -> Self {
        Self { provider }
    }

    /// The storage provider this reader reads with, usually the table's operations.
    #[must_use]
    pub fn table_operations(&self) -> &P {
        &self.provider
    }

    /// Reads the manifest list of each snapshot and runs `parse` on its bytes.
    pub fn visit_manifest_lists<R, F>(
        &self,
        snapshots: Vec<SnapshotRef>,
        progress_tracker: ProgressTracker,
        parse: F,
    ) -> impl Stream<Item = Result<(SnapshotRef, Read<R>), ManifestReaderError>> + use<P, R, F>
    where
        R: Send + 'static,
        F: Fn(&[u8], &SnapshotRef) -> Result<R, ReadError> + Clone + Send + Sync + 'static,
    {
        let provider = self.provider.clone();
        let budget = Arc::new(ListBudget::new());
        stream::iter(snapshots.into_iter().enumerate())
            .map(move |(i, snapshot)| {
                let provider = provider.clone();
                let progress_tracker = progress_tracker.clone();
                let parse = parse.clone();
                let budget = Arc::clone(&budget);
                async move {
                    let read = async {
                        let mut permit = budget.reserve(i).await?;
                        let bytes = read_with_retries(&provider, snapshot.manifest_list())
                            .await
                            .inspect_err(|e| {
                                let sid = snapshot.snapshot_id();
                                tracing::warn!(
                                    ?e,
                                    "Failed to read manifest list for snapshot {sid}: {e}"
                                );
                            })?;
                        budget.settle(&mut permit, bytes.as_ref().len());
                        Ok((permit, bytes))
                    };
                    let (permit, bytes) = unless_cancelled(&progress_tracker, read)
                        .await
                        .inspect_err(|_| budget.settle_failed())?;
                    let actual_length = byte_count(&bytes);
                    let parsed_snapshot = Arc::clone(&snapshot);
                    // The permit comes back with the result and goes once it is handed out.
                    let (result, _permit) = run_parse(snapshot.manifest_list(), move || {
                        (parse(bytes.as_ref(), &parsed_snapshot), permit)
                    })
                    .await
                    .map_err(ReadError::from)?;
                    let value = result?;
                    progress_tracker.increment(1);
                    Ok((
                        snapshot,
                        Read {
                            value,
                            actual_length,
                        },
                    ))
                }
            })
            .buffer_unordered(LIST_READS_IN_FLIGHT)
    }

    /// Reads each manifest and runs `parse` on its bytes.
    pub fn visit_manifests<R, F>(
        &self,
        manifests: Vec<ManifestRef>,
        progress_tracker: ProgressTracker,
        parse: F,
    ) -> impl Stream<Item = Result<(ManifestRef, Read<R>), ManifestReaderError>> + use<P, R, F>
    where
        R: Send + 'static,
        F: Fn(&[u8], &ManifestRef) -> Result<R, ReadError> + Clone + Send + Sync + 'static,
    {
        let provider = self.provider.clone();
        let bytes_in_flight = Arc::new(Semaphore::new(KIB_IN_FLIGHT as usize));
        stream::iter(manifests)
            .map(move |manifest| {
                let provider = provider.clone();
                let progress_tracker = progress_tracker.clone();
                let parse = parse.clone();
                let bytes_in_flight = Arc::clone(&bytes_in_flight);
                async move {
                    let read = async {
                        let permit = bytes_in_flight
                            .acquire_many_owned(kib_weight(manifest.length))
                            .await
                            .map_err(budget_closed)?;
                        let bytes = read_with_retries(&provider, &manifest.path)
                            .await
                            .inspect_err(|e| {
                                let path = &manifest.path;
                                tracing::warn!(?e, "Failed to read manifest file at {path}: {e}");
                            })?;
                        Ok((permit, bytes))
                    };
                    let (permit, bytes) = unless_cancelled(&progress_tracker, read).await?;
                    let actual_length = byte_count(&bytes);
                    let parsed_manifest = manifest.clone();
                    // The permit comes back with the result and goes once it is handed out.
                    let (result, _permit) = run_parse(&manifest.path, move || {
                        (parse(bytes.as_ref(), &parsed_manifest), permit)
                    })
                    .await
                    .map_err(ReadError::from)?;
                    let value = result?;
                    progress_tracker.increment(1);
                    Ok((
                        manifest,
                        Read {
                            value,
                            actual_length,
                        },
                    ))
                }
            })
            .buffer_unordered(MANIFEST_READS_IN_FLIGHT)
    }
}

/// Runs `f` unless or until `progress_tracker` is cancelled.
async fn unless_cancelled<R>(
    progress_tracker: &ProgressTracker,
    f: impl Future<Output = Result<R, ManifestReaderError>>,
) -> Result<R, ManifestReaderError> {
    tokio::select! {
        biased;
        () = progress_tracker.cancelled() => Err(CancelledError {}.into()),
        result = f => result,
    }
}

/// Permits a file of `length` bytes takes from the in-flight budget: at least one, at most the
/// whole budget. A file of unknown length takes an even share.
fn kib_weight(length: Option<i64>) -> u32 {
    let Some(length) = length
        .and_then(|length| u64::try_from(length).ok())
        .filter(|l| *l > 0)
    else {
        return KIB_IN_FLIGHT / u32::try_from(MANIFEST_READS_IN_FLIGHT).unwrap_or(u32::MAX);
    };
    u32::try_from(length.div_ceil(1024))
        .unwrap_or(u32::MAX)
        .clamp(1, KIB_IN_FLIGHT)
}

fn byte_count(bytes: &impl AsRef<[u8]>) -> u64 {
    u64::try_from(bytes.as_ref().len()).unwrap_or(u64::MAX)
}

fn budget_closed(e: impl std::fmt::Display) -> ReadError {
    InternalError::new(
        format!("Read budget is closed: {e}"),
        RetryableErrorKind::Temporary,
    )
    .into()
}

/// The in-flight budget of one manifest-list visit. A list's size is known only once it is read,
/// so the first read reserves the whole budget and runs alone. Every later read reserves the size
/// of the largest list read so far, and read `i` starts only once `⌈i/2⌉` reads ended, so the
/// reads that rely on that size grow with the sizes seen. A read gives back what its list did not
/// need.
#[derive(Debug)]
struct ListBudget {
    permits: Arc<Semaphore>,
    seen: watch::Sender<ListsSeen>,
}

#[derive(Debug, Clone, Copy, Default)]
struct ListsSeen {
    /// Reads that ended, with or without a list.
    ended: usize,
    /// Size of the largest list read so far, in units of 1024 bytes; 0 until a read ended.
    largest_kib: u32,
}

impl ListBudget {
    fn new() -> Self {
        Self {
            permits: Arc::new(Semaphore::new(KIB_IN_FLIGHT as usize)),
            seen: watch::Sender::new(ListsSeen::default()),
        }
    }

    /// Waits until read `i` may start and reserves its share of the budget.
    async fn reserve(&self, i: usize) -> Result<OwnedSemaphorePermit, ReadError> {
        let seen = *self
            .seen
            .subscribe()
            .wait_for(|seen| seen.ended >= i.div_ceil(2))
            .await
            .map_err(budget_closed)?;
        let kib = match seen.largest_kib {
            0 => KIB_IN_FLIGHT,
            kib => kib,
        };
        Arc::clone(&self.permits)
            .acquire_many_owned(kib)
            .await
            .map_err(budget_closed)
    }

    /// Records a list of `len` bytes and keeps only the permits it needs.
    fn settle(&self, permit: &mut OwnedSemaphorePermit, len: usize) {
        let kib = kib_weight(Some(i64::try_from(len).unwrap_or(i64::MAX)));
        self.seen.send_modify(|seen| {
            seen.ended += 1;
            seen.largest_kib = seen.largest_kib.max(kib);
        });
        let excess = permit.num_permits().saturating_sub(kib as usize);
        if excess > 0 {
            drop(permit.split(excess));
        }
    }

    /// Records a read that failed or stopped. Until a list is read, later reads reserve an even
    /// share of the budget.
    fn settle_failed(&self) {
        let share = KIB_IN_FLIGHT / u32::try_from(LIST_READS_IN_FLIGHT).unwrap_or(u32::MAX);
        self.seen.send_modify(|seen| {
            seen.ended += 1;
            if seen.largest_kib == 0 {
                seen.largest_kib = share.max(1);
            }
        });
    }
}

/// Reads `path`. A storage error names `path` as its location.
async fn read_with_retries<P: LakekeeperIoProvider>(
    provider: &P,
    path: &str,
) -> Result<impl AsRef<[u8]> + Send + 'static, ReadError> {
    with_storage_retries(provider, |io| async move {
        io.read_single(path).await.map_err(ReadError::from)
    })
    .await
    .map_err(|e| match e {
        ReadError::IOError(e) => ReadError::IOError(e.set_location(path)),
        e => e,
    })
}

#[cfg(test)]
mod tests {
    use lakekeeper_io::memory::MemoryStorage;

    use super::*;

    /// Storage for reader tests: one in-memory store.
    #[derive(Debug, Clone)]
    struct MemoryIo(MemoryStorage);

    impl LakekeeperIoProvider for MemoryIo {
        type LakekeeperIO = MemoryStorage;

        fn lakekeeper_io(&self) -> impl Future<Output = Self::LakekeeperIO> + Send {
            std::future::ready(self.0.clone())
        }
    }

    /// The reader reports the bytes it read, also when the manifest list records another length.
    #[tokio::test]
    async fn reports_the_bytes_read_not_the_recorded_length() {
        let io = MemoryStorage::new_isolated();
        let path = "memory://t/metadata/m0.avro".to_string();
        io.write(&path, vec![0; 3000].into()).await.unwrap();
        let reader = ManifestReader::new(MemoryIo(io));
        let manifest = ManifestRef {
            path,
            length: Some(10),
        };

        let results: Vec<_> = reader
            .visit_manifests(
                vec![manifest.clone()],
                ProgressTracker::default(),
                |bytes, _| Ok(bytes.len()),
            )
            .collect()
            .await;

        let [Ok((visited, read))] = results.as_slice() else {
            panic!("expected one result, got {results:?}");
        };
        assert_eq!(visited, &manifest);
        assert_eq!(
            read,
            &Read {
                value: 3000,
                actual_length: 3000
            }
        );
    }

    #[test]
    fn weighs_manifests_in_kib_within_the_budget() {
        assert_eq!(kib_weight(Some(1)), 1);
        assert_eq!(kib_weight(Some(1024)), 1);
        assert_eq!(kib_weight(Some(1025)), 2);
        assert_eq!(kib_weight(Some(i64::MAX)), KIB_IN_FLIGHT);
        // Some writers record no length or a wrong one.
        assert_eq!(kib_weight(None), 1024);
        assert_eq!(kib_weight(Some(0)), 1024);
        assert_eq!(kib_weight(Some(-5)), 1024);
    }

    /// A result counts against the budget until it is handed out, also while a slow consumer
    /// is busy with the one before.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn results_waiting_for_the_consumer_stay_within_the_budget() {
        use std::{
            pin::pin,
            sync::atomic::{AtomicUsize, Ordering},
        };

        /// A parse result; counts the results alive.
        struct Alive(Arc<AtomicUsize>);
        impl Drop for Alive {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::SeqCst);
            }
        }

        let io = MemoryStorage::new_isolated();
        // Each manifest declares an eighth of the budget.
        let mut manifests = Vec::new();
        for i in 0..40 {
            let path = format!("memory://t/metadata/m{i}.avro");
            io.write(&path, b"m".to_vec().into()).await.unwrap();
            manifests.push(ManifestRef {
                path,
                length: Some(i64::from(KIB_IN_FLIGHT) * 1024 / 8),
            });
        }

        let alive = Arc::new(AtomicUsize::new(0));
        let most_alive = Arc::new(AtomicUsize::new(0));
        let (counter, most) = (Arc::clone(&alive), Arc::clone(&most_alive));
        let reader = ManifestReader::new(MemoryIo(io));
        let mut results =
            pin!(
                reader.visit_manifests(manifests, ProgressTracker::default(), move |_, _| {
                    most.fetch_max(counter.fetch_add(1, Ordering::SeqCst) + 1, Ordering::SeqCst);
                    std::thread::sleep(Duration::from_millis(2));
                    Ok(Alive(Arc::clone(&counter)))
                },)
            );
        let mut consumed = 0;
        while let Some(result) = results.next().await {
            let result = result.unwrap();
            tokio::time::sleep(Duration::from_millis(20)).await;
            drop(result);
            consumed += 1;
        }
        assert_eq!(consumed, 40);
        // How close the peak gets to the budget depends on how fast the machine parses, so only
        // the bound is fixed. A result keeps its permit until it is handed out; with the permit
        // released at the end of the parse, 10 or more live at once.
        let most_alive = most_alive.load(Ordering::SeqCst);
        assert!(most_alive <= 8, "{most_alive} results alive at once");
    }

    /// Spawns read `i` reserving from `budget`; resolves to the permits it got.
    fn spawn_reserve(budget: &Arc<ListBudget>, i: usize) -> tokio::task::JoinHandle<usize> {
        let budget = Arc::clone(budget);
        tokio::spawn(async move { budget.reserve(i).await.unwrap().num_permits() })
    }

    #[tokio::test]
    async fn the_first_list_read_alone_sizes_the_reads_after_it() {
        let budget = Arc::new(ListBudget::new());
        let mut first = budget.reserve(0).await.unwrap();
        assert_eq!(first.num_permits(), KIB_IN_FLIGHT as usize);
        let next = spawn_reserve(&budget, 1);
        tokio::task::yield_now().await;
        assert!(!next.is_finished());

        budget.settle(&mut first, 100 * 1024);

        assert_eq!(first.num_permits(), 100);
        assert_eq!(next.await.unwrap(), 100);
    }

    #[tokio::test]
    async fn reads_start_as_earlier_reads_end() {
        let budget = Arc::new(ListBudget::new());
        let mut first = budget.reserve(0).await.unwrap();
        budget.settle(&mut first, 1024);
        let mut second = budget.reserve(1).await.unwrap();
        let second_wave = [spawn_reserve(&budget, 2), spawn_reserve(&budget, 3)];
        let third_wave = spawn_reserve(&budget, 5);
        tokio::task::yield_now().await;
        assert!(second_wave[0].is_finished());
        assert!(!second_wave[1].is_finished());

        // A larger list raises what later reads reserve.
        budget.settle(&mut second, 10 * 1024);

        let [third, fourth] = second_wave;
        assert_eq!(third.await.unwrap(), 1);
        assert_eq!(fourth.await.unwrap(), 10);
        tokio::task::yield_now().await;
        assert!(!third_wave.is_finished());
        budget.settle_failed();
        assert_eq!(third_wave.await.unwrap(), 10);
    }

    #[tokio::test]
    async fn a_failed_first_list_read_lets_the_others_go_ahead() {
        let budget = Arc::new(ListBudget::new());
        let first = budget.reserve(0).await.unwrap();
        let next = spawn_reserve(&budget, 1);

        budget.settle_failed();
        drop(first);

        assert_eq!(next.await.unwrap(), 1024);
    }
}
