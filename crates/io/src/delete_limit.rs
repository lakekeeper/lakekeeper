//! Process-wide limits for bulk deletes.
//!
//! A bulk delete sends many requests at once. Several bulk deletes against the same storage,
//! for example a table purge and a cleanup of orphan files, add up. Storage services throttle
//! above a request rate per bucket or account, and every request needs a connection. All bulk
//! deletes of the process therefore share one limit per storage endpoint. Single deletes take
//! no permit, so a request that waits for one, such as a table commit, does not queue for the
//! limit. On S3 and GCS, `remove_all` lists the files and deletes them as a bulk delete.

use std::{
    collections::HashMap,
    sync::{Arc, LazyLock, Mutex, PoisonError, Weak},
};

use tokio::sync::Semaphore;

/// Concurrent `DeleteObjects` requests per S3 bucket. Each deletes up to 1,000 keys, and AWS
/// accepts about 3,500 deletes per second for one prefix.
#[cfg(feature = "storage-s3")]
pub(crate) const S3_DELETE_REQUESTS: usize = 4;
/// Concurrent deletes per GCS bucket. A new bucket accepts about 1,000 writes per second.
#[cfg(feature = "storage-gcs")]
const GCS_DELETE_REQUESTS: usize = 32;
/// Concurrent deletes per ADLS filesystem. Each holds one connection.
#[cfg(feature = "storage-adls")]
const ADLS_DELETE_REQUESTS: usize = 64;

/// Semaphores of the endpoints that bulk deletes use. Creating a semaphore removes the entries
/// whose semaphore was dropped.
static LIMITS: LazyLock<Mutex<HashMap<String, Weak<Semaphore>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Storage endpoint whose bulk deletes share one limit.
#[derive(Debug, Clone, Copy)]
pub(crate) enum DeleteEndpoint<'a> {
    /// A bucket of an S3 service. `scope` names the service: the origin of its endpoint,
    /// `aws-` and the AWS region, or `aws` for a client without a region.
    #[cfg(feature = "storage-s3")]
    S3 { scope: &'a str, bucket: &'a str },
    #[cfg(feature = "storage-gcs")]
    Gcs { bucket: &'a str },
    /// A filesystem of an ADLS endpoint. On `OneLake`, a filesystem is a workspace: all
    /// workspaces share one endpoint.
    #[cfg(feature = "storage-adls")]
    Adls {
        endpoint: &'a str,
        filesystem: &'a str,
    },
}

impl DeleteEndpoint<'_> {
    fn key(&self) -> String {
        match self {
            #[cfg(feature = "storage-s3")]
            DeleteEndpoint::S3 { scope, bucket } => format!("s3:{scope}:{bucket}"),
            #[cfg(feature = "storage-gcs")]
            DeleteEndpoint::Gcs { bucket } => format!("gcs:{bucket}"),
            #[cfg(feature = "storage-adls")]
            DeleteEndpoint::Adls {
                endpoint,
                filesystem,
            } => format!("adls:{endpoint}:{filesystem}"),
        }
    }

    fn permits(&self) -> usize {
        match self {
            #[cfg(feature = "storage-s3")]
            DeleteEndpoint::S3 { .. } => S3_DELETE_REQUESTS,
            #[cfg(feature = "storage-gcs")]
            DeleteEndpoint::Gcs { .. } => GCS_DELETE_REQUESTS,
            #[cfg(feature = "storage-adls")]
            DeleteEndpoint::Adls { .. } => ADLS_DELETE_REQUESTS,
        }
    }
}

/// Limit on concurrent bulk-delete requests to one endpoint, shared by all storage clients of
/// the process.
#[derive(Debug, Clone)]
pub(crate) struct DeleteLimit(Arc<Semaphore>);

impl DeleteLimit {
    pub(crate) fn of(endpoint: &DeleteEndpoint<'_>) -> Self {
        let key = endpoint.key();
        let mut limits = LIMITS.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(semaphore) = limits.get(&key).and_then(Weak::upgrade) {
            return Self(semaphore);
        }
        limits.retain(|_, semaphore| semaphore.strong_count() > 0);
        let semaphore = Arc::new(Semaphore::new(endpoint.permits()));
        limits.insert(key, Arc::downgrade(&semaphore));
        Self(semaphore)
    }

    /// Runs `request`, including its retries, while it holds one permit of the endpoint.
    pub(crate) async fn run<F: Future>(self, request: F) -> F::Output {
        // The semaphore is never closed, so acquiring can only wait.
        let _permit = self.0.acquire_owned().await.ok();
        request.await
    }

    /// Takes every permit of the endpoint until the returned permit is dropped.
    #[cfg(test)]
    pub(crate) async fn hold_all(&self) -> tokio::sync::OwnedSemaphorePermit {
        let permits = u32::try_from(self.0.available_permits()).unwrap();
        self.0.clone().acquire_many_owned(permits).await.unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "storage-gcs")]
    #[test]
    fn same_endpoint_shares_one_limit() {
        let first = DeleteLimit::of(&DeleteEndpoint::Gcs {
            bucket: "limit-shared",
        });
        let second = DeleteLimit::of(&DeleteEndpoint::Gcs {
            bucket: "limit-shared",
        });
        let other = DeleteLimit::of(&DeleteEndpoint::Gcs {
            bucket: "limit-other",
        });
        assert!(Arc::ptr_eq(&first.0, &second.0));
        assert!(!Arc::ptr_eq(&first.0, &other.0));
        assert_eq!(first.0.available_permits(), GCS_DELETE_REQUESTS);
    }

    #[cfg(feature = "storage-s3")]
    #[test]
    fn s3_buckets_of_different_services_have_separate_limits() {
        let minio = DeleteLimit::of(&DeleteEndpoint::S3 {
            scope: "http://minio-a:9000",
            bucket: "limit-warehouse",
        });
        let other = DeleteLimit::of(&DeleteEndpoint::S3 {
            scope: "http://minio-b:9000",
            bucket: "limit-warehouse",
        });
        assert!(!Arc::ptr_eq(&minio.0, &other.0));
    }

    #[cfg(feature = "storage-adls")]
    #[test]
    fn dropped_limits_leave_the_registry() {
        drop(DeleteLimit::of(&DeleteEndpoint::Adls {
            endpoint: "https://limit.dfs.core.windows.net/",
            filesystem: "dropped",
        }));
        let _other = DeleteLimit::of(&DeleteEndpoint::Adls {
            endpoint: "https://limit.dfs.core.windows.net/",
            filesystem: "other",
        });
        let limits = LIMITS.lock().unwrap_or_else(PoisonError::into_inner);
        assert!(!limits.contains_key("adls:https://limit.dfs.core.windows.net/:dropped"));
        assert!(limits.contains_key("adls:https://limit.dfs.core.windows.net/:other"));
    }

    #[cfg(feature = "storage-s3")]
    fn s3_limit(bucket: &str) -> DeleteLimit {
        DeleteLimit::of(&DeleteEndpoint::S3 {
            scope: "test-scope",
            bucket,
        })
    }

    /// Runs `jobs` of one second each through `execute_with_parallelism` with a fan-out of 16,
    /// each holding a permit of `limit`, and records how many ran at once.
    #[cfg(feature = "storage-s3")]
    fn timed_jobs(
        limit: &DeleteLimit,
        jobs: usize,
        running: &Arc<std::sync::atomic::AtomicUsize>,
        max_running: &Arc<std::sync::atomic::AtomicUsize>,
    ) -> impl futures::Stream<Item = Result<(), tokio::task::JoinError>> + use<> {
        use std::sync::atomic::Ordering;
        let futures: Vec<_> = (0..jobs)
            .map(|_| {
                let running = running.clone();
                let max_running = max_running.clone();
                limit.clone().run(async move {
                    let now = running.fetch_add(1, Ordering::SeqCst) + 1;
                    max_running.fetch_max(now, Ordering::SeqCst);
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    running.fetch_sub(1, Ordering::SeqCst);
                })
            })
            .collect();
        crate::execute_with_parallelism(futures, 16)
    }

    #[cfg(feature = "storage-s3")]
    #[tokio::test(start_paused = true)]
    async fn concurrent_bulk_deletes_share_the_limit() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        use futures::StreamExt as _;

        let limit = s3_limit("limit-concurrent");
        let running = Arc::new(AtomicUsize::new(0));
        let max_running = Arc::new(AtomicUsize::new(0));
        let start = tokio::time::Instant::now();
        let batches = (0..3).map(|_| timed_jobs(&limit, 10, &running, &max_running).count());
        let completed = futures::future::join_all(batches).await;

        assert_eq!(completed, vec![10, 10, 10]);
        assert_eq!(max_running.load(Ordering::SeqCst), S3_DELETE_REQUESTS);
        // 30 jobs of one second, 4 at a time.
        assert_eq!(start.elapsed(), std::time::Duration::from_secs(8));
    }

    #[cfg(feature = "storage-s3")]
    #[tokio::test(start_paused = true)]
    async fn dropped_bulk_delete_returns_its_permits() {
        use std::sync::atomic::AtomicUsize;

        use futures::StreamExt as _;

        let limit = s3_limit("limit-dropped");
        let running = Arc::new(AtomicUsize::new(0));
        let max_running = Arc::new(AtomicUsize::new(0));
        let mut jobs = Box::pin(timed_jobs(&limit, 10, &running, &max_running));
        jobs.next().await.unwrap().unwrap();
        drop(jobs);

        let permits = tokio::time::timeout(
            // Shorter than a job: only aborted jobs return their permits in time.
            std::time::Duration::from_millis(500),
            limit
                .0
                .acquire_many(u32::try_from(S3_DELETE_REQUESTS).unwrap()),
        )
        .await;
        assert!(matches!(permits, Ok(Ok(_))));
    }
}
