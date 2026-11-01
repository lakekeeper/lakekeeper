use std::{collections::HashMap, sync::Arc};

use futures::future::BoxFuture;

use crate::{
    CancellationToken,
    service::tasks::{
        TaskQueueName,
        task_drain::{ArcTaskRunnerControl, run_as_worker},
    },
};

/// Infinitely running task worker loop function that polls tasks from a queue and
/// processes. Accepts a cancellation token for graceful shutdown.
///
/// A worker runs one task at a time: it finishes a task before it picks or polls again. A task
/// it picked counts as running until then, or until its future ends; a drain of the process
/// waits for it. Pick or poll inside the returned future: a task spawned from it is outside the
/// worker, so its picks are neither gated by a drain nor counted. Inside the returned future,
/// [`drain_token`](super::drain_token) is cancelled when the process starts to drain.
pub type TaskQueueWorkerFn = Arc<
    dyn Fn(tokio_util::sync::CancellationToken) -> BoxFuture<'static, ()> + Send + Sync + 'static,
>;

#[derive(Clone)]
pub(super) struct QueueWorkerConfig {
    pub(super) worker_fn: TaskQueueWorkerFn,
    /// Number of workers that run locally for this queue
    pub(super) num_workers: usize,
}

impl std::fmt::Debug for QueueWorkerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueueWorkerConfig")
            .field("worker_fn", &"Fn(...)")
            .field("num_workers", &self.num_workers)
            .finish()
    }
}

/// Runner for task queues that manages the worker processes
#[derive(Debug, Clone)]
pub struct TaskQueuesRunner {
    pub(super) registered_queues: Arc<HashMap<&'static TaskQueueName, QueueWorkerConfig>>,
    pub(super) cancellation_token: CancellationToken,
    pub(super) control: ArcTaskRunnerControl,
}

/// A running worker of a queue.
struct WorkerInfo {
    queue_name: &'static TaskQueueName,
    worker_id: usize,
    handle: tokio::task::JoinHandle<()>,
}

impl TaskQueuesRunner {
    /// Lets a drain of `control` stop these workers from picking.
    #[cfg(feature = "router")]
    #[must_use]
    pub(crate) fn with_control(self, control: ArcTaskRunnerControl) -> Self {
        Self { control, ..self }
    }

    fn spawn_worker(
        &self,
        queue_name: &'static TaskQueueName,
        worker_id: usize,
        queue: &QueueWorkerConfig,
    ) -> WorkerInfo {
        let worker = (queue.worker_fn)(self.cancellation_token.clone());
        WorkerInfo {
            queue_name,
            worker_id,
            handle: tokio::task::spawn(run_as_worker(Arc::clone(&self.control), worker)),
        }
    }

    /// Runs all registered task queue workers and monitors them, restarting any that exit.
    /// Accepts a cancellation token for graceful shutdown.
    pub async fn run_queue_workers(self, restart_workers: bool) {
        let mut workers = Vec::new();
        let registered_queues = Arc::clone(&self.registered_queues);

        // Initialize all workers
        for (queue_name, queue) in registered_queues.iter() {
            tracing::info!(
                "Starting {} workers for task queue `{queue_name}`.",
                queue.num_workers
            );

            for worker_id in 0..queue.num_workers {
                tracing::debug!(
                    "Starting `{queue_name}` worker {} ({}/{})",
                    worker_id,
                    worker_id + 1,
                    queue.num_workers
                );
                workers.push(self.spawn_worker(queue_name, worker_id, queue));
            }
        }

        // Main worker monitoring loop
        loop {
            if workers.is_empty() {
                return;
            }

            // Wait for any worker to complete
            let mut_handles: Vec<_> = workers.iter_mut().map(|w| &mut w.handle).collect();
            let (result, index, _) = futures::future::select_all(mut_handles).await;

            // Get the completed worker's info
            let worker = workers.swap_remove(index);
            self.log_worker_end(&worker, &result, restart_workers);

            // Restart the worker only if cancellation hasn't been requested
            if restart_workers && !self.cancellation_token.is_cancelled() {
                if let Some(queue) = registered_queues.get(worker.queue_name) {
                    tracing::debug!(
                        "Restarting task queue {} worker {}",
                        worker.queue_name,
                        worker.worker_id
                    );
                    workers.push(self.spawn_worker(worker.queue_name, worker.worker_id, queue));
                }
            } else if self.cancellation_token.is_cancelled() {
                tracing::info!(
                    "Cancellation requested, not restarting task queue {} worker {}",
                    worker.queue_name,
                    worker.worker_id
                );
            }
        }
    }

    fn log_worker_end(
        &self,
        worker: &WorkerInfo,
        result: &Result<(), tokio::task::JoinError>,
        restart_workers: bool,
    ) {
        let log_msg_suffix = if restart_workers {
            "Restarting worker"
        } else {
            "Restarting worker disabled"
        };

        match result {
            Ok(()) if !self.cancellation_token.is_cancelled() => tracing::warn!(
                "Task queue {} worker {} finished. {log_msg_suffix}",
                worker.queue_name,
                worker.worker_id
            ),
            Ok(()) => tracing::info!(
                "Task queue {} worker {} finished gracefully after cancellation.",
                worker.queue_name,
                worker.worker_id
            ),
            Err(e) => {
                if e.is_panic() {
                    tracing::error!(
                        ?e,
                        "Task queue {} worker {} panicked. {log_msg_suffix}",
                        worker.queue_name,
                        worker.worker_id
                    );
                } else if e.is_cancelled() {
                    tracing::warn!(
                        ?e,
                        "Task queue {} worker {} was cancelled.",
                        worker.queue_name,
                        worker.worker_id
                    );
                } else {
                    tracing::error!(
                        ?e,
                        "Task queue {} worker {} failed to join. {log_msg_suffix}",
                        worker.queue_name,
                        worker.worker_id
                    );
                }
            }
        }
    }
}

#[cfg(all(test, feature = "router"))]
mod tests {
    use std::{sync::LazyLock, time::Duration};

    use super::*;
    use crate::service::tasks::drain_token;

    static QUEUE_NAME: LazyLock<TaskQueueName> = LazyLock::new(|| TaskQueueName::new("test"));

    #[tokio::test]
    async fn a_drain_fires_the_drain_token_of_the_workers() {
        let worker_fn: TaskQueueWorkerFn =
            Arc::new(|_| Box::pin(async { drain_token().cancelled().await }));
        let runner = TaskQueuesRunner {
            registered_queues: Arc::new(HashMap::from([(
                &*QUEUE_NAME,
                QueueWorkerConfig {
                    worker_fn,
                    num_workers: 1,
                },
            )])),
            cancellation_token: CancellationToken::new(),
            control: ArcTaskRunnerControl::default(),
        };
        let control = ArcTaskRunnerControl::default();
        let runner = runner.with_control(Arc::clone(&control));
        control.stop_picking();
        tokio::time::timeout(Duration::from_secs(5), runner.run_queue_workers(false))
            .await
            .expect("the worker sees the drain");
    }
}
