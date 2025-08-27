use std::{collections::HashMap, sync::Arc};

use futures::future::BoxFuture;

use crate::CancellationToken;

/// Infinitely running task worker loop function that polls tasks from a queue and
/// processes. Accepts a cancellation token for graceful shutdown.
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
    pub(super) registered_queues: Arc<HashMap<&'static str, QueueWorkerConfig>>,
    pub(super) cancellation_token: CancellationToken,
}

impl TaskQueuesRunner {
    /// Runs all registered task queue workers and monitors them, restarting any that exit.
    /// Accepts a cancellation token for graceful shutdown.
    pub async fn run_queue_workers(self, restart_workers: bool) {
        // Create a structure to track worker information and hold task handles
        struct WorkerInfo {
            queue_name: &'static str,
            worker_id: usize,
            handle: tokio::task::JoinHandle<()>,
        }

        let mut workers = Vec::new();
        let registered_queues = Arc::clone(&self.registered_queues);

        // Initialize all workers
        for (queue_name, queue) in registered_queues.iter() {
            tracing::info!(
                "Starting {} workers for task queue `{queue_name}`.",
                queue.num_workers
            );

            for worker_id in 0..queue.num_workers {
                let task_fn = Arc::clone(&queue.worker_fn);
                let cancellation_token_clone = self.cancellation_token.clone();
                tracing::debug!(
                    "Starting `{queue_name}` worker {worker_id}/{}",
                    queue.num_workers
                );
                workers.push(WorkerInfo {
                    queue_name,
                    worker_id,
                    handle: tokio::task::spawn(task_fn(cancellation_token_clone)),
                });
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

            let log_msg_suffix = if restart_workers {
                "Restarting worker"
            } else {
                "Restarting worker disabled"
            };

            // Log the result
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
                Err(e) => tracing::error!(
                    ?e,
                    "Task queue {} worker {} panicked: {e}. {log_msg_suffix}",
                    worker.queue_name,
                    worker.worker_id
                ),
            }

            // Restart the worker only if cancellation hasn't been requested
            if restart_workers && !self.cancellation_token.is_cancelled() {
                if let Some(queue) = registered_queues.get(worker.queue_name) {
                    let task_fn = Arc::clone(&queue.worker_fn);
                    let cancellation_token_clone = self.cancellation_token.clone();
                    tracing::debug!(
                        "Restarting task queue {} worker {}",
                        worker.queue_name,
                        worker.worker_id
                    );
                    workers.push(WorkerInfo {
                        queue_name: worker.queue_name,
                        worker_id: worker.worker_id,
                        handle: tokio::task::spawn(task_fn(cancellation_token_clone)),
                    });
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
}
