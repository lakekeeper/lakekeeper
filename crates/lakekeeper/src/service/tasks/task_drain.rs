//! Stopping the task workers of a process gracefully.
//!
//! A drain closes a gate that every pick checks, fires [`drain_token`], waits until no picked
//! task runs, and only then lets the process cancel its workers. A worker counts as running a
//! task from the pick that returned it until it picks or polls again, or ends.

use std::{
    cell::Cell,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use tokio::{sync::Notify, time::Instant};

use crate::CancellationToken;

/// Reference to [`TaskRunnerControl`] that can be cheaply cloned and shared.
pub(crate) type ArcTaskRunnerControl = Arc<TaskRunnerControl>;

/// Shared by the workers of one runner and the drain.
#[derive(Debug)]
pub(crate) struct TaskRunnerControl {
    stop_picking: AtomicBool,
    /// Cancelled with the gate; see [`drain_token`].
    drain: CancellationToken,
    in_pick: AtomicUsize,
    running: AtomicUsize,
    /// Start, each pick that returned a task, and each task end.
    last_activity: Mutex<Instant>,
    changed: Notify,
}

impl Default for TaskRunnerControl {
    fn default() -> Self {
        Self {
            stop_picking: AtomicBool::new(false),
            drain: CancellationToken::new(),
            in_pick: AtomicUsize::new(0),
            running: AtomicUsize::new(0),
            last_activity: Mutex::new(Instant::now()),
            changed: Notify::new(),
        }
    }
}

impl TaskRunnerControl {
    fn lock_last_activity(&self) -> std::sync::MutexGuard<'_, Instant> {
        self.last_activity
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn task_started(&self) {
        self.running.fetch_add(1, Ordering::SeqCst);
        *self.lock_last_activity() = Instant::now();
    }

    fn task_ended(&self) {
        // Set before the counter drops: an idle timer that sees no running task reads this end.
        *self.lock_last_activity() = Instant::now();
        self.running.fetch_sub(1, Ordering::SeqCst);
        self.changed.notify_waiters();
    }
}

#[cfg(any(feature = "router", feature = "test-utils"))]
impl TaskRunnerControl {
    /// Picks return nothing more and [`drain_token`] fires; polling workers wait until they are
    /// cancelled.
    pub(crate) fn stop_picking(&self) {
        self.stop_picking.store(true, Ordering::SeqCst);
        self.drain.cancel();
        self.changed.notify_waiters();
    }

    /// Tasks the workers picked and have not ended.
    pub(crate) fn running_tasks(&self) -> usize {
        self.running.load(Ordering::SeqCst)
    }
}

tokio::task_local! {
    static WORKER: WorkerSlot;
}

/// One worker of a runner. Ends the worker's task when the worker ends or panics.
struct WorkerSlot {
    control: ArcTaskRunnerControl,
    busy: Cell<bool>,
}

impl Drop for WorkerSlot {
    fn drop(&mut self) {
        if self.busy.get() {
            self.control.task_ended();
        }
    }
}

/// Runs `worker` as a worker of the runner `control` belongs to.
pub(crate) fn run_as_worker<F: Future>(
    control: ArcTaskRunnerControl,
    worker: F,
) -> impl Future<Output = F::Output> {
    WORKER.scope(
        WorkerSlot {
            control,
            busy: Cell::new(false),
        },
        worker,
    )
}

/// A pick in flight, counted until dropped. Counts nothing outside a runner.
pub(crate) struct PickPermit {
    control: Option<ArcTaskRunnerControl>,
}

impl PickPermit {
    /// The pick returned a task: the worker runs it until it picks or polls again.
    pub(crate) fn task_picked(self) {
        let Some(control) = &self.control else {
            return;
        };
        let was_busy = WORKER.try_with(|slot| slot.busy.replace(true));
        debug_assert!(
            was_busy.is_ok(),
            "a counted permit exists only inside a worker"
        );
        if was_busy == Ok(false) {
            control.task_started();
        }
    }
}

impl Drop for PickPermit {
    fn drop(&mut self) {
        if let Some(control) = &self.control {
            control.in_pick.fetch_sub(1, Ordering::SeqCst);
            control.changed.notify_waiters();
        }
    }
}

/// Called when a worker picks or polls: the task it picked before has ended.
pub(crate) fn end_task() {
    let _outside_a_runner = WORKER.try_with(|slot| {
        if slot.busy.replace(false) {
            slot.control.task_ended();
        }
    });
}

/// Called before each pick. Returns `None` if the runner of this worker stopped picking. The
/// pick is counted before the gate is read, so a drain either sees it or the worker sees the
/// closed gate.
pub(crate) fn admit_pick() -> Option<PickPermit> {
    WORKER
        .try_with(|slot| {
            let control = &slot.control;
            control.in_pick.fetch_add(1, Ordering::SeqCst);
            let permit = PickPermit {
                control: Some(Arc::clone(control)),
            };
            (!control.stop_picking.load(Ordering::SeqCst)).then_some(permit)
        })
        .unwrap_or(Some(PickPermit { control: None }))
}

/// Cancelled when the runner of the calling worker starts to drain: a stop signal, the idle time
/// or the lifetime. Cancelling it affects only the caller's token.
///
/// Call it inside the future of a [`TaskQueueWorkerFn`](super::TaskQueueWorkerFn), not in the
/// closure that builds it. Elsewhere (a spawned task, outside a runner) the token never fires.
#[must_use]
pub fn drain_token() -> CancellationToken {
    WORKER
        .try_with(|slot| slot.control.drain.child_token())
        .unwrap_or_default()
}

/// A task runner for tests outside this crate: runs futures as its workers.
#[cfg(feature = "test-utils")]
#[derive(Debug, Clone, Default)]
pub struct TestTaskRunner {
    control: ArcTaskRunnerControl,
}

#[cfg(feature = "test-utils")]
impl TestTaskRunner {
    /// Runs `worker` as a worker of this runner.
    pub fn run_as_worker<F: Future>(&self, worker: F) -> impl Future<Output = F::Output> {
        run_as_worker(Arc::clone(&self.control), worker)
    }

    /// Tasks the workers picked and have not ended.
    #[must_use]
    pub fn running_tasks(&self) -> usize {
        self.control.running_tasks()
    }

    /// Closes the gate and fires [`drain_token`]: the workers pick nothing more.
    pub fn stop_picking(&self) {
        self.control.stop_picking();
    }
}

#[cfg(feature = "router")]
mod stop;
#[cfg(feature = "router")]
pub(crate) use stop::{DrainSettings, stop_signals, wait_for_stop_and_drain};

#[cfg(all(test, any(feature = "router", feature = "test-utils")))]
mod tests {
    use super::*;

    #[tokio::test]
    async fn the_drain_token_fires_when_the_runner_stops_picking() {
        let control = ArcTaskRunnerControl::default();
        let worker_control = Arc::clone(&control);
        run_as_worker(control, async move {
            let token = drain_token();
            assert!(!token.is_cancelled());
            worker_control.stop_picking();
            assert!(token.is_cancelled());
            // A token taken after the drain started is cancelled too.
            assert!(drain_token().is_cancelled());
        })
        .await;
    }

    #[tokio::test]
    async fn a_task_that_cancels_its_drain_token_does_not_drain_the_runner() {
        let control = ArcTaskRunnerControl::default();
        let worker_control = Arc::clone(&control);
        run_as_worker(control, async move {
            drain_token().cancel();
            assert!(!drain_token().is_cancelled());
            assert!(!worker_control.drain.is_cancelled());
        })
        .await;
    }

    #[tokio::test]
    async fn a_task_spawned_from_a_worker_gets_a_token_that_never_fires() {
        let control = ArcTaskRunnerControl::default();
        let worker_control = Arc::clone(&control);
        run_as_worker(control, async move {
            let spawned = tokio::spawn(async { drain_token() }).await.unwrap();
            let own = drain_token();
            worker_control.stop_picking();
            assert!(own.is_cancelled());
            assert!(!spawned.is_cancelled());
        })
        .await;
    }
}
