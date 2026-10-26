//! Stopping the task workers of a process gracefully.
//!
//! A drain closes a gate that every pick checks, waits until no picked task runs, and only then
//! lets the process cancel its workers. A worker counts as running a task from the pick that
//! returned it until it picks or polls again, or ends.

use std::{
    cell::Cell,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use tokio::{sync::Notify, time::Instant};

/// Reference to [`TaskRunnerControl`] that can be cheaply cloned and shared.
pub(crate) type ArcTaskRunnerControl = Arc<TaskRunnerControl>;

/// Shared by the workers of one runner and the drain.
#[derive(Debug)]
pub(crate) struct TaskRunnerControl {
    stop_picking: AtomicBool,
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
    /// Picks return nothing more; polling workers wait until they are cancelled.
    pub(crate) fn stop_picking(&self) {
        self.stop_picking.store(true, Ordering::SeqCst);
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

    /// Closes the gate: the workers pick nothing more.
    pub fn stop_picking(&self) {
        self.control.stop_picking();
    }
}

#[cfg(feature = "router")]
mod stop;
#[cfg(feature = "router")]
pub(crate) use stop::{DrainSettings, stop_signals, wait_for_stop_and_drain};
