//! Stopping the task workers of a process gracefully.
//!
//! A drain closes a gate that workers check before each pick, waits until no picked task runs,
//! and only then lets the process cancel its workers. A worker counts as running a task from the
//! pick that returned it until it polls again or ends.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
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
        *self.lock_last_activity() = Instant::now();
        self.running.fetch_sub(1, Ordering::SeqCst);
        self.changed.notify_waiters();
    }
}

tokio::task_local! {
    static WORKER: WorkerSlot;
}

/// One worker of a runner. Ends the worker's task when the worker ends or panics.
struct WorkerSlot {
    control: ArcTaskRunnerControl,
    busy: AtomicBool,
}

impl Drop for WorkerSlot {
    fn drop(&mut self) {
        if *self.busy.get_mut() {
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
            busy: AtomicBool::new(false),
        },
        worker,
    )
}

/// Whether a worker may pick.
pub(crate) enum PickAdmission {
    /// Pick, then report a picked task with [`PickPermit::task_picked`]. `None` outside a runner.
    Open(Option<PickPermit>),
    /// The runner stopped picking.
    Closed,
}

/// A pick in flight, counted until dropped.
pub(crate) struct PickPermit {
    control: ArcTaskRunnerControl,
}

impl PickPermit {
    /// The pick returned a task: the worker runs it until it polls again.
    pub(crate) fn task_picked(self) {
        let was_busy = WORKER.try_with(|slot| slot.busy.swap(true, Ordering::SeqCst));
        debug_assert!(was_busy.is_ok(), "a permit exists only inside a worker");
        if was_busy == Ok(false) {
            self.control.task_started();
        }
    }
}

impl Drop for PickPermit {
    fn drop(&mut self) {
        self.control.in_pick.fetch_sub(1, Ordering::SeqCst);
        self.control.changed.notify_waiters();
    }
}

/// Called when a worker polls for a task: the task it picked before has ended.
pub(crate) fn end_task() {
    let _outside_a_runner = WORKER.try_with(|slot| {
        if slot.busy.swap(false, Ordering::SeqCst) {
            slot.control.task_ended();
        }
    });
}

/// Called by a worker before each pick. Admits the pick unless its runner stopped picking. The
/// pick is counted before the gate is read, so a drain either sees it or the worker sees the
/// closed gate.
pub(crate) fn admit_pick() -> PickAdmission {
    WORKER
        .try_with(|slot| {
            let control = &slot.control;
            control.in_pick.fetch_add(1, Ordering::SeqCst);
            let permit = PickPermit {
                control: Arc::clone(control),
            };
            if control.stop_picking.load(Ordering::SeqCst) {
                drop(permit);
                PickAdmission::Closed
            } else {
                PickAdmission::Open(Some(permit))
            }
        })
        .unwrap_or(PickAdmission::Open(None))
}

/// A task runner for tests outside this crate: runs futures as its workers and drains them.
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
        self.control.running.load(Ordering::SeqCst)
    }

    /// Closes the gate: the workers pick nothing more.
    pub fn stop_picking(&self) {
        self.control.stop_picking.store(true, Ordering::SeqCst);
    }
}

#[cfg(feature = "router")]
mod stop;
#[cfg(feature = "router")]
pub(crate) use stop::{DrainSettings, StopReason, stop_signals, wait_for_stop_and_drain};
