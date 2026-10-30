//! Draining the task workers of a process after a stop signal, an idle time or a lifetime.

use std::{sync::atomic::Ordering, time::Duration};

use tokio::time::Instant;

use super::TaskRunnerControl;

impl TaskRunnerControl {
    /// Picking stopped, no pick is in flight and no picked task runs.
    pub(crate) fn is_drained(&self) -> bool {
        // `in_pick` before `running`: a pick raises `running` before it lowers `in_pick`.
        self.stop_picking.load(Ordering::SeqCst)
            && self.in_pick.load(Ordering::SeqCst) == 0
            && self.running.load(Ordering::SeqCst) == 0
    }

    /// When the last task ended or was picked, `None` while a task runs.
    pub(crate) fn idle_since(&self) -> Option<Instant> {
        (self.running_tasks() == 0).then(|| *self.lock_last_activity())
    }
}

/// How a process stops its task workers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DrainSettings {
    /// How long a drain started by a signal waits for running tasks.
    pub(crate) drain_timeout: Duration,
    /// Drain and exit after this long without a running or picked task.
    pub(crate) idle_exit_after: Option<Duration>,
    /// Drain and exit after this long.
    pub(crate) max_lifetime: Option<Duration>,
}

impl DrainSettings {
    pub(crate) fn from_config() -> Self {
        Self {
            drain_timeout: crate::CONFIG.task_drain_timeout,
            idle_exit_after: crate::CONFIG.task_idle_exit_after,
            max_lifetime: crate::CONFIG.task_max_lifetime,
        }
    }
}

/// What ended a process's work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StopReason {
    /// SIGTERM, SIGINT or Ctrl+C.
    Signal,
    /// [`DrainSettings::idle_exit_after`] passed without a task.
    Idle,
    /// [`DrainSettings::max_lifetime`] passed.
    Lifetime,
    /// The process was cancelled from inside.
    Cancelled,
}

/// One item per SIGTERM, SIGINT or Ctrl+C this process receives.
///
/// # Panics
/// If a signal handler cannot be installed.
pub(crate) fn stop_signals() -> tokio::sync::mpsc::UnboundedReceiver<()> {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        // Ctrl+C sends SIGINT, so it is not listened to separately.
        let mut terminate = signal(SignalKind::terminate()).expect("Failed to register SIGTERM");
        let mut interrupt = signal(SignalKind::interrupt()).expect("Failed to register SIGINT");
        tokio::spawn(async move {
            loop {
                let name = tokio::select! {
                    Some(()) = terminate.recv() => "SIGTERM",
                    Some(()) = interrupt.recv() => "SIGINT",
                    else => return,
                };
                tracing::info!("Received {name}");
                if tx.send(()).is_err() {
                    return;
                }
            }
        });
    }
    #[cfg(not(unix))]
    tokio::spawn(async move {
        while tokio::signal::ctrl_c().await.is_ok() {
            tracing::info!("Received Ctrl+C");
            if tx.send(()).is_err() {
                return;
            }
        }
    });
    rx
}

/// Waits for a signal, the idle timer or the lifetime timer, then drains: workers stop picking,
/// their running tasks see their drain token cancelled, and this returns once no picked task
/// runs. A drain after a signal waits at most
/// `drain_timeout`; with a zero timeout, workers stop picking and this returns at once. A drain after a timer waits without a deadline
/// until a signal caps it at `drain_timeout` from then. A second signal or a cancellation of
/// `cancellation_token` returns at once.
pub(crate) async fn wait_for_stop_and_drain(
    control: &TaskRunnerControl,
    settings: DrainSettings,
    signals: &mut tokio::sync::mpsc::UnboundedReceiver<()>,
    cancellation_token: &crate::CancellationToken,
) -> StopReason {
    let lifetime_end = settings.max_lifetime.map(|d| Instant::now() + d);
    let reason = tokio::select! {
        Some(()) = signals.recv() => StopReason::Signal,
        () = cancellation_token.cancelled() => StopReason::Cancelled,
        () = sleep_until(lifetime_end) => StopReason::Lifetime,
        () = wait_until_idle(control, settings.idle_exit_after) => StopReason::Idle,
    };
    match reason {
        StopReason::Signal => tracing::info!("Shutdown signal received"),
        StopReason::Idle => tracing::info!("No task ran for the idle exit time, stopping"),
        StopReason::Lifetime => tracing::info!("Max lifetime reached, stopping"),
        StopReason::Cancelled => {
            tracing::info!("Shutdown handler terminating due to external cancellation");
            return reason;
        }
    }
    control.stop_picking();
    if reason == StopReason::Signal && settings.drain_timeout.is_zero() {
        return reason;
    }

    let mut signalled = reason == StopReason::Signal;
    let mut deadline = signalled.then(|| Instant::now() + settings.drain_timeout);
    tracing::info!(
        ?reason,
        running_tasks = control.running_tasks(),
        timeout_seconds = deadline.map(|d| (d - Instant::now()).as_secs()),
        "Draining task workers: picking no new tasks and waiting for running ones to finish"
    );
    let mut progress = tokio::time::interval_at(
        Instant::now() + DRAIN_PROGRESS_INTERVAL,
        DRAIN_PROGRESS_INTERVAL,
    );
    loop {
        let changed = control.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        if control.is_drained() {
            tracing::info!(?reason, "Task workers drained");
            return reason;
        }
        tokio::select! {
            () = changed => {}
            Some(()) = signals.recv() => {
                if signalled {
                    tracing::info!("Second stop signal: stopping the task workers now");
                    return reason;
                }
                signalled = true;
                let capped = Instant::now() + settings.drain_timeout;
                deadline = Some(deadline.map_or(capped, |d| d.min(capped)));
                tracing::info!(
                    timeout_seconds = settings.drain_timeout.as_secs(),
                    "Stop signal during the drain: the task workers stop after the drain timeout"
                );
            }
            () = sleep_until(deadline) => {
                tracing::warn!(
                    running_tasks = control.running_tasks(),
                    "Drain timeout reached: stopping the task workers"
                );
                return reason;
            }
            () = cancellation_token.cancelled() => {
                tracing::info!("Shutdown handler terminating due to external cancellation");
                return StopReason::Cancelled;
            }
            _ = progress.tick() => tracing::info!(
                running_tasks = control.running_tasks(),
                seconds_left = deadline.map(|d| d.saturating_duration_since(Instant::now()).as_secs()),
                "Draining task workers"
            ),
        }
    }
}

const DRAIN_PROGRESS_INTERVAL: Duration = Duration::from_secs(30);
/// How often an idle timer looks again while a task runs.
const IDLE_RECHECK_INTERVAL: Duration = Duration::from_secs(1);

async fn sleep_until(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

async fn wait_until_idle(control: &TaskRunnerControl, after: Option<Duration>) {
    let Some(after) = after else {
        return std::future::pending().await;
    };
    loop {
        match control.idle_since() {
            Some(since) if since + after <= Instant::now() => return,
            Some(since) => tokio::time::sleep_until(since + after).await,
            None => tokio::time::sleep(IDLE_RECHECK_INTERVAL).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{
        super::{ArcTaskRunnerControl, admit_pick, end_task, run_as_worker},
        *,
    };
    use crate::CancellationToken;

    fn settings(
        drain_timeout: u64,
        idle_exit_after: Option<u64>,
        max_lifetime: Option<u64>,
    ) -> DrainSettings {
        DrainSettings {
            drain_timeout: Duration::from_secs(drain_timeout),
            idle_exit_after: idle_exit_after.map(Duration::from_secs),
            max_lifetime: max_lifetime.map(Duration::from_secs),
        }
    }

    /// A worker that picks a task when `work` has one and runs it for that many seconds.
    fn spawn_worker(
        control: &ArcTaskRunnerControl,
        mut work: tokio::sync::mpsc::UnboundedReceiver<u64>,
        cancellation_token: CancellationToken,
        finished: tokio::sync::mpsc::UnboundedSender<u64>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(run_as_worker(Arc::clone(control), async move {
            loop {
                end_task();
                let Some(permit) = admit_pick() else {
                    tokio::select! {
                        () = cancellation_token.cancelled() => return,
                        () = tokio::time::sleep(Duration::from_secs(1)) => continue,
                    }
                };
                let task = work.try_recv().ok();
                let Some(seconds) = task else {
                    drop(permit);
                    tokio::select! {
                        () = cancellation_token.cancelled() => return,
                        () = tokio::time::sleep(Duration::from_secs(1)) => continue,
                    }
                };
                permit.task_picked();
                tokio::select! {
                    () = cancellation_token.cancelled() => return,
                    () = tokio::time::sleep(Duration::from_secs(seconds)) => {
                        finished.send(seconds).unwrap();
                    }
                }
            }
        }))
    }

    struct Harness {
        control: ArcTaskRunnerControl,
        work: tokio::sync::mpsc::UnboundedSender<u64>,
        finished: tokio::sync::mpsc::UnboundedReceiver<u64>,
        signals: tokio::sync::mpsc::UnboundedSender<()>,
        signal_rx: Option<tokio::sync::mpsc::UnboundedReceiver<()>>,
        token: CancellationToken,
        created: Instant,
    }

    impl Harness {
        fn new() -> Self {
            let control = ArcTaskRunnerControl::default();
            let (work, work_rx) = tokio::sync::mpsc::unbounded_channel();
            let (finished_tx, finished) = tokio::sync::mpsc::unbounded_channel();
            let (signals, signal_rx) = tokio::sync::mpsc::unbounded_channel();
            let token = CancellationToken::new();
            spawn_worker(&control, work_rx, token.clone(), finished_tx);
            Self {
                control,
                work,
                finished,
                signals,
                signal_rx: Some(signal_rx),
                token,
                created: Instant::now(),
            }
        }

        /// Starts the drain; resolves to the reason and the whole seconds from the harness's
        /// creation to the drain's end.
        fn start(&mut self, settings: DrainSettings) -> tokio::task::JoinHandle<(StopReason, u64)> {
            let control = Arc::clone(&self.control);
            let mut signals = self.signal_rx.take().unwrap();
            let token = self.token.clone();
            let created = self.created;
            tokio::spawn(async move {
                let reason =
                    wait_for_stop_and_drain(&control, settings, &mut signals, &token).await;
                (reason, (Instant::now() - created).as_secs())
            })
        }
    }

    /// Starts a drain of `control`; resolves to the reason and the whole seconds from the start
    /// to the drain's end.
    fn start_drain(
        control: &ArcTaskRunnerControl,
        settings: DrainSettings,
    ) -> (
        tokio::task::JoinHandle<(StopReason, u64)>,
        tokio::sync::mpsc::UnboundedSender<()>,
        CancellationToken,
    ) {
        let control = Arc::clone(control);
        let (signals, mut signal_rx) = tokio::sync::mpsc::unbounded_channel();
        let token = CancellationToken::new();
        let drain_token = token.clone();
        let drain = tokio::spawn(async move {
            let start = Instant::now();
            let reason =
                wait_for_stop_and_drain(&control, settings, &mut signal_rx, &drain_token).await;
            (reason, (Instant::now() - start).as_secs())
        });
        (drain, signals, token)
    }

    #[tokio::test(start_paused = true)]
    async fn a_signal_with_zero_drain_timeout_returns_at_once() {
        let mut h = Harness::new();
        h.work.send(100).unwrap();
        tokio::time::sleep(Duration::from_secs(2)).await;
        let drain = h.start(settings(0, None, None));
        h.signals.send(()).unwrap();
        assert_eq!(drain.await.unwrap(), (StopReason::Signal, 2));
        assert_eq!(h.control.running_tasks(), 1);
        assert!(h.control.stop_picking.load(Ordering::SeqCst));
    }

    #[tokio::test(start_paused = true)]
    async fn a_signal_drain_waits_for_the_running_task() {
        let mut h = Harness::new();
        h.work.send(30).unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
        let drain = h.start(settings(60, None, None));
        h.signals.send(()).unwrap();
        // The task was picked at 0 s and ends at 30 s, before the first progress tick at 31 s.
        assert_eq!(drain.await.unwrap(), (StopReason::Signal, 30));
        assert_eq!(h.finished.try_recv().unwrap(), 30);
        assert!(h.control.drain.is_cancelled());
        // A task queued during the drain is not picked.
        h.work.send(5).unwrap();
        tokio::time::sleep(Duration::from_secs(10)).await;
        assert_eq!(h.control.running_tasks(), 0);
        assert!(h.finished.try_recv().is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn a_signal_drain_ends_at_the_drain_timeout() {
        let mut h = Harness::new();
        h.work.send(300).unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;
        let drain = h.start(settings(60, None, None));
        h.signals.send(()).unwrap();
        assert_eq!(drain.await.unwrap(), (StopReason::Signal, 60));
        assert_eq!(h.control.running_tasks(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_second_signal_ends_the_drain_at_once() {
        let mut h = Harness::new();
        h.work.send(300).unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;
        let drain = h.start(settings(600, None, None));
        h.signals.send(()).unwrap();
        tokio::time::sleep(Duration::from_secs(10)).await;
        h.signals.send(()).unwrap();
        assert_eq!(drain.await.unwrap(), (StopReason::Signal, 10));
    }

    #[tokio::test(start_paused = true)]
    async fn idle_exit_drains_without_tasks() {
        let mut h = Harness::new();
        let drain = h.start(settings(0, Some(180), None));
        assert_eq!(drain.await.unwrap(), (StopReason::Idle, 180));
        assert!(h.control.is_drained());
        assert!(h.control.drain.is_cancelled());
    }

    #[tokio::test(start_paused = true)]
    async fn idle_time_restarts_when_a_task_ends() {
        let mut h = Harness::new();
        let drain = h.start(settings(0, Some(180), None));
        tokio::time::sleep(Duration::from_millis(100_500)).await;
        h.work.send(50).unwrap();
        // Picked at 101 s, ends at 151 s, idle until 331 s.
        let (reason, seconds) = drain.await.unwrap();
        assert_eq!(reason, StopReason::Idle);
        assert_eq!(seconds, 331);
        assert_eq!(h.finished.try_recv().unwrap(), 50);
    }

    #[tokio::test(start_paused = true)]
    async fn a_task_picked_at_the_idle_edge_runs_to_its_end() {
        let control = ArcTaskRunnerControl::default();
        // A pick is in flight when the idle timer fires.
        let (pick_tx, pick_rx) = tokio::sync::oneshot::channel::<()>();
        let (ended_tx, mut ended_rx) = tokio::sync::mpsc::unbounded_channel();
        let worker = tokio::spawn(run_as_worker(Arc::clone(&control), async move {
            let permit = admit_pick().expect("the gate is open before the timer fires");
            pick_rx.await.unwrap();
            permit.task_picked();
            tokio::time::sleep(Duration::from_secs(40)).await;
            ended_tx.send(()).unwrap();
            end_task();
            assert!(admit_pick().is_none());
        }));
        let (drain, _signals, _token) = start_drain(&control, settings(0, Some(10), None));
        tokio::time::sleep(Duration::from_secs(11)).await;
        assert!(!drain.is_finished());
        pick_tx.send(()).unwrap();
        assert_eq!(drain.await.unwrap(), (StopReason::Idle, 51));
        assert_eq!(ended_rx.try_recv(), Ok(()));
        worker.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn a_lifetime_drain_waits_without_deadline() {
        let mut h = Harness::new();
        h.work.send(3000).unwrap();
        let drain = h.start(settings(0, None, Some(600)));
        // Picked at about 0 s, ends at 3000 s.
        assert_eq!(drain.await.unwrap(), (StopReason::Lifetime, 3000));
        assert!(h.control.drain.is_cancelled());
    }

    /// A task that watches its drain token ends when the lifetime passes, so the drain does
    /// not wait for the task's own end.
    #[tokio::test(start_paused = true)]
    async fn a_task_that_watches_the_drain_token_ends_a_lifetime_drain_early() {
        let control = ArcTaskRunnerControl::default();
        let (ended_tx, mut ended_rx) = tokio::sync::mpsc::unbounded_channel();
        let worker = tokio::spawn(run_as_worker(Arc::clone(&control), async move {
            admit_pick().expect("the gate is open").task_picked();
            let drain = super::super::drain_token();
            tokio::select! {
                () = drain.cancelled() => ended_tx.send("drained").unwrap(),
                () = tokio::time::sleep(Duration::from_mins(50)) => {
                    ended_tx.send("finished").unwrap();
                }
            }
            end_task();
        }));
        let (drain, _signals, _token) = start_drain(&control, settings(0, None, Some(600)));
        assert_eq!(drain.await.unwrap(), (StopReason::Lifetime, 600));
        assert_eq!(ended_rx.try_recv(), Ok("drained"));
        worker.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn a_signal_caps_a_lifetime_drain_at_the_drain_timeout() {
        let mut h = Harness::new();
        h.work.send(3000).unwrap();
        let drain = h.start(settings(20, None, Some(600)));
        tokio::time::sleep(Duration::from_secs(1000)).await;
        h.signals.send(()).unwrap();
        assert_eq!(drain.await.unwrap(), (StopReason::Lifetime, 1020));
        assert_eq!(h.control.running_tasks(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_signal_with_zero_drain_timeout_ends_a_lifetime_drain_at_once() {
        let mut h = Harness::new();
        h.work.send(3000).unwrap();
        let drain = h.start(settings(0, None, Some(600)));
        tokio::time::sleep(Duration::from_secs(1000)).await;
        h.signals.send(()).unwrap();
        assert_eq!(drain.await.unwrap(), (StopReason::Lifetime, 1000));
    }

    #[tokio::test(start_paused = true)]
    async fn cancellation_ends_the_wait() {
        let mut h = Harness::new();
        let drain = h.start(settings(0, None, None));
        tokio::time::sleep(Duration::from_secs(5)).await;
        h.token.cancel();
        assert_eq!(drain.await.unwrap(), (StopReason::Cancelled, 5));
        assert!(!h.control.drain.is_cancelled());
    }

    #[tokio::test(start_paused = true)]
    async fn cancellation_ends_the_drain() {
        let control = ArcTaskRunnerControl::default();
        // Like the built-in workers, this one does not watch the token while its task runs.
        let _worker = tokio::spawn(run_as_worker(Arc::clone(&control), async {
            admit_pick().expect("the gate is open").task_picked();
            std::future::pending::<()>().await;
        }));
        let (drain, signals, token) = start_drain(&control, settings(600, None, None));
        tokio::time::sleep(Duration::from_secs(1)).await;
        signals.send(()).unwrap();
        tokio::time::sleep(Duration::from_secs(4)).await;
        token.cancel();
        assert_eq!(drain.await.unwrap(), (StopReason::Cancelled, 5));
    }

    #[tokio::test(start_paused = true)]
    async fn a_panicking_worker_ends_its_task() {
        let control = ArcTaskRunnerControl::default();
        let worker = tokio::spawn(run_as_worker(Arc::clone(&control), async {
            admit_pick().expect("the gate is open").task_picked();
            panic!("the task panicked");
        }));
        assert!(worker.await.unwrap_err().is_panic());
        assert_eq!(control.running_tasks(), 0);
        control.stop_picking();
        assert!(control.is_drained());
    }

    #[tokio::test(start_paused = true)]
    async fn an_empty_pick_in_flight_ends_the_drain() {
        let control = ArcTaskRunnerControl::default();
        let (pick_tx, pick_rx) = tokio::sync::oneshot::channel::<()>();
        let _worker = tokio::spawn(run_as_worker(Arc::clone(&control), async move {
            let permit = admit_pick().expect("the gate is open");
            pick_rx.await.unwrap();
            // The pick returned no task.
            drop(permit);
            std::future::pending::<()>().await;
        }));
        let (drain, signals, _token) = start_drain(&control, settings(600, None, None));
        tokio::time::sleep(Duration::from_secs(1)).await;
        signals.send(()).unwrap();
        tokio::time::sleep(Duration::from_secs(4)).await;
        pick_tx.send(()).unwrap();
        assert_eq!(drain.await.unwrap(), (StopReason::Signal, 5));
    }

    #[tokio::test(start_paused = true)]
    async fn a_worker_that_panics_during_the_drain_ends_the_drain() {
        let control = ArcTaskRunnerControl::default();
        let (panic_tx, panic_rx) = tokio::sync::oneshot::channel::<()>();
        let _worker = tokio::spawn(run_as_worker(Arc::clone(&control), async move {
            admit_pick().expect("the gate is open").task_picked();
            panic_rx.await.unwrap();
            panic!("the task panicked");
        }));
        let (drain, signals, _token) = start_drain(&control, settings(600, None, None));
        tokio::time::sleep(Duration::from_secs(1)).await;
        signals.send(()).unwrap();
        tokio::time::sleep(Duration::from_secs(4)).await;
        panic_tx.send(()).unwrap();
        assert_eq!(drain.await.unwrap(), (StopReason::Signal, 5));
    }

    #[test]
    fn picks_outside_a_runner_are_not_gated() {
        let permit = admit_pick().expect("outside a runner the gate is open");
        assert!(permit.control.is_none());
    }
}
