//! Draining the task workers of a process after a stop signal, an idle time or a lifetime.

use std::{sync::atomic::Ordering, time::Duration};

use tokio::time::Instant;

use super::TaskRunnerControl;

impl TaskRunnerControl {
    /// Workers pick nothing more; they wait in their poll until they are cancelled.
    pub(crate) fn stop_picking(&self) {
        self.stop_picking.store(true, Ordering::SeqCst);
        self.changed.notify_waiters();
    }

    /// Picking stopped, no pick is in flight and no picked task runs.
    pub(crate) fn is_drained(&self) -> bool {
        self.stop_picking.load(Ordering::SeqCst)
            && self.in_pick.load(Ordering::SeqCst) == 0
            && self.running.load(Ordering::SeqCst) == 0
    }

    pub(crate) fn running_tasks(&self) -> usize {
        self.running.load(Ordering::SeqCst)
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

    /// Idle exit and max lifetime would stop the HTTP API with the workers.
    ///
    /// # Errors
    /// If either is set while the process serves the HTTP API, or is zero.
    pub(crate) fn validate(&self, serve_http_api: bool) -> anyhow::Result<()> {
        for (name, value) in [
            ("LAKEKEEPER__TASK_IDLE_EXIT_AFTER", self.idle_exit_after),
            ("LAKEKEEPER__TASK_MAX_LIFETIME", self.max_lifetime),
        ] {
            let Some(value) = value else { continue };
            anyhow::ensure!(
                !serve_http_api,
                "{name} is set, but this process serves the HTTP API, which would stop with \
                 the workers. Set it only on headless workers (LAKEKEEPER__SERVE_HTTP_API=false)."
            );
            anyhow::ensure!(
                !value.is_zero(),
                "{name} must be greater than 0. Leave it unset to turn it off."
            );
        }
        Ok(())
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
/// and this returns once no picked task runs. A drain after a signal waits at most
/// `drain_timeout`, and a zero timeout skips it. A drain after a timer waits without a deadline
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
        () = cancellation_token.cancelled() => return StopReason::Cancelled,
        () = sleep_until(lifetime_end) => StopReason::Lifetime,
        () = wait_until_idle(control, settings.idle_exit_after) => StopReason::Idle,
    };
    if reason == StopReason::Signal && settings.drain_timeout.is_zero() {
        return reason;
    }

    control.stop_picking();
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
                    tracing::info!("Second stop signal: cancelling running tasks now");
                    return reason;
                }
                signalled = true;
                let capped = Instant::now() + settings.drain_timeout;
                deadline = Some(deadline.map_or(capped, |d| d.min(capped)));
                tracing::info!(
                    timeout_seconds = settings.drain_timeout.as_secs(),
                    "Stop signal during the drain: running tasks are cancelled after the drain timeout"
                );
            }
            () = sleep_until(deadline) => {
                tracing::warn!(
                    running_tasks = control.running_tasks(),
                    "Drain timeout reached: cancelling running tasks"
                );
                return reason;
            }
            () = cancellation_token.cancelled() => return StopReason::Cancelled,
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
        super::{ArcTaskRunnerControl, PickAdmission, admit_pick, end_task, run_as_worker},
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
                let permit = match admit_pick() {
                    PickAdmission::Open(permit) => permit.expect("inside a runner"),
                    PickAdmission::Closed => {
                        tokio::select! {
                            () = cancellation_token.cancelled() => return,
                            () = tokio::time::sleep(Duration::from_secs(1)) => continue,
                        }
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

    #[tokio::test(start_paused = true)]
    async fn a_signal_with_zero_drain_timeout_returns_at_once() {
        let mut h = Harness::new();
        h.work.send(100).unwrap();
        tokio::time::sleep(Duration::from_secs(2)).await;
        let drain = h.start(settings(0, None, None));
        h.signals.send(()).unwrap();
        assert_eq!(drain.await.unwrap(), (StopReason::Signal, 2));
        assert_eq!(h.control.running_tasks(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_signal_drain_waits_for_the_running_task() {
        let mut h = Harness::new();
        h.work.send(30).unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;
        let drain = h.start(settings(60, None, None));
        h.signals.send(()).unwrap();
        // The task was picked at about 0 s and ends at 30 s; the worker then polls again.
        assert_eq!(drain.await.unwrap(), (StopReason::Signal, 30));
        assert_eq!(h.finished.try_recv().unwrap(), 30);
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
        let token = CancellationToken::new();
        let (_signals, mut signal_rx) = tokio::sync::mpsc::unbounded_channel();
        // A pick is in flight when the idle timer fires.
        let (pick_tx, pick_rx) = tokio::sync::oneshot::channel::<()>();
        let (ended_tx, mut ended_rx) = tokio::sync::mpsc::unbounded_channel();
        let worker = tokio::spawn(run_as_worker(Arc::clone(&control), async move {
            let PickAdmission::Open(Some(permit)) = admit_pick() else {
                panic!("the gate is open before the timer fires")
            };
            pick_rx.await.unwrap();
            permit.task_picked();
            tokio::time::sleep(Duration::from_secs(40)).await;
            ended_tx.send(()).unwrap();
            end_task();
            assert!(matches!(admit_pick(), PickAdmission::Closed));
        }));
        let drain = {
            let control = Arc::clone(&control);
            let token = token.clone();
            tokio::spawn(async move {
                let start = Instant::now();
                let reason = wait_for_stop_and_drain(
                    &control,
                    settings(0, Some(10), None),
                    &mut signal_rx,
                    &token,
                )
                .await;
                (reason, (Instant::now() - start).as_secs())
            })
        };
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
    async fn cancellation_ends_the_wait_and_the_drain() {
        let mut h = Harness::new();
        let drain = h.start(settings(0, None, None));
        tokio::time::sleep(Duration::from_secs(5)).await;
        h.token.cancel();
        assert_eq!(drain.await.unwrap(), (StopReason::Cancelled, 5));
    }

    #[tokio::test(start_paused = true)]
    async fn a_panicking_worker_ends_its_task() {
        let control = ArcTaskRunnerControl::default();
        let worker = tokio::spawn(run_as_worker(Arc::clone(&control), async {
            let PickAdmission::Open(Some(permit)) = admit_pick() else {
                panic!("the gate is open")
            };
            permit.task_picked();
            panic!("the task panicked");
        }));
        assert!(worker.await.unwrap_err().is_panic());
        assert_eq!(control.running_tasks(), 0);
        control.stop_picking();
        assert!(control.is_drained());
    }

    #[test]
    fn picks_outside_a_runner_are_not_gated() {
        assert!(matches!(admit_pick(), PickAdmission::Open(None)));
    }

    #[test]
    fn idle_exit_and_lifetime_refuse_the_http_api_and_zero() {
        let api = "this process serves the HTTP API";
        let zero = "must be greater than 0";
        for (settings, serve_http_api, expected) in [
            (settings(0, None, None), true, None),
            (settings(600, None, None), true, None),
            (settings(0, Some(180), None), false, None),
            (settings(0, Some(180), Some(3600)), false, None),
            (settings(0, Some(180), None), true, Some(api)),
            (settings(0, None, Some(3600)), true, Some(api)),
            (settings(0, Some(0), None), false, Some(zero)),
            (settings(0, None, Some(0)), false, Some(zero)),
        ] {
            let result = settings.validate(serve_http_api);
            match expected {
                None => assert!(result.is_ok(), "{settings:?}: {result:?}"),
                Some(expected) => {
                    let error = result.unwrap_err().to_string();
                    assert!(error.contains(expected), "{settings:?}: {error}");
                }
            }
        }
    }
}
