//! Read-only maintenance mode of this process.
//!
//! [`MAINTENANCE`] holds the mode set by `LAKEKEEPER__MAINTENANCE_MODE` and a
//! runtime flag that an instance admin sets through
//! `POST /management/v1/server/maintenance-mode`. The process is read-only
//! while either says so: the environment sets a floor the runtime flag cannot
//! lower. The flag lives in memory, so it covers only this process and a
//! restart returns it to the startup mode.

use std::{
    sync::{
        LazyLock,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use iceberg_ext::catalog::rest::ErrorModel;

pub use crate::config::MaintenanceMode;
use crate::{CONFIG, CancellationToken};

/// The maintenance mode of this process.
pub static MAINTENANCE: LazyLock<MaintenanceSwitch> =
    LazyLock::new(|| MaintenanceSwitch::new(CONFIG.maintenance_mode));

/// The startup maintenance mode plus a runtime read-only flag.
#[derive(Debug)]
pub struct MaintenanceSwitch {
    startup: MaintenanceMode,
    runtime_read_only: AtomicBool,
}

impl MaintenanceSwitch {
    #[must_use]
    pub fn new(startup: MaintenanceMode) -> Self {
        Self {
            startup,
            runtime_read_only: AtomicBool::new(false),
        }
    }

    /// The mode `LAKEKEEPER__MAINTENANCE_MODE` set at startup. A restart returns to it.
    #[must_use]
    pub fn startup(&self) -> MaintenanceMode {
        self.startup
    }

    /// The mode in effect: read-only if the startup mode or the runtime flag says so.
    #[must_use]
    pub fn effective(&self) -> MaintenanceMode {
        if self.is_read_only() {
            MaintenanceMode::ReadOnly
        } else {
            MaintenanceMode::Off
        }
    }

    #[must_use]
    pub fn is_read_only(&self) -> bool {
        self.startup.is_read_only() || self.runtime_read_only.load(Ordering::Relaxed)
    }

    /// Set the runtime flag and return the mode now in effect.
    ///
    /// # Errors
    /// [`MaintenanceModeSetAtStartup`] when asked for `off` while the startup
    /// mode is `read-only`.
    pub fn set(
        &self,
        mode: MaintenanceMode,
    ) -> Result<MaintenanceMode, MaintenanceModeSetAtStartup> {
        if self.startup.is_read_only() && !mode.is_read_only() {
            return Err(MaintenanceModeSetAtStartup);
        }
        self.runtime_read_only
            .store(mode.is_read_only(), Ordering::Relaxed);
        Ok(self.effective())
    }
}

/// Returned when switching off read-only that the startup mode sets. 409.
#[derive(Debug, thiserror::Error)]
#[error(
    "Read-only maintenance mode is set by LAKEKEEPER__MAINTENANCE_MODE and cannot be switched off at runtime."
)]
pub struct MaintenanceModeSetAtStartup;

impl From<MaintenanceModeSetAtStartup> for ErrorModel {
    fn from(err: MaintenanceModeSetAtStartup) -> Self {
        ErrorModel::conflict(err.to_string(), "MaintenanceModeSetAtStartup", None)
    }
}

/// How [`wait_until_writable`] ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitOutcome {
    Writable,
    Cancelled,
}

/// Wait until `switch` is no longer read-only, checking every `poll_interval`.
/// Returns [`WaitOutcome::Cancelled`] as soon as `cancellation_token` is cancelled.
pub async fn wait_until_writable(
    switch: &MaintenanceSwitch,
    poll_interval: Duration,
    cancellation_token: &CancellationToken,
) -> WaitOutcome {
    loop {
        if cancellation_token.is_cancelled() {
            return WaitOutcome::Cancelled;
        }
        if !switch.is_read_only() {
            return WaitOutcome::Writable;
        }
        tokio::select! {
            () = cancellation_token.cancelled() => return WaitOutcome::Cancelled,
            () = tokio::time::sleep(poll_interval) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use iceberg_ext::catalog::rest::ErrorModel;

    use super::*;
    use crate::CancellationToken;

    #[test]
    fn off_by_default_and_toggles_at_runtime() {
        let s = MaintenanceSwitch::new(MaintenanceMode::Off);
        assert_eq!(s.effective(), MaintenanceMode::Off);
        assert_eq!(
            s.set(MaintenanceMode::ReadOnly).unwrap(),
            MaintenanceMode::ReadOnly
        );
        assert!(s.is_read_only());
        // Setting the current mode again is idempotent.
        assert_eq!(
            s.set(MaintenanceMode::ReadOnly).unwrap(),
            MaintenanceMode::ReadOnly
        );
        assert_eq!(s.set(MaintenanceMode::Off).unwrap(), MaintenanceMode::Off);
        assert!(!s.is_read_only());
        assert_eq!(s.startup(), MaintenanceMode::Off);
    }

    #[test]
    fn startup_read_only_cannot_be_switched_off() {
        let s = MaintenanceSwitch::new(MaintenanceMode::ReadOnly);
        assert!(s.is_read_only());
        assert_eq!(
            s.set(MaintenanceMode::ReadOnly).unwrap(),
            MaintenanceMode::ReadOnly
        );
        let err = s.set(MaintenanceMode::Off).unwrap_err();
        let model = ErrorModel::from(err);
        assert_eq!(model.code, 409);
        assert_eq!(model.r#type, "MaintenanceModeSetAtStartup");
        assert!(s.is_read_only());
    }

    #[tokio::test(start_paused = true)]
    async fn wait_until_writable_returns_immediately_when_off() {
        let s = MaintenanceSwitch::new(MaintenanceMode::Off);
        let start = tokio::time::Instant::now();
        let out = wait_until_writable(&s, Duration::from_secs(10), &CancellationToken::new()).await;
        assert_eq!(out, WaitOutcome::Writable);
        assert_eq!(start.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn wait_until_writable_resumes_after_switch_off() {
        let s = Arc::new(MaintenanceSwitch::new(MaintenanceMode::Off));
        s.set(MaintenanceMode::ReadOnly).unwrap();
        let flipper = {
            let s = s.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(25)).await;
                s.set(MaintenanceMode::Off).unwrap();
            })
        };
        let start = tokio::time::Instant::now();
        let out = wait_until_writable(&s, Duration::from_secs(10), &CancellationToken::new()).await;
        flipper.await.unwrap();
        assert_eq!(out, WaitOutcome::Writable);
        // The switch flips at 25s; the wait notices on its next 10s tick.
        assert_eq!(start.elapsed(), Duration::from_secs(30));
    }

    #[tokio::test(start_paused = true)]
    async fn wait_until_writable_returns_cancelled_on_shutdown() {
        let s = MaintenanceSwitch::new(MaintenanceMode::ReadOnly);
        let token = CancellationToken::new();
        token.cancel();
        let start = tokio::time::Instant::now();
        assert_eq!(
            wait_until_writable(&s, Duration::from_secs(10), &token).await,
            WaitOutcome::Cancelled
        );
        assert_eq!(start.elapsed(), Duration::ZERO);
    }
}
