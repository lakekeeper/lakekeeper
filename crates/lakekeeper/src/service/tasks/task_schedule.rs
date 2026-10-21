//! Task schedules: recurring jobs that produce ordinary tasks.
//!
//! Ticks fall on `anchor + k * interval` for `k >= 0`. Each due tick inserts a task whose entity
//! id is the schedule id, so at most one instance per schedule is active. A tick that finds the
//! previous instance still active stays due and runs once that instance has finished. Ticks that
//! fall due meanwhile, or while no server materializes schedules, share that one run and are
//! counted in `missed_runs`.

use std::time::Duration;

use chrono::{DateTime, Utc};
use http::StatusCode;
use iceberg_ext::catalog::rest::ErrorModel;

use super::{TaskId, TaskQueueName};
pub use crate::service::TaskScheduleId;
use crate::{
    CancellationToken, WarehouseId,
    service::{
        ArcProjectId, CatalogBackendError, CatalogStore, CatalogTaskOps, Transaction,
        define_transparent_error, impl_error_stack_methods, impl_from_with_detail,
    },
};

/// The shortest interval a schedule may have.
pub const MIN_TASK_SCHEDULE_INTERVAL: chrono::Duration = chrono::Duration::minutes(1);

/// The longest interval a schedule may have: 100 years.
pub const MAX_TASK_SCHEDULE_INTERVAL: chrono::Duration = chrono::Duration::days(36_500);

/// The longest key a schedule may have, in characters.
pub const MAX_TASK_SCHEDULE_KEY_LENGTH: usize = 512;

/// Longest sleep between materializer passes: half of [`MIN_TASK_SCHEDULE_INTERVAL`], so drift
/// and jitter never skip a one-minute tick.
const MAX_MATERIALIZER_PERIOD: Duration = Duration::from_secs(30);

/// How many due schedules one materializer transaction handles.
const TASK_SCHEDULE_MATERIALIZE_BATCH_SIZE: i64 = 1000;

/// A schedule as stored.
#[derive(Debug, Clone, PartialEq)]
pub struct TaskSchedule {
    pub schedule_id: TaskScheduleId,
    pub project_id: ArcProjectId,
    /// `None` for a project-scoped schedule.
    pub warehouse_id: Option<WarehouseId>,
    pub queue_name: TaskQueueName,
    pub schedule_key: String,
    pub interval: chrono::Duration,
    pub anchor: DateTime<Utc>,
    pub payload: serde_json::Value,
    pub enabled: bool,
    pub next_run_at: DateTime<Utc>,
    pub missed_runs: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
}

/// Create or update the schedule identified by
/// (`project_id`, `warehouse_id`, `queue_name`, `schedule_key`).
#[derive(Debug, Clone, PartialEq, typed_builder::TypedBuilder)]
pub struct TaskScheduleUpsert {
    pub project_id: ArcProjectId,
    #[builder(default)]
    pub warehouse_id: Option<WarehouseId>,
    pub queue_name: TaskQueueName,
    #[builder(setter(into))]
    pub schedule_key: String,
    /// Between [`MIN_TASK_SCHEDULE_INTERVAL`] and [`MAX_TASK_SCHEDULE_INTERVAL`], in whole
    /// seconds; a fraction of a second is dropped.
    pub interval: chrono::Duration,
    /// Fixes when ticks fall. On create, `None` means now, so the first run is due at once.
    /// On update, `None` keeps the stored anchor.
    #[builder(default)]
    pub anchor: Option<DateTime<Utc>>,
    /// On create, `None` means `{}`. On update, `None` keeps the stored payload.
    #[builder(default)]
    pub payload: Option<serde_json::Value>,
    /// On create, `None` means enabled. On update, `None` keeps the stored state; enabling a
    /// disabled schedule moves its next run to its first tick at or after now.
    #[builder(default)]
    pub enabled: Option<bool>,
    /// Write only if the stored schedule is in this state. `None` writes in any state; a schedule
    /// of a newer spec version is refused either way.
    #[builder(default)]
    pub condition: Option<TaskScheduleCondition>,
}

/// The state a stored schedule must be in for a [`TaskScheduleUpsert`] to write it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskScheduleCondition {
    /// No schedule with this id is stored.
    Absent,
    /// The stored schedule has this interval.
    Interval(chrono::Duration),
}

/// A project with its project-level schedule and configuration of one queue.
#[derive(Debug, Clone, PartialEq)]
pub struct ProjectTaskScheduleState {
    pub project_id: ArcProjectId,
    /// Interval of the project-level schedule with the queue and key; `None` if there is none.
    pub interval: Option<chrono::Duration>,
    /// Project-level configuration of the queue, if set.
    pub queue_config: Option<serde_json::Value>,
}

/// Which schedules to list. Every set filter must match.
#[derive(Debug, Clone, PartialEq, typed_builder::TypedBuilder)]
pub struct TaskScheduleFilter {
    pub project_id: ArcProjectId,
    /// `None` lists the schedules of the project and of all its warehouses.
    #[builder(default)]
    pub warehouse_id: Option<WarehouseId>,
    #[builder(default)]
    pub queue_name: Option<TaskQueueName>,
    #[builder(default, setter(strip_option, into))]
    pub schedule_key: Option<String>,
}

/// A task the materializer inserted for a due schedule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaterializedTick {
    pub schedule_id: TaskScheduleId,
    pub task_id: TaskId,
    /// The task's `scheduled_for`: [`DueTick::tick`].
    pub tick: DateTime<Utc>,
    /// Earlier ticks this run covers.
    pub missed_runs: i64,
    pub next_run_at: DateTime<Utc>,
}

#[derive(thiserror::Error, Debug, PartialEq)]
#[error(
    "Task schedule interval of {interval_seconds} seconds is outside the allowed range of {minimum_seconds} to {maximum_seconds} seconds"
)]
pub struct TaskScheduleIntervalOutOfRangeError {
    pub interval_seconds: i64,
    pub minimum_seconds: i64,
    pub maximum_seconds: i64,
    pub stack: Vec<String>,
}
impl_error_stack_methods!(TaskScheduleIntervalOutOfRangeError);
impl From<TaskScheduleIntervalOutOfRangeError> for ErrorModel {
    fn from(value: TaskScheduleIntervalOutOfRangeError) -> Self {
        ErrorModel::builder()
            .code(StatusCode::BAD_REQUEST.as_u16())
            .message(value.to_string())
            .r#type("TaskScheduleIntervalOutOfRange")
            .stack(value.stack)
            .build()
    }
}

#[derive(thiserror::Error, Debug, PartialEq)]
#[error(
    "Task schedule key of {length} characters is outside the allowed range of 1 to {maximum} characters"
)]
pub struct TaskScheduleKeyLengthError {
    pub length: usize,
    pub maximum: usize,
    pub stack: Vec<String>,
}
impl_error_stack_methods!(TaskScheduleKeyLengthError);
impl From<TaskScheduleKeyLengthError> for ErrorModel {
    fn from(value: TaskScheduleKeyLengthError) -> Self {
        ErrorModel::builder()
            .code(StatusCode::BAD_REQUEST.as_u16())
            .message(value.to_string())
            .r#type("TaskScheduleKeyLength")
            .stack(value.stack)
            .build()
    }
}

/// The project or warehouse of a new schedule does not exist, or the warehouse belongs to another
/// project.
#[derive(thiserror::Error, Debug, PartialEq)]
#[error("{}", scope_not_found_message(project_id, *warehouse_id))]
pub struct TaskScheduleScopeNotFoundError {
    pub project_id: ArcProjectId,
    pub warehouse_id: Option<WarehouseId>,
    pub stack: Vec<String>,
}
impl_error_stack_methods!(TaskScheduleScopeNotFoundError);
impl From<TaskScheduleScopeNotFoundError> for ErrorModel {
    fn from(value: TaskScheduleScopeNotFoundError) -> Self {
        ErrorModel::builder()
            .code(StatusCode::NOT_FOUND.as_u16())
            .message(value.to_string())
            .r#type("TaskScheduleScopeNotFound")
            .stack(value.stack)
            .build()
    }
}

fn scope_not_found_message(project_id: &ArcProjectId, warehouse_id: Option<WarehouseId>) -> String {
    match warehouse_id {
        Some(warehouse_id) => {
            format!("Warehouse `{warehouse_id}` not found in project `{project_id}`")
        }
        None => format!("Project `{project_id}` not found"),
    }
}

/// The stored schedule uses a tick specification this server does not know.
#[derive(thiserror::Error, Debug, PartialEq)]
#[error(
    "Task schedule `{schedule_key}` of queue `{queue_name}` was written by a newer server version"
)]
pub struct TaskScheduleSpecVersionUnsupportedError {
    pub queue_name: TaskQueueName,
    pub schedule_key: String,
    pub stack: Vec<String>,
}
impl_error_stack_methods!(TaskScheduleSpecVersionUnsupportedError);
impl From<TaskScheduleSpecVersionUnsupportedError> for ErrorModel {
    fn from(value: TaskScheduleSpecVersionUnsupportedError) -> Self {
        ErrorModel::builder()
            .code(StatusCode::CONFLICT.as_u16())
            .message(value.to_string())
            .r#type("TaskScheduleSpecVersionUnsupported")
            .stack(value.stack)
            .build()
    }
}

/// The stored schedule is not in the state the upsert's condition requires.
#[derive(thiserror::Error, Debug, PartialEq)]
#[error("Task schedule `{schedule_key}` of queue `{queue_name}` does not meet the write condition")]
pub struct TaskScheduleConditionFailedError {
    pub queue_name: TaskQueueName,
    pub schedule_key: String,
    pub stack: Vec<String>,
}
impl_error_stack_methods!(TaskScheduleConditionFailedError);
impl From<TaskScheduleConditionFailedError> for ErrorModel {
    fn from(value: TaskScheduleConditionFailedError) -> Self {
        ErrorModel::builder()
            .code(StatusCode::CONFLICT.as_u16())
            .message(value.to_string())
            .r#type("TaskScheduleConditionFailed")
            .stack(value.stack)
            .build()
    }
}

#[derive(thiserror::Error, Debug, PartialEq)]
#[error("Task schedule with id `{schedule_id}` not found")]
pub struct TaskScheduleNotFoundError {
    pub schedule_id: TaskScheduleId,
    pub stack: Vec<String>,
}
impl_error_stack_methods!(TaskScheduleNotFoundError);
impl From<TaskScheduleNotFoundError> for ErrorModel {
    fn from(value: TaskScheduleNotFoundError) -> Self {
        ErrorModel::builder()
            .code(StatusCode::NOT_FOUND.as_u16())
            .message(value.to_string())
            .r#type("TaskScheduleNotFound")
            .stack(value.stack)
            .build()
    }
}

define_transparent_error! {
    pub enum UpsertTaskScheduleError,
    stack_message: "Error upserting task schedule in catalog",
    variants: [
        TaskScheduleIntervalOutOfRangeError,
        TaskScheduleKeyLengthError,
        TaskScheduleScopeNotFoundError,
        TaskScheduleSpecVersionUnsupportedError,
        TaskScheduleConditionFailedError,
        CatalogBackendError
    ]
}

define_transparent_error! {
    pub enum TriggerTaskScheduleError,
    stack_message: "Error triggering task schedule in catalog",
    variants: [
        TaskScheduleNotFoundError,
        CatalogBackendError
    ]
}

define_transparent_error! {
    pub enum TaskScheduleStoreError,
    stack_message: "Error accessing task schedules in catalog",
    variants: [
        CatalogBackendError
    ]
}

/// Validate an interval against [`MIN_TASK_SCHEDULE_INTERVAL`] and
/// [`MAX_TASK_SCHEDULE_INTERVAL`].
///
/// # Errors
/// If the interval is outside that range.
pub(crate) fn validate_task_schedule_interval(
    interval: chrono::Duration,
) -> Result<(), TaskScheduleIntervalOutOfRangeError> {
    if !(MIN_TASK_SCHEDULE_INTERVAL..=MAX_TASK_SCHEDULE_INTERVAL).contains(&interval) {
        return Err(TaskScheduleIntervalOutOfRangeError {
            interval_seconds: interval.num_seconds(),
            minimum_seconds: MIN_TASK_SCHEDULE_INTERVAL.num_seconds(),
            maximum_seconds: MAX_TASK_SCHEDULE_INTERVAL.num_seconds(),
            stack: Vec::new(),
        });
    }
    Ok(())
}

/// Validate a key against [`MAX_TASK_SCHEDULE_KEY_LENGTH`].
///
/// # Errors
/// If the key is empty or longer.
pub(crate) fn validate_task_schedule_key(key: &str) -> Result<(), TaskScheduleKeyLengthError> {
    let length = key.chars().count();
    if !(1..=MAX_TASK_SCHEDULE_KEY_LENGTH).contains(&length) {
        return Err(TaskScheduleKeyLengthError {
            length,
            maximum: MAX_TASK_SCHEDULE_KEY_LENGTH,
            stack: Vec::new(),
        });
    }
    Ok(())
}

/// A tick beyond the range of [`DateTime<Utc>`], or a schedule without a positive interval.
#[derive(thiserror::Error, Debug, Clone, Copy, PartialEq, Eq)]
#[error("Task schedule tick is outside the representable time range")]
pub struct TaskScheduleTickOutOfRangeError;

/// When a due schedule runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DueTick {
    /// The latest tick at or before now, or `next_run_at` if that is later: the run's
    /// `scheduled_for`.
    pub tick: DateTime<Utc>,
    /// The first tick after now.
    pub following: DateTime<Utc>,
    /// Whole intervals between `next_run_at` and now: the earlier ticks this run covers.
    pub missed_runs: i64,
}

fn interval_micros(interval_seconds: i64) -> Result<i128, TaskScheduleTickOutOfRangeError> {
    if interval_seconds <= 0 {
        return Err(TaskScheduleTickOutOfRangeError);
    }
    Ok(i128::from(interval_seconds) * 1_000_000)
}

fn micros_between(from: DateTime<Utc>, to: DateTime<Utc>) -> i128 {
    i128::from(to.timestamp_micros()) - i128::from(from.timestamp_micros())
}

fn plus_micros(
    t: DateTime<Utc>,
    micros: i128,
) -> Result<DateTime<Utc>, TaskScheduleTickOutOfRangeError> {
    i64::try_from(micros)
        .ok()
        .and_then(|micros| t.checked_add_signed(chrono::Duration::microseconds(micros)))
        .ok_or(TaskScheduleTickOutOfRangeError)
}

/// The first tick at or after `t`, where ticks fall on `anchor + k * interval_seconds`, `k >= 0`.
///
/// # Errors
/// If that tick is out of range or `interval_seconds` is not positive.
pub fn first_tick_at_or_after(
    anchor: DateTime<Utc>,
    interval_seconds: i64,
    t: DateTime<Utc>,
) -> Result<DateTime<Utc>, TaskScheduleTickOutOfRangeError> {
    let interval = interval_micros(interval_seconds)?;
    let elapsed = micros_between(anchor, t);
    let k = if elapsed <= 0 {
        0
    } else {
        (elapsed + interval - 1) / interval
    };
    plus_micros(anchor, k * interval)
}

/// When a schedule due at `next_run_at` runs at `now`.
///
/// # Errors
/// If a tick is out of range or `interval_seconds` is not positive.
pub fn due_tick(
    anchor: DateTime<Utc>,
    interval_seconds: i64,
    next_run_at: DateTime<Utc>,
    now: DateTime<Utc>,
) -> Result<DueTick, TaskScheduleTickOutOfRangeError> {
    let interval = interval_micros(interval_seconds)?;
    let elapsed = micros_between(anchor, now);
    let k = if elapsed < 0 {
        0
    } else {
        elapsed / interval + 1
    };
    let following = plus_micros(anchor, k * interval)?;
    let latest = plus_micros(anchor, (k - 1) * interval)?;
    let missed_runs = i64::try_from(micros_between(next_run_at, now).max(0) / interval)
        .map_err(|_| TaskScheduleTickOutOfRangeError)?;
    Ok(DueTick {
        tick: latest.max(next_run_at),
        following,
        missed_runs,
    })
}

/// Period between materializer passes: the task poll interval, at most 30 seconds.
#[must_use]
fn materializer_period(poll_interval: Duration) -> Duration {
    poll_interval.min(MAX_MATERIALIZER_PERIOD)
}

/// Run every due schedule until no due schedule is left.
///
/// # Errors
/// If a transaction cannot be started or committed, or the store fails.
pub async fn materialize_due_task_schedules<C: CatalogStore>(
    catalog_state: C::State,
) -> crate::api::Result<Vec<MaterializedTick>> {
    let mut all = Vec::new();
    loop {
        let mut t = C::Transaction::begin_write(catalog_state.clone()).await?;
        let ticks = C::materialize_due_task_schedules(
            TASK_SCHEDULE_MATERIALIZE_BATCH_SIZE,
            t.transaction(),
        )
        .await?;
        t.commit().await?;
        let done =
            i64::try_from(ticks.len()).unwrap_or(i64::MAX) < TASK_SCHEDULE_MATERIALIZE_BATCH_SIZE;
        all.extend(ticks);
        if done {
            return Ok(all);
        }
    }
}

/// Background loop that materializes due schedules. Safe to run on every server: due rows are
/// claimed with `SKIP LOCKED`.
pub async fn task_schedule_materializer<C: CatalogStore>(
    catalog_state: C::State,
    poll_interval: Duration,
    cancellation_token: CancellationToken,
) {
    let period = materializer_period(poll_interval);
    tracing::info!("Starting task schedule materializer with a period of {period:?}");
    loop {
        match materialize_due_task_schedules::<C>(catalog_state.clone()).await {
            Ok(ticks) => {
                for tick in &ticks {
                    if tick.missed_runs > 0 {
                        tracing::info!(
                            schedule_id = %tick.schedule_id,
                            task_id = %tick.task_id,
                            tick = %tick.tick,
                            missed_runs = tick.missed_runs,
                            next_run_at = %tick.next_run_at,
                            "Materialized task schedule tick covering missed ticks"
                        );
                    } else {
                        tracing::debug!(
                            schedule_id = %tick.schedule_id,
                            task_id = %tick.task_id,
                            next_run_at = %tick.next_run_at,
                            "Materialized task schedule tick"
                        );
                    }
                }
            }
            Err(e) => {
                tracing::error!("Failed to materialize task schedules: {e}");
            }
        }
        let jitter = Duration::from_millis(fastrand::u64(0..500));
        tokio::select! {
            () = cancellation_token.cancelled() => {
                tracing::info!("Graceful shutdown: exiting task schedule materializer");
                return;
            }
            () = tokio::time::sleep(period + jitter) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interval_is_accepted_from_one_minute_to_100_years() {
        let refused = |interval_seconds| {
            Err(TaskScheduleIntervalOutOfRangeError {
                interval_seconds,
                minimum_seconds: 60,
                maximum_seconds: 3_153_600_000,
                stack: Vec::new(),
            })
        };
        for (seconds, expected) in [
            (59, refused(59)),
            (60, Ok(())),
            (3_153_600_000, Ok(())),
            (3_153_600_001, refused(3_153_600_001)),
        ] {
            assert_eq!(
                validate_task_schedule_interval(chrono::Duration::seconds(seconds)),
                expected,
                "{seconds} s"
            );
        }
    }

    fn at(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    const HOUR: i64 = 3600;

    #[test]
    fn first_tick_before_the_anchor_is_the_anchor() {
        let anchor = at("2026-01-01T00:00:00Z");
        assert_eq!(
            first_tick_at_or_after(anchor, HOUR, at("2025-06-01T12:00:00Z")),
            Ok(anchor)
        );
        assert_eq!(first_tick_at_or_after(anchor, HOUR, anchor), Ok(anchor));
    }

    #[test]
    fn first_tick_is_exact_for_an_anchor_in_year_one() {
        // 63_600_000_000 s after the anchor is a tick of a 600 s interval.
        let anchor = at("0001-01-01T00:00:00Z");
        let tick = anchor + chrono::Duration::seconds(63_600_000_000);
        assert_eq!(
            first_tick_at_or_after(anchor, 600, tick - chrono::Duration::microseconds(1)),
            Ok(tick)
        );
    }

    #[test]
    fn first_tick_beyond_the_representable_range_is_refused() {
        assert_eq!(
            first_tick_at_or_after(
                DateTime::<Utc>::MIN_UTC,
                MAX_TASK_SCHEDULE_INTERVAL.num_seconds(),
                DateTime::<Utc>::MAX_UTC
            ),
            Err(TaskScheduleTickOutOfRangeError)
        );
    }

    #[test]
    fn a_non_positive_interval_is_refused() {
        let anchor = at("2026-01-01T00:00:00Z");
        assert_eq!(
            first_tick_at_or_after(anchor, 0, anchor),
            Err(TaskScheduleTickOutOfRangeError)
        );
        assert_eq!(
            due_tick(anchor, -1, anchor, anchor),
            Err(TaskScheduleTickOutOfRangeError)
        );
    }

    #[test]
    fn due_late_runs_the_latest_tick_and_counts_the_earlier_ones() {
        assert_eq!(
            due_tick(
                at("2026-01-01T00:00:00Z"),
                HOUR,
                at("2026-01-01T05:00:00Z"),
                at("2026-01-01T08:30:00Z")
            ),
            Ok(DueTick {
                tick: at("2026-01-01T08:00:00Z"),
                following: at("2026-01-01T09:00:00Z"),
                missed_runs: 3,
            })
        );
    }

    #[test]
    fn due_off_the_grid_runs_at_next_run_at_and_returns_to_the_grid() {
        // `next_run_at` later than the latest tick at or before now.
        assert_eq!(
            due_tick(
                at("2026-01-01T00:00:00Z"),
                HOUR,
                at("2026-01-01T05:30:00Z"),
                at("2026-01-01T05:45:00Z")
            ),
            Ok(DueTick {
                tick: at("2026-01-01T05:30:00Z"),
                following: at("2026-01-01T06:00:00Z"),
                missed_runs: 0,
            })
        );
    }

    #[test]
    fn due_matches_a_brute_force_walk_over_the_grid() {
        let anchor = at("2026-01-01T00:00:00.000007Z");
        for interval in [60, 61, 3600, 86_400, 90_061] {
            for offset in [0, 1, 59_999_999, 60_000_000, 7_777_777_777] {
                let now = anchor + chrono::Duration::microseconds(offset);
                let ticks = (0..)
                    .map(|k| anchor + chrono::Duration::seconds(k * interval))
                    .take_while(|t| *t <= now + chrono::Duration::seconds(interval));
                let mut latest = anchor;
                let mut following = anchor;
                for t in ticks {
                    if t > now {
                        following = t;
                        break;
                    }
                    latest = t;
                }
                assert_eq!(
                    due_tick(anchor, interval, anchor, now),
                    Ok(DueTick {
                        tick: latest,
                        following,
                        missed_runs: (latest - anchor).num_seconds() / interval,
                    }),
                    "interval {interval}, offset {offset}"
                );
                assert_eq!(
                    first_tick_at_or_after(anchor, interval, now),
                    Ok(if latest == now { latest } else { following }),
                    "interval {interval}, offset {offset}"
                );
            }
        }
    }
}
