//! Expires the dataset snapshots a retention policy does not keep.
//!
//! Expiring hides a snapshot and starts its grace period; nothing is deleted
//! here. The purge that follows, on its own queue, deletes what nobody restored.
//! A dataset follows the warehouse's queue config, off unless enabled, unless it
//! sets a policy of its own.
use std::{
    collections::{HashMap, HashSet},
    sync::LazyLock,
    time::Duration as StdDuration,
};

use chrono::{DateTime, Duration, Utc};
use iceberg_ext::catalog::rest::ErrorModel;
use serde::{Deserialize, Serialize};
use tracing::Instrument;
#[cfg(feature = "open-api")]
use utoipa::{PartialSchema, ToSchema};

use super::{
    ScheduleTaskMetadata, SpecializedTask, TaskConfig, TaskData, TaskExecutionDetails,
    WarehouseTaskEntityId,
    dataset_snapshot_purge_queue::{DatasetSnapshotPurgePayload, DatasetSnapshotPurgeTask},
};
use crate::{
    CancellationToken,
    api::Result,
    service::{
        ArcProjectId, CatalogDatasetOps, CatalogStore, CatalogTaskOps, DatasetId, DatasetRef,
        DatasetRefType, DatasetRetention, DatasetSnapshotId, DatasetSnapshotNode, LoadDatasetError,
        Transaction, WarehouseId,
        task_configs::TaskQueueConfigFilter,
        tasks::{TaskEntity, TaskQueueName},
    },
};

const QN_STR: &str = "dataset_snapshot_expiry";
pub static QUEUE_NAME: LazyLock<TaskQueueName> = LazyLock::new(|| QN_STR.into());
#[cfg(feature = "open-api")]
pub(crate) static API_CONFIG: LazyLock<super::QueueApiConfig> =
    LazyLock::new(|| super::QueueApiConfig {
        queue_name: &QUEUE_NAME,
        utoipa_type_name: DatasetSnapshotExpiryQueueConfig::name(),
        utoipa_schema: DatasetSnapshotExpiryQueueConfig::schema(),
        scope: super::QueueScope::Warehouse,
        user_scheduling: super::UserScheduling::Disabled,
    });

const DEFAULT_MAX_SNAPSHOT_AGE: Duration = Duration::days(5);
const DEFAULT_MIN_SNAPSHOTS_TO_KEEP: u32 = 1;
const DEFAULT_GRACE_PERIOD: Duration = Duration::days(7);

/// How long after a commit or ref deletion a retention pass runs. A burst of
/// commits in that window shares one pass: the task table holds one pending task
/// per dataset and queue.
pub const EXPIRY_DELAY: Duration = Duration::hours(1);

pub type DatasetSnapshotExpiryTask = SpecializedTask<
    DatasetSnapshotExpiryQueueConfig,
    DatasetSnapshotExpiryPayload,
    DatasetSnapshotExpiryExecutionDetails,
>;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatasetSnapshotExpiryPayload {}

impl TaskData for DatasetSnapshotExpiryPayload {}

/// The warehouse's retention policy for datasets.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(ToSchema))]
#[serde(rename_all = "kebab-case", default)]
pub struct DatasetSnapshotExpiryQueueConfig {
    /// Expire snapshots automatically. Defaults to `false`: history is kept.
    enabled: Option<bool>,
    /// How old a snapshot must be to expire, in ISO 8601 duration format.
    /// Defaults to 5 days (P5D).
    #[cfg_attr(feature = "open-api", schema(example = "P5D"))]
    #[serde(with = "crate::utils::time_conversion::iso8601_option_duration_serde")]
    max_snapshot_age: Option<Duration>,
    /// The newest snapshots of each branch that never expire, whatever their age.
    /// Defaults to 1, the head; below 1 it is 1.
    min_snapshots_to_keep: Option<u32>,
    /// How long an expired snapshot stays restorable before it is purged, in
    /// ISO 8601 duration format. Defaults to 7 days (P7D).
    #[cfg_attr(feature = "open-api", schema(example = "P7D"))]
    #[serde(with = "crate::utils::time_conversion::iso8601_option_duration_serde")]
    grace_period: Option<Duration>,
}

impl DatasetSnapshotExpiryQueueConfig {
    fn enabled(&self) -> bool {
        self.enabled.unwrap_or(false)
    }

    fn max_snapshot_age(&self) -> Duration {
        self.max_snapshot_age.unwrap_or(DEFAULT_MAX_SNAPSHOT_AGE)
    }

    fn min_snapshots_to_keep(&self) -> usize {
        usize::try_from(
            self.min_snapshots_to_keep
                .unwrap_or(DEFAULT_MIN_SNAPSHOTS_TO_KEEP)
                .max(1),
        )
        .unwrap_or(usize::MAX)
    }

    fn grace_period(&self) -> Duration {
        self.grace_period.unwrap_or(DEFAULT_GRACE_PERIOD)
    }
}

/// The warehouse's retention policy for datasets, as its queue config sets it.
pub(crate) async fn warehouse_retention<C: CatalogStore>(
    warehouse_id: WarehouseId,
    catalog_state: C::State,
) -> Result<DatasetSnapshotExpiryQueueConfig> {
    let Some(config) = C::get_task_queue_config(
        &TaskQueueConfigFilter::WarehouseId { warehouse_id },
        &QUEUE_NAME,
        catalog_state,
    )
    .await?
    else {
        return Ok(DatasetSnapshotExpiryQueueConfig::default());
    };
    serde_json::from_value(config.queue_config.config).map_err(|e| {
        ErrorModel::internal(
            format!("The `{QN_STR}` queue config does not parse: {e}"),
            "InvalidQueueConfig",
            None,
        )
        .into()
    })
}

/// When a snapshot expired at `now` may be purged: `grace_period` later, or never
/// for a grace period past what time can hold.
pub(crate) fn purge_after(now: DateTime<Utc>, grace_period: Duration) -> DateTime<Utc> {
    now.checked_add_signed(grace_period)
        .unwrap_or(DateTime::<Utc>::MAX_UTC)
}

/// The rule a retention pass applies to one dataset: its own policy, or the
/// warehouse's when it inherits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct EffectiveRetention {
    /// The age past which a snapshot may expire, and the newest snapshots of each
    /// branch kept whatever their age. `None` when nothing expires automatically.
    automatic: Option<(Duration, usize)>,
    /// How long an expired snapshot stays restorable.
    pub(crate) grace_period: Duration,
}

impl EffectiveRetention {
    pub(crate) fn resolve(
        dataset: &DatasetRetention,
        warehouse: &DatasetSnapshotExpiryQueueConfig,
    ) -> Self {
        let grace = |own: Option<Duration>| own.unwrap_or_else(|| warehouse.grace_period());
        let keep = |n: u32| usize::try_from(n.max(1)).unwrap_or(usize::MAX);
        match dataset {
            DatasetRetention::Inherit {} => Self {
                automatic: warehouse.enabled().then(|| {
                    (
                        warehouse.max_snapshot_age(),
                        warehouse.min_snapshots_to_keep(),
                    )
                }),
                grace_period: warehouse.grace_period(),
            },
            DatasetRetention::Manual { grace_period } => Self {
                automatic: None,
                grace_period: grace(*grace_period),
            },
            DatasetRetention::Ttl {
                max_snapshot_age,
                min_snapshots_to_keep,
                grace_period,
            } => Self {
                automatic: Some((
                    *max_snapshot_age,
                    keep(min_snapshots_to_keep.unwrap_or(DEFAULT_MIN_SNAPSHOTS_TO_KEEP)),
                )),
                grace_period: grace(*grace_period),
            },
            // Every snapshot is old enough; only its place in the branch keeps one.
            DatasetRetention::MaxCount {
                max_snapshots,
                grace_period,
            } => Self {
                automatic: Some((Duration::zero(), keep(*max_snapshots))),
                grace_period: grace(*grace_period),
            },
        }
    }
}

impl TaskConfig for DatasetSnapshotExpiryQueueConfig {
    fn queue_name() -> &'static TaskQueueName {
        &QUEUE_NAME
    }

    fn max_time_since_last_heartbeat() -> chrono::Duration {
        chrono::Duration::seconds(3600)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DatasetSnapshotExpiryExecutionDetails {}

impl TaskExecutionDetails for DatasetSnapshotExpiryExecutionDetails {}

/// Queue a retention pass for a dataset, [`EXPIRY_DELAY`] out, in the caller's
/// transaction. A pass already pending is the expected case: it sees this change
/// too, and is brought forward if a pass rescheduled itself for later.
pub(crate) async fn schedule_snapshot_expiry<C: CatalogStore>(
    project_id: ArcProjectId,
    entity: TaskEntity,
    transaction: &mut C::Transaction,
) -> Result<()> {
    let at = Utc::now() + EXPIRY_DELAY;
    let queued = DatasetSnapshotExpiryTask::schedule_task::<C>(
        ScheduleTaskMetadata {
            project_id,
            parent_task_id: None,
            scheduled_for: Some(at),
            entity: entity.clone(),
        },
        DatasetSnapshotExpiryPayload::default(),
        transaction.transaction(),
    )
    .await?;
    if queued.is_none()
        && let TaskEntity::EntityInWarehouse {
            warehouse_id,
            entity_id: WarehouseTaskEntityId::Dataset { dataset_id },
            ..
        } = entity
    {
        C::bring_dataset_task_forward(
            warehouse_id,
            dataset_id,
            QUEUE_NAME.as_str(),
            at,
            transaction.transaction(),
        )
        .await
        .map_err(ErrorModel::from)?;
    }
    Ok(())
}

async fn schedule_pass_at<C: CatalogStore>(
    project_id: ArcProjectId,
    entity: TaskEntity,
    at: DateTime<Utc>,
    transaction: <C::Transaction as Transaction<C::State>>::Transaction<'_>,
) -> Result<()> {
    DatasetSnapshotExpiryTask::schedule_task::<C>(
        ScheduleTaskMetadata {
            project_id,
            parent_task_id: None,
            scheduled_for: Some(at),
            entity,
        },
        DatasetSnapshotExpiryPayload::default(),
        transaction,
    )
    .await?;
    Ok(())
}

/// When a snapshot kept at `now` only for its age, or only for a grant, may next
/// be let go: the youngest's reaching `max_age`, or a grant on one old enough
/// running out. A snapshot a ref points at is kept whatever its age or grants, so
/// it waits on nothing. `None` when nothing waits on time, as under `max-count`.
fn next_pass_due(
    nodes: &[DatasetSnapshotNode],
    refs: &[DatasetRef],
    now: DateTime<Utc>,
    max_age: Duration,
) -> Option<DateTime<Utc>> {
    let cutoff = now
        .checked_sub_signed(max_age)
        .unwrap_or(DateTime::<Utc>::MIN_UTC);
    let heads: HashSet<DatasetSnapshotId> = refs.iter().filter_map(|r| r.snapshot_id).collect();
    nodes
        .iter()
        .filter(|n| !n.expired && !heads.contains(&n.snapshot_id))
        .filter_map(|n| {
            if n.created_at > cutoff {
                n.created_at.checked_add_signed(max_age)
            } else {
                n.pinned_until
            }
        })
        .min()
}

/// The active snapshots retention lets go at `now`.
///
/// Kept: every tag's snapshot, every snapshot a live grant reads, and on each
/// branch the newest `min_keep` plus any younger than `max_age`. A snapshot no ref
/// reaches, a deleted branch's, expires once it is older than `max_age`. Nothing
/// expires when a ref points at a snapshot the graph lacks: what that branch
/// keeps cannot be told.
fn snapshots_to_expire(
    nodes: &[DatasetSnapshotNode],
    refs: &[DatasetRef],
    now: DateTime<Utc>,
    max_age: Duration,
    min_keep: usize,
) -> Vec<DatasetSnapshotId> {
    // An age past what time can hold expires nothing.
    let cutoff = now
        .checked_sub_signed(max_age)
        .unwrap_or(DateTime::<Utc>::MIN_UTC);
    let by_id: HashMap<DatasetSnapshotId, &DatasetSnapshotNode> =
        nodes.iter().map(|n| (n.snapshot_id, n)).collect();
    if refs
        .iter()
        .filter_map(|r| r.snapshot_id)
        .any(|head| !by_id.contains_key(&head))
    {
        return Vec::new();
    }
    let mut kept: HashSet<DatasetSnapshotId> = nodes
        .iter()
        .filter(|n| n.pinned_until.is_some())
        .map(|n| n.snapshot_id)
        .collect();
    for r in refs {
        let Some(head) = r.snapshot_id else { continue };
        if r.typ == DatasetRefType::Tag {
            kept.insert(head);
            continue;
        }
        let mut next = Some(head);
        let mut depth = 0;
        while let Some(node) = next.and_then(|id| by_id.get(&id)) {
            if depth >= min_keep && node.created_at < cutoff {
                break;
            }
            kept.insert(node.snapshot_id);
            depth += 1;
            next = node.parent_snapshot_id;
        }
    }
    nodes
        .iter()
        .filter(|n| !n.expired && n.created_at < cutoff && !kept.contains(&n.snapshot_id))
        .map(|n| n.snapshot_id)
        .collect()
}

pub(crate) async fn dataset_snapshot_expiry_worker<C: CatalogStore>(
    catalog_state: C::State,
    poll_interval: StdDuration,
    cancellation_token: CancellationToken,
) {
    loop {
        let task = DatasetSnapshotExpiryTask::poll_for_new_task::<C>(
            catalog_state.clone(),
            &poll_interval,
            cancellation_token.clone(),
        )
        .await;

        let Some(task) = task else {
            tracing::info!("Graceful shutdown: exiting `{QN_STR}` worker");
            return;
        };

        let span = if let Some((warehouse_id, entity_id, entity_name)) =
            task.task_metadata.warehouse_task_sub_entity()
        {
            tracing::debug_span!(
                QN_STR,
                warehouse_id = %warehouse_id,
                entity_id = %entity_id.as_uuid(),
                entity_name = %entity_name.join("."),
                attempt = %task.attempt(),
                task_id = %task.task_id(),
            )
        } else {
            tracing::debug_span!(
                QN_STR,
                entity_type = "Not Specified",
                attempt = %task.attempt(),
                task_id = %task.task_id(),
            )
        };

        instrumented_expiry::<C>(catalog_state.clone(), &task)
            .instrument(span.or_current())
            .await;
    }
}

async fn instrumented_expiry<C: CatalogStore>(
    catalog_state: C::State,
    task: &DatasetSnapshotExpiryTask,
) {
    match expire::<C>(task, catalog_state.clone()).await {
        Ok(expired) => {
            tracing::info!(
                "Task of `{QN_STR}` worker exited successfully. Expired {expired} snapshots."
            );
        }
        Err(err) => {
            tracing::error!(
                "Error in `{QN_STR}` worker. Failed to expire dataset snapshots. {err}"
            );
            let detail = format!("Failed to expire dataset snapshots.\nError: {}", err.error);
            task.record_failure::<C>(catalog_state, &detail).await;
        }
    }
}

async fn expire<C: CatalogStore>(
    task: &DatasetSnapshotExpiryTask,
    catalog_state: C::State,
) -> Result<u64> {
    let entity = task.task_metadata.entity.clone();
    let TaskEntity::EntityInWarehouse {
        warehouse_id,
        entity_id,
        entity_name: _,
    } = &entity
    else {
        return Err(ErrorModel::internal(
            format!("Unexpected task scope for `{QN_STR}` task. Task must have a dataset scope."),
            "UnexpectedTaskScopeForDatasetSnapshotExpiry",
            None,
        )
        .into());
    };
    let warehouse_id = *warehouse_id;
    let dataset_id = DatasetId::from(entity_id.as_uuid());
    let config = task.config.clone().unwrap_or_default();

    let mut t = C::Transaction::begin_write(catalog_state).await?;
    // A dropped dataset, soft-deleted or gone, is left as it is.
    let retention = match C::load_dataset_by_id(warehouse_id, dataset_id, t.transaction()).await {
        Ok(info) => Some(info.retention),
        Err(LoadDatasetError::DatasetNotFound(_)) => None,
        Err(e) => return Err(e.into()),
    };
    let policy = retention.map(|r| EffectiveRetention::resolve(&r, &config));
    let (expired, next_pass) = if let Some(EffectiveRetention {
        automatic: Some((max_age, min_keep)),
        grace_period,
    }) = policy
    {
        // The graph first: a ref moved between the two reads is then walked where
        // it stands. A head committed meanwhile is one the graph lacks, and that
        // pass expires nothing.
        let nodes =
            C::list_dataset_snapshot_graph(warehouse_id, dataset_id, t.transaction()).await?;
        let refs = C::list_dataset_refs(warehouse_id, dataset_id, t.transaction()).await?;
        let now = Utc::now();
        let ids = snapshots_to_expire(&nodes, &refs, now, max_age, min_keep);
        let expired = C::expire_dataset_snapshots(
            warehouse_id,
            dataset_id,
            &ids,
            purge_after(now, grace_period),
            t.transaction(),
        )
        .await?;
        (expired, next_pass_due(&nodes, &refs, now, max_age))
    } else {
        (0, None)
    };
    task.record_success_in_transaction::<C>(
        t.transaction(),
        Some(&format!("Expired {expired} snapshots")),
    )
    .await;
    // What this pass kept for its age or a grant is looked at again once that
    // may have changed, though nobody writes to the dataset; the delay batches
    // snapshots that age out together, and an hour from now at the soonest keeps a
    // clock behind the database's from spinning the queue. Queued after the record
    // above releases this pass's own slot; a write brings it forward.
    if let Some(due) = next_pass {
        let at = due
            .checked_add_signed(EXPIRY_DELAY)
            .unwrap_or(due)
            .max(Utc::now() + EXPIRY_DELAY);
        schedule_pass_at::<C>(
            task.task_metadata.project_id.clone(),
            entity.clone(),
            at,
            t.transaction(),
        )
        .await?;
    }
    // A purge already pending keeps its time: snapshots due before it wait for
    // it, and it reschedules itself for the rest. `purge_after` is the earliest a
    // snapshot is deleted, not when.
    if let (true, Some(policy)) = (expired > 0, policy) {
        DatasetSnapshotPurgeTask::schedule_task::<C>(
            ScheduleTaskMetadata {
                project_id: task.task_metadata.project_id.clone(),
                parent_task_id: Some(task.task_id()),
                scheduled_for: Some(purge_after(Utc::now(), policy.grace_period)),
                entity,
            },
            DatasetSnapshotPurgePayload::default(),
            t.transaction(),
        )
        .await?;
    }
    t.commit().await?;
    Ok(expired)
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;

    fn node(
        parent: Option<DatasetSnapshotId>,
        age_days: i64,
        now: DateTime<Utc>,
    ) -> DatasetSnapshotNode {
        DatasetSnapshotNode {
            snapshot_id: DatasetSnapshotId::from(Uuid::now_v7()),
            parent_snapshot_id: parent,
            created_at: now - Duration::days(age_days),
            expired: false,
            pinned_until: None,
        }
    }

    fn reference(name: &str, typ: DatasetRefType, head: &DatasetSnapshotNode) -> DatasetRef {
        DatasetRef {
            name: name.to_string(),
            typ,
            snapshot_id: Some(head.snapshot_id),
            protected: false,
        }
    }

    /// A chain of ten daily commits on `main`, oldest first.
    fn chain(now: DateTime<Utc>) -> Vec<DatasetSnapshotNode> {
        let mut nodes: Vec<DatasetSnapshotNode> = Vec::new();
        for age in (0..10).rev() {
            let parent = nodes.last().map(|n| n.snapshot_id);
            nodes.push(node(parent, age, now));
        }
        nodes
    }

    fn ids(nodes: &[DatasetSnapshotNode]) -> Vec<DatasetSnapshotId> {
        nodes.iter().map(|n| n.snapshot_id).collect()
    }

    /// What a pass keeps only for its age, or only for a grant, is looked at again
    /// once that runs out; with nothing waiting on time, no pass is due.
    #[test]
    fn test_the_next_pass_is_due_when_the_youngest_ages_out() {
        let now = Utc::now();
        let mut nodes = chain(now);
        // Ten daily commits under a limit of four and a half days: the oldest the
        // limit keeps is four days old, and reaches it in half a day.
        let limit = Duration::hours(108);
        assert_eq!(
            next_pass_due(&nodes, &[], now, limit),
            Some(nodes[5].created_at + limit)
        );
        assert_eq!(next_pass_due(&nodes, &[], now, Duration::zero()), None);
        let grant_ends = now + Duration::hours(3);
        nodes[0].pinned_until = Some(grant_ends);
        assert_eq!(
            next_pass_due(&nodes, &[], now, Duration::zero()),
            Some(grant_ends)
        );
        // A ref's snapshot is kept whatever its grant: nothing waits on it.
        let refs = [reference("v1", DatasetRefType::Tag, &nodes[0])];
        assert_eq!(next_pass_due(&nodes, &refs, now, Duration::zero()), None);
        nodes[0].expired = true;
        assert_eq!(next_pass_due(&nodes, &[], now, Duration::zero()), None);
    }

    /// Ancestors older than the age limit expire; the head and anything younger
    /// stay.
    #[test]
    fn test_a_branch_keeps_its_recent_history() {
        let now = Utc::now();
        let nodes = chain(now);
        let refs = [reference(
            "main",
            DatasetRefType::Branch,
            nodes.last().unwrap(),
        )];
        let expired = snapshots_to_expire(&nodes, &refs, now, Duration::hours(5 * 24 + 1), 1);
        // Ages 9..=6 are past five days.
        assert_eq!(expired, ids(&nodes[..4]));
    }

    /// `min_keep` holds the newest snapshots of a branch whatever their age, and
    /// the head is always kept.
    #[test]
    fn test_min_keep_holds_the_newest_snapshots() {
        let now = Utc::now();
        let nodes = chain(now);
        let refs = [reference(
            "main",
            DatasetRefType::Branch,
            nodes.last().unwrap(),
        )];
        assert_eq!(
            snapshots_to_expire(&nodes, &refs, now, Duration::zero(), 3),
            ids(&nodes[..7])
        );
        assert_eq!(
            snapshots_to_expire(&nodes, &refs, now, Duration::zero(), 1),
            ids(&nodes[..9])
        );
    }

    /// A tag keeps its snapshot however deep in history, and a live grant keeps
    /// the one it reads.
    #[test]
    fn test_tags_and_live_grants_keep_their_snapshots() {
        let now = Utc::now();
        let mut nodes = chain(now);
        nodes[4].pinned_until = Some(now + Duration::hours(1));
        let refs = [
            reference("main", DatasetRefType::Branch, nodes.last().unwrap()),
            reference("v1", DatasetRefType::Tag, &nodes[1]),
        ];
        let expired = snapshots_to_expire(&nodes, &refs, now, Duration::zero(), 1);
        let kept: Vec<_> = [1, 4, 9].map(|i| nodes[i].snapshot_id).to_vec();
        assert!(kept.iter().all(|id| !expired.contains(id)), "{expired:?}");
        assert_eq!(expired.len(), 7);
    }

    /// A deleted branch's snapshots expire once old, and a young one waits; an
    /// already expired snapshot is not expired again.
    #[test]
    fn test_unreachable_snapshots_expire_by_age() {
        let now = Utc::now();
        let mut nodes = vec![node(None, 10, now), node(None, 1, now)];
        nodes.push(DatasetSnapshotNode {
            expired: true,
            ..node(None, 20, now)
        });
        assert_eq!(
            snapshots_to_expire(&nodes, &[], now, Duration::days(5), 1),
            vec![nodes[0].snapshot_id]
        );
    }

    /// A branch whose head the graph lacks keeps everything: its history cannot
    /// be walked, so nothing on it is known to be old enough.
    #[test]
    fn test_a_head_missing_from_the_graph_expires_nothing() {
        let now = Utc::now();
        let nodes = chain(now);
        let committed_since = node(nodes.last().map(|n| n.snapshot_id), 0, now);
        let refs = [reference("main", DatasetRefType::Branch, &committed_since)];
        assert!(snapshots_to_expire(&nodes, &refs, now, Duration::zero(), 1).is_empty());
    }

    /// A policy no clock can hold expires nothing and purges never: the arithmetic
    /// saturates where it would overflow.
    #[test]
    fn test_durations_past_what_time_holds_saturate() {
        let now = Utc::now();
        let nodes = chain(now);
        let refs = [reference(
            "main",
            DatasetRefType::Branch,
            nodes.last().unwrap(),
        )];
        assert!(snapshots_to_expire(&nodes, &refs, now, Duration::MAX, 1).is_empty());
        assert_eq!(purge_after(now, Duration::MAX), DateTime::<Utc>::MAX_UTC);
    }

    fn warehouse(config: serde_json::Value) -> DatasetSnapshotExpiryQueueConfig {
        serde_json::from_value(config).unwrap()
    }

    /// A dataset that inherits follows the warehouse, off unless it is enabled.
    #[test]
    fn test_an_inheriting_dataset_follows_the_warehouse() {
        let off = warehouse(serde_json::json!({"grace-period": "P1D"}));
        assert_eq!(
            EffectiveRetention::resolve(&DatasetRetention::Inherit {}, &off),
            EffectiveRetention {
                automatic: None,
                grace_period: Duration::days(1),
            }
        );
        let on = warehouse(serde_json::json!({
            "enabled": true,
            "max-snapshot-age": "P2D",
            "min-snapshots-to-keep": 3,
        }));
        assert_eq!(
            EffectiveRetention::resolve(&DatasetRetention::Inherit {}, &on),
            EffectiveRetention {
                automatic: Some((Duration::days(2), 3)),
                grace_period: DEFAULT_GRACE_PERIOD,
            }
        );
    }

    /// A dataset's own policy applies whether or not the warehouse's is enabled,
    /// and borrows only the grace period it leaves unset.
    #[test]
    fn test_a_dataset_policy_overrides_the_warehouse() {
        let on = warehouse(serde_json::json!({
            "enabled": true,
            "max-snapshot-age": "P2D",
            "grace-period": "P3D",
        }));
        let off = DatasetSnapshotExpiryQueueConfig::default();

        let manual = DatasetRetention::Manual { grace_period: None };
        assert_eq!(
            EffectiveRetention::resolve(&manual, &on),
            EffectiveRetention {
                automatic: None,
                grace_period: Duration::days(3),
            }
        );

        let ttl = DatasetRetention::Ttl {
            max_snapshot_age: Duration::days(30),
            min_snapshots_to_keep: None,
            grace_period: Some(Duration::zero()),
        };
        assert_eq!(
            EffectiveRetention::resolve(&ttl, &off),
            EffectiveRetention {
                automatic: Some((Duration::days(30), 1)),
                grace_period: Duration::zero(),
            }
        );

        let max_count = DatasetRetention::MaxCount {
            max_snapshots: 5,
            grace_period: None,
        };
        assert_eq!(
            EffectiveRetention::resolve(&max_count, &off),
            EffectiveRetention {
                automatic: Some((Duration::zero(), 5)),
                grace_period: DEFAULT_GRACE_PERIOD,
            }
        );
    }
}
