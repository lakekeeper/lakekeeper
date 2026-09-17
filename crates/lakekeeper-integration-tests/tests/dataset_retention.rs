//! Snapshot retention: expiry hides a snapshot, purge deletes it, restore brings
//! it back in between.
//!
//! The purge test is the one the design rests on: a survivor that reconstructed
//! through purged snapshots must read exactly the same files afterwards.
use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

use chrono::{TimeZone as _, Utc};
use http::StatusCode;
use iceberg::NamespaceIdent;
use lakekeeper::{
    api::{
        ApiContext, ErrorModel,
        data::v1::datasets::{
            CommitDatasetRequest, CommitFile, CreateDatasetAccessGrantRequest,
            CreateDatasetRefRequest, DatasetParameters, DatasetRefParameters, DatasetRefSource,
            DatasetService as _, DatasetSnapshotParameters, DiffDatasetQuery,
            ExpireDatasetSnapshotResponse, ImportDatasetRequest, LoadDatasetResponse,
            UpdateDatasetSettingsRequest,
        },
        iceberg::types::{DropParams, Prefix},
        management::v1::{
            task_queue::{QueueConfig, SetTaskQueueConfigRequest},
            warehouse::TabularDeleteProfile,
        },
    },
    server::CatalogServer,
    service::{
        CatalogDatasetOps, CatalogStore, CatalogTaskOps, ConstraintViolationPolicy,
        DatasetAccessGrantCreation, DatasetAccessGrantId, DatasetConstraints, DatasetId,
        DatasetRefType, DatasetRetention, DatasetSnapshotId, ManifestEntry, StagedChanges, State,
        Transaction,
        authz::{AllowAllAuthorizer, tests::HidingAuthorizer},
        tasks::dataset_snapshot_expiry_queue,
    },
};
use lakekeeper_integration_tests::{
    TestWarehouseResponse, create_dataset, create_ns, memory_io_profile, random_request_metadata,
    setup, spawn_build_in_queues,
};
use lakekeeper_io::{LakekeeperStorage as _, memory::MemoryStorage};
use lakekeeper_storage_postgres::{PostgresBackend, SecretsState};
use sqlx::PgPool;
use uuid::Uuid;

type Ctx = ApiContext<State<AllowAllAuthorizer, PostgresBackend, SecretsState>>;

const DS: &str = "images";

struct Dataset {
    ctx: Ctx,
    pool: PgPool,
    prefix: String,
    ns: String,
    id: DatasetId,
}

async fn make_dataset(pool: PgPool) -> (Dataset, TestWarehouseResponse) {
    let (ctx, warehouse) = setup(
        pool.clone(),
        memory_io_profile(),
        None,
        AllowAllAuthorizer::default(),
        TabularDeleteProfile::Hard {},
        None,
        1,
        None,
    )
    .await;
    let prefix = warehouse.warehouse_id.to_string();
    let ns = format!("ns_{}", Uuid::now_v7());
    create_ns(ctx.clone(), prefix.clone(), ns.clone()).await;
    let id = create_dataset(ctx.clone(), prefix.clone(), ns.clone(), DS)
        .await
        .unwrap()
        .dataset
        .id;
    (
        Dataset {
            ctx,
            pool,
            prefix,
            ns,
            id,
        },
        warehouse,
    )
}

fn file(key: &str, etag: &str) -> CommitFile {
    CommitFile {
        logical_key: key.to_string(),
        physical_path: None,
        etag: Some(etag.to_string()),
        size: Some(1),
        content_type: None,
        checksum: None,
        version_id: None,
        last_modified: None,
    }
}

impl Dataset {
    fn params(&self) -> DatasetParameters {
        DatasetParameters {
            prefix: Some(Prefix(self.prefix.clone())),
            namespace: NamespaceIdent::new(self.ns.clone()),
            dataset_name: DS.to_string(),
        }
    }

    fn ref_params(&self, name: &str) -> DatasetRefParameters {
        DatasetRefParameters {
            prefix: Some(Prefix(self.prefix.clone())),
            namespace: NamespaceIdent::new(self.ns.clone()),
            dataset_name: DS.to_string(),
            ref_name: name.to_string(),
        }
    }

    fn warehouse_id(&self) -> lakekeeper::WarehouseId {
        self.prefix.parse::<Uuid>().unwrap().into()
    }

    async fn commit(
        &self,
        parent: Option<DatasetSnapshotId>,
        added: Vec<CommitFile>,
        removed: &[&str],
    ) -> DatasetSnapshotId {
        CatalogServer::commit_dataset(
            self.ref_params("main"),
            CommitDatasetRequest {
                parent_snapshot_id: parent,
                added,
                removed: removed.iter().map(ToString::to_string).collect(),
                summary: None,
                on_constraint_violation: None,
            },
            self.ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap()
        .snapshot_id
    }

    async fn create_ref(
        &self,
        name: &str,
        typ: DatasetRefType,
        snapshot_id: DatasetSnapshotId,
    ) -> lakekeeper::api::Result<()> {
        CatalogServer::create_dataset_ref(
            self.params(),
            CreateDatasetRefRequest {
                name: name.to_string(),
                typ,
                source: DatasetRefSource::Snapshot { snapshot_id },
            },
            self.ctx.clone(),
            random_request_metadata(),
        )
        .await
        .map(|_| ())
    }

    /// `snapshot_id`'s files, key to etag, read the way every reader reads them.
    async fn files(&self, snapshot_id: DatasetSnapshotId) -> BTreeMap<String, Option<String>> {
        let mut t = <PostgresBackend as CatalogStore>::Transaction::begin_read(
            self.ctx.v1_state.catalog.clone(),
        )
        .await
        .unwrap();
        let (files, _) = PostgresBackend::list_snapshot_files(
            self.warehouse_id(),
            self.id,
            snapshot_id,
            None,
            None,
            10_000,
            false,
            t.transaction(),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();
        files.into_iter().map(|f| (f.logical_key, f.etag)).collect()
    }

    async fn expire(&self, ids: &[DatasetSnapshotId], purge_after: chrono::DateTime<Utc>) -> u64 {
        let mut t = <PostgresBackend as CatalogStore>::Transaction::begin_write(
            self.ctx.v1_state.catalog.clone(),
        )
        .await
        .unwrap();
        let expired = PostgresBackend::expire_dataset_snapshots(
            self.warehouse_id(),
            self.id,
            ids,
            purge_after,
            t.transaction(),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();
        expired
    }

    async fn purge(&self) -> u64 {
        let mut t = <PostgresBackend as CatalogStore>::Transaction::begin_write(
            self.ctx.v1_state.catalog.clone(),
        )
        .await
        .unwrap();
        let purge = PostgresBackend::purge_expired_dataset_snapshots(
            self.warehouse_id(),
            self.id,
            t.transaction(),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();
        purge.purged
    }

    fn snapshot_params(&self, snapshot_id: DatasetSnapshotId) -> DatasetSnapshotParameters {
        DatasetSnapshotParameters {
            prefix: Some(Prefix(self.prefix.clone())),
            namespace: NamespaceIdent::new(self.ns.clone()),
            dataset_name: DS.to_string(),
            snapshot_id,
        }
    }

    async fn restore(&self, snapshot_id: DatasetSnapshotId) -> lakekeeper::api::Result<()> {
        CatalogServer::restore_dataset_snapshot(
            self.snapshot_params(snapshot_id),
            self.ctx.clone(),
            random_request_metadata(),
        )
        .await
        .map(|_| ())
    }

    async fn expire_by_hand(
        &self,
        snapshot_id: DatasetSnapshotId,
    ) -> lakekeeper::api::Result<ExpireDatasetSnapshotResponse> {
        CatalogServer::expire_dataset_snapshot(
            self.snapshot_params(snapshot_id),
            self.ctx.clone(),
            random_request_metadata(),
        )
        .await
    }

    async fn set_retention(
        &self,
        retention: DatasetRetention,
    ) -> lakekeeper::api::Result<LoadDatasetResponse> {
        CatalogServer::update_dataset_settings(
            self.params(),
            UpdateDatasetSettingsRequest {
                retention: Some(retention),
                ..UpdateDatasetSettingsRequest::default()
            },
            self.ctx.clone(),
            random_request_metadata(),
        )
        .await
    }

    async fn set_warehouse_retention(
        &self,
        warehouse: &TestWarehouseResponse,
        config: serde_json::Value,
    ) {
        let mut t = <PostgresBackend as CatalogStore>::Transaction::begin_write(
            self.ctx.v1_state.catalog.clone(),
        )
        .await
        .unwrap();
        <PostgresBackend as CatalogTaskOps>::set_task_queue_config(
            warehouse.project_id.clone(),
            Some(warehouse.warehouse_id),
            &dataset_snapshot_expiry_queue::QUEUE_NAME,
            &SetTaskQueueConfigRequest {
                queue_config: QueueConfig::from_json(config),
                max_seconds_since_last_heartbeat: None,
            },
            t.transaction(),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();
    }

    /// Age the dataset's history by an hour, so a pass that expires by age does so
    /// whichever clock, the database's or this process's, runs ahead.
    async fn age_history(&self) {
        sqlx::query(
            "UPDATE dataset_snapshot SET created_at = created_at - interval '1 hour' WHERE dataset_id = $1",
        )
        .bind(*self.id)
        .execute(&self.pool)
        .await
        .unwrap();
    }

    /// Bring the pending retention pass forward to now.
    async fn retention_due_now(&self) {
        sqlx::query(
            "UPDATE task SET scheduled_for = now() WHERE queue_name = 'dataset_snapshot_expiry'",
        )
        .execute(&self.pool)
        .await
        .unwrap();
    }

    /// Run the built-in queues until `done`, for at most 30 seconds.
    async fn run_queues_until(&self, what: &str, done: impl AsyncFn() -> bool) {
        let cancellation_token = lakekeeper::CancellationToken::new();
        let workers = spawn_build_in_queues(
            &self.ctx,
            Some(Duration::from_millis(100)),
            cancellation_token.clone(),
        )
        .await;
        let deadline = Instant::now() + Duration::from_secs(30);
        while !done().await {
            assert!(Instant::now() < deadline, "{what}");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        cancellation_token.cancel();
        let _ = workers.await;
    }

    /// Passes that ran to the end: a failed one expires nothing either.
    async fn finished_passes(&self) -> i64 {
        sqlx::query_scalar(
            "SELECT count(*) FROM task_log WHERE queue_name = 'dataset_snapshot_expiry' AND entity_id = $1 AND status = 'success'",
        )
        .bind(*self.id)
        .fetch_one(&self.pool)
        .await
        .unwrap()
    }

    async fn expired_count(&self) -> i64 {
        sqlx::query_scalar(
            "SELECT count(*) FROM dataset_snapshot WHERE dataset_id = $1 AND status = 'expired'",
        )
        .bind(*self.id)
        .fetch_one(&self.pool)
        .await
        .unwrap()
    }

    async fn begin_write(&self) -> <PostgresBackend as CatalogStore>::Transaction {
        <PostgresBackend as CatalogStore>::Transaction::begin_write(
            self.ctx.v1_state.catalog.clone(),
        )
        .await
        .unwrap()
    }

    /// Expire `snapshot_id` by hand in a transaction of its own, which commits.
    fn expire_in_background(
        &self,
        snapshot_id: DatasetSnapshotId,
    ) -> tokio::task::JoinHandle<Result<chrono::DateTime<Utc>, ErrorModel>> {
        let (catalog, warehouse_id, dataset_id) = (
            self.ctx.v1_state.catalog.clone(),
            self.warehouse_id(),
            self.id,
        );
        tokio::spawn(async move {
            let mut t = <PostgresBackend as CatalogStore>::Transaction::begin_write(catalog)
                .await
                .unwrap();
            let expired = PostgresBackend::expire_dataset_snapshot(
                warehouse_id,
                dataset_id,
                snapshot_id,
                Utc::now() + chrono::Duration::days(1),
                t.transaction(),
            )
            .await
            .map_err(ErrorModel::from)?;
            t.commit().await.unwrap();
            Ok(expired)
        })
    }

    async fn snapshot_count(&self) -> i64 {
        sqlx::query_scalar("SELECT count(*) FROM dataset_snapshot WHERE dataset_id = $1")
            .bind(*self.id)
            .fetch_one(&self.pool)
            .await
            .unwrap()
    }
}

/// Nothing held expires: not a ref's head, not what a live grant reads. What
/// does expire is hidden from every read until it is restored.
#[sqlx::test]
async fn test_an_expired_snapshot_is_hidden_until_restored(pool: PgPool) {
    let (ds, _) = make_dataset(pool).await;
    let c1 = ds.commit(None, vec![file("a", "1")], &[]).await;
    let c2 = ds.commit(Some(c1), vec![file("b", "1")], &[]).await;
    let c3 = ds.commit(Some(c2), vec![file("c", "1")], &[]).await;

    // A reader holds c1 through a grant issued on a tag, then the tag goes.
    ds.create_ref("old", DatasetRefType::Tag, c1).await.unwrap();
    CatalogServer::create_dataset_access_grant(
        ds.ref_params("old"),
        CreateDatasetAccessGrantRequest::default(),
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    CatalogServer::delete_dataset_ref(
        ds.ref_params("old"),
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    let later = Utc::now() + chrono::Duration::days(1);
    assert_eq!(
        ds.expire(&[c1, c2, c3], later).await,
        1,
        "only c2 is unheld"
    );

    let hidden = ds
        .create_ref("back", DatasetRefType::Branch, c2)
        .await
        .unwrap_err();
    assert_eq!(hidden.error.code, StatusCode::NOT_FOUND, "{hidden:?}");
    let hidden = CatalogServer::diff_dataset(
        ds.params(),
        DiffDatasetQuery {
            from_snapshot_id: Some(c2),
            to: Some("main".to_string()),
            ..DiffDatasetQuery::default()
        },
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap_err();
    assert_eq!(hidden.error.code, StatusCode::NOT_FOUND, "{hidden:?}");

    ds.restore(c2).await.unwrap();
    ds.create_ref("back", DatasetRefType::Branch, c2)
        .await
        .unwrap();
    let again = ds.restore(c2).await.unwrap_err();
    assert_eq!(
        again.error.code,
        StatusCode::NOT_FOUND,
        "an active snapshot is not restored"
    );
}

/// Purge deletes expired snapshots and nothing a survivor reads: a tag in the
/// middle of the chain and the head both read the same files afterwards,
/// although every delta between them and the root is gone.
#[sqlx::test]
async fn test_purge_keeps_every_survivor_readable(pool: PgPool) {
    let (ds, _) = make_dataset(pool).await;
    let c1 = ds
        .commit(
            None,
            vec![file("a", "1"), file("b", "1"), file("c", "1")],
            &[],
        )
        .await;
    let c2 = ds.commit(Some(c1), vec![file("d", "1")], &["a"]).await;
    let c3 = ds
        .commit(Some(c2), vec![file("b", "2"), file("e", "1")], &[])
        .await;
    let c4 = ds.commit(Some(c3), vec![file("f", "1")], &[]).await;
    let c5 = ds.commit(Some(c4), vec![], &["c"]).await;
    let c6 = ds.commit(Some(c5), vec![file("g", "1")], &[]).await;
    ds.create_ref("v1", DatasetRefType::Tag, c3).await.unwrap();

    let tag_before = ds.files(c3).await;
    let head_before = ds.files(c6).await;
    assert_eq!(
        tag_before.keys().collect::<Vec<_>>(),
        ["b", "c", "d", "e"],
        "precondition: the tag depends on deltas below it"
    );

    let past = Utc::now() - chrono::Duration::seconds(1);
    assert_eq!(ds.expire(&[c1, c2, c4, c5], past).await, 4);
    assert_eq!(ds.purge().await, 4);

    assert_eq!(ds.snapshot_count().await, 2);
    assert_eq!(ds.files(c3).await, tag_before);
    assert_eq!(ds.files(c6).await, head_before);
    let rows: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM dataset_manifest_entry WHERE snapshot_id = ANY($1)",
    )
    .bind([c1, c2, c4, c5].map(|id| *id).to_vec())
    .fetch_one(&ds.pool)
    .await
    .unwrap();
    assert_eq!(rows, 0, "the purged snapshots' rows are gone");

    // History after the purge still commits on top of the survivors.
    ds.commit(Some(c6), vec![file("h", "1")], &[]).await;
}

/// A snapshot still in its grace period is not purged.
#[sqlx::test]
async fn test_purge_waits_out_the_grace_period(pool: PgPool) {
    let (ds, _) = make_dataset(pool).await;
    let c1 = ds.commit(None, vec![file("a", "1")], &[]).await;
    ds.commit(Some(c1), vec![file("b", "1")], &[]).await;

    assert_eq!(
        ds.expire(&[c1], Utc::now() + chrono::Duration::days(1))
            .await,
        1
    );
    assert_eq!(ds.purge().await, 0);
    assert_eq!(ds.snapshot_count().await, 2);
}

/// End to end, from the warehouse's policy: commits queue one pass between them,
/// the pass expires what the policy lets go, and with no grace period the purge
/// follows at once. A tag keeps its snapshot and the head survives.
#[sqlx::test]
async fn test_the_retention_queues_expire_and_purge_old_history(pool: PgPool) {
    let (ds, warehouse) = make_dataset(pool.clone()).await;
    ds.set_warehouse_retention(
        &warehouse,
        serde_json::json!({
            "enabled": true,
            "max-snapshot-age": "PT0S",
            "min-snapshots-to-keep": 1,
            "grace-period": "PT0S",
        }),
    )
    .await;

    let c1 = ds.commit(None, vec![file("a", "1")], &[]).await;
    let c2 = ds.commit(Some(c1), vec![file("b", "1")], &[]).await;
    let c3 = ds.commit(Some(c2), vec![file("c", "1")], &[]).await;
    let c4 = ds.commit(Some(c3), vec![], &["a"]).await;
    ds.create_ref("v1", DatasetRefType::Tag, c2).await.unwrap();
    let tag_files = ds.files(c2).await;
    let head_files = ds.files(c4).await;

    // Four commits, one pending pass, an hour out: bring it forward.
    let pending: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM task WHERE queue_name = 'dataset_snapshot_expiry' AND entity_id = $1",
    )
    .bind(*ds.id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(pending, 1);
    ds.age_history().await;
    ds.retention_due_now().await;
    ds.run_queues_until("retention did not converge", async || {
        ds.snapshot_count().await <= 2
    })
    .await;

    assert_eq!(ds.files(c2).await, tag_files, "the tag survives");
    assert_eq!(ds.files(c4).await, head_files, "the head survives");
}

/// Retention is off unless the warehouse enables it: with a policy that would
/// expire everything but the head, and no `enabled`, the pass a commit queues runs
/// and expires nothing.
#[sqlx::test]
async fn test_retention_is_off_by_default(pool: PgPool) {
    let (ds, warehouse) = make_dataset(pool).await;
    ds.set_warehouse_retention(
        &warehouse,
        serde_json::json!({
            "max-snapshot-age": "PT0S",
            "grace-period": "PT0S",
        }),
    )
    .await;
    let c1 = ds.commit(None, vec![file("a", "1")], &[]).await;
    ds.commit(Some(c1), vec![file("b", "1")], &[]).await;
    ds.age_history().await;
    ds.retention_due_now().await;
    ds.run_queues_until("the pass never ran", async || {
        ds.finished_passes().await > 0
    })
    .await;

    assert_eq!(ds.expired_count().await, 0);
    assert_eq!(ds.snapshot_count().await, 2);
}

/// A dataset's own policy applies with the warehouse's off: max-count keeps the
/// newest two snapshots of `main`, and with no grace period the purge follows.
#[sqlx::test]
async fn test_a_dataset_policy_expires_with_the_warehouse_off(pool: PgPool) {
    let (ds, _) = make_dataset(pool).await;
    ds.set_retention(DatasetRetention::MaxCount {
        max_snapshots: 2,
        grace_period: Some(chrono::Duration::zero()),
    })
    .await
    .unwrap();
    let c1 = ds.commit(None, vec![file("a", "1")], &[]).await;
    let c2 = ds.commit(Some(c1), vec![file("b", "1")], &[]).await;
    let c3 = ds.commit(Some(c2), vec![file("c", "1")], &[]).await;
    let c4 = ds.commit(Some(c3), vec![], &["a"]).await;
    let kept = (ds.files(c3).await, ds.files(c4).await);

    ds.age_history().await;
    ds.retention_due_now().await;
    ds.run_queues_until("the dataset's policy did not apply", async || {
        ds.snapshot_count().await <= 2
    })
    .await;

    assert_eq!((ds.files(c3).await, ds.files(c4).await), kept);
}

/// Manual keeps what the warehouse's policy would expire; a snapshot expires only
/// when asked to, and waits out the dataset's grace period.
#[sqlx::test]
async fn test_a_manual_policy_expires_only_what_is_asked(pool: PgPool) {
    let (ds, warehouse) = make_dataset(pool).await;
    ds.set_warehouse_retention(
        &warehouse,
        serde_json::json!({
            "enabled": true,
            "max-snapshot-age": "PT0S",
            "grace-period": "PT0S",
        }),
    )
    .await;
    ds.set_retention(DatasetRetention::Manual {
        grace_period: Some(chrono::Duration::days(2)),
    })
    .await
    .unwrap();
    let c1 = ds.commit(None, vec![file("a", "1")], &[]).await;
    ds.commit(Some(c1), vec![file("b", "1")], &[]).await;
    ds.age_history().await;
    ds.retention_due_now().await;
    ds.run_queues_until("the pass never ran", async || {
        ds.finished_passes().await > 0
    })
    .await;
    assert_eq!(ds.expired_count().await, 0);

    let asked = Utc::now();
    let expired = ds.expire_by_hand(c1).await.unwrap();
    let answered = Utc::now();
    assert!(
        (asked + chrono::Duration::days(2)..=answered + chrono::Duration::days(2))
            .contains(&expired.purge_after),
        "the dataset's grace period: {expired:?}"
    );
    assert_eq!(ds.expired_count().await, 1);
}

/// Expiring by hand: an unheld snapshot expires and its purge is queued, and
/// asking again answers the same. A snapshot a ref points at or a live grant
/// reads is refused, and one that does not exist is not found.
#[sqlx::test]
async fn test_expiring_a_snapshot_by_hand(pool: PgPool) {
    let (ds, _) = make_dataset(pool).await;
    let c1 = ds.commit(None, vec![file("a", "1")], &[]).await;
    let c2 = ds.commit(Some(c1), vec![file("b", "1")], &[]).await;
    let c3 = ds.commit(Some(c2), vec![file("c", "1")], &[]).await;
    let c4 = ds.commit(Some(c3), vec![file("d", "1")], &[]).await;
    ds.create_ref("v1", DatasetRefType::Tag, c1).await.unwrap();
    // A grant issued on a tag keeps its snapshot after the tag goes.
    ds.create_ref("old", DatasetRefType::Tag, c2).await.unwrap();
    CatalogServer::create_dataset_access_grant(
        ds.ref_params("old"),
        CreateDatasetAccessGrantRequest::default(),
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    CatalogServer::delete_dataset_ref(
        ds.ref_params("old"),
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    let asked = Utc::now();
    let expired = ds.expire_by_hand(c3).await.unwrap();
    let answered = Utc::now();
    assert_eq!(expired.snapshot_id, c3);
    assert!(
        (asked + chrono::Duration::days(7)..=answered + chrono::Duration::days(7))
            .contains(&expired.purge_after),
        "the default grace period: {expired:?}"
    );
    let again = ds.expire_by_hand(c3).await.unwrap();
    assert_eq!(again.purge_after, expired.purge_after);
    let purges: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM task WHERE queue_name = 'dataset_snapshot_purge' AND entity_id = $1",
    )
    .bind(*ds.id)
    .fetch_one(&ds.pool)
    .await
    .unwrap();
    assert_eq!(purges, 1, "the purge is queued");

    for (held, by) in [(c1, "a tag"), (c2, "a live grant"), (c4, "main's head")] {
        let err = ds.expire_by_hand(held).await.unwrap_err();
        assert_eq!(err.error.code, StatusCode::CONFLICT, "{by}: {err:?}");
        assert_eq!(err.error.r#type, "DatasetSnapshotHeld", "{by}");
    }
    let unknown = ds
        .expire_by_hand(DatasetSnapshotId::from(Uuid::now_v7()))
        .await
        .unwrap_err();
    assert_eq!(unknown.error.code, StatusCode::NOT_FOUND, "{unknown:?}");
    assert_eq!(ds.expired_count().await, 1);

    ds.restore(c3).await.unwrap();
}

/// Expiring answers with the `purge-after` it stored, at the database's
/// microseconds, so a retry answers the same. The nanoseconds are explicit: this
/// machine's clock may not produce any.
#[sqlx::test]
async fn test_expiring_answers_with_the_stored_purge_time(pool: PgPool) {
    let (ds, _) = make_dataset(pool).await;
    let c1 = ds.commit(None, vec![file("a", "1")], &[]).await;
    ds.commit(Some(c1), vec![file("b", "1")], &[]).await;
    let asked = Utc
        .timestamp_opt(Utc::now().timestamp() + 86_400, 123_456_789)
        .unwrap();
    let expire = async || {
        let mut t = <PostgresBackend as CatalogStore>::Transaction::begin_write(
            ds.ctx.v1_state.catalog.clone(),
        )
        .await
        .unwrap();
        let purge_after = PostgresBackend::expire_dataset_snapshot(
            ds.warehouse_id(),
            ds.id,
            c1,
            asked,
            t.transaction(),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();
        purge_after
    };

    let first = expire().await;
    assert_eq!(first.timestamp_subsec_nanos(), 123_456_000, "{first}");
    assert_eq!(expire().await, first);
}

/// Settings keep a policy until it is replaced, a change to constraints alone
/// leaves it be, and a policy that keeps nothing or counts backwards is refused.
#[sqlx::test]
async fn test_retention_settings(pool: PgPool) {
    let (ds, _) = make_dataset(pool).await;
    let policy: DatasetRetention = serde_json::from_value(serde_json::json!({
        "mode": "ttl",
        "max-snapshot-age": "P30D",
        "min-snapshots-to-keep": 3,
    }))
    .unwrap();
    let set = ds.set_retention(policy.clone()).await.unwrap();
    assert_eq!(set.dataset.retention.as_ref(), Some(&policy));

    CatalogServer::update_dataset_settings(
        ds.params(),
        UpdateDatasetSettingsRequest {
            constraints: Some(DatasetConstraints::default()),
            retention: None,
        },
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    let loaded =
        CatalogServer::load_dataset(ds.params(), ds.ctx.clone(), random_request_metadata())
            .await
            .unwrap();
    assert_eq!(loaded.dataset.retention.as_ref(), Some(&policy));

    let inherited = ds
        .set_retention(DatasetRetention::Inherit {})
        .await
        .unwrap();
    assert_eq!(inherited.dataset.retention, None, "back to the warehouse's");

    for invalid in [
        DatasetRetention::MaxCount {
            max_snapshots: 0,
            grace_period: None,
        },
        DatasetRetention::Ttl {
            max_snapshot_age: chrono::Duration::days(-1),
            min_snapshots_to_keep: None,
            grace_period: None,
        },
        DatasetRetention::Manual {
            grace_period: Some(chrono::Duration::seconds(-1)),
        },
        // Parses, but lies past what time can hold.
        DatasetRetention::Ttl {
            max_snapshot_age: chrono::Duration::days(100_000_000),
            min_snapshots_to_keep: None,
            grace_period: None,
        },
        DatasetRetention::Manual {
            grace_period: Some(chrono::Duration::days(100_000_000)),
        },
        DatasetRetention::Ttl {
            max_snapshot_age: chrono::Duration::days(30),
            min_snapshots_to_keep: Some(0),
            grace_period: None,
        },
    ] {
        let err = ds.set_retention(invalid.clone()).await.unwrap_err();
        assert_eq!(err.error.code, StatusCode::BAD_REQUEST, "{invalid:?}");
        assert_eq!(err.error.r#type, "InvalidRetention", "{invalid:?}");
    }
}

/// A retention policy expires snapshots, so setting one takes `UpdateRetention`
/// and changing constraints does not.
#[sqlx::test]
async fn test_setting_a_retention_policy_takes_its_own_action(pool: PgPool) {
    let authorizer = HidingAuthorizer::new();
    let (ctx, warehouse) = setup(
        pool,
        memory_io_profile(),
        None,
        authorizer.clone(),
        TabularDeleteProfile::Hard {},
        None,
        1,
        None,
    )
    .await;
    let prefix = warehouse.warehouse_id.to_string();
    let ns = format!("ns_{}", Uuid::now_v7());
    create_ns(ctx.clone(), prefix.clone(), ns.clone()).await;
    create_dataset(ctx.clone(), prefix.clone(), ns.clone(), DS)
        .await
        .unwrap();
    let update = |request: UpdateDatasetSettingsRequest| {
        CatalogServer::update_dataset_settings(
            DatasetParameters {
                prefix: Some(Prefix(prefix.clone())),
                namespace: NamespaceIdent::new(ns.clone()),
                dataset_name: DS.to_string(),
            },
            request,
            ctx.clone(),
            random_request_metadata(),
        )
    };
    authorizer.block_action("dataset:UpdateRetention");

    update(UpdateDatasetSettingsRequest {
        constraints: Some(DatasetConstraints::default()),
        retention: None,
    })
    .await
    .expect("constraints alone are a write");
    for request in [
        UpdateDatasetSettingsRequest {
            constraints: None,
            retention: Some(DatasetRetention::MaxCount {
                max_snapshots: 1,
                grace_period: Some(chrono::Duration::zero()),
            }),
        },
        UpdateDatasetSettingsRequest {
            constraints: Some(DatasetConstraints::default()),
            retention: Some(DatasetRetention::Inherit {}),
        },
    ] {
        let err = update(request.clone()).await.unwrap_err();
        assert_eq!(
            err.error.code,
            StatusCode::FORBIDDEN,
            "{request:?}: {err:?}"
        );
        assert_eq!(err.error.r#type, "DatasetActionForbidden", "{request:?}");
    }
}

/// Wait until the other side of a race waits on a lock this test holds: only then
/// does committing show what that side does once the lock is released.
async fn until_blocked(pool: &PgPool) {
    until_blocked_at(pool, "").await;
}

/// [`until_blocked`], for a statement containing `fragment`.
async fn until_blocked_at(pool: &PgPool, fragment: &str) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let waiting: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_stat_activity
             WHERE datname = current_database() AND wait_event_type = 'Lock'
               AND strpos(query, $1) > 0",
        )
        .bind(fragment)
        .fetch_one(pool)
        .await
        .unwrap();
        if waiting > 0 {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "the other side never waited on the lock"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// A ref created while an expiry runs keeps its snapshot: the expiry waits for the
/// ref's transaction and then finds the snapshot held.
#[sqlx::test]
async fn test_an_expiry_waits_for_a_ref_being_created(pool: PgPool) {
    let (ds, _) = make_dataset(pool).await;
    let c1 = ds.commit(None, vec![file("a", "1")], &[]).await;
    ds.commit(Some(c1), vec![file("b", "1")], &[]).await;

    let mut creating = ds.begin_write().await;
    PostgresBackend::create_dataset_ref(
        ds.warehouse_id(),
        ds.id,
        "v1",
        DatasetRefType::Tag,
        c1,
        creating.transaction(),
    )
    .await
    .unwrap();
    let expiring = ds.expire_in_background(c1);
    until_blocked(&ds.pool).await;
    creating.commit().await.unwrap();

    let err = expiring.await.unwrap().unwrap_err();
    assert_eq!(err.r#type, "DatasetSnapshotHeld", "{err:?}");
    assert_eq!(ds.expired_count().await, 0);
}

/// A ref created while an expiry is in flight is refused once the expiry commits,
/// and points at nothing expired.
#[sqlx::test]
async fn test_a_ref_waits_for_an_expiry_in_flight(pool: PgPool) {
    let (ds, _) = make_dataset(pool).await;
    let c1 = ds.commit(None, vec![file("a", "1")], &[]).await;
    ds.commit(Some(c1), vec![file("b", "1")], &[]).await;

    let mut expiring = ds.begin_write().await;
    PostgresBackend::expire_dataset_snapshot(
        ds.warehouse_id(),
        ds.id,
        c1,
        Utc::now() + chrono::Duration::days(1),
        expiring.transaction(),
    )
    .await
    .unwrap();
    let creating = tokio::spawn({
        let (catalog, warehouse_id, dataset_id) =
            (ds.ctx.v1_state.catalog.clone(), ds.warehouse_id(), ds.id);
        async move {
            let mut t = <PostgresBackend as CatalogStore>::Transaction::begin_write(catalog)
                .await
                .unwrap();
            let created = PostgresBackend::create_dataset_ref(
                warehouse_id,
                dataset_id,
                "v1",
                DatasetRefType::Tag,
                c1,
                t.transaction(),
            )
            .await
            .map_err(ErrorModel::from)?;
            t.commit().await.unwrap();
            Ok::<_, ErrorModel>(created)
        }
    });
    until_blocked(&ds.pool).await;
    expiring.commit().await.unwrap();

    let err = creating.await.unwrap().unwrap_err();
    assert_eq!(err.r#type, "DatasetSnapshotNotFound", "{err:?}");
    let on_expired: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM dataset_ref r JOIN dataset_snapshot s USING (snapshot_id) \
         WHERE s.dataset_id = $1 AND s.status = 'expired'",
    )
    .bind(*ds.id)
    .fetch_one(&ds.pool)
    .await
    .unwrap();
    assert_eq!(on_expired, 0, "no ref points at an expired snapshot");
}

/// A grant made while an expiry runs keeps its snapshot, as a ref does.
#[sqlx::test]
async fn test_an_expiry_waits_for_a_grant_being_made(pool: PgPool) {
    let (ds, _) = make_dataset(pool).await;
    let c1 = ds.commit(None, vec![file("a", "1")], &[]).await;
    ds.commit(Some(c1), vec![file("b", "1")], &[]).await;

    let mut granting = ds.begin_write().await;
    PostgresBackend::create_dataset_access_grant(
        DatasetAccessGrantCreation {
            grant_id: DatasetAccessGrantId::new_random(),
            warehouse_id: ds.warehouse_id(),
            dataset_id: ds.id,
            snapshot_id: c1,
            ref_name: "main".to_string(),
            actor: "reader".to_string(),
            content_type: None,
            expires_at: Utc::now() + chrono::Duration::hours(1),
            idempotency_key: None,
        },
        granting.transaction(),
    )
    .await
    .unwrap();
    let expiring = ds.expire_in_background(c1);
    until_blocked(&ds.pool).await;
    granting.commit().await.unwrap();

    let err = expiring.await.unwrap().unwrap_err();
    assert_eq!(err.r#type, "DatasetSnapshotHeld", "{err:?}");
}

/// The retention pass waits for a ref being created on a snapshot it planned to
/// expire, and then keeps it.
#[sqlx::test]
async fn test_a_retention_pass_waits_for_a_ref_being_created(pool: PgPool) {
    let (ds, _) = make_dataset(pool).await;
    let c1 = ds.commit(None, vec![file("a", "1")], &[]).await;
    ds.commit(Some(c1), vec![file("b", "1")], &[]).await;

    let mut creating = ds.begin_write().await;
    PostgresBackend::create_dataset_ref(
        ds.warehouse_id(),
        ds.id,
        "v1",
        DatasetRefType::Tag,
        c1,
        creating.transaction(),
    )
    .await
    .unwrap();
    let passing = tokio::spawn({
        let (catalog, warehouse_id, dataset_id) =
            (ds.ctx.v1_state.catalog.clone(), ds.warehouse_id(), ds.id);
        async move {
            let mut t = <PostgresBackend as CatalogStore>::Transaction::begin_write(catalog)
                .await
                .unwrap();
            let expired = PostgresBackend::expire_dataset_snapshots(
                warehouse_id,
                dataset_id,
                &[c1],
                Utc::now() + chrono::Duration::days(1),
                t.transaction(),
            )
            .await
            .unwrap();
            t.commit().await.unwrap();
            expired
        }
    });
    until_blocked(&ds.pool).await;
    creating.commit().await.unwrap();

    assert_eq!(passing.await.unwrap(), 0, "the tag holds it");
    assert_eq!(ds.expired_count().await, 0);
}

/// A staged commit rebased onto a branch that moved keeps its changes, though the
/// snapshot it was built on expired meanwhile: its rows are still there to tell
/// what the staged commit changed and what the branch did.
#[sqlx::test]
async fn test_a_rebase_over_an_expired_parent_keeps_its_changes(pool: PgPool) {
    let (ds, _) = make_dataset(pool).await;
    let base = ds
        .commit(None, vec![file("a", "1"), file("b", "1")], &[])
        .await;

    // A staging snapshot on `base` that rewrites `a`, as an import builds one.
    let staged = DatasetSnapshotId::from(Uuid::now_v7());
    let mut t = ds.begin_write().await;
    PostgresBackend::begin_dataset_commit(
        ds.warehouse_id(),
        ds.id,
        "main",
        staged,
        Some(base),
        "memory://retention",
        None,
        t.transaction(),
    )
    .await
    .unwrap();
    PostgresBackend::stage_dataset_files(
        ds.warehouse_id(),
        ds.id,
        staged,
        &[],
        &[ManifestEntry {
            logical_key: "a".to_string(),
            physical_path: "a".to_string(),
            etag: Some("2".to_string()),
            size: Some(1),
            content_type: None,
            checksum: None,
            version_id: None,
            last_modified: None,
        }],
        &[],
        ConstraintViolationPolicy::Reject,
        t.transaction(),
    )
    .await
    .unwrap();
    t.commit().await.unwrap();

    // The branch moves on without touching `a`, and nothing holds `base` any more:
    // it expires, due at once, and the purge leaves it for the staging snapshot.
    let moved = ds.commit(Some(base), vec![file("c", "1")], &[]).await;
    assert_eq!(
        ds.expire(&[base], Utc::now() - chrono::Duration::seconds(1))
            .await,
        1
    );
    assert_eq!(ds.purge().await, 0, "a staging snapshot builds on it");

    let mut t = ds.begin_write().await;
    let dropped = PostgresBackend::rebase_staging_snapshot(
        ds.warehouse_id(),
        ds.id,
        staged,
        Some(moved),
        t.transaction(),
    )
    .await
    .unwrap();
    t.commit().await.unwrap();
    assert_eq!(
        dropped,
        StagedChanges::default(),
        "the rewrite of `a` survives"
    );
}

/// A staged commit rebased onto a head that expired since it was read keeps its
/// changes: the head's rows still tell what the branch did.
#[sqlx::test]
async fn test_a_rebase_onto_an_expired_head_keeps_its_changes(pool: PgPool) {
    let (ds, _) = make_dataset(pool).await;
    let base = ds
        .commit(None, vec![file("a", "1"), file("b", "1")], &[])
        .await;

    let staged = DatasetSnapshotId::from(Uuid::now_v7());
    let mut t = ds.begin_write().await;
    PostgresBackend::begin_dataset_commit(
        ds.warehouse_id(),
        ds.id,
        "main",
        staged,
        Some(base),
        "memory://retention",
        None,
        t.transaction(),
    )
    .await
    .unwrap();
    PostgresBackend::stage_dataset_files(
        ds.warehouse_id(),
        ds.id,
        staged,
        &[],
        &[ManifestEntry {
            logical_key: "a".to_string(),
            physical_path: "a".to_string(),
            etag: Some("2".to_string()),
            size: Some(1),
            content_type: None,
            checksum: None,
            version_id: None,
            last_modified: None,
        }],
        &[],
        ConstraintViolationPolicy::Reject,
        t.transaction(),
    )
    .await
    .unwrap();
    t.commit().await.unwrap();

    // The import read `read` as the head, then the branch moved past it and it
    // expired, all before the rebase.
    let read = ds.commit(Some(base), vec![file("c", "1")], &[]).await;
    ds.commit(Some(read), vec![file("d", "1")], &[]).await;
    assert_eq!(
        ds.expire(&[read], Utc::now() + chrono::Duration::days(1))
            .await,
        1
    );

    let mut t = ds.begin_write().await;
    let dropped = PostgresBackend::rebase_staging_snapshot(
        ds.warehouse_id(),
        ds.id,
        staged,
        Some(read),
        t.transaction(),
    )
    .await
    .unwrap();
    t.commit().await.unwrap();
    assert_eq!(
        dropped,
        StagedChanges::default(),
        "the rewrite of `a` survives"
    );
}

/// A branch reset while a retention pass reads the dataset keeps what it now
/// needs: the pass reads the refs after the graph, so it walks them as they stand.
#[sqlx::test]
async fn test_a_retention_pass_walks_a_branch_reset_while_it_reads(pool: PgPool) {
    let (ds, _) = make_dataset(pool).await;
    let c1 = ds.commit(None, vec![file("a", "1")], &[]).await;
    let c2 = ds.commit(Some(c1), vec![file("b", "1")], &[]).await;
    let c3 = ds.commit(Some(c2), vec![file("c", "1")], &[]).await;
    let c4 = ds.commit(Some(c3), vec![file("d", "1")], &[]).await;
    ds.set_retention(DatasetRetention::MaxCount {
        max_snapshots: 2,
        grace_period: None,
    })
    .await
    .unwrap();
    ds.age_history().await;

    // Hold the pass at its read of the graph, and reset `main` to `c2` meanwhile.
    let mut resetting = ds.begin_write().await;
    sqlx::query("LOCK TABLE dataset_snapshot IN ACCESS EXCLUSIVE MODE")
        .execute(&mut **resetting.transaction())
        .await
        .unwrap();
    ds.retention_due_now().await;
    let cancellation_token = lakekeeper::CancellationToken::new();
    let workers = spawn_build_in_queues(
        &ds.ctx,
        Some(Duration::from_millis(100)),
        cancellation_token.clone(),
    )
    .await;
    until_blocked_at(&ds.pool, "pinned_until").await;
    PostgresBackend::move_dataset_ref(
        ds.warehouse_id(),
        ds.id,
        "main",
        c2,
        Some(c4),
        false,
        resetting.transaction(),
    )
    .await
    .unwrap();
    resetting.commit().await.unwrap();

    let deadline = Instant::now() + Duration::from_secs(30);
    while ds.finished_passes().await == 0 {
        assert!(Instant::now() < deadline, "the pass did not finish");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    cancellation_token.cancel();
    let _ = workers.await;
    let expired: Vec<Uuid> = sqlx::query_scalar(
        "SELECT snapshot_id FROM dataset_snapshot WHERE dataset_id = $1 AND status = 'expired' \
         ORDER BY snapshot_id",
    )
    .bind(*ds.id)
    .fetch_all(&ds.pool)
    .await
    .unwrap();
    assert_eq!(
        expired,
        vec![*c3, *c4],
        "`main` keeps `c2` and `c1`; the history it left expires"
    );
}

/// A checkpoint waits for a purge in flight. The purge folds the snapshot above the
/// cut into a checkpoint and deletes the rows below; a checkpoint that read the
/// chain before that and its rows after would read past the fold into rows the
/// purge cut off, and restate a file the purged snapshot had removed.
#[sqlx::test]
async fn test_a_checkpoint_waits_for_a_purge_in_flight(pool: PgPool) {
    let (ds, _) = make_dataset(pool).await;
    let first = ds.commit(None, vec![file("k", "1")], &[]).await;
    ds.create_ref("keep", DatasetRefType::Tag, first)
        .await
        .unwrap();
    let removing = ds.commit(Some(first), vec![], &["k"]).await;
    // Far enough past the purge's fold that the branch is due one of its own.
    let mut head = ds.commit(Some(removing), vec![file("a", "1")], &[]).await;
    for i in 0..25 {
        head = ds
            .commit(Some(head), vec![file(&format!("f{i}"), "1")], &[])
            .await;
    }
    assert_eq!(
        ds.expire(&[removing], Utc::now() - chrono::Duration::seconds(1))
            .await,
        1
    );

    let mut purging = ds.begin_write().await;
    let purge = PostgresBackend::purge_expired_dataset_snapshots(
        ds.warehouse_id(),
        ds.id,
        purging.transaction(),
    )
    .await
    .unwrap();
    assert_eq!(purge.purged, 1);
    let checkpointing = tokio::spawn({
        let (catalog, warehouse_id, dataset_id) =
            (ds.ctx.v1_state.catalog.clone(), ds.warehouse_id(), ds.id);
        async move {
            let mut t = <PostgresBackend as CatalogStore>::Transaction::begin_write(catalog)
                .await
                .unwrap();
            let folded = PostgresBackend::checkpoint_dataset_branch(
                warehouse_id,
                dataset_id,
                "main",
                t.transaction(),
            )
            .await
            .unwrap();
            t.commit().await.unwrap();
            folded
        }
    });
    until_blocked_at(&ds.pool, "pg_advisory_xact_lock").await;
    purging.commit().await.unwrap();

    assert_eq!(checkpointing.await.unwrap(), Some(head));
    assert!(
        !ds.files(head).await.contains_key("k"),
        "the removal survives the fold"
    );
}

/// A pass that keeps a snapshot only because it is too young queues the next for
/// when it ages out, so history ages out though nobody writes to the dataset.
#[sqlx::test]
async fn test_a_pass_queues_the_next_for_history_still_too_young(pool: PgPool) {
    let (ds, _) = make_dataset(pool).await;
    let c1 = ds.commit(None, vec![file("a", "1")], &[]).await;
    ds.commit(Some(c1), vec![file("b", "1")], &[]).await;
    ds.set_retention(DatasetRetention::Ttl {
        max_snapshot_age: chrono::Duration::days(1),
        min_snapshots_to_keep: None,
        grace_period: None,
    })
    .await
    .unwrap();
    ds.retention_due_now().await;
    ds.run_queues_until("the pass did not run", async || {
        ds.finished_passes().await >= 1
    })
    .await;

    let next: Vec<chrono::DateTime<Utc>> = sqlx::query_scalar(
        "SELECT scheduled_for FROM task WHERE queue_name = 'dataset_snapshot_expiry' AND entity_id = $1",
    )
    .bind(*ds.id)
    .fetch_all(&ds.pool)
    .await
    .unwrap();
    assert_eq!(next.len(), 1, "the next pass is queued");
    assert!(
        next[0] > Utc::now() + chrono::Duration::hours(23),
        "for when `c1` ages out: {next:?}"
    );
}

/// A write brings forward a pass that rescheduled itself for later: the write's
/// pass is an hour out, whatever the last pass found.
#[sqlx::test]
async fn test_a_write_brings_a_rescheduled_pass_forward(pool: PgPool) {
    let (ds, _) = make_dataset(pool).await;
    let c1 = ds.commit(None, vec![file("a", "1")], &[]).await;
    let c2 = ds.commit(Some(c1), vec![file("b", "1")], &[]).await;
    ds.set_retention(DatasetRetention::Ttl {
        max_snapshot_age: chrono::Duration::days(30),
        min_snapshots_to_keep: None,
        grace_period: None,
    })
    .await
    .unwrap();
    ds.retention_due_now().await;
    ds.run_queues_until("the pass did not run", async || {
        ds.finished_passes().await >= 1
    })
    .await;
    let pending = || async {
        sqlx::query_scalar::<_, chrono::DateTime<Utc>>(
            "SELECT scheduled_for FROM task WHERE queue_name = 'dataset_snapshot_expiry' AND entity_id = $1",
        )
        .bind(*ds.id)
        .fetch_all(&ds.pool)
        .await
        .unwrap()
    };
    assert!(
        pending().await[0] > Utc::now() + chrono::Duration::days(29),
        "rescheduled for when `c1` ages out"
    );

    ds.commit(Some(c2), vec![file("c", "1")], &[]).await;
    let next = pending().await;
    assert_eq!(next.len(), 1);
    assert!(
        next[0] <= Utc::now() + chrono::Duration::hours(1),
        "the commit's pass is an hour out: {next:?}"
    );
}

/// While an import stages into a dataset, its purge waits: the import's rebase
/// walks the chain from the head it began on, which a purge would cut.
#[sqlx::test]
async fn test_a_purge_waits_for_an_import_staging_into_the_dataset(pool: PgPool) {
    let (ds, _) = make_dataset(pool).await;
    let base = ds.commit(None, vec![file("a", "1")], &[]).await;
    let staged = DatasetSnapshotId::from(Uuid::now_v7());
    let mut t = ds.begin_write().await;
    PostgresBackend::begin_dataset_commit(
        ds.warehouse_id(),
        ds.id,
        "main",
        staged,
        Some(base),
        "memory://retention",
        None,
        t.transaction(),
    )
    .await
    .unwrap();
    t.commit().await.unwrap();
    let between = ds.commit(Some(base), vec![file("b", "1")], &[]).await;
    ds.commit(Some(between), vec![file("c", "1")], &[]).await;
    assert_eq!(
        ds.expire(&[between], Utc::now() - chrono::Duration::seconds(1))
            .await,
        1
    );

    assert_eq!(ds.purge().await, 0, "the import holds it");
    let mut t = ds.begin_write().await;
    PostgresBackend::abort_dataset_commit(ds.warehouse_id(), ds.id, staged, t.transaction())
        .await
        .unwrap();
    t.commit().await.unwrap();
    assert_eq!(ds.purge().await, 1, "purged once the import is done");
}

/// An import dropped mid-flight, as the request layer drops one at
/// `max-request-time` and when the client hangs up, stops holding the purge.
#[sqlx::test]
async fn test_an_import_dropped_mid_flight_stops_holding_the_purge(pool: PgPool) {
    let (ds, _) = make_dataset(pool).await;
    let base = ds.commit(None, vec![file("a", "1")], &[]).await;
    ds.commit(Some(base), vec![file("b", "1")], &[]).await;
    assert_eq!(
        ds.expire(&[base], Utc::now() - chrono::Duration::seconds(1))
            .await,
        1
    );
    let loaded =
        CatalogServer::load_dataset(ds.params(), ds.ctx.clone(), random_request_metadata())
            .await
            .unwrap();
    MemoryStorage::new()
        .write(
            &format!("{}/c", loaded.dataset.location.trim_end_matches('/')),
            bytes::Bytes::from_static(b"x"),
        )
        .await
        .unwrap();

    // Hold the import at its pointer move, past everything it staged, and drop it
    // there.
    let mut moving = ds.begin_write().await;
    sqlx::query("SELECT 1 FROM dataset_ref WHERE dataset_id = $1 AND name = 'main' FOR UPDATE")
        .bind(*ds.id)
        .execute(&mut **moving.transaction())
        .await
        .unwrap();
    let import = tokio::spawn(CatalogServer::import_dataset(
        ds.params(),
        ImportDatasetRequest::default(),
        ds.ctx.clone(),
        random_request_metadata(),
    ));
    until_blocked_at(&ds.pool, "UPDATE dataset_ref").await;
    import.abort();
    assert!(import.await.unwrap_err().is_cancelled());
    moving.commit().await.unwrap();

    let deadline = Instant::now() + Duration::from_secs(10);
    while ds.purge().await == 0 {
        assert!(
            Instant::now() < deadline,
            "the dropped import still holds the purge"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// A reader re-reads the chain after the rows: a purge committing between the two
/// folds a snapshot into a checkpoint and deletes the rows below it, and rows read
/// against the old chain would bring back a file a purged snapshot removed.
#[sqlx::test]
async fn test_a_listing_rereads_a_chain_purged_under_it(pool: PgPool) {
    let (ds, _) = make_dataset(pool).await;
    let first = ds.commit(None, vec![file("k", "1")], &[]).await;
    ds.create_ref("keep", DatasetRefType::Tag, first)
        .await
        .unwrap();
    let removing = ds.commit(Some(first), vec![], &["k"]).await;
    let boundary = ds.commit(Some(removing), vec![file("a", "1")], &[]).await;
    let head = ds.commit(Some(boundary), vec![file("b", "1")], &[]).await;
    assert_eq!(
        ds.expire(&[removing], Utc::now() - chrono::Duration::seconds(1))
            .await,
        1
    );

    // Hold the listing between its chain and its rows, and purge meanwhile.
    let mut purging = ds.begin_write().await;
    sqlx::query("LOCK TABLE dataset_manifest_entry IN ACCESS EXCLUSIVE MODE")
        .execute(&mut **purging.transaction())
        .await
        .unwrap();
    let listing = tokio::spawn({
        let (catalog, warehouse_id, dataset_id) =
            (ds.ctx.v1_state.catalog.clone(), ds.warehouse_id(), ds.id);
        async move {
            let mut t = <PostgresBackend as CatalogStore>::Transaction::begin_read(catalog)
                .await
                .unwrap();
            let (files, _) = PostgresBackend::list_snapshot_files(
                warehouse_id,
                dataset_id,
                head,
                None,
                None,
                10_000,
                false,
                t.transaction(),
            )
            .await
            .unwrap();
            t.commit().await.unwrap();
            files.into_iter().map(|f| f.logical_key).collect::<Vec<_>>()
        }
    });
    until_blocked_at(&ds.pool, "logical_key >= $5").await;
    let purge = PostgresBackend::purge_expired_dataset_snapshots(
        ds.warehouse_id(),
        ds.id,
        purging.transaction(),
    )
    .await
    .unwrap();
    assert_eq!(purge.purged, 1);
    purging.commit().await.unwrap();

    assert_eq!(listing.await.unwrap(), ["a", "b"]);
}

/// A dataset is purged at most once a day: a purge that cuts history folds a
/// checkpoint, which restates every file, so one fold covers a day of expiries.
#[sqlx::test]
async fn test_purges_of_a_dataset_run_a_day_apart(pool: PgPool) {
    let (ds, _) = make_dataset(pool).await;
    let c1 = ds.commit(None, vec![file("a", "1")], &[]).await;
    let c2 = ds.commit(Some(c1), vec![file("b", "1")], &[]).await;
    ds.commit(Some(c2), vec![file("c", "1")], &[]).await;
    ds.set_retention(DatasetRetention::Manual {
        grace_period: Some(chrono::Duration::zero()),
    })
    .await
    .unwrap();
    ds.expire_by_hand(c1).await.unwrap();
    // Due a minute later, while the purge of `c1` is pending.
    ds.set_retention(DatasetRetention::Manual {
        grace_period: Some(chrono::Duration::minutes(1)),
    })
    .await
    .unwrap();
    ds.expire_by_hand(c2).await.unwrap();

    ds.run_queues_until("`c1` was not purged", async || {
        ds.snapshot_count().await == 2
    })
    .await;
    let next: Vec<chrono::DateTime<Utc>> = sqlx::query_scalar(
        "SELECT scheduled_for FROM task WHERE queue_name = 'dataset_snapshot_purge' AND entity_id = $1",
    )
    .bind(*ds.id)
    .fetch_all(&ds.pool)
    .await
    .unwrap();
    assert_eq!(next.len(), 1, "the next purge is queued");
    assert!(
        next[0] > Utc::now() + chrono::Duration::hours(23),
        "a day after this one: {next:?}"
    );
}

/// A hard drop cancels what is queued for the dataset: a retention pass or a purge
/// is queued for days ahead, and an open task would hold up the warehouse's
/// deletion.
#[sqlx::test]
async fn test_dropping_a_dataset_cancels_its_queued_tasks(pool: PgPool) {
    let (ds, _) = make_dataset(pool).await;
    ds.commit(None, vec![file("a", "1")], &[]).await;
    let queued = || async {
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM task WHERE entity_id = $1")
            .bind(*ds.id)
            .fetch_one(&ds.pool)
            .await
            .unwrap()
    };
    assert!(queued().await > 0, "the commit queued a retention pass");

    CatalogServer::drop_dataset(
        ds.params(),
        DropParams {
            purge_requested: false,
            force: false,
        },
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    assert_eq!(queued().await, 0);
}

/// Constraints and retention each change under their own authority, so a request
/// carrying both needs both: one refused `UpdateSettings` may set a policy, but not
/// slip a constraints change in beside it.
#[sqlx::test]
async fn test_a_request_with_both_settings_takes_both_actions(pool: PgPool) {
    let authorizer = HidingAuthorizer::new();
    let (ctx, warehouse) = setup(
        pool,
        memory_io_profile(),
        None,
        authorizer.clone(),
        TabularDeleteProfile::Hard {},
        None,
        1,
        None,
    )
    .await;
    let prefix = warehouse.warehouse_id.to_string();
    let ns = format!("ns_{}", Uuid::now_v7());
    create_ns(ctx.clone(), prefix.clone(), ns.clone()).await;
    create_dataset(ctx.clone(), prefix.clone(), ns.clone(), DS)
        .await
        .unwrap();
    let update = |request: UpdateDatasetSettingsRequest| {
        CatalogServer::update_dataset_settings(
            DatasetParameters {
                prefix: Some(Prefix(prefix.clone())),
                namespace: NamespaceIdent::new(ns.clone()),
                dataset_name: DS.to_string(),
            },
            request,
            ctx.clone(),
            random_request_metadata(),
        )
    };
    authorizer.block_action("dataset:UpdateSettings");

    update(UpdateDatasetSettingsRequest {
        constraints: None,
        retention: Some(DatasetRetention::Manual { grace_period: None }),
    })
    .await
    .expect("a policy alone takes UpdateRetention");
    let err = update(UpdateDatasetSettingsRequest {
        constraints: Some(DatasetConstraints::default()),
        retention: Some(DatasetRetention::Inherit {}),
    })
    .await
    .unwrap_err();
    assert_eq!(err.error.code, StatusCode::FORBIDDEN, "{err:?}");
}

/// A policy set on a dataset nobody writes to still applies: the change queues a
/// retention pass, as a commit does.
#[sqlx::test]
async fn test_a_policy_change_queues_a_retention_pass(pool: PgPool) {
    let (ds, _) = make_dataset(pool).await;
    let c1 = ds.commit(None, vec![file("a", "1")], &[]).await;
    ds.commit(Some(c1), vec![file("b", "1")], &[]).await;
    // Drop the pass the commits queued, so only the policy change can queue one.
    sqlx::query("DELETE FROM task WHERE queue_name = 'dataset_snapshot_expiry'")
        .execute(&ds.pool)
        .await
        .unwrap();

    ds.set_retention(DatasetRetention::MaxCount {
        max_snapshots: 1,
        grace_period: Some(chrono::Duration::zero()),
    })
    .await
    .unwrap();
    let pending: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM task WHERE queue_name = 'dataset_snapshot_expiry' AND entity_id = $1",
    )
    .bind(*ds.id)
    .fetch_one(&ds.pool)
    .await
    .unwrap();
    assert_eq!(pending, 1);

    ds.age_history().await;
    ds.retention_due_now().await;
    ds.run_queues_until("the policy did not apply", async || {
        ds.snapshot_count().await <= 1
    })
    .await;
}

/// A dataset that inherits expires by hand with the warehouse's grace period.
#[sqlx::test]
async fn test_expiring_by_hand_takes_the_warehouse_grace_period(pool: PgPool) {
    let (ds, warehouse) = make_dataset(pool).await;
    ds.set_warehouse_retention(&warehouse, serde_json::json!({"grace-period": "P3D"}))
        .await;
    let c1 = ds.commit(None, vec![file("a", "1")], &[]).await;
    ds.commit(Some(c1), vec![file("b", "1")], &[]).await;

    let asked = Utc::now();
    let expired = ds.expire_by_hand(c1).await.unwrap();
    let answered = Utc::now();
    assert!(
        (asked + chrono::Duration::days(3)..=answered + chrono::Duration::days(3))
            .contains(&expired.purge_after),
        "{expired:?}"
    );
}
