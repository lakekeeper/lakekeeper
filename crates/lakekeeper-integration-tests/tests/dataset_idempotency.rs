//! Retrying a dataset mutation with its `Idempotency-Key`.
//!
//! A lost response is the case the key exists for: the mutation happened, the
//! caller does not know it, and the retry must neither repeat it nor fail. Each
//! test here performs an operation, changes the state underneath it where that
//! matters, and replays it.
use http::StatusCode;
use lakekeeper::{
    api::{
        RequestMetadata, Result,
        data::v1::datasets::{
            CommitDatasetRequest, CommitFile, CreateDatasetAccessGrantRequest,
            CreateDatasetRefRequest, DatasetRefSource, DatasetService as _, MoveDatasetRefRequest,
            UpdateDatasetSettingsRequest,
        },
    },
    server::CatalogServer,
    service::{
        CatalogDatasetOps, ConstraintViolationPolicy, DatasetConstraints, DatasetOwnership,
        DatasetRefType, DatasetSnapshotId, Transaction, authz::tests::HidingAuthorizer,
        idempotency::IdempotencyKey,
    },
};
use lakekeeper_integration_tests::{
    CommitFileExt as _, DATASET, TestDataset, TestNamespace, file, metadata_with_key,
    metadata_with_key_as, new_key, random_request_metadata,
};
use lakekeeper_storage_postgres::PostgresBackend;
use sqlx::PgPool;

fn commit_request(parent: Option<DatasetSnapshotId>, key: &str) -> CommitDatasetRequest {
    CommitDatasetRequest {
        parent_snapshot_id: parent,
        added: vec![file(key).content_type("image/jpeg")],
        removed: vec![],
        summary: None,
        on_constraint_violation: None,
    }
}

async fn commit_on(
    ds: &TestDataset,
    request: CommitDatasetRequest,
    metadata: RequestMetadata,
) -> Result<DatasetSnapshotId> {
    CatalogServer::commit_dataset(ds.ref_params("main"), request, ds.ctx.clone(), metadata)
        .await
        .map(|s| s.snapshot_id)
}

async fn snapshot_count(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM dataset_snapshot")
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn grant_count(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM dataset_access_grant")
        .fetch_one(pool)
        .await
        .unwrap()
}

#[sqlx::test]
async fn test_a_replayed_commit_answers_with_its_own_snapshot(pool: PgPool) {
    let ds = TestDataset::managed(pool.clone()).await;
    let key = new_key();

    let first = commit_on(&ds, commit_request(None, "a.jpg"), metadata_with_key(key))
        .await
        .expect("keyed commit");
    // Another writer lands on top before the retry arrives.
    let second = commit_on(
        &ds,
        commit_request(Some(first), "b.jpg"),
        random_request_metadata(),
    )
    .await
    .expect("second commit");
    let snapshots = snapshot_count(&pool).await;

    // Its parent is stale now, so executing it again would be a 409.
    let replayed = commit_on(&ds, commit_request(None, "a.jpg"), metadata_with_key(key))
        .await
        .expect("the replay succeeds");

    assert_eq!(
        replayed, first,
        "the snapshot the commit published, not the head"
    );
    assert_ne!(replayed, second);
    assert_eq!(snapshot_count(&pool).await, snapshots);
}

#[sqlx::test]
async fn test_a_failed_commit_leaves_its_key_unspent(pool: PgPool) {
    let ds = TestDataset::managed(pool).await;
    let head = commit_on(
        &ds,
        commit_request(None, "a.jpg"),
        random_request_metadata(),
    )
    .await
    .unwrap();
    let key = new_key();

    let err = commit_on(&ds, commit_request(None, "b.jpg"), metadata_with_key(key))
        .await
        .expect_err("a stale parent conflicts");
    assert_eq!(err.error.code, StatusCode::CONFLICT, "got: {err:?}");

    // The rebased retry is a new attempt, and it runs.
    let rebased = commit_on(
        &ds,
        commit_request(Some(head), "b.jpg"),
        metadata_with_key(key),
    )
    .await
    .expect("the rebased retry commits");
    assert_ne!(rebased, head);
}

#[sqlx::test]
async fn test_a_commit_key_spent_on_another_dataset_is_refused(pool: PgPool) {
    let ns = TestNamespace::new(pool).await;
    let ds = ns
        .create_dataset(DATASET, DatasetOwnership::Managed, None)
        .await;
    let labels = ns
        .create_dataset("labels", DatasetOwnership::Managed, None)
        .await;
    let key = new_key();
    commit_on(&ds, commit_request(None, "a.jpg"), metadata_with_key(key))
        .await
        .unwrap();

    let err = commit_on(
        &labels,
        commit_request(None, "a.jpg"),
        metadata_with_key(key),
    )
    .await
    .expect_err("another dataset's snapshot is not this one's");
    assert_eq!(err.error.code, StatusCode::BAD_REQUEST, "got: {err:?}");
    assert_eq!(err.error.r#type, "IdempotencyKeyReused");
}

#[sqlx::test]
async fn test_a_replayed_grant_request_answers_with_the_same_grant(pool: PgPool) {
    let ds = TestDataset::managed(pool.clone()).await;
    commit_on(
        &ds,
        commit_request(None, "a.jpg"),
        random_request_metadata(),
    )
    .await
    .unwrap();
    let key = new_key();
    let request = CreateDatasetAccessGrantRequest { content_type: None };

    let first = CatalogServer::create_dataset_access_grant(
        ds.ref_params("main"),
        request.clone(),
        ds.ctx.clone(),
        metadata_with_key_as(key, "oidc~alice"),
    )
    .await
    .expect("keyed grant");
    let replayed = CatalogServer::create_dataset_access_grant(
        ds.ref_params("main"),
        request,
        ds.ctx.clone(),
        metadata_with_key_as(key, "oidc~alice"),
    )
    .await
    .expect("the replay succeeds");

    assert_eq!(replayed.grant_id, first.grant_id);
    assert_eq!(replayed.expires_at, first.expires_at);
    assert_eq!(grant_count(&pool).await, 1);
}

#[sqlx::test]
async fn test_a_grant_key_replayed_by_another_caller_hands_over_nothing(pool: PgPool) {
    let ds = TestDataset::managed(pool).await;
    commit_on(
        &ds,
        commit_request(None, "a.jpg"),
        random_request_metadata(),
    )
    .await
    .unwrap();
    let key = new_key();
    let request = CreateDatasetAccessGrantRequest { content_type: None };
    CatalogServer::create_dataset_access_grant(
        ds.ref_params("main"),
        request.clone(),
        ds.ctx.clone(),
        metadata_with_key_as(key, "oidc~alice"),
    )
    .await
    .unwrap();

    let err = CatalogServer::create_dataset_access_grant(
        ds.ref_params("main"),
        request,
        ds.ctx.clone(),
        metadata_with_key_as(key, "oidc~bob"),
    )
    .await
    .expect_err("alice's grant is not bob's");
    assert_eq!(err.error.code, StatusCode::BAD_REQUEST, "got: {err:?}");
    assert_eq!(err.error.r#type, "IdempotencyKeyReused");
}

#[sqlx::test]
async fn test_replayed_ref_operations_do_not_run_again(pool: PgPool) {
    let ds = TestDataset::managed(pool).await;
    let first = commit_on(
        &ds,
        commit_request(None, "a.jpg"),
        random_request_metadata(),
    )
    .await
    .unwrap();
    let second = commit_on(
        &ds,
        commit_request(Some(first), "b.jpg"),
        random_request_metadata(),
    )
    .await
    .unwrap();

    // A second create would be a 409: the ref exists.
    let create_key = new_key();
    let create = || CreateDatasetRefRequest {
        name: "exp".to_string(),
        typ: DatasetRefType::Branch,
        source: DatasetRefSource::Ref {
            name: "main".to_string(),
        },
    };
    for _ in 0..2 {
        let created = CatalogServer::create_dataset_ref(
            ds.params(),
            create(),
            ds.ctx.clone(),
            metadata_with_key(create_key),
        )
        .await
        .expect("create and its replay");
        assert_eq!(created.snapshot_id, Some(second));
    }

    // A second reset would be a 409: the head is `first` by then.
    let reset_key = new_key();
    for _ in 0..2 {
        let moved = CatalogServer::move_dataset_ref(
            ds.ref_params("exp"),
            MoveDatasetRefRequest {
                snapshot_id: first,
                expected_snapshot_id: Some(second),
                fast_forward: false,
            },
            ds.ctx.clone(),
            metadata_with_key(reset_key),
        )
        .await
        .expect("reset and its replay");
        assert_eq!(moved.snapshot_id, Some(first));
    }

    // A second delete would be a 404: the ref is gone.
    let delete_key = new_key();
    for _ in 0..2 {
        CatalogServer::delete_dataset_ref(
            ds.ref_params("exp"),
            ds.ctx.clone(),
            metadata_with_key(delete_key),
        )
        .await
        .expect("delete and its replay");
    }
}

/// As if the key's record outlived its lifetime and was cleaned up.
async fn lapse(pool: &PgPool, key: IdempotencyKey) {
    sqlx::query("DELETE FROM idempotency_record WHERE idempotency_key = $1")
        .bind(key.as_uuid())
        .execute(pool)
        .await
        .unwrap();
}

/// The key's record lapses long before the snapshot that carries the key: a late
/// retry is answered from the snapshot, and commits nothing.
#[sqlx::test]
async fn test_a_commit_retried_after_its_record_lapsed_is_answered_from_the_snapshot(pool: PgPool) {
    let ds = TestDataset::managed(pool.clone()).await;
    let key = new_key();
    let first = commit_on(&ds, commit_request(None, "a.jpg"), metadata_with_key(key))
        .await
        .unwrap();
    lapse(&pool, key).await;

    let retried = commit_on(&ds, commit_request(None, "a.jpg"), metadata_with_key(key))
        .await
        .expect("a late retry");
    assert_eq!(retried, first);
    assert_eq!(snapshot_count(&pool).await, 1);
}

/// As for a commit: a late retry of a grant request is answered from the grant.
#[sqlx::test]
async fn test_a_grant_retried_after_its_record_lapsed_is_answered_from_the_grant(pool: PgPool) {
    let ds = TestDataset::managed(pool.clone()).await;
    commit_on(
        &ds,
        commit_request(None, "a.jpg"),
        random_request_metadata(),
    )
    .await
    .unwrap();
    let key = new_key();
    let request = CreateDatasetAccessGrantRequest { content_type: None };
    let first = CatalogServer::create_dataset_access_grant(
        ds.ref_params("main"),
        request.clone(),
        ds.ctx.clone(),
        metadata_with_key_as(key, "oidc~alice"),
    )
    .await
    .unwrap();
    lapse(&pool, key).await;

    let retried = CatalogServer::create_dataset_access_grant(
        ds.ref_params("main"),
        request,
        ds.ctx.clone(),
        metadata_with_key_as(key, "oidc~alice"),
    )
    .await
    .expect("a late retry");
    assert_eq!(retried.grant_id, first.grant_id);
    assert_eq!(grant_count(&pool).await, 1);
}

/// A retry is answered with what the commit published, though the snapshot has
/// expired since: the commit happened.
#[sqlx::test]
async fn test_a_commit_replay_answers_once_its_snapshot_expired(pool: PgPool) {
    let ds = TestDataset::managed(pool.clone()).await;
    let key = new_key();
    let first = commit_on(&ds, commit_request(None, "a.jpg"), metadata_with_key(key))
        .await
        .unwrap();
    commit_on(
        &ds,
        commit_request(Some(first), "b.jpg"),
        random_request_metadata(),
    )
    .await
    .unwrap();
    CatalogServer::expire_dataset_snapshot(
        ds.snapshot_params(first),
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    let replayed = commit_on(&ds, commit_request(None, "a.jpg"), metadata_with_key(key))
        .await
        .expect("the replay");
    assert_eq!(replayed, first);
}

/// A retry whose commit's snapshot has been purged finds nothing to answer with,
/// and says so: it does not commit again.
#[sqlx::test]
async fn test_a_commit_replay_after_its_snapshot_was_purged_is_not_found(pool: PgPool) {
    let ds = TestDataset::managed(pool.clone()).await;
    let key = new_key();
    let first = commit_on(&ds, commit_request(None, "a.jpg"), metadata_with_key(key))
        .await
        .unwrap();
    commit_on(
        &ds,
        commit_request(Some(first), "b.jpg"),
        random_request_metadata(),
    )
    .await
    .unwrap();
    CatalogServer::expire_dataset_snapshot(
        ds.snapshot_params(first),
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    sqlx::query(
        "UPDATE dataset_snapshot SET purge_after = now() - interval '1 second' WHERE snapshot_id = $1",
    )
    .bind(*first)
    .execute(&pool)
    .await
    .unwrap();
    let mut t = ds.begin_write().await;
    let purged =
        PostgresBackend::purge_expired_dataset_snapshots(ds.warehouse_id, ds.id, t.transaction())
            .await
            .unwrap();
    t.commit().await.unwrap();
    assert_eq!(purged.purged, 1);

    let err = commit_on(&ds, commit_request(None, "a.jpg"), metadata_with_key(key))
        .await
        .unwrap_err();
    assert_eq!(err.error.code, StatusCode::NOT_FOUND, "{err:?}");
    assert_eq!(err.error.r#type, "DatasetSnapshotNotFound");
    assert_eq!(snapshot_count(&pool).await, 1, "nothing committed again");
}

/// A retry whose grant has expired and been swept finds nothing to answer with,
/// and says so: it does not grant again.
#[sqlx::test]
async fn test_a_grant_replay_after_its_grant_was_swept_is_not_found(pool: PgPool) {
    let ds = TestDataset::managed(pool.clone()).await;
    commit_on(
        &ds,
        commit_request(None, "a.jpg"),
        random_request_metadata(),
    )
    .await
    .unwrap();
    let key = new_key();
    let request = CreateDatasetAccessGrantRequest { content_type: None };
    CatalogServer::create_dataset_access_grant(
        ds.ref_params("main"),
        request.clone(),
        ds.ctx.clone(),
        metadata_with_key_as(key, "oidc~alice"),
    )
    .await
    .unwrap();
    sqlx::query("UPDATE dataset_access_grant SET expires_at = now() - interval '1 second'")
        .execute(&pool)
        .await
        .unwrap();
    // Making another grant sweeps the expired one.
    CatalogServer::create_dataset_access_grant(
        ds.ref_params("main"),
        request.clone(),
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    assert_eq!(grant_count(&pool).await, 1);

    let err = CatalogServer::create_dataset_access_grant(
        ds.ref_params("main"),
        request,
        ds.ctx.clone(),
        metadata_with_key_as(key, "oidc~alice"),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.code, StatusCode::NOT_FOUND, "{err:?}");
    assert_eq!(err.error.r#type, "DatasetAccessGrantNotFound");
    assert_eq!(grant_count(&pool).await, 1, "nothing granted again");
}

/// A replay reports what the commit's constraints left out, as the lost response
/// did: the caller learns which files did not land.
#[sqlx::test]
async fn test_a_commit_replay_reports_what_it_skipped(pool: PgPool) {
    let ds = TestDataset::managed(pool).await;
    CatalogServer::update_dataset_settings(
        ds.params(),
        UpdateDatasetSettingsRequest {
            constraints: Some(DatasetConstraints {
                allowed_content_types: Some(vec!["image/jpeg".to_string()]),
                max_file_size: None,
            }),
            retention: None,
        },
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    let key = new_key();
    let mut request = commit_request(None, "a.jpg");
    request.added.push(CommitFile {
        logical_key: "notes.txt".to_string(),
        content_type: Some("text/plain".to_string()),
        ..request.added[0].clone()
    });
    request.on_constraint_violation = Some(ConstraintViolationPolicy::Skip);
    let commit = |request: CommitDatasetRequest| {
        CatalogServer::commit_dataset(
            ds.ref_params("main"),
            request,
            ds.ctx.clone(),
            metadata_with_key(key),
        )
    };

    let first = commit(request.clone()).await.unwrap();
    assert_eq!(first.skipped.len(), 1);
    let replayed = commit(request).await.unwrap();
    assert_eq!(replayed.skipped, first.skipped);
}

/// A commit's replay answers with what it published, which no read of the dataset
/// shows, so it takes the commit's own authority.
#[sqlx::test]
async fn test_a_commit_replay_takes_the_commit_action(pool: PgPool) {
    let authorizer = HidingAuthorizer::new();
    let ds = TestNamespace::with_authorizer(pool, authorizer.clone())
        .await
        .create_dataset(DATASET, DatasetOwnership::Managed, None)
        .await;
    let key = new_key();
    let commit = || {
        CatalogServer::commit_dataset(
            ds.ref_params("main"),
            commit_request(None, "a.jpg"),
            ds.ctx.clone(),
            metadata_with_key(key),
        )
    };
    commit().await.unwrap();

    authorizer.block_action(r#"dataset:Commit { target_refs: {"main"} }"#);
    let err = commit().await.unwrap_err();
    assert_eq!(err.error.code, StatusCode::FORBIDDEN, "{err:?}");
}

/// A late retry is checked as a replay is: another caller's grant, or another
/// dataset's snapshot, is not answered with.
#[sqlx::test]
async fn test_a_late_retry_hands_over_nothing_that_is_not_its_own(pool: PgPool) {
    let ns = TestNamespace::new(pool.clone()).await;
    let ds = ns
        .create_dataset(DATASET, DatasetOwnership::Managed, None)
        .await;
    let other = ns
        .create_dataset("other", DatasetOwnership::Managed, None)
        .await;
    commit_on(
        &ds,
        commit_request(None, "a.jpg"),
        random_request_metadata(),
    )
    .await
    .unwrap();
    let request = CreateDatasetAccessGrantRequest { content_type: None };

    let grant_key = new_key();
    CatalogServer::create_dataset_access_grant(
        ds.ref_params("main"),
        request.clone(),
        ds.ctx.clone(),
        metadata_with_key_as(grant_key, "oidc~alice"),
    )
    .await
    .unwrap();
    lapse(&pool, grant_key).await;
    let err = CatalogServer::create_dataset_access_grant(
        ds.ref_params("main"),
        request,
        ds.ctx.clone(),
        metadata_with_key_as(grant_key, "oidc~bob"),
    )
    .await
    .expect_err("alice's grant is not bob's");
    assert_eq!(err.error.r#type, "IdempotencyKeyReused", "{err:?}");

    let commit_key = new_key();
    commit_on(
        &other,
        commit_request(None, "b.jpg"),
        metadata_with_key(commit_key),
    )
    .await
    .unwrap();
    lapse(&pool, commit_key).await;
    let err = commit_on(
        &ds,
        commit_request(None, "b.jpg"),
        metadata_with_key(commit_key),
    )
    .await
    .expect_err("the other dataset's snapshot is not this one's");
    assert_eq!(err.error.r#type, "IdempotencyKeyReused", "{err:?}");
}
