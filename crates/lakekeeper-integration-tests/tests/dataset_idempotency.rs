//! Retrying a dataset mutation with its `Idempotency-Key`.
//!
//! A lost response is the case the key exists for: the mutation happened, the
//! caller does not know it, and the retry must neither repeat it nor fail. Each
//! test here performs an operation, changes the state underneath it where that
//! matters, and replays it.
use http::StatusCode;
use iceberg::NamespaceIdent;
use lakekeeper::{
    api::{
        ApiContext, RequestMetadata, RequestMetadataTestBuilder,
        data::v1::datasets::{
            CommitDatasetRequest, CommitFile, CreateDatasetAccessGrantRequest,
            CreateDatasetRefRequest, DatasetParameters, DatasetRefParameters, DatasetRefSource,
            DatasetService as _, DatasetSnapshotParameters, MoveDatasetRefRequest,
            UpdateDatasetSettingsRequest,
        },
        iceberg::types::Prefix,
        management::v1::warehouse::TabularDeleteProfile,
    },
    server::CatalogServer,
    service::{
        CatalogDatasetOps, CatalogStore, ConstraintViolationPolicy, DatasetConstraints, DatasetId,
        DatasetRefType, DatasetSnapshotId, State, Transaction, UserId,
        authn::Actor,
        authz::{AllowAllAuthorizer, tests::HidingAuthorizer},
        idempotency::IdempotencyKey,
    },
};
use lakekeeper_integration_tests::{
    create_dataset, create_ns, memory_io_profile, random_request_metadata, setup,
};
use lakekeeper_storage_postgres::{PostgresBackend, SecretsState};
use sqlx::PgPool;
use uuid::Uuid;

type TestApiContext = ApiContext<State<AllowAllAuthorizer, PostgresBackend, SecretsState>>;

const DS: &str = "images";

async fn make_dataset(pool: PgPool) -> (TestApiContext, String, String) {
    let (ctx, warehouse) = setup(
        pool,
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
    create_dataset(ctx.clone(), prefix.clone(), ns.clone(), DS)
        .await
        .unwrap();
    (ctx, prefix, ns)
}

fn ds_params(prefix: &str, ns: &str) -> DatasetParameters {
    DatasetParameters {
        prefix: Some(Prefix(prefix.to_string())),
        namespace: NamespaceIdent::new(ns.to_string()),
        dataset_name: DS.to_string(),
    }
}

fn ref_params(prefix: &str, ns: &str, dataset: &str, r: &str) -> DatasetRefParameters {
    DatasetRefParameters {
        prefix: Some(Prefix(prefix.to_string())),
        namespace: NamespaceIdent::new(ns.to_string()),
        dataset_name: dataset.to_string(),
        ref_name: r.to_string(),
    }
}

fn new_key() -> IdempotencyKey {
    IdempotencyKey::parse(&Uuid::now_v7().to_string()).unwrap()
}

fn keyed(key: IdempotencyKey) -> RequestMetadata {
    let mut metadata = random_request_metadata();
    metadata.with_idempotency_key(key);
    metadata
}

fn keyed_as(user: &str, key: IdempotencyKey) -> RequestMetadata {
    let mut metadata = RequestMetadataTestBuilder::builder()
        .actor(Actor::Principal(UserId::new_unchecked("oidc", user)))
        .build();
    metadata.with_idempotency_key(key);
    metadata
}

fn commit_request(parent: Option<DatasetSnapshotId>, key: &str) -> CommitDatasetRequest {
    CommitDatasetRequest {
        parent_snapshot_id: parent,
        added: vec![CommitFile {
            logical_key: key.to_string(),
            physical_path: None,
            etag: None,
            size: Some(1),
            content_type: Some("image/jpeg".to_string()),
            checksum: None,
            version_id: None,
            last_modified: None,
        }],
        removed: vec![],
        summary: None,
        on_constraint_violation: None,
    }
}

async fn commit_on(
    ctx: &TestApiContext,
    prefix: &str,
    ns: &str,
    dataset: &str,
    request: CommitDatasetRequest,
    metadata: RequestMetadata,
) -> lakekeeper::api::Result<DatasetSnapshotId> {
    CatalogServer::commit_dataset(
        ref_params(prefix, ns, dataset, "main"),
        request,
        ctx.clone(),
        metadata,
    )
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
    let (ctx, prefix, ns) = make_dataset(pool.clone()).await;
    let key = new_key();

    let first = commit_on(
        &ctx,
        &prefix,
        &ns,
        DS,
        commit_request(None, "a.jpg"),
        keyed(key),
    )
    .await
    .expect("keyed commit");
    // Another writer lands on top before the retry arrives.
    let second = commit_on(
        &ctx,
        &prefix,
        &ns,
        DS,
        commit_request(Some(first), "b.jpg"),
        random_request_metadata(),
    )
    .await
    .expect("second commit");
    let snapshots = snapshot_count(&pool).await;

    // Its parent is stale now, so executing it again would be a 409.
    let replayed = commit_on(
        &ctx,
        &prefix,
        &ns,
        DS,
        commit_request(None, "a.jpg"),
        keyed(key),
    )
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
    let (ctx, prefix, ns) = make_dataset(pool).await;
    let head = commit_on(
        &ctx,
        &prefix,
        &ns,
        DS,
        commit_request(None, "a.jpg"),
        random_request_metadata(),
    )
    .await
    .unwrap();
    let key = new_key();

    let err = commit_on(
        &ctx,
        &prefix,
        &ns,
        DS,
        commit_request(None, "b.jpg"),
        keyed(key),
    )
    .await
    .expect_err("a stale parent conflicts");
    assert_eq!(err.error.code, StatusCode::CONFLICT, "got: {err:?}");

    // The rebased retry is a new attempt, and it runs.
    let rebased = commit_on(
        &ctx,
        &prefix,
        &ns,
        DS,
        commit_request(Some(head), "b.jpg"),
        keyed(key),
    )
    .await
    .expect("the rebased retry commits");
    assert_ne!(rebased, head);
}

#[sqlx::test]
async fn test_a_commit_key_spent_on_another_dataset_is_refused(pool: PgPool) {
    let (ctx, prefix, ns) = make_dataset(pool).await;
    create_dataset(ctx.clone(), prefix.clone(), ns.clone(), "labels")
        .await
        .unwrap();
    let key = new_key();
    commit_on(
        &ctx,
        &prefix,
        &ns,
        DS,
        commit_request(None, "a.jpg"),
        keyed(key),
    )
    .await
    .unwrap();

    let err = commit_on(
        &ctx,
        &prefix,
        &ns,
        "labels",
        commit_request(None, "a.jpg"),
        keyed(key),
    )
    .await
    .expect_err("another dataset's snapshot is not this one's");
    assert_eq!(err.error.code, StatusCode::BAD_REQUEST, "got: {err:?}");
    assert_eq!(err.error.r#type, "IdempotencyKeyReused");
}

#[sqlx::test]
async fn test_a_replayed_grant_request_answers_with_the_same_grant(pool: PgPool) {
    let (ctx, prefix, ns) = make_dataset(pool.clone()).await;
    commit_on(
        &ctx,
        &prefix,
        &ns,
        DS,
        commit_request(None, "a.jpg"),
        random_request_metadata(),
    )
    .await
    .unwrap();
    let key = new_key();
    let request = CreateDatasetAccessGrantRequest { content_type: None };

    let first = CatalogServer::create_dataset_access_grant(
        ref_params(&prefix, &ns, DS, "main"),
        request.clone(),
        ctx.clone(),
        keyed_as("alice", key),
    )
    .await
    .expect("keyed grant");
    let replayed = CatalogServer::create_dataset_access_grant(
        ref_params(&prefix, &ns, DS, "main"),
        request,
        ctx.clone(),
        keyed_as("alice", key),
    )
    .await
    .expect("the replay succeeds");

    assert_eq!(replayed.grant_id, first.grant_id);
    assert_eq!(replayed.expires_at, first.expires_at);
    assert_eq!(grant_count(&pool).await, 1);
}

#[sqlx::test]
async fn test_a_grant_key_replayed_by_another_caller_hands_over_nothing(pool: PgPool) {
    let (ctx, prefix, ns) = make_dataset(pool).await;
    commit_on(
        &ctx,
        &prefix,
        &ns,
        DS,
        commit_request(None, "a.jpg"),
        random_request_metadata(),
    )
    .await
    .unwrap();
    let key = new_key();
    let request = CreateDatasetAccessGrantRequest { content_type: None };
    CatalogServer::create_dataset_access_grant(
        ref_params(&prefix, &ns, DS, "main"),
        request.clone(),
        ctx.clone(),
        keyed_as("alice", key),
    )
    .await
    .unwrap();

    let err = CatalogServer::create_dataset_access_grant(
        ref_params(&prefix, &ns, DS, "main"),
        request,
        ctx,
        keyed_as("bob", key),
    )
    .await
    .expect_err("alice's grant is not bob's");
    assert_eq!(err.error.code, StatusCode::BAD_REQUEST, "got: {err:?}");
    assert_eq!(err.error.r#type, "IdempotencyKeyReused");
}

#[sqlx::test]
async fn test_replayed_ref_operations_do_not_run_again(pool: PgPool) {
    let (ctx, prefix, ns) = make_dataset(pool).await;
    let first = commit_on(
        &ctx,
        &prefix,
        &ns,
        DS,
        commit_request(None, "a.jpg"),
        random_request_metadata(),
    )
    .await
    .unwrap();
    let second = commit_on(
        &ctx,
        &prefix,
        &ns,
        DS,
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
            ds_params(&prefix, &ns),
            create(),
            ctx.clone(),
            keyed(create_key),
        )
        .await
        .expect("create and its replay");
        assert_eq!(created.snapshot_id, Some(second));
    }

    // A second reset would be a 409: the head is `first` by then.
    let reset_key = new_key();
    for _ in 0..2 {
        let moved = CatalogServer::move_dataset_ref(
            ref_params(&prefix, &ns, DS, "exp"),
            MoveDatasetRefRequest {
                snapshot_id: first,
                expected_snapshot_id: Some(second),
                fast_forward: false,
            },
            ctx.clone(),
            keyed(reset_key),
        )
        .await
        .expect("reset and its replay");
        assert_eq!(moved.snapshot_id, Some(first));
    }

    // A second delete would be a 404: the ref is gone.
    let delete_key = new_key();
    for _ in 0..2 {
        CatalogServer::delete_dataset_ref(
            ref_params(&prefix, &ns, DS, "exp"),
            ctx.clone(),
            keyed(delete_key),
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
    let (ctx, prefix, ns) = make_dataset(pool.clone()).await;
    let key = new_key();
    let first = commit_on(
        &ctx,
        &prefix,
        &ns,
        DS,
        commit_request(None, "a.jpg"),
        keyed(key),
    )
    .await
    .unwrap();
    lapse(&pool, key).await;

    let retried = commit_on(
        &ctx,
        &prefix,
        &ns,
        DS,
        commit_request(None, "a.jpg"),
        keyed(key),
    )
    .await
    .expect("a late retry");
    assert_eq!(retried, first);
    assert_eq!(snapshot_count(&pool).await, 1);
}

/// As for a commit: a late retry of a grant request is answered from the grant.
#[sqlx::test]
async fn test_a_grant_retried_after_its_record_lapsed_is_answered_from_the_grant(pool: PgPool) {
    let (ctx, prefix, ns) = make_dataset(pool.clone()).await;
    commit_on(
        &ctx,
        &prefix,
        &ns,
        DS,
        commit_request(None, "a.jpg"),
        random_request_metadata(),
    )
    .await
    .unwrap();
    let key = new_key();
    let request = CreateDatasetAccessGrantRequest { content_type: None };
    let first = CatalogServer::create_dataset_access_grant(
        ref_params(&prefix, &ns, DS, "main"),
        request.clone(),
        ctx.clone(),
        keyed_as("alice", key),
    )
    .await
    .unwrap();
    lapse(&pool, key).await;

    let retried = CatalogServer::create_dataset_access_grant(
        ref_params(&prefix, &ns, DS, "main"),
        request,
        ctx,
        keyed_as("alice", key),
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
    let (ctx, prefix, ns) = make_dataset(pool.clone()).await;
    let key = new_key();
    let first = commit_on(
        &ctx,
        &prefix,
        &ns,
        DS,
        commit_request(None, "a.jpg"),
        keyed(key),
    )
    .await
    .unwrap();
    commit_on(
        &ctx,
        &prefix,
        &ns,
        DS,
        commit_request(Some(first), "b.jpg"),
        random_request_metadata(),
    )
    .await
    .unwrap();
    CatalogServer::expire_dataset_snapshot(
        DatasetSnapshotParameters {
            prefix: Some(Prefix(prefix.clone())),
            namespace: NamespaceIdent::new(ns.clone()),
            dataset_name: DS.to_string(),
            snapshot_id: first,
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    let replayed = commit_on(
        &ctx,
        &prefix,
        &ns,
        DS,
        commit_request(None, "a.jpg"),
        keyed(key),
    )
    .await
    .expect("the replay");
    assert_eq!(replayed, first);
}

/// A retry whose commit's snapshot has been purged finds nothing to answer with,
/// and says so: it does not commit again.
#[sqlx::test]
async fn test_a_commit_replay_after_its_snapshot_was_purged_is_not_found(pool: PgPool) {
    let (ctx, prefix, ns) = make_dataset(pool.clone()).await;
    let key = new_key();
    let first = commit_on(
        &ctx,
        &prefix,
        &ns,
        DS,
        commit_request(None, "a.jpg"),
        keyed(key),
    )
    .await
    .unwrap();
    commit_on(
        &ctx,
        &prefix,
        &ns,
        DS,
        commit_request(Some(first), "b.jpg"),
        random_request_metadata(),
    )
    .await
    .unwrap();
    CatalogServer::expire_dataset_snapshot(
        DatasetSnapshotParameters {
            prefix: Some(Prefix(prefix.clone())),
            namespace: NamespaceIdent::new(ns.clone()),
            dataset_name: DS.to_string(),
            snapshot_id: first,
        },
        ctx.clone(),
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
    let dataset_id: Uuid =
        sqlx::query_scalar("SELECT dataset_id FROM dataset_snapshot WHERE snapshot_id = $1")
            .bind(*first)
            .fetch_one(&pool)
            .await
            .unwrap();
    let mut t =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();
    let purged = PostgresBackend::purge_expired_dataset_snapshots(
        prefix.parse::<Uuid>().unwrap().into(),
        DatasetId::from(dataset_id),
        t.transaction(),
    )
    .await
    .unwrap();
    t.commit().await.unwrap();
    assert_eq!(purged.purged, 1);

    let err = commit_on(
        &ctx,
        &prefix,
        &ns,
        DS,
        commit_request(None, "a.jpg"),
        keyed(key),
    )
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
    let (ctx, prefix, ns) = make_dataset(pool.clone()).await;
    commit_on(
        &ctx,
        &prefix,
        &ns,
        DS,
        commit_request(None, "a.jpg"),
        random_request_metadata(),
    )
    .await
    .unwrap();
    let key = new_key();
    let request = CreateDatasetAccessGrantRequest { content_type: None };
    CatalogServer::create_dataset_access_grant(
        ref_params(&prefix, &ns, DS, "main"),
        request.clone(),
        ctx.clone(),
        keyed_as("alice", key),
    )
    .await
    .unwrap();
    sqlx::query("UPDATE dataset_access_grant SET expires_at = now() - interval '1 second'")
        .execute(&pool)
        .await
        .unwrap();
    // Making another grant sweeps the expired one.
    CatalogServer::create_dataset_access_grant(
        ref_params(&prefix, &ns, DS, "main"),
        request.clone(),
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    assert_eq!(grant_count(&pool).await, 1);

    let err = CatalogServer::create_dataset_access_grant(
        ref_params(&prefix, &ns, DS, "main"),
        request,
        ctx.clone(),
        keyed_as("alice", key),
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
    let (ctx, prefix, ns) = make_dataset(pool).await;
    CatalogServer::update_dataset_settings(
        ds_params(&prefix, &ns),
        UpdateDatasetSettingsRequest {
            constraints: Some(DatasetConstraints {
                allowed_content_types: Some(vec!["image/jpeg".to_string()]),
                max_file_size: None,
            }),
            retention: None,
        },
        ctx.clone(),
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
            ref_params(&prefix, &ns, DS, "main"),
            request,
            ctx.clone(),
            keyed(key),
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
    let key = new_key();
    let commit = || {
        CatalogServer::commit_dataset(
            ref_params(&prefix, &ns, DS, "main"),
            commit_request(None, "a.jpg"),
            ctx.clone(),
            keyed(key),
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
    let (ctx, prefix, ns) = make_dataset(pool.clone()).await;
    create_dataset(ctx.clone(), prefix.clone(), ns.clone(), "other")
        .await
        .unwrap();
    commit_on(
        &ctx,
        &prefix,
        &ns,
        DS,
        commit_request(None, "a.jpg"),
        random_request_metadata(),
    )
    .await
    .unwrap();
    let request = CreateDatasetAccessGrantRequest { content_type: None };

    let grant_key = new_key();
    CatalogServer::create_dataset_access_grant(
        ref_params(&prefix, &ns, DS, "main"),
        request.clone(),
        ctx.clone(),
        keyed_as("alice", grant_key),
    )
    .await
    .unwrap();
    lapse(&pool, grant_key).await;
    let err = CatalogServer::create_dataset_access_grant(
        ref_params(&prefix, &ns, DS, "main"),
        request,
        ctx.clone(),
        keyed_as("bob", grant_key),
    )
    .await
    .expect_err("alice's grant is not bob's");
    assert_eq!(err.error.r#type, "IdempotencyKeyReused", "{err:?}");

    let commit_key = new_key();
    commit_on(
        &ctx,
        &prefix,
        &ns,
        "other",
        commit_request(None, "b.jpg"),
        keyed(commit_key),
    )
    .await
    .unwrap();
    lapse(&pool, commit_key).await;
    let err = commit_on(
        &ctx,
        &prefix,
        &ns,
        DS,
        commit_request(None, "b.jpg"),
        keyed(commit_key),
    )
    .await
    .expect_err("the other dataset's snapshot is not this one's");
    assert_eq!(err.error.r#type, "IdempotencyKeyReused", "{err:?}");
}
