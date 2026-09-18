//! Versioning tests: commits, refs, and the concurrency semantics.
//!
//! The interesting assertions here are the negative ones. A tag that keeps
//! pointing at its original snapshot after the branch moves on, a stale writer
//! that loses rather than overwrites, and a fast-forward that refuses to abandon
//! commits are the properties the whole design rests on.
use http::StatusCode;
use iceberg::NamespaceIdent;
use lakekeeper::{
    api::{
        ApiContext,
        data::v1::datasets::{
            CommitDatasetRequest, CommitFile, CreateDatasetRefRequest, CreateDatasetRequest,
            DatasetParameters, DatasetRefParameters, DatasetRefSource, DatasetService as _,
            ListDatasetFilesQuery, MoveDatasetRefRequest, SetDatasetRefProtectionRequest,
        },
        iceberg::{types::Prefix, v1::namespace::NamespaceParameters},
        management::v1::warehouse::TabularDeleteProfile,
    },
    server::CatalogServer,
    service::{
        CatalogDatasetOps, CatalogStore, DatasetConstraints, DatasetRefType, DatasetSnapshotId,
        ManifestEntry, State, Transaction, authz::AllowAllAuthorizer,
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
    let ns_name = format!("ns_{}", Uuid::now_v7());
    create_ns(ctx.clone(), prefix.clone(), ns_name.clone()).await;
    create_dataset(ctx.clone(), prefix.clone(), ns_name.clone(), DS)
        .await
        .unwrap();
    (ctx, prefix, ns_name)
}

fn ds_params(prefix: &str, ns: &str) -> DatasetParameters {
    DatasetParameters {
        prefix: Some(Prefix(prefix.to_string())),
        namespace: NamespaceIdent::new(ns.to_string()),
        dataset_name: DS.to_string(),
    }
}

fn ref_params(prefix: &str, ns: &str, r: &str) -> DatasetRefParameters {
    DatasetRefParameters {
        prefix: Some(Prefix(prefix.to_string())),
        namespace: NamespaceIdent::new(ns.to_string()),
        dataset_name: DS.to_string(),
        ref_name: r.to_string(),
    }
}

fn file(key: &str) -> CommitFile {
    CommitFile {
        logical_key: key.to_string(),
        physical_path: None,
        etag: Some("etag".to_string()),
        size: Some(1024),
        content_type: Some("image/jpeg".to_string()),
        checksum: None,
        version_id: None,
        last_modified: None,
    }
}

async fn commit(
    ctx: &TestApiContext,
    prefix: &str,
    ns: &str,
    branch: &str,
    parent: Option<DatasetSnapshotId>,
    added: Vec<CommitFile>,
    removed: Vec<String>,
) -> lakekeeper::api::Result<DatasetSnapshotId> {
    CatalogServer::commit_dataset(
        ref_params(prefix, ns, branch),
        CommitDatasetRequest {
            parent_snapshot_id: parent,
            added,
            removed,
            summary: None,
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .map(|s| s.snapshot_id)
}

async fn file_keys(ctx: &TestApiContext, prefix: &str, ns: &str, r: &str) -> Vec<String> {
    let listed = CatalogServer::list_dataset_files(
        ref_params(prefix, ns, r),
        ListDatasetFilesQuery::default(),
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .expect("files list");
    let mut keys: Vec<String> = listed.files.into_iter().map(|f| f.logical_key).collect();
    keys.sort();
    keys
}

#[sqlx::test]
async fn test_new_dataset_has_an_empty_main_branch(pool: PgPool) {
    let (ctx, prefix, ns) = make_dataset(pool).await;

    let refs =
        CatalogServer::list_dataset_refs(ds_params(&prefix, &ns), ctx, random_request_metadata())
            .await
            .unwrap();

    assert_eq!(refs.refs.len(), 1);
    assert_eq!(refs.refs[0].name, "main");
    assert_eq!(refs.refs[0].typ, DatasetRefType::Branch);
    // `main` exists from creation but points at nothing: the first commit's
    // compare-and-swap is against NULL.
    assert!(refs.refs[0].snapshot_id.is_none());
}

#[sqlx::test]
async fn test_commit_then_list_files(pool: PgPool) {
    let (ctx, prefix, ns) = make_dataset(pool).await;

    let snap = commit(
        &ctx,
        &prefix,
        &ns,
        "main",
        None,
        vec![file("train/a.jpg"), file("train/b.jpg")],
        vec![],
    )
    .await
    .expect("first commit");

    assert_eq!(
        file_keys(&ctx, &prefix, &ns, "main").await,
        vec!["train/a.jpg", "train/b.jpg"]
    );

    // The second commit is a delta against the first, not a rewrite.
    commit(
        &ctx,
        &prefix,
        &ns,
        "main",
        Some(snap),
        vec![file("train/c.jpg")],
        vec!["train/a.jpg".to_string()],
    )
    .await
    .expect("second commit");

    assert_eq!(
        file_keys(&ctx, &prefix, &ns, "main").await,
        vec!["train/b.jpg", "train/c.jpg"]
    );
}

#[sqlx::test]
async fn test_stale_commit_conflicts_and_reports_the_head(pool: PgPool) {
    let (ctx, prefix, ns) = make_dataset(pool).await;

    let first = commit(&ctx, &prefix, &ns, "main", None, vec![file("a")], vec![])
        .await
        .unwrap();
    commit(
        &ctx,
        &prefix,
        &ns,
        "main",
        Some(first),
        vec![file("b")],
        vec![],
    )
    .await
    .unwrap();

    // A writer still holding `first` as its parent has been overtaken. It must
    // lose rather than clobber the commit that landed in between.
    let err = commit(
        &ctx,
        &prefix,
        &ns,
        "main",
        Some(first),
        vec![file("c")],
        vec![],
    )
    .await
    .expect_err("a stale parent must conflict");
    assert_eq!(err.error.code, StatusCode::CONFLICT, "got: {err:?}");

    // And the losing commit left nothing behind.
    assert_eq!(file_keys(&ctx, &prefix, &ns, "main").await, vec!["a", "b"]);
}

#[sqlx::test]
async fn test_tag_pins_its_snapshot_while_the_branch_moves_on(pool: PgPool) {
    let (ctx, prefix, ns) = make_dataset(pool).await;

    let first = commit(
        &ctx,
        &prefix,
        &ns,
        "main",
        None,
        vec![file("train/a.jpg")],
        vec![],
    )
    .await
    .unwrap();

    CatalogServer::create_dataset_ref(
        ds_params(&prefix, &ns),
        CreateDatasetRefRequest {
            name: "training-2026-07".to_string(),
            typ: DatasetRefType::Tag,
            source: DatasetRefSource::Ref {
                name: "main".to_string(),
            },
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .expect("tag creation");

    commit(
        &ctx,
        &prefix,
        &ns,
        "main",
        Some(first),
        vec![file("train/b.jpg")],
        vec![],
    )
    .await
    .unwrap();

    // This is the reproducibility promise: the tag resolves to the same files
    // it did when it was cut, however far the branch has moved.
    assert_eq!(
        file_keys(&ctx, &prefix, &ns, "training-2026-07").await,
        vec!["train/a.jpg"]
    );
    assert_eq!(
        file_keys(&ctx, &prefix, &ns, "main").await,
        vec!["train/a.jpg", "train/b.jpg"]
    );
}

#[sqlx::test]
async fn test_commit_to_a_tag_is_refused(pool: PgPool) {
    let (ctx, prefix, ns) = make_dataset(pool).await;

    let snap = commit(&ctx, &prefix, &ns, "main", None, vec![file("a")], vec![])
        .await
        .unwrap();
    CatalogServer::create_dataset_ref(
        ds_params(&prefix, &ns),
        CreateDatasetRefRequest {
            name: "v1".to_string(),
            typ: DatasetRefType::Tag,
            source: DatasetRefSource::Snapshot { snapshot_id: snap },
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    let err = commit(
        &ctx,
        &prefix,
        &ns,
        "v1",
        Some(snap),
        vec![file("b")],
        vec![],
    )
    .await
    .expect_err("a tag must not accept commits");
    assert_eq!(err.error.code, StatusCode::CONFLICT, "got: {err:?}");
}

#[sqlx::test]
async fn test_branch_is_zero_copy_and_diverges_independently(pool: PgPool) {
    let (ctx, prefix, ns) = make_dataset(pool).await;

    let base = commit(
        &ctx,
        &prefix,
        &ns,
        "main",
        None,
        vec![file("shared")],
        vec![],
    )
    .await
    .unwrap();

    CatalogServer::create_dataset_ref(
        ds_params(&prefix, &ns),
        CreateDatasetRefRequest {
            name: "experiment".to_string(),
            typ: DatasetRefType::Branch,
            source: DatasetRefSource::Snapshot { snapshot_id: base },
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    commit(
        &ctx,
        &prefix,
        &ns,
        "experiment",
        Some(base),
        vec![file("only-on-experiment")],
        vec![],
    )
    .await
    .unwrap();

    // Committing to a branch leaves every other ref untouched: visibility is
    // defined by manifests, not by what is in the bucket.
    assert_eq!(file_keys(&ctx, &prefix, &ns, "main").await, vec!["shared"]);
    assert_eq!(
        file_keys(&ctx, &prefix, &ns, "experiment").await,
        vec!["only-on-experiment", "shared"]
    );
}

#[sqlx::test]
async fn test_fast_forward_promotes_and_refuses_a_non_descendant(pool: PgPool) {
    let (ctx, prefix, ns) = make_dataset(pool).await;

    let base = commit(&ctx, &prefix, &ns, "main", None, vec![file("base")], vec![])
        .await
        .unwrap();

    CatalogServer::create_dataset_ref(
        ds_params(&prefix, &ns),
        CreateDatasetRefRequest {
            name: "incoming".to_string(),
            typ: DatasetRefType::Branch,
            source: DatasetRefSource::Snapshot { snapshot_id: base },
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    let ahead = commit(
        &ctx,
        &prefix,
        &ns,
        "incoming",
        Some(base),
        vec![file("new")],
        vec![],
    )
    .await
    .unwrap();

    // Promote: main is behind, and `ahead` descends from it.
    CatalogServer::move_dataset_ref(
        ref_params(&prefix, &ns, "main"),
        MoveDatasetRefRequest {
            snapshot_id: ahead,
            expected_snapshot_id: Some(base),
            fast_forward: true,
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .expect("fast-forward to a descendant");

    assert_eq!(
        file_keys(&ctx, &prefix, &ns, "main").await,
        vec!["base", "new"]
    );

    // Going back to `base` would abandon `ahead`. Fast-forward must refuse;
    // that is what reset is for, and reset needs its own permission.
    let err = CatalogServer::move_dataset_ref(
        ref_params(&prefix, &ns, "main"),
        MoveDatasetRefRequest {
            snapshot_id: base,
            expected_snapshot_id: Some(ahead),
            fast_forward: true,
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .expect_err("fast-forward must refuse a non-descendant");
    assert_eq!(err.error.code, StatusCode::CONFLICT, "got: {err:?}");

    // The same move as a reset is allowed.
    CatalogServer::move_dataset_ref(
        ref_params(&prefix, &ns, "main"),
        MoveDatasetRefRequest {
            snapshot_id: base,
            expected_snapshot_id: Some(ahead),
            fast_forward: false,
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .expect("reset may move a branch backwards");

    assert_eq!(file_keys(&ctx, &prefix, &ns, "main").await, vec!["base"]);
}

#[sqlx::test]
async fn test_main_cannot_be_deleted_but_other_refs_can(pool: PgPool) {
    let (ctx, prefix, ns) = make_dataset(pool).await;

    let snap = commit(&ctx, &prefix, &ns, "main", None, vec![file("a")], vec![])
        .await
        .unwrap();
    CatalogServer::create_dataset_ref(
        ds_params(&prefix, &ns),
        CreateDatasetRefRequest {
            name: "scratch".to_string(),
            typ: DatasetRefType::Branch,
            source: DatasetRefSource::Snapshot { snapshot_id: snap },
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    let err = CatalogServer::delete_dataset_ref(
        ref_params(&prefix, &ns, "main"),
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .expect_err("main must not be deletable");
    assert_eq!(err.error.code, StatusCode::BAD_REQUEST, "got: {err:?}");

    CatalogServer::delete_dataset_ref(
        ref_params(&prefix, &ns, "scratch"),
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .expect("a non-main ref deletes");

    let refs =
        CatalogServer::list_dataset_refs(ds_params(&prefix, &ns), ctx, random_request_metadata())
            .await
            .unwrap();
    assert_eq!(refs.refs.len(), 1);
    assert_eq!(refs.refs[0].name, "main");
}

#[sqlx::test]
async fn test_content_type_filter(pool: PgPool) {
    let (ctx, prefix, ns) = make_dataset(pool).await;

    let mut pdf = file("docs/a.pdf");
    pdf.content_type = Some("application/pdf".to_string());
    commit(
        &ctx,
        &prefix,
        &ns,
        "main",
        None,
        vec![file("train/a.jpg"), pdf],
        vec![],
    )
    .await
    .unwrap();

    let listed = CatalogServer::list_dataset_files(
        ref_params(&prefix, &ns, "main"),
        ListDatasetFilesQuery {
            content_type: Some("application/pdf".to_string()),
            ..Default::default()
        },
        ctx,
        random_request_metadata(),
    )
    .await
    .unwrap();

    let keys: Vec<String> = listed.files.into_iter().map(|f| f.logical_key).collect();
    assert_eq!(keys, vec!["docs/a.pdf"]);
}

#[sqlx::test]
async fn test_constraints_are_enforced_at_commit(pool: PgPool) {
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

    CatalogServer::create_dataset(
        NamespaceParameters {
            prefix: Some(Prefix(prefix.clone())),
            namespace: NamespaceIdent::new(ns.clone()),
        },
        CreateDatasetRequest {
            name: DS.to_string(),
            location: None,
            constraints: Some(DatasetConstraints {
                allowed_content_types: Some(vec!["image/jpeg".to_string()]),
                max_file_size: Some(2048),
            }),
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    let mut wrong_type = file("docs/a.pdf");
    wrong_type.content_type = Some("application/pdf".to_string());
    let err = commit(&ctx, &prefix, &ns, "main", None, vec![wrong_type], vec![])
        .await
        .expect_err("a disallowed content type must fail the commit");
    assert_eq!(err.error.code, StatusCode::BAD_REQUEST, "got: {err:?}");

    let mut too_big = file("train/big.jpg");
    too_big.size = Some(9999);
    let err = commit(&ctx, &prefix, &ns, "main", None, vec![too_big], vec![])
        .await
        .expect_err("an oversized file must fail the commit");
    assert_eq!(err.error.code, StatusCode::BAD_REQUEST, "got: {err:?}");

    // A rejected commit is not a partial commit: nothing landed.
    let refs = CatalogServer::list_dataset_refs(
        ds_params(&prefix, &ns),
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    assert!(refs.refs[0].snapshot_id.is_none());

    commit(
        &ctx,
        &prefix,
        &ns,
        "main",
        None,
        vec![file("train/ok.jpg")],
        vec![],
    )
    .await
    .expect("a conforming file commits");
}

/// Pagination must stay on the snapshot the first page resolved, or a commit
/// landing between pages would splice two different file lists together.
#[sqlx::test]
async fn test_pagination_is_pinned_to_the_first_pages_snapshot(pool: PgPool) {
    let (ctx, prefix, ns) = make_dataset(pool).await;

    let first = commit(
        &ctx,
        &prefix,
        &ns,
        "main",
        None,
        vec![file("a"), file("b"), file("c")],
        vec![],
    )
    .await
    .unwrap();

    let page1 = CatalogServer::list_dataset_files(
        ref_params(&prefix, &ns, "main"),
        ListDatasetFilesQuery {
            page_size: Some(2),
            ..Default::default()
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    assert_eq!(page1.files.len(), 2);
    assert_eq!(page1.snapshot_id, Some(first));
    let token = page1.next_page_token.clone().expect("a token");

    // The branch moves on: "d" is added and "c" -- which page 2 is about to
    // return -- is removed.
    commit(
        &ctx,
        &prefix,
        &ns,
        "main",
        Some(first),
        vec![file("d")],
        vec!["c".to_string()],
    )
    .await
    .unwrap();

    let page2 = CatalogServer::list_dataset_files(
        ref_params(&prefix, &ns, "main"),
        ListDatasetFilesQuery {
            page_token: Some(token),
            page_size: Some(2),
            ..Default::default()
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    // Still reading the snapshot page 1 resolved: "c" is present and "d" is not.
    assert_eq!(
        page2.snapshot_id,
        Some(first),
        "page 2 must stay on snapshot 1"
    );
    let keys: Vec<String> = page2.files.into_iter().map(|f| f.logical_key).collect();
    assert_eq!(keys, vec!["c"], "page 2 must reflect the pinned snapshot");

    // A fresh listing does see the new state.
    assert_eq!(
        file_keys(&ctx, &prefix, &ns, "main").await,
        vec!["a", "b", "d"]
    );
}

/// Genuinely concurrent committers, not a sequential stand-in.
///
/// All of them read the same parent and race to move the branch. The
/// compare-and-swap must admit exactly one: the losers have to fail rather than
/// interleave, and the winner's files must be the only ones present.
#[sqlx::test]
async fn test_concurrent_committers_admit_exactly_one_winner(pool: PgPool) {
    let (ctx, prefix, ns) = make_dataset(pool).await;

    let base = commit(&ctx, &prefix, &ns, "main", None, vec![file("base")], vec![])
        .await
        .unwrap();

    const WRITERS: usize = 8;
    let mut handles = Vec::with_capacity(WRITERS);
    for i in 0..WRITERS {
        let ctx = ctx.clone();
        let prefix = prefix.clone();
        let ns = ns.clone();
        handles.push(tokio::spawn(async move {
            // Every writer commits against the same parent it read.
            commit(
                &ctx,
                &prefix,
                &ns,
                "main",
                Some(base),
                vec![file(&format!("writer-{i}"))],
                vec![],
            )
            .await
        }));
    }

    let mut winners = Vec::new();
    let mut conflicts = 0;
    for h in handles {
        match h.await.expect("task did not panic") {
            Ok(snapshot) => winners.push(snapshot),
            Err(e) => {
                assert_eq!(
                    e.error.code,
                    StatusCode::CONFLICT,
                    "a loser must lose with CONFLICT, got: {e:?}"
                );
                conflicts += 1;
            }
        }
    }

    assert_eq!(winners.len(), 1, "exactly one committer may win a round");
    assert_eq!(
        conflicts,
        WRITERS - 1,
        "every other committer must conflict"
    );

    // The branch is on the winner, and only the winner's file landed: a losing
    // commit leaves no manifest rows behind.
    let keys = file_keys(&ctx, &prefix, &ns, "main").await;
    assert_eq!(keys.len(), 2, "base plus exactly one writer, got {keys:?}");
    assert!(keys.contains(&"base".to_string()));

    let refs = CatalogServer::list_dataset_refs(
        ds_params(&prefix, &ns),
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    let main = refs.refs.iter().find(|r| r.name == "main").unwrap();
    assert_eq!(main.snapshot_id, Some(winners[0]));
}

/// The losers of a race must be able to rebase and land without re-uploading:
/// a conflict costs nothing physical, so retrying on the new head is the whole
/// recovery path.
#[sqlx::test]
async fn test_a_conflicted_writer_succeeds_after_rebasing(pool: PgPool) {
    let (ctx, prefix, ns) = make_dataset(pool).await;

    let base = commit(&ctx, &prefix, &ns, "main", None, vec![file("base")], vec![])
        .await
        .unwrap();
    let winner = commit(
        &ctx,
        &prefix,
        &ns,
        "main",
        Some(base),
        vec![file("winner")],
        vec![],
    )
    .await
    .unwrap();

    let err = commit(
        &ctx,
        &prefix,
        &ns,
        "main",
        Some(base),
        vec![file("loser")],
        vec![],
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.code, StatusCode::CONFLICT);

    // Same payload, new parent.
    commit(
        &ctx,
        &prefix,
        &ns,
        "main",
        Some(winner),
        vec![file("loser")],
        vec![],
    )
    .await
    .expect("rebased commit lands");

    assert_eq!(
        file_keys(&ctx, &prefix, &ns, "main").await,
        vec!["base", "loser", "winner"]
    );
}

/// A long chain must reconstruct correctly across the checkpoint boundary.
///
/// Checkpoints exist to bound reconstruction, so the property that matters is
/// that they change *cost*, never the answer: adds, removals and re-adds made
/// before, at, and after a checkpoint must all still resolve the same way.
#[sqlx::test]
async fn test_reconstruction_is_correct_across_a_checkpoint(pool: PgPool) {
    let (ctx, prefix, ns) = make_dataset(pool).await;

    // Commit past the checkpoint interval (20) one file at a time.
    let mut parent = None;
    for i in 0..25 {
        parent = Some(
            commit(
                &ctx,
                &prefix,
                &ns,
                "main",
                parent,
                vec![file(&format!("f{i:02}"))],
                vec![],
            )
            .await
            .unwrap_or_else(|e| panic!("commit {i} failed: {e:?}")),
        );
    }

    // Remove a file added before the checkpoint, and re-add one removed earlier.
    parent = Some(
        commit(
            &ctx,
            &prefix,
            &ns,
            "main",
            parent,
            vec![],
            vec!["f03".to_string()],
        )
        .await
        .unwrap(),
    );
    commit(
        &ctx,
        &prefix,
        &ns,
        "main",
        parent,
        vec![file("f03")],
        vec!["f07".to_string()],
    )
    .await
    .unwrap();

    let keys = file_keys(&ctx, &prefix, &ns, "main").await;

    // 25 files, minus f07, and f03 re-added.
    let expected: Vec<String> = (0..25)
        .map(|i| format!("f{i:02}"))
        .filter(|k| k != "f07")
        .collect();
    assert_eq!(keys, expected, "reconstruction across a checkpoint");

    // A tag cut before all of this still resolves to its own snapshot's state,
    // proving checkpoints did not rewrite history.
    let refs = CatalogServer::list_dataset_refs(
        ds_params(&prefix, &ns),
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    assert_eq!(refs.refs.len(), 1);
}

/// At least one snapshot in a long chain must actually become a checkpoint,
/// otherwise the bound above is never exercised in practice.
#[sqlx::test]
async fn test_a_long_chain_produces_a_checkpoint(pool: PgPool) {
    let (ctx, prefix, ns) = make_dataset(pool.clone()).await;

    let mut parent = None;
    for i in 0..25 {
        let snapshot = commit(
            &ctx,
            &prefix,
            &ns,
            "main",
            parent,
            vec![file(&format!("f{i:02}"))],
            vec![],
        )
        .await
        .unwrap();
        parent = Some(snapshot);
    }

    // Crossing the interval enqueues the fold rather than performing it: the
    // commit that trips the threshold must not pay for restating the whole file
    // set. Twenty-five commits past the threshold still produce exactly one task,
    // because the task table admits one active task per (entity, queue).
    let queued: i64 =
        sqlx::query_scalar("SELECT count(*) FROM task WHERE queue_name = 'dataset_checkpoint'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(queued, 1, "one fold enqueued, not one per commit");

    // No commit wrote a checkpoint itself.
    let folded: i64 =
        sqlx::query_scalar("SELECT count(*) FROM dataset_snapshot WHERE is_checkpoint")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(folded, 0, "the commit path must not fold");

    // Now do what the worker does.
    let dataset_id: Uuid = sqlx::query_scalar("SELECT dataset_id FROM dataset")
        .fetch_one(&pool)
        .await
        .unwrap();
    let warehouse_id = prefix.parse::<Uuid>().unwrap();

    let mut t =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();
    let checkpointed = PostgresBackend::checkpoint_dataset_branch(
        warehouse_id.into(),
        dataset_id.into(),
        "main",
        t.transaction(),
    )
    .await
    .unwrap();
    t.commit().await.unwrap();

    assert_eq!(
        checkpointed,
        Some(parent.unwrap()),
        "the fold targets the branch head at run time, not the snapshot that \
         tripped the threshold"
    );

    // Folding changes how far back a read walks, never what it returns.
    assert_eq!(file_keys(&ctx, &prefix, &ns, "main").await.len(), 25);

    // A second run finds nothing due and is a cheap no-op, which is what makes
    // retries and duplicate tasks harmless.
    let mut t =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();
    let again = PostgresBackend::checkpoint_dataset_branch(
        warehouse_id.into(),
        dataset_id.into(),
        "main",
        t.transaction(),
    )
    .await
    .unwrap();
    t.commit().await.unwrap();
    assert_eq!(again, None, "an already-folded branch has nothing due");
}

/// A protected branch refuses direct commits and deletion for everyone, and takes
/// changes only by fast-forward. Structural, so it holds regardless of permission.
#[sqlx::test]
async fn test_a_protected_branch_takes_changes_only_by_fast_forward(pool: PgPool) {
    let (ctx, prefix, ns) = make_dataset(pool).await;

    let base = commit(&ctx, &prefix, &ns, "main", None, vec![file("base")], vec![])
        .await
        .unwrap();

    CatalogServer::create_dataset_ref(
        ds_params(&prefix, &ns),
        CreateDatasetRefRequest {
            name: "incoming".to_string(),
            typ: DatasetRefType::Branch,
            source: DatasetRefSource::Snapshot { snapshot_id: base },
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    let protected = CatalogServer::set_dataset_ref_protection(
        ref_params(&prefix, &ns, "main"),
        SetDatasetRefProtectionRequest { protected: true },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .expect("protection can be set");
    assert!(protected.protected);

    // Direct commits are refused ...
    let err = commit(
        &ctx,
        &prefix,
        &ns,
        "main",
        Some(base),
        vec![file("direct")],
        vec![],
    )
    .await
    .expect_err("a protected branch must refuse direct commits");
    assert_eq!(err.error.code, StatusCode::FORBIDDEN, "got: {err:?}");

    // ... but work done on another branch still lands by fast-forward.
    let ahead = commit(
        &ctx,
        &prefix,
        &ns,
        "incoming",
        Some(base),
        vec![file("reviewed")],
        vec![],
    )
    .await
    .unwrap();

    CatalogServer::move_dataset_ref(
        ref_params(&prefix, &ns, "main"),
        MoveDatasetRefRequest {
            snapshot_id: ahead,
            expected_snapshot_id: Some(base),
            fast_forward: true,
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .expect("fast-forward reaches a protected branch");

    assert_eq!(
        file_keys(&ctx, &prefix, &ns, "main").await,
        vec!["base", "reviewed"]
    );

    // Lifting protection restores direct commits.
    CatalogServer::set_dataset_ref_protection(
        ref_params(&prefix, &ns, "main"),
        SetDatasetRefProtectionRequest { protected: false },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    commit(
        &ctx,
        &prefix,
        &ns,
        "main",
        Some(ahead),
        vec![file("direct")],
        vec![],
    )
    .await
    .expect("an unprotected branch accepts commits again");
}

#[sqlx::test]
async fn test_a_protected_ref_cannot_be_deleted(pool: PgPool) {
    let (ctx, prefix, ns) = make_dataset(pool).await;

    let snap = commit(&ctx, &prefix, &ns, "main", None, vec![file("a")], vec![])
        .await
        .unwrap();
    CatalogServer::create_dataset_ref(
        ds_params(&prefix, &ns),
        CreateDatasetRefRequest {
            name: "release".to_string(),
            typ: DatasetRefType::Tag,
            source: DatasetRefSource::Snapshot { snapshot_id: snap },
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    CatalogServer::set_dataset_ref_protection(
        ref_params(&prefix, &ns, "release"),
        SetDatasetRefProtectionRequest { protected: true },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    let err = CatalogServer::delete_dataset_ref(
        ref_params(&prefix, &ns, "release"),
        ctx,
        random_request_metadata(),
    )
    .await
    .expect_err("a protected ref must not be deletable");
    assert_eq!(err.error.code, StatusCode::FORBIDDEN, "got: {err:?}");
}

#[sqlx::test]
async fn test_keys_escaping_the_dataset_location_are_refused(pool: PgPool) {
    let (ctx, prefix, ns) = make_dataset(pool).await;

    for bad in ["/absolute.jpg", "../escape.jpg", "train/../../escape.jpg"] {
        let err = commit(&ctx, &prefix, &ns, "main", None, vec![file(bad)], vec![])
            .await
            .unwrap_err();
        assert_eq!(
            err.error.code,
            StatusCode::BAD_REQUEST,
            "key '{bad}' must be refused, got: {err:?}"
        );
    }
}

#[sqlx::test]
async fn test_unaddressable_ref_names_are_refused(pool: PgPool) {
    let (ctx, prefix, ns) = make_dataset(pool).await;

    let base = commit(&ctx, &prefix, &ns, "main", None, vec![file("a")], vec![])
        .await
        .unwrap();

    // A ref name travels as a single path segment. Anything that splits the
    // segment or changes how the URL parses would produce a ref that cannot be
    // addressed again, so it is refused up front rather than written and lost.
    for bad in [
        "",
        "feature/x",
        "with space",
        "a%2Fb",
        "q?x",
        "frag#1",
        "..",
        "tab\there",
    ] {
        let err = CatalogServer::create_dataset_ref(
            ds_params(&prefix, &ns),
            CreateDatasetRefRequest {
                name: bad.to_string(),
                typ: DatasetRefType::Branch,
                source: DatasetRefSource::Snapshot { snapshot_id: base },
            },
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .expect_err(&format!("ref name {bad:?} should be refused"));
        assert_eq!(err.error.code, 400, "ref name {bad:?}");
        assert_eq!(err.error.r#type, "InvalidRefName", "ref name {bad:?}");
    }

    // Names that stay addressable are accepted, including the ones people
    // actually reach for.
    for good in ["v1.2.3", "release_2026", "exp-42", "Ünïcode"] {
        CatalogServer::create_dataset_ref(
            ds_params(&prefix, &ns),
            CreateDatasetRefRequest {
                name: good.to_string(),
                typ: DatasetRefType::Branch,
                source: DatasetRefSource::Snapshot { snapshot_id: base },
            },
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap_or_else(|e| panic!("ref name {good:?} should be accepted: {e:?}"));
    }
}

#[sqlx::test]
async fn test_a_page_of_only_removed_files_still_hands_back_a_token(pool: PgPool) {
    let (ctx, prefix, ns) = make_dataset(pool).await;

    // Twelve files, of which the first ten are then deleted. With a page size of
    // ten, the first page scans exactly the ten removed keys.
    let keys: Vec<String> = (0..12).map(|i| format!("f{i:02}")).collect();
    let base = commit(
        &ctx,
        &prefix,
        &ns,
        "main",
        None,
        keys.iter().map(|k| file(k)).collect(),
        vec![],
    )
    .await
    .unwrap();
    commit(
        &ctx,
        &prefix,
        &ns,
        "main",
        Some(base),
        vec![],
        keys[..10].to_vec(),
    )
    .await
    .unwrap();

    // The paging bound is applied inside the manifest fold, so a page can come
    // back empty while files remain. That is only safe because the token tracks
    // how far the scan reached rather than the last surviving row -- otherwise a
    // caller would read this empty page as the end and lose the two live files.
    let first = CatalogServer::list_dataset_files(
        ref_params(&prefix, &ns, "main"),
        ListDatasetFilesQuery {
            page_size: Some(10),
            ..Default::default()
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    assert!(first.files.is_empty(), "every key on this page was removed");
    let token = first
        .next_page_token
        .expect("an empty page must still continue the scan");

    let second = CatalogServer::list_dataset_files(
        ref_params(&prefix, &ns, "main"),
        ListDatasetFilesQuery {
            page_size: Some(10),
            page_token: Some(token),
            ..Default::default()
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    assert_eq!(
        second
            .files
            .into_iter()
            .map(|f| f.logical_key)
            .collect::<Vec<_>>(),
        vec!["f10", "f11"],
    );
    assert!(
        second.next_page_token.is_none(),
        "a short page is the end of the scan"
    );
}

#[sqlx::test]
async fn test_a_staged_commit_is_invisible_until_it_is_finished(pool: PgPool) {
    let (ctx, prefix, ns) = make_dataset(pool.clone()).await;
    let warehouse_id = prefix.parse::<Uuid>().unwrap();
    let dataset_id: Uuid = sqlx::query_scalar("SELECT dataset_id FROM dataset")
        .fetch_one(&pool)
        .await
        .unwrap();
    let staged = DatasetSnapshotId::from(Uuid::now_v7());

    let mut t =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();
    PostgresBackend::begin_dataset_commit(
        warehouse_id.into(),
        dataset_id.into(),
        "main",
        staged,
        None,
        "memory://test/ds",
        None,
        t.transaction(),
    )
    .await
    .unwrap();
    PostgresBackend::stage_dataset_files(
        warehouse_id.into(),
        dataset_id.into(),
        staged,
        &[ManifestEntry {
            logical_key: "staged.txt".to_string(),
            physical_path: "memory://test/ds/staged.txt".to_string(),
            etag: None,
            size: Some(1),
            content_type: None,
            checksum: None,
            version_id: None,
            last_modified: None,
        }],
        &[],
        &[],
        t.transaction(),
    )
    .await
    .unwrap();
    t.commit().await.unwrap();

    // Rows are written but no ref points at the snapshot, so nothing resolves
    // through it. This is what lets a scan run for minutes without anyone seeing
    // a half-built file list.
    assert!(file_keys(&ctx, &prefix, &ns, "main").await.is_empty());

    let mut t =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();
    PostgresBackend::finish_dataset_commit(
        warehouse_id.into(),
        dataset_id.into(),
        "main",
        staged,
        None,
        t.transaction(),
    )
    .await
    .unwrap();
    t.commit().await.unwrap();

    assert_eq!(
        file_keys(&ctx, &prefix, &ns, "main").await,
        vec!["staged.txt"]
    );
}

#[sqlx::test]
async fn test_an_abandoned_staging_snapshot_is_swept_but_a_live_one_is_not(pool: PgPool) {
    let (ctx, prefix, _ns) = make_dataset(pool.clone()).await;
    let warehouse_id = prefix.parse::<Uuid>().unwrap();
    let dataset_id: Uuid = sqlx::query_scalar("SELECT dataset_id FROM dataset")
        .fetch_one(&pool)
        .await
        .unwrap();

    // Two abandoned commits: one that "died" long ago, one still in flight.
    for snapshot in [DatasetSnapshotId::from(Uuid::now_v7()); 1] {
        let mut t = <PostgresBackend as CatalogStore>::Transaction::begin_write(
            ctx.v1_state.catalog.clone(),
        )
        .await
        .unwrap();
        PostgresBackend::begin_dataset_commit(
            warehouse_id.into(),
            dataset_id.into(),
            "main",
            snapshot,
            None,
            "memory://test/ds",
            None,
            t.transaction(),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();
    }
    let fresh = DatasetSnapshotId::from(Uuid::now_v7());
    let mut t =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();
    PostgresBackend::begin_dataset_commit(
        warehouse_id.into(),
        dataset_id.into(),
        "main",
        fresh,
        None,
        "memory://test/ds",
        None,
        t.transaction(),
    )
    .await
    .unwrap();
    t.commit().await.unwrap();

    // Age the first one past the threshold.
    sqlx::query("UPDATE dataset_snapshot SET created_at = now() - interval '48 hours' WHERE snapshot_id <> $1")
        .bind(*fresh)
        .execute(&pool)
        .await
        .unwrap();

    let mut t =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(ctx.v1_state.catalog.clone())
            .await
            .unwrap();
    let swept = PostgresBackend::expire_staging_snapshots(
        warehouse_id.into(),
        Some(dataset_id.into()),
        chrono::Utc::now() - chrono::TimeDelta::hours(24),
        t.transaction(),
    )
    .await
    .unwrap();
    t.commit().await.unwrap();

    assert_eq!(swept, 1, "only the aged snapshot should be swept");

    // The in-flight one survives: sweeping it would destroy a running import.
    let remaining: i64 =
        sqlx::query_scalar("SELECT count(*) FROM dataset_snapshot WHERE status = 'staging'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(remaining, 1);
}
