//! Versioning tests: commits, refs, and the concurrency semantics.
//!
//! The interesting assertions here are the negative ones. A tag that keeps
//! pointing at its original snapshot after the branch moves on, a stale writer
//! that loses without overwriting, and a fast-forward that refuses to abandon
//! commits are the properties the whole design rests on.
use std::future::Future;

use chrono::Utc;
use http::StatusCode;
use lakekeeper::{
    api::{
        Result,
        data::v1::datasets::{
            CommitDatasetRequest, CreateDatasetRefRequest, DatasetRefSource, DatasetService as _,
            ImportDatasetRequest, ImportDatasetResponse, ImportMode, ListDatasetFilesQuery,
            MoveDatasetRefRequest, SetDatasetRefProtectionRequest,
        },
        iceberg::{types::DropParams, v1::namespace::NamespaceDropFlags},
    },
    server::CatalogServer,
    service::{
        CatalogDatasetOps, ConstraintViolationPolicy, DatasetConstraints, DatasetOwnership,
        DatasetRefType, DatasetSnapshotId, ListedObject, ManifestEntry, Transaction,
        authz::tests::HidingAuthorizer,
    },
};
use lakekeeper_integration_tests::{
    CommitFileExt as _, CommitGate, DATASET, TestDataset, TestNamespace, drop_namespace, file,
    random_request_metadata, wait_for_lock_wait,
};
use lakekeeper_storage_postgres::PostgresBackend;
use sqlx::PgPool;
use uuid::Uuid;

#[sqlx::test]
async fn test_new_dataset_has_an_empty_main_branch(pool: PgPool) {
    let ds = TestDataset::managed(pool).await;

    let refs =
        CatalogServer::list_dataset_refs(ds.params(), ds.ctx.clone(), random_request_metadata())
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
    let ds = TestDataset::managed(pool).await;

    let snap = ds
        .try_commit(
            "main",
            None,
            vec![file("train/a.jpg"), file("train/b.jpg")],
            &[],
        )
        .await
        .expect("first commit");

    assert_eq!(
        ds.file_keys("main").await,
        vec!["train/a.jpg", "train/b.jpg"]
    );

    // The second commit is a delta against the first, not a rewrite.
    ds.try_commit(
        "main",
        Some(snap),
        vec![file("train/c.jpg")],
        &["train/a.jpg"],
    )
    .await
    .expect("second commit");

    assert_eq!(
        ds.file_keys("main").await,
        vec!["train/b.jpg", "train/c.jpg"]
    );
}

#[sqlx::test]
async fn test_stale_commit_conflicts_and_reports_the_head(pool: PgPool) {
    let ds = TestDataset::managed(pool).await;

    let first = ds.commit(None, vec![file("a")], &[]).await;
    ds.commit(Some(first), vec![file("b")], &[]).await;

    // A writer still holding `first` as its parent has been overtaken. It must
    // lose, leaving the commit that landed in between intact.
    let err = ds
        .try_commit("main", Some(first), vec![file("c")], &[])
        .await
        .expect_err("a stale parent must conflict");
    assert_eq!(err.error.code, StatusCode::CONFLICT, "got: {err:?}");

    // And the losing commit left nothing behind.
    assert_eq!(ds.file_keys("main").await, vec!["a", "b"]);
}

#[sqlx::test]
async fn test_tag_pins_its_snapshot_while_the_branch_moves_on(pool: PgPool) {
    let ds = TestDataset::managed(pool).await;

    let first = ds.commit(None, vec![file("train/a.jpg")], &[]).await;

    CatalogServer::create_dataset_ref(
        ds.params(),
        CreateDatasetRefRequest {
            name: "training-2026-07".to_string(),
            typ: DatasetRefType::Tag,
            source: DatasetRefSource::Ref {
                name: "main".to_string(),
            },
        },
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .expect("tag creation");

    ds.commit(Some(first), vec![file("train/b.jpg")], &[]).await;

    // This is the reproducibility promise: the tag resolves to the same files
    // it did when it was cut, however far the branch has moved.
    assert_eq!(ds.file_keys("training-2026-07").await, vec!["train/a.jpg"]);
    assert_eq!(
        ds.file_keys("main").await,
        vec!["train/a.jpg", "train/b.jpg"]
    );
}

#[sqlx::test]
async fn test_commit_to_a_tag_is_refused(pool: PgPool) {
    let ds = TestDataset::managed(pool).await;

    let snap = ds.commit(None, vec![file("a")], &[]).await;
    CatalogServer::create_dataset_ref(
        ds.params(),
        CreateDatasetRefRequest {
            name: "v1".to_string(),
            typ: DatasetRefType::Tag,
            source: DatasetRefSource::Snapshot { snapshot_id: snap },
        },
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    let err = ds
        .try_commit("v1", Some(snap), vec![file("b")], &[])
        .await
        .expect_err("a tag must not accept commits");
    assert_eq!(err.error.code, StatusCode::CONFLICT, "got: {err:?}");
}

#[sqlx::test]
async fn test_branch_is_zero_copy_and_diverges_independently(pool: PgPool) {
    let ds = TestDataset::managed(pool).await;

    let base = ds.commit(None, vec![file("shared")], &[]).await;

    CatalogServer::create_dataset_ref(
        ds.params(),
        CreateDatasetRefRequest {
            name: "experiment".to_string(),
            typ: DatasetRefType::Branch,
            source: DatasetRefSource::Snapshot { snapshot_id: base },
        },
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    ds.commit_to(
        "experiment",
        Some(base),
        vec![file("only-on-experiment")],
        &[],
    )
    .await;

    // Committing to a branch leaves every other ref untouched: visibility is
    // defined by manifests, not by what is in the bucket.
    assert_eq!(ds.file_keys("main").await, vec!["shared"]);
    assert_eq!(
        ds.file_keys("experiment").await,
        vec!["only-on-experiment", "shared"]
    );
}

#[sqlx::test]
async fn test_fast_forward_promotes_and_refuses_a_non_descendant(pool: PgPool) {
    let ds = TestDataset::managed(pool).await;

    let base = ds.commit(None, vec![file("base")], &[]).await;

    CatalogServer::create_dataset_ref(
        ds.params(),
        CreateDatasetRefRequest {
            name: "incoming".to_string(),
            typ: DatasetRefType::Branch,
            source: DatasetRefSource::Snapshot { snapshot_id: base },
        },
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    let ahead = ds
        .commit_to("incoming", Some(base), vec![file("new")], &[])
        .await;

    // Promote: main is behind, and `ahead` descends from it.
    CatalogServer::move_dataset_ref(
        ds.ref_params("main"),
        MoveDatasetRefRequest {
            snapshot_id: ahead,
            expected_snapshot_id: Some(base),
            fast_forward: true,
        },
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .expect("fast-forward to a descendant");

    assert_eq!(ds.file_keys("main").await, vec!["base", "new"]);

    // Going back to `base` would abandon `ahead`. Fast-forward must refuse;
    // that is what reset is for, and reset needs its own permission.
    let err = CatalogServer::move_dataset_ref(
        ds.ref_params("main"),
        MoveDatasetRefRequest {
            snapshot_id: base,
            expected_snapshot_id: Some(ahead),
            fast_forward: true,
        },
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .expect_err("fast-forward must refuse a non-descendant");
    assert_eq!(err.error.code, StatusCode::CONFLICT, "got: {err:?}");

    // The same move as a reset is allowed.
    CatalogServer::move_dataset_ref(
        ds.ref_params("main"),
        MoveDatasetRefRequest {
            snapshot_id: base,
            expected_snapshot_id: Some(ahead),
            fast_forward: false,
        },
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .expect("reset may move a branch backwards");

    assert_eq!(ds.file_keys("main").await, vec!["base"]);
}

#[sqlx::test]
async fn test_main_cannot_be_deleted_but_other_refs_can(pool: PgPool) {
    let ds = TestDataset::managed(pool).await;

    let snap = ds.commit(None, vec![file("a")], &[]).await;
    CatalogServer::create_dataset_ref(
        ds.params(),
        CreateDatasetRefRequest {
            name: "scratch".to_string(),
            typ: DatasetRefType::Branch,
            source: DatasetRefSource::Snapshot { snapshot_id: snap },
        },
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    let err = CatalogServer::delete_dataset_ref(
        ds.ref_params("main"),
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .expect_err("main must not be deletable");
    // A structural refusal, as deleting a protected tabular is.
    assert_eq!(err.error.code, StatusCode::CONFLICT, "got: {err:?}");
    assert_eq!(err.error.r#type, "CannotDeleteMainBranch");

    CatalogServer::delete_dataset_ref(
        ds.ref_params("scratch"),
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .expect("a non-main ref deletes");

    let refs =
        CatalogServer::list_dataset_refs(ds.params(), ds.ctx.clone(), random_request_metadata())
            .await
            .unwrap();
    assert_eq!(refs.refs.len(), 1);
    assert_eq!(refs.refs[0].name, "main");
}

#[sqlx::test]
async fn test_content_type_filter(pool: PgPool) {
    let ds = TestDataset::managed(pool).await;

    ds.commit(
        None,
        vec![
            file("train/a.jpg").content_type("image/jpeg"),
            file("docs/a.pdf").content_type("application/pdf"),
        ],
        &[],
    )
    .await;

    let listed = CatalogServer::list_dataset_files(
        ds.ref_params("main"),
        ListDatasetFilesQuery {
            content_type: Some("application/pdf".to_string()),
            ..Default::default()
        },
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    let keys: Vec<String> = listed.files.into_iter().map(|f| f.logical_key).collect();
    assert_eq!(keys, vec!["docs/a.pdf"]);
}

#[sqlx::test]
async fn test_constraints_are_enforced_at_commit(pool: PgPool) {
    let ds = TestNamespace::new(pool)
        .await
        .create_dataset(
            DATASET,
            DatasetOwnership::Managed,
            Some(DatasetConstraints {
                allowed_content_types: Some(vec!["image/jpeg".to_string()]),
                max_file_size: Some(2048),
            }),
        )
        .await;

    let wrong_type = file("docs/a.pdf").content_type("application/pdf");
    let err = ds
        .try_commit("main", None, vec![wrong_type], &[])
        .await
        .expect_err("a disallowed content type must fail the commit");
    assert_eq!(err.error.code, StatusCode::BAD_REQUEST, "got: {err:?}");

    let too_big = file("train/big.jpg").content_type("image/jpeg").size(9999);
    let err = ds
        .try_commit("main", None, vec![too_big], &[])
        .await
        .expect_err("an oversized file must fail the commit");
    assert_eq!(err.error.code, StatusCode::BAD_REQUEST, "got: {err:?}");

    // A limit that cannot be checked must not pass.
    let mut no_size = file("train/no-size.jpg").content_type("image/jpeg");
    no_size.size = None;
    let err = ds
        .try_commit("main", None, vec![no_size], &[])
        .await
        .expect_err("a file without a size must fail a size limit");
    assert_eq!(err.error.code, StatusCode::BAD_REQUEST, "got: {err:?}");

    // A rejected commit is not a partial commit: nothing landed.
    let refs =
        CatalogServer::list_dataset_refs(ds.params(), ds.ctx.clone(), random_request_metadata())
            .await
            .unwrap();
    assert!(refs.refs[0].snapshot_id.is_none());

    ds.try_commit(
        "main",
        None,
        vec![file("train/ok.jpg").content_type("image/jpeg")],
        &[],
    )
    .await
    .expect("a conforming file commits");
}

/// Pagination must stay on the snapshot the first page resolved, or a commit
/// landing between pages would splice two different file lists together.
#[sqlx::test]
async fn test_pagination_is_pinned_to_the_first_pages_snapshot(pool: PgPool) {
    let ds = TestDataset::managed(pool).await;

    let first = ds
        .commit(None, vec![file("a"), file("b"), file("c")], &[])
        .await;

    let page1 = CatalogServer::list_dataset_files(
        ds.ref_params("main"),
        ListDatasetFilesQuery {
            page_size: Some(2),
            ..Default::default()
        },
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    assert_eq!(page1.files.len(), 2);
    assert_eq!(page1.snapshot_id, Some(first));
    let token = page1.next_page_token.clone().expect("a token");

    // The branch moves on: "d" is added and "c" — which page 2 is about to
    // return — is removed.
    ds.commit(Some(first), vec![file("d")], &["c"]).await;

    let page2 = CatalogServer::list_dataset_files(
        ds.ref_params("main"),
        ListDatasetFilesQuery {
            page_token: Some(token),
            page_size: Some(2),
            ..Default::default()
        },
        ds.ctx.clone(),
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
    assert_eq!(ds.file_keys("main").await, vec!["a", "b", "d"]);
}

/// A page token continues only the listing it came from: not a listing of another
/// ref, and not one whose snapshot is unpublished.
#[sqlx::test]
async fn test_a_page_token_continues_only_its_own_listing(pool: PgPool) {
    let ds = TestDataset::managed(pool.clone()).await;
    let head = ds.commit(None, vec![file("a"), file("b")], &[]).await;
    CatalogServer::create_dataset_ref(
        ds.params(),
        CreateDatasetRefRequest {
            name: "dev".to_string(),
            typ: DatasetRefType::Branch,
            source: DatasetRefSource::Ref {
                name: "main".to_string(),
            },
        },
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    let list = |r: &str, page_token: Option<String>| {
        CatalogServer::list_dataset_files(
            ds.ref_params(r),
            ListDatasetFilesQuery {
                page_token,
                page_size: Some(1),
                ..Default::default()
            },
            ds.ctx.clone(),
            random_request_metadata(),
        )
    };
    let token = list("main", None)
        .await
        .unwrap()
        .next_page_token
        .expect("a second page");

    let err = list("dev", Some(token.clone()))
        .await
        .expect_err("a token from main must not continue a listing of dev");
    assert_eq!(err.error.code, StatusCode::BAD_REQUEST, "{err:?}");
    list("main", Some(token.clone()))
        .await
        .expect("the token continues its own listing");

    sqlx::query("UPDATE dataset_snapshot SET status = 'staging' WHERE snapshot_id = $1")
        .bind(Uuid::from(head))
        .execute(&pool)
        .await
        .unwrap();
    let err = list("main", Some(token))
        .await
        .expect_err("a token must not reach an unpublished snapshot");
    assert_eq!(err.error.code, StatusCode::NOT_FOUND, "{err:?}");
}

/// Genuinely concurrent committers, not a sequential stand-in.
///
/// All of them read the same parent and race to move the branch. The
/// compare-and-swap must admit exactly one: the losers fail outright, and the
/// winner's files must be the only ones present.
#[sqlx::test]
async fn test_concurrent_committers_admit_exactly_one_winner(pool: PgPool) {
    let ds = TestDataset::managed(pool).await;

    let base = ds.commit(None, vec![file("base")], &[]).await;

    const WRITERS: usize = 8;
    let mut handles = Vec::with_capacity(WRITERS);
    for i in 0..WRITERS {
        let ds = ds.clone();
        handles.push(tokio::spawn(async move {
            // Every writer commits against the same parent it read.
            ds.try_commit("main", Some(base), vec![file(&format!("writer-{i}"))], &[])
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
    let keys = ds.file_keys("main").await;
    assert_eq!(keys.len(), 2, "base plus exactly one writer, got {keys:?}");
    assert!(keys.contains(&"base".to_string()));

    let refs =
        CatalogServer::list_dataset_refs(ds.params(), ds.ctx.clone(), random_request_metadata())
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
    let ds = TestDataset::managed(pool).await;

    let base = ds.commit(None, vec![file("base")], &[]).await;
    let winner = ds.commit(Some(base), vec![file("winner")], &[]).await;

    let err = ds
        .try_commit("main", Some(base), vec![file("loser")], &[])
        .await
        .unwrap_err();
    assert_eq!(err.error.code, StatusCode::CONFLICT);

    // Same payload, new parent.
    ds.try_commit("main", Some(winner), vec![file("loser")], &[])
        .await
        .expect("rebased commit lands");

    assert_eq!(ds.file_keys("main").await, vec!["base", "loser", "winner"]);
}

/// A long chain must reconstruct correctly across the checkpoint boundary.
///
/// Checkpoints exist to bound reconstruction, so the property that matters is
/// that they change *cost*, never the answer: adds, removals and re-adds made
/// before and after a checkpoint must all still resolve the same way, and a tag
/// cut before the fold must keep resolving to its own snapshot.
#[sqlx::test]
async fn test_reconstruction_is_correct_across_a_checkpoint(pool: PgPool) {
    let ds = TestDataset::managed(pool.clone()).await;

    let mut parent = None;
    for i in 0..10 {
        parent = Some(
            ds.try_commit("main", parent, vec![file(&format!("f{i:02}"))], &[])
                .await
                .unwrap_or_else(|e| panic!("commit {i} failed: {e:?}")),
        );
    }
    CatalogServer::create_dataset_ref(
        ds.params(),
        CreateDatasetRefRequest {
            name: "before".to_string(),
            typ: DatasetRefType::Tag,
            source: DatasetRefSource::Snapshot {
                snapshot_id: parent.unwrap(),
            },
        },
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    // A removal the checkpoint has to fold in.
    parent = Some(ds.commit(parent, vec![], &["f05"]).await);
    for i in 10..25 {
        parent = Some(
            ds.try_commit("main", parent, vec![file(&format!("f{i:02}"))], &[])
                .await
                .unwrap_or_else(|e| panic!("commit {i} failed: {e:?}")),
        );
    }

    // Commits only enqueue the fold; do what the worker does.
    let folded = ds.checkpoint("main").await;
    assert_eq!(folded, parent, "the head is the checkpoint");

    // Deltas on top of the checkpoint: remove a file it restates, and re-add the
    // one it folded out.
    parent = Some(ds.commit(parent, vec![], &["f03"]).await);
    ds.commit(parent, vec![file("f05")], &["f07"]).await;

    let expected: Vec<String> = (0..25)
        .map(|i| format!("f{i:02}"))
        .filter(|k| k != "f03" && k != "f07")
        .collect();
    assert_eq!(
        ds.file_keys("main").await,
        expected,
        "reconstruction across a checkpoint"
    );

    // The fold restated the head; it did not rewrite what came before it.
    let tagged: Vec<String> = (0..10).map(|i| format!("f{i:02}")).collect();
    assert_eq!(ds.file_keys("before").await, tagged);
}

/// At least one snapshot in a long chain must actually become a checkpoint,
/// otherwise the bound above is never exercised in practice.
#[sqlx::test]
async fn test_a_long_chain_produces_a_checkpoint(pool: PgPool) {
    let ds = TestDataset::managed(pool.clone()).await;

    let mut parent = None;
    for i in 0..25 {
        let snapshot = ds
            .commit(parent, vec![file(&format!("f{i:02}"))], &[])
            .await;
        parent = Some(snapshot);
    }

    // Crossing the interval only enqueues the fold: the commit that trips the
    // threshold must not pay for restating the whole file set. Twenty-five commits
    // past the threshold still produce exactly one task, because the task table
    // admits one active task per (entity, queue).
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
    let checkpointed = ds.checkpoint("main").await;

    assert_eq!(
        checkpointed,
        Some(parent.unwrap()),
        "the fold targets the branch head at run time, not the snapshot that \
         tripped the threshold"
    );

    // Folding changes how far back a read walks, never what it returns.
    assert_eq!(ds.file_keys("main").await.len(), 25);

    // A second run finds nothing due and is a cheap no-op, which is what makes
    // retries and duplicate tasks harmless.
    let again = ds.checkpoint("main").await;
    assert_eq!(again, None, "an already-folded branch has nothing due");
}

/// A protected branch refuses direct commits, resets and deletion for everyone, and takes
/// changes only by fast-forward. Structural, so it holds regardless of permission.
#[sqlx::test]
async fn test_a_protected_branch_takes_changes_only_by_fast_forward(pool: PgPool) {
    let ds = TestDataset::managed(pool).await;

    let base = ds.commit(None, vec![file("base")], &[]).await;

    CatalogServer::create_dataset_ref(
        ds.params(),
        CreateDatasetRefRequest {
            name: "incoming".to_string(),
            typ: DatasetRefType::Branch,
            source: DatasetRefSource::Snapshot { snapshot_id: base },
        },
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    let protected = CatalogServer::set_dataset_ref_protection(
        ds.ref_params("main"),
        SetDatasetRefProtectionRequest { protected: true },
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .expect("protection can be set");
    assert!(protected.protected);

    // Direct commits are refused ...
    let err = ds
        .try_commit("main", Some(base), vec![file("direct")], &[])
        .await
        .expect_err("a protected branch must refuse direct commits");
    assert_eq!(err.error.code, StatusCode::CONFLICT, "got: {err:?}");
    assert_eq!(err.error.r#type, "DatasetRefProtected");

    // ... but work done on another branch still lands by fast-forward.
    let ahead = ds
        .commit_to("incoming", Some(base), vec![file("reviewed")], &[])
        .await;

    CatalogServer::move_dataset_ref(
        ds.ref_params("main"),
        MoveDatasetRefRequest {
            snapshot_id: ahead,
            expected_snapshot_id: Some(base),
            fast_forward: true,
        },
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .expect("fast-forward reaches a protected branch");

    assert_eq!(ds.file_keys("main").await, vec!["base", "reviewed"]);

    // Lifting protection restores direct commits.
    CatalogServer::set_dataset_ref_protection(
        ds.ref_params("main"),
        SetDatasetRefProtectionRequest { protected: false },
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    ds.try_commit("main", Some(ahead), vec![file("direct")], &[])
        .await
        .expect("an unprotected branch accepts commits again");
}

#[sqlx::test]
async fn test_a_protected_ref_cannot_be_deleted(pool: PgPool) {
    let ds = TestDataset::managed(pool).await;

    let snap = ds.commit(None, vec![file("a")], &[]).await;
    CatalogServer::create_dataset_ref(
        ds.params(),
        CreateDatasetRefRequest {
            name: "release".to_string(),
            typ: DatasetRefType::Tag,
            source: DatasetRefSource::Snapshot { snapshot_id: snap },
        },
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    CatalogServer::set_dataset_ref_protection(
        ds.ref_params("release"),
        SetDatasetRefProtectionRequest { protected: true },
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    let err = CatalogServer::delete_dataset_ref(
        ds.ref_params("release"),
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .expect_err("a protected ref must not be deletable");
    assert_eq!(err.error.code, StatusCode::CONFLICT, "got: {err:?}");
    assert_eq!(err.error.r#type, "DatasetRefProtected");
}

/// A key names one object inside the dataset: a full URI would make it its own
/// physical path, read and signed wherever it points, and an empty segment would
/// read another key's object.
#[sqlx::test]
async fn test_keys_escaping_the_dataset_location_are_refused(pool: PgPool) {
    let ds = TestDataset::managed(pool).await;

    let too_long = "k".repeat(1025);
    for bad in [
        "/absolute.jpg",
        "../escape.jpg",
        "train/../../escape.jpg",
        "s3://other-bucket/secret.parquet",
        "memory://elsewhere/secret.parquet",
        "a//b.jpg",
        "dir/",
        "./a.jpg",
        "zero\u{200b}width.jpg",
        too_long.as_str(),
        // What a signer building a URL resolves as `..` or a separator.
        "%2e%2e/%2e%2e/other-bucket/secret.parquet",
        "a/.%2E/b.jpg",
        "x\\..\\..\\secret.parquet",
        // What would end a URL's path.
        "music/track #1.mp3",
        "why?.jpg",
    ] {
        let err = ds
            .try_commit("main", None, vec![file(bad)], &[])
            .await
            .unwrap_err();
        assert_eq!(
            err.error.code,
            StatusCode::BAD_REQUEST,
            "key '{bad}' must be refused, got: {err:?}"
        );
        assert_eq!(err.error.r#type, "InvalidKey", "key '{bad}'");
    }
    assert!(ds.file_keys("main").await.is_empty());

    // Spaces, escapes that are no dot segment, and letters beyond ASCII are names.
    let fine = ["train/cat 1.jpg", "%41.jpg", "données/é.jpg"];
    ds.try_commit("main", None, fine.iter().map(|k| file(k)).collect(), &[])
        .await
        .expect("ordinary names commit");
}

/// A commit names each key once: adding it twice, or adding and removing it, has
/// no single meaning, and would be counted as both.
#[sqlx::test]
async fn test_a_commit_naming_a_key_twice_is_refused(pool: PgPool) {
    let ds = TestDataset::managed(pool).await;
    let base = ds.commit(None, vec![file("a")], &[]).await;

    for (added, removed) in [
        (vec![file("b"), file("b")], &[][..]),
        (vec![file("a")], &["a"][..]),
    ] {
        let err = ds
            .try_commit("main", Some(base), added, removed)
            .await
            .unwrap_err();
        assert_eq!(err.error.code, StatusCode::BAD_REQUEST, "{err:?}");
        assert_eq!(err.error.r#type, "DuplicateKey");
    }
}

/// A commit whose parent is no snapshot of the branch, a typo or one purged
/// since, is a conflict like any stale parent: the caller learns the head.
#[sqlx::test]
async fn test_a_commit_on_an_unknown_parent_conflicts(pool: PgPool) {
    let ds = TestDataset::managed(pool).await;
    let head = ds.commit(None, vec![file("a")], &[]).await;

    let unknown = DatasetSnapshotId::from(Uuid::now_v7());
    let err = ds
        .try_commit("main", Some(unknown), vec![file("b")], &[])
        .await
        .unwrap_err();
    assert_eq!(err.error.code, StatusCode::CONFLICT, "{err:?}");
    assert!(
        err.error
            .stack
            .iter()
            .any(|line| line.contains(&head.to_string())),
        "the head is reported: {err:?}"
    );
}

#[sqlx::test]
async fn test_unaddressable_ref_names_are_refused(pool: PgPool) {
    let ds = TestDataset::managed(pool).await;

    let base = ds.commit(None, vec![file("a")], &[]).await;

    // A ref name travels as a single path segment. Anything that splits the
    // segment or changes how the URL parses would produce a ref that cannot be
    // addressed again, so it is refused up front.
    for bad in [
        "",
        "feature/x",
        "with space",
        "a%2Fb",
        "q?x",
        "frag#1",
        "..",
        "tab\there",
        "main\u{200b}",
    ] {
        let err = CatalogServer::create_dataset_ref(
            ds.params(),
            CreateDatasetRefRequest {
                name: bad.to_string(),
                typ: DatasetRefType::Branch,
                source: DatasetRefSource::Snapshot { snapshot_id: base },
            },
            ds.ctx.clone(),
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
            ds.params(),
            CreateDatasetRefRequest {
                name: good.to_string(),
                typ: DatasetRefType::Branch,
                source: DatasetRefSource::Snapshot { snapshot_id: base },
            },
            ds.ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap_or_else(|e| panic!("ref name {good:?} should be accepted: {e:?}"));
    }
}

#[sqlx::test]
async fn test_a_page_of_only_removed_files_still_hands_back_a_token(pool: PgPool) {
    let ds = TestDataset::managed(pool).await;

    // Twelve files, of which the first ten are then deleted. With a page size of
    // ten, the first page scans exactly the ten removed keys.
    let keys: Vec<String> = (0..12).map(|i| format!("f{i:02}")).collect();
    let base = ds
        .commit(None, keys.iter().map(|k| file(k)).collect(), &[])
        .await;
    let removed: Vec<&str> = keys[..10].iter().map(String::as_str).collect();
    ds.commit(Some(base), vec![], &removed).await;

    // The paging bound is applied inside the manifest fold, so a page can come
    // back empty while files remain. That is only safe because the token tracks
    // how far the scan reached, not the last surviving row — otherwise a caller
    // would read this empty page as the end and lose the two live files.
    let first = CatalogServer::list_dataset_files(
        ds.ref_params("main"),
        ListDatasetFilesQuery {
            page_size: Some(10),
            ..Default::default()
        },
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    assert!(first.files.is_empty(), "every key on this page was removed");
    let token = first
        .next_page_token
        .expect("an empty page must still continue the scan");

    let second = CatalogServer::list_dataset_files(
        ds.ref_params("main"),
        ListDatasetFilesQuery {
            page_size: Some(10),
            page_token: Some(token),
            ..Default::default()
        },
        ds.ctx.clone(),
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
    let ds = TestDataset::managed(pool.clone()).await;
    let staged = DatasetSnapshotId::from(Uuid::now_v7());

    let mut t = ds.begin_write().await;
    PostgresBackend::begin_dataset_commit(
        ds.warehouse_id,
        ds.id,
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
        ds.warehouse_id,
        ds.id,
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
        ConstraintViolationPolicy::Reject,
        t.transaction(),
    )
    .await
    .unwrap();
    t.commit().await.unwrap();

    // Rows are written but no ref points at the snapshot, so nothing resolves
    // through it. This is what lets a scan run for minutes without anyone seeing
    // a half-built file list.
    assert!(ds.file_keys("main").await.is_empty());

    let mut t = ds.begin_write().await;
    PostgresBackend::finish_dataset_commit(
        ds.warehouse_id,
        ds.id,
        "main",
        staged,
        None,
        t.transaction(),
    )
    .await
    .unwrap();
    t.commit().await.unwrap();

    assert_eq!(ds.file_keys("main").await, vec!["staged.txt"]);
}

#[sqlx::test]
async fn test_an_abandoned_staging_snapshot_is_swept_but_a_live_one_is_not(pool: PgPool) {
    let ds = TestDataset::managed(pool.clone()).await;

    // Two abandoned commits: one that "died" long ago, one still in flight.
    for snapshot in [DatasetSnapshotId::from(Uuid::now_v7()); 1] {
        let mut t = ds.begin_write().await;
        PostgresBackend::begin_dataset_commit(
            ds.warehouse_id,
            ds.id,
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
    let mut t = ds.begin_write().await;
    PostgresBackend::begin_dataset_commit(
        ds.warehouse_id,
        ds.id,
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

    let mut t = ds.begin_write().await;
    let swept = PostgresBackend::expire_staging_snapshots(
        ds.warehouse_id,
        Some(ds.id),
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

/// Commits a chain of snapshots with a tag and a second branch on it, so a drop
/// has a whole history to remove.
async fn commit_history(ds: &TestDataset) {
    let mut parent = None;
    for i in 0..3 {
        parent = Some(ds.commit(parent, vec![file(&format!("f{i}"))], &[]).await);
    }
    for (name, typ) in [("v1", DatasetRefType::Tag), ("dev", DatasetRefType::Branch)] {
        CatalogServer::create_dataset_ref(
            ds.params(),
            CreateDatasetRefRequest {
                name: name.to_string(),
                typ,
                source: DatasetRefSource::Snapshot {
                    snapshot_id: parent.unwrap(),
                },
            },
            ds.ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();
    }
}

/// Rows per history table: snapshots, refs, manifest entries.
async fn history_rows(pool: &PgPool) -> [i64; 3] {
    let mut rows = [0; 3];
    for (slot, count) in rows.iter_mut().zip([
        "SELECT count(*) FROM dataset_snapshot",
        "SELECT count(*) FROM dataset_ref",
        "SELECT count(*) FROM dataset_manifest_entry",
    ]) {
        *slot = sqlx::query_scalar(count).fetch_one(pool).await.unwrap();
    }
    rows
}

/// Dropping a dataset takes its history with it. The cascade deletes every
/// snapshot in one statement while each parent edge is `on delete restrict`, so
/// this is what shows a chain can be deleted at all.
#[sqlx::test]
async fn test_dropping_a_dataset_removes_its_history(pool: PgPool) {
    let ds = TestDataset::managed(pool.clone()).await;
    commit_history(&ds).await;
    // Three snapshots; main, v1 and dev; one manifest row per commit.
    assert_eq!(history_rows(&pool).await, [3, 3, 3]);

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

    assert_eq!(
        history_rows(&pool).await,
        [0, 0, 0],
        "history outlived the dataset"
    );
}

/// The same cascade, reached through a recursive namespace drop, which deletes
/// the namespace's tabulars in the statement that deletes the namespaces.
#[sqlx::test]
async fn test_a_recursive_namespace_drop_removes_a_datasets_history(pool: PgPool) {
    let ds = TestDataset::managed(pool.clone()).await;
    commit_history(&ds).await;
    // Three snapshots; main, v1 and dev; one manifest row per commit.
    assert_eq!(history_rows(&pool).await, [3, 3, 3]);

    drop_namespace(
        ds.ctx.clone(),
        NamespaceDropFlags {
            force: false,
            purge: false,
            recursive: true,
        },
        ds.namespace_params(),
    )
    .await
    .unwrap();

    assert_eq!(
        history_rows(&pool).await,
        [0, 0, 0],
        "history outlived the dataset"
    );
}

/// A tag names one snapshot for good. Neither a fast-forward nor a reset may move
/// it, or a tag cut for a training run would stop naming the files it was cut on.
#[sqlx::test]
async fn test_a_tag_cannot_be_moved(pool: PgPool) {
    let ds = TestDataset::managed(pool).await;
    let first = ds.commit(None, vec![file("a")], &[]).await;
    CatalogServer::create_dataset_ref(
        ds.params(),
        CreateDatasetRefRequest {
            name: "v1".to_string(),
            typ: DatasetRefType::Tag,
            source: DatasetRefSource::Snapshot { snapshot_id: first },
        },
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    // A descendant, so a fast-forward would otherwise be allowed.
    let second = ds.commit(Some(first), vec![file("b")], &[]).await;

    for fast_forward in [true, false] {
        let err = CatalogServer::move_dataset_ref(
            ds.ref_params("v1"),
            MoveDatasetRefRequest {
                snapshot_id: second,
                expected_snapshot_id: Some(first),
                fast_forward,
            },
            ds.ctx.clone(),
            random_request_metadata(),
        )
        .await
        .expect_err("a tag must not move");
        assert_eq!(
            err.error.code,
            StatusCode::CONFLICT,
            "fast_forward={fast_forward}: {err:?}"
        );
    }
    assert_eq!(ds.file_keys("v1").await, vec!["a"]);
}

/// Protection is structural: a protected branch takes changes only by
/// fast-forward, so a reset is refused whatever the caller may do otherwise.
#[sqlx::test]
async fn test_a_protected_branch_cannot_be_reset(pool: PgPool) {
    let ds = TestDataset::managed(pool).await;
    let base = ds.commit(None, vec![file("base")], &[]).await;
    let ahead = ds.commit(Some(base), vec![file("new")], &[]).await;
    CatalogServer::set_dataset_ref_protection(
        ds.ref_params("main"),
        SetDatasetRefProtectionRequest { protected: true },
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    let err = CatalogServer::move_dataset_ref(
        ds.ref_params("main"),
        MoveDatasetRefRequest {
            snapshot_id: base,
            expected_snapshot_id: Some(ahead),
            fast_forward: false,
        },
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .expect_err("a protected branch must refuse a reset");
    assert_eq!(err.error.code, StatusCode::CONFLICT, "got: {err:?}");
    assert_eq!(err.error.r#type, "DatasetRefProtected");
    assert_eq!(ds.file_keys("main").await, vec!["base", "new"]);
}

/// Publishing checks the snapshot as well as the pointer. A snapshot staged on the
/// wrong parent must not become the head even when the pointer still matches, or
/// the branch would skip every commit between the two.
#[sqlx::test]
async fn test_a_snapshot_staged_on_the_wrong_parent_is_not_published(pool: PgPool) {
    let ds = TestDataset::managed(pool.clone()).await;
    let head = ds.commit(None, vec![file("a")], &[]).await;

    let stray = DatasetSnapshotId::from(Uuid::now_v7());
    let mut t = ds.begin_write().await;
    // Staged as though main had no commits.
    PostgresBackend::begin_dataset_commit(
        ds.warehouse_id,
        ds.id,
        "main",
        stray,
        None,
        &ds.location,
        None,
        t.transaction(),
    )
    .await
    .unwrap();
    let published = PostgresBackend::finish_dataset_commit(
        ds.warehouse_id,
        ds.id,
        "main",
        stray,
        Some(head),
        t.transaction(),
    )
    .await;
    t.commit().await.unwrap();

    assert!(
        published.is_err(),
        "a mis-parented snapshot must not be published"
    );
    assert_eq!(ds.file_keys("main").await, vec!["a"]);
}

/// A published snapshot is immutable: nothing may add files to it, even through
/// the store directly.
#[sqlx::test]
async fn test_files_cannot_be_staged_into_a_published_snapshot(pool: PgPool) {
    let ds = TestDataset::managed(pool.clone()).await;
    let head = ds.commit(None, vec![file("a")], &[]).await;

    let sneaked = ManifestEntry {
        logical_key: "sneaked".to_string(),
        physical_path: "sneaked".to_string(),
        etag: None,
        size: Some(1),
        content_type: None,
        checksum: None,
        version_id: None,
        last_modified: None,
    };
    let mut t = ds.begin_write().await;
    let staged = PostgresBackend::stage_dataset_files(
        ds.warehouse_id,
        ds.id,
        head,
        &[sneaked],
        &[],
        &[],
        ConstraintViolationPolicy::Reject,
        t.transaction(),
    )
    .await;
    t.commit().await.unwrap();

    assert!(
        staged.is_err(),
        "a published snapshot must refuse new files"
    );
    assert_eq!(ds.file_keys("main").await, vec!["a"]);
}

/// A declared physical path stays inside the dataset: relative, or a full URI under
/// its location. One elsewhere would point readers at objects the dataset does not
/// govern.
#[sqlx::test]
async fn test_a_physical_path_outside_the_dataset_is_refused(pool: PgPool) {
    let ds = TestDataset::managed(pool.clone()).await;
    let location = ds.location.trim_end_matches('/');

    // The sibling shares the location as a string prefix but is not inside it, and
    // the location itself is no object below it.
    for outside in [
        "s3://other-bucket/x.jpg".to_string(),
        format!("{location}-sibling/x.jpg"),
        location.to_string(),
    ] {
        let err = ds
            .try_commit("main", None, vec![file("x.jpg").at(outside.clone())], &[])
            .await
            .expect_err("a path outside the dataset must be refused");
        assert_eq!(
            err.error.code,
            StatusCode::BAD_REQUEST,
            "{outside}: {err:?}"
        );
    }

    ds.try_commit(
        "main",
        None,
        vec![
            file("inside.jpg").at(format!("{location}/raw/inside.jpg")),
            file("relative.jpg").at("raw/relative.jpg"),
        ],
        &[],
    )
    .await
    .expect("paths inside the dataset commit");
}

/// Protects `main` in a transaction left open, runs `op` against the branch, and
/// lets the protection land only once `op` is waiting on the ref's row lock — that
/// is, after `op`'s own checks have read the branch as unprotected.
async fn protect_main_while<F, T>(pool: &PgPool, op: F) -> T
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let mut protector = pool.begin().await.unwrap();
    sqlx::query("UPDATE dataset_ref SET protected = true WHERE name = 'main'")
        .execute(&mut *protector)
        .await
        .unwrap();
    commit_once_waited_on(pool, protector, op).await
}

/// Runs `op`, and commits `holder` — a transaction that updated a ref — only once
/// `op` is waiting on that ref's row lock, after everything `op` read before it.
async fn commit_once_waited_on<F, T>(
    pool: &PgPool,
    holder: sqlx::Transaction<'static, sqlx::Postgres>,
    op: F,
) -> T
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let op = tokio::spawn(op);
    wait_for_lock_wait(pool, "UPDATE dataset_ref").await;
    holder.commit().await.unwrap();
    op.await.unwrap()
}

/// Protection is re-checked where the pointer moves: a branch protected after a
/// commit read it, but before the pointer moved, still refuses the commit.
#[sqlx::test]
async fn test_a_branch_protected_mid_commit_refuses_it(pool: PgPool) {
    let ds = TestDataset::managed(pool.clone()).await;
    let head = ds.commit(None, vec![file("a")], &[]).await;

    let committing = ds.clone();
    let published = protect_main_while(&pool, async move {
        let mut t = committing.begin_write().await;
        let snapshot = DatasetSnapshotId::from(Uuid::now_v7());
        PostgresBackend::begin_dataset_commit(
            committing.warehouse_id,
            committing.id,
            "main",
            snapshot,
            Some(head),
            &committing.location,
            None,
            t.transaction(),
        )
        .await
        .unwrap();
        let published = PostgresBackend::finish_dataset_commit(
            committing.warehouse_id,
            committing.id,
            "main",
            snapshot,
            Some(head),
            t.transaction(),
        )
        .await
        .map(|_| ());
        t.commit().await.unwrap();
        published
    })
    .await;

    assert!(
        published.is_err(),
        "a commit must not land on a branch protected under it"
    );
    assert_eq!(ds.file_keys("main").await, vec!["a"]);
}

/// The same for a reset: protection that lands mid-move still refuses it.
#[sqlx::test]
async fn test_a_branch_protected_mid_reset_refuses_it(pool: PgPool) {
    let ds = TestDataset::managed(pool.clone()).await;
    let base = ds.commit(None, vec![file("base")], &[]).await;
    let ahead = ds.commit(Some(base), vec![file("new")], &[]).await;

    let resetting = ds.clone();
    let moved = protect_main_while(&pool, async move {
        CatalogServer::move_dataset_ref(
            resetting.ref_params("main"),
            MoveDatasetRefRequest {
                snapshot_id: base,
                expected_snapshot_id: Some(ahead),
                fast_forward: false,
            },
            resetting.ctx,
            random_request_metadata(),
        )
        .await
        .map(|_| ())
        .map_err(|e| e.error.code)
    })
    .await;

    assert_eq!(moved, Err(StatusCode::CONFLICT.as_u16()));
    assert_eq!(ds.file_keys("main").await, vec!["base", "new"]);
}

async fn sync_import(
    ds: &TestDataset,
    sub_prefix: Option<&str>,
    suffix: Option<&str>,
) -> Result<ImportDatasetResponse> {
    CatalogServer::import_dataset(
        ds.params(),
        ImportDatasetRequest {
            mode: Some(ImportMode::Sync),
            sub_prefix: sub_prefix.map(ToString::to_string),
            suffix: suffix.map(ToString::to_string),
            ..Default::default()
        },
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
}

/// A dataset kept current only by imports still has its chain folded: an import
/// past the checkpoint interval enqueues the fold, as a commit does.
#[sqlx::test]
async fn test_a_chain_grown_by_imports_alone_is_folded(pool: PgPool) {
    let ds = TestDataset::imported(pool.clone()).await;

    for i in 0..25 {
        ds.write(&format!("f{i:02}"), b"x").await;
        let imported = sync_import(&ds, None, None).await.unwrap();
        assert_eq!(imported.imported, 1, "import {i} publishes one file");
    }

    let queued: i64 =
        sqlx::query_scalar("SELECT count(*) FROM task WHERE queue_name = 'dataset_checkpoint'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(queued, 1, "one fold enqueued");
}

/// A sync narrowed by sub-prefix or suffix judges only the keys it scanned: a key
/// it never looked at is not missing.
#[sqlx::test]
async fn test_a_narrowed_sync_removes_only_what_it_scanned(pool: PgPool) {
    let ds = TestDataset::imported(pool.clone()).await;
    // `shard=00-x/` sorts just before `shard=00/` and `shard=000/` just after it,
    // so both sit on the edges of the range a narrowed walk covers.
    for key in [
        "top.csv",
        "shard=00-x/d.parquet",
        "shard=00/a.parquet",
        "shard=00/b.csv",
        "shard=000/e.parquet",
        "shard=01/c.parquet",
    ] {
        ds.write(key, b"x").await;
    }
    let first = sync_import(&ds, None, None).await.unwrap();
    assert_eq!(first.imported, 6);

    // Every object is still there, so no narrowing may remove anything. A trailing
    // `/` names the same directory.
    for (sub_prefix, suffix) in [
        (Some("shard=00"), None),
        (Some("shard=00/"), None),
        (None, Some(".parquet")),
        (Some("shard=00"), Some(".parquet")),
    ] {
        let outcome = sync_import(&ds, sub_prefix, suffix).await.unwrap();
        assert_eq!(
            outcome.removed, 0,
            "sub-prefix {sub_prefix:?}, suffix {suffix:?}"
        );
    }

    // An object that vanished inside the scan is still removed.
    ds.delete("shard=00/a.parquet").await;
    let outcome = sync_import(&ds, Some("shard=00"), None).await.unwrap();
    assert_eq!(outcome.removed, 1);
    assert_eq!(
        ds.file_keys("main").await,
        [
            "shard=00-x/d.parquet",
            "shard=00/b.csv",
            "shard=000/e.parquet",
            "shard=01/c.parquet",
            "top.csv"
        ]
    );
}

/// Job output carries markers and scratch files: the default excludes keep them out,
/// and a glob-narrowed sync judges only the keys its globs match.
#[sqlx::test]
async fn test_globs_and_default_excludes_decide_what_an_import_sees(pool: PgPool) {
    let ds = TestDataset::imported(pool.clone()).await;
    for key in [
        "out/part-0.parquet",
        "out/part-1.csv",
        "out/_SUCCESS",
        "out/_temporary/0/part-2.parquet",
    ] {
        ds.write(key, b"x").await;
    }
    let import = |request: ImportDatasetRequest| {
        CatalogServer::import_dataset(
            ds.params(),
            request,
            ds.ctx.clone(),
            random_request_metadata(),
        )
    };

    import(ImportDatasetRequest::default()).await.unwrap();
    assert_eq!(
        ds.file_keys("main").await,
        ["out/part-0.parquet", "out/part-1.csv"],
        "the marker and the scratch file are left out"
    );

    // With the defaults off, a sync registers them too.
    import(ImportDatasetRequest {
        mode: Some(ImportMode::Sync),
        default_excludes: Some(false),
        ..Default::default()
    })
    .await
    .unwrap();
    assert_eq!(ds.file_keys("main").await.len(), 4);

    // A sync narrowed to parquet judges only parquet keys: the CSV stays although
    // the scan never looked for it, and the excluded scratch file is not removed.
    ds.delete("out/part-0.parquet").await;
    let synced = import(ImportDatasetRequest {
        mode: Some(ImportMode::Sync),
        include: Some(vec!["**/*.parquet".to_string()]),
        exclude: Some(vec!["**/_temporary/**".to_string()]),
        default_excludes: Some(false),
        ..Default::default()
    })
    .await
    .unwrap();
    assert_eq!(synced.removed, 1);
    assert_eq!(
        ds.file_keys("main").await,
        [
            "out/_SUCCESS",
            "out/_temporary/0/part-2.parquet",
            "out/part-1.csv"
        ]
    );

    let err = import(ImportDatasetRequest {
        include: Some(vec!["out/[".to_string()]),
        ..Default::default()
    })
    .await
    .expect_err("a glob that does not parse is refused");
    assert_eq!(err.error.r#type, "InvalidGlob");
}

/// An imported dataset with `keys` written to storage and synced onto `main`, and
/// the head the sync published.
async fn synced_dataset(pool: PgPool, keys: &[&str]) -> (TestDataset, Option<DatasetSnapshotId>) {
    let ds = TestDataset::imported(pool).await;
    for key in keys {
        ds.write(key, b"x").await;
    }
    let head = sync_import(&ds, None, None).await.unwrap().snapshot_id;
    (ds, head)
}

/// Another writer's commit on `main`, publishing `added` and `modified` over
/// `parent`, in a transaction left open for [`commit_once_waited_on`] to release.
async fn open_commit(
    ds: &TestDataset,
    parent: Option<DatasetSnapshotId>,
    added: &[ManifestEntry],
    modified: &[ManifestEntry],
) -> sqlx::Transaction<'static, sqlx::Postgres> {
    let mut writer = ds.pool.begin().await.unwrap();
    let snapshot = DatasetSnapshotId::from(Uuid::now_v7());
    PostgresBackend::begin_dataset_commit(
        ds.warehouse_id,
        ds.id,
        "main",
        snapshot,
        parent,
        &ds.location,
        None,
        &mut writer,
    )
    .await
    .unwrap();
    PostgresBackend::stage_dataset_files(
        ds.warehouse_id,
        ds.id,
        snapshot,
        added,
        modified,
        &[],
        ConstraintViolationPolicy::Reject,
        &mut writer,
    )
    .await
    .unwrap();
    PostgresBackend::finish_dataset_commit(
        ds.warehouse_id,
        ds.id,
        "main",
        snapshot,
        parent,
        &mut writer,
    )
    .await
    .unwrap();
    writer
}

fn entry(key: &str, physical_path: &str, size: i64) -> ManifestEntry {
    ManifestEntry {
        logical_key: key.to_string(),
        physical_path: physical_path.to_string(),
        etag: None,
        size: Some(size),
        content_type: None,
        checksum: None,
        version_id: None,
        last_modified: None,
    }
}

/// A sync run against `main` while [`open_commit`]'s writer holds the ref.
async fn sync_racing(
    ds: &TestDataset,
    writer: sqlx::Transaction<'static, sqlx::Postgres>,
) -> ImportDatasetResponse {
    let sync = {
        let ds = ds.clone();
        async move { sync_import(&ds, None, None).await }
    };
    commit_once_waited_on(&ds.pool, writer, sync).await.unwrap()
}

/// A branch reset while a sync runs fails the sync, which publishes nothing: its
/// listing was merged against history the branch has left, and carried over
/// it would drop files the listing showed.
#[sqlx::test]
async fn test_a_sync_across_a_reset_publishes_nothing(pool: PgPool) {
    let (ds, first) = synced_dataset(pool.clone(), &["a.txt"]).await;
    for key in ["b.txt", "c.txt"] {
        ds.write(key, b"x").await;
    }
    // `main` holds a and b; the sync below has c to add.
    let second = ds.commit(first, vec![file("b.txt")], &[]).await;

    let mut resetting = pool.begin().await.unwrap();
    PostgresBackend::move_dataset_ref(
        ds.warehouse_id,
        ds.id,
        "main",
        first.unwrap(),
        Some(second),
        false,
        &mut resetting,
    )
    .await
    .unwrap();
    let sync = {
        let ds = ds.clone();
        async move { sync_import(&ds, None, None).await }
    };
    let err = commit_once_waited_on(&pool, resetting, sync)
        .await
        .unwrap_err();
    assert_eq!(err.error.code, StatusCode::CONFLICT, "{err:?}");
    assert_eq!(ds.file_keys("main").await, ["a.txt"]);
}

/// An import reads on through the head it began on though retention expires it
/// once the branch has moved past: the head's rows stay while the import stages on
/// top of it.
#[sqlx::test]
async fn test_an_import_reads_through_a_head_that_expired_meanwhile(pool: PgPool) {
    let (ds, head) = synced_dataset(pool.clone(), &["a.txt"]).await;
    ds.write("b.txt", b"x").await;

    // Held while it lists: the branch moves on, and the head it read expires.
    let gate = CommitGate::install(&pool, "dataset_import_listing", "INSERT").await;
    let importing = tokio::spawn({
        let ds = ds.clone();
        async move { sync_import(&ds, None, None).await }
    });
    gate.wait_for_a_held_commit().await;
    ds.commit(head, vec![file("late.txt")], &[]).await;
    let mut t = ds.begin_write().await;
    let expired = PostgresBackend::expire_dataset_snapshots(
        ds.warehouse_id,
        ds.id,
        &[head.unwrap()],
        Utc::now() + chrono::Duration::days(1),
        t.transaction(),
    )
    .await
    .unwrap();
    t.commit().await.unwrap();
    assert_eq!(expired, 1);
    gate.release().await;

    let synced = importing.await.unwrap().expect("the import finishes");
    assert_eq!(synced.imported, 1, "{synced:?}");
    assert_eq!(ds.file_keys("main").await, ["a.txt", "b.txt", "late.txt"]);
}

/// A file committed while a sync runs survives it. The sync's listing may predate
/// the file, so not finding it there is no evidence it is gone.
#[sqlx::test]
async fn test_a_sync_keeps_a_file_committed_while_it_ran(pool: PgPool) {
    let (ds, head) = synced_dataset(pool, &["a.txt"]).await;
    // Gives the sync below something to publish, so it reaches the pointer move.
    ds.write("b.txt", b"x").await;

    let writer = open_commit(&ds, head, &[entry("late.txt", "late.txt", 1)], &[]).await;
    let outcome = sync_racing(&ds, writer).await;
    assert_eq!((outcome.imported, outcome.removed), (1, 0));
    assert_eq!(ds.file_keys("main").await, ["a.txt", "b.txt", "late.txt"]);
}

/// A sync rebased onto a commit that landed while it ran publishes files its
/// listing never saw, so its check leaves that snapshot unchecked.
#[sqlx::test]
async fn test_a_rebased_sync_does_not_check_its_own_snapshot(pool: PgPool) {
    let (ds, head) = synced_dataset(pool.clone(), &["a.txt"]).await;
    ds.write("b.txt", b"x").await;

    // `late.txt` has no object: the listing could not have shown it.
    let writer = open_commit(&ds, head, &[entry("late.txt", "late.txt", 1)], &[]).await;
    let sync = {
        let ds = ds.clone();
        async move {
            CatalogServer::import_dataset(
                ds.params(),
                ImportDatasetRequest {
                    mode: Some(ImportMode::Sync),
                    check_materialization: Some(true),
                    ..Default::default()
                },
                ds.ctx.clone(),
                random_request_metadata(),
            )
            .await
        }
    };
    let outcome = commit_once_waited_on(&pool, writer, sync).await.unwrap();
    assert_eq!(outcome.imported, 1, "the sync published, rebased");
    assert_eq!(
        outcome.materialization.unwrap().checked_snapshots,
        0,
        "its own snapshot holds the commit's file"
    );
}

/// A file a commit changed while a sync ran keeps that commit's entry, even though
/// the sync's listing did not find the object: the commit is the newer word.
#[sqlx::test]
async fn test_a_sync_leaves_alone_a_file_changed_while_it_ran(pool: PgPool) {
    let (ds, head) = synced_dataset(pool, &["a.txt", "moved.txt"]).await;
    ds.delete("moved.txt").await;
    ds.write("b.txt", b"x").await;

    let writer = open_commit(
        &ds,
        head,
        &[],
        &[entry("moved.txt", "elsewhere/moved.txt", 7)],
    )
    .await;
    let outcome = sync_racing(&ds, writer).await;
    assert_eq!((outcome.imported, outcome.removed), (1, 0));
    let listed = CatalogServer::list_dataset_files(
        ds.ref_params("main"),
        ListDatasetFilesQuery::default(),
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    let moved = listed
        .files
        .iter()
        .find(|f| f.logical_key == "moved.txt")
        .expect("the file the commit changed survives the sync");
    assert_eq!(
        (moved.physical_path.as_str(), moved.size),
        ("elsewhere/moved.txt", Some(7))
    );
    assert_eq!(listed.files.len(), 3);
}

/// When every change a sync staged was to a file a commit changed first, nothing is
/// left to publish: the sync reports the commit's head and leaves no snapshot.
#[sqlx::test]
async fn test_a_sync_superseded_by_a_commit_publishes_nothing(pool: PgPool) {
    let (ds, head) = synced_dataset(pool.clone(), &["a.txt", "moved.txt"]).await;
    ds.delete("moved.txt").await;

    let writer = open_commit(
        &ds,
        head,
        &[],
        &[entry("moved.txt", "elsewhere/moved.txt", 7)],
    )
    .await;
    let outcome = sync_racing(&ds, writer).await;
    let committed: Uuid =
        sqlx::query_scalar("SELECT snapshot_id FROM dataset_ref WHERE name = 'main'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        (outcome.snapshot_id, outcome.imported, outcome.removed),
        (Some(committed.into()), 0, 0)
    );
    assert_ne!(outcome.snapshot_id, head, "the commit moved the branch");
    let staging: i64 =
        sqlx::query_scalar("SELECT count(*) FROM dataset_snapshot WHERE status = 'staging'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        staging, 0,
        "the superseded snapshot must not be left behind"
    );
    assert_eq!(ds.file_keys("main").await, ["a.txt", "moved.txt"]);
}

/// The ref a versioning action names reaches the authorizer, so a policy can decide
/// per ref. Here commits and fast-forwards to `main`, a tag named `release` and a
/// purging drop are refused; the same actions on anything else go through.
#[sqlx::test]
async fn test_an_authorizer_can_decide_per_ref(pool: PgPool) {
    let authorizer = HidingAuthorizer::new();
    let ds = TestNamespace::with_authorizer(pool, authorizer.clone())
        .await
        .create_dataset(DATASET, DatasetOwnership::Imported, None)
        .await;
    let commit_to = |branch: &str, parent: Option<DatasetSnapshotId>| {
        CatalogServer::commit_dataset(
            ds.ref_params(branch),
            CommitDatasetRequest {
                parent_snapshot_id: parent,
                added: vec![file(&format!("{branch}.jpg"))],
                removed: vec![],
                summary: None,
                on_constraint_violation: None,
            },
            ds.ctx.clone(),
            random_request_metadata(),
        )
    };
    let create_ref = |name: &str, typ: DatasetRefType| {
        CatalogServer::create_dataset_ref(
            ds.params(),
            CreateDatasetRefRequest {
                name: name.to_string(),
                typ,
                source: DatasetRefSource::Ref {
                    name: "main".to_string(),
                },
            },
            ds.ctx.clone(),
            random_request_metadata(),
        )
    };
    let forbidden = |result: Result<()>, what: &str| {
        let err = result.expect_err(what);
        assert_eq!(err.error.code, StatusCode::FORBIDDEN, "{what}: {err:?}");
    };

    let base = commit_to("main", None).await.unwrap().snapshot_id;
    authorizer.block_action(r#"dataset:Commit { target_refs: {"main"} }"#);
    authorizer.block_action(r#"dataset:Promote { target_refs: {"main"} }"#);
    authorizer.block_action(r#"dataset:ManageRefs { target_refs: {"release"} }"#);
    authorizer.block_action("dataset:Drop { force: false, purge: true }");

    forbidden(
        commit_to("main", Some(base)).await.map(|_| ()),
        "a commit to main",
    );
    forbidden(
        CatalogServer::import_dataset(
            ds.params(),
            ImportDatasetRequest::default(),
            ds.ctx.clone(),
            random_request_metadata(),
        )
        .await
        .map(|_| ()),
        "an import into main",
    );
    forbidden(
        create_ref("release", DatasetRefType::Tag).await.map(|_| ()),
        "a tag named release",
    );
    create_ref("v1", DatasetRefType::Tag).await.unwrap();
    create_ref("dev", DatasetRefType::Branch).await.unwrap();
    let ahead = commit_to("dev", Some(base)).await.unwrap().snapshot_id;
    forbidden(
        CatalogServer::move_dataset_ref(
            ds.ref_params("main"),
            MoveDatasetRefRequest {
                snapshot_id: ahead,
                expected_snapshot_id: Some(base),
                fast_forward: true,
            },
            ds.ctx.clone(),
            random_request_metadata(),
        )
        .await
        .map(|_| ()),
        "a fast-forward of main",
    );
    forbidden(
        CatalogServer::drop_dataset(
            ds.params(),
            DropParams {
                purge_requested: true,
                force: false,
            },
            ds.ctx.clone(),
            random_request_metadata(),
        )
        .await,
        "a purging drop",
    );
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
    .expect("a drop that does not purge");
}

/// A sync merges its listing against the manifest across many pages on both sides,
/// in byte order: keys whose byte order differs from a dictionary's — case,
/// punctuation beside `/`, non-ASCII — must meet their recorded twins, or an
/// unchanged prefix would report every such file as removed and re-added.
#[sqlx::test]
async fn test_a_sync_merges_across_pages_in_byte_order(pool: PgPool) {
    const FILES: usize = 2_300;
    let ds = TestDataset::imported(pool.clone()).await;
    let key = |i: usize| {
        let lead = ["A/", "a/", "a-", "a.", "Z_", "\u{e9}/", "b"][i % 7];
        format!("{lead}{i:05}.jpg")
    };
    for i in 0..FILES {
        ds.write(&key(i), b"x").await;
    }
    let first = sync_import(&ds, None, None).await.unwrap();
    assert_eq!(first.imported, i64::try_from(FILES).unwrap());

    let unchanged = sync_import(&ds, None, None).await.unwrap();
    assert_eq!(
        (unchanged.imported, unchanged.modified, unchanged.removed),
        (0, 0, 0),
        "an unchanged prefix must merge to no change"
    );
    assert_eq!(unchanged.snapshot_id, first.snapshot_id);

    // Changes spread across the pages of both sides.
    for i in [3, 1_401, 2_299] {
        ds.write(&key(i), b"longer").await;
    }
    for i in [0, 999, 1_000, 2_298] {
        ds.delete(&key(i)).await;
    }
    for i in FILES..FILES + 5 {
        ds.write(&key(i), b"x").await;
    }
    let changed = sync_import(&ds, None, None).await.unwrap();
    assert_eq!(
        (changed.imported, changed.modified, changed.removed),
        (5, 3, 4)
    );
    let mut expected: Vec<String> = (0..FILES + 5)
        .filter(|i| ![0, 999, 1_000, 2_298].contains(i))
        .map(key)
        .collect();
    expected.sort();
    let mut listed = Vec::new();
    let mut page_token = None;
    loop {
        let page = CatalogServer::list_dataset_files(
            ds.ref_params("main"),
            ListDatasetFilesQuery {
                page_token,
                page_size: Some(1_000),
                ..Default::default()
            },
            ds.ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();
        listed.extend(page.files.into_iter().map(|f| f.logical_key));
        match page.next_page_token {
            Some(token) => page_token = Some(token),
            None => break,
        }
    }
    assert_eq!(listed, expected, "files are listed in byte order");
}

/// An import's spool reads back in byte order whatever order it was written in,
/// which is what frees the merge from the order storage lists in. The key columns
/// carry that collation themselves, so the database's default cannot change it.
#[sqlx::test]
async fn test_the_import_spool_reads_back_in_byte_order(pool: PgPool) {
    let ds = TestDataset::managed(pool.clone()).await;

    let collations: Vec<(String, Option<String>)> = sqlx::query_as(
        "SELECT table_name::text, collation_name::text FROM information_schema.columns
         WHERE column_name = 'logical_key'
           AND table_name IN ('dataset_manifest_entry', 'dataset_import_listing',
                              'dataset_degraded_file')
         ORDER BY table_name",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        collations,
        [
            ("dataset_degraded_file".to_string(), Some("C".to_string())),
            ("dataset_import_listing".to_string(), Some("C".to_string())),
            ("dataset_manifest_entry".to_string(), Some("C".to_string())),
        ]
    );

    let mut t = pool.begin().await.unwrap();
    // With index scans off, the order must come from the query itself, not from
    // a plan that happens to walk the primary key.
    for setting in [
        "SET LOCAL enable_indexscan = off",
        "SET LOCAL enable_indexonlyscan = off",
        "SET LOCAL enable_bitmapscan = off",
    ] {
        sqlx::query(setting).execute(&mut *t).await.unwrap();
    }
    let staging = DatasetSnapshotId::from(Uuid::now_v7());
    PostgresBackend::begin_dataset_commit(
        ds.warehouse_id,
        ds.id,
        "main",
        staging,
        None,
        &ds.location,
        None,
        &mut t,
    )
    .await
    .unwrap();
    // Written in an order no sort would produce, as a depth-first listing might.
    let written = ["b", "a/z", "\u{e9}", "a.b", "B", "a/a", "a-b", "a"];
    let objects: Vec<ListedObject> = written
        .iter()
        .map(|k| ListedObject {
            logical_key: (*k).to_string(),
            physical_path: (*k).to_string(),
            size: Some(1),
            last_modified: None,
            etag: None,
            version_id: None,
            referenced: false,
        })
        .collect();
    PostgresBackend::spool_import_listing(ds.warehouse_id, staging, &objects, &mut t)
        .await
        .unwrap();
    let mut read: Vec<String> = Vec::new();
    loop {
        let page = PostgresBackend::read_import_listing(
            ds.warehouse_id,
            staging,
            read.last().map(String::as_str),
            3,
            &mut t,
        )
        .await
        .unwrap();
        if page.is_empty() {
            break;
        }
        read.extend(page.into_iter().map(|o| o.logical_key));
    }
    let mut expected: Vec<String> = written.iter().map(|k| (*k).to_string()).collect();
    expected.sort();
    assert_eq!(read, expected);
}
