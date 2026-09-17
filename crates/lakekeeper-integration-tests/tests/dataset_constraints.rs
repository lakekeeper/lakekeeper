//! Constraints under `reject` and `skip`.
//!
//! A commit refuses a violating file by default; an import skips it by default,
//! since a prefix written by other tools carries stray files no dataset wants.
//! Either way nothing is silent: a refusal names the files, a skip reports them.
use http::StatusCode;
use lakekeeper::{
    api::{
        Result,
        data::v1::datasets::{
            CommitDatasetRequest, CommitFile, DatasetService as _, ImportDatasetRequest,
            ImportDatasetResponse, ImportMode, UpdateDatasetSettingsRequest,
        },
    },
    server::CatalogServer,
    service::{ConstraintViolationPolicy, DatasetConstraints, DatasetOwnership},
};
use lakekeeper_integration_tests::{
    CommitFileExt as _, DATASET, TestDataset, TestNamespace, file, random_request_metadata,
};
use sqlx::PgPool;

/// A dataset holding `constraints`.
async fn constrained_dataset(
    pool: PgPool,
    ownership: DatasetOwnership,
    constraints: DatasetConstraints,
) -> TestDataset {
    TestNamespace::new(pool)
        .await
        .create_dataset(DATASET, ownership, Some(constraints))
        .await
}

fn images_only() -> DatasetConstraints {
    DatasetConstraints {
        allowed_content_types: Some(vec!["image/jpeg".to_string()]),
        max_file_size: None,
    }
}

async fn import(
    ds: &TestDataset,
    mode: ImportMode,
    on_constraint_violation: Option<ConstraintViolationPolicy>,
) -> Result<ImportDatasetResponse> {
    CatalogServer::import_dataset(
        ds.params(),
        ImportDatasetRequest {
            mode: Some(mode),
            on_constraint_violation,
            ..Default::default()
        },
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
}

/// Commits `added` on top of `main`'s head.
async fn commit(ds: &TestDataset, added: Vec<CommitFile>) -> Result<()> {
    ds.try_commit("main", ds.head("main").await, added, &[])
        .await
        .map(|_| ())
}

async fn update_constraints(
    ds: &TestDataset,
    constraints: Option<DatasetConstraints>,
) -> Result<Option<DatasetConstraints>> {
    CatalogServer::update_dataset_settings(
        ds.params(),
        UpdateDatasetSettingsRequest {
            constraints,
            retention: None,
        },
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .map(|r| r.dataset.constraints)
}

#[sqlx::test]
async fn test_a_skip_commit_records_what_conforms_and_reports_the_rest(pool: PgPool) {
    let ds = constrained_dataset(pool, DatasetOwnership::Managed, images_only()).await;

    let committed = CatalogServer::commit_dataset(
        ds.ref_params("main"),
        CommitDatasetRequest {
            parent_snapshot_id: None,
            added: vec![
                file("a.jpg").content_type("image/jpeg"),
                file("notes.pdf").content_type("application/pdf"),
                file("b.jpg").content_type("image/jpeg"),
            ],
            removed: vec![],
            summary: None,
            on_constraint_violation: Some(ConstraintViolationPolicy::Skip),
        },
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .expect("a skip commit succeeds");

    assert_eq!(ds.file_keys("main").await, ["a.jpg", "b.jpg"]);
    assert_eq!(committed.skipped.len(), 1);
    assert_eq!(committed.skipped[0].logical_key, "notes.pdf");
    assert!(
        committed.skipped[0].reason.contains("application/pdf"),
        "{:?}",
        committed.skipped[0]
    );
}

#[sqlx::test]
async fn test_an_import_skips_what_constraints_refuse_by_default(pool: PgPool) {
    let ds = constrained_dataset(pool, DatasetOwnership::Imported, images_only()).await;
    for key in ["a.jpg", "b.jpg", "notes.txt"] {
        ds.write(key, b"x").await;
    }

    let imported = import(&ds, ImportMode::AddOnly, None)
        .await
        .expect("a stray file does not stop the import");

    assert_eq!(imported.imported, 2);
    assert_eq!(imported.skipped, 1);
    assert_eq!(imported.skipped_files[0].logical_key, "notes.txt");
    assert_eq!(ds.file_keys("main").await, ["a.jpg", "b.jpg"]);
}

#[sqlx::test]
async fn test_an_import_asked_to_reject_refuses_and_publishes_nothing(pool: PgPool) {
    let ds = constrained_dataset(pool, DatasetOwnership::Imported, images_only()).await;
    for key in ["a.jpg", "notes.txt"] {
        ds.write(key, b"x").await;
    }

    let err = import(
        &ds,
        ImportMode::AddOnly,
        Some(ConstraintViolationPolicy::Reject),
    )
    .await
    .expect_err("reject refuses the import");

    assert_eq!(err.error.code, StatusCode::BAD_REQUEST, "got: {err:?}");
    assert_eq!(err.error.r#type, "DatasetConstraintViolation");
    assert!(ds.file_keys("main").await.is_empty());
}

/// A key whose new bytes are refused leaves the dataset. Kept, its entry would
/// describe bytes storage has replaced.
#[sqlx::test]
async fn test_a_sync_removes_a_file_whose_new_bytes_are_refused(pool: PgPool) {
    let ds = constrained_dataset(
        pool,
        DatasetOwnership::Imported,
        DatasetConstraints {
            allowed_content_types: None,
            max_file_size: Some(4),
        },
    )
    .await;
    ds.write("a.bin", b"x").await;
    ds.write("b.bin", b"x").await;
    import(&ds, ImportMode::Sync, None).await.unwrap();

    ds.write("a.bin", b"far too large").await;
    let synced = import(&ds, ImportMode::Sync, None)
        .await
        .expect("the sync succeeds");

    assert_eq!(synced.modified, 0);
    assert_eq!(synced.removed, 1);
    assert_eq!(synced.skipped, 1);
    assert_eq!(synced.skipped_files[0].logical_key, "a.bin");
    assert_eq!(ds.file_keys("main").await, ["b.bin"]);
}

#[sqlx::test]
async fn test_an_import_names_a_hundred_skipped_objects_and_counts_the_rest(pool: PgPool) {
    let ds = constrained_dataset(pool, DatasetOwnership::Imported, images_only()).await;
    for i in 0..150 {
        ds.write(&format!("logs/{i:03}.txt"), b"x").await;
    }

    let imported = import(&ds, ImportMode::AddOnly, None).await.unwrap();

    assert_eq!(imported.imported, 0);
    assert_eq!(imported.skipped, 150);
    assert_eq!(imported.skipped_files.len(), 100);
    // An import that registered nothing publishes nothing.
    assert_eq!(imported.snapshot_id, None);
    assert!(ds.file_keys("main").await.is_empty());
}

#[sqlx::test]
async fn test_updated_constraints_bound_the_next_commit_only(pool: PgPool) {
    let ds = constrained_dataset(
        pool,
        DatasetOwnership::Managed,
        DatasetConstraints::default(),
    )
    .await;
    commit(&ds, vec![file("notes.pdf").content_type("application/pdf")])
        .await
        .expect("no constraints yet");

    let updated = update_constraints(&ds, Some(images_only()))
        .await
        .expect("the update succeeds");
    assert_eq!(updated, Some(images_only()));

    let err = commit(&ds, vec![file("more.pdf").content_type("application/pdf")])
        .await
        .expect_err("the new constraints bound the next commit");
    assert_eq!(err.error.r#type, "DatasetConstraintViolation");
    // What was committed before stays: constraints are not re-checked.
    assert_eq!(ds.file_keys("main").await, ["notes.pdf"]);
}

#[sqlx::test]
async fn test_empty_constraints_lift_them(pool: PgPool) {
    let ds = constrained_dataset(pool, DatasetOwnership::Managed, images_only()).await;

    let updated = update_constraints(&ds, Some(DatasetConstraints::default()))
        .await
        .unwrap();

    assert_eq!(updated, None);
    commit(&ds, vec![file("notes.pdf").content_type("application/pdf")])
        .await
        .expect("nothing bounds the commit any more");
}

#[sqlx::test]
async fn test_a_settings_update_leaves_what_it_does_not_name(pool: PgPool) {
    let ds = constrained_dataset(pool, DatasetOwnership::Managed, images_only()).await;

    let updated = update_constraints(&ds, None).await.unwrap();

    assert_eq!(updated, Some(images_only()));
}

#[sqlx::test]
async fn test_constraints_no_file_could_meet_are_refused(pool: PgPool) {
    let ds = constrained_dataset(pool, DatasetOwnership::Managed, images_only()).await;

    for (unmeetable, what) in [
        (
            DatasetConstraints {
                allowed_content_types: None,
                max_file_size: Some(0),
            },
            "a zero size limit",
        ),
        (
            DatasetConstraints {
                allowed_content_types: Some(vec![]),
                max_file_size: None,
            },
            "an empty list of content types",
        ),
    ] {
        let err = update_constraints(&ds, Some(unmeetable))
            .await
            .expect_err(what);
        assert_eq!(err.error.code, StatusCode::BAD_REQUEST, "{what}: {err:?}");
        assert_eq!(err.error.r#type, "InvalidConstraints", "{what}");
    }
}
