//! Materialization: whether the objects behind a snapshot's files are still there.
//!
//! An import run with `check-materialization` compares every snapshot a ref points
//! at against its listing. The objects of an imported dataset belong to whoever
//! writes the bucket, so they can vanish under a tag; the check is how that shows.
use http::StatusCode;
use lakekeeper::{
    api::{
        Result,
        data::v1::datasets::{
            CommitFile, CreateDatasetRefRequest, DatasetRefSource, DatasetService as _,
            ImportDatasetRequest, ImportDatasetResponse, ImportMode, ListDatasetFilesQuery,
            MaterializationStatus, SnapshotMaterializationQuery, SnapshotMaterializationResponse,
        },
    },
    server::CatalogServer,
    service::{
        CatalogDatasetOps, ConstraintViolationPolicy, DatasetRefType, DatasetSnapshotId,
        DegradedFileProblem, ManifestEntry, Transaction,
    },
};
use lakekeeper_integration_tests::{
    CommitFileExt as _, CommitGate, TestDataset, file, random_request_metadata,
};
use lakekeeper_storage_postgres::PostgresBackend;
use sqlx::PgPool;
use uuid::Uuid;

async fn import(ds: &TestDataset, mode: ImportMode, check: bool) -> ImportDatasetResponse {
    CatalogServer::import_dataset(
        ds.params(),
        ImportDatasetRequest {
            mode: Some(mode),
            check_materialization: Some(check),
            ..Default::default()
        },
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap()
}

async fn tag(ds: &TestDataset, name: &str) {
    CatalogServer::create_dataset_ref(
        ds.params(),
        CreateDatasetRefRequest {
            name: name.to_string(),
            typ: DatasetRefType::Tag,
            source: DatasetRefSource::Ref {
                name: "main".to_string(),
            },
        },
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
}

async fn materialization(
    ds: &TestDataset,
    snapshot: DatasetSnapshotId,
    query: SnapshotMaterializationQuery,
) -> Result<SnapshotMaterializationResponse> {
    CatalogServer::get_dataset_snapshot_materialization(
        ds.snapshot_params(snapshot),
        query,
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
}

async fn missing_keys(ds: &TestDataset, snapshot: DatasetSnapshotId) -> Vec<String> {
    degraded_keys(ds, snapshot, DegradedFileProblem::Missing).await
}

async fn degraded_keys(
    ds: &TestDataset,
    snapshot: DatasetSnapshotId,
    problem: DegradedFileProblem,
) -> Vec<String> {
    materialization(ds, snapshot, SnapshotMaterializationQuery::default())
        .await
        .unwrap()
        .files
        .into_iter()
        .filter(|f| f.problem == problem)
        .map(|f| f.logical_key)
        .collect()
}

#[sqlx::test]
async fn test_a_checked_import_vouches_for_every_ref(pool: PgPool) {
    let ds = TestDataset::imported(pool).await;
    ds.write("a.jpg", b"x").await;
    ds.write("b.jpg", b"x").await;

    let imported = import(&ds, ImportMode::AddOnly, true).await;

    let report = imported.materialization.expect("the check ran");
    assert_eq!(
        (report.checked_snapshots, report.degraded_snapshots),
        (1, 0)
    );
    let head = ds.head("main").await.unwrap();
    let checked = materialization(&ds, head, SnapshotMaterializationQuery::default())
        .await
        .unwrap();
    assert_eq!(checked.status, MaterializationStatus::FullyMaterialized);
    assert_eq!(checked.missing_files, Some(0));
    // The listing of refs carries the badge.
    let listed =
        CatalogServer::list_dataset_refs(ds.params(), ds.ctx.clone(), random_request_metadata())
            .await
            .unwrap();
    let main = listed.refs.iter().find(|r| r.name == "main").unwrap();
    assert_eq!(
        main.materialization.as_ref().map(|m| m.status),
        Some(MaterializationStatus::FullyMaterialized)
    );
}

#[sqlx::test]
async fn test_an_object_gone_out_of_band_degrades_every_snapshot_holding_it(pool: PgPool) {
    let ds = TestDataset::imported(pool).await;
    ds.write("a.jpg", b"x").await;
    ds.write("b.jpg", b"x").await;
    import(&ds, ImportMode::AddOnly, false).await;
    tag(&ds, "v1").await;
    let v1 = ds.head("v1").await.unwrap();

    // Nothing to register, so the import publishes nothing; the check still runs.
    ds.delete("b.jpg").await;
    let unchanged = import(&ds, ImportMode::AddOnly, true).await;
    assert_eq!(unchanged.imported, 0);
    assert_eq!(unchanged.materialization.unwrap().degraded_snapshots, 1);
    assert_eq!(missing_keys(&ds, v1).await, ["b.jpg"]);

    // A sync drops the key from the branch, so the new head is whole again; the
    // tag keeps the file, and stays degraded.
    let synced = import(&ds, ImportMode::Sync, true).await;
    let report = synced.materialization.unwrap();
    assert_eq!(
        (report.checked_snapshots, report.degraded_snapshots),
        (2, 1)
    );
    let head = ds.head("main").await.unwrap();
    assert_ne!(head, v1);
    assert!(missing_keys(&ds, head).await.is_empty());
    let tagged = materialization(&ds, v1, SnapshotMaterializationQuery::default())
        .await
        .unwrap();
    assert_eq!(tagged.status, MaterializationStatus::PartiallyDegraded);
    assert_eq!(tagged.missing_files, Some(1));
}

/// An object written again out of band is not what its snapshots recorded: the
/// check reports the file as changed, beside the missing one, and the ref's badge
/// counts both. A sync records the new bytes on the branch; the tag keeps the old
/// entry, and stays degraded.
#[sqlx::test]
async fn test_an_object_written_again_degrades_the_snapshots_that_recorded_it(pool: PgPool) {
    let ds = TestDataset::imported(pool).await;
    ds.write("a.jpg", b"x").await;
    ds.write("b.jpg", b"x").await;
    import(&ds, ImportMode::AddOnly, false).await;
    tag(&ds, "v1").await;
    let v1 = ds.head("v1").await.unwrap();

    ds.write("a.jpg", b"written again").await;
    ds.delete("b.jpg").await;
    let checked = import(&ds, ImportMode::AddOnly, true).await;
    assert_eq!(checked.materialization.unwrap().degraded_snapshots, 1);
    assert_eq!(
        degraded_keys(&ds, v1, DegradedFileProblem::Changed).await,
        ["a.jpg"]
    );
    assert_eq!(missing_keys(&ds, v1).await, ["b.jpg"]);
    let tagged = materialization(&ds, v1, SnapshotMaterializationQuery::default())
        .await
        .unwrap();
    assert_eq!(tagged.status, MaterializationStatus::PartiallyDegraded);
    assert_eq!(
        (tagged.missing_files, tagged.changed_files),
        (Some(1), Some(1))
    );
    let badge =
        CatalogServer::list_dataset_refs(ds.params(), ds.ctx.clone(), random_request_metadata())
            .await
            .unwrap()
            .refs
            .into_iter()
            .find(|r| r.name == "v1")
            .and_then(|r| r.materialization)
            .unwrap();
    assert_eq!((badge.missing_files, badge.changed_files), (1, 1));

    import(&ds, ImportMode::Sync, true).await;
    let head = ds.head("main").await.unwrap();
    assert_ne!(head, v1);
    assert!(
        degraded_keys(&ds, head, DegradedFileProblem::Changed)
            .await
            .is_empty()
    );
    assert_eq!(
        degraded_keys(&ds, v1, DegradedFileProblem::Changed).await,
        ["a.jpg"]
    );
}

#[sqlx::test]
async fn test_a_committed_file_is_judged_by_where_its_bytes_are(pool: PgPool) {
    let ds = TestDataset::imported(pool).await;
    ds.write("raw/present.bin", b"x").await;
    ds.commit(
        None,
        vec![
            file("train/kept.bin").at("raw/present.bin"),
            file("train/lost.bin").at("raw/absent.bin"),
            // The in-memory store keeps no versions: no reader reads this one,
            // so the file is judged by its key.
            file("train/pinned.bin")
                .at("raw/versioned.bin")
                .version_id("v7"),
        ],
        &[],
    )
    .await;

    import(&ds, ImportMode::AddOnly, true).await;

    let head = ds.head("main").await.unwrap();
    let missing = materialization(&ds, head, SnapshotMaterializationQuery::default())
        .await
        .unwrap()
        .files;
    assert_eq!(missing.len(), 2);
    assert_eq!(missing[0].logical_key, "train/lost.bin");
    assert_eq!(missing[0].physical_path, "raw/absent.bin");
    assert_eq!(missing[0].problem, DegradedFileProblem::Missing);
    assert_eq!(missing[1].logical_key, "train/pinned.bin");
}

/// A snapshot committed after an import's listing began is left unchecked: the
/// listing may have passed a key before the file under it was written.
#[sqlx::test]
async fn test_a_snapshot_newer_than_the_listing_is_left_unchecked(pool: PgPool) {
    let ds = TestDataset::imported(pool).await;
    ds.write("a.jpg", b"x").await;
    import(&ds, ImportMode::AddOnly, false).await;
    tag(&ds, "v1").await;
    let v1 = ds.head("v1").await.unwrap();
    let newer = ds
        .commit(Some(v1), vec![file("b.jpg").at("b.jpg")], &[])
        .await;
    // As if committed while the next import was listing.
    sqlx::query(
        "UPDATE dataset_snapshot SET created_at = now() + interval '1 hour' WHERE snapshot_id = $1",
    )
    .bind(*newer)
    .execute(&ds.pool)
    .await
    .unwrap();

    let checked = import(&ds, ImportMode::AddOnly, true).await;
    let report = checked.materialization.unwrap();
    assert_eq!(
        (report.checked_snapshots, report.degraded_snapshots),
        (1, 0),
        "only the tag is judged"
    );
    let unchecked = materialization(&ds, newer, SnapshotMaterializationQuery::default())
        .await
        .unwrap();
    assert_eq!(unchecked.status, MaterializationStatus::Unchecked);
}

/// A snapshot published after an import's listing began is left unchecked, though
/// it was opened before: the import that staged it may have listed a key the
/// checking import's listing had already passed.
#[sqlx::test]
async fn test_a_snapshot_published_during_the_listing_is_left_unchecked(pool: PgPool) {
    let ds = TestDataset::imported(pool).await;
    ds.write("a.jpg", b"x").await;
    import(&ds, ImportMode::AddOnly, false).await;
    let head = ds.head("main").await.unwrap();
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

    // Another import opens a snapshot on `dev`, with a file written after the
    // checking import's listing passed its key, and publishes only later.
    let late = DatasetSnapshotId::from(Uuid::now_v7());
    let mut t = ds.begin_write().await;
    PostgresBackend::begin_dataset_commit(
        ds.warehouse_id,
        ds.id,
        "dev",
        late,
        Some(head),
        &ds.location,
        None,
        t.transaction(),
    )
    .await
    .unwrap();
    PostgresBackend::stage_dataset_files(
        ds.warehouse_id,
        ds.id,
        late,
        &[ManifestEntry {
            logical_key: "late.jpg".to_string(),
            physical_path: "late.jpg".to_string(),
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

    // Hold the checking import in its listing while the other one publishes.
    let gate = CommitGate::install(&ds.pool, "dataset_import_listing", "INSERT").await;
    let checking = tokio::spawn({
        let (params, ctx) = (ds.params(), ds.ctx.clone());
        async move {
            CatalogServer::import_dataset(
                params,
                ImportDatasetRequest {
                    mode: Some(ImportMode::AddOnly),
                    check_materialization: Some(true),
                    ..Default::default()
                },
                ctx,
                random_request_metadata(),
            )
            .await
            .unwrap()
        }
    });
    gate.wait_for_a_held_commit().await;
    let mut t = ds.begin_write().await;
    PostgresBackend::finish_dataset_commit(
        ds.warehouse_id,
        ds.id,
        "dev",
        late,
        Some(head),
        t.transaction(),
    )
    .await
    .unwrap();
    t.commit().await.unwrap();
    gate.release().await;
    checking.await.unwrap();

    let unchecked = materialization(&ds, late, SnapshotMaterializationQuery::default())
        .await
        .unwrap();
    assert_eq!(
        unchecked.status,
        MaterializationStatus::Unchecked,
        "{unchecked:?}"
    );
}

/// A file is judged by the key a listing reports for its bytes: one whose bytes the
/// scan leaves out is not judged, whatever the file is called.
#[sqlx::test]
async fn test_a_file_is_judged_by_its_listed_key(pool: PgPool) {
    let ds = TestDataset::imported(pool).await;
    ds.write("blobs/x", b"x").await;
    ds.commit(None, vec![file("a.jpg").at("blobs/x")], &[])
        .await;

    let checked = CatalogServer::import_dataset(
        ds.params(),
        ImportDatasetRequest {
            exclude: Some(vec!["blobs/**".to_string()]),
            check_materialization: Some(true),
            ..Default::default()
        },
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    assert_eq!(checked.materialization.unwrap().degraded_snapshots, 0);
    let head = ds.head("main").await.unwrap();
    assert!(missing_keys(&ds, head).await.is_empty());
}

/// A sync that re-records an object whose bytes are unchanged keeps what its
/// producer said of it: the checksum, and the content type.
#[sqlx::test]
async fn test_a_resync_keeps_what_the_producer_recorded(pool: PgPool) {
    let ds = TestDataset::imported(pool).await;
    ds.write("a.bin", b"x").await;
    ds.commit(
        None,
        vec![CommitFile {
            checksum: Some(
                "sha256:2d711642b726b04401627ca9fbac32f5c8530fb1903cc4db02258717921a4881"
                    .to_string(),
            ),
            ..file("a.bin")
                .at("a.bin")
                .content_type("application/x-custom")
        }],
        &[],
    )
    .await;

    // The commit recorded no modification time, so the listing's counts as new.
    let synced = import(&ds, ImportMode::Sync, false).await;
    assert_eq!(synced.modified, 1);
    let files = CatalogServer::list_dataset_files(
        ds.ref_params("main"),
        ListDatasetFilesQuery::default(),
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap()
    .files;
    assert_eq!(
        files[0].content_type.as_deref(),
        Some("application/x-custom")
    );
    assert!(files[0].checksum.is_some(), "{:?}", files[0]);
}

#[sqlx::test]
async fn test_missing_files_page_in_key_order(pool: PgPool) {
    let ds = TestDataset::imported(pool).await;
    for i in 0..5 {
        ds.write(&format!("f{i}"), b"x").await;
    }
    import(&ds, ImportMode::AddOnly, false).await;
    for i in 0..5 {
        ds.delete(&format!("f{i}")).await;
    }
    import(&ds, ImportMode::AddOnly, true).await;
    let head = ds.head("main").await.unwrap();

    let mut keys = Vec::new();
    let mut token = None;
    loop {
        let page = materialization(
            &ds,
            head,
            SnapshotMaterializationQuery {
                page_token: token,
                page_size: Some(2),
            },
        )
        .await
        .unwrap();
        assert_eq!(page.missing_files, Some(5));
        assert!(page.files.len() <= 2);
        keys.extend(page.files.into_iter().map(|f| f.logical_key));
        token = page.next_page_token;
        if token.is_none() {
            break;
        }
    }
    assert_eq!(keys, ["f0", "f1", "f2", "f3", "f4"]);
}

#[sqlx::test]
async fn test_an_unchecked_snapshot_says_so(pool: PgPool) {
    let ds = TestDataset::imported(pool).await;
    ds.write("a.jpg", b"x").await;
    import(&ds, ImportMode::AddOnly, false).await;

    let unchecked = materialization(
        &ds,
        ds.head("main").await.unwrap(),
        SnapshotMaterializationQuery::default(),
    )
    .await
    .unwrap();
    assert_eq!(unchecked.status, MaterializationStatus::Unchecked);
    assert_eq!(unchecked.missing_files, None);

    let err = materialization(
        &ds,
        DatasetSnapshotId::from(Uuid::now_v7()),
        SnapshotMaterializationQuery::default(),
    )
    .await
    .expect_err("no such snapshot");
    assert_eq!(err.error.code, StatusCode::NOT_FOUND, "got: {err:?}");
}

#[sqlx::test]
async fn test_a_narrowed_scan_cannot_vouch_for_a_snapshot(pool: PgPool) {
    let ds = TestDataset::imported(pool).await;

    let err = CatalogServer::import_dataset(
        ds.params(),
        ImportDatasetRequest {
            sub_prefix: Some("train".to_string()),
            check_materialization: Some(true),
            ..Default::default()
        },
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .expect_err("a narrowed scan cannot check");
    assert_eq!(err.error.r#type, "InvalidMaterializationCheck");
}

#[sqlx::test]
async fn test_a_published_import_leaves_no_spool_behind(pool: PgPool) {
    let ds = TestDataset::imported(pool).await;
    ds.write("a.jpg", b"x").await;

    import(&ds, ImportMode::AddOnly, true).await;
    ds.write("b.jpg", b"x").await;
    import(&ds, ImportMode::AddOnly, false).await;

    let spooled: i64 = sqlx::query_scalar("SELECT count(*) FROM dataset_import_listing")
        .fetch_one(&ds.pool)
        .await
        .unwrap();
    assert_eq!(spooled, 0);
}

/// A scan cut short by `max-files` saw too little to vouch for anything: it checks
/// nothing, and leaves every snapshot as it was.
#[sqlx::test]
async fn test_a_truncated_scan_checks_nothing(pool: PgPool) {
    let ds = TestDataset::imported(pool).await;
    for key in ["a", "b", "c"] {
        ds.write(key, b"x").await;
    }
    import(&ds, ImportMode::AddOnly, false).await;
    let head = ds.head("main").await.unwrap();

    let truncated = CatalogServer::import_dataset(
        ds.params(),
        ImportDatasetRequest {
            max_files: Some(1),
            check_materialization: Some(true),
            ..Default::default()
        },
        ds.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    assert!(truncated.truncated);
    assert!(truncated.materialization.is_none());
    let status = materialization(&ds, head, SnapshotMaterializationQuery::default())
        .await
        .unwrap()
        .status;
    assert_eq!(status, MaterializationStatus::Unchecked);
}
