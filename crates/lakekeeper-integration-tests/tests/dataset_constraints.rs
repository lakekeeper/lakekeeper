//! Constraints under `reject` and `skip`.
//!
//! A commit refuses a violating file by default; an import skips it by default,
//! since a prefix written by other tools carries stray files no dataset wants.
//! Either way nothing is silent: a refusal names the files, a skip reports them.
use http::StatusCode;
use iceberg::NamespaceIdent;
use lakekeeper::{
    api::{
        ApiContext,
        data::v1::datasets::{
            CommitDatasetRequest, CommitFile, CreateDatasetRequest, DatasetParameters,
            DatasetRefParameters, DatasetService as _, ImportDatasetRequest, ImportDatasetResponse,
            ImportMode, ListDatasetFilesQuery, UpdateDatasetSettingsRequest,
        },
        iceberg::{types::Prefix, v1::namespace::NamespaceParameters},
        management::v1::warehouse::TabularDeleteProfile,
    },
    server::CatalogServer,
    service::{ConstraintViolationPolicy, DatasetConstraints, State, authz::AllowAllAuthorizer},
};
use lakekeeper_integration_tests::{create_ns, memory_io_profile, random_request_metadata, setup};
use lakekeeper_io::LakekeeperStorage as _;
use lakekeeper_storage_postgres::{PostgresBackend, SecretsState};
use sqlx::PgPool;
use uuid::Uuid;

type TestApiContext = ApiContext<State<AllowAllAuthorizer, PostgresBackend, SecretsState>>;

const DS: &str = "images";

/// A dataset holding `constraints`, and its location.
async fn constrained_dataset(
    pool: PgPool,
    constraints: DatasetConstraints,
) -> (TestApiContext, String, String, String) {
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
    let created = CatalogServer::create_dataset(
        NamespaceParameters {
            prefix: Some(Prefix(prefix.clone())),
            namespace: NamespaceIdent::new(ns.clone()),
        },
        CreateDatasetRequest {
            name: DS.to_string(),
            location: None,
            constraints: Some(constraints),
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    (ctx, prefix, ns, created.dataset.location)
}

fn images_only() -> DatasetConstraints {
    DatasetConstraints {
        allowed_content_types: Some(vec!["image/jpeg".to_string()]),
        max_file_size: None,
    }
}

fn ds_params(prefix: &str, ns: &str) -> DatasetParameters {
    DatasetParameters {
        prefix: Some(Prefix(prefix.to_string())),
        namespace: NamespaceIdent::new(ns.to_string()),
        dataset_name: DS.to_string(),
    }
}

fn main_params(prefix: &str, ns: &str) -> DatasetRefParameters {
    DatasetRefParameters {
        prefix: Some(Prefix(prefix.to_string())),
        namespace: NamespaceIdent::new(ns.to_string()),
        dataset_name: DS.to_string(),
        ref_name: "main".to_string(),
    }
}

fn file(key: &str, content_type: &str) -> CommitFile {
    CommitFile {
        logical_key: key.to_string(),
        physical_path: None,
        etag: None,
        size: Some(1),
        content_type: Some(content_type.to_string()),
        checksum: None,
        version_id: None,
        last_modified: None,
    }
}

async fn write(location: &str, key: &str, bytes: &'static [u8]) {
    // The warehouse's memory profile reads the same thread-local store.
    lakekeeper_io::memory::MemoryStorage::new()
        .write(
            &format!("{location}/{key}"),
            bytes::Bytes::from_static(bytes),
        )
        .await
        .unwrap();
}

async fn import(
    ctx: &TestApiContext,
    prefix: &str,
    ns: &str,
    mode: ImportMode,
    on_constraint_violation: Option<ConstraintViolationPolicy>,
) -> lakekeeper::api::Result<ImportDatasetResponse> {
    CatalogServer::import_dataset(
        ds_params(prefix, ns),
        ImportDatasetRequest {
            mode: Some(mode),
            on_constraint_violation,
            ..Default::default()
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
}

async fn commit(
    ctx: &TestApiContext,
    prefix: &str,
    ns: &str,
    added: Vec<CommitFile>,
) -> lakekeeper::api::Result<()> {
    let head = CatalogServer::list_dataset_refs(
        ds_params(prefix, ns),
        ctx.clone(),
        random_request_metadata(),
    )
    .await?
    .refs
    .into_iter()
    .find(|r| r.name == "main")
    .and_then(|r| r.snapshot_id);
    CatalogServer::commit_dataset(
        main_params(prefix, ns),
        CommitDatasetRequest {
            parent_snapshot_id: head,
            added,
            removed: vec![],
            summary: None,
            on_constraint_violation: None,
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .map(|_| ())
}

async fn update_constraints(
    ctx: &TestApiContext,
    prefix: &str,
    ns: &str,
    constraints: Option<DatasetConstraints>,
) -> lakekeeper::api::Result<Option<DatasetConstraints>> {
    CatalogServer::update_dataset_settings(
        ds_params(prefix, ns),
        UpdateDatasetSettingsRequest {
            constraints,
            retention: None,
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .map(|r| r.dataset.constraints)
}

async fn file_keys(ctx: &TestApiContext, prefix: &str, ns: &str) -> Vec<String> {
    CatalogServer::list_dataset_files(
        main_params(prefix, ns),
        ListDatasetFilesQuery::default(),
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap()
    .files
    .into_iter()
    .map(|f| f.logical_key)
    .collect()
}

#[sqlx::test]
async fn test_a_skip_commit_records_what_conforms_and_reports_the_rest(pool: PgPool) {
    let (ctx, prefix, ns, _) = constrained_dataset(pool, images_only()).await;

    let committed = CatalogServer::commit_dataset(
        main_params(&prefix, &ns),
        CommitDatasetRequest {
            parent_snapshot_id: None,
            added: vec![
                file("a.jpg", "image/jpeg"),
                file("notes.pdf", "application/pdf"),
                file("b.jpg", "image/jpeg"),
            ],
            removed: vec![],
            summary: None,
            on_constraint_violation: Some(ConstraintViolationPolicy::Skip),
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .expect("a skip commit succeeds");

    assert_eq!(file_keys(&ctx, &prefix, &ns).await, ["a.jpg", "b.jpg"]);
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
    let (ctx, prefix, ns, location) = constrained_dataset(pool, images_only()).await;
    for key in ["a.jpg", "b.jpg", "notes.txt"] {
        write(&location, key, b"x").await;
    }

    let imported = import(&ctx, &prefix, &ns, ImportMode::AddOnly, None)
        .await
        .expect("a stray file does not stop the import");

    assert_eq!(imported.imported, 2);
    assert_eq!(imported.skipped, 1);
    assert_eq!(imported.skipped_files[0].logical_key, "notes.txt");
    assert_eq!(file_keys(&ctx, &prefix, &ns).await, ["a.jpg", "b.jpg"]);
}

#[sqlx::test]
async fn test_an_import_asked_to_reject_refuses_and_publishes_nothing(pool: PgPool) {
    let (ctx, prefix, ns, location) = constrained_dataset(pool, images_only()).await;
    for key in ["a.jpg", "notes.txt"] {
        write(&location, key, b"x").await;
    }

    let err = import(
        &ctx,
        &prefix,
        &ns,
        ImportMode::AddOnly,
        Some(ConstraintViolationPolicy::Reject),
    )
    .await
    .expect_err("reject refuses the import");

    assert_eq!(err.error.code, StatusCode::BAD_REQUEST, "got: {err:?}");
    assert_eq!(err.error.r#type, "DatasetConstraintViolation");
    assert!(file_keys(&ctx, &prefix, &ns).await.is_empty());
}

/// A key whose new bytes are refused leaves the dataset. Kept, its entry would
/// describe bytes storage has replaced.
#[sqlx::test]
async fn test_a_sync_removes_a_file_whose_new_bytes_are_refused(pool: PgPool) {
    let (ctx, prefix, ns, location) = constrained_dataset(
        pool,
        DatasetConstraints {
            allowed_content_types: None,
            max_file_size: Some(4),
        },
    )
    .await;
    write(&location, "a.bin", b"x").await;
    write(&location, "b.bin", b"x").await;
    import(&ctx, &prefix, &ns, ImportMode::Sync, None)
        .await
        .unwrap();

    write(&location, "a.bin", b"far too large").await;
    let synced = import(&ctx, &prefix, &ns, ImportMode::Sync, None)
        .await
        .expect("the sync succeeds");

    assert_eq!(synced.modified, 0);
    assert_eq!(synced.removed, 1);
    assert_eq!(synced.skipped, 1);
    assert_eq!(synced.skipped_files[0].logical_key, "a.bin");
    assert_eq!(file_keys(&ctx, &prefix, &ns).await, ["b.bin"]);
}

#[sqlx::test]
async fn test_an_import_names_a_hundred_skipped_objects_and_counts_the_rest(pool: PgPool) {
    let (ctx, prefix, ns, location) = constrained_dataset(pool, images_only()).await;
    for i in 0..150 {
        lakekeeper_io::memory::MemoryStorage::new()
            .write(
                &format!("{location}/logs/{i:03}.txt"),
                bytes::Bytes::from_static(b"x"),
            )
            .await
            .unwrap();
    }

    let imported = import(&ctx, &prefix, &ns, ImportMode::AddOnly, None)
        .await
        .unwrap();

    assert_eq!(imported.imported, 0);
    assert_eq!(imported.skipped, 150);
    assert_eq!(imported.skipped_files.len(), 100);
    // An import that registered nothing publishes nothing.
    assert_eq!(imported.snapshot_id, None);
    assert!(file_keys(&ctx, &prefix, &ns).await.is_empty());
}

#[sqlx::test]
async fn test_updated_constraints_bound_the_next_commit_only(pool: PgPool) {
    let (ctx, prefix, ns, _) = constrained_dataset(pool, DatasetConstraints::default()).await;
    commit(
        &ctx,
        &prefix,
        &ns,
        vec![file("notes.pdf", "application/pdf")],
    )
    .await
    .expect("no constraints yet");

    let updated = update_constraints(&ctx, &prefix, &ns, Some(images_only()))
        .await
        .expect("the update succeeds");
    assert_eq!(updated, Some(images_only()));

    let err = commit(
        &ctx,
        &prefix,
        &ns,
        vec![file("more.pdf", "application/pdf")],
    )
    .await
    .expect_err("the new constraints bound the next commit");
    assert_eq!(err.error.r#type, "DatasetConstraintViolation");
    // What was committed before stays: constraints are not re-checked.
    assert_eq!(file_keys(&ctx, &prefix, &ns).await, ["notes.pdf"]);
}

#[sqlx::test]
async fn test_empty_constraints_lift_them(pool: PgPool) {
    let (ctx, prefix, ns, _) = constrained_dataset(pool, images_only()).await;

    let updated = update_constraints(&ctx, &prefix, &ns, Some(DatasetConstraints::default()))
        .await
        .unwrap();

    assert_eq!(updated, None);
    commit(
        &ctx,
        &prefix,
        &ns,
        vec![file("notes.pdf", "application/pdf")],
    )
    .await
    .expect("nothing bounds the commit any more");
}

#[sqlx::test]
async fn test_a_settings_update_leaves_what_it_does_not_name(pool: PgPool) {
    let (ctx, prefix, ns, _) = constrained_dataset(pool, images_only()).await;

    let updated = update_constraints(&ctx, &prefix, &ns, None).await.unwrap();

    assert_eq!(updated, Some(images_only()));
}

#[sqlx::test]
async fn test_constraints_no_file_could_meet_are_refused(pool: PgPool) {
    let (ctx, prefix, ns, _) = constrained_dataset(pool, images_only()).await;

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
        let err = update_constraints(&ctx, &prefix, &ns, Some(unmeetable))
            .await
            .expect_err(what);
        assert_eq!(err.error.code, StatusCode::BAD_REQUEST, "{what}: {err:?}");
        assert_eq!(err.error.r#type, "InvalidConstraints", "{what}");
    }
}
