//! End-to-end tests for the Dataset API.
//!
//! Datasets are a `tabular` subtype, so two properties get exercised here that
//! no dataset-only test would catch: a dataset takes a name out of the same
//! per-namespace name space as tables and views, and dropping one goes through
//! the shared tabular deletion path.
use http::StatusCode;
use iceberg::NamespaceIdent;
use lakekeeper::{
    api::{
        ApiContext,
        data::v1::datasets::{
            CreateDatasetRequest, DatasetParameters, DatasetService as _, ImportDatasetRequest,
            ImportMode, ListDatasetsQuery, LoadDatasetCredentialsRequest, RenameDatasetRequest,
            RenameDatasetTarget,
        },
        iceberg::{
            types::{DropParams, Prefix},
            v1::{
                DataAccess,
                namespace::{NamespaceDropFlags, NamespaceParameters},
            },
        },
        management::v1::{
            ApiServer, dataset::DatasetManagementService as _, warehouse::TabularDeleteProfile,
        },
    },
    server::CatalogServer,
    service::{
        CatalogTabularOps, DatasetOwnership, Location, State, TabularListFlags, ViewOrTableInfo,
        WarehouseId, authz::AllowAllAuthorizer, idempotency::IdempotencyKey,
    },
};
use lakekeeper_integration_tests::{
    create_dataset, create_generic_table, create_ns, create_table, drop_namespace,
    memory_io_profile, random_request_metadata, setup,
};
use lakekeeper_io::LakekeeperStorage as _;
use lakekeeper_storage_postgres::{PostgresBackend, SecretsState};
use sqlx::PgPool;
use uuid::Uuid;

type TestApiContext = ApiContext<State<AllowAllAuthorizer, PostgresBackend, SecretsState>>;

async fn make_ns(pool: PgPool) -> (TestApiContext, String, String) {
    let (ctx, prefix, ns_name, _base) = make_ns_with_base(pool).await;
    (ctx, prefix, ns_name)
}

/// Also yields the warehouse's base location. A dataset location must be a
/// sublocation of the storage profile — that is what confines an import to the
/// warehouse's own bucket — so import tests have to address a real sublocation.
async fn make_ns_with_base(pool: PgPool) -> (TestApiContext, String, String, String) {
    let profile = memory_io_profile();
    let base = profile
        .base_location()
        .expect("memory profile has a base location")
        .to_string();
    let (ctx, warehouse) = setup(
        pool,
        profile,
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
    (ctx, prefix, ns_name, base)
}

fn ns_params(prefix: &str, ns_name: &str) -> NamespaceParameters {
    NamespaceParameters {
        prefix: Some(Prefix(prefix.to_string())),
        namespace: NamespaceIdent::new(ns_name.to_string()),
    }
}

fn dataset_params(prefix: &str, ns_name: &str, name: &str) -> DatasetParameters {
    DatasetParameters {
        prefix: Some(Prefix(prefix.to_string())),
        namespace: NamespaceIdent::new(ns_name.to_string()),
        dataset_name: name.to_string(),
    }
}

#[sqlx::test]
async fn test_create_and_load_dataset(pool: PgPool) {
    let (ctx, prefix, ns_name) = make_ns(pool).await;

    let created = create_dataset(ctx.clone(), prefix.clone(), ns_name.clone(), "images")
        .await
        .expect("dataset creation succeeds");
    assert_eq!(created.dataset.name, "images");
    // No location was supplied, so Lakekeeper allocated and owns the prefix.
    assert_eq!(created.dataset.ownership, DatasetOwnership::Managed);
    assert!(!created.dataset.protected);

    let loaded = CatalogServer::load_dataset(
        dataset_params(&prefix, &ns_name, "images"),
        ctx,
        random_request_metadata(),
    )
    .await
    .expect("dataset loads");

    assert_eq!(loaded.dataset.id, created.dataset.id);
    assert_eq!(loaded.dataset.location, created.dataset.location);
    assert_eq!(loaded.dataset.ownership, DatasetOwnership::Managed);
}

#[sqlx::test]
async fn test_create_imported_dataset_borrows_the_supplied_location(pool: PgPool) {
    let (ctx, prefix, ns_name, base) = make_ns_with_base(pool).await;
    let location = format!("{}/ml/images/raw", base.trim_end_matches('/'));

    let created = CatalogServer::create_dataset(
        ns_params(&prefix, &ns_name),
        CreateDatasetRequest {
            name: "raw".to_string(),
            location: Some(location),
            constraints: None,
        },
        ctx,
        random_request_metadata(),
    )
    .await
    .expect("import succeeds");

    // A caller-supplied location means the prefix pre-existed: Lakekeeper borrows
    // it and must never treat it as its own to delete.
    assert_eq!(created.dataset.ownership, DatasetOwnership::Imported);
    assert!(
        created.dataset.location.contains("ml/images/raw"),
        "location should be the supplied prefix, got {}",
        created.dataset.location
    );
}

#[sqlx::test]
async fn test_list_datasets(pool: PgPool) {
    let (ctx, prefix, ns_name) = make_ns(pool).await;

    for name in ["alpha", "beta", "gamma"] {
        create_dataset(ctx.clone(), prefix.clone(), ns_name.clone(), name)
            .await
            .unwrap();
    }

    let listed = CatalogServer::list_datasets(
        ns_params(&prefix, &ns_name),
        ListDatasetsQuery::default(),
        ctx,
        random_request_metadata(),
    )
    .await
    .expect("list succeeds");

    let mut names: Vec<&str> = listed.identifiers.iter().map(|i| i.name.as_str()).collect();
    names.sort_unstable();
    assert_eq!(names, vec!["alpha", "beta", "gamma"]);
}

#[sqlx::test]
async fn test_drop_dataset(pool: PgPool) {
    let (ctx, prefix, ns_name) = make_ns(pool).await;

    create_dataset(ctx.clone(), prefix.clone(), ns_name.clone(), "images")
        .await
        .unwrap();

    CatalogServer::drop_dataset(
        dataset_params(&prefix, &ns_name, "images"),
        DropParams {
            purge_requested: false,
            force: false,
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .expect("drop succeeds");

    let err = CatalogServer::load_dataset(
        dataset_params(&prefix, &ns_name, "images"),
        ctx,
        random_request_metadata(),
    )
    .await
    .expect_err("dropped dataset must not load");
    assert_eq!(err.error.code, StatusCode::NOT_FOUND, "got: {err:?}");
}

#[sqlx::test]
async fn test_purging_an_imported_dataset_is_refused(pool: PgPool) {
    let (ctx, prefix, ns_name, base) = make_ns_with_base(pool).await;
    let location = format!("{}/borrowed", base.trim_end_matches('/'));

    CatalogServer::create_dataset(
        ns_params(&prefix, &ns_name),
        CreateDatasetRequest {
            name: "borrowed".to_string(),
            location: Some(location),
            constraints: None,
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    // The objects belong to whoever put them there. Refusing the purge is the
    // whole point of tracking ownership.
    let err = CatalogServer::drop_dataset(
        dataset_params(&prefix, &ns_name, "borrowed"),
        DropParams {
            purge_requested: true,
            force: false,
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .expect_err("purging borrowed files must be refused");
    assert_eq!(err.error.code, StatusCode::BAD_REQUEST, "got: {err:?}");

    // Refusing the purge must not have dropped it either.
    CatalogServer::load_dataset(
        dataset_params(&prefix, &ns_name, "borrowed"),
        ctx,
        random_request_metadata(),
    )
    .await
    .expect("dataset still exists after a refused purge");
}

#[sqlx::test]
async fn test_iceberg_table_blocks_dataset_with_same_name(pool: PgPool) {
    let (ctx, prefix, ns_name) = make_ns(pool).await;

    create_table(
        ctx.clone(),
        prefix.clone(),
        ns_name.clone(),
        "collide".to_string(),
        false,
    )
    .await
    .unwrap();

    let err = create_dataset(ctx, prefix, ns_name, "collide")
        .await
        .expect_err("dataset create must fail when an iceberg table holds the name");
    assert_eq!(err.error.code, StatusCode::CONFLICT, "got: {err:?}");
}

#[sqlx::test]
async fn test_dataset_blocks_iceberg_table_with_same_name(pool: PgPool) {
    let (ctx, prefix, ns_name) = make_ns(pool).await;

    create_dataset(ctx.clone(), prefix.clone(), ns_name.clone(), "collide")
        .await
        .unwrap();

    let err = create_table(ctx, prefix, ns_name, "collide".to_string(), false)
        .await
        .expect_err("iceberg table create must fail when a dataset holds the name");
    assert_eq!(err.error.code, StatusCode::CONFLICT, "got: {err:?}");
}

#[sqlx::test]
async fn test_dataset_blocks_generic_table_with_same_name(pool: PgPool) {
    let (ctx, prefix, ns_name) = make_ns(pool).await;

    create_dataset(ctx.clone(), prefix.clone(), ns_name.clone(), "collide")
        .await
        .unwrap();

    let err = create_generic_table(ctx, prefix, ns_name, "collide".to_string())
        .await
        .expect_err("generic table create must fail when a dataset holds the name");
    assert_eq!(err.error.code, StatusCode::CONFLICT, "got: {err:?}");
}

#[sqlx::test]
async fn test_duplicate_dataset_name_is_refused(pool: PgPool) {
    let (ctx, prefix, ns_name) = make_ns(pool).await;

    create_dataset(ctx.clone(), prefix.clone(), ns_name.clone(), "images")
        .await
        .unwrap();

    let err = create_dataset(ctx, prefix, ns_name, "images")
        .await
        .expect_err("a second dataset with the same name must fail");
    assert_eq!(err.error.code, StatusCode::CONFLICT, "got: {err:?}");
}

#[sqlx::test]
async fn test_load_missing_dataset_is_not_found(pool: PgPool) {
    let (ctx, prefix, ns_name) = make_ns(pool).await;

    let err = CatalogServer::load_dataset(
        dataset_params(&prefix, &ns_name, "nope"),
        ctx,
        random_request_metadata(),
    )
    .await
    .expect_err("loading a missing dataset must fail");
    assert_eq!(err.error.code, StatusCode::NOT_FOUND, "got: {err:?}");
}

#[sqlx::test]
async fn test_a_protected_dataset_cannot_be_dropped(pool: PgPool) {
    let (ctx, prefix, ns_name) = make_ns(pool).await;

    let created = create_dataset(ctx.clone(), prefix.clone(), ns_name.clone(), "images")
        .await
        .unwrap();
    let warehouse_id = prefix.parse::<Uuid>().unwrap();

    let set = ApiServer::set_dataset_protection(
        created.dataset.id,
        warehouse_id.into(),
        true,
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .expect("protection can be set");
    assert!(set.protected);

    let err = CatalogServer::drop_dataset(
        dataset_params(&prefix, &ns_name, "images"),
        DropParams {
            purge_requested: false,
            force: false,
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .expect_err("a protected dataset must not drop");
    assert_eq!(err.error.code, StatusCode::CONFLICT);

    // Reading back through the management API, not the value we just sent.
    let got = ApiServer::get_dataset_protection(
        created.dataset.id,
        warehouse_id.into(),
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    assert!(got.protected);

    ApiServer::set_dataset_protection(
        created.dataset.id,
        warehouse_id.into(),
        false,
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    CatalogServer::drop_dataset(
        dataset_params(&prefix, &ns_name, "images"),
        DropParams {
            purge_requested: false,
            force: false,
        },
        ctx,
        random_request_metadata(),
    )
    .await
    .expect("lifting protection allows the drop");
}

#[sqlx::test]
async fn test_rename_dataset_within_and_across_namespaces(pool: PgPool) {
    let (ctx, prefix, ns_name) = make_ns(pool).await;
    let other_ns = format!("ns_{}", Uuid::now_v7());
    create_ns(ctx.clone(), prefix.clone(), other_ns.clone()).await;

    create_dataset(ctx.clone(), prefix.clone(), ns_name.clone(), "images")
        .await
        .unwrap();

    CatalogServer::rename_dataset(
        Some(Prefix(prefix.clone())),
        RenameDatasetRequest {
            source: RenameDatasetTarget {
                namespace: vec![ns_name.clone()],
                name: "images".to_string(),
            },
            destination: RenameDatasetTarget {
                namespace: vec![ns_name.clone()],
                name: "pictures".to_string(),
            },
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .expect("rename within a namespace");

    CatalogServer::load_dataset(
        dataset_params(&prefix, &ns_name, "images"),
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .expect_err("the old name must not resolve");
    CatalogServer::load_dataset(
        dataset_params(&prefix, &ns_name, "pictures"),
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .expect("the new name resolves");

    // Across namespaces the authorizer has to re-point the dataset's parent edge,
    // so this is the case that exercises detach/attach.
    CatalogServer::rename_dataset(
        Some(Prefix(prefix.clone())),
        RenameDatasetRequest {
            source: RenameDatasetTarget {
                namespace: vec![ns_name.clone()],
                name: "pictures".to_string(),
            },
            destination: RenameDatasetTarget {
                namespace: vec![other_ns.clone()],
                name: "pictures".to_string(),
            },
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .expect("rename across namespaces");

    CatalogServer::load_dataset(
        dataset_params(&prefix, &other_ns, "pictures"),
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .expect("resolves in the destination namespace");
    CatalogServer::load_dataset(
        dataset_params(&prefix, &ns_name, "pictures"),
        ctx,
        random_request_metadata(),
    )
    .await
    .expect_err("must not resolve in the source namespace");
}

#[sqlx::test]
async fn test_rename_dataset_idempotent_replay(pool: PgPool) {
    let (ctx, prefix, ns_name) = make_ns(pool).await;
    create_dataset(ctx.clone(), prefix.clone(), ns_name.clone(), "images")
        .await
        .unwrap();

    let key = IdempotencyKey::parse(&Uuid::now_v7().to_string()).unwrap();
    let mut metadata = random_request_metadata();
    metadata.with_idempotency_key(key);
    let request = RenameDatasetRequest {
        source: RenameDatasetTarget {
            namespace: vec![ns_name.clone()],
            name: "images".to_string(),
        },
        destination: RenameDatasetTarget {
            namespace: vec![ns_name.clone()],
            name: "pictures".to_string(),
        },
    };

    CatalogServer::rename_dataset(
        Some(Prefix(prefix.clone())),
        request.clone(),
        ctx.clone(),
        metadata.clone(),
    )
    .await
    .expect("rename with an idempotency key");
    CatalogServer::rename_dataset(Some(Prefix(prefix)), request, ctx, metadata)
        .await
        .expect("idempotent replay should succeed");
}

#[sqlx::test]
async fn test_dataset_credentials_load_for_an_existing_dataset_only(pool: PgPool) {
    let (ctx, prefix, ns_name) = make_ns(pool).await;

    let created = create_dataset(ctx.clone(), prefix.clone(), ns_name.clone(), "images")
        .await
        .unwrap();

    let load = || {
        CatalogServer::load_dataset_credentials(
            dataset_params(&prefix, &ns_name, "images"),
            LoadDatasetCredentialsRequest {},
            DataAccess::not_specified(),
            ctx.clone(),
            random_request_metadata(),
        )
    };
    let first = load()
        .await
        .expect("credentials are vended for a dataset the caller may read");

    // The memory profile vends no credentials, so what is left to check is the
    // prefix they would be scoped to: the dataset's own, not the namespace's.
    assert!(
        created
            .dataset
            .location
            .contains(&created.dataset.id.to_string()),
        "dataset location must be its own prefix, got {}",
        created.dataset.location
    );
    // A writer writes into a folder of its own below the location, fresh every time,
    // so it never holds a credential for an object a snapshot records.
    let write_prefix = first
        .write_prefix
        .expect("a writer is given a write prefix");
    let data_dir = format!("{}/data/", created.dataset.location.trim_end_matches('/'));
    assert!(
        write_prefix.starts_with(&data_dir) && write_prefix.len() > data_dir.len(),
        "{write_prefix} must be a folder below {data_dir}"
    );
    let second = load().await.unwrap();
    assert_ne!(second.write_prefix.as_deref(), Some(write_prefix.as_str()));

    // A dataset that does not exist must not vend anything.
    CatalogServer::load_dataset_credentials(
        dataset_params(&prefix, &ns_name, "absent"),
        LoadDatasetCredentialsRequest {},
        DataAccess::not_specified(),
        ctx,
        random_request_metadata(),
    )
    .await
    .expect_err("no credentials for a dataset that does not exist");
}

/// An imported dataset's prefix is borrowed and may hold objects that are not the
/// dataset's, so no credential is vended for it: its files are read through
/// access grants.
#[sqlx::test]
async fn test_an_imported_dataset_vends_no_credentials(pool: PgPool) {
    let (ctx, prefix, ns_name, base) = make_ns_with_base(pool).await;
    CatalogServer::create_dataset(
        ns_params(&prefix, &ns_name),
        CreateDatasetRequest {
            name: "borrowed".to_string(),
            location: Some(format!("{}/borrowed", base.trim_end_matches('/'))),
            constraints: None,
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    let err = CatalogServer::load_dataset_credentials(
        dataset_params(&prefix, &ns_name, "borrowed"),
        LoadDatasetCredentialsRequest {},
        DataAccess::not_specified(),
        ctx,
        random_request_metadata(),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.code, StatusCode::CONFLICT, "{err:?}");
    assert_eq!(err.error.r#type, "CannotVendCredentialsForImportedDataset");
}

#[sqlx::test]
async fn test_a_dataset_is_found_by_a_path_under_its_location(pool: PgPool) {
    let (ctx, prefix, ns_name) = make_ns(pool).await;
    let created = create_dataset(ctx.clone(), prefix.clone(), ns_name, "images")
        .await
        .unwrap();
    let warehouse_id = WarehouseId::from(prefix.parse::<Uuid>().unwrap());

    // A dataset has no metadata location, like a generic table. The lookup must not
    // read that as "staged" and hide it from an active-only search.
    let mut under = created.dataset.location.parse::<Location>().unwrap();
    under.push("train/0001.jpg");
    let found = PostgresBackend::get_tabular_infos_by_s3_location(
        warehouse_id,
        &under,
        TabularListFlags::active(),
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();

    assert!(
        matches!(&found, Some(ViewOrTableInfo::Dataset(info)) if info.tabular_id == created.dataset.id),
        "expected the dataset, got {found:?}"
    );
}

/// A scan that finds nothing new leaves the branch where it is. An empty snapshot
/// would still count towards the next checkpoint, which restates every file, so a
/// scheduled sync over an unchanged prefix must publish nothing.
#[sqlx::test]
async fn test_an_import_that_changes_nothing_publishes_nothing(pool: PgPool) {
    let (ctx, prefix, ns_name) = make_ns(pool.clone()).await;
    let created = create_dataset(ctx.clone(), prefix.clone(), ns_name.clone(), "images")
        .await
        .unwrap();
    // The warehouse's memory profile reads the same thread-local store.
    let storage = lakekeeper_io::memory::MemoryStorage::new();
    for key in ["a.txt", "b.txt"] {
        storage
            .write(
                &format!("{}/{key}", created.dataset.location),
                bytes::Bytes::from_static(b"x"),
            )
            .await
            .unwrap();
    }
    let import = |mode| {
        CatalogServer::import_dataset(
            dataset_params(&prefix, &ns_name, "images"),
            ImportDatasetRequest {
                mode: Some(mode),
                ..Default::default()
            },
            ctx.clone(),
            random_request_metadata(),
        )
    };

    let first = import(ImportMode::AddOnly).await.unwrap();
    assert_eq!(first.imported, 2);
    let head = first
        .snapshot_id
        .expect("the first import publishes a snapshot");

    for mode in [ImportMode::AddOnly, ImportMode::Sync] {
        let again = import(mode).await.unwrap();
        assert_eq!((again.imported, again.modified, again.removed), (0, 0, 0));
        assert_eq!(
            again.snapshot_id,
            Some(head),
            "{mode:?} reports the unchanged head"
        );
    }
    let snapshots: i64 = sqlx::query_scalar("SELECT count(*) FROM dataset_snapshot")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        snapshots, 1,
        "a no-op import must not leave a snapshot behind"
    );
}

/// A recursive drop with purge must leave an imported dataset's objects alone: it
/// borrows its prefix. The managed dataset beside it is the control.
#[sqlx::test]
async fn test_a_purging_namespace_drop_spares_an_imported_datasets_objects(pool: PgPool) {
    let (ctx, prefix, ns_name, base) = make_ns_with_base(pool.clone()).await;
    let imported = CatalogServer::create_dataset(
        ns_params(&prefix, &ns_name),
        CreateDatasetRequest {
            name: "raw".to_string(),
            location: Some(format!("{}/ml/images/raw", base.trim_end_matches('/'))),
            constraints: None,
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    let managed = create_dataset(ctx.clone(), prefix.clone(), ns_name.clone(), "images")
        .await
        .unwrap();

    drop_namespace(
        ctx.clone(),
        NamespaceDropFlags {
            force: false,
            purge: true,
            recursive: true,
        },
        ns_params(&prefix, &ns_name),
    )
    .await
    .unwrap();

    for (created, expected) in [(&imported, 0), (&managed, 1)] {
        // The purge worker may already have run, so both tables count.
        let purges: i64 = sqlx::query_scalar(
            "SELECT (SELECT count(*) FROM task WHERE queue_name = 'tabular_purge' AND entity_id = $1)
                  + (SELECT count(*) FROM task_log WHERE queue_name = 'tabular_purge' AND entity_id = $1)",
        )
        .bind(*created.dataset.id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            purges, expected,
            "{:?} dataset: expected {expected} purge task(s)",
            created.dataset.ownership
        );
    }
}

/// A token the server never issued is the caller's mistake, not the server's.
#[sqlx::test]
async fn test_a_malformed_page_token_is_a_bad_request(pool: PgPool) {
    let (ctx, prefix, ns_name) = make_ns(pool).await;
    let err = CatalogServer::list_datasets(
        ns_params(&prefix, &ns_name),
        ListDatasetsQuery {
            page_token: Some("not-a-token".to_string()),
            page_size: None,
        },
        ctx,
        random_request_metadata(),
    )
    .await
    .expect_err("a malformed token must be refused");
    assert_eq!(err.error.code, StatusCode::BAD_REQUEST, "got: {err:?}");
}

/// A failed import publishes nothing and leaves nothing staged behind it.
#[sqlx::test]
async fn test_a_failed_import_leaves_nothing_staged(pool: PgPool) {
    let (ctx, prefix, ns_name) = make_ns(pool.clone()).await;
    let created = create_dataset(ctx.clone(), prefix.clone(), ns_name.clone(), "images")
        .await
        .unwrap();
    let storage = lakekeeper_io::memory::MemoryStorage::new();
    for key in ["a.txt", "b.txt", "c.txt"] {
        storage
            .write(
                &format!("{}/{key}", created.dataset.location),
                bytes::Bytes::from_static(b"x"),
            )
            .await
            .unwrap();
    }

    // A sync over a truncated scan is refused only after the scan has staged rows.
    let err = CatalogServer::import_dataset(
        dataset_params(&prefix, &ns_name, "images"),
        ImportDatasetRequest {
            mode: Some(ImportMode::Sync),
            max_files: Some(2),
            ..Default::default()
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .expect_err("a sync over a truncated scan must be refused");
    assert_eq!(err.error.code, StatusCode::BAD_REQUEST, "got: {err:?}");

    let staging: i64 =
        sqlx::query_scalar("SELECT count(*) FROM dataset_snapshot WHERE status = 'staging'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        staging, 0,
        "the failed import left a staging snapshot behind"
    );
}

/// A queued import no scan could honour is refused up front, not accepted with a
/// task id and left to fail in the worker.
#[sqlx::test]
async fn test_a_queued_import_with_invalid_parameters_is_refused(pool: PgPool) {
    let (ctx, prefix, ns_name) = make_ns(pool.clone()).await;
    create_dataset(ctx.clone(), prefix.clone(), ns_name.clone(), "images")
        .await
        .unwrap();
    for request in [
        ImportDatasetRequest {
            queued: Some(true),
            max_files: Some(0),
            ..Default::default()
        },
        ImportDatasetRequest {
            queued: Some(true),
            sub_prefix: Some("../elsewhere".to_string()),
            ..Default::default()
        },
    ] {
        let err = CatalogServer::import_dataset(
            dataset_params(&prefix, &ns_name, "images"),
            request.clone(),
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .expect_err("an import no scan could honour must be refused");
        assert_eq!(
            err.error.code,
            StatusCode::BAD_REQUEST,
            "{request:?}: {err:?}"
        );
    }
    let queued: i64 =
        sqlx::query_scalar("SELECT count(*) FROM task WHERE queue_name = 'dataset_import'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(queued, 0, "a refused import must not be enqueued");
}
