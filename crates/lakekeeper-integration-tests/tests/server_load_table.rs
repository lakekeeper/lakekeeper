// Extracted from crates/lakekeeper/src/server/tables/load_table.rs.
// Original location was `#[cfg(any())] mod tests` (VAK-437 split).

use std::collections::HashMap;

use iceberg::{
    NamespaceIdent, TableIdent, TableUpdate,
    spec::{
        MAIN_BRANCH, NestedField, Operation, PrimitiveType, Schema, Snapshot, SnapshotReference,
        SnapshotRetention, Summary, Type, UnboundPartitionSpec,
    },
};
use iceberg_ext::catalog::rest::{CreateTableRequest, ETag, LoadTableResult};

/// The shape a client echoes back in `If-None-Match`: what the HTTP layer's
/// `parse_etags` yields from the wire value — weak marker and quotes stripped.
/// These tests drive the handler directly, so they have to do it themselves.
fn as_client_etag(etag: &ETag) -> ETag {
    ETag::from(etag.validator())
}
use lakekeeper::{
    api::{
        ApiContext,
        iceberg::v1::{
            NamespaceParameters, Prefix, TableParameters,
            namespace::NamespaceService as _,
            tables::{
                DataAccess, DataAccessMode, LoadTableFilters, LoadTableRequest,
                LoadTableResultOrNotModified, SnapshotsQuery, TablesService as _,
            },
        },
        management::v1::warehouse::TabularDeleteProfile,
    },
    server::{CatalogServer, tables::load_table::load_table},
    service::{
        State,
        authz::{AllowAllAuthorizer, CatalogTableAction, tests::HidingAuthorizer},
    },
};
use lakekeeper_integration_tests::{
    SetupTestCatalog, create_ns, create_table, memory_io_profile, random_request_metadata,
    setup_simple,
};
use lakekeeper_storage_postgres::{PostgresBackend, SecretsState};
use sqlx::PgPool;

fn create_test_schema() -> Schema {
    Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .unwrap()
}

fn create_table_request(table_name: &str) -> CreateTableRequest {
    CreateTableRequest {
        name: table_name.to_string(),
        location: None,
        schema: create_test_schema(),
        partition_spec: Some(UnboundPartitionSpec::builder().build()),
        write_order: None,
        stage_create: Some(false),
        properties: None,
    }
}

#[allow(clippy::too_many_lines)]
async fn setup_simple_table(
    pool: PgPool,
) -> (
    ApiContext<State<AllowAllAuthorizer, PostgresBackend, SecretsState>>,
    NamespaceParameters,
    TableIdent,
    LoadTableResult,
) {
    let prof = lakekeeper_integration_tests::memory_io_profile();
    let (ctx, warehouse) = setup_simple(
        pool,
        prof,
        None,
        AllowAllAuthorizer::default(),
        TabularDeleteProfile::Hard {},
        None,
    )
    .await;

    // Create namespace
    let ns_name = NamespaceIdent::new("test_namespace".to_string());
    let ns_params = NamespaceParameters {
        namespace: ns_name.clone(),
        prefix: Some(warehouse.warehouse_id.to_string().into()),
    };

    let _ = CatalogServer::create_namespace(
        ns_params.prefix.clone(),
        lakekeeper::api::iceberg::v1::CreateNamespaceRequest {
            namespace: ns_name.clone(),
            properties: None,
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    // Create table
    let table_ident = TableIdent::new(ns_name, "test_table".to_string());
    let table = CatalogServer::create_table(
        ns_params.clone(),
        create_table_request("test_table"),
        DataAccess::not_specified(),
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    (ctx, ns_params, table_ident, table)
}

#[allow(clippy::too_many_lines)]
async fn setup_table_with_snapshots(
    pool: PgPool,
) -> (
    ApiContext<State<AllowAllAuthorizer, PostgresBackend, SecretsState>>,
    NamespaceParameters,
    TableIdent,
    LoadTableResult,
) {
    let prof = lakekeeper_integration_tests::memory_io_profile();
    let (ctx, warehouse) = setup_simple(
        pool,
        prof,
        None,
        AllowAllAuthorizer::default(),
        TabularDeleteProfile::Hard {},
        None,
    )
    .await;

    // Create namespace
    let ns_name = NamespaceIdent::new("test_namespace".to_string());
    let ns_params = NamespaceParameters {
        namespace: ns_name.clone(),
        prefix: Some(warehouse.warehouse_id.to_string().into()),
    };

    let _ = CatalogServer::create_namespace(
        ns_params.prefix.clone(),
        lakekeeper::api::iceberg::v1::CreateNamespaceRequest {
            namespace: ns_name.clone(),
            properties: None,
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    // Create table
    let table_ident = TableIdent::new(ns_name, "test_table".to_string());
    let table = CatalogServer::create_table(
        ns_params.clone(),
        create_table_request("test_table"),
        DataAccess::not_specified(),
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    // Add multiple snapshots to the table
    let table_params = TableParameters {
        prefix: Some(warehouse.warehouse_id.to_string().into()),
        table: table_ident.clone(),
    };

    // Add first snapshot (snapshot_id: 1) - use current time plus some offset
    let base_time = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap();

    let snapshot1 = Snapshot::builder()
        .with_snapshot_id(1)
        .with_timestamp_ms(base_time + 1000)
        .with_sequence_number(1)
        .with_manifest_list("/path/to/manifest1.avro")
        .with_summary(Summary {
            operation: Operation::Append,
            additional_properties: HashMap::new(),
        })
        .with_schema_id(0)
        .build();

    let commit_request1 = iceberg_ext::catalog::rest::CommitTableRequest {
        identifier: Some(table_ident.clone()),
        requirements: vec![],
        updates: vec![TableUpdate::AddSnapshot {
            snapshot: snapshot1,
        }],
    };

    CatalogServer::commit_table(
        table_params.clone(),
        commit_request1,
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    // Add second snapshot (snapshot_id: 2)
    let snapshot2 = Snapshot::builder()
        .with_snapshot_id(2)
        .with_timestamp_ms(base_time + 2000)
        .with_sequence_number(2)
        .with_manifest_list("/path/to/manifest2.avro")
        .with_summary(Summary {
            operation: Operation::Append,
            additional_properties: HashMap::new(),
        })
        .with_schema_id(0)
        .build();

    let commit_request2 = iceberg_ext::catalog::rest::CommitTableRequest {
        identifier: Some(table_ident.clone()),
        requirements: vec![],
        updates: vec![TableUpdate::AddSnapshot {
            snapshot: snapshot2,
        }],
    };

    CatalogServer::commit_table(
        table_params.clone(),
        commit_request2,
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    // Add third snapshot (snapshot_id: 3)
    let snapshot3 = Snapshot::builder()
        .with_snapshot_id(3)
        .with_timestamp_ms(base_time + 3000)
        .with_sequence_number(3)
        .with_manifest_list("/path/to/manifest3.avro")
        .with_summary(Summary {
            operation: Operation::Append,
            additional_properties: HashMap::new(),
        })
        .with_schema_id(0)
        .build();

    let commit_request3 = iceberg_ext::catalog::rest::CommitTableRequest {
        identifier: Some(table_ident.clone()),
        requirements: vec![],
        updates: vec![TableUpdate::AddSnapshot {
            snapshot: snapshot3,
        }],
    };

    CatalogServer::commit_table(
        table_params.clone(),
        commit_request3,
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    // Set references - add "main" branch pointing to snapshot 2 and "test_branch" pointing to snapshot 3
    let set_ref_main = TableUpdate::SetSnapshotRef {
        ref_name: MAIN_BRANCH.to_string(),
        reference: SnapshotReference {
            snapshot_id: 2,
            retention: SnapshotRetention::Branch {
                min_snapshots_to_keep: None,
                max_snapshot_age_ms: None,
                max_ref_age_ms: None,
            },
        },
    };

    let set_ref_test_branch = TableUpdate::SetSnapshotRef {
        ref_name: "test_branch".to_string(),
        reference: SnapshotReference {
            snapshot_id: 3,
            retention: SnapshotRetention::Branch {
                min_snapshots_to_keep: None,
                max_snapshot_age_ms: None,
                max_ref_age_ms: None,
            },
        },
    };

    let commit_request_refs = iceberg_ext::catalog::rest::CommitTableRequest {
        identifier: Some(table_ident.clone()),
        requirements: vec![],
        updates: vec![set_ref_main, set_ref_test_branch],
    };

    CatalogServer::commit_table(
        table_params.clone(),
        commit_request_refs,
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    (ctx, ns_params, table_ident, table)
}

/// A caller with `GetMetadata` but neither `ReadData` nor `WriteData` gets no
/// storage config — the one body shape the `CatalogDefaultsOnly` ETag axis exists
/// for. It must still carry the advertisement, and must still carry a `config`
/// map at all, which is what distinguishes it from a commit response.
#[sqlx::test]
async fn test_load_table_without_storage_access_still_advertises_planning(pool: PgPool) {
    let authz = HidingAuthorizer::new();
    let (ctx, warehouse) = SetupTestCatalog::builder()
        .pool(pool)
        .authorizer(authz.clone())
        .storage_profile(memory_io_profile())
        .build()
        .setup()
        .await;

    let prefix = warehouse.warehouse_id.to_string();
    let ns = create_ns(ctx.clone(), prefix.clone(), "planning_ns".to_string()).await;
    create_table(ctx.clone(), &prefix, "planning_ns", "t1", false)
        .await
        .unwrap();

    // Metadata stays visible; storage access does not.
    authz.block_action(&format!("table:{:?}", CatalogTableAction::ReadData));
    authz.block_action(&format!("table:{:?}", CatalogTableAction::WriteData));

    let result = CatalogServer::load_table(
        TableParameters {
            prefix: Some(Prefix(prefix)),
            table: TableIdent::new(ns.namespace.clone(), "t1".to_string()),
        },
        LoadTableRequest::builder().build(),
        ctx,
        random_request_metadata(),
    )
    .await
    .expect("metadata access alone must still load the table");

    let LoadTableResultOrNotModified::LoadTableResult(result) = result else {
        panic!("an unconditional load must not return 304");
    };

    assert!(
        result.storage_credentials.is_none(),
        "no storage access must vend nothing: {:?}",
        result.storage_credentials
    );
    let config = result
        .config
        .expect("a load without storage access still carries a config");
    assert_eq!(
        config.get("scan-planning-mode").map(String::as_str),
        Some("client"),
        "the advertisement must not depend on storage access: {config:?}"
    );
}

/// The spec puts `scan-planning-mode` in the `loadTable` config map, and we serve
/// no `planTableScan` endpoint, so every load must say `client`. Asserted on the
/// response rather than on the helper, since the helper is easy to leave unwired.
#[sqlx::test]
async fn test_load_table_advertises_client_side_scan_planning(pool: PgPool) {
    let (ctx, ns_params, table_ident, _) = setup_table_with_snapshots(pool).await;

    let result = CatalogServer::load_table(
        TableParameters {
            prefix: ns_params.prefix.clone(),
            table: table_ident.clone(),
        },
        LoadTableRequest::builder().build(),
        ctx,
        random_request_metadata(),
    )
    .await
    .unwrap();

    let LoadTableResultOrNotModified::LoadTableResult(result) = result else {
        panic!("an unconditional load must not return 304");
    };

    let config = result.config.expect("a load always carries a config");
    assert_eq!(
        config.get("scan-planning-mode").map(String::as_str),
        Some("client"),
        "loadTable must advertise client-side planning: {config:?}"
    );
}

#[sqlx::test]
async fn test_load_table_snapshots_filter_all(pool: PgPool) {
    let (ctx, ns_params, table_ident, _) = setup_table_with_snapshots(pool).await;

    let table_params = TableParameters {
        prefix: ns_params.prefix.clone(),
        table: table_ident.clone(),
    };

    // Test with SnapshotsQuery::All - should return all snapshots
    let filters = LoadTableFilters {
        snapshots: SnapshotsQuery::All,
    };

    let result = CatalogServer::load_table(
        table_params,
        LoadTableRequest::builder().filters(filters).build(),
        ctx,
        random_request_metadata(),
    )
    .await
    .unwrap();

    let LoadTableResultOrNotModified::LoadTableResult(result) = result else {
        panic!("Expected LoadTableResult");
    };

    // Verify that all snapshots are present (1, 2, and 3)

    let snapshots: Vec<i64> = result
        .metadata
        .snapshots()
        .map(|s| s.snapshot_id())
        .collect();

    assert_eq!(snapshots.len(), 3);
    assert!(snapshots.contains(&1));
    assert!(snapshots.contains(&2));
    assert!(snapshots.contains(&3));

    // Verify snapshot details - check manifest lists and that timestamps are reasonable
    let snapshot1 = result.metadata.snapshot_by_id(1).unwrap();
    assert!(snapshot1.timestamp_ms() > 0);
    assert_eq!(snapshot1.manifest_list(), "/path/to/manifest1.avro");

    let snapshot2 = result.metadata.snapshot_by_id(2).unwrap();
    assert!(snapshot2.timestamp_ms() > snapshot1.timestamp_ms());
    assert_eq!(snapshot2.manifest_list(), "/path/to/manifest2.avro");

    let snapshot3 = result.metadata.snapshot_by_id(3).unwrap();
    assert!(snapshot3.timestamp_ms() > snapshot2.timestamp_ms());
    assert_eq!(snapshot3.manifest_list(), "/path/to/manifest3.avro");
}

#[sqlx::test]
async fn test_load_table_snapshots_filter_refs(pool: PgPool) {
    let (ctx, ns_params, table_ident, _) = setup_table_with_snapshots(pool).await;

    let table_params = TableParameters {
        prefix: ns_params.prefix.clone(),
        table: table_ident.clone(),
    };

    // Test with SnapshotsQuery::Refs - should return only snapshots referenced by branches
    let filters = LoadTableFilters {
        snapshots: SnapshotsQuery::Refs,
    };

    let result = CatalogServer::load_table(
        table_params,
        LoadTableRequest::builder().filters(filters).build(),
        ctx,
        random_request_metadata(),
    )
    .await
    .unwrap();

    let LoadTableResultOrNotModified::LoadTableResult(result) = result else {
        panic!("Expected LoadTableResult");
    };

    // Verify that only referenced snapshots are present (2 and 3)
    // Snapshot 1 should be filtered out as it's not referenced by any branch
    let snapshots: Vec<i64> = result
        .metadata
        .snapshots()
        .map(|s| s.snapshot_id())
        .collect();

    assert_eq!(snapshots.len(), 2);
    assert!(!snapshots.contains(&1)); // Snapshot 1 should be filtered out
    assert!(snapshots.contains(&2)); // Referenced by "main" branch
    assert!(snapshots.contains(&3)); // Referenced by "test_branch"

    // Verify snapshot details for referenced snapshots
    let snapshot2 = result.metadata.snapshot_by_id(2).unwrap();
    assert!(snapshot2.timestamp_ms() > 0);
    assert_eq!(snapshot2.manifest_list(), "/path/to/manifest2.avro");

    let snapshot3 = result.metadata.snapshot_by_id(3).unwrap();
    assert!(snapshot3.timestamp_ms() > snapshot2.timestamp_ms());
    assert_eq!(snapshot3.manifest_list(), "/path/to/manifest3.avro");

    // Verify that snapshot 1 is not present
    assert!(result.metadata.snapshot_by_id(1).is_none());
}

#[sqlx::test]
async fn test_load_table_snapshots_filter_default_behavior(pool: PgPool) {
    let (ctx, ns_params, table_ident, _) = setup_table_with_snapshots(pool).await;

    let table_params = TableParameters {
        prefix: ns_params.prefix.clone(),
        table: table_ident.clone(),
    };

    // Test with default LoadTableFilters (should use SnapshotsQuery::All by default)
    let result = CatalogServer::load_table(
        table_params,
        LoadTableRequest::builder().build(),
        ctx,
        random_request_metadata(),
    )
    .await
    .unwrap();

    let LoadTableResultOrNotModified::LoadTableResult(result) = result else {
        panic!("Expected LoadTableResult");
    };

    // Verify that all snapshots are present by default
    let snapshots: Vec<i64> = result
        .metadata
        .snapshots()
        .map(|s| s.snapshot_id())
        .collect();

    assert_eq!(snapshots.len(), 3);
    assert!(snapshots.contains(&1));
    assert!(snapshots.contains(&2));
    assert!(snapshots.contains(&3));
}

#[sqlx::test]
async fn test_load_table_snapshots_filter_with_no_refs(pool: PgPool) {
    let prof = lakekeeper_integration_tests::memory_io_profile();
    let (ctx, warehouse) = setup_simple(
        pool,
        prof,
        None,
        AllowAllAuthorizer::default(),
        TabularDeleteProfile::Hard {},
        None,
    )
    .await;

    // Create namespace
    let ns_name = NamespaceIdent::new("test_namespace_no_refs".to_string());
    let ns_params = NamespaceParameters {
        namespace: ns_name.clone(),
        prefix: Some(warehouse.warehouse_id.to_string().into()),
    };

    let _ = CatalogServer::create_namespace(
        ns_params.prefix.clone(),
        lakekeeper::api::iceberg::v1::CreateNamespaceRequest {
            namespace: ns_name.clone(),
            properties: None,
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    // Create table
    let table_ident = TableIdent::new(ns_name, "test_table_no_refs".to_string());
    let _table = CatalogServer::create_table(
        ns_params.clone(),
        create_table_request("test_table_no_refs"),
        DataAccess::not_specified(),
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    let table_params = TableParameters {
        prefix: Some(warehouse.warehouse_id.to_string().into()),
        table: table_ident.clone(),
    };

    // Add a snapshot but don't create any references
    let base_time = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap();

    let snapshot1 = Snapshot::builder()
        .with_snapshot_id(1)
        .with_timestamp_ms(base_time + 1000)
        .with_sequence_number(1)
        .with_manifest_list("/path/to/manifest1.avro")
        .with_summary(Summary {
            operation: Operation::Append,
            additional_properties: HashMap::new(),
        })
        .with_schema_id(0)
        .build();

    let commit_request = iceberg_ext::catalog::rest::CommitTableRequest {
        identifier: Some(table_ident.clone()),
        requirements: vec![],
        updates: vec![TableUpdate::AddSnapshot {
            snapshot: snapshot1,
        }],
    };

    CatalogServer::commit_table(
        table_params.clone(),
        commit_request,
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    // Test with SnapshotsQuery::Refs - should return no snapshots since there are no refs
    let filters = LoadTableFilters {
        snapshots: SnapshotsQuery::Refs,
    };

    let result = CatalogServer::load_table(
        table_params.clone(),
        LoadTableRequest::builder().filters(filters).build(),
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    let LoadTableResultOrNotModified::LoadTableResult(result) = result else {
        panic!("Expected LoadTableResult");
    };

    // Verify that no snapshots are returned when using Refs filter with no references
    let snapshots: Vec<i64> = result
        .metadata
        .snapshots()
        .map(|s| s.snapshot_id())
        .collect();

    assert_eq!(snapshots.len(), 0);

    // Test with SnapshotsQuery::All - should return all snapshots
    let filters_all = LoadTableFilters {
        snapshots: SnapshotsQuery::All,
    };

    let result_all = CatalogServer::load_table(
        table_params,
        LoadTableRequest::builder().filters(filters_all).build(),
        ctx,
        random_request_metadata(),
    )
    .await
    .unwrap();

    let LoadTableResultOrNotModified::LoadTableResult(result_all) = result_all else {
        panic!("Expected LoadTableResult");
    };

    // Verify that all snapshots are returned with All filter
    let snapshots_all: Vec<i64> = result_all
        .metadata
        .snapshots()
        .map(|s| s.snapshot_id())
        .collect();

    assert_eq!(snapshots_all.len(), 1);
    assert!(snapshots_all.contains(&1));
}

#[sqlx::test]
async fn test_load_table_snapshots_filter_behavior_difference(pool: PgPool) {
    let (ctx, ns_params, table_ident, _) = setup_table_with_snapshots(pool).await;

    let table_params = TableParameters {
        prefix: ns_params.prefix.clone(),
        table: table_ident.clone(),
    };

    // Test both filter types on the same table to verify behavior difference
    let filters_all = LoadTableFilters {
        snapshots: SnapshotsQuery::All,
    };

    let filters_refs = LoadTableFilters {
        snapshots: SnapshotsQuery::Refs,
    };

    let result_all = CatalogServer::load_table(
        table_params.clone(),
        LoadTableRequest::builder().filters(filters_all).build(),
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    let LoadTableResultOrNotModified::LoadTableResult(result_all) = result_all else {
        panic!("Expected LoadTableResult");
    };

    let result_refs = CatalogServer::load_table(
        table_params,
        LoadTableRequest::builder().filters(filters_refs).build(),
        ctx,
        random_request_metadata(),
    )
    .await
    .unwrap();

    let LoadTableResultOrNotModified::LoadTableResult(result_refs) = result_refs else {
        panic!("Expected LoadTableResult");
    };

    let snapshots_all: Vec<i64> = result_all
        .metadata
        .snapshots()
        .map(|s| s.snapshot_id())
        .collect();

    let snapshots_refs: Vec<i64> = result_refs
        .metadata
        .snapshots()
        .map(|s| s.snapshot_id())
        .collect();

    // Verify the behavior difference
    assert_eq!(snapshots_all.len(), 3); // All snapshots
    assert_eq!(snapshots_refs.len(), 2); // Only referenced snapshots

    // Verify specific differences
    assert!(snapshots_all.contains(&1)); // Unreferenced snapshot present in All
    assert!(!snapshots_refs.contains(&1)); // Unreferenced snapshot filtered out in Refs

    // Both should contain referenced snapshots
    assert!(snapshots_all.contains(&2) && snapshots_refs.contains(&2));
    assert!(snapshots_all.contains(&3) && snapshots_refs.contains(&3));

    // Verify that the difference is exactly the unreferenced snapshot
    let diff: Vec<i64> = snapshots_all
        .iter()
        .filter(|id| !snapshots_refs.contains(id))
        .copied()
        .collect();

    assert_eq!(diff, vec![1]); // Only snapshot 1 should be filtered out
}

#[sqlx::test]
async fn test_load_table_returns_not_modified_with_single_matching_etag(pool: PgPool) {
    let (api_context, namespace_parameters, table_identifier, table) =
        setup_simple_table(pool).await;
    let parameters = TableParameters {
        prefix: namespace_parameters.prefix.clone(),
        table: table_identifier.clone(),
    };

    let request_metadata = random_request_metadata();

    // Taken from the response `create_table` produced, so these also pin that
    // handler's ETag shape against what a default load matches.
    let etag = table.etag().expect("created table should carry an ETag");
    let etags = vec![as_client_etag(&etag)];
    let load_table_result = Box::pin(load_table(
        parameters,
        LoadTableRequest::builder().etags(etags).build(),
        api_context,
        request_metadata,
    ))
    .await;
    let Ok(result) = load_table_result else {
        panic!("Dummy table could not be loaded");
    };
    assert_eq!(
        result,
        LoadTableResultOrNotModified::NotModifiedResponse(etag)
    );
}

#[sqlx::test]
async fn test_load_table_returns_not_modified_when_given_multiple_etags_and_one_matches(
    pool: PgPool,
) {
    let (api_context, namespace_parameters, table_identifier, table) =
        setup_simple_table(pool).await;
    let parameters = TableParameters {
        prefix: namespace_parameters.prefix.clone(),
        table: table_identifier.clone(),
    };

    let request_metadata = random_request_metadata();

    // Taken from the response `create_table` produced, so these also pin that
    // handler's ETag shape against what a default load matches.
    let etag = table.etag().expect("created table should carry an ETag");
    let etags = vec![
        "a4b2f6c1dd87".into(),
        as_client_etag(&etag),
        "b6f8c2d4a45f".into(),
    ];
    let load_table_result = Box::pin(load_table(
        parameters,
        LoadTableRequest::builder().etags(etags).build(),
        api_context,
        request_metadata,
    ))
    .await;
    let Ok(result) = load_table_result else {
        panic!("Dummy table could not be loaded");
    };
    assert_eq!(
        result,
        LoadTableResultOrNotModified::NotModifiedResponse(etag)
    );
}

#[sqlx::test]
async fn test_load_table_returns_not_modified_when_given_wildcard(pool: PgPool) {
    let (api_context, namespace_parameters, table_identifier, table) =
        setup_simple_table(pool).await;
    let parameters = TableParameters {
        prefix: namespace_parameters.prefix.clone(),
        table: table_identifier.clone(),
    };

    let request_metadata = random_request_metadata();

    // Taken from the response `create_table` produced, so these also pin that
    // handler's ETag shape against what a default load matches.
    let etag = table.etag().expect("created table should carry an ETag");
    let etags = vec!["*".into()];
    let load_table_result = Box::pin(load_table(
        parameters,
        LoadTableRequest::builder().etags(etags).build(),
        api_context,
        request_metadata,
    ))
    .await;
    let Ok(result) = load_table_result else {
        panic!("Dummy table could not be loaded");
    };
    assert_eq!(
        result,
        LoadTableResultOrNotModified::NotModifiedResponse(etag)
    );
}

/// A conditional load must not be answered across snapshot filters.
///
/// Drives the `load_table` handler (not the HTTP layer): load with
/// `snapshots=refs`, which drops the unreferenced snapshot 1, keep the returned
/// ETag, then revalidate asking for `snapshots=all`. That must return a full
/// body, not a 304 — otherwise the client keeps using the truncated snapshot
/// list as if it were complete.
#[sqlx::test]
async fn test_load_table_does_not_return_not_modified_across_snapshot_filters(pool: PgPool) {
    let (api_context, namespace_parameters, table_identifier, _) =
        setup_table_with_snapshots(pool).await;
    let parameters = TableParameters {
        prefix: namespace_parameters.prefix.clone(),
        table: table_identifier.clone(),
    };

    // Load with `refs` and keep the ETag the server handed out.
    let refs_result = Box::pin(load_table(
        parameters.clone(),
        LoadTableRequest::builder()
            .filters(LoadTableFilters {
                snapshots: SnapshotsQuery::Refs,
            })
            .build(),
        api_context.clone(),
        random_request_metadata(),
    ))
    .await
    .expect("refs load failed");

    let LoadTableResultOrNotModified::LoadTableResult(refs_body) = refs_result else {
        panic!("expected a full response for the priming load");
    };
    // Precondition: `refs` really did truncate the snapshot list.
    let refs_snapshots: Vec<i64> = refs_body
        .metadata
        .snapshots()
        .map(|s| s.snapshot_id())
        .collect();
    assert!(
        !refs_snapshots.contains(&1),
        "precondition failed: `refs` should have dropped the unreferenced snapshot"
    );
    let refs_etag = refs_body
        .etag()
        .expect("refs response should carry an ETag");

    // Revalidate for `all` with the `refs` ETag -> must NOT be a 304.
    let all_result = Box::pin(load_table(
        parameters.clone(),
        LoadTableRequest::builder()
            .filters(LoadTableFilters {
                snapshots: SnapshotsQuery::All,
            })
            .etags(vec![as_client_etag(&refs_etag)])
            .build(),
        api_context.clone(),
        random_request_metadata(),
    ))
    .await
    .expect("all load failed");

    let LoadTableResultOrNotModified::LoadTableResult(all_body) = all_result else {
        panic!(
            "served 304 for `snapshots=all` from a `snapshots=refs` ETag: \
             the client would reuse a truncated snapshot list"
        );
    };
    let all_snapshots: Vec<i64> = all_body
        .metadata
        .snapshots()
        .map(|s| s.snapshot_id())
        .collect();
    assert!(
        all_snapshots.contains(&1),
        "the `all` response must carry the unreferenced snapshot"
    );

    // The same ETag must still 304 against its own representation, so the fix
    // narrows conditional requests rather than disabling them.
    let refs_again = Box::pin(load_table(
        parameters,
        LoadTableRequest::builder()
            .filters(LoadTableFilters {
                snapshots: SnapshotsQuery::Refs,
            })
            .etags(vec![as_client_etag(&refs_etag)])
            .build(),
        api_context,
        random_request_metadata(),
    ))
    .await
    .expect("refs revalidation failed");
    assert_eq!(
        refs_again,
        LoadTableResultOrNotModified::NotModifiedResponse(refs_etag),
        "a `refs` ETag must still 304 a `refs` request"
    );
}

/// A conditional load must not be answered across access-delegation modes.
///
/// Drives the `load_table` handler on the in-memory profile, which vends no
/// expiring credentials — so the `vends_credentials` gate never fires and the
/// response shape is the only thing preventing the 304.
#[sqlx::test]
async fn test_load_table_does_not_return_not_modified_across_delegation_modes(pool: PgPool) {
    let (api_context, namespace_parameters, table_identifier, _) = setup_simple_table(pool).await;
    let parameters = TableParameters {
        prefix: namespace_parameters.prefix.clone(),
        table: table_identifier.clone(),
    };
    let vended: DataAccessMode = DataAccess {
        vended_credentials: true,
        remote_signing: false,
    }
    .into();
    let signing: DataAccessMode = DataAccess {
        vended_credentials: false,
        remote_signing: true,
    }
    .into();

    let load = |data_access: DataAccessMode, etags: Vec<ETag>| {
        let parameters = parameters.clone();
        let api_context = api_context.clone();
        async move {
            Box::pin(load_table(
                parameters,
                LoadTableRequest::builder()
                    .data_access(data_access)
                    .etags(etags)
                    .build(),
                api_context,
                random_request_metadata(),
            ))
            .await
            .expect("load failed")
        }
    };

    // Prime a cache entry under vended-credentials.
    let LoadTableResultOrNotModified::LoadTableResult(body) = load(vended, vec![]).await else {
        panic!("expected a full response for the priming load");
    };
    let etag = body.etag().expect("response should carry an ETag");
    let echoed = vec![as_client_etag(&etag)];

    // Revalidating under a different delegation must not 304.
    assert!(
        matches!(
            load(signing, echoed.clone()).await,
            LoadTableResultOrNotModified::LoadTableResult(_)
        ),
        "304'd a remote-signing request from a vended-credentials ETag: the client \
         would reuse storage config it did not request"
    );

    // The same tag still 304s its own delegation, so conditional requests keep working.
    assert_eq!(
        load(vended, echoed).await,
        LoadTableResultOrNotModified::NotModifiedResponse(etag),
        "a vended-credentials ETag must still 304 a vended-credentials request"
    );
}
