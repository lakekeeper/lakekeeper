// Extracted from crates/lakekeeper/src/server/tables.rs (VAK-437 split).
// Original location was `#[cfg(any())] pub(crate) mod test`.

use std::{collections::HashMap, sync::Arc};

use http::StatusCode;
use iceberg::{
    NamespaceIdent, TableIdent, TableUpdate,
    spec::{
        EncryptedKey, FormatVersion, MAIN_BRANCH, NestedField, Operation, PrimitiveType, Schema,
        Snapshot, SnapshotReference, SnapshotRetention, Summary, TableMetadata, TableProperties,
        Transform, Type, UnboundPartitionField, UnboundPartitionSpec,
    },
};
use iceberg_ext::catalog::rest::{
    CommitTableRequest, CommitTransactionRequest, CreateNamespaceResponse, CreateTableRequest,
    LoadTableResult, RenameTableRequest,
};
use itertools::Itertools;
use lakekeeper::{
    WarehouseId,
    api::{
        ApiContext, RequestMetadata,
        iceberg::{
            types::{PageToken, Prefix},
            v1::{
                DataAccess, DropParams, ListTablesQuery, LoadTableResultOrNotModified,
                NamespaceParameters, TableParameters,
                tables::{LoadTableRequest, TablesService as _},
            },
        },
        management::v1::{
            ApiServer as ManagementApiServer, table::TableManagementService,
            warehouse::TabularDeleteProfile,
        },
    },
    server::{
        CatalogServer,
        tables::{CommitContext, commit_tables_with_authz},
    },
    service::{
        CatalogStore, CatalogTabularOps, SecretStore, State, TableId, TabularListFlags, UserId,
        authz::{AllowAllAuthorizer, CatalogTableAction, tests::HidingAuthorizer},
    },
};
use lakekeeper_integration_tests::{
    create_ns, create_table_request as create_request, impl_pagination_tests, memory_io_profile,
    setup_simple, tabular_test_multi_warehouse_setup,
};
use lakekeeper_storage_postgres::{
    PostgresBackend, SecretsState, tabular::table::tests::initialize_table,
    test_utils::random_request_metadata,
};
use sqlx::PgPool;
use uuid::Uuid;
fn partition_spec() -> UnboundPartitionSpec {
    UnboundPartitionSpec::builder()
        .with_spec_id(0)
        .add_partition_field(2, "y", Transform::Identity)
        .unwrap()
        .build()
}

// Helper functions to reduce repetitive code in tests

/// Creates a standard test schema with id and name fields
fn create_test_schema() -> Schema {
    Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", iceberg::spec::Type::Primitive(PrimitiveType::Int))
                .into(),
            NestedField::required(
                2,
                "name",
                iceberg::spec::Type::Primitive(PrimitiveType::String),
            )
            .into(),
        ])
        .build()
        .unwrap()
}

/// Creates a `CreateTableRequest` with the given name and format version.
/// Named to disambiguate from `create_request` (the imported helper, which
/// can't set a format version).
fn create_table_request_with_format(
    name: &str,
    format_version: Option<FormatVersion>,
) -> CreateTableRequest {
    let mut properties = None;
    if let Some(version) = format_version {
        properties = Some(HashMap::from([(
            TableProperties::PROPERTY_FORMAT_VERSION.to_string(),
            match version {
                FormatVersion::V1 => "1".to_string(),
                FormatVersion::V2 => "2".to_string(),
                FormatVersion::V3 => "3".to_string(),
            },
        )]));
    }

    CreateTableRequest {
        name: name.to_string(),
        location: None,
        schema: create_test_schema(),
        partition_spec: Some(UnboundPartitionSpec::builder().build()),
        write_order: None,
        stage_create: Some(false),
        properties,
    }
}

/// Helper to load a table using `CatalogServer`
async fn load_table(
    ctx: &ApiContext<
        State<impl lakekeeper::service::authz::Authorizer, impl CatalogStore, impl SecretStore>,
    >,
    ns_params: &NamespaceParameters,
    table_name: &str,
) -> LoadTableResult {
    let table_ident = TableIdent {
        namespace: ns_params.namespace.clone(),
        name: table_name.to_string(),
    };

    let load_table_result = CatalogServer::load_table(
        TableParameters {
            prefix: ns_params.prefix.clone(),
            table: table_ident,
        },
        LoadTableRequest::builder().build(),
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    let LoadTableResultOrNotModified::LoadTableResult(load_table_result) = load_table_result else {
        panic!("Expected LoadTableResult, got NotModified");
    };
    load_table_result
}

/// Helper to commit table changes
async fn commit_table_changes(
    ctx: &ApiContext<
        State<impl lakekeeper::service::authz::Authorizer, impl CatalogStore, impl SecretStore>,
    >,
    ns_params: &NamespaceParameters,
    table_ident: &TableIdent,
    updates: Vec<TableUpdate>,
) -> CommitContext {
    Arc::unwrap_or_clone(
        commit_tables_with_authz(
            ns_params.prefix.clone(),
            CommitTransactionRequest {
                table_changes: vec![CommitTableRequest {
                    identifier: Some(table_ident.clone()),
                    requirements: vec![],
                    updates,
                }],
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
            None,
        )
        .await
        .unwrap()
        .unwrap_committed(),
    )
    .into_iter()
    .next()
    .unwrap()
}

/// Helper to create a standard snapshot for testing
fn create_test_snapshot_v3(
    snapshot_id: i64,
    timestamp_ms: i64,
    sequence_number: i64,
    manifest_list: &str,
    row_range: Option<(u64, u64)>,
    added_records: u64,
    key_id: &str,
) -> Snapshot {
    let base_builder = Snapshot::builder()
        .with_snapshot_id(snapshot_id)
        .with_timestamp_ms(timestamp_ms)
        .with_sequence_number(sequence_number)
        .with_schema_id(0)
        .with_manifest_list(manifest_list)
        .with_encryption_key_id(Some(key_id.to_string()))
        .with_summary(Summary {
            operation: Operation::Append,
            additional_properties: HashMap::from_iter(vec![
                ("added-data-files".to_string(), "1".to_string()),
                ("added-records".to_string(), added_records.to_string()),
            ]),
        });

    if let Some((first_row_id, added_rows_count)) = row_range {
        base_builder
            .with_row_range(first_row_id, added_rows_count)
            .build()
    } else {
        base_builder.build()
    }
}

/// Helper to create a snapshot reference
fn create_snapshot_reference(snapshot_id: i64) -> SnapshotReference {
    SnapshotReference {
        snapshot_id,
        retention: SnapshotRetention::Branch {
            min_snapshots_to_keep: Some(10),
            max_snapshot_age_ms: None,
            max_ref_age_ms: None,
        },
    }
}

#[sqlx::test]
async fn test_set_properties_commit_table(pool: sqlx::PgPool) {
    let (ctx, ns, ns_params, table) = commit_test_setup(pool).await;

    let table_metadata = (*table.metadata)
        .clone()
        .into_builder(table.metadata_location)
        .set_properties(HashMap::from([
            ("p1".into(), "v2".into()),
            ("p2".into(), "v2".into()),
        ]))
        .unwrap()
        .build()
        .unwrap();
    let updates = table_metadata.changes;
    let _ = Arc::unwrap_or_clone(
        commit_tables_with_authz(
            ns_params.prefix.clone(),
            CommitTransactionRequest {
                table_changes: vec![CommitTableRequest {
                    identifier: Some(TableIdent {
                        namespace: ns.namespace.clone(),
                        name: "tab-1".to_string(),
                    }),
                    requirements: vec![],
                    updates,
                }],
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
            None,
        )
        .await
        .unwrap()
        .unwrap_committed(),
    )
    .into_iter()
    .next()
    .unwrap()
    .new_metadata;

    let tab = CatalogServer::load_table(
        TableParameters {
            prefix: ns_params.prefix,
            table: TableIdent {
                namespace: ns.namespace.clone(),
                name: "tab-1".to_string(),
            },
        },
        LoadTableRequest::builder().build(),
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    let LoadTableResultOrNotModified::LoadTableResult(tab) = tab else {
        panic!("Expected LoadTableResult, got NotModified");
    };

    assert_table_metadata_are_equal(&table_metadata.metadata, &tab.metadata);
}

fn schema() -> Schema {
    Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "x", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(2, "y", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(3, "z", Type::Primitive(PrimitiveType::Long)).into(),
        ])
        .build()
        .unwrap()
}

fn assert_table_metadata_are_equal(expected: &TableMetadata, actual: &TableMetadata) {
    assert_eq!(actual.location(), expected.location());
    assert_eq!(actual.properties(), expected.properties());
    assert_eq!(
        actual
            .snapshots()
            .sorted_by_key(|s| s.snapshot_id())
            .collect_vec(),
        expected
            .snapshots()
            .sorted_by_key(|s| s.snapshot_id())
            .collect_vec()
    );
    assert_eq!(
        actual
            .partition_specs_iter()
            .sorted_by_key(|ps| ps.spec_id())
            .collect_vec(),
        expected
            .partition_specs_iter()
            .sorted_by_key(|ps| ps.spec_id())
            .collect_vec()
    );
    assert_eq!(
        actual
            .partition_statistics_iter()
            .sorted_by_key(|s| (s.snapshot_id, &s.statistics_path))
            .collect_vec(),
        expected
            .partition_statistics_iter()
            .sorted_by_key(|s| (s.snapshot_id, &s.statistics_path))
            .collect_vec()
    );
    assert_eq!(
        actual
            .sort_orders_iter()
            .sorted_by_key(|s| s.order_id)
            .collect_vec(),
        expected
            .sort_orders_iter()
            .sorted_by_key(|s| s.order_id)
            .collect_vec()
    );
    assert_eq!(
        actual
            .statistics_iter()
            .sorted_by_key(|s| (s.snapshot_id, &s.statistics_path))
            .collect_vec(),
        expected
            .statistics_iter()
            .sorted_by_key(|s| (s.snapshot_id, &s.statistics_path))
            .collect_vec()
    );
    assert_eq!(actual.history(), expected.history());
    assert_eq!(actual.current_schema_id(), expected.current_schema_id());
    assert_eq!(actual.current_snapshot_id(), expected.current_snapshot_id());
    assert_eq!(
        actual.default_partition_spec(),
        expected.default_partition_spec()
    );
    assert_eq!(actual.default_sort_order(), expected.default_sort_order());
    assert_eq!(actual.format_version(), expected.format_version());
    assert_eq!(actual.last_column_id(), expected.last_column_id());
    assert_eq!(
        actual.last_sequence_number(),
        expected.last_sequence_number()
    );
    assert_eq!(actual.last_partition_id(), expected.last_partition_id());
}

#[sqlx::test]
async fn test_add_partition_spec_commit_table(pool: sqlx::PgPool) {
    let (ctx, ns, ns_params, table) = commit_test_setup(pool).await;

    let added_spec = UnboundPartitionSpec::builder()
        .with_spec_id(10)
        .add_partition_fields(vec![
            UnboundPartitionField {
                // The previous field - has field_id set
                name: "y".to_string(),
                transform: Transform::Identity,
                source_id: 2,
                field_id: Some(1000),
            },
            UnboundPartitionField {
                // A new field without field id - should still be without field id in changes
                name: "z".to_string(),
                transform: Transform::Identity,
                source_id: 3,
                field_id: None,
            },
        ])
        .unwrap()
        .build();

    let table_metadata = (*table.metadata)
        .clone()
        .into_builder(table.metadata_location)
        .add_schema(schema())
        .unwrap()
        .set_current_schema(-1)
        .unwrap()
        .add_partition_spec(partition_spec())
        .unwrap()
        .add_partition_spec(added_spec.clone())
        .unwrap()
        .build()
        .unwrap();

    let updates = table_metadata.changes;
    let _ = commit_tables_with_authz(
        ns_params.prefix.clone(),
        CommitTransactionRequest {
            table_changes: vec![CommitTableRequest {
                identifier: Some(TableIdent {
                    namespace: ns.namespace.clone(),
                    name: "tab-1".to_string(),
                }),
                requirements: vec![],
                updates,
            }],
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
        None,
    )
    .await
    .unwrap();

    let tab = CatalogServer::load_table(
        TableParameters {
            prefix: ns_params.prefix,
            table: TableIdent {
                namespace: ns.namespace.clone(),
                name: "tab-1".to_string(),
            },
        },
        LoadTableRequest::builder().build(),
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    let LoadTableResultOrNotModified::LoadTableResult(tab) = tab else {
        panic!("Expected LoadTableResult, got NotModified");
    };

    assert_table_metadata_are_equal(&table_metadata.metadata, &tab.metadata);
}

#[sqlx::test]
async fn test_set_default_partition_spec(pool: PgPool) {
    let (ctx, ns, ns_params, table) = commit_test_setup(pool).await;

    let added_spec = UnboundPartitionSpec::builder()
        .with_spec_id(10)
        .add_partition_field(1, "y_bucket[2]", Transform::Bucket(2))
        .unwrap()
        .build();

    let table_metadata = (*table.metadata)
        .clone()
        .into_builder(table.metadata_location)
        .add_partition_spec(added_spec)
        .unwrap()
        .set_default_partition_spec(-1)
        .unwrap()
        .build()
        .unwrap();
    let updates = table_metadata.changes;

    let _ = Arc::unwrap_or_clone(
        commit_tables_with_authz(
            ns_params.prefix.clone(),
            CommitTransactionRequest {
                table_changes: vec![CommitTableRequest {
                    identifier: Some(TableIdent {
                        namespace: ns.namespace.clone(),
                        name: "tab-1".to_string(),
                    }),
                    requirements: vec![],
                    updates,
                }],
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
            None,
        )
        .await
        .unwrap()
        .unwrap_committed(),
    )
    .into_iter()
    .next()
    .unwrap()
    .new_metadata;

    let tab = CatalogServer::load_table(
        TableParameters {
            prefix: ns_params.prefix,
            table: TableIdent {
                namespace: ns.namespace.clone(),
                name: "tab-1".to_string(),
            },
        },
        LoadTableRequest::builder().build(),
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    let LoadTableResultOrNotModified::LoadTableResult(tab) = tab else {
        panic!("Expected LoadTableResult, got NotModified");
    };

    assert_table_metadata_are_equal(&table_metadata.metadata, &tab.metadata);
}

#[sqlx::test]
async fn test_set_ref(pool: PgPool) {
    let (ctx, ns, ns_params, table) = commit_test_setup(pool).await;
    let last_updated = table.metadata.last_updated_ms();
    let builder = (*table.metadata)
        .clone()
        .into_builder(table.metadata_location);

    let snapshot = Snapshot::builder()
        .with_snapshot_id(1)
        .with_timestamp_ms(last_updated + 1)
        .with_sequence_number(0)
        .with_schema_id(0)
        .with_manifest_list("/snap-1.avro")
        .with_summary(Summary {
            operation: Operation::Append,
            additional_properties: HashMap::from_iter(vec![
                (
                    "spark.app.id".to_string(),
                    "local-1662532784305".to_string(),
                ),
                ("added-data-files".to_string(), "4".to_string()),
                ("added-records".to_string(), "4".to_string()),
                ("added-files-size".to_string(), "6001".to_string()),
            ]),
        })
        .build();

    let builder = builder
        .add_snapshot(snapshot.clone())
        .unwrap()
        .set_ref(
            MAIN_BRANCH,
            SnapshotReference {
                snapshot_id: 1,
                retention: SnapshotRetention::Branch {
                    min_snapshots_to_keep: Some(10),
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                },
            },
        )
        .unwrap()
        .build()
        .unwrap();
    let updates = builder.changes;

    let _ = commit_tables_with_authz(
        ns_params.prefix.clone(),
        CommitTransactionRequest {
            table_changes: vec![CommitTableRequest {
                identifier: Some(TableIdent {
                    namespace: ns.namespace.clone(),
                    name: "tab-1".to_string(),
                }),
                requirements: vec![],
                updates,
            }],
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
        None,
    )
    .await
    .unwrap();

    let tab = CatalogServer::load_table(
        TableParameters {
            prefix: ns_params.prefix.clone(),
            table: TableIdent {
                namespace: ns.namespace.clone(),
                name: "tab-1".to_string(),
            },
        },
        LoadTableRequest::builder().build(),
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    let LoadTableResultOrNotModified::LoadTableResult(tab) = tab else {
        panic!("Expected LoadTableResult, got NotModified");
    };

    assert_eq!(&*tab.metadata, &builder.metadata);
}

#[sqlx::test]
async fn test_expire_metadata_log(pool: PgPool) {
    let (ctx, ns, ns_params, table) = commit_test_setup(pool).await;
    let table_ident = TableIdent {
        namespace: ns.namespace.clone(),
        name: "tab-1".to_string(),
    };
    let builder = (*table.metadata)
        .clone()
        .into_builder(table.metadata_location)
        .set_properties(HashMap::from_iter([(
            TableProperties::PROPERTY_METADATA_PREVIOUS_VERSIONS_MAX.to_string(),
            "2".to_string(),
        )]))
        .unwrap()
        .build()
        .unwrap();
    let _ = commit_tables_with_authz(
        ns_params.prefix.clone(),
        CommitTransactionRequest {
            table_changes: vec![CommitTableRequest {
                identifier: Some(table_ident.clone()),
                requirements: vec![],
                updates: builder.changes,
            }],
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
        None,
    )
    .await
    .unwrap();

    let tab = CatalogServer::load_table(
        TableParameters {
            prefix: ns_params.prefix.clone(),
            table: table_ident.clone(),
        },
        LoadTableRequest::builder().build(),
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    let LoadTableResultOrNotModified::LoadTableResult(tab) = tab else {
        panic!("Expected LoadTableResult, got NotModified");
    };

    assert_table_metadata_are_equal(&builder.metadata, &tab.metadata);

    let builder = builder
        .metadata
        .into_builder(tab.metadata_location)
        .set_properties(HashMap::from_iter(vec![(
            "change_nr".to_string(),
            "1".to_string(),
        )]))
        .unwrap()
        .build()
        .unwrap();

    let committed = Arc::unwrap_or_clone(
        commit_tables_with_authz(
            ns_params.prefix.clone(),
            CommitTransactionRequest {
                table_changes: vec![CommitTableRequest {
                    identifier: Some(table_ident.clone()),
                    requirements: vec![],
                    updates: builder.changes,
                }],
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
            None,
        )
        .await
        .unwrap()
        .unwrap_committed(),
    )
    .into_iter()
    .next()
    .unwrap();

    let tab = CatalogServer::load_table(
        TableParameters {
            prefix: ns_params.prefix.clone(),
            table: table_ident.clone(),
        },
        LoadTableRequest::builder().build(),
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    let LoadTableResultOrNotModified::LoadTableResult(tab) = tab else {
        panic!("Expected LoadTableResult, got NotModified");
    };

    assert_table_metadata_are_equal(&builder.metadata, &tab.metadata);

    let builder = (*committed.new_metadata)
        .clone()
        .into_builder(tab.metadata_location)
        .set_properties(HashMap::from_iter(vec![(
            "change_nr".to_string(),
            "2".to_string(),
        )]))
        .unwrap()
        .build()
        .unwrap();

    let _ = Arc::unwrap_or_clone(
        commit_tables_with_authz(
            ns_params.prefix.clone(),
            CommitTransactionRequest {
                table_changes: vec![CommitTableRequest {
                    identifier: Some(table_ident.clone()),
                    requirements: vec![],
                    updates: builder.changes,
                }],
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
            None,
        )
        .await
        .unwrap()
        .unwrap_committed(),
    )
    .into_iter()
    .next()
    .unwrap();

    let tab = CatalogServer::load_table(
        TableParameters {
            prefix: ns_params.prefix,
            table: table_ident.clone(),
        },
        LoadTableRequest::builder().build(),
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    let LoadTableResultOrNotModified::LoadTableResult(tab) = tab else {
        panic!("Expected LoadTableResult, got NotModified");
    };

    assert_table_metadata_are_equal(&builder.metadata, &tab.metadata);
}

#[sqlx::test]
async fn test_default_format_version_is_v2(pg_pool: PgPool) {
    let (ctx, _ns, ns_params, _) = table_test_setup(pg_pool).await;
    let create_request = create_table_request_with_format("my_table", None);
    let table = CatalogServer::create_table(
        ns_params.clone(),
        create_request,
        DataAccess {
            vended_credentials: true,
            remote_signing: false,
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    assert_eq!(table.metadata.format_version(), FormatVersion::V2);
}

#[sqlx::test]
#[allow(clippy::too_many_lines)]
async fn test_table_v3(pg_pool: PgPool) {
    let (ctx, ns, ns_params, _) = table_test_setup(pg_pool).await;
    let create_request = create_table_request_with_format("my_table", Some(FormatVersion::V3));
    let table = CatalogServer::create_table(
        ns_params.clone(),
        create_request,
        DataAccess {
            vended_credentials: true,
            remote_signing: false,
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    assert_eq!(table.metadata.format_version(), FormatVersion::V3);
    assert_eq!(table.metadata.next_row_id(), 0);

    // Create table identifier for commits
    let table_ident = TableIdent {
        namespace: ns.namespace.clone(),
        name: "my_table".to_string(),
    };

    // Add a snapshot with row_range (0, 100)
    let last_updated = table.metadata.last_updated_ms();

    let snapshot1 = create_test_snapshot_v3(
        1,
        last_updated + 1,
        1,
        "/snap-1.avro",
        Some((0, 100)),
        100,
        "key-1",
    );

    // Commit using Catalog
    let encryption_key = EncryptedKey::builder()
        .key_id("key-1")
        .encrypted_key_metadata("key-metadata".as_bytes().to_vec())
        .encrypted_by_id("my-vault".to_string())
        .build();

    commit_table_changes(
        &ctx,
        &ns_params,
        &table_ident,
        vec![
            TableUpdate::AddSnapshot {
                snapshot: snapshot1,
            },
            TableUpdate::SetSnapshotRef {
                ref_name: MAIN_BRANCH.to_string(),
                reference: create_snapshot_reference(1),
            },
            TableUpdate::AddEncryptionKey {
                encryption_key: encryption_key.clone(),
            },
        ],
    )
    .await;

    // Load using Catalog and assert next_row_id = 100
    let loaded_table = load_table(&ctx, &ns_params, "my_table").await;
    assert_eq!(loaded_table.metadata.next_row_id(), 100);
    let current_snapshot = loaded_table
        .metadata
        .current_snapshot()
        .expect("There should be a current snapshot");
    assert_eq!(current_snapshot.snapshot_id(), 1);
    assert_eq!(current_snapshot.row_range(), Some((0, 100)));
    assert_eq!(
        loaded_table.metadata.encryption_key("key-1"),
        Some(&encryption_key)
    );
    assert_eq!(current_snapshot.encryption_key_id(), Some("key-1"));

    let snapshot2_invalid = create_test_snapshot_v3(
        2,
        last_updated + 2,
        2,
        "/snap-2-invalid.avro",
        Some((50, 100)),
        100,
        "key-1",
    );

    // This commit should fail due to row range overlap
    let invalid_commit_result = commit_tables_with_authz(
        ns_params.prefix.clone(),
        CommitTransactionRequest {
            table_changes: vec![CommitTableRequest {
                identifier: Some(table_ident.clone()),
                requirements: vec![],
                updates: vec![TableUpdate::AddSnapshot {
                    snapshot: snapshot2_invalid,
                }],
            }],
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
        None,
    )
    .await;

    // Assert that the commit fails
    assert!(invalid_commit_result.is_err());
    let err_string = invalid_commit_result.as_ref().unwrap_err().to_string();
    assert!(
        err_string.contains("first-row-id is behind table next-row-id"),
        "The error message `{err_string}` did not contain the expected text",
    );

    // Add another snapshot with row_range (100, 50) - this should succeed
    // because it doesn't overlap (rows 100-149)
    let loaded_table2 = load_table(&ctx, &ns_params, "my_table").await;

    assert_eq!(loaded_table2.metadata.next_row_id(), 100);
    assert_eq!(loaded_table2.metadata.format_version(), FormatVersion::V3);

    let snapshot3_valid = create_test_snapshot_v3(
        3,
        last_updated + 3,
        2,
        "/snap-3-valid.avro",
        Some((100, 50)), // first_row_id: 100, added_rows_count: 50
        50,              // added_records: 50
        "key-1",
    );

    // This commit should succeed
    commit_table_changes(
        &ctx,
        &ns_params,
        &table_ident,
        vec![TableUpdate::AddSnapshot {
            snapshot: snapshot3_valid,
        }],
    )
    .await;

    // Load again and check next_row_id should now be 150
    let final_table = load_table(&ctx, &ns_params, "my_table").await;

    assert_eq!(final_table.metadata.next_row_id(), 150);
    println!(
        "Available snapshot ids: {:?}",
        final_table
            .metadata
            .snapshots()
            .map(|s| s.snapshot_id())
            .collect::<Vec<_>>()
    );
    let snapshot = final_table.metadata.snapshot_by_id(3).unwrap();
    assert_eq!(snapshot.row_range(), Some((100, 50)));
    assert_eq!(snapshot.manifest_list(), "/snap-3-valid.avro");
}

#[sqlx::test]
async fn test_v2_to_v3_migration(pg_pool: PgPool) {
    let (ctx, ns, ns_params, _) = table_test_setup(pg_pool).await;

    // Create a v2 table (default version)
    let create_request = CreateTableRequest {
        name: "my_migration_table".to_string(),
        location: None,
        schema: Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", iceberg::spec::Type::Primitive(PrimitiveType::Int))
                    .into(),
                NestedField::required(
                    2,
                    "name",
                    iceberg::spec::Type::Primitive(PrimitiveType::String),
                )
                .into(),
            ])
            .build()
            .unwrap(),
        partition_spec: Some(UnboundPartitionSpec::builder().build()),
        write_order: None,
        stage_create: Some(false),
        properties: None, // No format version specified, should default to V2
    };

    let table = CatalogServer::create_table(
        ns_params.clone(),
        create_request,
        DataAccess {
            vended_credentials: true,
            remote_signing: false,
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    // Verify it's a V2 table
    assert_eq!(table.metadata.format_version(), FormatVersion::V2);

    // Create table identifier for commits
    let table_ident = TableIdent {
        namespace: ns.namespace.clone(),
        name: "my_migration_table".to_string(),
    };

    // Add a snapshot to the V2 table (without row_range, as V2 snapshots don't have it)
    let last_updated = table.metadata.last_updated_ms();
    let builder = (*table.metadata)
        .clone()
        .into_builder(table.metadata_location);

    let snapshot1 = Snapshot::builder()
        .with_snapshot_id(1)
        .with_timestamp_ms(last_updated + 1)
        .with_sequence_number(1)
        .with_schema_id(0)
        .with_manifest_list("/snap-1.avro")
        // No row_range for V2 table - this is the realistic scenario
        .with_summary(Summary {
            operation: Operation::Append,
            additional_properties: HashMap::from_iter(vec![
                ("added-data-files".to_string(), "1".to_string()),
                ("added-records".to_string(), "100".to_string()),
            ]),
        })
        .build();

    let builder = builder
        .add_snapshot(snapshot1)
        .unwrap()
        .set_ref(
            MAIN_BRANCH,
            SnapshotReference {
                snapshot_id: 1,
                retention: SnapshotRetention::Branch {
                    min_snapshots_to_keep: Some(10),
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                },
            },
        )
        .unwrap()
        .build()
        .unwrap();

    // Commit the snapshot
    commit_tables_with_authz(
        ns_params.prefix.clone(),
        CommitTransactionRequest {
            table_changes: vec![CommitTableRequest {
                identifier: Some(table_ident.clone()),
                requirements: vec![],
                updates: builder.changes,
            }],
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
        None,
    )
    .await
    .unwrap();

    // Load table and verify it's still V2
    let loaded_table_v2 = CatalogServer::load_table(
        TableParameters {
            prefix: ns_params.prefix.clone(),
            table: table_ident.clone(),
        },
        LoadTableRequest::builder().build(),
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    let LoadTableResultOrNotModified::LoadTableResult(loaded_table_v2) = loaded_table_v2 else {
        panic!("Expected LoadTableResult, got NotModified");
    };

    assert_eq!(loaded_table_v2.metadata.format_version(), FormatVersion::V2);

    // Upgrade to V3 using TableUpdate::UpgradeFormatVersion
    commit_tables_with_authz(
        ns_params.prefix.clone(),
        CommitTransactionRequest {
            table_changes: vec![CommitTableRequest {
                identifier: Some(table_ident.clone()),
                requirements: vec![],
                updates: vec![TableUpdate::UpgradeFormatVersion {
                    format_version: FormatVersion::V3,
                }],
            }],
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
        None,
    )
    .await
    .unwrap();

    // Load table again -> should be V3 and next_row_id should be 0 (NULL equivalent)
    let loaded_table_v3 = CatalogServer::load_table(
        TableParameters {
            prefix: ns_params.prefix.clone(),
            table: table_ident.clone(),
        },
        LoadTableRequest::builder().build(),
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    let LoadTableResultOrNotModified::LoadTableResult(loaded_table_v3) = loaded_table_v3 else {
        panic!("Expected LoadTableResult, got NotModified");
    };

    assert_eq!(loaded_table_v3.metadata.format_version(), FormatVersion::V3);
    assert_eq!(loaded_table_v3.metadata.next_row_id(), 0); // Should be 0 after migration

    // Add a snapshot with row_range to the V3 table
    let snapshot2 = Snapshot::builder()
        .with_snapshot_id(2)
        .with_timestamp_ms(last_updated + 2)
        .with_sequence_number(2)
        .with_schema_id(0)
        .with_manifest_list("/snap-2.avro")
        .with_row_range(0, 50) // first_row_id: 0, added_rows_count: 50
        .with_summary(Summary {
            operation: Operation::Append,
            additional_properties: HashMap::from_iter(vec![
                ("added-data-files".to_string(), "1".to_string()),
                ("added-records".to_string(), "50".to_string()),
            ]),
        })
        .build();

    commit_tables_with_authz(
        ns_params.prefix.clone(),
        CommitTransactionRequest {
            table_changes: vec![CommitTableRequest {
                identifier: Some(table_ident.clone()),
                requirements: vec![],
                updates: vec![TableUpdate::AddSnapshot {
                    snapshot: snapshot2,
                }],
            }],
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
        None,
    )
    .await
    .unwrap();

    // Load table -> next_row_id should now be increased to 50
    let final_table = CatalogServer::load_table(
        TableParameters {
            prefix: ns_params.prefix.clone(),
            table: table_ident.clone(),
        },
        LoadTableRequest::builder().build(),
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    let LoadTableResultOrNotModified::LoadTableResult(final_table) = final_table else {
        panic!("Expected LoadTableResult, got NotModified");
    };

    assert_eq!(final_table.metadata.format_version(), FormatVersion::V3);
    assert_eq!(final_table.metadata.next_row_id(), 50);
}

#[sqlx::test]
async fn test_remove_snapshot_commit(pg_pool: PgPool) {
    let (ctx, ns, ns_params, table) = commit_test_setup(pg_pool).await;
    let table_ident = TableIdent {
        namespace: ns.namespace.clone(),
        name: "tab-1".to_string(),
    };
    let last_updated = table.metadata.last_updated_ms();
    let builder = (*table.metadata)
        .clone()
        .into_builder(table.metadata_location);

    let snap = Snapshot::builder()
        .with_snapshot_id(1)
        .with_timestamp_ms(last_updated + 1)
        .with_sequence_number(0)
        .with_schema_id(0)
        .with_manifest_list("/snap-1.avro")
        .with_summary(Summary {
            operation: Operation::Append,
            additional_properties: HashMap::from_iter(vec![
                (
                    "spark.app.id".to_string(),
                    "local-1662532784305".to_string(),
                ),
                ("added-data-files".to_string(), "4".to_string()),
                ("added-records".to_string(), "4".to_string()),
                ("added-files-size".to_string(), "6001".to_string()),
            ]),
        })
        .build();

    let builder = builder
        .add_snapshot(snap)
        .unwrap()
        .set_ref(
            MAIN_BRANCH,
            SnapshotReference {
                snapshot_id: 1,
                retention: SnapshotRetention::Branch {
                    min_snapshots_to_keep: Some(10),
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                },
            },
        )
        .unwrap()
        .build()
        .unwrap();

    let _ = commit_tables_with_authz(
        ns_params.prefix.clone(),
        CommitTransactionRequest {
            table_changes: vec![CommitTableRequest {
                identifier: Some(table_ident.clone()),
                requirements: vec![],
                updates: builder.changes,
            }],
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
        None,
    )
    .await
    .unwrap();

    let tab = CatalogServer::load_table(
        TableParameters {
            prefix: ns_params.prefix.clone(),
            table: table_ident.clone(),
        },
        LoadTableRequest::builder().build(),
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    let LoadTableResultOrNotModified::LoadTableResult(tab) = tab else {
        panic!("Expected LoadTableResult, got NotModified");
    };

    assert_eq!(tab.metadata.history(), builder.metadata.history());
    assert_eq!(&*tab.metadata, &builder.metadata);

    assert_json_diff::assert_json_eq!(
        serde_json::to_value(tab.metadata.clone()).unwrap(),
        serde_json::to_value(builder.metadata.clone()).unwrap()
    );

    let last_updated = tab.metadata.last_updated_ms();
    let builder = builder.metadata.into_builder(tab.metadata_location);

    let snap = Snapshot::builder()
        .with_snapshot_id(2)
        .with_parent_snapshot_id(Some(1))
        .with_timestamp_ms(last_updated + 1)
        .with_sequence_number(1)
        .with_schema_id(0)
        .with_manifest_list("/snap-2.avro")
        .with_summary(Summary {
            operation: Operation::Append,
            additional_properties: HashMap::from_iter(vec![
                (
                    "spark.app.id".to_string(),
                    "local-1662532784305".to_string(),
                ),
                ("added-data-files".to_string(), "4".to_string()),
                ("added-records".to_string(), "4".to_string()),
                ("added-files-size".to_string(), "6001".to_string()),
            ]),
        })
        .build();

    let builder = builder.add_snapshot(snap).unwrap().build().unwrap();

    let updates = builder.changes;

    let _ = commit_tables_with_authz(
        ns_params.prefix.clone(),
        CommitTransactionRequest {
            table_changes: vec![CommitTableRequest {
                identifier: Some(table_ident.clone()),
                requirements: vec![],
                updates,
            }],
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
        None,
    )
    .await
    .unwrap();

    let tab = CatalogServer::load_table(
        TableParameters {
            prefix: ns_params.prefix.clone(),
            table: table_ident.clone(),
        },
        LoadTableRequest::builder().build(),
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    let LoadTableResultOrNotModified::LoadTableResult(tab) = tab else {
        panic!("Expected LoadTableResult, got NotModified");
    };

    assert_eq!(&*tab.metadata, &builder.metadata);

    let last_updated = tab.metadata.last_updated_ms();
    let builder = builder.metadata.into_builder(tab.metadata_location);

    let snap = Snapshot::builder()
        .with_snapshot_id(3)
        .with_timestamp_ms(last_updated + 1)
        .with_parent_snapshot_id(Some(2))
        .with_sequence_number(2)
        .with_schema_id(0)
        .with_manifest_list("/snap-2.avro")
        .with_summary(Summary {
            operation: Operation::Append,
            additional_properties: HashMap::from_iter(vec![
                (
                    "spark.app.id".to_string(),
                    "local-1662532784305".to_string(),
                ),
                ("added-data-files".to_string(), "4".to_string()),
                ("added-records".to_string(), "4".to_string()),
                ("added-files-size".to_string(), "6001".to_string()),
            ]),
        })
        .build();

    let builder = builder.add_snapshot(snap).unwrap().build().unwrap();

    let updates = builder.changes;

    let _ = commit_tables_with_authz(
        ns_params.prefix.clone(),
        CommitTransactionRequest {
            table_changes: vec![CommitTableRequest {
                identifier: Some(table_ident.clone()),
                requirements: vec![],
                updates,
            }],
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
        None,
    )
    .await
    .unwrap();

    let tab = CatalogServer::load_table(
        TableParameters {
            prefix: ns_params.prefix.clone(),
            table: table_ident.clone(),
        },
        LoadTableRequest::builder().build(),
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    let LoadTableResultOrNotModified::LoadTableResult(tab) = tab else {
        panic!("Expected LoadTableResult, got NotModified");
    };

    assert_eq!(&*tab.metadata, &builder.metadata);

    let builder = builder
        .metadata
        .into_builder(tab.metadata_location)
        .remove_snapshots(&[2])
        .build()
        .unwrap();

    let updates = builder.changes;

    let _ = commit_tables_with_authz(
        ns_params.prefix.clone(),
        CommitTransactionRequest {
            table_changes: vec![CommitTableRequest {
                identifier: Some(table_ident.clone()),
                requirements: vec![],
                updates,
            }],
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
        None,
    )
    .await
    .unwrap();

    let tab = CatalogServer::load_table(
        TableParameters {
            prefix: ns_params.prefix.clone(),
            table: table_ident.clone(),
        },
        LoadTableRequest::builder().build(),
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    let LoadTableResultOrNotModified::LoadTableResult(tab) = tab else {
        panic!("Expected LoadTableResult, got NotModified");
    };

    assert_eq!(tab.metadata.history(), builder.metadata.history());
    assert_eq!(
        tab.metadata
            .snapshots()
            .sorted_by_key(|s| s.snapshot_id())
            .collect_vec(),
        builder
            .metadata
            .snapshots()
            .sorted_by_key(|s| s.snapshot_id())
            .collect_vec()
    );
    assert_table_metadata_are_equal(&builder.metadata, &tab.metadata);
}

async fn commit_test_setup(
    pool: PgPool,
) -> (
    ApiContext<State<AllowAllAuthorizer, PostgresBackend, SecretsState>>,
    CreateNamespaceResponse,
    NamespaceParameters,
    LoadTableResult,
) {
    let (ctx, ns, ns_params, _) = table_test_setup(pool).await;
    let table = CatalogServer::create_table(
        ns_params.clone(),
        create_request(Some("tab-1".to_string()), Some(false)),
        DataAccess {
            vended_credentials: true,
            remote_signing: false,
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();
    (ctx, ns, ns_params, table)
}

async fn table_test_setup(
    pool: PgPool,
) -> (
    ApiContext<State<AllowAllAuthorizer, PostgresBackend, SecretsState>>,
    CreateNamespaceResponse,
    NamespaceParameters,
    String,
) {
    let prof = memory_io_profile();
    let base_loc = prof.base_location().unwrap().to_string();
    let (ctx, warehouse) = setup_simple(
        pool.clone(),
        prof,
        None,
        AllowAllAuthorizer::default(),
        TabularDeleteProfile::Hard {},
        None,
    )
    .await;
    let ns = create_ns(
        ctx.clone(),
        warehouse.warehouse_id.to_string(),
        "ns1".to_string(),
    )
    .await;
    let ns_params = NamespaceParameters {
        prefix: Some(Prefix(warehouse.warehouse_id.to_string())),
        namespace: ns.namespace.clone(),
    };
    (ctx, ns, ns_params, base_loc)
}

#[sqlx::test]
async fn test_can_create_tables_with_same_prefix_1(pool: PgPool) {
    let (ctx, _, ns_params, base_location) = table_test_setup(pool).await;
    let tmp_id = Uuid::now_v7();
    let mut create_request_1 = create_request(Some("tab-1".to_string()), Some(false));
    create_request_1.location = Some(format!("{base_location}/{tmp_id}/my-table-2"));
    let mut create_request_2 = create_request(Some("tab-2".to_string()), Some(false));
    create_request_2.location = Some(format!("{base_location}/{tmp_id}/my-table"));

    let _ = CatalogServer::create_table(
        ns_params.clone(),
        create_request_1,
        DataAccess::not_specified(),
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    CatalogServer::create_table(
        ns_params.clone(),
        create_request_2,
        DataAccess::not_specified(),
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();
}

#[sqlx::test]
async fn test_can_create_tables_with_same_prefix_2(pool: PgPool) {
    let (ctx, _, ns_params, base_location) = table_test_setup(pool).await;
    let tmp_id = Uuid::now_v7();
    let mut create_request_1 = create_request(Some("tab-1".to_string()), Some(false));
    create_request_1.location = Some(format!("{base_location}/{tmp_id}/my-table"));
    let mut create_request_2 = create_request(Some("tab-2".to_string()), Some(false));
    create_request_2.location = Some(format!("{base_location}/{tmp_id}/my-table-2"));

    let _ = CatalogServer::create_table(
        ns_params.clone(),
        create_request_1,
        DataAccess::not_specified(),
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    CatalogServer::create_table(
        ns_params.clone(),
        create_request_2,
        DataAccess::not_specified(),
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();
}

#[sqlx::test]
async fn test_cannot_create_table_at_same_location(pool: PgPool) {
    let (ctx, _, ns_params, base_location) = table_test_setup(pool).await;
    let tmp_id = Uuid::now_v7();
    let mut create_request_1 = create_request(Some("tab-1".to_string()), Some(false));
    create_request_1.location = Some(format!("{base_location}/{tmp_id}/bucket"));
    let mut create_request_2 = create_request(Some("tab-2".to_string()), Some(false));
    create_request_2.location = Some(format!("{base_location}/{tmp_id}/bucket"));

    let _ = CatalogServer::create_table(
        ns_params.clone(),
        create_request_1,
        DataAccess::not_specified(),
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    let e = CatalogServer::create_table(
        ns_params.clone(),
        create_request_2,
        DataAccess::not_specified(),
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .expect_err("Table was created at same location which should not be possible");
    assert_eq!(e.error.code, StatusCode::CONFLICT, "{e:?}");
    assert_eq!(e.error.r#type.as_str(), "LocationAlreadyTaken");
}

#[sqlx::test]
async fn test_cannot_create_staged_tables_at_sublocations_1(pool: PgPool) {
    let (ctx, _, ns_params, base_location) = table_test_setup(pool).await;
    let tmp_id = Uuid::now_v7();
    let mut create_request_1 = create_request(Some("tab-1".to_string()), Some(false));
    create_request_1.stage_create = Some(true);
    create_request_1.location = Some(format!("{base_location}/{tmp_id}/bucket/inner"));
    let mut create_request_2 = create_request(Some("tab-2".to_string()), Some(false));
    create_request_2.stage_create = Some(true);
    create_request_2.location = Some(format!("{base_location}/{tmp_id}/bucket"));
    let _ = CatalogServer::create_table(
        ns_params.clone(),
        create_request_1,
        DataAccess::not_specified(),
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    let e = CatalogServer::create_table(
        ns_params.clone(),
        create_request_2,
        DataAccess::not_specified(),
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .expect_err("Staged table could be created at sublocation which should not be possible");
    assert_eq!(e.error.code, StatusCode::CONFLICT, "{e:?}");
    assert_eq!(e.error.r#type.as_str(), "LocationAlreadyTaken");
}

#[sqlx::test]
async fn test_cannot_create_staged_tables_at_sublocations_2(pool: PgPool) {
    let (ctx, _, ns_params, base_location) = table_test_setup(pool).await;
    let tmp_id = Uuid::now_v7();
    let mut create_request_1 = create_request(Some("tab-1".to_string()), Some(false));
    create_request_1.stage_create = Some(true);
    create_request_1.location = Some(format!("{base_location}/{tmp_id}/bucket"));
    let mut create_request_2 = create_request(Some("tab-2".to_string()), Some(false));
    create_request_2.stage_create = Some(true);
    create_request_2.location = Some(format!("{base_location}/{tmp_id}/bucket/inner"));
    let _ = CatalogServer::create_table(
        ns_params.clone(),
        create_request_1,
        DataAccess::not_specified(),
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    let e = CatalogServer::create_table(
        ns_params.clone(),
        create_request_2,
        DataAccess::not_specified(),
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .expect_err("Staged table could be created at sublocation which should not be possible");
    assert_eq!(e.error.code, StatusCode::CONFLICT, "{e:?}");
    assert_eq!(e.error.r#type.as_str(), "LocationAlreadyTaken");
}

#[sqlx::test]
async fn test_cannot_create_tables_at_sublocations_1(pool: PgPool) {
    let (ctx, _, ns_params, base_location) = table_test_setup(pool).await;
    let tmp_id = Uuid::now_v7();

    let mut create_request_1 = create_request(Some("tab-1".to_string()), Some(false));
    create_request_1.location = Some(format!("{base_location}/{tmp_id}/bucket"));
    let mut create_request_2 = create_request(Some("tab-2".to_string()), Some(false));
    create_request_2.location = Some(format!("{base_location}/{tmp_id}/bucket/sublocation"));
    let _ = CatalogServer::create_table(
        ns_params.clone(),
        create_request_1,
        DataAccess::not_specified(),
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    let e = CatalogServer::create_table(
        ns_params.clone(),
        create_request_2,
        DataAccess::not_specified(),
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .expect_err("Staged table could be created at sublocation which should not be possible");
    assert_eq!(e.error.code, StatusCode::CONFLICT, "{e:?}");
    assert_eq!(e.error.r#type.as_str(), "LocationAlreadyTaken");
}

async fn pagination_test_setup(
    pool: PgPool,
    n_tables: usize,
    hidden_ranges: &[(usize, usize)],
) -> (
    ApiContext<State<HidingAuthorizer, PostgresBackend, SecretsState>>,
    NamespaceParameters,
) {
    let prof = memory_io_profile();
    let base_location = prof.base_location().unwrap();
    let authz = HidingAuthorizer::new();
    // Prevent hidden tables from becoming visible through `can_list_everything`.
    authz.block_can_list_everything();

    let (ctx, warehouse) = setup_simple(
        pool.clone(),
        prof,
        None,
        authz.clone(),
        TabularDeleteProfile::Hard {},
        Some(UserId::new_unchecked("oidc", "test-user-id")),
    )
    .await;
    let ns = create_ns(
        ctx.clone(),
        warehouse.warehouse_id.to_string(),
        "ns1".to_string(),
    )
    .await;
    let ns_params = NamespaceParameters {
        prefix: Some(Prefix(warehouse.warehouse_id.to_string())),
        namespace: ns.namespace.clone(),
    };
    for i in 0..n_tables {
        let mut create_request = create_request(Some(format!("{i}")), Some(false));
        create_request.location = Some(format!("{base_location}/bucket/{i}"));
        let tab = CatalogServer::create_table(
            ns_params.clone(),
            create_request,
            DataAccess::not_specified(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();
        for (start, end) in hidden_ranges.iter().copied() {
            if i >= start && i < end {
                authz.hide(&format!(
                    "table:{}/{}",
                    warehouse.warehouse_id,
                    tab.metadata.uuid()
                ));
            }
        }
    }

    (ctx, ns_params)
}

impl_pagination_tests!(
    table,
    pagination_test_setup,
    CatalogServer,
    ListTablesQuery,
    identifiers,
    |tid| { tid.name }
);

#[sqlx::test]
async fn test_table_pagination(pool: sqlx::PgPool) {
    let prof = memory_io_profile();

    let authz = HidingAuthorizer::new();
    // Prevent hidden tables from becoming visible through `can_list_everything`.
    authz.block_can_list_everything();

    let (ctx, warehouse) = setup_simple(
        pool.clone(),
        prof,
        None,
        authz.clone(),
        TabularDeleteProfile::Hard {},
        Some(UserId::new_unchecked("oidc", "test-user-id")),
    )
    .await;
    let ns = create_ns(
        ctx.clone(),
        warehouse.warehouse_id.to_string(),
        "ns1".to_string(),
    )
    .await;
    let ns_params = NamespaceParameters {
        prefix: Some(Prefix(warehouse.warehouse_id.to_string())),
        namespace: ns.namespace.clone(),
    };
    // create 10 staged tables
    for i in 0..10 {
        let _ = CatalogServer::create_table(
            ns_params.clone(),
            create_request(Some(format!("tab-{i}")), Some(false)),
            DataAccess {
                vended_credentials: true,
                remote_signing: false,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();
    }

    // list 1 more than existing tables
    let all = CatalogServer::list_tables(
        ns_params.clone(),
        ListTablesQuery {
            page_token: PageToken::NotSpecified,
            page_size: Some(11),
            return_uuids: true,
            return_protection_status: true,
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();
    assert_eq!(all.identifiers.len(), 10);

    // list exactly amount of existing tables
    let all = CatalogServer::list_tables(
        ns_params.clone(),
        ListTablesQuery {
            page_token: PageToken::NotSpecified,
            page_size: Some(10),
            return_uuids: true,
            return_protection_status: true,
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();
    assert_eq!(all.identifiers.len(), 10);

    // next page is empty
    let next = CatalogServer::list_tables(
        ns_params.clone(),
        ListTablesQuery {
            page_token: PageToken::Present(all.next_page_token.unwrap()),
            page_size: Some(10),
            return_uuids: true,
            return_protection_status: true,
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    assert_eq!(next.identifiers.len(), 0);
    assert!(next.next_page_token.is_none());

    let first_six = CatalogServer::list_tables(
        ns_params.clone(),
        ListTablesQuery {
            page_token: PageToken::NotSpecified,
            page_size: Some(6),
            return_uuids: true,
            return_protection_status: true,
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();
    assert_eq!(first_six.identifiers.len(), 6);
    assert!(first_six.next_page_token.is_some());
    let first_six_items = first_six
        .identifiers
        .iter()
        .map(|i| i.name.clone())
        .sorted()
        .collect::<Vec<_>>();

    for (i, item) in first_six_items.iter().enumerate().take(6) {
        assert_eq!(item, &format!("tab-{i}"));
    }

    let next_four = CatalogServer::list_tables(
        ns_params.clone(),
        ListTablesQuery {
            page_token: PageToken::Present(first_six.next_page_token.unwrap()),
            page_size: Some(6),
            return_uuids: true,
            return_protection_status: true,
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();
    assert_eq!(next_four.identifiers.len(), 4);
    // page-size > number of items left -> no next page
    assert!(next_four.next_page_token.is_none());

    let next_four_items = next_four
        .identifiers
        .iter()
        .map(|i| i.name.clone())
        .sorted()
        .collect::<Vec<_>>();

    for (idx, i) in (6..10).enumerate() {
        assert_eq!(next_four_items[idx], format!("tab-{i}"));
    }

    let mut ids = all.table_uuids.unwrap();
    ids.sort();
    for t in ids.iter().take(6).skip(4) {
        authz.hide(&format!("table:{}/{t}", warehouse.warehouse_id));
    }

    let page = CatalogServer::list_tables(
        ns_params.clone(),
        ListTablesQuery {
            page_token: PageToken::NotSpecified,
            page_size: Some(5),
            return_uuids: true,
            return_protection_status: true,
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    assert_eq!(page.identifiers.len(), 5);
    assert!(page.next_page_token.is_some());
    let page_items = page
        .identifiers
        .iter()
        .map(|i| i.name.clone())
        .sorted()
        .collect::<Vec<_>>();
    for (i, item) in page_items.iter().enumerate() {
        let tab_id = if i > 3 { i + 2 } else { i };
        assert_eq!(item, &format!("tab-{tab_id}"));
    }

    let next_page = CatalogServer::list_tables(
        ns_params.clone(),
        ListTablesQuery {
            page_token: PageToken::Present(page.next_page_token.unwrap()),
            page_size: Some(6),
            return_uuids: true,
            return_protection_status: true,
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    assert_eq!(next_page.identifiers.len(), 3);

    let next_page_items = next_page
        .identifiers
        .iter()
        .map(|i| i.name.clone())
        .sorted()
        .collect::<Vec<_>>();

    for (idx, i) in (7..10).enumerate() {
        assert_eq!(next_page_items[idx], format!("tab-{i}"));
    }
}

#[sqlx::test]
async fn test_list_tables(pool: sqlx::PgPool) {
    let prof = memory_io_profile();

    let authz = HidingAuthorizer::new();

    let (ctx, warehouse) = setup_simple(
        pool.clone(),
        prof,
        None,
        authz.clone(),
        TabularDeleteProfile::Hard {},
        Some(UserId::new_unchecked("oidc", "test-user-id")),
    )
    .await;
    let ns = create_ns(
        ctx.clone(),
        warehouse.warehouse_id.to_string(),
        "ns1".to_string(),
    )
    .await;
    let ns_params = NamespaceParameters {
        prefix: Some(Prefix(warehouse.warehouse_id.to_string())),
        namespace: ns.namespace.clone(),
    };
    // create 10 staged tables
    for i in 0..10 {
        let _ = CatalogServer::create_table(
            ns_params.clone(),
            create_request(Some(format!("tab-{i}")), Some(false)),
            DataAccess {
                vended_credentials: true,
                remote_signing: false,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();
    }

    // By default `HidingAuthorizer` allows everything, meaning the quick check path in
    // `list_tables` will be hit since `can_list_everything: true`.
    let all = CatalogServer::list_tables(
        ns_params.clone(),
        ListTablesQuery {
            page_token: PageToken::NotSpecified,
            page_size: Some(11),
            return_uuids: true,
            return_protection_status: true,
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();
    assert_eq!(all.identifiers.len(), 10);

    // Block `can_list_everything` to hit alternative code path.
    ctx.v1_state.authz.block_can_list_everything();
    let all = CatalogServer::list_tables(
        ns_params.clone(),
        ListTablesQuery {
            page_token: PageToken::NotSpecified,
            page_size: Some(11),
            return_uuids: true,
            return_protection_status: true,
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();
    assert_eq!(all.identifiers.len(), 10);
}

#[sqlx::test]
async fn test_cannot_drop_protected_table(pool: PgPool) {
    let (ctx, _, ns_params, _) = table_test_setup(pool).await;
    let table_ident = TableIdent {
        namespace: ns_params.namespace.clone(),
        name: "tab-1".to_string(),
    };
    let tab = CatalogServer::create_table(
        ns_params.clone(),
        create_request(Some("tab-1".to_string()), Some(false)),
        DataAccess::not_specified(),
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    ManagementApiServer::set_table_protection(
        tab.metadata.uuid().into(),
        WarehouseId::from_str_or_internal(ns_params.prefix.clone().unwrap().as_str()).unwrap(),
        true,
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    let e = CatalogServer::drop_table(
        TableParameters {
            prefix: ns_params.prefix.clone(),
            table: table_ident.clone(),
        },
        DropParams {
            purge_requested: true,
            force: false,
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .expect_err("Table was dropped which should not be possible");
    assert_eq!(e.error.code, StatusCode::CONFLICT, "{e:?}");

    ManagementApiServer::set_table_protection(
        tab.metadata.uuid().into(),
        WarehouseId::from_str_or_internal(ns_params.prefix.clone().unwrap().as_str()).unwrap(),
        false,
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    CatalogServer::drop_table(
        TableParameters {
            prefix: ns_params.prefix.clone(),
            table: table_ident.clone(),
        },
        DropParams {
            purge_requested: true,
            force: true,
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();
}

#[sqlx::test]
async fn test_can_force_drop_protected_table(pool: PgPool) {
    let (ctx, _, ns_params, _) = table_test_setup(pool).await;
    let table_ident = TableIdent {
        namespace: ns_params.namespace.clone(),
        name: "tab-1".to_string(),
    };
    let tab = CatalogServer::create_table(
        ns_params.clone(),
        create_request(Some("tab-1".to_string()), Some(false)),
        DataAccess::not_specified(),
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    ManagementApiServer::set_table_protection(
        tab.metadata.uuid().into(),
        WarehouseId::from_str_or_internal(ns_params.prefix.clone().unwrap().as_str()).unwrap(),
        true,
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    CatalogServer::drop_table(
        TableParameters {
            prefix: ns_params.prefix.clone(),
            table: table_ident.clone(),
        },
        DropParams {
            purge_requested: true,
            force: true,
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .expect("Table couldn't be force dropped which should be possible");
}

#[sqlx::test]
async fn test_rename_table_without_can_rename(pool: sqlx::PgPool) {
    let prof = memory_io_profile();
    let authz = HidingAuthorizer::new();
    let (ctx, warehouse) = setup_simple(
        pool.clone(),
        prof,
        None,
        authz.clone(),
        TabularDeleteProfile::Hard {},
        Some(UserId::new_unchecked("oidc", "test-user-id")),
    )
    .await;

    let from_ns = create_ns(
        ctx.clone(),
        warehouse.warehouse_id.to_string(),
        "from_ns".to_string(),
    )
    .await;
    let to_ns = create_ns(
        ctx.clone(),
        warehouse.warehouse_id.to_string(),
        "to_ns".to_string(),
    )
    .await;
    let ns_params = NamespaceParameters {
        prefix: Some(Prefix(warehouse.warehouse_id.to_string())),
        namespace: from_ns.namespace.clone(),
    };
    let prefix = Some(Prefix(warehouse.warehouse_id.to_string()));
    let table_name = "from_table".to_string();
    CatalogServer::create_table(
        ns_params.clone(),
        create_request(Some(table_name.clone()), Some(false)),
        DataAccess {
            vended_credentials: true,
            remote_signing: false,
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    // Not authorized to rename the source table
    authz.block_action(format!("table:{:?}", CatalogTableAction::Rename).as_str());
    let rename_table_request = RenameTableRequest {
        source: TableIdent {
            namespace: ns_params.namespace.clone(),
            name: table_name.clone(),
        },
        destination: TableIdent {
            namespace: to_ns.namespace.clone(),
            name: table_name.clone(),
        },
    };
    let response = CatalogServer::rename_table(
        prefix.clone(),
        rename_table_request.clone(),
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap_err();

    assert_eq!(response.error.code, StatusCode::FORBIDDEN, "{response:?}");

    // If we also block the get_metadata_action, the user is not allowed to know if the table exists.
    // thus, we should get a 404 instead.
    authz.block_action(format!("table:{:?}", CatalogTableAction::GetMetadata).as_str());
    let response = CatalogServer::rename_table(
        prefix,
        rename_table_request,
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap_err();
    assert_eq!(response.error.code, StatusCode::NOT_FOUND, "{response:?}");
}

#[sqlx::test]
async fn test_rename_table_without_can_create(pool: sqlx::PgPool) {
    let prof = memory_io_profile();
    let authz = HidingAuthorizer::new();
    let (ctx, warehouse) = setup_simple(
        pool.clone(),
        prof,
        None,
        authz.clone(),
        TabularDeleteProfile::Hard {},
        Some(UserId::new_unchecked("oidc", "test-user-id")),
    )
    .await;

    let from_ns = create_ns(
        ctx.clone(),
        warehouse.warehouse_id.to_string(),
        "from_ns".to_string(),
    )
    .await;
    let to_ns = create_ns(
        ctx.clone(),
        warehouse.warehouse_id.to_string(),
        "to_ns".to_string(),
    )
    .await;
    let ns_params = NamespaceParameters {
        prefix: Some(Prefix(warehouse.warehouse_id.to_string())),
        namespace: from_ns.namespace.clone(),
    };
    let prefix = Some(Prefix(warehouse.warehouse_id.to_string()));
    let table_name = "from_table".to_string();
    CatalogServer::create_table(
        ns_params.clone(),
        create_request(Some(table_name.clone()), Some(false)),
        DataAccess {
            vended_credentials: true,
            remote_signing: false,
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    // Not authorized to create a table in the destination namepsace
    // Block any CreateTable namespace action (prefix match — fields are dynamic).
    authz.block_action("namespace:CreateTable");
    let response = CatalogServer::rename_table(
        prefix,
        RenameTableRequest {
            source: TableIdent {
                namespace: ns_params.namespace.clone(),
                name: table_name.clone(),
            },
            destination: TableIdent {
                namespace: to_ns.namespace.clone(),
                name: table_name,
            },
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap_err();

    assert_eq!(response.error.code, StatusCode::FORBIDDEN);
    assert_eq!(response.error.r#type, "NamespaceActionForbidden");
}

#[sqlx::test]
async fn test_rename_table_without_target_namespace(pool: sqlx::PgPool) {
    let prof = memory_io_profile();
    let (ctx, warehouse) = setup_simple(
        pool.clone(),
        prof,
        None,
        AllowAllAuthorizer::default(),
        TabularDeleteProfile::Hard {},
        Some(UserId::new_unchecked("oidc", "test-user-id")),
    )
    .await;

    let from_ns = create_ns(
        ctx.clone(),
        warehouse.warehouse_id.to_string(),
        "from_ns".to_string(),
    )
    .await;
    let ns_params = NamespaceParameters {
        prefix: Some(Prefix(warehouse.warehouse_id.to_string())),
        namespace: from_ns.namespace.clone(),
    };
    let prefix = Some(Prefix(warehouse.warehouse_id.to_string()));
    let table_name = "from_table".to_string();
    CatalogServer::create_table(
        ns_params.clone(),
        create_request(Some(table_name.clone()), Some(false)),
        DataAccess {
            vended_credentials: true,
            remote_signing: false,
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    // All actions are allowed but the target namespace does not exist
    let response = CatalogServer::rename_table(
        prefix,
        RenameTableRequest {
            source: TableIdent {
                namespace: ns_params.namespace.clone(),
                name: table_name.clone(),
            },
            destination: TableIdent {
                namespace: NamespaceIdent::new("to_ns".to_string()),
                name: table_name,
            },
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap_err();

    assert_eq!(response.error.code, StatusCode::NOT_FOUND);
    assert_eq!(response.error.r#type, "NoSuchNamespaceException");
}

#[sqlx::test]
async fn test_rename_table_without_source_table(pool: sqlx::PgPool) {
    let prof = memory_io_profile();
    let (ctx, warehouse) = setup_simple(
        pool.clone(),
        prof,
        None,
        AllowAllAuthorizer::default(),
        TabularDeleteProfile::Hard {},
        Some(UserId::new_unchecked("oidc", "test-user-id")),
    )
    .await;

    let from_ns = create_ns(
        ctx.clone(),
        warehouse.warehouse_id.to_string(),
        "from_ns".to_string(),
    )
    .await;
    let to_ns = create_ns(
        ctx.clone(),
        warehouse.warehouse_id.to_string(),
        "to_ns".to_string(),
    )
    .await;
    let prefix = Some(Prefix(warehouse.warehouse_id.to_string()));
    let table_name = "from_table".to_string();

    // All actions are allowed but the origin table does not exist
    let response = CatalogServer::rename_table(
        prefix,
        RenameTableRequest {
            source: TableIdent {
                namespace: from_ns.namespace.clone(),
                name: table_name.clone(),
            },
            destination: TableIdent {
                namespace: to_ns.namespace.clone(),
                name: table_name,
            },
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap_err();

    assert_eq!(response.error.code, StatusCode::NOT_FOUND);
}

#[sqlx::test]
async fn test_register_table_with_overwrite(pool: PgPool) {
    let (ctx, ns, ns_params, _) = table_test_setup(pool).await;

    // Create a table first
    let initial_table = CatalogServer::create_table(
        ns_params.clone(),
        create_request(Some("test_overwrite".to_string()), Some(false)),
        DataAccess::not_specified(),
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    // Verify the table exists
    let table_ident = TableIdent {
        namespace: ns.namespace.clone(),
        name: "test_overwrite".to_string(),
    };

    CatalogServer::table_exists(
        TableParameters {
            prefix: ns_params.prefix.clone(),
            table: table_ident.clone(),
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    // Now create a second table to use for the overwrite test
    let second_table = CatalogServer::create_table(
        ns_params.clone(),
        create_request(Some("second_table".to_string()), Some(false)),
        DataAccess::not_specified(),
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    // Read table metadata

    // Drop second table, keep data
    CatalogServer::drop_table(
        TableParameters {
            prefix: ns_params.prefix.clone(),
            table: TableIdent {
                namespace: ns.namespace.clone(),
                name: "second_table".to_string(),
            },
        },
        DropParams {
            purge_requested: false,
            force: false,
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .expect("Failed to drop second table");

    // Test without overwrite flag - should fail
    let register_request = iceberg_ext::catalog::rest::RegisterTableRequest::builder()
        .name("test_overwrite".to_string())
        .metadata_location(second_table.metadata_location.as_ref().unwrap().clone())
        .build();

    CatalogServer::register_table(
        ns_params.clone(),
        register_request.clone(),
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .expect_err("Registration should fail without overwrite flag");

    // Test with overwrite flag - should succeed
    let register_request_with_overwrite =
        iceberg_ext::catalog::rest::RegisterTableRequest::builder()
            .name("test_overwrite".to_string())
            .metadata_location(second_table.metadata_location.as_ref().unwrap().clone())
            .overwrite(true)
            .build();

    let result = CatalogServer::register_table(
        ns_params.clone(),
        register_request_with_overwrite,
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await;

    assert!(
        result.is_ok(),
        "Registration with overwrite flag should succeed, but failed with: {:?}",
        result.err().map(|e| e.error.message)
    );

    // Verify the table exists and has the new metadata
    let loaded_table = CatalogServer::load_table(
        TableParameters {
            prefix: ns_params.prefix,
            table: table_ident,
        },
        LoadTableRequest::builder().build(),
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    let LoadTableResultOrNotModified::LoadTableResult(loaded_table) = loaded_table else {
        panic!("Expected LoadTableResult, got NotModified");
    };

    // The loaded table should have the UUID and content of the second table
    assert_eq!(loaded_table.metadata.uuid(), second_table.metadata.uuid());
    assert_ne!(loaded_table.metadata.uuid(), initial_table.metadata.uuid());
}

// Reasons for using a mix of PostgresCatalog and CatalogServer:
//
// - PostgresCatalog: required for specifying id of table to be created
// - CatalogServer: required for taking TabularDeleteProfile into account
#[sqlx::test]
async fn test_reuse_table_ids_hard_delete(pool: PgPool) {
    let delete_profile = TabularDeleteProfile::Hard {};
    let (ctx, mut wh_ns_data, _base_loc) =
        tabular_test_multi_warehouse_setup(pool.clone(), 3, delete_profile).await;

    let t_id = TableId::new_random();
    let t_name = "t1".to_string();
    let list_flags = TabularListFlags::all();

    // Create tables with the same table ID across different warehouses.
    for (wh_id, _ns_id, ns_params) in &wh_ns_data {
        let _inited_table = initialize_table(
            *wh_id,
            ctx.v1_state.catalog.clone(),
            false,
            Some(ns_params.namespace.clone()),
            Some(t_id),
            Some(t_name.clone()),
        )
        .await;

        // Verify table creation.
        let _meta =
            PostgresBackend::get_table_info(*wh_id, t_id, list_flags, ctx.v1_state.catalog.clone())
                .await
                .unwrap()
                .expect("table and metadata should exist");
    }

    // Hard delete one of the tables.
    let deleted_table_data = wh_ns_data.pop().unwrap();
    CatalogServer::drop_table(
        TableParameters {
            prefix: deleted_table_data.2.prefix.clone(),
            table: TableIdent {
                namespace: deleted_table_data.2.namespace.clone(),
                name: t_name.clone(),
            },
        },
        DropParams {
            purge_requested: false,
            force: false,
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    // Deleted table cannot be accessed anymore.
    let deleted_res = PostgresBackend::get_table_info(
        deleted_table_data.0,
        t_id,
        list_flags,
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();
    assert!(deleted_res.is_none(), "Table should be deleted");

    // Tables in other warehouses are still there.
    assert!(!wh_ns_data.is_empty());
    for (wh_id, _ns_id, _ns_params) in &wh_ns_data {
        PostgresBackend::get_table_info(*wh_id, t_id, list_flags, ctx.v1_state.catalog.clone())
            .await
            .unwrap()
            .expect("table and metadata should still exist");
    }

    // As the delete was hard, the table can be recreated in the warehouse.
    let _inited_table = initialize_table(
        deleted_table_data.0,
        ctx.v1_state.catalog.clone(),
        false,
        Some(deleted_table_data.2.namespace.clone()),
        Some(t_id),
        Some(t_name.clone()),
    )
    .await;
    let _meta = PostgresBackend::get_table_info(
        deleted_table_data.0,
        t_id,
        list_flags,
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap()
    .expect("table and metadata should exist");
}

// Reasons for using a mix of PostgresCatalog and CatalogServer:
//
// - PostgresCatalog: required for specifying id of table to be created
// - CatalogServer: required for taking TabularDeleteProfile into account
#[sqlx::test]
async fn test_reuse_table_ids_soft_delete(pool: PgPool) {
    let delete_profile = TabularDeleteProfile::Soft {
        expiration_seconds: chrono::Duration::seconds(10),
    };
    let (ctx, mut wh_ns_data, _base_loc) =
        tabular_test_multi_warehouse_setup(pool.clone(), 3, delete_profile).await;

    let t_id = TableId::new_random();
    let t_name = "t1".to_string();
    let list_flags_active = TabularListFlags::active();

    // Create tables with the same table ID across different warehouses.
    for (wh_id, _ns_id, ns_params) in &wh_ns_data {
        let _inited_table = initialize_table(
            *wh_id,
            ctx.v1_state.catalog.clone(),
            false,
            Some(ns_params.namespace.clone()),
            Some(t_id),
            Some(t_name.clone()),
        )
        .await;

        // Verify table creation.
        let _meta = PostgresBackend::get_table_info(
            *wh_id,
            t_id,
            list_flags_active,
            ctx.v1_state.catalog.clone(),
        )
        .await
        .unwrap()
        .expect("table and metadata should exist");
    }

    // Soft delete one of the tables.
    let deleted_table_data = wh_ns_data.pop().unwrap();
    CatalogServer::drop_table(
        TableParameters {
            prefix: deleted_table_data.2.prefix.clone(),
            table: TableIdent {
                namespace: deleted_table_data.2.namespace.clone(),
                name: t_name.clone(),
            },
        },
        DropParams {
            purge_requested: false,
            force: false,
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    // Check availability depending on list flags.
    let deleted_res = PostgresBackend::get_table_info(
        deleted_table_data.0,
        t_id,
        list_flags_active,
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();
    assert!(deleted_res.is_none(), "Table should be soft deleted");
    let deleted_res = PostgresBackend::get_table_info(
        deleted_table_data.0,
        t_id,
        TabularListFlags::all(), // include soft deleted
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();
    assert!(deleted_res.is_some(), "Table should be only soft deleted");

    // Tables in other warehouses are still there.
    assert!(!wh_ns_data.is_empty());
    for (wh_id, _ns_id, _ns_params) in &wh_ns_data {
        PostgresBackend::get_table_info(
            *wh_id,
            t_id,
            list_flags_active,
            ctx.v1_state.catalog.clone(),
        )
        .await
        .unwrap()
        .expect("table and metadata should still exist");
    }
}
