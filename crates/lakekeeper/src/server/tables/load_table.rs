use std::{collections::HashMap, sync::Arc};

use http::StatusCode;
use iceberg_ext::catalog::rest::StorageCredential;

use crate::{
    api::iceberg::v1::{
        tables::{DataAccessMode, LoadTableFilters},
        ApiContext, LoadTableResult, Result, TableIdent, TableParameters,
    },
    request_metadata::RequestMetadata,
    server::{
        maybe_get_secret, require_warehouse_id,
        tables::{authorize_load_table, parse_location, validate_table_or_view_ident},
    },
    service::{
        authz::Authorizer, secrets::SecretStore, AuthZTableInfo as _, CachePolicy, CatalogStore,
        CatalogTableOps, CatalogWarehouseOps, LoadTableResponse as CatalogLoadTableResult, State,
        TableId, TableIdentOrId, TabularListFlags, TabularNotFound, Transaction,
    },
    WarehouseId,
};

/// Load a table from the catalog
#[allow(clippy::too_many_lines)]
pub(super) async fn load_table<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    parameters: TableParameters,
    data_access: impl Into<DataAccessMode> + Send,
    filters: LoadTableFilters,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
) -> Result<LoadTableResult> {
    // ------------------- VALIDATIONS -------------------
    let TableParameters { prefix, table } = parameters;
    let warehouse_id = require_warehouse_id(prefix.as_ref())?;
    // It is important to throw a 404 if a table cannot be found,
    // because spark might check if `table`.`branch` exists, which should return 404.
    // Only then will it treat it as a branch.
    if let Err(mut e) = validate_table_or_view_ident(&table) {
        if e.error.r#type == *"NamespaceDepthExceeded" {
            e.error.code = StatusCode::NOT_FOUND.into();
        }
        return Err(e);
    }

    // ------------------- AUTHZ -------------------
    let authorizer = state.v1_state.authz;
    let catalog_state = state.v1_state.catalog;

    let (table_info, storage_permissions) = authorize_load_table::<C, A>(
        &request_metadata,
        table,
        warehouse_id,
        TabularListFlags::active(),
        authorizer,
        catalog_state.clone(),
    )
    .await?;

    // ------------------- BUSINESS LOGIC -------------------
    let mut t = C::Transaction::begin_read(catalog_state.clone()).await?;
    let CatalogLoadTableResult {
        table_id,
        namespace_id: _,
        table_metadata,
        metadata_location,
        warehouse_updated_at: warehouse_last_updated_at,
    } = load_table_inner::<C>(
        warehouse_id,
        table_info.table_id(),
        table_info.table_ident(),
        false,
        &filters,
        &mut t,
    )
    .await?;
    t.commit().await?;

    let warehouse = C::require_warehouse_by_id_cache_aware(
        warehouse_id,
        CachePolicy::OnlyIfNewerThan(warehouse_last_updated_at),
        catalog_state.clone(),
    )
    .await?;

    let table_location =
        parse_location(table_metadata.location(), StatusCode::INTERNAL_SERVER_ERROR)?;

    let storage_config = if let Some(storage_permissions) = storage_permissions {
        let storage_secret =
            maybe_get_secret(warehouse.storage_secret_id, &state.v1_state.secrets).await?;
        Some(
            warehouse
                .storage_profile
                .generate_table_config(
                    data_access.into(),
                    storage_secret.as_ref(),
                    &table_location,
                    storage_permissions,
                    &request_metadata,
                    warehouse_id,
                    table_id.into(),
                )
                .await?,
        )
    } else {
        None
    };

    let storage_credentials = storage_config.as_ref().and_then(|c| {
        (!c.creds.inner().is_empty()).then(|| {
            vec![StorageCredential {
                prefix: table_location.to_string(),
                config: c.creds.clone().into(),
            }]
        })
    });

    let load_table_result = LoadTableResult {
        metadata_location: metadata_location.as_ref().map(ToString::to_string),
        metadata: Arc::new(table_metadata),
        config: storage_config.map(|c| c.config.into()),
        storage_credentials,
    };

    Ok(load_table_result)
}

/// Load a table from the catalog, ensuring that it is not staged
///
/// # Errors
/// Returns an error if the table is staged, if it cannot be found, or if a DB error occurs.
async fn load_table_inner<C: CatalogStore>(
    warehouse_id: WarehouseId,
    table_id: TableId,
    table_ident: &TableIdent,
    include_deleted: bool,
    load_table_filters: &LoadTableFilters,
    t: &mut C::Transaction,
) -> Result<CatalogLoadTableResult> {
    let mut metadatas = C::load_tables(
        warehouse_id,
        [table_id],
        include_deleted,
        load_table_filters,
        t.transaction(),
    )
    .await?
    .into_iter()
    .map(|r| (r.table_id, r))
    .collect::<HashMap<_, _>>();
    let result = metadatas.remove(&table_id).ok_or_else(|| {
        TabularNotFound::new(warehouse_id, TableIdentOrId::from(table_ident.clone()))
            .append_detail("Table metadata not returned from table load".to_string())
    })?;
    if !metadatas.is_empty() {
        tracing::error!(
            "Unexpected extra table metadatas returned when loading table {:?} in warehouse {:?}: {:?}",
            table_ident,
            warehouse_id,
            metadatas.keys()
        );
    }
    require_not_staged(
        warehouse_id,
        table_ident.clone(),
        result.metadata_location.as_ref(),
    )?;
    Ok(result)
}

fn require_not_staged<T>(
    warehouse_id: WarehouseId,
    table_ident: impl Into<TableIdentOrId>,
    metadata_location: Option<&T>,
) -> std::result::Result<(), TabularNotFound> {
    if metadata_location.is_none() {
        return Err(TabularNotFound::new(warehouse_id, table_ident.into())
            .append_detail("Table is in staged state; operation requires active table"));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use iceberg::{
        spec::{
            NestedField, Operation, PrimitiveType, Schema, Snapshot, SnapshotReference,
            SnapshotRetention, Summary, Type, UnboundPartitionSpec, MAIN_BRANCH,
        },
        NamespaceIdent, TableIdent, TableUpdate,
    };
    use iceberg_ext::catalog::rest::{CreateTableRequest, LoadTableResult};
    use sqlx::PgPool;

    use crate::{
        api::{
            iceberg::v1::{
                namespace::NamespaceService as _,
                tables::{DataAccess, LoadTableFilters, SnapshotsQuery, TablesService as _},
                NamespaceParameters, TableParameters,
            },
            management::v1::warehouse::TabularDeleteProfile,
            ApiContext,
        },
        implementations::postgres::{PostgresBackend, SecretsState},
        server::{test::setup, CatalogServer},
        service::{authz::AllowAllAuthorizer, State},
        tests::random_request_metadata,
    };

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
    async fn setup_table_with_snapshots(
        pool: PgPool,
    ) -> (
        ApiContext<State<AllowAllAuthorizer, PostgresBackend, SecretsState>>,
        NamespaceParameters,
        TableIdent,
        LoadTableResult,
    ) {
        let prof = crate::server::test::memory_io_profile();
        let (ctx, warehouse) = setup(
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
            crate::api::iceberg::v1::CreateNamespaceRequest {
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
            DataAccess::not_specified(),
            filters,
            ctx,
            random_request_metadata(),
        )
        .await
        .unwrap();

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
            DataAccess::not_specified(),
            filters,
            ctx,
            random_request_metadata(),
        )
        .await
        .unwrap();

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
        let filters = LoadTableFilters::default();

        let result = CatalogServer::load_table(
            table_params,
            DataAccess::not_specified(),
            filters,
            ctx,
            random_request_metadata(),
        )
        .await
        .unwrap();

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
        let prof = crate::server::test::memory_io_profile();
        let (ctx, warehouse) = setup(
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
            crate::api::iceberg::v1::CreateNamespaceRequest {
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
            DataAccess::not_specified(),
            filters,
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();

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
            DataAccess::not_specified(),
            filters_all,
            ctx,
            random_request_metadata(),
        )
        .await
        .unwrap();

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
            DataAccess::not_specified(),
            filters_all,
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();

        let result_refs = CatalogServer::load_table(
            table_params,
            DataAccess::not_specified(),
            filters_refs,
            ctx,
            random_request_metadata(),
        )
        .await
        .unwrap();

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
}
