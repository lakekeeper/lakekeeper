use std::{collections::HashSet, str::FromStr as _};

use iceberg::spec::{TableMetadata, TableMetadataRef};
use itertools::Itertools;
use lakekeeper_io::Location;
use sqlx::{FromRow, Postgres, Row, Transaction};

use crate::{
    WarehouseId,
    implementations::postgres::{
        dbutils::DBErrorHandler,
        tabular::{
            FromTabularRowError, TabularRow,
            table::{
                DbTableFormatVersion, MAX_PARAMETERS, TableUpdateFlags,
                common::{self, expire_metadata_log_entries, remove_snapshot_log_entries},
            },
        },
    },
    server::tables::TableMetadataDiffs,
    service::{
        CommitTableTransactionError, ConversionError, InternalBackendErrors,
        InternalParseLocationError, TableCommit, TableId, TableInfo, TabularNotFound,
        TooManyUpdatesInCommit, UnexpectedTabularInResponse, ViewOrTableInfo,
    },
};

impl From<FromTabularRowError> for CommitTableTransactionError {
    fn from(err: FromTabularRowError) -> Self {
        match err {
            FromTabularRowError::InternalParseLocationError(e) => e.into(),
            FromTabularRowError::InvalidNamespaceIdentifier(e) => e.into(),
        }
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) async fn commit_table_transaction(
    warehouse_id: WarehouseId,
    commits: impl IntoIterator<Item = TableCommit> + Send,
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<Vec<TableInfo>, CommitTableTransactionError> {
    let commits: Vec<TableCommit> = commits.into_iter().collect();
    // Validate commit count so that we do not exceed the maximum number of parameters in a single query
    validate_commit_count(&commits)?;
    let tabular_ids_in_commit = commits
        .iter()
        .map(|c| c.new_metadata.uuid())
        .collect::<HashSet<_>>();

    let results = commits
        .into_iter()
        .map(|c| {
            let TableCommit {
                new_metadata,
                new_metadata_location,
                previous_metadata_location,
                updates,
                diffs,
            } = c;
            let new_location = Location::from_str(new_metadata.location())
                .map_err(InternalParseLocationError::from)?;
            Ok((
                TableMetadataTransition {
                    warehouse_id,
                    previous_metadata_location,
                    new_metadata,
                    new_metadata_location,
                    new_location,
                },
                (updates, diffs),
            ))
        })
        .collect::<Result<Vec<_>, InternalParseLocationError>>();
    let (location_metadata_pairs, table_change_operations): (Vec<_>, Vec<_>) =
        results?.into_iter().unzip();

    // Perform changes in the DB to all sub-tables (schemas, snapshots, partitions etc.)
    for ((updates, diffs), TableMetadataTransition { new_metadata, .. }) in table_change_operations
        .into_iter()
        .zip(location_metadata_pairs.iter())
    {
        let updates = TableUpdateFlags::from(updates.as_slice());
        apply_metadata_changes(transaction, warehouse_id, updates, new_metadata, diffs).await?;
    }

    let new_metadata_lookup = location_metadata_pairs
        .iter()
        .map(|t| (t.new_metadata.uuid(), t.new_metadata.clone()))
        .collect::<std::collections::HashMap<_, _>>();

    // Update tabular (metadata location, fs_location, fs_protocol) and top level table metadata
    // (format_version, last_column_id, last_sequence_number, last_updated_ms, last_partition_id)
    let (mut query_table_update, mut query_tabular_update) =
        build_table_and_tabular_update_queries(location_metadata_pairs)?;

    let updated_tables = query_table_update
        .build()
        .fetch_all(&mut **transaction)
        .await
        .map_err(|e| {
            e.into_catalog_backend_error()
                .append_detail("Error committing Table metadata updates")
        })?;
    let updated_tables_ids: HashSet<uuid::Uuid> =
        updated_tables.into_iter().map(|row| row.get(0)).collect();

    let updated_tabulars = query_tabular_update
        .build()
        .fetch_all(&mut **transaction)
        .await
        .map_err(|e| {
            e.into_catalog_backend_error()
                .append_detail("Error committing Table metadata location updates")
        })?;
    let table_infos = updated_tabulars
        .iter()
        .map(|row| {
            let mut table_or_view_info = TabularRow::from_row(row)
                .map_err(|e| {
                    e.into_catalog_backend_error()
                        .append_detail("Failed to build `TabularRow` after table commit")
                })?
                .try_into_table_or_view(warehouse_id)
                .map_err(CommitTableTransactionError::from)?;

            // Update properties from new metadata, as we don't return them from DB
            // for performance reasons
            let Some(properties) = new_metadata_lookup
                .get(&*table_or_view_info.tabular_id())
                .map(|m| m.properties().clone()) else {
                    return Err(CommitTableTransactionError::from(
                        UnexpectedTabularInResponse::new()
                            .append_detail(format!(
                                "Updated tabular id {} which was not part of the commit",
                                table_or_view_info.tabular_id()
                            )),
                    ));
                };
            match &mut table_or_view_info {
                ViewOrTableInfo::Table(table_info) => {
                    table_info.properties = properties;
                }
               ViewOrTableInfo::View(_view_info) => {
                    // This commit is for tables only
                    debug_assert!(false, "Commit should not return views");
                }
            }

            let tabular_id = table_or_view_info.tabular_id();
            let Some(table_info) = table_or_view_info.into_table_info() else {
                return Err(UnexpectedTabularInResponse::new()
                    .append_detail(format!(
                        "Expected table commit to only return tables, found {tabular_id} among tabulars"
                    ))
                    .into());
            };

            Ok(table_info)
        })
        .collect::<Result<Vec<TableInfo>, CommitTableTransactionError>>()?;
    let updated_tabulars_ids = table_infos
        .iter()
        .map(|t| *t.tabular_id)
        .collect::<HashSet<_>>();

    verify_commit_completeness(
        warehouse_id,
        CommitVerificationData {
            tabular_ids_in_commit,
            updated_tables_ids,
            updated_tabulars_ids,
        },
    )?;

    Ok(table_infos)
}

struct TableMetadataTransition {
    warehouse_id: WarehouseId,
    previous_metadata_location: Option<Location>,
    new_metadata: TableMetadataRef,
    new_metadata_location: Location,
    new_location: Location,
}

struct CommitVerificationData {
    tabular_ids_in_commit: HashSet<uuid::Uuid>,
    updated_tables_ids: HashSet<uuid::Uuid>,
    updated_tabulars_ids: HashSet<uuid::Uuid>,
}

#[allow(clippy::too_many_lines)]
fn build_table_and_tabular_update_queries(
    location_metadata_pairs: Vec<TableMetadataTransition>,
) -> Result<
    (
        sqlx::QueryBuilder<'static, Postgres>,
        sqlx::QueryBuilder<'static, Postgres>,
    ),
    ConversionError,
> {
    let n_commits = location_metadata_pairs.len();
    let mut query_builder_table = sqlx::QueryBuilder::new(
        r#"
        UPDATE "table" as t
        SET table_format_version = c."table_format_version",
            last_column_id = c."last_column_id",
            last_sequence_number = c."last_sequence_number",
            last_updated_ms = c."last_updated_ms",
            last_partition_id = c."last_partition_id",
            next_row_id = c."next_row_id"
        FROM (VALUES
        "#,
    );

    let mut query_builder_tabular = sqlx::QueryBuilder::new(
        r#"
        WITH updated AS (
            UPDATE "tabular" as t
            SET "metadata_location" = c."new_metadata_location",
            "fs_location" = c."fs_location",
            "fs_protocol" = c."fs_protocol"
            FROM (VALUES
        "#,
    );
    for (
        i,
        TableMetadataTransition {
            warehouse_id,
            previous_metadata_location,
            new_metadata,
            new_metadata_location,
            new_location,
        },
    ) in location_metadata_pairs.into_iter().enumerate()
    {
        let fs_protocol = new_location.scheme();
        let fs_location = new_location.authority_and_path();

        let next_row_id = i64::try_from(new_metadata.next_row_id()).map_err(|e| {
            ConversionError::new_external(
                format!(
                    "Next row id is {} but must be between 0 and i64::MAX",
                    new_metadata.next_row_id()
                ),
                e,
            )
        })?;

        query_builder_table.push("(");
        query_builder_table.push_bind(*warehouse_id);
        query_builder_table.push(", ");
        query_builder_table.push_bind(new_metadata.uuid());
        query_builder_table.push(", ");
        query_builder_table.push_bind(DbTableFormatVersion::from(new_metadata.format_version()));
        query_builder_table.push(", ");
        query_builder_table.push_bind(new_metadata.last_column_id());
        query_builder_table.push(", ");
        query_builder_table.push_bind(new_metadata.last_sequence_number());
        query_builder_table.push(", ");
        query_builder_table.push_bind(new_metadata.last_updated_ms());
        query_builder_table.push(", ");
        query_builder_table.push_bind(new_metadata.last_partition_id());
        query_builder_table.push(", ");
        query_builder_table.push_bind(next_row_id);
        query_builder_table.push(")");

        query_builder_tabular.push("(");
        query_builder_tabular.push_bind(*warehouse_id);
        query_builder_tabular.push(", ");
        query_builder_tabular.push_bind(new_metadata.uuid());
        query_builder_tabular.push(", ");
        query_builder_tabular.push_bind(new_metadata_location.to_string());
        query_builder_tabular.push(", ");
        query_builder_tabular.push_bind(fs_location.to_string());
        query_builder_tabular.push(", ");
        query_builder_tabular.push_bind(fs_protocol.to_string());
        query_builder_tabular.push(", ");
        query_builder_tabular.push_bind(previous_metadata_location.map(|l| l.to_string()));
        query_builder_tabular.push(")");

        if i != n_commits - 1 {
            query_builder_table.push(", ");
            query_builder_tabular.push(", ");
        }
    }

    query_builder_table
        .push(") as c(warehouse_id, table_id, table_format_version, last_column_id, last_sequence_number, last_updated_ms, last_partition_id, next_row_id) WHERE c.warehouse_id = t.warehouse_id AND c.table_id = t.table_id");
    query_builder_tabular.push(
        ") as c(warehouse_id, table_id, new_metadata_location, fs_location, fs_protocol, old_metadata_location) WHERE c.warehouse_id = t.warehouse_id AND c.table_id = t.tabular_id AND t.typ = 'table' AND t.metadata_location IS NOT DISTINCT FROM c.old_metadata_location",
    );

    query_builder_table.push(" RETURNING t.table_id");
    // Copy from get_tabular_infos_by_ids
    query_builder_tabular.push(
        r#" RETURNING
                t.warehouse_id,
                t.tabular_id,
                t.namespace_id,
                t.name as tabular_name,
                t.typ as "typ: TabularType",
                t.metadata_location,
                t.updated_at,
                t.protected,
                t.fs_location,
                t.fs_protocol
        )
        SELECT 
            u.*, 
            w.version as warehouse_version,
            n.namespace_name,
            n.version as namespace_version,
            NULL::text[] as view_properties_keys,
            NULL::text[] as view_properties_values,
            NULL::text[] as table_properties_keys,
            NULL::text[] as table_properties_values
        FROM updated u
        INNER JOIN warehouse w ON u.warehouse_id = w.warehouse_id
        INNER JOIN namespace n ON n.namespace_id = u.namespace_id AND n.warehouse_id = u.warehouse_id
        "#,
    );

    Ok((query_builder_table, query_builder_tabular))
}

fn verify_commit_completeness(
    warehouse_id: WarehouseId,
    verification_data: CommitVerificationData,
) -> Result<(), CommitTableTransactionError> {
    let CommitVerificationData {
        tabular_ids_in_commit,
        updated_tables_ids,
        updated_tabulars_ids,
    } = verification_data;

    // Update for "table" table filters on `(warehouse_id, tabular_id)`, so that all tabular
    // IDs are guaranteed to be unique, as they are in the same warehouse.
    let missing_tables = tabular_ids_in_commit.difference(&updated_tables_ids);
    if let Some(missing_id) = missing_tables.into_iter().next() {
        return Err(TabularNotFound::new(warehouse_id, TableId::from(*missing_id)).into());
    }

    // Update for `tabular` table filters on `(warehouse_id, table_id, metadata_location)`.
    let missing_updates = tabular_ids_in_commit.difference(&updated_tabulars_ids);
    if let Some(missing_id) = missing_updates.into_iter().next() {
        return Err(TabularNotFound::new(warehouse_id, TableId::from(*missing_id)).into());
    }
    Ok(())
}

fn validate_commit_count(commits: &[TableCommit]) -> Result<(), TooManyUpdatesInCommit> {
    // Per-commit bind counts
    const PER_COMMIT_TABLE_BINDS: usize = 8;
    const PER_COMMIT_TABULAR_BINDS: usize = 6;
    // Limit is dictated by the larger of the two queries; table is the bottleneck.
    let max_commits_table = MAX_PARAMETERS / PER_COMMIT_TABLE_BINDS;
    let max_commits_tabular = MAX_PARAMETERS / PER_COMMIT_TABULAR_BINDS;
    let max_commits = max_commits_table.min(max_commits_tabular);
    if commits.len() > max_commits {
        return Err(TooManyUpdatesInCommit::new());
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
async fn apply_metadata_changes(
    transaction: &mut Transaction<'_, Postgres>,
    warehouse_id: WarehouseId,
    table_updates: TableUpdateFlags,
    new_metadata: &TableMetadata,
    diffs: TableMetadataDiffs,
) -> Result<(), InternalBackendErrors> {
    let table_id = TableId::from(new_metadata.uuid());
    let TableUpdateFlags {
        snapshot_refs,
        properties,
    } = table_updates;
    // no dependencies
    if !diffs.added_schemas.is_empty() {
        common::insert_schemas(
            diffs
                .added_schemas
                .into_iter()
                .filter_map(|s| new_metadata.schema_by_id(s))
                .collect::<Vec<_>>()
                .into_iter(),
            transaction,
            warehouse_id,
            table_id,
        )
        .await?;
    }

    // must run after insert_schemas
    if let Some(schema_id) = diffs.new_current_schema_id {
        common::set_current_schema(schema_id, transaction, warehouse_id, table_id).await?;
    }

    // No dependencies technically, could depend on columns in schema, so run after set_current_schema
    if !diffs.added_partition_specs.is_empty() {
        common::insert_partition_specs(
            diffs
                .added_partition_specs
                .into_iter()
                .filter_map(|s| new_metadata.partition_spec_by_id(s))
                .collect::<Vec<_>>()
                .into_iter(),
            transaction,
            warehouse_id,
            table_id,
        )
        .await?;
    }

    // Must run after insert_partition_specs
    if let Some(default_spec_id) = diffs.default_partition_spec_id {
        common::set_default_partition_spec(transaction, warehouse_id, table_id, default_spec_id)
            .await?;
    }

    // Should run after insert_schemas
    if !diffs.added_sort_orders.is_empty() {
        common::insert_sort_orders(
            diffs
                .added_sort_orders
                .into_iter()
                .filter_map(|id| new_metadata.sort_order_by_id(id))
                .collect_vec()
                .into_iter(),
            transaction,
            warehouse_id,
            table_id,
        )
        .await?;
    }

    // Must run after insert_sort_orders
    if let Some(default_sort_order_id) = diffs.default_sort_order_id {
        common::set_default_sort_order(default_sort_order_id, transaction, warehouse_id, table_id)
            .await?;
    }

    if !diffs.added_encryption_keys.is_empty() {
        common::insert_table_encryption_keys(
            warehouse_id,
            table_id,
            diffs
                .added_encryption_keys
                .iter()
                .filter_map(|k| new_metadata.encryption_key(k))
                .collect::<Vec<_>>()
                .into_iter(),
            transaction,
        )
        .await?;
    }

    // Must run after insert_schemas & after insert_encryption_keys
    if !diffs.added_snapshots.is_empty() {
        common::insert_snapshots(
            warehouse_id,
            table_id,
            diffs
                .added_snapshots
                .into_iter()
                .filter_map(|s| new_metadata.snapshot_by_id(s))
                .collect::<Vec<_>>()
                .into_iter(),
            transaction,
        )
        .await?;
    }

    // Must run after insert_snapshots
    if snapshot_refs {
        common::insert_snapshot_refs(warehouse_id, new_metadata, transaction).await?;
    }

    // Must run after insert_snapshots, technically not enforced
    if diffs.head_of_snapshot_log_changed
        && let Some(snap) = new_metadata.history().last()
    {
        common::insert_snapshot_log([snap].into_iter(), transaction, warehouse_id, table_id)
            .await?;
    }

    // no deps technically enforced
    if diffs.n_removed_snapshot_log > 0 {
        remove_snapshot_log_entries(
            diffs.n_removed_snapshot_log,
            transaction,
            warehouse_id,
            table_id,
        )
        .await?;
    }

    // no deps technically enforced
    if diffs.expired_metadata_logs > 0 {
        expire_metadata_log_entries(
            warehouse_id,
            table_id,
            diffs.expired_metadata_logs,
            transaction,
        )
        .await?;
    }
    // no deps technically enforced
    if diffs.added_metadata_log > 0 {
        common::insert_metadata_log(
            warehouse_id,
            table_id,
            new_metadata
                .metadata_log()
                .iter()
                .rev()
                .take(diffs.added_metadata_log)
                .rev()
                .cloned(),
            transaction,
        )
        .await?;
    }

    // Must run after insert_snapshots
    if !diffs.added_partition_stats.is_empty() {
        common::insert_partition_statistics(
            warehouse_id,
            table_id,
            diffs
                .added_partition_stats
                .into_iter()
                .filter_map(|s| new_metadata.partition_statistics_for_snapshot(s))
                .collect::<Vec<_>>()
                .into_iter(),
            transaction,
        )
        .await?;
    }
    // Must run after insert_partition_statistics
    if !diffs.added_stats.is_empty() {
        common::insert_table_statistics(
            warehouse_id,
            table_id,
            diffs
                .added_stats
                .into_iter()
                .filter_map(|s| new_metadata.statistics_for_snapshot(s))
                .collect::<Vec<_>>()
                .into_iter(),
            transaction,
        )
        .await?;
    }
    // Must run before remove_snapshots
    if !diffs.removed_stats.is_empty() {
        common::remove_table_statistics(warehouse_id, table_id, diffs.removed_stats, transaction)
            .await?;
    }
    // Must run before remove_snapshots
    if !diffs.removed_partition_stats.is_empty() {
        common::remove_partition_statistics(
            warehouse_id,
            table_id,
            diffs.removed_partition_stats,
            transaction,
        )
        .await?;
    }

    // Must run after insert_snapshots
    if !diffs.removed_snapshots.is_empty() {
        common::remove_snapshots(warehouse_id, table_id, diffs.removed_snapshots, transaction)
            .await?;
    }

    // Must run after set_default_partition_spec
    if !diffs.removed_partition_specs.is_empty() {
        common::remove_partition_specs(
            warehouse_id,
            table_id,
            diffs.removed_partition_specs,
            transaction,
        )
        .await?;
    }

    // Must run after set_default_sort_order
    if !diffs.removed_sort_orders.is_empty() {
        common::remove_sort_orders(
            warehouse_id,
            table_id,
            diffs.removed_sort_orders,
            transaction,
        )
        .await?;
    }

    // Must run after remove_snapshots, and remove_partition_specs and remove_sort_orders
    if !diffs.removed_schemas.is_empty() {
        common::remove_schemas(warehouse_id, table_id, diffs.removed_schemas, transaction).await?;
    }

    // Must run after remove_snapshots
    if !diffs.removed_encryption_keys.is_empty() {
        common::remove_table_encryption_keys(
            warehouse_id,
            table_id,
            &diffs.removed_encryption_keys,
            transaction,
        )
        .await?;
    }

    if properties {
        common::set_table_properties(
            warehouse_id,
            table_id,
            new_metadata.properties(),
            transaction,
        )
        .await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use iceberg::{
        NamespaceIdent,
        spec::{
            FormatVersion, NestedField, NullOrder, Operation, PrimitiveType, Schema, Snapshot,
            SortDirection, SortField, SortOrder, Summary, TableMetadata, TableMetadataBuilder,
            Transform, Type, UnboundPartitionSpec,
        },
    };
    use lakekeeper_io::Location;

    use super::*;
    use crate::{
        api::iceberg::v1::tables::LoadTableFilters,
        implementations::{
            CatalogState,
            postgres::{
                PostgresBackend, namespace::tests::initialize_namespace,
                warehouse::test::initialize_warehouse,
            },
        },
        server::tables::calculate_diffs,
        service::{CatalogTableOps, TableCreation, TableInfo},
    };

    const TEST_LOCATION: &str = "s3://bucket/test/location";

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

    fn sort_order() -> SortOrder {
        let schema = schema();
        SortOrder::builder()
            .with_order_id(1)
            .with_sort_field(SortField {
                source_id: 3,
                transform: Transform::Bucket(4),
                direction: SortDirection::Descending,
                null_order: NullOrder::First,
            })
            .build(&schema)
            .unwrap()
    }

    fn partition_spec() -> UnboundPartitionSpec {
        UnboundPartitionSpec::builder()
            .with_spec_id(0)
            .add_partition_field(2, "y", Transform::Identity)
            .unwrap()
            .build()
    }

    fn builder_without_changes(format_version: FormatVersion) -> TableMetadataBuilder {
        TableMetadataBuilder::new(
            schema(),
            partition_spec(),
            sort_order(),
            TEST_LOCATION.to_string(),
            format_version,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata
        .into_builder(Some(
            "s3://bucket/test/location/metadata/metadata1.json".to_string(),
        ))
    }

    async fn setup_table(pool: sqlx::PgPool) -> (TableInfo, TableMetadata) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let warehouse_id = initialize_warehouse(state.clone(), None, None, None, true).await;
        let namespace_ident = NamespaceIdent::from_vec(vec!["test".to_string()]).unwrap();
        let namespace =
            initialize_namespace(state.clone(), warehouse_id, &namespace_ident, None).await;

        let metadata = builder_without_changes(FormatVersion::V2)
            .build()
            .unwrap()
            .metadata;
        let metadata_location =
            Location::from_str("s3://bucket/test/location/metadata/metadata1.json").unwrap();

        let table_creation = TableCreation {
            warehouse_id,
            namespace_id: namespace.namespace_id(),
            table_ident: &iceberg::TableIdent {
                namespace: namespace.namespace_ident().clone(),
                name: format!("table_{}", uuid::Uuid::now_v7()),
            },
            metadata_location: Some(&metadata_location),
            table_metadata: &metadata,
        };

        let mut t = pool.begin().await.unwrap();
        let (table_info, _staged_table) = PostgresBackend::create_table(table_creation, &mut t)
            .await
            .unwrap();
        t.commit().await.unwrap();

        assert_eq!(table_info.metadata_location, Some(metadata_location));

        (table_info, metadata)
    }

    fn snapshot_1() -> Snapshot {
        Snapshot::builder()
            .with_snapshot_id(1)
            .with_timestamp_ms(chrono::Utc::now().timestamp_millis())
            .with_sequence_number(0)
            .with_schema_id(0)
            .with_manifest_list("/snap-1.avro")
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::from_iter(vec![(
                    "added-files-size".to_string(),
                    "6001".to_string(),
                )]),
            })
            .build()
    }

    #[sqlx::test]
    async fn test_commit_returns_updated_table_info(pool: sqlx::PgPool) {
        let (previous_table_info, previous_metadata) = setup_table(pool.clone()).await;
        let previous_metadata_location = previous_table_info.metadata_location.clone().unwrap();
        let warehouse_id = previous_table_info.warehouse_id;

        let snapshot = snapshot_1();

        let new_metadata_build_result = previous_metadata
            .clone()
            .into_builder(previous_table_info.metadata_location.map(|l| l.to_string()))
            .add_snapshot(snapshot)
            .unwrap()
            .set_properties(HashMap::from_iter(vec![(
                "new_property".to_string(),
                "new_value".to_string(),
            )]))
            .unwrap()
            .build()
            .unwrap();
        let new_metadata = new_metadata_build_result.metadata;
        let updates = new_metadata_build_result.changes;
        let new_metadata_location =
            Location::from_str("s3://bucket/test/location/metadata/metadata2.json").unwrap();

        let commit = TableCommit {
            new_metadata: Arc::new(new_metadata.clone()),
            new_metadata_location: new_metadata_location.clone(),
            previous_metadata_location: Some(previous_metadata_location),
            updates: Arc::new(updates),
            diffs: calculate_diffs(&new_metadata, &previous_metadata, 1, 0),
        };

        let mut t = pool.begin().await.unwrap();
        let new_table_infos = commit_table_transaction(warehouse_id, vec![commit], &mut t)
            .await
            .unwrap();
        t.commit().await.unwrap();

        assert_eq!(new_table_infos.len(), 1);
        let new_table_info = &new_table_infos[0];
        assert_eq!(new_table_info.tabular_id, previous_table_info.tabular_id);
        assert_eq!(
            new_table_info.metadata_location,
            Some(new_metadata_location)
        );
        assert_eq!(new_table_info.location, previous_table_info.location);
        assert_eq!(
            new_table_info.warehouse_id,
            previous_table_info.warehouse_id
        );
        assert_eq!(
            new_table_info.properties.get("new_property"),
            Some(&"new_value".to_string())
        );

        let mut t = pool.begin().await.unwrap();
        let new_metadata_loaded = PostgresBackend::load_tables(
            warehouse_id,
            [TableId::from(new_metadata.uuid())],
            false,
            &LoadTableFilters::default(),
            &mut t,
        )
        .await
        .unwrap();
        t.commit().await.unwrap();
        assert_eq!(new_metadata_loaded.len(), 1);
        let new_metadata_loaded = &new_metadata_loaded[0];
        pretty_assertions::assert_eq!(new_metadata_loaded.table_metadata, new_metadata);
    }
}
