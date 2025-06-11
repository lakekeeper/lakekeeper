use std::collections::HashSet;

use iceberg::spec::{FormatVersion, TableMetadata};
use iceberg_ext::{catalog::rest::ErrorModel, configs::Location};
use itertools::Itertools;
use sqlx::{Postgres, Row, Transaction};

use crate::{
    api,
    catalog::tables::{TableMetadataDiffs, CONCURRENT_UPDATE_ERROR_TYPE},
    implementations::postgres::{
        dbutils::DBErrorHandler,
        tabular::table::{
            common::{self, expire_metadata_log_entries, remove_snapshot_log_entries},
            DbTableFormatVersion, TableUpdates, MAX_PARAMETERS,
        },
    },
    service::{storage::split_location, TableCommit},
    WarehouseId,
};

pub(crate) async fn commit_table_transaction(
    // We do not need the warehouse_id here, because table_ids are unique across warehouses
    _: WarehouseId,
    commits: impl IntoIterator<Item = TableCommit> + Send,
    transaction: &mut Transaction<'_, Postgres>,
) -> api::Result<()> {
    let commits: Vec<TableCommit> = commits.into_iter().collect();
    // Validate commit count so that we do not exceed the maximum number of parameters in a single query
    validate_commit_count(&commits)?;
    let tabular_ids_in_commit = commits
        .iter()
        .map(|c| c.new_metadata.uuid())
        .collect::<HashSet<_>>();

    let (location_metadata_pairs, table_change_operations): (Vec<_>, Vec<_>) = commits
        .into_iter()
        .map(|c| {
            let TableCommit {
                new_metadata,
                new_metadata_location,
                previous_metadata_location,
                updates,
                diffs,
            } = c;
            (
                TableMetadataTransition {
                    previous_metadata_location,
                    new_metadata,
                    new_metadata_location,
                },
                (updates, diffs),
            )
        })
        .unzip();

    // Perform changes in the DB to all sub-tables (schemas, snapshots, partitions etc.)
    for ((updates, diffs), TableMetadataTransition { new_metadata, .. }) in table_change_operations
        .into_iter()
        .zip(location_metadata_pairs.iter())
    {
        let updates = TableUpdates::from(updates.as_slice());
        apply_metadata_changes(transaction, updates, new_metadata, diffs).await?;
    }

    // Update tabular (metadata location, fs_location, fs_protocol) and top level table metadata
    // (format_version, last_column_id, last_sequence_number, last_updated_ms, last_partition_id)
    let (mut query_meta_update, mut query_meta_location_update) =
        build_table_and_tabular_update_queries(location_metadata_pairs)?;

    let updated_tables = query_meta_update
        .build()
        .fetch_all(&mut **transaction)
        .await
        .map_err(|e| e.into_error_model("Error committing tablemetadata updates".to_string()))?;
    let updated_tables_ids: HashSet<uuid::Uuid> =
        updated_tables.into_iter().map(|row| row.get(0)).collect();

    let updated_tabulars = query_meta_location_update
        .build()
        .fetch_all(&mut **transaction)
        .await
        .map_err(|e| {
            e.into_error_model("Error committing tablemetadata location updates".to_string())
        })?;
    let updated_tabulars_ids: HashSet<uuid::Uuid> =
        updated_tabulars.into_iter().map(|row| row.get(0)).collect();

    verify_commit_completeness(CommitVerificationData {
        tabular_ids_in_commit,
        updated_tables_ids,
        updated_tabulars_ids,
    })?;

    Ok(())
}

struct TableMetadataTransition {
    previous_metadata_location: Option<Location>,
    new_metadata: TableMetadata,
    new_metadata_location: Location,
}

struct CommitVerificationData {
    tabular_ids_in_commit: HashSet<uuid::Uuid>,
    updated_tables_ids: HashSet<uuid::Uuid>,
    updated_tabulars_ids: HashSet<uuid::Uuid>,
}

fn build_table_and_tabular_update_queries(
    location_metadata_pairs: Vec<TableMetadataTransition>,
) -> Result<
    (
        sqlx::QueryBuilder<'static, Postgres>,
        sqlx::QueryBuilder<'static, Postgres>,
    ),
    ErrorModel,
> {
    let n_commits = location_metadata_pairs.len();
    let mut query_builder_table = sqlx::QueryBuilder::new(
        r#"
        UPDATE "table" as t
        SET table_format_version = c."table_format_version",
            last_column_id = c."last_column_id",
            last_sequence_number = c."last_sequence_number",
            last_updated_ms = c."last_updated_ms",
            last_partition_id = c."last_partition_id"
        FROM (VALUES
        "#,
    );

    let mut query_builder_tabular = sqlx::QueryBuilder::new(
        r#"
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
            previous_metadata_location,
            new_metadata,
            new_metadata_location,
        },
    ) in location_metadata_pairs.into_iter().enumerate()
    {
        let (fs_protocol, fs_location) = split_location(new_metadata.location())?;

        query_builder_table.push("(");
        query_builder_table.push_bind(new_metadata.uuid());
        query_builder_table.push(", ");
        query_builder_table.push_bind(match new_metadata.format_version() {
            FormatVersion::V1 => DbTableFormatVersion::V1,
            FormatVersion::V2 => DbTableFormatVersion::V2,
        });
        query_builder_table.push(", ");
        query_builder_table.push_bind(new_metadata.last_column_id());
        query_builder_table.push(", ");
        query_builder_table.push_bind(new_metadata.last_sequence_number());
        query_builder_table.push(", ");
        query_builder_table.push_bind(new_metadata.last_updated_ms());
        query_builder_table.push(", ");
        query_builder_table.push_bind(new_metadata.last_partition_id());
        query_builder_table.push(")");

        query_builder_tabular.push("(");
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
        .push(") as c(table_id, table_format_version, last_column_id, last_sequence_number, last_updated_ms, last_partition_id) WHERE c.table_id = t.table_id");
    query_builder_tabular.push(
        ") as c(table_id, new_metadata_location, fs_location, fs_protocol, old_metadata_location) WHERE c.table_id = t.tabular_id AND t.typ = 'table' AND t.metadata_location IS NOT DISTINCT FROM c.old_metadata_location",
    );

    query_builder_table.push(" RETURNING t.table_id");
    query_builder_tabular.push(" RETURNING t.tabular_id");

    Ok((query_builder_table, query_builder_tabular))
}

fn verify_commit_completeness(verification_data: CommitVerificationData) -> api::Result<()> {
    let CommitVerificationData {
        tabular_ids_in_commit,
        updated_tables_ids,
        updated_tabulars_ids,
    } = verification_data;

    // Update for "table" table filters on `tabular_id`
    if tabular_ids_in_commit != updated_tables_ids {
        let missing_ids = tabular_ids_in_commit
            .difference(&updated_tables_ids)
            .collect_vec();
        return Err(ErrorModel::bad_request(
            format!("Tables with the following IDs no longer exist: {missing_ids:?}"),
            "TableNotFound".to_string(),
            None,
        )
        .into());
    }

    // Update for `tabular` table filters on `table_id` and `metadata_location`.
    if tabular_ids_in_commit != updated_tabulars_ids {
        let missing_ids = tabular_ids_in_commit
            .difference(&updated_tabulars_ids)
            .collect_vec();
        return Err(ErrorModel::bad_request(
            format!("Concurrent updates to tables with IDs: {missing_ids:?}"),
            CONCURRENT_UPDATE_ERROR_TYPE,
            None,
        )
        .into());
    }

    Ok(())
}

fn validate_commit_count(commits: &[TableCommit]) -> api::Result<()> {
    if commits.len() > (MAX_PARAMETERS / 4) {
        return Err(ErrorModel::bad_request(
            "Too many updates in single commit",
            "TooManyTablesForCommit".to_string(),
            None,
        )
        .into());
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
async fn apply_metadata_changes(
    transaction: &mut Transaction<'_, Postgres>,
    table_updates: TableUpdates,
    new_metadata: &TableMetadata,
    diffs: TableMetadataDiffs,
) -> api::Result<()> {
    let TableUpdates {
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
            new_metadata.uuid(),
        )
        .await?;
    }

    // must run after insert_schemas
    if let Some(schema_id) = diffs.new_current_schema_id {
        common::set_current_schema(schema_id, transaction, new_metadata.uuid()).await?;
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
            new_metadata.uuid(),
        )
        .await?;
    }

    // Must run after insert_partition_specs
    if let Some(default_spec_id) = diffs.default_partition_spec_id {
        common::set_default_partition_spec(transaction, new_metadata.uuid(), default_spec_id)
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
            new_metadata.uuid(),
        )
        .await?;
    }

    // Must run after insert_sort_orders
    if let Some(default_sort_order_id) = diffs.default_sort_order_id {
        common::set_default_sort_order(default_sort_order_id, transaction, new_metadata.uuid())
            .await?;
    }

    // Must run after insert_schemas
    if !diffs.added_snapshots.is_empty() {
        common::insert_snapshots(
            new_metadata.uuid(),
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
        common::insert_snapshot_refs(new_metadata, transaction).await?;
    }

    // Must run after insert_snapshots, technically not enforced
    if diffs.head_of_snapshot_log_changed {
        if let Some(snap) = new_metadata.history().last() {
            common::insert_snapshot_log([snap].into_iter(), transaction, new_metadata.uuid())
                .await?;
        }
    }

    // no deps technically enforced
    if diffs.n_removed_snapshot_log > 0 {
        remove_snapshot_log_entries(
            diffs.n_removed_snapshot_log,
            transaction,
            new_metadata.uuid(),
        )
        .await?;
    }

    // no deps technically enforced
    if diffs.expired_metadata_logs > 0 {
        expire_metadata_log_entries(
            new_metadata.uuid(),
            diffs.expired_metadata_logs,
            transaction,
        )
        .await?;
    }
    // no deps technically enforced
    if diffs.added_metadata_log > 0 {
        common::insert_metadata_log(
            new_metadata.uuid(),
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
            new_metadata.uuid(),
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
            new_metadata.uuid(),
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
        common::remove_table_statistics(new_metadata.uuid(), diffs.removed_stats, transaction)
            .await?;
    }
    // Must run before remove_snapshots
    if !diffs.removed_partition_stats.is_empty() {
        common::remove_partition_statistics(
            new_metadata.uuid(),
            diffs.removed_partition_stats,
            transaction,
        )
        .await?;
    }

    // Must run after insert_snapshots
    if !diffs.removed_snapshots.is_empty() {
        common::remove_snapshots(new_metadata.uuid(), diffs.removed_snapshots, transaction).await?;
    }

    // Must run after set_default_partition_spec
    if !diffs.removed_partition_specs.is_empty() {
        common::remove_partition_specs(
            new_metadata.uuid(),
            diffs.removed_partition_specs,
            transaction,
        )
        .await?;
    }

    // Must run after set_default_sort_order
    if !diffs.removed_sort_orders.is_empty() {
        common::remove_sort_orders(new_metadata.uuid(), diffs.removed_sort_orders, transaction)
            .await?;
    }

    // Must run after remove_snapshots, and remove_partition_specs and remove_sort_orders
    if !&diffs.removed_schemas.is_empty() {
        common::remove_schemas(new_metadata.uuid(), diffs.removed_schemas, transaction).await?;
    }

    if properties {
        common::set_table_properties(new_metadata.uuid(), new_metadata.properties(), transaction)
            .await?;
    }
    Ok(())
}
