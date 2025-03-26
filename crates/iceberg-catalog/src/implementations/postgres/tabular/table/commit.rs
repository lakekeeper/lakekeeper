use iceberg::spec::{FormatVersion, TableMetadata};
use iceberg_ext::{catalog::rest::ErrorModel, configs::Location};
use itertools::Itertools;
use sqlx::{Postgres, Transaction};

use crate::{
    api,
    catalog::tables::TableMetadataDiffs,
    implementations::postgres::{
        dbutils::DBErrorHandler,
        tabular::table::{
            common::{self, expire_metadata_log_entries, remove_snapshot_log_entries},
            DbTableFormatVersion, TableUpdates, MAX_PARAMETERS,
        },
    },
    service::{storage::split_location, TableCommit},
    WarehouseIdent,
};

pub(crate) async fn commit_table_transaction(
    // We do not need the warehouse_id here, because table_ids are unique across warehouses
    _: WarehouseIdent,
    commits: impl IntoIterator<Item = TableCommit> + Send,
    transaction: &mut Transaction<'_, Postgres>,
) -> api::Result<()> {
    let commits: Vec<TableCommit> = commits.into_iter().collect();
    validate_commit_count(&commits)?;
    let n_commits = commits.len();
    let (meta, atomic): (Vec<_>, Vec<_>) = commits
        .into_iter()
        .map(|c| {
            let TableCommit {
                new_metadata,
                new_metadata_location,
                updates,
                diffs,
            } = c;
            let t = (new_metadata, new_metadata_location);
            (t, (updates, diffs))
        })
        .unzip();

    for ((updates, diffs), (meta, _)) in atomic.into_iter().zip(meta.iter()) {
        let updates = TableUpdates::from(updates.as_slice());
        handle_atomic_updates(transaction, updates, meta, diffs).await?;
    }

    let (mut query_meta_update, mut query_meta_location_update) = build_queries(n_commits, meta)?;

    // futures::try_join didn't work due to concurrent mutable borrow of transaction
    let updated_meta = query_meta_update
        .build()
        .fetch_all(&mut **transaction)
        .await
        .map_err(|e| e.into_error_model("Error committing tablemetadata updates".to_string()))?;

    let updated_meta_location = query_meta_location_update
        .build()
        .fetch_all(&mut **transaction)
        .await
        .map_err(|e| {
            e.into_error_model("Error committing tablemetadata location updates".to_string())
        })?;

    check_post_conditions(updated_meta.len(), n_commits, updated_meta_location.len())?;

    Ok(())
}

fn build_queries(
    n_commits: usize,
    meta: Vec<(TableMetadata, Location)>,
) -> Result<
    (
        sqlx::QueryBuilder<'static, Postgres>,
        sqlx::QueryBuilder<'static, Postgres>,
    ),
    ErrorModel,
> {
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
        SET "metadata_location" = c."metadata_location",
        "fs_location" = c."fs_location",
        "fs_protocol" = c."fs_protocol"
        FROM (VALUES
        "#,
    );
    for (i, (new_metadata, new_metadata_location)) in meta.into_iter().enumerate() {
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
        query_builder_tabular.push(")");

        if i != n_commits - 1 {
            query_builder_table.push(", ");
            query_builder_tabular.push(", ");
        }
    }

    query_builder_table
        .push(") as c(table_id, table_format_version, last_column_id, last_sequence_number, last_updated_ms, last_partition_id) WHERE c.table_id = t.table_id");
    query_builder_tabular.push(
        ") as c(table_id, metadata_location, fs_location, fs_protocol) WHERE c.table_id = t.tabular_id AND t.typ = 'table'",
    );

    query_builder_table.push(" RETURNING t.table_id");
    query_builder_tabular.push(" RETURNING t.tabular_id");

    Ok((query_builder_table, query_builder_tabular))
}

fn check_post_conditions(
    updated_meta_len: usize,
    n_commits: usize,
    updated_meta_location_len: usize,
) -> api::Result<()> {
    if updated_meta_len != n_commits || updated_meta_location_len != n_commits {
        return Err(ErrorModel::internal(
            "Error committing table updates",
            "CommitTableUpdateError",
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
async fn handle_atomic_updates(
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
