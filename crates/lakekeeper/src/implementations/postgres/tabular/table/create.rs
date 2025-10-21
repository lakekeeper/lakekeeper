use std::str::FromStr;

use iceberg::{spec::TableMetadata, TableIdent};
use lakekeeper_io::Location;
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::{
    implementations::postgres::{
        dbutils::DBErrorHandler,
        tabular::{
            create_tabular,
            table::{
                common::{self},
                next_row_id_as_i64, DbTableFormatVersion,
            },
            CreateTabular, TabularType,
        },
    },
    service::{
        AuthZTableInfo as _, CatalogBackendError, CreateTableError, InternalBackendErrors,
        InternalParseLocationError, NamespaceId, StagedTableId, TableCreation, TableId, TableInfo,
        UnexpectedTabularInResponse,
    },
    WarehouseId,
};

#[allow(clippy::too_many_lines)]
pub(crate) async fn create_table(
    TableCreation {
        warehouse_id,
        namespace_id,
        table_ident,
        table_metadata,
        metadata_location,
    }: TableCreation<'_>,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<(TableInfo, Option<StagedTableId>), CreateTableError> {
    let TableIdent { namespace: _, name } = table_ident;
    let location =
        Location::from_str(table_metadata.location()).map_err(InternalParseLocationError::from)?;

    let staged_table_id =
        maybe_delete_staged_tabular(warehouse_id, namespace_id, transaction, name).await?;

    let tabular_info = create_tabular(
        CreateTabular {
            id: table_metadata.uuid(),
            name,
            namespace_id: *namespace_id,
            warehouse_id: *warehouse_id,
            typ: TabularType::Table,
            metadata_location,
            location: &location,
        },
        transaction,
    )
    .await?;
    let Some(table_info) = tabular_info.into_table_info() else {
        return Err(UnexpectedTabularInResponse::new().into());
    };
    let table_id = table_info.table_id();

    insert_table(table_metadata, transaction, *warehouse_id, table_id).await?;

    common::insert_schemas(
        table_metadata.schemas_iter(),
        transaction,
        warehouse_id,
        table_id,
    )
    .await?;
    common::set_current_schema(
        table_metadata.current_schema_id(),
        transaction,
        warehouse_id,
        table_id,
    )
    .await?;
    common::insert_partition_specs(
        table_metadata.partition_specs_iter(),
        transaction,
        warehouse_id,
        table_id,
    )
    .await?;
    common::set_default_partition_spec(
        transaction,
        warehouse_id,
        table_id,
        table_metadata.default_partition_spec().spec_id(),
    )
    .await?;
    common::insert_snapshots(
        warehouse_id,
        table_id,
        table_metadata.snapshots(),
        transaction,
    )
    .await?;
    common::insert_snapshot_refs(warehouse_id, table_metadata, transaction).await?;
    common::insert_snapshot_log(
        table_metadata.history().iter(),
        transaction,
        warehouse_id,
        table_id,
    )
    .await?;

    common::insert_sort_orders(
        table_metadata.sort_orders_iter(),
        transaction,
        warehouse_id,
        table_id,
    )
    .await?;
    common::set_default_sort_order(
        table_metadata.default_sort_order_id(),
        transaction,
        warehouse_id,
        table_id,
    )
    .await?;

    common::set_table_properties(
        warehouse_id,
        table_id,
        table_metadata.properties(),
        transaction,
    )
    .await?;

    common::insert_metadata_log(
        warehouse_id,
        table_id,
        table_metadata.metadata_log().iter().cloned(),
        transaction,
    )
    .await?;

    common::insert_partition_statistics(
        warehouse_id,
        table_id,
        table_metadata.partition_statistics_iter(),
        transaction,
    )
    .await?;
    common::insert_table_statistics(
        warehouse_id,
        table_id,
        table_metadata.statistics_iter(),
        transaction,
    )
    .await?;
    common::insert_table_encryption_keys(
        warehouse_id,
        table_id,
        table_metadata.encryption_keys_iter(),
        transaction,
    )
    .await?;

    Ok((table_info, staged_table_id))
}

async fn maybe_delete_staged_tabular(
    warehouse_id: WarehouseId,
    namespace_id: NamespaceId,
    transaction: &mut Transaction<'_, Postgres>,
    name: &String,
    // Returns the staged table id if it was deleted
) -> Result<Option<StagedTableId>, CatalogBackendError> {
    // we delete any staged table which has the same namespace + name
    // staged tables do not have a metadata_location and can be overwritten
    let staged_tabular_id = sqlx::query!(
        r#"DELETE FROM tabular t 
           WHERE t.warehouse_id = $3 AND t.namespace_id = $1 AND t.name = $2 AND t.metadata_location IS NULL
           RETURNING t.tabular_id
        "#,
        *namespace_id,
        name,
        *warehouse_id
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|e| {
        e.into_catalog_backend_error()
            .append_detail("Failed to find and delete staged table")
    })?
    .map(|row| StagedTableId(TableId::from(row.tabular_id)));

    if staged_tabular_id.is_some() {
        tracing::debug!(
            "Overwriting staged tabular entry for table '{}' within namespace_id: '{}'",
            name,
            namespace_id
        );
    }

    Ok(staged_tabular_id)
}

async fn insert_table(
    table_metadata: &TableMetadata,
    transaction: &mut Transaction<'_, Postgres>,
    warehouse_id: Uuid,
    table_id: TableId,
) -> Result<(), InternalBackendErrors> {
    let next_row_id = next_row_id_as_i64(table_metadata.next_row_id())?;
    let _ = sqlx::query!(
        r#"
        INSERT INTO "table" (warehouse_id,
                             table_id,
                             table_format_version,
                             last_column_id,
                             last_sequence_number,
                             last_updated_ms,
                             last_partition_id,
                             next_row_id
                             )
        (
            SELECT $1, $2, $3, $4, $5, $6, $7, $8
            WHERE EXISTS (SELECT 1
                FROM active_tables
                WHERE active_tables.warehouse_id = $1
                    AND active_tables.table_id = $2))
        RETURNING "table_id"
        "#,
        warehouse_id,
        *table_id,
        DbTableFormatVersion::from(table_metadata.format_version()) as _,
        table_metadata.last_column_id(),
        table_metadata.last_sequence_number(),
        table_metadata.last_updated_ms(),
        table_metadata.last_partition_id(),
        next_row_id
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|e| {
        e.into_catalog_backend_error()
            .append_detail("Failed to insert table")
    })?;
    Ok(())
}
