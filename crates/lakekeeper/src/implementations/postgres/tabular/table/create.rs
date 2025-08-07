use std::str::FromStr;

use iceberg::{
    spec::{FormatVersion, TableMetadata},
    TableIdent,
};
use iceberg_ext::catalog::rest::ErrorModel;
use lakekeeper_io::Location;
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::{
    api::{self, Result},
    implementations::postgres::{
        dbutils::DBErrorHandler,
        tabular::{
            create_tabular,
            table::{common, DbTableFormatVersion},
            CreateTabular, TabularType,
        },
    },
    service::{CreateTableResponse, NamespaceId, TableCreation, TableId},
};

pub(crate) async fn create_table(
    TableCreation {
        namespace_id,
        table_ident,
        table_metadata,
        metadata_location,
    }: TableCreation<'_>,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> api::Result<CreateTableResponse> {
    let TableIdent { namespace: _, name } = table_ident;
    let location = Location::from_str(table_metadata.location()).map_err(|err| {
        ErrorModel::bad_request(
            format!("Invalid location: '{}'", table_metadata.location()),
            "InvalidLocation",
            Some(Box::new(err)),
        )
    })?;

    let staged_table_id = maybe_delete_staged_table(namespace_id, transaction, name).await?;

    let tabular_id = create_tabular(
        CreateTabular {
            id: table_metadata.uuid(),
            name,
            namespace_id: *namespace_id,
            typ: TabularType::Table,
            metadata_location,
            location: &location,
        },
        transaction,
    )
    .await?;

    insert_table(&table_metadata, transaction, tabular_id).await?;

    common::insert_schemas(table_metadata.schemas_iter(), transaction, tabular_id).await?;
    common::set_current_schema(table_metadata.current_schema_id(), transaction, tabular_id).await?;

    common::insert_partition_specs(
        table_metadata.partition_specs_iter(),
        transaction,
        tabular_id,
    )
    .await?;
    common::set_default_partition_spec(
        transaction,
        tabular_id,
        table_metadata.default_partition_spec().spec_id(),
    )
    .await?;

    common::insert_snapshots(tabular_id, table_metadata.snapshots(), transaction).await?;
    common::insert_snapshot_refs(&table_metadata, transaction).await?;
    common::insert_snapshot_log(table_metadata.history().iter(), transaction, tabular_id).await?;

    common::insert_sort_orders(table_metadata.sort_orders_iter(), transaction, tabular_id).await?;
    common::set_default_sort_order(
        table_metadata.default_sort_order_id(),
        transaction,
        tabular_id,
    )
    .await?;

    common::set_table_properties(tabular_id, table_metadata.properties(), transaction).await?;

    common::insert_metadata_log(
        tabular_id,
        table_metadata.metadata_log().iter().cloned(),
        transaction,
    )
    .await?;

    common::insert_partition_statistics(
        tabular_id,
        table_metadata.partition_statistics_iter(),
        transaction,
    )
    .await?;
    common::insert_table_statistics(tabular_id, table_metadata.statistics_iter(), transaction)
        .await?;

    Ok(CreateTableResponse {
        table_metadata,
        staged_table_id,
    })
}

async fn maybe_delete_staged_table(
    namespace_id: NamespaceId,
    transaction: &mut Transaction<'_, Postgres>,
    name: &String,
    // Returns the staged table id if it was deleted
) -> Result<Option<TableId>> {
    // we delete any staged table which has the same namespace + name
    // staged tables do not have a metadata_location and can be overwritten
    let staged_tabular_id = sqlx::query!(
        r#"DELETE FROM tabular t 
           WHERE t.namespace_id = $1 AND t.name = $2 AND t.metadata_location IS NULL
           RETURNING t.tabular_id
        "#,
        *namespace_id,
        name
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|e| {
        let message = "Error creating table".to_string();
        tracing::warn!(
            ?e,
            "Failed to delete potentially staged table with same ident",
        );
        e.into_error_model(message)
    })?
    .map(|row| TableId::from(row.tabular_id));

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
    tabular_id: Uuid,
) -> Result<()> {
    let _ = sqlx::query!(
        r#"
        INSERT INTO "table" (table_id,
                             table_format_version,
                             last_column_id,
                             last_sequence_number,
                             last_updated_ms,
                             last_partition_id
                             )
        (
            SELECT $1, $2, $3, $4, $5, $6
            WHERE EXISTS (SELECT 1
                FROM active_tables
                WHERE active_tables.table_id = $1))
        RETURNING "table_id"
        "#,
        tabular_id,
        match table_metadata.format_version() {
            FormatVersion::V1 => DbTableFormatVersion::V1,
            FormatVersion::V2 => DbTableFormatVersion::V2,
        } as _,
        table_metadata.last_column_id(),
        table_metadata.last_sequence_number(),
        table_metadata.last_updated_ms(),
        table_metadata.last_partition_id()
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|e| {
        tracing::warn!("Error creating table: {}", e);
        e.into_error_model("Error creating table".to_string())
    })?;
    Ok(())
}
