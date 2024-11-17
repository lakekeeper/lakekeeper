use crate::api;
use crate::implementations::postgres::dbutils::DBErrorHandler;
use crate::implementations::postgres::tabular::table::{common, DbTableFormatVersion};
use crate::implementations::postgres::tabular::{create_tabular, CreateTabular, TabularType};
use crate::service::{CreateTableResponse, TableCreation};
use iceberg::spec::FormatVersion;
use iceberg::TableIdent;
use iceberg_ext::catalog::rest::ErrorModel;
use iceberg_ext::configs::Location;
use std::str::FromStr;

// TODO: split this into multiple functions
#[allow(clippy::too_many_lines)]
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

    let table_metadata_ser = serde_json::to_value(table_metadata.clone()).map_err(|e| {
        ErrorModel::internal(
            "Error serializing table metadata",
            "TableMetadataSerializationError",
            Some(Box::new(e)),
        )
    })?;

    // we delete any staged table which has the same namespace + name
    // staged tables do not have a metadata_location and can be overwritten
    if sqlx::query!(r#"DELETE FROM tabular t WHERE t.namespace_id = $1 AND t.name = $2 AND t.metadata_location IS NULL"#, *namespace_id, name)
        .execute(&mut **transaction)
        .await
        .map_err(|e| {
            let message = "Error creating table".to_string();
            tracing::warn!("Failed to delete potentially staged table with same ident due to: '{}'", message);
            e.into_error_model(message)
        })?.rows_affected() > 0 {
        tracing::debug!(
            "Overwriting staged tabular entry for table '{}' within namespace_id: '{}'",
            name,
            namespace_id
        );
    }

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

    let _update_result = sqlx::query!(
        r#"
        INSERT INTO "table" (table_id,
                             metadata,
                             table_format_version,
                             last_column_id,
                             last_sequence_number,
                             last_updated_ms,
                             last_partition_id)
        (
            SELECT $1, $2, $3, $4, $5, $6, $7
            WHERE EXISTS (SELECT 1
                FROM active_tables
                WHERE active_tables.table_id = $1))
        RETURNING "table_id"
        "#,
        tabular_id,
        table_metadata_ser,
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

    common::insert_schemas(table_metadata.schemas_iter(), transaction, tabular_id).await?;
    common::insert_current_schema(&table_metadata, transaction, tabular_id).await?;

    common::insert_partition_specs(
        table_metadata.partition_specs_iter(),
        transaction,
        tabular_id,
    )
    .await?;
    common::insert_default_partition_spec(
        transaction,
        tabular_id,
        table_metadata.default_partition_spec(),
    )
    .await?;

    common::insert_snapshots(tabular_id, table_metadata.snapshots(), transaction).await?;
    common::set_current_snapshot(&table_metadata, transaction).await?;
    common::insert_snapshot_refs(&table_metadata, transaction).await?;
    common::insert_snapshot_log(table_metadata.history().iter(), transaction, tabular_id).await?;

    common::insert_sort_orders(table_metadata.sort_orders_iter(), transaction, tabular_id).await?;
    common::insert_default_sort_order(&table_metadata, transaction).await?;

    common::set_table_properties(tabular_id, table_metadata.properties(), transaction).await?;

    common::insert_metadata_log(
        tabular_id,
        table_metadata.metadata_log().iter().cloned(),
        transaction,
    )
    .await?;

    Ok(CreateTableResponse { table_metadata })
}
