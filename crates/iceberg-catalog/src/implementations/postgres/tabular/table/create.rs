use crate::api;
use crate::implementations::postgres::dbutils::DBErrorHandler;
use crate::implementations::postgres::tabular::table::DbTableFormatVersion;
use crate::implementations::postgres::tabular::{create_tabular, CreateTabular, TabularType};
use crate::service::{CreateTableResponse, TableCreation};
use iceberg::spec::{
    BoundPartitionSpecRef, FormatVersion, MetadataLog, SnapshotRef, TableMetadata,
};
use iceberg::TableIdent;
use iceberg_ext::catalog::rest::ErrorModel;
use iceberg_ext::configs::Location;
use sqlx::{PgConnection, Postgres, Transaction};
use std::collections::HashMap;
use std::str::FromStr;
use uuid::Uuid;

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

    let table_metadata_ser = serde_json::to_value(table_metadata.clone()).map_err(|e| {
        ErrorModel::internal(
            "Error serializing table metadata",
            "TableMetadataSerializationError",
            Some(Box::new(e)),
        )
    })?;

    let tabular_id = create_tabular(
        CreateTabular {
            id: table_metadata.uuid(),
            name,
            namespace_id: *namespace_id,
            typ: TabularType::Table,
            metadata_location,
            location: &Location::from_str(table_metadata.location()).map_err(|err| {
                ErrorModel::bad_request(
                    format!("Invalid location: '{}'", table_metadata.location()),
                    "InvalidLocation",
                    Some(Box::new(err)),
                )
            })?,
        },
        transaction,
    )
    .await?;
    let last_seq = table_metadata.last_sequence_number();
    let last_col = table_metadata.last_column_id();
    let last_updated = table_metadata.last_updated_ms();
    let last_partition = table_metadata.last_partition_id();

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
        ON CONFLICT ON CONSTRAINT "table_pkey"
        DO UPDATE SET "metadata" = $2
        RETURNING "table_id"
        "#,
        tabular_id,
        table_metadata_ser,
        match table_metadata.format_version() {
            FormatVersion::V1 => DbTableFormatVersion::V1,
            FormatVersion::V2 => DbTableFormatVersion::V2,
        } as _,
        last_col,
        last_seq,
        last_updated,
        last_partition
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|e| {
        tracing::warn!("Error creating table: {}", e);
        e.into_error_model("Error creating table".to_string())
    })?;

    insert_schemas(&table_metadata, transaction, tabular_id).await?;
    insert_current_schema(&table_metadata, transaction, tabular_id).await?;

    insert_partition_specs(&table_metadata, transaction, tabular_id).await?;

    insert_default_partition_spec(
        transaction,
        tabular_id,
        table_metadata.default_partition_spec(),
    )
    .await?;

    set_table_properties(table_metadata.properties(), tabular_id, transaction).await?;

    insert_snapshots(table_metadata.snapshots(), transaction, tabular_id).await?;
    set_current_snapshot(&table_metadata, transaction, tabular_id).await?;

    insert_sort_orders(&table_metadata, transaction, tabular_id).await?;
    insert_default_sort_order(&table_metadata, transaction, tabular_id).await?;

    insert_snapshot_log(&table_metadata, transaction, tabular_id).await?;

    insert_metadata_log(
        table_metadata.metadata_log().iter().map(|s| s.clone()),
        transaction,
        tabular_id,
    )
    .await?;

    insert_snapshot_refs(&table_metadata, transaction, tabular_id).await?;

    Ok(CreateTableResponse { table_metadata })
}

pub(super) async fn insert_schemas(
    table_metadata: &TableMetadata,
    transaction: &mut Transaction<'_, Postgres>,
    tabular_id: Uuid,
) -> api::Result<()> {
    let schemas = table_metadata.schemas_iter().len();
    let mut ids = Vec::with_capacity(schemas);
    let mut table_ids = Vec::with_capacity(schemas);
    let mut schemas = Vec::with_capacity(schemas);

    for s in table_metadata.schemas_iter() {
        ids.push(s.schema_id());
        table_ids.push(tabular_id);
        schemas.push(serde_json::to_value(s).map_err(|er| {
            ErrorModel::internal(
                "Error serializing schema",
                "SchemaSerializationError",
                Some(Box::new(er)),
            )
        })?);
    }

    let _ = sqlx::query!(
        r#"INSERT INTO table_schema(schema_id, table_id, schema)
           SELECT * FROM UNNEST($1::INT[], $2::UUID[], $3::JSONB[])
           ON CONFLICT DO NOTHING"#,
        &ids,
        &table_ids,
        &schemas
    )
    .execute(&mut **transaction)
    .await
    .map_err(|err| {
        tracing::warn!("Error creating table: {}", err);
        err.into_error_model("Error inserting table schema".to_string())
    })?;

    Ok(())
}

pub(super) async fn insert_current_schema(
    table_metadata: &TableMetadata,
    transaction: &mut Transaction<'_, Postgres>,
    tabular_id: Uuid,
) -> api::Result<()> {
    let _ = sqlx::query!(
        r#"INSERT INTO table_current_schema (table_id, schema_id) VALUES ($1, $2)
           ON CONFLICT (table_id) DO UPDATE SET schema_id = EXCLUDED.schema_id"#,
        tabular_id,
        table_metadata.current_schema_id()
    )
    .execute(&mut **transaction)
    .await
    .map_err(|err| {
        tracing::warn!("Error creating table: {}", err);
        err.into_error_model("Error inserting table current schema".to_string())
    })?;
    Ok(())
}

pub(crate) async fn insert_partition_specs(
    table_metadata: &TableMetadata,
    transaction: &mut Transaction<'_, Postgres>,
    tabular_id: Uuid,
) -> api::Result<()> {
    let partition_specs = table_metadata.partition_specs_iter();
    for part_spec in partition_specs {
        let _ = sqlx::query!(
            r#"INSERT INTO table_partition_spec(partition_spec_id, table_id, partition_spec) VALUES ($1, $2, $3)
                ON CONFLICT DO NOTHING"#,
            part_spec.spec_id(),
            tabular_id,
            serde_json::to_value(part_spec).map_err(|er| ErrorModel::internal(
                "Error serializing partition spec",
                "PartitionSpecSerializationError",
                Some(Box::new(er)),
            ))?
        )
            .execute(&mut **transaction)
            .await
            .map_err(|err| {
                tracing::warn!("Error creating table: {}", err);
                err.into_error_model("Error inserting table partition spec".to_string())
            })?;
    }

    Ok(())
}

pub(crate) async fn insert_default_partition_spec(
    transaction: &mut Transaction<'_, Postgres>,
    tabular_id: Uuid,
    default_spec: &BoundPartitionSpecRef,
) -> api::Result<()> {
    // insert default part spec
    let _ = sqlx::query!(
        r#"INSERT INTO table_default_partition_spec(partition_spec_id, table_id, schema_id) VALUES ($1, $2, $3)
           ON CONFLICT (table_id) DO UPDATE SET schema_id = EXCLUDED.schema_id, partition_spec_id = EXCLUDED.partition_spec_id"#,
        default_spec.spec_id(),
        tabular_id,
        default_spec.schema_ref().schema_id(),
    )
        .execute(&mut **transaction)
        .await
        .map_err(|err| {
            tracing::warn!("Error creating table: {}", err);
            err.into_error_model("Error inserting table default partition spec".to_string())
        })?;
    Ok(())
}

pub(crate) async fn insert_sort_orders(
    table_metadata: &TableMetadata,
    transaction: &mut Transaction<'_, Postgres>,
    tabular_id: Uuid,
) -> api::Result<()> {
    for sort_order in table_metadata.sort_orders_iter() {
        let _ = sqlx::query!(
            r#"INSERT INTO table_sort_order(sort_order_id, table_id, sort_order) VALUES ($1, $2, $3)"#,
            sort_order.order_id,
            tabular_id,
            serde_json::to_value(sort_order).map_err(|er| ErrorModel::internal(
                "Error serializing sort order",
                "SortOrderSerializationError",
                Some(Box::new(er)),
            ))?
        ).execute(&mut **transaction).await.map_err(|err| {
            tracing::warn!("Error creating table: {}", err);
            err.into_error_model("Error inserting table sort order".to_string())
        })?;
    }

    Ok(())
}

pub(crate) async fn insert_default_sort_order(
    table_metadata: &TableMetadata,
    transaction: &mut Transaction<'_, Postgres>,
    tabular_id: Uuid,
) -> api::Result<()> {
    let _ = sqlx::query!(
        r#"INSERT INTO table_default_sort_order(table_id, sort_order_id) VALUES ($1, $2)"#,
        tabular_id,
        table_metadata.default_sort_order_id(),
    )
    .execute(&mut **transaction)
    .await
    .map_err(|err| {
        tracing::warn!("Error creating table: {}", err);
        err.into_error_model("Error inserting table sort order".to_string())
    })?;
    Ok(())
}

pub(super) async fn set_current_snapshot(
    table_metadata: &TableMetadata,
    transaction: &mut Transaction<'_, Postgres>,
    tabular_id: Uuid,
) -> api::Result<()> {
    // set current snap
    if let Some(current_snapshot) = table_metadata.current_snapshot() {
        let _ = sqlx::query!(
            r#"INSERT INTO table_current_snapshot(snapshot_id, table_id) VALUES ($1, $2)
                ON CONFLICT (table_id) DO UPDATE SET snapshot_id = EXCLUDED.snapshot_id"#,
            current_snapshot.snapshot_id(),
            tabular_id
        )
        .execute(&mut **transaction)
        .await
        .map_err(|err| {
            tracing::warn!("Error creating table: {}", err);
            err.into_error_model("Error inserting table current snapshot".to_string())
        })?;
    }
    Ok(())
}

async fn insert_snapshot_log(
    table_metadata: &TableMetadata,
    transaction: &mut Transaction<'_, Postgres>,
    tabular_id: Uuid,
) -> api::Result<()> {
    for log in table_metadata.history() {
        let _ = sqlx::query!(
            r#"INSERT INTO table_snapshot_log(snapshot_id, table_id, timestamp) VALUES ($1, $2, $3)"#,
            log.snapshot_id,
            tabular_id,
            log.timestamp_ms()
        )
            .execute(&mut **transaction)
            .await
            .map_err(|err| {
                tracing::warn!("Error creating table: {}", err);
                err.into_error_model("Error inserting table snapshot log".to_string())
            })?;
    }
    Ok(())
}

pub(super) async fn insert_metadata_log(
    log: impl ExactSizeIterator<Item = MetadataLog>,
    transaction: &mut Transaction<'_, Postgres>,
    tabular_id: Uuid,
) -> api::Result<()> {
    let mut timestamps = Vec::with_capacity(log.len());
    let mut metadata_files = Vec::with_capacity(log.len());

    for MetadataLog {
        timestamp_ms,
        metadata_file,
    } in log
    {
        timestamps.push(timestamp_ms);
        metadata_files.push(metadata_file);
    }

    let _ = sqlx::query!(
        r#"INSERT INTO table_metadata_log(table_id, timestamp, metadata_file)
       SELECT $1, unnest($2::BIGINT[]), unnest($3::TEXT[])"#,
        tabular_id,
        &timestamps,
        &metadata_files
    )
    .execute(&mut **transaction)
    .await
    .map_err(|err| {
        tracing::warn!("Error creating table: {}", err);
        err.into_error_model("Error inserting table metadata log".to_string())
    })?;
    Ok(())
}

pub(super) async fn insert_snapshot_refs(
    table_metadata: &TableMetadata,
    transaction: &mut Transaction<'_, Postgres>,
    tabular_id: Uuid,
) -> api::Result<()> {
    let mut refnames = Vec::new();
    let mut snapshot_ids = Vec::new();
    let mut retentions = Vec::new();

    for (refname, snapshot_ref) in table_metadata.refs() {
        refnames.push(refname.clone());
        snapshot_ids.push(snapshot_ref.snapshot_id);
        retentions.push(serde_json::to_value(&snapshot_ref.retention).map_err(|er| {
            ErrorModel::internal(
                "Error serializing retention",
                "RetentionSerializationError",
                Some(Box::new(er)),
            )
        })?);
    }

    let _ = sqlx::query!(
        r#"
        WITH deleted AS (
            DELETE FROM table_refs
            WHERE table_id = $1 AND table_ref_name = ANY($2::TEXT[])
        )
        INSERT INTO table_refs(table_id,
                              table_ref_name,
                              snapshot_id,
                              retention)
        SELECT $1, unnest($2::TEXT[]), unnest($3::BIGINT[]), unnest($4::JSONB[])"#,
        tabular_id,
        &refnames,
        &snapshot_ids,
        &retentions,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|err| {
        tracing::warn!("Error creating table: {}", err);
        err.into_error_model("Error inserting table refs".to_string())
    })?;

    Ok(())
}

pub(super) async fn remove_snapshots(
    table_id: Uuid,
    snapshot_ids: Vec<i64>,
    transaction: &mut Transaction<'_, Postgres>,
) -> api::Result<()> {
    let _ = sqlx::query!(
        r#"DELETE FROM table_snapshot WHERE table_id = $1 AND snapshot_id = ANY($2::BIGINT[])"#,
        table_id,
        &snapshot_ids,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|err| {
        tracing::warn!("Error creating table: {}", err);
        err.into_error_model("Error deleting table snapshots".to_string())
    })?;

    Ok(())
}

pub(super) async fn insert_snapshots(
    snapshots: impl ExactSizeIterator<Item = &SnapshotRef>,
    transaction: &mut Transaction<'_, Postgres>,
    tabular_id: Uuid,
) -> api::Result<()> {
    let snap_cnt = snapshots.len();

    let mut ids = Vec::with_capacity(snap_cnt);
    let mut tabs = Vec::with_capacity(snap_cnt);
    let mut parents = Vec::with_capacity(snap_cnt);
    let mut seqs = Vec::with_capacity(snap_cnt);
    let mut manifs = Vec::with_capacity(snap_cnt);
    let mut summaries = Vec::with_capacity(snap_cnt);
    let mut schemas = Vec::with_capacity(snap_cnt);
    let mut timestamps = Vec::with_capacity(snap_cnt);

    for snap in snapshots {
        ids.push(snap.snapshot_id());
        tabs.push(tabular_id);
        parents.push(snap.parent_snapshot_id());
        seqs.push(snap.sequence_number());
        manifs.push(snap.manifest_list().to_string());
        summaries.push(serde_json::to_value(snap.summary()).map_err(|er| {
            ErrorModel::internal(
                "Error serializing snapshot summary",
                "SnapshotSummarySerializationError",
                Some(Box::new(er)),
            )
        })?);
        schemas.push(snap.schema_id());
        timestamps.push(snap.timestamp_ms());
    }
    let _ = sqlx::query!(
        r#"INSERT INTO table_snapshot(snapshot_id,
                                          table_id,
                                          parent_snapshot_id,
                                          sequence_number,
                                          manifest_list,
                                          summary,
                                          schema_id,
                                          timestamp_ms)
            SELECT * FROM UNNEST(
                $1::BIGINT[],
                $2::UUID[],
                $3::BIGINT[],
                $4::BIGINT[],
                $5::TEXT[],
                $6::JSONB[],
                $7::INT[],
                $8::BIGINT[]
            )"#,
        &ids,
        &tabs,
        &parents as _,
        &seqs,
        &manifs,
        &summaries,
        &schemas as _,
        &timestamps
    )
    .execute(&mut **transaction)
    .await
    .map_err(|err| {
        tracing::warn!("Error creating table: {}", err);
        err.into_error_model("Error inserting table snapshot".to_string())
    })?;

    Ok(())
}

pub(crate) async fn set_table_properties(
    properties: &HashMap<String, String>,
    table_id: Uuid,
    transaction: &mut PgConnection,
) -> api::Result<()> {
    let (keys, vals): (Vec<String>, Vec<String>) = properties
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .unzip();
    sqlx::query!(
        r#"WITH drop as (DELETE FROM table_properties WHERE table_id = $1) INSERT INTO table_properties (table_id, key, value)
           VALUES ($1, UNNEST($2::text[]), UNNEST($3::text[]));"#,
        table_id,
        &keys,
        &vals
    )
    .execute(transaction)
    .await
    .map_err(|e| {
        let message = "Error inserting table property".to_string();
        tracing::warn!("{}", message);
        e.into_error_model(message)
    })?;
    Ok(())
}
