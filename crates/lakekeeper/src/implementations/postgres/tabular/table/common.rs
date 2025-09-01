use std::{collections::HashMap, ops::Range};

use iceberg::spec::{
    MetadataLog, PartitionSpecRef, PartitionStatisticsFile, SchemaRef, SnapshotLog, SnapshotRef,
    SortOrderRef, StatisticsFile, TableMetadata,
};
use iceberg_ext::catalog::rest::ErrorModel;
use sqlx::{PgConnection, Postgres, Transaction};
use uuid::Uuid;

use crate::{api, implementations::postgres::dbutils::DBErrorHandler, WarehouseId};

pub(super) async fn remove_schemas(
    warehouse_id: WarehouseId,
    table_id: Uuid,
    schema_ids: Vec<i32>,
    transaction: &mut Transaction<'_, Postgres>,
) -> api::Result<()> {
    let _ = sqlx::query!(
        r#"DELETE FROM table_schema
           WHERE warehouse_id = $1 AND table_id = $2 AND schema_id = ANY($3::INT[])"#,
        *warehouse_id,
        table_id,
        &schema_ids,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|err| {
        tracing::warn!("Error deleting table schema: {}", err);
        err.into_error_model("Error deleting table schemas".to_string())
    })?;

    Ok(())
}

pub(super) async fn insert_schemas(
    schema_iter: impl ExactSizeIterator<Item = &SchemaRef>,
    transaction: &mut Transaction<'_, Postgres>,
    warehouse_id: WarehouseId,
    tabular_id: Uuid,
) -> api::Result<()> {
    let num_schemas = schema_iter.len();
    let mut ids = Vec::with_capacity(num_schemas);
    let mut schemas = Vec::with_capacity(num_schemas);
    let warehouse_ids = vec![*warehouse_id; num_schemas];
    let table_ids = vec![tabular_id; num_schemas];

    for s in schema_iter {
        ids.push(s.schema_id());
        schemas.push(serde_json::to_value(s).map_err(|er| {
            ErrorModel::internal(
                "Error serializing schema",
                "SchemaSerializationError",
                Some(Box::new(er)),
            )
        })?);
    }

    let _ = sqlx::query!(
        r#"INSERT INTO table_schema(schema_id, table_id, warehouse_id, schema)
           SELECT * FROM UNNEST($1::INT[], $2::UUID[], $3::UUID[], $4::JSONB[])"#,
        &ids,
        &table_ids,
        &warehouse_ids,
        &schemas
    )
    .execute(&mut **transaction)
    .await
    .map_err(|err| {
        tracing::warn!("Error inserting table schema: {}", err);
        err.into_error_model("Error inserting table schema".to_string())
    })?;

    Ok(())
}

pub(super) async fn set_current_schema(
    new_schema_id: i32,
    transaction: &mut Transaction<'_, Postgres>,
    warehouse_id: WarehouseId,
    tabular_id: Uuid,
) -> api::Result<()> {
    let _ = sqlx::query!(
        r#"INSERT INTO table_current_schema (warehouse_id, table_id, schema_id) VALUES ($1, $2, $3)
           ON CONFLICT (warehouse_id, table_id) DO UPDATE SET schema_id = EXCLUDED.schema_id
        "#,
        *warehouse_id,
        tabular_id,
        new_schema_id
    )
    .execute(&mut **transaction)
    .await
    .map_err(|err| {
        tracing::warn!("Error inserting table current schema: {}", err);
        err.into_error_model("Error inserting table current schema".to_string())
    })?;
    Ok(())
}

pub(super) async fn remove_partition_specs(
    warehouse_id: WarehouseId,
    table_id: Uuid,
    spec_ids: Vec<i32>,
    transaction: &mut Transaction<'_, Postgres>,
) -> api::Result<()> {
    let _ = sqlx::query!(
        r#"DELETE FROM table_partition_spec
           WHERE warehouse_id = $1 AND table_id = $2 AND partition_spec_id = ANY($3::INT[])"#,
        *warehouse_id,
        table_id,
        &spec_ids,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|err| {
        tracing::warn!("Error deleting table partition specs: {}", err);
        err.into_error_model("Error deleting table partition specs".to_string())
    })?;

    Ok(())
}

pub(crate) async fn insert_partition_specs(
    partition_specs: impl ExactSizeIterator<Item = &PartitionSpecRef>,
    transaction: &mut Transaction<'_, Postgres>,
    warehouse_id: WarehouseId,
    tabular_id: Uuid,
) -> api::Result<()> {
    let mut spec_ids = Vec::with_capacity(partition_specs.len());
    let mut specs = Vec::with_capacity(partition_specs.len());

    for part_spec in partition_specs {
        spec_ids.push(part_spec.spec_id());
        specs.push(serde_json::to_value(part_spec).map_err(|er| {
            ErrorModel::internal(
                "Error serializing partition spec",
                "PartitionSpecSerializationError",
                Some(Box::new(er)),
            )
        })?);
    }

    let _ = sqlx::query!(
        r#"INSERT INTO table_partition_spec(partition_spec_id, table_id, warehouse_id, partition_spec)
               SELECT sid, $2, $3, s FROM UNNEST($1::INT[], $4::JSONB[]) u(sid, s)"#,
        &spec_ids,
        tabular_id,
        *warehouse_id,
        &specs
    )
    .execute(&mut **transaction)
    .await
    .map_err(|err| {
        tracing::warn!("Error inserting table partition spec: {}", err);
        err.into_error_model("Error inserting table partition spec".to_string())
    })?;

    Ok(())
}

pub(crate) async fn set_default_partition_spec(
    transaction: &mut Transaction<'_, Postgres>,
    warehouse_id: WarehouseId,
    tabular_id: Uuid,
    default_spec_id: i32,
) -> api::Result<()> {
    let _ = sqlx::query!(
        r#"INSERT INTO table_default_partition_spec(partition_spec_id, table_id, warehouse_id)
           VALUES ($1, $2, $3)
           ON CONFLICT (table_id, warehouse_id)
           DO UPDATE SET partition_spec_id = EXCLUDED.partition_spec_id"#,
        default_spec_id,
        tabular_id,
        *warehouse_id,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|err| {
        tracing::warn!("Error inserting table default partition spec: {}", err);
        err.into_error_model("Error inserting table default partition spec".to_string())
    })?;
    Ok(())
}

pub(crate) async fn remove_sort_orders(
    warehouse_id: WarehouseId,
    table_id: Uuid,
    order_ids: Vec<i64>,
    transaction: &mut Transaction<'_, Postgres>,
) -> api::Result<()> {
    let _ = sqlx::query!(
        r#"DELETE FROM table_sort_order
           WHERE warehouse_id = $1 AND table_id = $2 AND sort_order_id = ANY($3::BIGINT[])"#,
        *warehouse_id,
        table_id,
        &order_ids,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|err| {
        tracing::warn!("Error deleting table sort orders: {}", err);
        err.into_error_model("Error deleting table sort orders".to_string())
    })?;

    Ok(())
}

pub(crate) async fn insert_sort_orders(
    sort_orders_iter: impl ExactSizeIterator<Item = &SortOrderRef>,
    transaction: &mut Transaction<'_, Postgres>,
    warehouse_id: WarehouseId,
    tabular_id: Uuid,
) -> api::Result<()> {
    let n_orders = sort_orders_iter.len();
    let mut sort_order_ids = Vec::with_capacity(n_orders);
    let mut sort_orders = Vec::with_capacity(n_orders);

    for sort_order in sort_orders_iter {
        sort_order_ids.push(sort_order.order_id);
        sort_orders.push(serde_json::to_value(sort_order).map_err(|er| {
            ErrorModel::internal(
                "Error serializing sort order",
                "SortOrderSerializationError",
                Some(Box::new(er)),
            )
        })?);
    }

    let _ = sqlx::query!(
        r#"INSERT INTO table_sort_order(sort_order_id, table_id, warehouse_id, sort_order)
           SELECT sid, $2, $3, s FROM UNNEST($1::BIGINT[], $4::JSONB[]) u(sid, s)"#,
        &sort_order_ids,
        tabular_id,
        *warehouse_id,
        &sort_orders
    )
    .execute(&mut **transaction)
    .await
    .map_err(|err| {
        tracing::warn!("Error inserting table sort order: {}", err);
        err.into_error_model("Error inserting table sort order".to_string())
    })?;

    Ok(())
}

pub(crate) async fn set_default_sort_order(
    default_sort_order_id: i64,
    transaction: &mut Transaction<'_, Postgres>,
    warehouse_id: WarehouseId,
    tabular_id: Uuid,
) -> api::Result<()> {
    let _ = sqlx::query!(
        r#"INSERT INTO table_default_sort_order(warehouse_id, table_id, sort_order_id)
           VALUES ($1, $2, $3)
           ON CONFLICT (warehouse_id, table_id)
           DO UPDATE SET sort_order_id = EXCLUDED.sort_order_id"#,
        *warehouse_id,
        tabular_id,
        default_sort_order_id,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|err| {
        tracing::warn!("Error inserting table sort order: {}", err);
        err.into_error_model("Error inserting table sort order".to_string())
    })?;
    Ok(())
}

pub(crate) async fn remove_snapshot_log_entries(
    n_entries: usize,
    transaction: &mut Transaction<'_, Postgres>,
    warehouse_id: WarehouseId,
    tabular_id: Uuid,
) -> api::Result<()> {
    let i: i64 = n_entries.try_into().map_err(|e| {
        ErrorModel::internal(
            "Too many snapshot log entries to expire.",
            "TooManySnapshotLogEntries",
            Some(Box::new(e)),
        )
    })?;
    let exec = sqlx::query!(
        r#"DELETE FROM table_snapshot_log WHERE warehouse_id = $1 AND table_id = $2
           AND sequence_number
           IN (SELECT sequence_number FROM table_snapshot_log
                   WHERE warehouse_id = $1 AND table_id =  $2
                   ORDER BY sequence_number ASC LIMIT $3)"#,
        *warehouse_id,
        tabular_id,
        i
    )
    .execute(&mut **transaction)
    .await
    .map_err(|err| {
        tracing::warn!("Error expiring table snapshot log entries: {}", err);
        err.into_error_model("Error expiring table snapshot log entries".to_string())
    })?;

    tracing::debug!(
        "Expired {} snapshot log entries for table_id {} in warehouse_id {}",
        exec.rows_affected(),
        tabular_id,
        warehouse_id,
    );
    Ok(())
}

pub(crate) async fn insert_snapshot_log(
    snapshots: impl ExactSizeIterator<Item = &SnapshotLog>,
    transaction: &mut Transaction<'_, Postgres>,
    warehouse_id: WarehouseId,
    tabular_id: Uuid,
) -> api::Result<()> {
    let (snap, stamp): (Vec<_>, Vec<_>) = snapshots
        .map(|log| (log.snapshot_id, log.timestamp_ms))
        .unzip();
    let seq = 0i64..snap.len().try_into().map_err(|e| {
        ErrorModel::internal(
            "Too many snapshot log entries.",
            "TooManySnapshotLogEntries",
            Some(Box::new(e)),
        )
    })?;
    let _ = sqlx::query!(
        r#"INSERT INTO table_snapshot_log(warehouse_id, table_id, snapshot_id, timestamp)
           SELECT $2, $3, sid, ts FROM UNNEST($1::BIGINT[], $4::BIGINT[], $5::BIGINT[]) u(sid, ts, seq) ORDER BY seq ASC"#,
        &snap,
        *warehouse_id,
        &tabular_id,
        &stamp,
        &seq.collect::<Vec<_>>()
    )
    .execute(&mut **transaction)
    .await
    .map_err(|err| {
        tracing::warn!("Error inserting table snapshot log: {}", err);
        err.into_error_model("Error inserting table snapshot log".to_string())
    })?;
    Ok(())
}

pub(super) async fn expire_metadata_log_entries(
    warehouse_id: WarehouseId,
    tabular_id: Uuid,
    n_entries: usize,
    transaction: &mut Transaction<'_, Postgres>,
) -> api::Result<()> {
    let i: i64 = n_entries.try_into().map_err(|e| {
        ErrorModel::internal(
            "Too many metadata log entries to expire.",
            "TooManyMetadataLogEntries",
            Some(Box::new(e)),
        )
    })?;
    let exec = sqlx::query!(
        r#"DELETE FROM table_metadata_log WHERE warehouse_id = $1 AND table_id = $2
           AND sequence_number
           IN (SELECT sequence_number FROM table_metadata_log
                   WHERE warehouse_id = $1 AND table_id = $2
                   ORDER BY sequence_number ASC LIMIT $3)"#,
        *warehouse_id,
        tabular_id,
        i
    )
    .execute(&mut **transaction)
    .await
    .map_err(|err| {
        tracing::warn!("Error expiring table metadata log entries: {}", err);
        err.into_error_model("Error expiring table metadata log entries".to_string())
    })?;

    tracing::debug!(
        "Expired {} metadata log entries for table_id {} in warehouse_id {}",
        exec.rows_affected(),
        tabular_id,
        warehouse_id,
    );
    Ok(())
}

pub(super) async fn insert_metadata_log(
    warehouse_id: WarehouseId,
    tabular_id: Uuid,
    log: impl ExactSizeIterator<Item = MetadataLog>,
    transaction: &mut Transaction<'_, Postgres>,
) -> api::Result<()> {
    let mut timestamps = Vec::with_capacity(log.len());
    let mut metadata_files = Vec::with_capacity(log.len());
    let seqs: Range<i64> = 0..log.len().try_into().map_err(|e| {
        ErrorModel::internal(
            "Too many metadata log entries.",
            "TooManyMetadataLogEntries",
            Some(Box::new(e)),
        )
    })?;
    for MetadataLog {
        timestamp_ms,
        metadata_file,
    } in log
    {
        timestamps.push(timestamp_ms);
        metadata_files.push(metadata_file);
    }

    let _ = sqlx::query!(
        r#"INSERT INTO table_metadata_log(warehouse_id, table_id, timestamp, metadata_file)
           SELECT $1, $2, ts, mf FROM UNNEST($3::BIGINT[], $4::TEXT[], $5::BIGINT[]) u (ts, mf, seq) ORDER BY seq ASC"#,
        *warehouse_id,
        tabular_id,
        &timestamps,
        &metadata_files,
        &seqs.collect::<Vec<_>>(),
    )
    .execute(&mut **transaction)
    .await
    .map_err(|err| {
        tracing::warn!("Error inserting table metadata log: {}", err);
        err.into_error_model("Error inserting table metadata log".to_string())
    })?;
    Ok(())
}

pub(super) async fn insert_snapshot_refs(
    warehouse_id: WarehouseId,
    table_metadata: &TableMetadata,
    transaction: &mut Transaction<'_, Postgres>,
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
            WHERE warehouse_id = $1 AND table_id = $2
            AND table_ref_name NOT IN (SELECT unnest($3::TEXT[]))
        )
        INSERT INTO table_refs(warehouse_id,
                              table_id,
                              table_ref_name,
                              snapshot_id,
                              retention)
        SELECT $1, $2, u.* FROM UNNEST($3::TEXT[], $4::BIGINT[], $5::JSONB[]) u
        ON CONFLICT (warehouse_id, table_id, table_ref_name)
        DO UPDATE SET snapshot_id = EXCLUDED.snapshot_id, retention = EXCLUDED.retention"#,
        *warehouse_id,
        table_metadata.uuid(),
        &refnames,
        &snapshot_ids,
        &retentions,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|err| {
        tracing::warn!("Error inserting table refs: {}", err);
        err.into_error_model("Error inserting table refs".to_string())
    })?;

    Ok(())
}

pub(super) async fn remove_snapshots(
    warehouse_id: WarehouseId,
    table_id: Uuid,
    snapshot_ids: Vec<i64>,
    transaction: &mut Transaction<'_, Postgres>,
) -> api::Result<()> {
    let _ = sqlx::query!(
        r#"DELETE FROM table_snapshot
           WHERE warehouse_id = $1 AND table_id = $2 AND snapshot_id = ANY($3::BIGINT[])"#,
        *warehouse_id,
        table_id,
        &snapshot_ids,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|err| {
        tracing::warn!("Error deleting table snapshots: {}", err);
        err.into_error_model("Error deleting table snapshots".to_string())
    })?;

    Ok(())
}

pub(super) async fn insert_snapshots(
    warehouse_id: WarehouseId,
    tabular_id: Uuid,
    snapshots: impl ExactSizeIterator<Item = &SnapshotRef>,
    transaction: &mut Transaction<'_, Postgres>,
) -> api::Result<()> {
    let snap_cnt = snapshots.len();

    // Column values changing for every row.
    let mut ids = Vec::with_capacity(snap_cnt);
    let mut parents = Vec::with_capacity(snap_cnt);
    let mut seqs = Vec::with_capacity(snap_cnt);
    let mut manifs = Vec::with_capacity(snap_cnt);
    let mut summaries = Vec::with_capacity(snap_cnt);
    let mut schemas = Vec::with_capacity(snap_cnt);
    let mut timestamps = Vec::with_capacity(snap_cnt);

    // Column values the same for every row.
    let warehouses = vec![*warehouse_id; snap_cnt];
    let tabs = vec![tabular_id; snap_cnt];

    for snap in snapshots {
        ids.push(snap.snapshot_id());
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
                                          warehouse_id,
                                          parent_snapshot_id,
                                          sequence_number,
                                          manifest_list,
                                          summary,
                                          schema_id,
                                          timestamp_ms)
            SELECT * FROM UNNEST(
                $1::BIGINT[],
                $2::UUID[],
                $3::UUID[],
                $4::BIGINT[],
                $5::BIGINT[],
                $6::TEXT[],
                $7::JSONB[],
                $8::INT[],
                $9::BIGINT[]
            )"#,
        &ids,
        &tabs,
        &warehouses,
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
        tracing::warn!("Error inserting table snapshot: {}", err);
        err.into_error_model("Error inserting table snapshot".to_string())
    })?;

    Ok(())
}

pub(crate) async fn set_table_properties(
    warehouse_id: WarehouseId,
    table_id: Uuid,
    properties: &HashMap<String, String>,
    transaction: &mut PgConnection,
) -> api::Result<()> {
    let (keys, vals): (Vec<String>, Vec<String>) = properties
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .unzip();
    sqlx::query!(
        r#"WITH drop as (DELETE FROM table_properties WHERE warehouse_id = $1 AND table_id = $2)
           INSERT INTO table_properties (warehouse_id, table_id, key, value)
           SELECT $1, $2, u.* FROM UNNEST($3::text[], $4::text[]) u
           ON CONFLICT (key, table_id, warehouse_id) DO UPDATE SET value = EXCLUDED.value;"#,
        *warehouse_id,
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

pub(super) async fn insert_partition_statistics(
    warehouse_id: WarehouseId,
    tabular_id: Uuid,
    partition_statistics: impl ExactSizeIterator<Item = &PartitionStatisticsFile>,
    transaction: &mut Transaction<'_, Postgres>,
) -> api::Result<()> {
    let n_stats = partition_statistics.len();
    let mut snapshot_ids = Vec::with_capacity(n_stats);
    let mut paths = Vec::with_capacity(n_stats);
    let mut file_size_in_bytes = Vec::with_capacity(n_stats);

    for stat in partition_statistics {
        snapshot_ids.push(stat.snapshot_id);
        paths.push(stat.statistics_path.clone());
        file_size_in_bytes.push(stat.file_size_in_bytes);
    }

    let _ = sqlx::query!(
        r#"INSERT INTO partition_statistics(table_id, warehouse_id, snapshot_id, statistics_path, file_size_in_bytes)
           SELECT $2, $3, u.* FROM UNNEST($1::BIGINT[], $4::TEXT[], $5::BIGINT[]) u"#,
        &snapshot_ids,
        tabular_id,
        *warehouse_id,
        &paths,
        &file_size_in_bytes
    )
    .execute(&mut **transaction)
    .await
    .map_err(|err| {
        tracing::warn!("Error inserting partition statistics: {}", err);
        err.into_error_model("Error inserting partition statistics".to_string())
    })?;

    Ok(())
}

pub(super) async fn remove_partition_statistics(
    warehouse_id: WarehouseId,
    table_id: Uuid,
    snapshot_ids: Vec<i64>,
    transaction: &mut Transaction<'_, Postgres>,
) -> api::Result<()> {
    let _ = sqlx::query!(
        r#"DELETE FROM partition_statistics
           WHERE warehouse_id = $1 AND table_id = $2 AND snapshot_id = ANY($3::BIGINT[])"#,
        *warehouse_id,
        table_id,
        &snapshot_ids,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|err| {
        tracing::warn!("Error deleting partition statistics for table {table_id}: {err}");
        err.into_error_model("Error deleting partition statistics".to_string())
    })?;

    Ok(())
}

pub(super) async fn insert_table_statistics(
    warehouse_id: WarehouseId,
    tabular_id: Uuid,
    statistics: impl ExactSizeIterator<Item = &StatisticsFile>,
    transaction: &mut Transaction<'_, Postgres>,
) -> api::Result<()> {
    let n_stats = statistics.len();
    let mut snapshot_ids = Vec::with_capacity(n_stats);
    let mut paths = Vec::with_capacity(n_stats);
    let mut file_size_in_bytes = Vec::with_capacity(n_stats);
    let mut file_footer_size_in_bytes = Vec::with_capacity(n_stats);
    let mut key_metadata = Vec::with_capacity(n_stats);
    let mut blob_metadata = Vec::with_capacity(n_stats);

    for stat in statistics {
        snapshot_ids.push(stat.snapshot_id);
        paths.push(stat.statistics_path.clone());
        file_size_in_bytes.push(stat.file_size_in_bytes);
        file_footer_size_in_bytes.push(stat.file_footer_size_in_bytes);
        key_metadata.push(stat.key_metadata.clone());
        blob_metadata.push(serde_json::to_value(&stat.blob_metadata).map_err(|er| {
            tracing::warn!(
                "Error creating table - failed to serialize BlobMetadata of StatisticsFile {er}",
            );
            ErrorModel::internal(
                "Error serializing blob metadata",
                "BlobMetadataSerializationError",
                Some(Box::new(er)),
            )
        })?);
    }

    let _ = sqlx::query!(
        r#"INSERT INTO table_statistics(table_id, warehouse_id, snapshot_id, statistics_path, file_size_in_bytes, file_footer_size_in_bytes, key_metadata, blob_metadata)
           SELECT $2, $3, u.* FROM UNNEST($1::BIGINT[], $4::TEXT[], $5::BIGINT[], $6::BIGINT[], $7::TEXT[], $8::JSONB[]) u"#,
        &snapshot_ids,
        tabular_id,
        *warehouse_id,
        &paths,
        &file_size_in_bytes,
        &file_footer_size_in_bytes,
        &key_metadata as _,
        &blob_metadata
    )
    .execute(&mut **transaction)
    .await
    .map_err(|err| {
        tracing::warn!("Error inserting table statistics: {}", err);
        err.into_error_model("Error inserting table statistics".to_string())
    })?;

    Ok(())
}

pub(super) async fn remove_table_statistics(
    warehouse_id: WarehouseId,
    table_id: Uuid,
    statistics_ids: Vec<i64>,
    transaction: &mut Transaction<'_, Postgres>,
) -> api::Result<()> {
    let _ = sqlx::query!(
        r#"DELETE FROM table_statistics
           WHERE warehouse_id = $1 AND table_id = $2 AND snapshot_id = ANY($3::BIGINT[])"#,
        *warehouse_id,
        table_id,
        &statistics_ids,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|err| {
        tracing::warn!("Error deleting table statistics: {}", err);
        err.into_error_model("Error deleting table statistics".to_string())
    })?;

    Ok(())
}
