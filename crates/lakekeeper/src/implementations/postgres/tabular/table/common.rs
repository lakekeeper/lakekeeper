use std::{collections::HashMap, ops::Range};

use iceberg::spec::{
    EncryptedKey, MetadataLog, PartitionSpecRef, PartitionStatisticsFile, SchemaRef, SnapshotLog,
    SnapshotRef, SortOrderRef, StatisticsFile, TableMetadata,
};
use sqlx::{PgConnection, Postgres, Transaction};

use crate::{
    WarehouseId,
    implementations::postgres::{
        dbutils::DBErrorHandler,
        tabular::table::{assigned_rows_as_i64, first_row_id_as_i64},
    },
    service::{
        CatalogBackendError, ConversionError, InternalBackendErrors, SerializationError, TableId,
    },
};

pub(super) async fn remove_schemas(
    warehouse_id: WarehouseId,
    table_id: TableId,
    schema_ids: Vec<i32>,
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<(), CatalogBackendError> {
    if schema_ids.is_empty() {
        return Ok(());
    }

    let _ = sqlx::query!(
        r#"DELETE FROM table_schema
           WHERE warehouse_id = $1 AND table_id = $2 AND schema_id = ANY($3::INT[])"#,
        *warehouse_id,
        *table_id,
        &schema_ids,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| {
        e.into_catalog_backend_error()
            .append_detail("Failed to remove schemas")
    })?;

    Ok(())
}

pub(super) async fn insert_schemas(
    schema_iter: impl ExactSizeIterator<Item = &SchemaRef>,
    transaction: &mut Transaction<'_, Postgres>,
    warehouse_id: WarehouseId,
    table_id: TableId,
) -> Result<(), InternalBackendErrors> {
    if schema_iter.len() == 0 {
        return Ok(());
    }

    let num_schemas = schema_iter.len();
    let mut ids = Vec::with_capacity(num_schemas);
    let mut schemas = Vec::with_capacity(num_schemas);
    let table_ids = vec![*table_id; num_schemas];

    for s in schema_iter {
        ids.push(s.schema_id());
        schemas.push(serde_json::to_value(s).map_err(|e| SerializationError::new("schema", e))?);
    }

    let _ = sqlx::query!(
        r#"INSERT INTO table_schema(schema_id, table_id, schema, warehouse_id)
           SELECT *, $3 FROM UNNEST($1::INT[], $2::UUID[], $4::JSONB[])"#,
        &ids,
        &table_ids,
        *warehouse_id,
        &schemas
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| {
        e.into_catalog_backend_error()
            .append_detail("Failed to insert schema")
    })?;

    Ok(())
}

pub(super) async fn set_current_schema(
    new_schema_id: i32,
    transaction: &mut Transaction<'_, Postgres>,
    warehouse_id: WarehouseId,
    table_id: TableId,
) -> Result<(), CatalogBackendError> {
    let _ = sqlx::query!(
        r#"INSERT INTO table_current_schema (warehouse_id, table_id, schema_id) VALUES ($1, $2, $3)
           ON CONFLICT (warehouse_id, table_id) DO UPDATE SET schema_id = EXCLUDED.schema_id
        "#,
        *warehouse_id,
        *table_id,
        new_schema_id
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| {
        e.into_catalog_backend_error()
            .append_detail("Failed to set current schema")
    })?;
    Ok(())
}

pub(super) async fn remove_partition_specs(
    warehouse_id: WarehouseId,
    table_id: TableId,
    spec_ids: Vec<i32>,
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<(), CatalogBackendError> {
    if spec_ids.is_empty() {
        return Ok(());
    }

    let _ = sqlx::query!(
        r#"DELETE FROM table_partition_spec
           WHERE warehouse_id = $1 AND table_id = $2 AND partition_spec_id = ANY($3::INT[])"#,
        *warehouse_id,
        *table_id,
        &spec_ids,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| {
        e.into_catalog_backend_error()
            .append_detail("Failed to remove partition specs")
    })?;

    Ok(())
}

pub(crate) async fn insert_partition_specs(
    partition_specs: impl ExactSizeIterator<Item = &PartitionSpecRef>,
    transaction: &mut Transaction<'_, Postgres>,
    warehouse_id: WarehouseId,
    table_id: TableId,
) -> Result<(), InternalBackendErrors> {
    if partition_specs.len() == 0 {
        return Ok(());
    }

    let mut spec_ids = Vec::with_capacity(partition_specs.len());
    let mut specs = Vec::with_capacity(partition_specs.len());

    for part_spec in partition_specs {
        spec_ids.push(part_spec.spec_id());
        specs.push(
            serde_json::to_value(part_spec)
                .map_err(|e| SerializationError::new("partition spec", e))?,
        );
    }

    let _ = sqlx::query!(
        r#"INSERT INTO table_partition_spec(partition_spec_id, table_id, warehouse_id, partition_spec)
               SELECT sid, $2, $3, s FROM UNNEST($1::INT[], $4::JSONB[]) u(sid, s)"#,
        &spec_ids,
        *table_id,
        *warehouse_id,
        &specs
    )
    .execute(&mut **transaction)
    .await
    .map_err(|err| {
        err.into_catalog_backend_error()
            .append_detail("Failed to insert partition specs")
    })?;

    Ok(())
}

pub(crate) async fn set_default_partition_spec(
    transaction: &mut Transaction<'_, Postgres>,
    warehouse_id: WarehouseId,
    table_id: TableId,
    default_spec_id: i32,
) -> Result<(), CatalogBackendError> {
    let _ = sqlx::query!(
        r#"INSERT INTO table_default_partition_spec(partition_spec_id, table_id, warehouse_id)
           VALUES ($1, $2, $3)
           ON CONFLICT (warehouse_id, table_id)
           DO UPDATE SET partition_spec_id = EXCLUDED.partition_spec_id"#,
        default_spec_id,
        *table_id,
        *warehouse_id,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|err| {
        err.into_catalog_backend_error()
            .append_detail("Failed to set default partition spec")
    })?;
    Ok(())
}

pub(crate) async fn remove_sort_orders(
    warehouse_id: WarehouseId,
    table_id: TableId,
    order_ids: Vec<i64>,
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<(), CatalogBackendError> {
    if order_ids.is_empty() {
        return Ok(());
    }
    let _ = sqlx::query!(
        r#"DELETE FROM table_sort_order
           WHERE warehouse_id = $1 AND table_id = $2 AND sort_order_id = ANY($3::BIGINT[])"#,
        *warehouse_id,
        *table_id,
        &order_ids,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|err| {
        err.into_catalog_backend_error()
            .append_detail("Failed to remove sort orders")
    })?;

    Ok(())
}

pub(crate) async fn insert_sort_orders(
    sort_orders_iter: impl ExactSizeIterator<Item = &SortOrderRef>,
    transaction: &mut Transaction<'_, Postgres>,
    warehouse_id: WarehouseId,
    table_id: TableId,
) -> Result<(), InternalBackendErrors> {
    let n_orders = sort_orders_iter.len();
    if n_orders == 0 {
        return Ok(());
    }
    let mut sort_order_ids = Vec::with_capacity(n_orders);
    let mut sort_orders = Vec::with_capacity(n_orders);

    for sort_order in sort_orders_iter {
        sort_order_ids.push(sort_order.order_id);
        sort_orders.push(
            serde_json::to_value(sort_order)
                .map_err(|e| SerializationError::new("sort order", e))?,
        );
    }

    let _ = sqlx::query!(
        r#"INSERT INTO table_sort_order(sort_order_id, table_id, warehouse_id, sort_order)
           SELECT sid, $2, $3, s FROM UNNEST($1::BIGINT[], $4::JSONB[]) u(sid, s)"#,
        &sort_order_ids,
        *table_id,
        *warehouse_id,
        &sort_orders
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| {
        e.into_catalog_backend_error()
            .append_detail("Failed to insert sort orders")
    })?;

    Ok(())
}

pub(crate) async fn set_default_sort_order(
    default_sort_order_id: i64,
    transaction: &mut Transaction<'_, Postgres>,
    warehouse_id: WarehouseId,
    table_id: TableId,
) -> Result<(), CatalogBackendError> {
    let _ = sqlx::query!(
        r#"INSERT INTO table_default_sort_order(warehouse_id, table_id, sort_order_id)
           VALUES ($1, $2, $3)
           ON CONFLICT (warehouse_id, table_id)
           DO UPDATE SET sort_order_id = EXCLUDED.sort_order_id"#,
        *warehouse_id,
        *table_id,
        default_sort_order_id,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| {
        e.into_catalog_backend_error()
            .append_detail("Failed to set default sort order")
    })?;
    Ok(())
}

pub(crate) async fn remove_snapshot_log_entries(
    n_entries: usize,
    transaction: &mut Transaction<'_, Postgres>,
    warehouse_id: WarehouseId,
    table_id: TableId,
) -> Result<(), InternalBackendErrors> {
    let i: i64 = n_entries
        .try_into()
        .map_err(|e| ConversionError::new("Too many snapshot log entries to expire.", e))?;
    let exec = sqlx::query!(
        r#"DELETE FROM table_snapshot_log WHERE warehouse_id = $1 AND table_id = $2
           AND sequence_number
           IN (SELECT sequence_number FROM table_snapshot_log
                   WHERE warehouse_id = $1 AND table_id =  $2
                   ORDER BY sequence_number ASC LIMIT $3)"#,
        *warehouse_id,
        *table_id,
        i
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| {
        e.into_catalog_backend_error()
            .append_detail("Failed to expire snapshot log entries")
    })?;

    tracing::debug!(
        "Expired {} snapshot log entries for table_id {} in warehouse_id {}",
        exec.rows_affected(),
        table_id,
        warehouse_id,
    );
    Ok(())
}

pub(crate) async fn insert_snapshot_log(
    snapshots: impl ExactSizeIterator<Item = &SnapshotLog>,
    transaction: &mut Transaction<'_, Postgres>,
    warehouse_id: WarehouseId,
    table_id: TableId,
) -> Result<(), InternalBackendErrors> {
    if snapshots.len() == 0 {
        return Ok(());
    }

    let (snap, stamp): (Vec<_>, Vec<_>) = snapshots
        .map(|log| (log.snapshot_id, log.timestamp_ms))
        .unzip();
    let seq = 0i64..snap
        .len()
        .try_into()
        .map_err(|e| ConversionError::new("Too many snapshot log entries.", e))?;
    let _ = sqlx::query!(
        r#"INSERT INTO table_snapshot_log(warehouse_id, table_id, snapshot_id, timestamp)
           SELECT $2, $3, sid, ts FROM UNNEST($1::BIGINT[], $4::BIGINT[], $5::BIGINT[]) u(sid, ts, seq) ORDER BY seq ASC"#,
        &snap,
        *warehouse_id,
        *table_id,
        &stamp,
        &seq.collect::<Vec<_>>()
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| {
        e.into_catalog_backend_error()
            .append_detail("Failed to insert snapshot log entries")
    })?;
    Ok(())
}

pub(super) async fn expire_metadata_log_entries(
    warehouse_id: WarehouseId,
    table_id: TableId,
    n_entries: usize,
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<(), InternalBackendErrors> {
    let i: i64 = n_entries
        .try_into()
        .map_err(|e| ConversionError::new("Too many metadata log entries to expire.", e))?;
    let exec = sqlx::query!(
        r#"DELETE FROM table_metadata_log WHERE warehouse_id = $1 AND table_id = $2
           AND sequence_number
           IN (SELECT sequence_number FROM table_metadata_log
                   WHERE warehouse_id = $1 AND table_id = $2
                   ORDER BY sequence_number ASC LIMIT $3)"#,
        *warehouse_id,
        *table_id,
        i
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| {
        e.into_catalog_backend_error()
            .append_detail("Failed to expire metadata log entries")
    })?;

    tracing::debug!(
        "Expired {} metadata log entries for table_id {} in warehouse_id {}",
        exec.rows_affected(),
        table_id,
        warehouse_id,
    );
    Ok(())
}

pub(super) async fn insert_metadata_log(
    warehouse_id: WarehouseId,
    table_id: TableId,
    log: impl ExactSizeIterator<Item = MetadataLog>,
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<(), InternalBackendErrors> {
    if log.len() == 0 {
        return Ok(());
    }
    let mut timestamps = Vec::with_capacity(log.len());
    let mut metadata_files = Vec::with_capacity(log.len());
    let seqs: Range<i64> = 0..log
        .len()
        .try_into()
        .map_err(|e| ConversionError::new("Too many metadata log entries.", e))?;
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
        *table_id,
        &timestamps,
        &metadata_files,
        &seqs.collect::<Vec<_>>(),
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| {
        e.into_catalog_backend_error()
            .append_detail("Failed to insert metadata log entries")
    })?;
    Ok(())
}

pub(super) async fn insert_snapshot_refs(
    warehouse_id: WarehouseId,
    table_metadata: &TableMetadata,
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<(), InternalBackendErrors> {
    let n_refs = table_metadata.refs().len();
    if n_refs == 0 {
        return Ok(());
    }

    let mut refnames = Vec::with_capacity(n_refs);
    let mut snapshot_ids = Vec::with_capacity(n_refs);
    let mut retentions = Vec::with_capacity(n_refs);

    for (refname, snapshot_ref) in table_metadata.refs() {
        refnames.push(refname.clone());
        snapshot_ids.push(snapshot_ref.snapshot_id);
        retentions.push(
            serde_json::to_value(&snapshot_ref.retention)
                .map_err(|e| SerializationError::new("snapshot ref retention", e))?,
        );
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
    .map_err(|e| {
        e.into_catalog_backend_error()
            .append_detail("Failed to insert snapshot refs")
    })?;

    Ok(())
}

pub(super) async fn remove_snapshots(
    warehouse_id: WarehouseId,
    table_id: TableId,
    snapshot_ids: Vec<i64>,
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<(), CatalogBackendError> {
    let _ = sqlx::query!(
        r#"DELETE FROM table_snapshot
           WHERE warehouse_id = $1 AND table_id = $2 AND snapshot_id = ANY($3::BIGINT[])"#,
        *warehouse_id,
        *table_id,
        &snapshot_ids,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| {
        e.into_catalog_backend_error()
            .append_detail("Failed to remove snapshots")
    })?;

    Ok(())
}

pub(super) async fn insert_snapshots(
    warehouse_id: WarehouseId,
    table_id: TableId,
    snapshots: impl ExactSizeIterator<Item = &SnapshotRef>,
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<(), InternalBackendErrors> {
    if snapshots.len() == 0 {
        return Ok(());
    }

    let snap_cnt = snapshots.len();

    // Column values changing for every row.
    let mut ids = Vec::with_capacity(snap_cnt);
    let mut parents = Vec::with_capacity(snap_cnt);
    let mut seqs = Vec::with_capacity(snap_cnt);
    let mut manifs = Vec::with_capacity(snap_cnt);
    let mut summaries = Vec::with_capacity(snap_cnt);
    let mut schemas = Vec::with_capacity(snap_cnt);
    let mut timestamps = Vec::with_capacity(snap_cnt);
    let mut first_row_ids = Vec::with_capacity(snap_cnt);
    let mut assigned_rows = Vec::with_capacity(snap_cnt);
    let mut key_ids = Vec::with_capacity(snap_cnt);

    for snap in snapshots {
        ids.push(snap.snapshot_id());
        parents.push(snap.parent_snapshot_id());
        seqs.push(snap.sequence_number());
        manifs.push(snap.manifest_list().to_string());
        summaries.push(
            serde_json::to_value(snap.summary())
                .map_err(|e| SerializationError::new("snapshot summary", e))?,
        );
        schemas.push(snap.schema_id());
        timestamps.push(snap.timestamp_ms());
        first_row_ids.push(snap.first_row_id().map(first_row_id_as_i64).transpose()?);
        assigned_rows.push(
            snap.added_rows_count()
                .map(assigned_rows_as_i64)
                .transpose()?,
        );
        key_ids.push(snap.encryption_key_id());
    }
    let _ = sqlx::query!(
        r#"INSERT INTO table_snapshot(warehouse_id,
                                      table_id,
                                      snapshot_id,
                                      parent_snapshot_id,
                                      sequence_number,
                                      manifest_list,
                                      summary,
                                      schema_id,
                                      timestamp_ms,
                                      first_row_id,
                                      assigned_rows,
                                      key_id)
            SELECT $3, $2, * FROM UNNEST(
                $1::BIGINT[],
                $4::BIGINT[],
                $5::BIGINT[],
                $6::TEXT[],
                $7::JSONB[],
                $8::INT[],
                $9::BIGINT[],
                $10::BIGINT[],
                $11::BIGINT[],
                $12::TEXT[]
            )"#,
        &ids,
        *table_id,
        *warehouse_id,
        &parents as _,
        &seqs,
        &manifs,
        &summaries,
        &schemas as _,
        &timestamps,
        &first_row_ids as _,
        &assigned_rows as _,
        &key_ids as _,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| {
        e.into_catalog_backend_error()
            .append_detail("Failed to insert snapshots")
    })?;

    Ok(())
}

pub(crate) async fn set_table_properties(
    warehouse_id: WarehouseId,
    table_id: TableId,
    properties: &HashMap<String, String>,
    transaction: &mut PgConnection,
) -> Result<(), CatalogBackendError> {
    if properties.is_empty() {
        return Ok(());
    }
    let (keys, vals): (Vec<String>, Vec<String>) = properties
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .unzip();
    sqlx::query!(
        r#"WITH drop as (DELETE FROM table_properties WHERE warehouse_id = $1 AND table_id = $2)
           INSERT INTO table_properties (warehouse_id, table_id, key, value)
           SELECT $1, $2, u.* FROM UNNEST($3::text[], $4::text[]) u
           ON CONFLICT (warehouse_id, table_id, key) DO UPDATE SET value = EXCLUDED.value;"#,
        *warehouse_id,
        *table_id,
        &keys,
        &vals
    )
    .execute(transaction)
    .await
    .map_err(|e| {
        e.into_catalog_backend_error()
            .append_detail("Failed to set table properties")
    })?;
    Ok(())
}

pub(super) async fn insert_partition_statistics(
    warehouse_id: WarehouseId,
    table_id: TableId,
    partition_statistics: impl ExactSizeIterator<Item = &PartitionStatisticsFile>,
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<(), InternalBackendErrors> {
    let n_stats = partition_statistics.len();
    if n_stats == 0 {
        return Ok(());
    }
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
        *table_id,
        *warehouse_id,
        &paths,
        &file_size_in_bytes
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| {
        e.into_catalog_backend_error()
            .append_detail("Failed to insert partition statistics")
    })?;

    Ok(())
}

pub(super) async fn remove_partition_statistics(
    warehouse_id: WarehouseId,
    table_id: TableId,
    snapshot_ids: Vec<i64>,
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<(), CatalogBackendError> {
    let _ = sqlx::query!(
        r#"DELETE FROM partition_statistics
           WHERE warehouse_id = $1 AND table_id = $2 AND snapshot_id = ANY($3::BIGINT[])"#,
        *warehouse_id,
        *table_id,
        &snapshot_ids,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| {
        e.into_catalog_backend_error()
            .append_detail("Failed to remove partition statistics")
    })?;

    Ok(())
}

pub(super) async fn insert_table_statistics(
    warehouse_id: WarehouseId,
    table_id: TableId,
    statistics: impl ExactSizeIterator<Item = &StatisticsFile>,
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<(), InternalBackendErrors> {
    let n_stats = statistics.len();
    if n_stats == 0 {
        return Ok(());
    }
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
        blob_metadata.push(
            serde_json::to_value(&stat.blob_metadata)
                .map_err(|e| SerializationError::new("table statistics blob metadata", e))?,
        );
    }

    let _ = sqlx::query!(
        r#"INSERT INTO table_statistics(table_id, warehouse_id, snapshot_id, statistics_path, file_size_in_bytes, file_footer_size_in_bytes, key_metadata, blob_metadata)
           SELECT $2, $3, u.* FROM UNNEST($1::BIGINT[], $4::TEXT[], $5::BIGINT[], $6::BIGINT[], $7::TEXT[], $8::JSONB[]) u"#,
        &snapshot_ids,
        *table_id,
        *warehouse_id,
        &paths,
        &file_size_in_bytes,
        &file_footer_size_in_bytes,
        &key_metadata as _,
        &blob_metadata
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| {
        e.into_catalog_backend_error()
            .append_detail("Failed to insert table statistics")
    })?;

    Ok(())
}

pub(super) async fn remove_table_statistics(
    warehouse_id: WarehouseId,
    table_id: TableId,
    statistics_ids: Vec<i64>,
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<(), CatalogBackendError> {
    let _ = sqlx::query!(
        r#"DELETE FROM table_statistics
           WHERE warehouse_id = $1 AND table_id = $2 AND snapshot_id = ANY($3::BIGINT[])"#,
        *warehouse_id,
        *table_id,
        &statistics_ids,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| {
        e.into_catalog_backend_error()
            .append_detail("Failed to remove table statistics")
    })?;

    Ok(())
}

pub(crate) async fn insert_table_encryption_keys(
    warehouse_id: WarehouseId,
    table_id: TableId,
    encrypted_keys_iter: impl ExactSizeIterator<Item = &EncryptedKey>,
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<(), InternalBackendErrors> {
    let n_keys = encrypted_keys_iter.len();
    if n_keys == 0 {
        return Ok(());
    }
    let mut key_ids = Vec::with_capacity(n_keys);
    let mut key_metadatas = Vec::with_capacity(n_keys);
    let mut encrypted_by_ids = Vec::with_capacity(n_keys);
    let mut properties = Vec::with_capacity(n_keys);

    for key in encrypted_keys_iter {
        key_ids.push(key.key_id().to_string());
        key_metadatas.push(key.encrypted_key_metadata().to_vec());
        encrypted_by_ids.push(key.encrypted_by_id());
        properties.push(
            serde_json::to_value(key.properties())
                .map_err(|e| SerializationError::new("table encryption key properties", e))?,
        );
    }

    let _ = sqlx::query!(
        r#"INSERT INTO table_encryption_keys(warehouse_id, table_id, key_id, encrypted_key_metadata, encrypted_by_id, properties)
           SELECT $1, $2, u.* FROM UNNEST($3::TEXT[], $4::BYTEA[], $5::TEXT[], $6::JSONB[]) u"#,
        *warehouse_id,
        *table_id,
        &key_ids,
        &key_metadatas,
        &encrypted_by_ids as _,
        &properties
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| {
        e.into_catalog_backend_error()
            .append_detail("Failed to insert table encryption keys")
    })?;

    Ok(())
}

pub(crate) async fn remove_table_encryption_keys(
    warehouse_id: WarehouseId,
    table_id: TableId,
    encryption_key_ids: &[String],
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<(), CatalogBackendError> {
    let _ = sqlx::query!(
        r#"DELETE FROM table_encryption_keys
           WHERE warehouse_id = $1 AND table_id = $2 AND key_id = ANY($3::TEXT[])"#,
        *warehouse_id,
        *table_id,
        encryption_key_ids,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| {
        e.into_catalog_backend_error()
            .append_detail("Failed to remove table encryption keys")
    })?;

    Ok(())
}
