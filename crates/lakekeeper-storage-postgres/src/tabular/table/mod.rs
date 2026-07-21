mod commit;
mod common;
mod create;
pub(crate) mod normalized_schema;

use std::{collections::HashMap, default::Default, ops::Deref, str::FromStr, sync::Arc};

pub(crate) use commit::commit_table_transaction;
pub(crate) use common::SchemaFieldBatch;
pub(crate) use create::create_table;
use iceberg::{
    TableUpdate,
    spec::{
        BlobMetadata, EncryptedKey, FormatVersion, MAIN_BRANCH, PartitionSpec, SnapshotRetention,
        SortOrder, Summary,
    },
};
use iceberg_ext::spec::TableMetadata;
use lakekeeper::{
    WarehouseId,
    api::iceberg::v1::tables::{LoadTableFilters, SnapshotsQuery},
    service::{
        ConversionError, InternalParseLocationError, InternalTableMetadataBuildFailed,
        LoadTableError, LoadTableResponse, RequiredTableComponentMissing, TableId,
        storage::join_location,
    },
};
use sqlx::types::Json;
use uuid::Uuid;

const MAX_PARAMETERS: usize = 30000;

#[inline]
pub(crate) fn next_row_id_as_i64(next_row_id: u64) -> Result<i64, ConversionError> {
    let next_row_id = i64::try_from(next_row_id).map_err(|e| {
        ConversionError::new(
            format!("Next row id is {next_row_id} but must be between 0 and i64::MAX"),
            e,
        )
    })?;
    Ok(next_row_id)
}

#[inline]
pub(crate) fn first_row_id_as_i64(first_row_id: u64) -> Result<i64, ConversionError> {
    let first_row_id = i64::try_from(first_row_id).map_err(|e| {
        ConversionError::new(
            format!("Snapshot first_row_id is {first_row_id} but must be between 0 and i64::MAX"),
            e,
        )
    })?;
    Ok(first_row_id)
}

#[inline]
pub(crate) fn assigned_rows_as_i64(assigned_rows: u64) -> Result<i64, ConversionError> {
    let assigned_rows = i64::try_from(assigned_rows).map_err(|e| {
        ConversionError::new(
            format!("Snapshot assigned_rows (added_rows) is {assigned_rows} but must be between 0 and i64::MAX"),
            e,
        )
    })?;
    Ok(assigned_rows)
}

#[inline]
pub(crate) fn first_row_id_as_u64(first_row_id: i64) -> Result<u64, ConversionError> {
    let first_row_id = u64::try_from(first_row_id).map_err(|e| {
        ConversionError::new(
            format!("Snapshot first_row_id is {first_row_id} but must be between 0 and u64::MAX"),
            e,
        )
    })?;
    Ok(first_row_id)
}

#[inline]
pub(crate) fn assigned_rows_as_u64(assigned_rows: i64) -> Result<u64, ConversionError> {
    let assigned_rows = u64::try_from(assigned_rows).map_err(|e| {
        ConversionError::new(
            format!("Snapshot assigned_rows (added_rows) is {assigned_rows} but must be between 0 and u64::MAX"),
            e,
        )
    })?;
    Ok(assigned_rows)
}

#[derive(Debug, sqlx::Type)]
#[sqlx(type_name = "table_format_version", rename_all = "kebab-case")]
pub enum DbTableFormatVersion {
    #[sqlx(rename = "1")]
    V1,
    #[sqlx(rename = "2")]
    V2,
    #[sqlx(rename = "3")]
    V3,
}

impl From<DbTableFormatVersion> for FormatVersion {
    fn from(v: DbTableFormatVersion) -> Self {
        match v {
            DbTableFormatVersion::V1 => FormatVersion::V1,
            DbTableFormatVersion::V2 => FormatVersion::V2,
            DbTableFormatVersion::V3 => FormatVersion::V3,
        }
    }
}

impl From<FormatVersion> for DbTableFormatVersion {
    fn from(v: FormatVersion) -> Self {
        match v {
            FormatVersion::V1 => DbTableFormatVersion::V1,
            FormatVersion::V2 => DbTableFormatVersion::V2,
            FormatVersion::V3 => DbTableFormatVersion::V3,
        }
    }
}

#[expect(dead_code)]
#[derive(sqlx::FromRow)]
struct TableQueryStruct {
    warehouse_id: Uuid,
    table_id: Uuid,
    table_name: String,
    namespace_name: Vec<String>,
    namespace_id: Uuid,
    table_ref_names: Option<Vec<String>>,
    table_ref_snapshot_ids: Option<Vec<i64>>,
    table_ref_retention: Option<Vec<Json<SnapshotRetention>>>,
    default_sort_order_id: Option<i64>,
    sort_order_ids: Option<Vec<i64>>,
    sort_orders: Option<Vec<Json<SortOrder>>>,
    metadata_log_timestamps: Option<Vec<i64>>,
    metadata_log_files: Option<Vec<String>>,
    snapshot_log_timestamps: Option<Vec<i64>>,
    snapshot_log_ids: Option<Vec<i64>>,
    snapshot_ids: Option<Vec<i64>>,
    snapshot_parent_snapshot_id: Option<Vec<Option<i64>>>,
    snapshot_sequence_number: Option<Vec<i64>>,
    snapshot_manifest_list: Option<Vec<String>>,
    snapshot_summary: Option<Vec<Json<Summary>>>,
    snapshot_schema_id: Option<Vec<Option<i32>>>,
    snapshot_timestamp_ms: Option<Vec<i64>>,
    snapshot_first_row_ids: Option<Vec<Option<i64>>>,
    snapshot_assigned_rows: Option<Vec<Option<i64>>>,
    snapshot_key_ids: Option<Vec<Option<String>>>,
    metadata_location: Option<String>,
    table_fs_location: String,
    table_fs_protocol: String,
    warehouse_version: i64,
    table_properties_keys: Option<Vec<String>>,
    table_properties_values: Option<Vec<String>>,
    default_partition_spec_id: Option<i32>,
    partition_spec_ids: Option<Vec<i32>>,
    partition_specs: Option<Vec<Json<PartitionSpec>>>,
    current_schema: Option<i32>,
    table_format_version: DbTableFormatVersion,
    next_row_id: i64,
    last_sequence_number: i64,
    last_column_id: i32,
    last_updated_ms: i64,
    last_partition_id: i32,
    partition_stats_snapshot_ids: Option<Vec<i64>>,
    partition_stats_statistics_paths: Option<Vec<String>>,
    partition_stats_file_size_in_bytes: Option<Vec<i64>>,
    table_stats_snapshot_ids: Option<Vec<i64>>,
    table_stats_statistics_paths: Option<Vec<String>>,
    table_stats_file_size_in_bytes: Option<Vec<i64>>,
    table_stats_file_footer_size_in_bytes: Option<Vec<i64>>,
    table_stats_key_metadata: Option<Vec<Option<String>>>,
    table_stats_blob_metadata: Option<Vec<Json<Vec<BlobMetadata>>>>,
    encryption_key_ids: Option<Vec<String>>,
    encryption_encrypted_key_metadatas: Option<Vec<Vec<u8>>>,
    encryption_encrypted_by_ids: Option<Vec<Option<String>>>,
    encryption_properties: Option<Vec<Option<serde_json::Value>>>,
}

impl TableQueryStruct {
    #[expect(clippy::too_many_lines)]
    fn into_table_metadata(
        self,
        schema_rows: Vec<normalized_schema::SchemaFieldRow>,
        expected_schema_ids: &[i32],
    ) -> Result<TableMetadata, LoadTableError> {
        fn expect<T>(
            field: Option<T>,
            field_name: &str,
            info: &(WarehouseId, TableId),
        ) -> Result<T, RequiredTableComponentMissing> {
            if let Some(v) = field {
                Ok(v)
            } else {
                Err(RequiredTableComponentMissing::new(info.0, info.1)
                    .append_detail(format!("Missing required component: {field_name}")))
            }
        }

        let warehouse_id = self.warehouse_id.into();
        let table_id = self.table_id.into();
        let info = (warehouse_id, table_id);

        // Schemas are assembled from the normalized `schema_field` rows (one row per field,
        // fetched separately and grouped per table), not from a JSONB blob.
        let schemas = normalized_schema::assemble_schemas(schema_rows, expected_schema_ids)
            .map_err(|e| {
                RequiredTableComponentMissing::new(warehouse_id, table_id).append_detail(format!(
                    "Failed to assemble schemas from schema_field rows: {e}"
                ))
            })?;

        let partition_specs = expect(self.partition_spec_ids, "Partition Spec IDs", &info)?
            .into_iter()
            .zip(
                expect(self.partition_specs, "Partition Specs", &info)?
                    .into_iter()
                    .map(|s| Arc::new(s.0)),
            )
            .collect::<HashMap<_, _>>();

        let default_partition_spec_id = expect(
            self.default_partition_spec_id,
            "Default Partition Spec ID",
            &info,
        )?;
        let default_spec = partition_specs
            .get(&default_partition_spec_id)
            .ok_or_else(|| {
                RequiredTableComponentMissing::new(warehouse_id, table_id).append_detail(format!(
                    "Default partition spec id {default_partition_spec_id} not found in loaded partition specs"
                ))
            })?
            .clone();

        let properties = self
            .table_properties_keys
            .unwrap_or_default()
            .into_iter()
            .zip(self.table_properties_values.unwrap_or_default())
            .collect::<HashMap<_, _>>();

        let snapshots = itertools::multizip((
            self.snapshot_ids.unwrap_or_default(),
            self.snapshot_schema_id.unwrap_or_default(),
            self.snapshot_summary.unwrap_or_default(),
            self.snapshot_manifest_list.unwrap_or_default(),
            self.snapshot_parent_snapshot_id.unwrap_or_default(),
            self.snapshot_sequence_number.unwrap_or_default(),
            self.snapshot_timestamp_ms.unwrap_or_default(),
            self.snapshot_first_row_ids.unwrap_or_default(),
            self.snapshot_assigned_rows.unwrap_or_default(),
            self.snapshot_key_ids.unwrap_or_default(),
        ))
        .map(
            |(
                snap_id,
                schema_id,
                summary,
                manifest,
                parent_snap,
                seq,
                timestamp_ms,
                first_row_id,
                assigned_rows,
                key_id,
            )| {
                Ok((
                    snap_id,
                    Arc::new({
                        let builder = iceberg::spec::Snapshot::builder()
                            .with_manifest_list(manifest)
                            .with_parent_snapshot_id(parent_snap)
                            .with_sequence_number(seq)
                            .with_snapshot_id(snap_id)
                            .with_summary(summary.0)
                            .with_timestamp_ms(timestamp_ms)
                            .with_encryption_key_id(key_id);
                        let row_range = if let (Some(first_row_id), Some(assigned_rows)) =
                            (first_row_id, assigned_rows)
                        {
                            let first_row_id = first_row_id_as_u64(first_row_id)?;
                            let assigned_rows = assigned_rows_as_u64(assigned_rows)?;
                            Some((first_row_id, assigned_rows))
                        } else {
                            None
                        };

                        match (schema_id, row_range) {
                            (Some(sid), Some(rr)) => builder
                                .with_schema_id(sid)
                                .with_row_range(rr.0, rr.1)
                                .build(),
                            (Some(sid), None) => builder.with_schema_id(sid).build(),
                            (None, Some(rr)) => builder.with_row_range(rr.0, rr.1).build(),
                            (None, None) => builder.build(),
                        }
                    }),
                ))
            },
        )
        .collect::<Result<HashMap<_, _>, LoadTableError>>()?;

        let snapshot_log = itertools::multizip((
            self.snapshot_log_ids.unwrap_or_default(),
            self.snapshot_log_timestamps.unwrap_or_default(),
        ))
        .map(|(snap_id, timestamp)| iceberg::spec::SnapshotLog {
            snapshot_id: snap_id,
            timestamp_ms: timestamp,
        })
        .collect::<Vec<_>>();

        let metadata_log = itertools::multizip((
            self.metadata_log_files.unwrap_or_default(),
            self.metadata_log_timestamps.unwrap_or_default(),
        ))
        .map(|(file, timestamp)| iceberg::spec::MetadataLog {
            metadata_file: file,
            timestamp_ms: timestamp,
        })
        .collect::<Vec<_>>();

        let sort_orders = itertools::multizip((
            expect(self.sort_order_ids, "Sort Order IDs", &info)?,
            expect(self.sort_orders, "Sort Orders", &info)?,
        ))
        .map(|(sort_order_id, sort_order)| (sort_order_id, Arc::new(sort_order.0)))
        .collect::<HashMap<_, _>>();

        let refs = itertools::multizip((
            self.table_ref_names.unwrap_or_default(),
            self.table_ref_snapshot_ids.unwrap_or_default(),
            self.table_ref_retention.unwrap_or_default(),
        ))
        .map(|(name, snap_id, retention)| {
            (
                name,
                iceberg::spec::SnapshotReference {
                    snapshot_id: snap_id,
                    retention: retention.0,
                },
            )
        })
        .collect::<HashMap<_, _>>();

        let current_snapshot_id = refs.get(MAIN_BRANCH).map(|s| s.snapshot_id);

        let partition_statistics = itertools::multizip((
            self.partition_stats_snapshot_ids.unwrap_or_default(),
            self.partition_stats_statistics_paths.unwrap_or_default(),
            self.partition_stats_file_size_in_bytes.unwrap_or_default(),
        ))
        .map(|(snapshot_id, statistics_path, file_size_in_bytes)| {
            (
                snapshot_id,
                iceberg::spec::PartitionStatisticsFile {
                    snapshot_id,
                    statistics_path,
                    file_size_in_bytes,
                },
            )
        })
        .collect::<HashMap<_, _>>();

        let statistics = itertools::multizip((
            self.table_stats_snapshot_ids.unwrap_or_default(),
            self.table_stats_statistics_paths.unwrap_or_default(),
            self.table_stats_file_size_in_bytes.unwrap_or_default(),
            self.table_stats_file_footer_size_in_bytes
                .unwrap_or_default(),
            self.table_stats_key_metadata.unwrap_or_default(),
            self.table_stats_blob_metadata.unwrap_or_default(),
        ))
        .map(
            |(
                snapshot_id,
                statistics_path,
                file_size_in_bytes,
                file_footer_size_in_bytes,
                key_metadata,
                blob_metadata,
            )| {
                (
                    snapshot_id,
                    iceberg::spec::StatisticsFile {
                        snapshot_id,
                        statistics_path,
                        file_size_in_bytes,
                        file_footer_size_in_bytes,
                        key_metadata,
                        blob_metadata: blob_metadata.deref().clone(),
                    },
                )
            },
        )
        .collect::<HashMap<_, _>>();

        let current_schema_id = expect(self.current_schema, "Current Schema ID", &info)?;

        // A zero-row current schema almost certainly means lost `schema_field` rows (a zero-column
        // table is unusable), so fail loud rather than silently loading an empty current schema.
        // Legitimately-empty *non-current* schemas are still reconstructed by `assemble_schemas`.
        if let Some(s) = schemas.get(&current_schema_id)
            && s.as_struct().fields().is_empty()
        {
            return Err(RequiredTableComponentMissing::new(warehouse_id, table_id)
                .append_detail(format!(
                    "Current schema {current_schema_id} has no fields (schema_field rows missing)."
                ))
                .into());
        }

        let default_partition_type = default_spec
            .partition_type(schemas.get(&current_schema_id).ok_or_else(|| {
                RequiredTableComponentMissing::new(warehouse_id, table_id).append_detail(format!(
                    "No schema exists with the current schema id {current_schema_id} in DB."
                ))
            })?)
            .map_err(|e| {
                RequiredTableComponentMissing::new(warehouse_id, table_id).append_detail(format!(
                    "Error re-creating default partition type after DB load: {e}"
                ))
            })?;

        let next_row_id = u64::try_from(self.next_row_id).map_err(|e| {
            ConversionError::new(
                format!(
                    "Error converting next_row_id to u64. Got: {}",
                    self.next_row_id
                ),
                e,
            )
        })?;

        let encryption_keys = itertools::multizip((
            self.encryption_key_ids.unwrap_or_default(),
            self.encryption_encrypted_key_metadatas.unwrap_or_default(),
            self.encryption_encrypted_by_ids.unwrap_or_default(),
            self.encryption_properties.unwrap_or_default(),
        ))
        .map(
            |(key_id, encrypted_key_metadata, encrypted_by_id, properties)| {
                let properties = properties
                    .and_then(|p| serde_json::from_value::<HashMap<String, String>>(p).ok())
                    .unwrap_or_default();
                let encrypted_key = EncryptedKey::builder()
                    .key_id(key_id.clone())
                    .encrypted_key_metadata(encrypted_key_metadata)
                    .properties(properties);
                let encrypted_key = if let Some(encrypted_by_id) = encrypted_by_id {
                    encrypted_key.encrypted_by_id(encrypted_by_id).build()
                } else {
                    encrypted_key.build()
                };
                (key_id, encrypted_key)
            },
        )
        .collect::<HashMap<_, _>>();

        let table_location = join_location(&self.table_fs_protocol, &self.table_fs_location)
            .map_err(InternalParseLocationError::from)?
            .to_string();
        let mut table_metadata = TableMetadata::builder()
            .format_version(FormatVersion::from(self.table_format_version))
            .table_uuid(self.table_id)
            .location(table_location)
            .last_sequence_number(self.last_sequence_number)
            .last_updated_ms(self.last_updated_ms)
            .last_column_id(self.last_column_id)
            .schemas(schemas)
            .current_schema_id(current_schema_id)
            .partition_specs(partition_specs)
            .default_spec(default_spec)
            .default_partition_type(default_partition_type)
            .last_partition_id(self.last_partition_id)
            .properties(properties)
            .current_snapshot_id(current_snapshot_id)
            .snapshots(snapshots)
            .snapshot_log(snapshot_log)
            .metadata_log(metadata_log)
            .sort_orders(sort_orders)
            .default_sort_order_id(expect(
                self.default_sort_order_id,
                "Default Sort Order ID",
                &info,
            )?)
            .refs(refs)
            .partition_statistics(partition_statistics)
            .statistics(statistics)
            .encryption_keys(encryption_keys)
            .next_row_id(next_row_id)
            .build_unchecked();

        table_metadata.try_normalize().map_err(|e| {
            InternalTableMetadataBuildFailed::new(warehouse_id, table_id).append_detail(format!(
                "Failed to normalize table metadata after DB load: {e}"
            ))
        })?;

        Ok(table_metadata)
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) async fn load_tables(
    warehouse_id: WarehouseId,
    tables: impl IntoIterator<Item = TableId>,
    include_deleted: bool,
    filters: &LoadTableFilters,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<Vec<LoadTableResponse>, LoadTableError> {
    let table_ids = &tables.into_iter().map(Into::into).collect::<Vec<_>>();
    let LoadTableFilters {
        snapshots: snapshots_filter,
    } = filters;

    let table = sqlx::query_as!(
        TableQueryStruct,
        r#"
        WITH filtered_table_refs AS (
            SELECT warehouse_id, table_id, snapshot_id, table_ref_name, retention
            FROM table_refs
            WHERE warehouse_id = $1 AND table_id = ANY($2)
        ),
        snapshots_to_load AS (
            -- refs mode: drive from filtered_table_refs (one index lookup per ref)
            SELECT ts.table_id, ts.snapshot_id, ts.parent_snapshot_id, ts.sequence_number,
                   ts.manifest_list, ts.summary, ts.schema_id, ts.timestamp_ms,
                   ts.first_row_id, ts.assigned_rows, ts.key_id
            FROM table_snapshot ts
            INNER JOIN filtered_table_refs ftr
                ON ftr.warehouse_id = ts.warehouse_id
               AND ftr.table_id    = ts.table_id
               AND ftr.snapshot_id = ts.snapshot_id
            WHERE $4 = 'refs'
            UNION ALL
            -- all mode: full scan, unchanged behaviour
            SELECT table_id, snapshot_id, parent_snapshot_id, sequence_number,
                   manifest_list, summary, schema_id, timestamp_ms,
                   first_row_id, assigned_rows, key_id
            FROM table_snapshot
            WHERE warehouse_id = $1 AND table_id = ANY($2)
            AND $4 = 'all'
        )
        SELECT
            t.warehouse_id,
            t.table_id,
            t.last_sequence_number,
            t.last_column_id,
            t.last_updated_ms,
            t.last_partition_id,
            t.table_format_version as "table_format_version: DbTableFormatVersion",
            t.next_row_id,
            ti.name as "table_name",
            ti.fs_location as "table_fs_location",
            ti.fs_protocol as "table_fs_protocol",
            ti.tabular_namespace_name as "namespace_name",
            ti.namespace_id,
            ti."metadata_location",
            w.version as "warehouse_version",
            tcs.schema_id as "current_schema",
            tdps.partition_spec_id as "default_partition_spec_id",
            tsnap.snapshot_ids,
            tsnap.parent_snapshot_ids as "snapshot_parent_snapshot_id: Vec<Option<i64>>",
            tsnap.sequence_numbers as "snapshot_sequence_number",
            tsnap.manifest_lists as "snapshot_manifest_list: Vec<String>",
            tsnap.timestamp as "snapshot_timestamp_ms",
            tsnap.summaries as "snapshot_summary: Vec<Json<Summary>>",
            tsnap.schema_ids as "snapshot_schema_id: Vec<Option<i32>>",
            tsnap.first_row_ids as "snapshot_first_row_ids: Vec<Option<i64>>",
            tsnap.assigned_rows as "snapshot_assigned_rows: Vec<Option<i64>>",
            tsnap.key_id as "snapshot_key_ids: Vec<Option<String>>",
            tdsort.sort_order_id as "default_sort_order_id?",
            tps.partition_spec_id as "partition_spec_ids",
            tps.partition_spec as "partition_specs: Vec<Json<PartitionSpec>>",
            tp.keys as "table_properties_keys",
            tp.values as "table_properties_values",
            tsl.snapshot_ids as "snapshot_log_ids",
            tsl.timestamps as "snapshot_log_timestamps",
            tml.metadata_files as "metadata_log_files",
            tml.timestamps as "metadata_log_timestamps",
            tso.sort_order_ids as "sort_order_ids",
            tso.sort_orders as "sort_orders: Vec<Json<SortOrder>>",
            tr.table_ref_names as "table_ref_names",
            tr.snapshot_ids as "table_ref_snapshot_ids",
            tr.retentions as "table_ref_retention: Vec<Json<SnapshotRetention>>",
            pstat.snapshot_ids as "partition_stats_snapshot_ids",
            pstat.statistics_paths as "partition_stats_statistics_paths",
            pstat.file_size_in_bytes_s as "partition_stats_file_size_in_bytes",
            tstat.snapshot_ids as "table_stats_snapshot_ids",
            tstat.statistics_paths as "table_stats_statistics_paths",
            tstat.file_size_in_bytes_s as "table_stats_file_size_in_bytes",
            tstat.file_footer_size_in_bytes_s as "table_stats_file_footer_size_in_bytes",
            tstat.key_metadatas as "table_stats_key_metadata: Vec<Option<String>>",
            tstat.blob_metadatas as "table_stats_blob_metadata: Vec<Json<Vec<BlobMetadata>>>",
            tenc.key_ids as "encryption_key_ids",
            tenc.encrypted_key_metadatas as "encryption_encrypted_key_metadatas",
            tenc.encrypted_by_ids as "encryption_encrypted_by_ids: Vec<Option<String>>",
            tenc.properties as "encryption_properties: Vec<Option<serde_json::Value>>"
        FROM "table" t
        INNER JOIN tabular ti ON ti.warehouse_id = $1 AND t.table_id = ti.tabular_id
        INNER JOIN warehouse w ON w.warehouse_id = $1
        INNER JOIN table_current_schema tcs
            ON tcs.warehouse_id = $1 AND tcs.table_id = t.table_id
        LEFT JOIN table_default_partition_spec tdps
            ON tdps.warehouse_id = $1 AND tdps.table_id = t.table_id
        LEFT JOIN table_default_sort_order tdsort
            ON tdsort.warehouse_id = $1 AND tdsort.table_id = t.table_id
        LEFT JOIN (SELECT table_id,
                          ARRAY_AGG(partition_spec) as partition_spec,
                          ARRAY_AGG(partition_spec_id) as partition_spec_id
                   FROM table_partition_spec WHERE warehouse_id = $1 AND table_id = ANY($2)
                   GROUP BY table_id) tps ON tps.table_id = t.table_id
        LEFT JOIN (SELECT table_id,
                            ARRAY_AGG(key) as keys,
                            ARRAY_AGG(value) as values
                     FROM table_properties WHERE warehouse_id = $1 AND table_id = ANY($2)
                     GROUP BY table_id) tp ON tp.table_id = t.table_id
        LEFT JOIN (SELECT table_id,
                          ARRAY_AGG(snapshot_id) as snapshot_ids,
                          ARRAY_AGG(parent_snapshot_id) as parent_snapshot_ids,
                          ARRAY_AGG(sequence_number) as sequence_numbers,
                          ARRAY_AGG(manifest_list) as manifest_lists,
                          ARRAY_AGG(summary) as summaries,
                          ARRAY_AGG(schema_id) as schema_ids,
                          ARRAY_AGG(timestamp_ms) as timestamp,
                          ARRAY_AGG(first_row_id) as first_row_ids,
                          ARRAY_AGG(assigned_rows) as assigned_rows,
                          ARRAY_AGG(key_id) as key_id
                   FROM snapshots_to_load
                   GROUP BY table_id) tsnap ON tsnap.table_id = t.table_id
        LEFT JOIN (SELECT table_id,
                          ARRAY_AGG(snapshot_id ORDER BY sequence_number) as snapshot_ids,
                          ARRAY_AGG(timestamp ORDER BY sequence_number) as timestamps
                     FROM table_snapshot_log WHERE warehouse_id = $1 AND table_id = ANY($2)
                     GROUP BY table_id) tsl ON tsl.table_id = t.table_id
        LEFT JOIN (SELECT table_id,
                          ARRAY_AGG(timestamp ORDER BY sequence_number) as timestamps,
                          ARRAY_AGG(metadata_file ORDER BY sequence_number) as metadata_files
                   FROM table_metadata_log WHERE warehouse_id = $1 AND table_id = ANY($2)
                   GROUP BY table_id) tml ON tml.table_id = t.table_id
        LEFT JOIN (SELECT table_id,
                          ARRAY_AGG(sort_order_id) as sort_order_ids,
                          ARRAY_AGG(sort_order) as sort_orders
                     FROM table_sort_order WHERE warehouse_id = $1 AND table_id = ANY($2)
                     GROUP BY table_id) tso ON tso.table_id = t.table_id
        LEFT JOIN (SELECT table_id,
                          ARRAY_AGG(table_ref_name) as table_ref_names,
                          ARRAY_AGG(snapshot_id) as snapshot_ids,
                          ARRAY_AGG(retention) as retentions
                   FROM filtered_table_refs
                   GROUP BY table_id) tr ON tr.table_id = t.table_id
        LEFT JOIN (SELECT table_id,
                          ARRAY_AGG(snapshot_id) as snapshot_ids,
                          ARRAY_AGG(statistics_path) as statistics_paths,
                          ARRAY_AGG(file_size_in_bytes) as file_size_in_bytes_s
                    FROM partition_statistics WHERE warehouse_id = $1 AND table_id = ANY($2)
                    GROUP BY table_id) pstat ON pstat.table_id = t.table_id
        LEFT JOIN (SELECT table_id,
                          ARRAY_AGG(snapshot_id) as snapshot_ids,
                          ARRAY_AGG(statistics_path) as statistics_paths,
                          ARRAY_AGG(file_size_in_bytes) as file_size_in_bytes_s,
                          ARRAY_AGG(file_footer_size_in_bytes) as file_footer_size_in_bytes_s,
                          ARRAY_AGG(key_metadata) as key_metadatas,
                          ARRAY_AGG(blob_metadata) as blob_metadatas
                    FROM table_statistics WHERE warehouse_id = $1 AND table_id = ANY($2)
                    GROUP BY table_id) tstat ON tstat.table_id = t.table_id
        LEFT JOIN (
            SELECT table_id,
                   ARRAY_AGG(key_id) as key_ids,
                   ARRAY_AGG(encrypted_key_metadata) as encrypted_key_metadatas,
                   ARRAY_AGG(encrypted_by_id) as encrypted_by_ids,
                   ARRAY_AGG(properties) as properties
            FROM table_encryption_keys
            WHERE warehouse_id = $1 AND table_id = ANY($2)
            GROUP BY table_id
        ) tenc ON tenc.table_id = t.table_id
        WHERE t.warehouse_id = $1
            AND w.status = 'active'
            AND (ti.deleted_at IS NULL OR $3)
            AND t."table_id" = ANY($2)
        "#,
        *warehouse_id,
        &table_ids,
        include_deleted,
        match snapshots_filter {
            SnapshotsQuery::All => "all",
            SnapshotsQuery::Refs => "refs",
        }
    )
    .fetch_all(&mut **transaction)
    .await
    .map_err(super::super::dbutils::DBErrorHandler::into_catalog_backend_error)?;

    // Schemas live in the normalized `schema_field` table (one row per field). Fetch them flat
    // for the whole batch in one query and group per table; assembled in `into_table_metadata`.
    let schema_field_rows = sqlx::query!(
        r#"SELECT tabular_id,
                  schema_id, field_id, parent_field_id, ordinal, name, required, doc,
                  type_kind::text as "type_kind!", type_params, initial_default, write_default,
                  is_identifier
           FROM schema_field
           WHERE warehouse_id = $1 AND tabular_id = ANY($2)
           ORDER BY tabular_id, schema_id, parent_field_id, ordinal"#,
        *warehouse_id,
        &table_ids,
    )
    .fetch_all(&mut **transaction)
    .await
    .map_err(super::super::dbutils::DBErrorHandler::into_catalog_backend_error)?;

    let mut schema_rows_by_table: HashMap<Uuid, Vec<normalized_schema::SchemaFieldRow>> =
        HashMap::new();
    for r in schema_field_rows {
        schema_rows_by_table.entry(r.tabular_id).or_default().push(
            normalized_schema::SchemaFieldRow {
                schema_id: r.schema_id,
                field_id: r.field_id,
                parent_field_id: r.parent_field_id,
                ordinal: r.ordinal,
                name: r.name,
                required: r.required,
                doc: r.doc,
                type_kind: r.type_kind,
                type_params: r.type_params,
                initial_default: r.initial_default,
                write_default: r.write_default,
                is_identifier: r.is_identifier,
            },
        );
    }

    // Authoritative schema-id set per table (the `table_schema` anchor rows). Drives assembly so a
    // legitimately-empty schema (anchor present, no field rows) is reconstructed, not dropped.
    let schema_anchor_rows = sqlx::query!(
        r#"SELECT table_id, schema_id FROM table_schema
           WHERE warehouse_id = $1 AND table_id = ANY($2)"#,
        *warehouse_id,
        &table_ids,
    )
    .fetch_all(&mut **transaction)
    .await
    .map_err(super::super::dbutils::DBErrorHandler::into_catalog_backend_error)?;
    let mut schema_ids_by_table: HashMap<Uuid, Vec<i32>> = HashMap::new();
    for r in schema_anchor_rows {
        schema_ids_by_table
            .entry(r.table_id)
            .or_default()
            .push(r.schema_id);
    }

    table
        .into_iter()
        .map(|table| {
            let warehouse_version = table.warehouse_version;
            let table_id = table.table_id.into();
            let metadata_location = table
                .metadata_location
                .as_deref()
                .map(FromStr::from_str)
                .transpose()
                .map_err(InternalParseLocationError::from)?;
            let namespace_id = table.namespace_id.into();
            let schema_rows = schema_rows_by_table
                .remove(&table.table_id)
                .unwrap_or_default();
            let expected_schema_ids = schema_ids_by_table
                .remove(&table.table_id)
                .unwrap_or_default();
            let table_metadata = table.into_table_metadata(schema_rows, &expected_schema_ids)?;

            Ok(LoadTableResponse {
                table_id,
                namespace_id,
                table_metadata,
                metadata_location,
                warehouse_version: warehouse_version.into(),
            })
        })
        .collect()
}

#[derive(Default)]
#[allow(clippy::struct_excessive_bools)]
struct TableUpdateFlags {
    snapshot_refs: bool,
    properties: bool,
}

impl From<&[TableUpdate]> for TableUpdateFlags {
    fn from(value: &[TableUpdate]) -> Self {
        let mut s = TableUpdateFlags::default();
        for u in value {
            match u {
                TableUpdate::RemoveSnapshotRef { .. } | TableUpdate::SetSnapshotRef { .. } => {
                    s.snapshot_refs = true;
                }
                TableUpdate::RemoveProperties { .. } | TableUpdate::SetProperties { .. } => {
                    s.properties = true;
                }
                _ => {}
            }
        }
        s
    }
}

#[cfg(any(test, feature = "test-utils"))]
#[allow(unused_imports, dead_code)]
pub mod tests {
    // Desired behavior:
    // - Stage-Create => Load fails with 404
    // - No Stage-Create => Next create fails with 409, load succeeds
    // - Stage-Create => Next stage-create works & overwrites
    // - Stage-Create => Next regular create works & overwrites

    use std::{default::Default, time::SystemTime};

    use iceberg::{
        NamespaceIdent, TableIdent,
        spec::{
            NestedField, Operation, PrimitiveType, Schema, Snapshot, SnapshotReference,
            UnboundPartitionSpec,
        },
    };
    use iceberg_ext::catalog::rest::CreateTableRequest;
    use lakekeeper::{
        api::{
            iceberg::{
                types::PageToken,
                v1::{PaginationQuery, tables::LoadTableFilters},
            },
            management::v1::{DeleteKind, warehouse::WarehouseStatus},
        },
        server::tables::create_table::create_table_request_into_table_metadata,
        service::{
            AllowedFormatVersions, CreateTableError, NamedEntity, NamespaceId, RenameTabularError,
            TableCreation, TabularIdentBorrowed, TabularListFlags, ViewOrTableInfo,
            tasks::{
                ScheduleTaskMetadata, TaskEntity, WarehouseTaskEntityId,
                tabular_expiration_queue::{TabularExpirationPayload, TabularExpirationTask},
            },
        },
    };
    use lakekeeper_io::Location;
    use uuid::Uuid;

    use super::*;
    use crate::{
        CatalogState, PostgresBackend,
        namespace::tests::initialize_namespace,
        tabular::{
            drop_tabular, get_tabular_infos_by_idents, get_tabular_infos_by_ids,
            get_tabular_infos_by_s3_location, list_tabulars, mark_tabular_as_deleted,
            rename_tabular, table::create::create_table,
        },
        warehouse::{set_warehouse_status, test::initialize_warehouse},
    };

    fn create_request(
        stage_create: Option<bool>,
        table_name: Option<String>,
    ) -> (CreateTableRequest, Option<Location>) {
        let location = format!("s3://my_bucket/my_table/{}", Uuid::now_v7());

        let metadata_location = if let Some(stage_create) = stage_create {
            if stage_create {
                None
            } else {
                Some(
                    format!("{location}/metadata/metadata-{}.json", Uuid::now_v7())
                        .parse()
                        .unwrap(),
                )
            }
        } else {
            Some(
                format!("{location}/metadata/metadata-{}.json", Uuid::now_v7())
                    .parse()
                    .unwrap(),
            )
        };

        (
            CreateTableRequest {
                name: table_name.unwrap_or("my_table".to_string()),
                location: Some(location),
                schema: Schema::builder()
                    .with_fields(vec![
                        NestedField::required(
                            1,
                            "id",
                            iceberg::spec::Type::Primitive(PrimitiveType::Int),
                        )
                        .into(),
                        NestedField::required(
                            2,
                            "name",
                            iceberg::spec::Type::Primitive(PrimitiveType::String),
                        )
                        .into(),
                    ])
                    .build()
                    .unwrap(),
                partition_spec: Some(UnboundPartitionSpec::builder().build()),
                write_order: None,
                stage_create,
                properties: None,
            },
            metadata_location,
        )
    }

    pub(crate) async fn get_namespace_id(
        state: CatalogState,
        warehouse_id: WarehouseId,
        namespace: &NamespaceIdent,
    ) -> NamespaceId {
        let namespace = sqlx::query!(
            r#"
            SELECT namespace_id
            FROM namespace
            WHERE warehouse_id = $1 AND namespace_name = $2
            "#,
            *warehouse_id,
            &**namespace
        )
        .fetch_one(&state.read_pool())
        .await
        .unwrap();
        namespace.namespace_id.into()
    }

    pub struct InitializedTable {
        #[allow(dead_code)]
        pub namespace_id: NamespaceId,
        pub namespace: NamespaceIdent,
        pub table_id: TableId,
        pub table_ident: TableIdent,
    }

    /// Creates a table in the given warehouse.
    ///
    /// Parameters:
    ///
    /// * `namespace`: If provided creates the table in that namespace, otherwise creates new one.
    /// * `table_id`: If provided uses this as the table's id, otherwise creates a random id.
    pub async fn initialize_table(
        warehouse_id: WarehouseId,
        state: CatalogState,
        staged: bool,
        namespace: Option<NamespaceIdent>,
        table_id: Option<TableId>,
        table_name: Option<String>,
    ) -> InitializedTable {
        // my_namespace_<uuid>
        let namespace = if let Some(namespace) = namespace {
            namespace
        } else {
            let namespace =
                NamespaceIdent::from_vec(vec![format!("my_namespace_{}", Uuid::now_v7())]).unwrap();
            initialize_namespace(state.clone(), warehouse_id, &namespace, None).await;
            namespace
        };
        let namespace_id = get_namespace_id(state.clone(), warehouse_id, &namespace).await;

        let (request, metadata_location) = create_request(Some(staged), table_name);
        let table_ident = TableIdent {
            namespace: namespace.clone(),
            name: request.name.clone(),
        };
        let table_id = table_id.unwrap_or_else(|| Uuid::now_v7().into());

        let table_metadata = create_table_request_into_table_metadata(
            table_id,
            request,
            &AllowedFormatVersions::default(),
            None,
        )
        .unwrap();
        let schema = table_metadata.current_schema_id();
        let table_metadata = table_metadata
            .into_builder(None)
            .add_snapshot(
                Snapshot::builder()
                    .with_manifest_list("a.txt")
                    .with_parent_snapshot_id(None)
                    .with_schema_id(schema)
                    .with_sequence_number(1)
                    .with_snapshot_id(1)
                    .with_summary(Summary {
                        operation: Operation::Append,
                        additional_properties: HashMap::default(),
                    })
                    .with_timestamp_ms(
                        SystemTime::now()
                            .duration_since(SystemTime::UNIX_EPOCH)
                            .unwrap()
                            .as_millis()
                            .try_into()
                            .unwrap(),
                    )
                    .build(),
            )
            .unwrap()
            .set_ref(
                "my_ref",
                SnapshotReference {
                    snapshot_id: 1,
                    retention: SnapshotRetention::Tag {
                        max_ref_age_ms: None,
                    },
                },
            )
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let create = TableCreation {
            warehouse_id,
            namespace_id,
            table_ident: &table_ident,
            table_metadata: &table_metadata,
            metadata_location: metadata_location.as_ref(),
        };
        let mut transaction = state.write_pool().begin().await.unwrap();
        let _create_result = create_table(create, &mut transaction).await.unwrap();

        transaction.commit().await.unwrap();

        InitializedTable {
            namespace_id,
            namespace,
            table_id,
            table_ident,
        }
    }

    /// Create a real table (via the production `create_table` path, which writes `schema_field`)
    /// whose current schema is `schema`, in a fresh namespace. Returns the table id and the PERSISTED
    /// current schema — create-time normalization may reassign field ids, so assertions should use
    /// the returned schema, not the input.
    pub(crate) async fn create_table_with_schema(
        state: CatalogState,
        warehouse_id: WarehouseId,
        schema: Schema,
    ) -> (TableId, Schema) {
        let namespace =
            NamespaceIdent::from_vec(vec![format!("my_namespace_{}", Uuid::now_v7())]).unwrap();
        initialize_namespace(state.clone(), warehouse_id, &namespace, None).await;
        let namespace_id = get_namespace_id(state.clone(), warehouse_id, &namespace).await;

        let table_id: TableId = Uuid::now_v7().into();
        let name = format!("my_table_{}", Uuid::now_v7());
        let location = format!("s3://my_bucket/{}", Uuid::now_v7());
        let metadata_location: Location =
            format!("{location}/metadata/metadata-{}.json", Uuid::now_v7())
                .parse()
                .unwrap();
        let request = CreateTableRequest {
            name: name.clone(),
            location: Some(location),
            schema,
            partition_spec: Some(UnboundPartitionSpec::builder().build()),
            write_order: None,
            stage_create: Some(false),
            properties: None,
        };
        let table_metadata = create_table_request_into_table_metadata(
            table_id,
            request,
            &AllowedFormatVersions::default(),
            // v3: test schemas may carry non-null column defaults, invalid before v3.
            Some(FormatVersion::V3),
        )
        .unwrap();
        let table_ident = TableIdent { namespace, name };
        let create = TableCreation {
            warehouse_id,
            namespace_id,
            table_ident: &table_ident,
            table_metadata: &table_metadata,
            metadata_location: Some(&metadata_location),
        };
        let mut transaction = state.write_pool().begin().await.unwrap();
        create_table(create, &mut transaction).await.unwrap();
        transaction.commit().await.unwrap();

        (table_id, table_metadata.current_schema().as_ref().clone())
    }

    #[sqlx::test]
    async fn test_final_create(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let namespace = NamespaceIdent::from_vec(vec!["my_namespace".to_string()]).unwrap();
        initialize_namespace(state.clone(), warehouse_id, &namespace, None).await;
        let namespace_id = get_namespace_id(state.clone(), warehouse_id, &namespace).await;

        let (request, metadata_location) = create_request(None, None);
        let table_ident = TableIdent {
            namespace: namespace.clone(),
            name: request.name.clone(),
        };
        assert!(metadata_location.is_some());

        let mut transaction = pool.begin().await.unwrap();
        let table_id = uuid::Uuid::now_v7().into();

        let table_metadata = create_table_request_into_table_metadata(
            table_id,
            request,
            &AllowedFormatVersions::default(),
            None,
        )
        .unwrap();

        let request = TableCreation {
            warehouse_id,
            namespace_id,
            table_ident: &table_ident,
            table_metadata: &table_metadata,
            metadata_location: metadata_location.as_ref(),
        };

        let original_table_metadata = request.table_metadata;
        let (tabular_info, staged) = create_table(request.clone(), &mut transaction)
            .await
            .unwrap();
        transaction.commit().await.unwrap();
        assert_eq!(staged, None);
        assert_eq!(tabular_info.tabular_id, table_id);
        assert_eq!(tabular_info.metadata_location, metadata_location);

        let mut transaction = pool.begin().await.unwrap();
        // Second create should fail
        let mut request = request;
        // exchange location else we fail on unique constraint there
        let location = format!("s3://my_bucket/my_table/other/{}", Uuid::now_v7())
            .as_str()
            .parse::<Location>()
            .unwrap();
        let build = (*request.table_metadata)
            .clone()
            .into_builder(None)
            .set_location(location.to_string())
            .assign_uuid(Uuid::now_v7())
            .build()
            .unwrap()
            .metadata;
        request.table_metadata = &build;
        let create_err = create_table(request, &mut transaction).await.unwrap_err();

        assert!(matches!(
            create_err,
            CreateTableError::TabularAlreadyExists(_)
        ));

        // Load should succeed
        let mut t = pool.begin().await.unwrap();
        let load_result = load_tables(
            warehouse_id,
            vec![table_id],
            false,
            &LoadTableFilters::default(),
            &mut t,
        )
        .await
        .unwrap();
        let load_result = load_result
            .into_iter()
            .map(|r| (r.table_id, r))
            .collect::<HashMap<_, _>>();
        assert_eq!(load_result.len(), 1);
        assert_eq!(
            &load_result.get(&table_id).unwrap().table_metadata,
            original_table_metadata
        );
    }

    #[sqlx::test]
    async fn test_stage_create(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let namespace = NamespaceIdent::from_vec(vec!["my_namespace".to_string()]).unwrap();
        initialize_namespace(state.clone(), warehouse_id, &namespace, None).await;
        let namespace_id = get_namespace_id(state.clone(), warehouse_id, &namespace).await;

        let (request, metadata_location) = create_request(Some(true), None);
        let table_ident = TableIdent {
            namespace: namespace.clone(),
            name: request.name.clone(),
        };
        assert_eq!(metadata_location, None);

        let mut transaction = pool.begin().await.unwrap();
        let staged_table_id = uuid::Uuid::now_v7().into();
        let table_metadata = create_table_request_into_table_metadata(
            staged_table_id,
            request,
            &AllowedFormatVersions::default(),
            None,
        )
        .unwrap();

        let request = TableCreation {
            warehouse_id,
            namespace_id,
            table_ident: &table_ident,
            table_metadata: &table_metadata,
            metadata_location: metadata_location.as_ref(),
        };

        let _create_result = create_table(request.clone(), &mut transaction)
            .await
            .unwrap();
        transaction.commit().await.unwrap();

        // Its staged - should not have metadata_location
        let load = load_tables(
            warehouse_id,
            [staged_table_id],
            false,
            &LoadTableFilters::default(),
            &mut pool.begin().await.unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(load.len(), 1);
        let load = &load[0];
        assert!(load.metadata_location.is_none());

        // Second create should succeed, even with different id
        let mut transaction = pool.begin().await.unwrap();
        let mut request = request;
        let updated_metadata = (*request.table_metadata)
            .clone()
            .into_builder(None)
            .assign_uuid(Uuid::now_v7())
            .build()
            .unwrap()
            .metadata;
        request.table_metadata = &updated_metadata;

        let _create_result = create_table(request, &mut transaction).await.unwrap();
        transaction.commit().await.unwrap();

        // We can overwrite the table with a regular create
        let (request, metadata_location) = create_request(Some(false), None);

        let table_metadata = create_table_request_into_table_metadata(
            staged_table_id,
            request,
            &AllowedFormatVersions::default(),
            None,
        )
        .unwrap();

        let request = TableCreation {
            warehouse_id,
            namespace_id,
            table_ident: &table_ident,
            table_metadata: &table_metadata,
            metadata_location: metadata_location.as_ref(),
        };
        let mut transaction = pool.begin().await.unwrap();
        let (_create_result, previous_staged_table) =
            create_table(request, &mut transaction).await.unwrap();
        transaction.commit().await.unwrap();
        // New table get's new id
        assert!(previous_staged_table.unwrap().0 != staged_table_id);
        let load_result = load_tables(
            warehouse_id,
            [staged_table_id],
            false,
            &LoadTableFilters::default(),
            &mut pool.begin().await.unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(load_result.len(), 1);
        let load_result = &load_result[0];
        let s1 = format!("{:#?}", load_result.table_metadata);
        let s2 = format!("{table_metadata:#?}");
        let diff = similar::TextDiff::from_lines(&s1, &s2);
        let diff = diff
            .unified_diff()
            .context_radius(15)
            .missing_newline_hint(false)
            .to_string();
        assert_eq!(load_result.table_metadata, table_metadata, "{diff}");
        assert_eq!(load_result.metadata_location, metadata_location);
    }

    #[sqlx::test]
    async fn test_to_id(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let namespace = NamespaceIdent::from_vec(vec!["my_namespace".to_string()]).unwrap();
        initialize_namespace(state.clone(), warehouse_id, &namespace, None).await;

        let table_ident = TableIdent {
            namespace: namespace.clone(),
            name: "my_table".to_string(),
        };

        let infos = get_tabular_infos_by_idents(
            warehouse_id,
            &[TabularIdentBorrowed::Table(&table_ident)],
            TabularListFlags::active(),
            &state.read_pool(),
        )
        .await
        .unwrap()
        .into_values()
        .collect::<Vec<_>>();
        assert!(infos.is_empty());
        drop(table_ident);

        let table = initialize_table(warehouse_id, state.clone(), true, None, None, None).await;

        // Table is staged - no result if include_staged is false
        let infos = get_tabular_infos_by_idents(
            warehouse_id,
            &[TabularIdentBorrowed::Table(&table.table_ident)],
            TabularListFlags::active(),
            &state.read_pool(),
        )
        .await
        .unwrap()
        .into_values()
        .collect::<Vec<_>>();
        assert!(infos.is_empty());

        let infos = get_tabular_infos_by_idents(
            warehouse_id,
            &[TabularIdentBorrowed::Table(&table.table_ident)],
            TabularListFlags {
                include_staged: true,
                ..TabularListFlags::active()
            },
            &state.read_pool(),
        )
        .await
        .unwrap()
        .into_values()
        .collect::<Vec<_>>();
        assert_eq!(infos.len(), 1);
        let info = &infos[0];
        assert_eq!(info.tabular_id(), table.table_id.into());
    }

    #[sqlx::test]
    async fn test_to_ids(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let namespace = NamespaceIdent::from_vec(vec!["my_namespace".to_string()]).unwrap();
        initialize_namespace(state.clone(), warehouse_id, &namespace, None).await;

        let table_ident = TableIdent {
            namespace: namespace.clone(),
            name: "my_table".to_string(),
        };

        let infos = get_tabular_infos_by_idents(
            warehouse_id,
            &[TabularIdentBorrowed::Table(&table_ident)],
            TabularListFlags::active(),
            &state.read_pool(),
        )
        .await
        .unwrap()
        .into_values()
        .collect::<Vec<_>>();
        assert_eq!(infos.len(), 0);

        let table_1 = initialize_table(warehouse_id, state.clone(), true, None, None, None).await;

        // Table is staged - no result if include_staged is false
        let infos = get_tabular_infos_by_idents(
            warehouse_id,
            &[TabularIdentBorrowed::Table(&table_1.table_ident)],
            TabularListFlags::active(),
            &state.read_pool(),
        )
        .await
        .unwrap()
        .into_values()
        .collect::<Vec<_>>();
        assert_eq!(infos.len(), 0);

        let infos = get_tabular_infos_by_idents(
            warehouse_id,
            &[TabularIdentBorrowed::Table(&table_1.table_ident)],
            TabularListFlags {
                include_staged: true,
                ..TabularListFlags::active()
            },
            &state.read_pool(),
        )
        .await
        .unwrap()
        .into_values()
        .collect::<Vec<_>>();
        assert_eq!(infos.len(), 1);
        assert_eq!(infos[0].tabular_id(), table_1.table_id.into());

        // Second Table
        let table_2 = initialize_table(warehouse_id, state.clone(), false, None, None, None).await;

        let infos = get_tabular_infos_by_idents(
            warehouse_id,
            &[
                TabularIdentBorrowed::Table(&table_1.table_ident),
                TabularIdentBorrowed::Table(&table_2.table_ident),
            ],
            TabularListFlags::active(),
            &state.read_pool(),
        )
        .await
        .unwrap()
        .into_values()
        .collect::<Vec<_>>();
        // Only table_2 should be returned (table_1 is staged)
        assert_eq!(infos.len(), 1);
        assert_eq!(infos[0].tabular_id(), table_2.table_id.into());

        let infos = get_tabular_infos_by_idents(
            warehouse_id,
            &[
                TabularIdentBorrowed::Table(&table_1.table_ident),
                TabularIdentBorrowed::Table(&table_2.table_ident),
            ],
            TabularListFlags {
                include_staged: true,
                ..TabularListFlags::active()
            },
            &state.read_pool(),
        )
        .await
        .unwrap()
        .into_values()
        .collect::<Vec<_>>();
        // Both tables should be returned
        assert_eq!(infos.len(), 2);
        let ids: std::collections::HashSet<_> =
            infos.iter().map(ViewOrTableInfo::tabular_id).collect();
        assert!(ids.contains(&table_1.table_id.into()));
        assert!(ids.contains(&table_2.table_id.into()));
    }

    #[sqlx::test]
    async fn test_to_ids_case_insensitivity(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let namespace_parent = NamespaceIdent::from_vec(vec!["my_namespace".to_string()]).unwrap();
        let namespace_lower =
            NamespaceIdent::from_vec(vec!["my_namespace".to_string(), "child".to_string()])
                .unwrap();
        let namespace_upper =
            NamespaceIdent::from_vec(vec!["MY_NAMESPACE".to_string(), "CHILD".to_string()])
                .unwrap();
        initialize_namespace(state.clone(), warehouse_id, &namespace_parent, None).await;
        initialize_namespace(state.clone(), warehouse_id, &namespace_lower, None).await;

        let table_ident_lower = TableIdent {
            namespace: namespace_lower.clone(),
            name: "my_table".to_string(),
        };
        let table_ident_upper = TableIdent {
            namespace: namespace_upper.clone(),
            name: "MY_TABLE".to_string(),
        };

        let created = initialize_table(
            warehouse_id,
            state.clone(),
            false,
            Some(namespace_lower.clone()),
            None,
            Some(table_ident_lower.name.clone()),
        )
        .await;
        let _ = initialize_table(
            warehouse_id,
            state.clone(),
            false,
            Some(namespace_lower.clone()),
            None,
            Some("a_table_not_to_be_included_in_results".to_string()),
        )
        .await;

        // Lower idents are in db and we query upper.
        let infos = get_tabular_infos_by_idents(
            warehouse_id,
            &[TabularIdentBorrowed::Table(&table_ident_upper)],
            TabularListFlags::active(),
            &state.read_pool(),
        )
        .await
        .unwrap()
        .into_values()
        .collect::<Vec<_>>();
        assert_eq!(infos.len(), 1);
        // Should find the table by case-insensitive match
        assert_eq!(infos[0].tabular_id(), created.table_id.into());

        // Verify behavior of querying the same table twice with different cases.
        let infos = get_tabular_infos_by_idents(
            warehouse_id,
            &[
                TabularIdentBorrowed::Table(&table_ident_lower),
                TabularIdentBorrowed::Table(&table_ident_upper),
            ],
            TabularListFlags::active(),
            &state.read_pool(),
        )
        .await
        .unwrap();
        // Both queries should resolve to the same table, but keyed by the input casing
        assert_eq!(infos.len(), 2);
        assert!(infos.contains_key(&table_ident_lower));
        assert!(infos.contains_key(&table_ident_upper));
        let id_lower = infos[&table_ident_lower].tabular_id();
        let id_upper = infos[&table_ident_upper].tabular_id();
        assert_eq!(id_lower, id_upper);
        assert_eq!(id_lower, created.table_id.into());
    }

    #[sqlx::test]
    async fn test_rename_without_namespace(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let table = initialize_table(warehouse_id, state.clone(), false, None, None, None).await;

        let new_table_ident = TableIdent {
            namespace: table.namespace.clone(),
            name: "new_table".to_string(),
        };

        let mut transaction = pool.begin().await.unwrap();
        rename_tabular(
            warehouse_id,
            table.table_id.into(),
            &table.table_ident,
            &new_table_ident,
            &mut transaction,
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();

        let infos = get_tabular_infos_by_idents(
            warehouse_id,
            &[TabularIdentBorrowed::Table(&table.table_ident)],
            TabularListFlags::active(),
            &state.read_pool(),
        )
        .await
        .unwrap()
        .into_values()
        .collect::<Vec<_>>();
        assert_eq!(infos.len(), 0);

        let exists = get_tabular_infos_by_idents(
            warehouse_id,
            &[TabularIdentBorrowed::Table(&new_table_ident)],
            TabularListFlags::active(),
            &state.read_pool(),
        )
        .await
        .unwrap()
        .into_values()
        .collect::<Vec<_>>();
        // Table id should be the same
        assert_eq!(exists.len(), 1);
        assert_eq!(exists[0].tabular_id(), table.table_id.into());
    }

    #[sqlx::test]
    async fn test_rename_with_namespace(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let table = initialize_table(warehouse_id, state.clone(), false, None, None, None).await;

        let new_namespace = NamespaceIdent::from_vec(vec!["new_namespace".to_string()]).unwrap();
        initialize_namespace(state.clone(), warehouse_id, &new_namespace, None).await;

        let new_table_ident = TableIdent {
            namespace: new_namespace.clone(),
            name: "new_table".to_string(),
        };

        let mut transaction = pool.begin().await.unwrap();
        rename_tabular(
            warehouse_id,
            table.table_id.into(),
            &table.table_ident,
            &new_table_ident,
            &mut transaction,
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();

        let infos = get_tabular_infos_by_idents(
            warehouse_id,
            &[TabularIdentBorrowed::Table(&table.table_ident)],
            TabularListFlags::active(),
            &state.read_pool(),
        )
        .await
        .unwrap()
        .into_values()
        .collect::<Vec<_>>();
        assert_eq!(infos.len(), 0);

        let infos = get_tabular_infos_by_idents(
            warehouse_id,
            &[TabularIdentBorrowed::Table(&new_table_ident)],
            TabularListFlags::active(),
            &state.read_pool(),
        )
        .await
        .unwrap()
        .into_values()
        .collect::<Vec<_>>();
        assert_eq!(infos.len(), 1);
        assert_eq!(infos[0].tabular_id(), table.table_id.into());
    }

    #[sqlx::test]
    async fn test_rename_to_non_existent_namespace(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let table = initialize_table(warehouse_id, state.clone(), false, None, None, None).await;

        let new_namespace = NamespaceIdent::from_vec(vec!["new_namespace".to_string()]).unwrap();

        let new_table_ident = TableIdent {
            namespace: new_namespace.clone(),
            name: "new_table".to_string(),
        };

        let mut transaction = pool.begin().await.unwrap();
        let rename_err = rename_tabular(
            warehouse_id,
            table.table_id.into(),
            &table.table_ident,
            &new_table_ident,
            &mut transaction,
        )
        .await
        .unwrap_err();
        assert!(
            matches!(rename_err, RenameTabularError::TabularNotFound(_),),
            "unexpected error: {rename_err:?}"
        );

        transaction.rollback().await.unwrap();
    }

    #[sqlx::test]
    async fn test_list_tables(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let namespace = NamespaceIdent::from_vec(vec!["my_namespace".to_string()]).unwrap();
        let namespace_id = initialize_namespace(state.clone(), warehouse_id, &namespace, None)
            .await
            .namespace_id();
        let tables = list_tabulars(
            warehouse_id,
            Some(namespace_id),
            TabularListFlags::active(),
            &state.read_pool(),
            None,
            PaginationQuery::empty(),
        )
        .await
        .unwrap();
        assert_eq!(tables.len(), 0);

        let table1 = initialize_table(warehouse_id, state.clone(), false, None, None, None).await;

        let tables = list_tabulars(
            warehouse_id,
            Some(table1.namespace_id),
            TabularListFlags::active(),
            &state.read_pool(),
            None,
            PaginationQuery::empty(),
        )
        .await
        .unwrap();
        assert_eq!(tables.len(), 1);
        assert_eq!(
            tables.get(&table1.table_id.into()).unwrap().tabular_ident(),
            &table1.table_ident
        );

        let table2 = initialize_table(warehouse_id, state.clone(), true, None, None, None).await;
        let tables = list_tabulars(
            warehouse_id,
            Some(table2.namespace_id),
            TabularListFlags::active(),
            &state.read_pool(),
            None,
            PaginationQuery::empty(),
        )
        .await
        .unwrap();
        assert_eq!(tables.len(), 0);
        let tables = list_tabulars(
            warehouse_id,
            Some(table2.namespace_id),
            TabularListFlags {
                include_staged: true,
                ..TabularListFlags::active()
            },
            &state.read_pool(),
            None,
            PaginationQuery::empty(),
        )
        .await
        .unwrap();
        assert_eq!(tables.len(), 1);
        assert_eq!(
            tables.get(&table2.table_id.into()).unwrap().tabular_ident(),
            &table2.table_ident
        );
    }

    #[sqlx::test]
    async fn test_list_tables_pagination(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let namespace = NamespaceIdent::from_vec(vec!["my_namespace".to_string()]).unwrap();
        let namespace_id = initialize_namespace(state.clone(), warehouse_id, &namespace, None)
            .await
            .namespace_id();
        let tables = list_tabulars(
            warehouse_id,
            Some(namespace_id),
            TabularListFlags::active(),
            &state.read_pool(),
            None,
            PaginationQuery::empty(),
        )
        .await
        .unwrap();
        assert_eq!(tables.len(), 0);

        let _ = initialize_table(
            warehouse_id,
            state.clone(),
            false,
            Some(namespace.clone()),
            None,
            Some("t1".into()),
        )
        .await;
        let table2 = initialize_table(
            warehouse_id,
            state.clone(),
            true,
            Some(namespace.clone()),
            None,
            Some("t2".into()),
        )
        .await;
        let table3 = initialize_table(
            warehouse_id,
            state.clone(),
            true,
            Some(namespace.clone()),
            None,
            Some("t3".into()),
        )
        .await;

        let tables = list_tabulars(
            warehouse_id,
            Some(namespace_id),
            TabularListFlags {
                include_staged: true,
                ..TabularListFlags::active()
            },
            &state.read_pool(),
            None,
            PaginationQuery {
                page_token: PageToken::NotSpecified,
                page_size: Some(2),
            },
        )
        .await
        .unwrap();
        assert_eq!(tables.len(), 2);

        assert_eq!(
            tables.get(&table2.table_id.into()).unwrap().tabular_ident(),
            &table2.table_ident
        );

        let tables = list_tabulars(
            warehouse_id,
            Some(namespace_id),
            TabularListFlags {
                include_staged: true,
                ..TabularListFlags::active()
            },
            &state.read_pool(),
            None,
            PaginationQuery {
                page_token: PageToken::Present(tables.next_token().unwrap().to_string()),
                page_size: Some(2),
            },
        )
        .await
        .unwrap();

        assert_eq!(tables.len(), 1);
        assert_eq!(
            tables.get(&table3.table_id.into()).unwrap().tabular_ident(),
            &table3.table_ident
        );

        let tables = list_tabulars(
            warehouse_id,
            Some(namespace_id),
            TabularListFlags {
                include_staged: true,
                ..TabularListFlags::active()
            },
            &state.read_pool(),
            None,
            PaginationQuery {
                page_token: PageToken::Present(tables.next_token().unwrap().to_string()),
                page_size: Some(2),
            },
        )
        .await
        .unwrap();
        assert_eq!(tables.len(), 0);
        assert!(tables.next_token().is_none());
    }

    #[sqlx::test]
    #[cfg_attr(test, tracing_test::traced_test)]
    async fn test_get_id_by_location(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let table = initialize_table(warehouse_id, state.clone(), false, None, None, None).await;

        let table_info = get_tabular_infos_by_ids(
            warehouse_id,
            &[table.table_id.into()],
            TabularListFlags::active(),
            &state.read_pool(),
        )
        .await
        .unwrap();

        assert_eq!(table_info.len(), 1);
        let table_info = &table_info[0];
        assert_eq!(table_info.tabular_id(), table.table_id.into());
        // Exact path works
        let table_info_by_location = get_tabular_infos_by_s3_location(
            warehouse_id,
            table_info.location(),
            TabularListFlags::active(),
            state.clone(),
        )
        .await
        .unwrap()
        .unwrap();
        pretty_assertions::assert_eq!(table_info, &table_info_by_location);

        let mut subpath = table_info.metadata_location().unwrap().clone();
        subpath.push("data/foo.parquet");
        // Subpath works
        let table_info_by_location = get_tabular_infos_by_s3_location(
            warehouse_id,
            &subpath,
            TabularListFlags::active(),
            state.clone(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(table_info, &table_info_by_location);

        // Metadata path works
        let table_info_by_location = get_tabular_infos_by_s3_location(
            warehouse_id,
            table_info.metadata_location().unwrap(),
            TabularListFlags::active(),
            state.clone(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(table_info, &table_info_by_location);

        // Path without trailing slash works
        let mut metadata_location = table_info.metadata_location().unwrap().clone();
        metadata_location.without_trailing_slash();
        let table_info_by_location = get_tabular_infos_by_s3_location(
            warehouse_id,
            &metadata_location,
            TabularListFlags::active(),
            state.clone(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(table_info, &table_info_by_location);

        metadata_location.with_trailing_slash();
        // Path with trailing slash works
        let table_info_by_location = get_tabular_infos_by_s3_location(
            warehouse_id,
            &metadata_location,
            TabularListFlags::active(),
            state.clone(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(table_info, &table_info_by_location);

        let table_location = table_info.location().to_string();
        let shorter = table_location.as_str()[0..table_location.len() - 2]
            .to_string()
            .parse()
            .unwrap();

        // Shorter path does not work
        let table_info_by_location = get_tabular_infos_by_s3_location(
            warehouse_id,
            &shorter,
            TabularListFlags::active(),
            state.clone(),
        )
        .await
        .unwrap();
        assert_eq!(table_info_by_location, None);
    }

    #[sqlx::test]
    async fn test_cannot_get_table_of_inactive_warehouse(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let table = initialize_table(warehouse_id, state.clone(), false, None, None, None).await;
        let mut transaction = pool.begin().await.expect("Failed to start transaction");
        set_warehouse_status(warehouse_id, WarehouseStatus::Inactive, &mut transaction)
            .await
            .expect("Failed to set warehouse status");
        transaction.commit().await.unwrap();

        let r = get_tabular_infos_by_ids(
            warehouse_id,
            &[table.table_id.into()],
            TabularListFlags::active(),
            &state.read_pool(),
        )
        .await
        .unwrap();
        assert!(r.is_empty());
    }

    #[sqlx::test]
    async fn test_drop_table(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let (project_id, warehouse_id) =
            initialize_warehouse(state.clone(), None, None, None, true).await;
        let table = initialize_table(warehouse_id, state.clone(), false, None, None, None).await;

        let mut transaction = pool.begin().await.unwrap();

        let _ = TabularExpirationTask::schedule_task::<PostgresBackend>(
            ScheduleTaskMetadata {
                project_id,
                parent_task_id: None,
                scheduled_for: Some(chrono::Utc::now() + chrono::Duration::seconds(1)),
                entity: TaskEntity::EntityInWarehouse {
                    entity_id: WarehouseTaskEntityId::Table {
                        table_id: table.table_id,
                    },
                    warehouse_id,
                    entity_name: table.table_ident.into_name_parts(),
                },
            },
            TabularExpirationPayload::new(DeleteKind::Purge),
            &mut transaction,
        )
        .await
        .unwrap();

        mark_tabular_as_deleted(
            warehouse_id,
            table.table_id.into(),
            false,
            None,
            &mut transaction,
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();

        assert_eq!(
            get_tabular_infos_by_ids(
                warehouse_id,
                &[table.table_id.into()],
                TabularListFlags::active(),
                &state.read_pool(),
            )
            .await
            .unwrap()
            .len(),
            0
        );

        let result = get_tabular_infos_by_ids(
            warehouse_id,
            &[table.table_id.into()],
            TabularListFlags {
                include_deleted: true,
                ..TabularListFlags::active()
            },
            &state.read_pool(),
        )
        .await
        .unwrap();
        assert_eq!(result.len(), 1);
        let result = &result[0];
        assert_eq!(result.tabular_id(), table.table_id.into());

        let mut transaction = pool.begin().await.unwrap();

        drop_tabular(
            warehouse_id,
            table.table_id.into(),
            false,
            None,
            &mut transaction,
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();

        assert_eq!(
            get_tabular_infos_by_ids(
                warehouse_id,
                &[table.table_id.into()],
                TabularListFlags {
                    include_deleted: true,
                    ..TabularListFlags::active()
                },
                &state.read_pool(),
            )
            .await
            .unwrap()
            .len(),
            0
        );
    }

    #[sqlx::test]
    async fn test_rename_to_different_case(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let table = initialize_table(
            warehouse_id,
            state.clone(),
            false,
            None,
            None,
            Some("my_table".to_string()),
        )
        .await;

        // Rename to a name that differs only in case
        let new_table_ident = TableIdent {
            namespace: table.namespace.clone(),
            name: "My_Table".to_string(),
        };

        let mut transaction = pool.begin().await.unwrap();
        rename_tabular(
            warehouse_id,
            table.table_id.into(),
            &table.table_ident,
            &new_table_ident,
            &mut transaction,
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();

        // Old name should still find it (case-insensitive)
        let infos = get_tabular_infos_by_idents(
            warehouse_id,
            &[TabularIdentBorrowed::Table(&table.table_ident)],
            TabularListFlags::active(),
            &state.read_pool(),
        )
        .await
        .unwrap();
        assert_eq!(infos.len(), 1);
        let info = infos
            .get(&table.table_ident)
            .expect("old ident should match");
        assert_eq!(info.tabular_id(), table.table_id.into());

        // New name should also find it
        let infos = get_tabular_infos_by_idents(
            warehouse_id,
            &[TabularIdentBorrowed::Table(&new_table_ident)],
            TabularListFlags::active(),
            &state.read_pool(),
        )
        .await
        .unwrap();
        assert_eq!(infos.len(), 1);
        let info = infos.get(&new_table_ident).expect("new ident should match");
        assert_eq!(info.tabular_id(), table.table_id.into());

        // The stored name should be the new case
        let listed = list_tabulars(
            warehouse_id,
            Some(table.namespace_id),
            TabularListFlags::active(),
            &state.read_pool(),
            None,
            PaginationQuery::empty(),
        )
        .await
        .unwrap();
        assert_eq!(listed.len(), 1);
        let (_, info, _) = listed.into_iter_with_page_tokens().next().unwrap();
        assert_eq!(info.tabular_ident().name, "My_Table");
    }

    #[sqlx::test]
    async fn test_list_tables_case_insensitive_namespace(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let namespace = NamespaceIdent::from_vec(vec!["My_Namespace".to_string()]).unwrap();
        let namespace_id = initialize_namespace(state.clone(), warehouse_id, &namespace, None)
            .await
            .namespace_id();

        let _ = initialize_table(
            warehouse_id,
            state.clone(),
            false,
            Some(namespace.clone()),
            None,
            Some("table_one".to_string()),
        )
        .await;
        let _ = initialize_table(
            warehouse_id,
            state.clone(),
            false,
            Some(namespace.clone()),
            None,
            Some("table_two".to_string()),
        )
        .await;

        // Look up both tables using uppercase namespace
        let upper_ident_1 = TableIdent {
            namespace: NamespaceIdent::from_vec(vec!["MY_NAMESPACE".to_string()]).unwrap(),
            name: "TABLE_ONE".to_string(),
        };
        let upper_ident_2 = TableIdent {
            namespace: NamespaceIdent::from_vec(vec!["MY_NAMESPACE".to_string()]).unwrap(),
            name: "TABLE_TWO".to_string(),
        };

        let infos = get_tabular_infos_by_idents(
            warehouse_id,
            &[
                TabularIdentBorrowed::Table(&upper_ident_1),
                TabularIdentBorrowed::Table(&upper_ident_2),
            ],
            TabularListFlags::active(),
            &state.read_pool(),
        )
        .await
        .unwrap();
        assert_eq!(infos.len(), 2);

        // Listing by namespace_id should return original case
        let listed = list_tabulars(
            warehouse_id,
            Some(namespace_id),
            TabularListFlags::active(),
            &state.read_pool(),
            None,
            PaginationQuery::empty(),
        )
        .await
        .unwrap();
        assert_eq!(listed.len(), 2);
        let names: std::collections::HashSet<String> = listed
            .iter()
            .map(|(_, info)| info.tabular_ident().name.clone())
            .collect();
        assert!(names.contains("table_one"));
        assert!(names.contains("table_two"));
    }

    // ── helpers ─────────────────────────────────────────────────────────────

    /// Two required primitive columns; field 1 is the identifier.
    fn two_col() -> Schema {
        Schema::builder()
            .with_schema_id(0)
            .with_identifier_field_ids(vec![1])
            .with_fields(vec![
                NestedField::required(1, "id", iceberg::spec::Type::Primitive(PrimitiveType::Int))
                    .into(),
                NestedField::required(
                    2,
                    "name",
                    iceberg::spec::Type::Primitive(PrimitiveType::String),
                )
                .into(),
            ])
            .build()
            .unwrap()
    }

    /// Rich schema exercising struct, list, map, decimal, uuid, identifier
    /// field, and a primitive column with an `initial_default`.
    fn nested_corpus_schema() -> Schema {
        use iceberg::spec::{ListType, MapType, StructType, Type};

        let address_struct = Type::Struct(StructType::new(vec![
            NestedField::required(3, "street", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(4, "city", Type::Primitive(PrimitiveType::String)).into(),
        ]));

        let tag_list = Type::List(ListType::new(
            NestedField::list_element(6, Type::Primitive(PrimitiveType::String), true).into(),
        ));

        let props_map = Type::Map(MapType::new(
            NestedField::map_key_element(8, Type::Primitive(PrimitiveType::String)).into(),
            NestedField::map_value_element(9, Type::Primitive(PrimitiveType::Long), false).into(),
        ));

        let count_with_default =
            NestedField::required(10, "count", Type::Primitive(PrimitiveType::Int))
                .with_initial_default(iceberg::spec::Literal::Primitive(
                    iceberg::spec::PrimitiveLiteral::Int(7),
                ));

        Schema::builder()
            .with_schema_id(0)
            .with_identifier_field_ids(vec![1])
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::required(2, "address", address_struct).into(),
                NestedField::required(5, "tags", tag_list).into(),
                NestedField::required(7, "props", props_map).into(),
                NestedField::required(
                    11,
                    "amount",
                    Type::Primitive(PrimitiveType::Decimal {
                        precision: 10,
                        scale: 2,
                    }),
                )
                .into(),
                NestedField::required(12, "uid", Type::Primitive(PrimitiveType::Uuid)).into(),
                Arc::new(count_with_default),
            ])
            .build()
            .unwrap()
    }

    /// Count rows in `table` scoped to one warehouse + table pair.
    async fn row_count(
        pool: &sqlx::PgPool,
        table: &str,
        wh: WarehouseId,
        table_id: TableId,
    ) -> i64 {
        let q = format!("SELECT count(*) FROM {table} WHERE warehouse_id=$1 AND tabular_id=$2");
        sqlx::query_scalar(sqlx::AssertSqlSafe(q))
            .bind(*wh)
            .bind(*table_id)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    // ── A. Load assembly ─────────────────────────────────────────────────────

    #[sqlx::test]
    async fn load_returns_assembled_nested_schema(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, wh) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let (table_id, persisted) =
            create_table_with_schema(state.clone(), wh, nested_corpus_schema()).await;

        let mut txn = pool.begin().await.unwrap();
        let mut loaded = load_tables(
            wh,
            vec![table_id],
            false,
            &LoadTableFilters::default(),
            &mut txn,
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();

        let md = loaded.pop().unwrap().table_metadata;
        assert_eq!(md.current_schema().as_ref(), &persisted);
    }

    #[sqlx::test]
    async fn load_returns_all_schema_versions(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, wh) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let (table_id, s0) = create_table_with_schema(state.clone(), wh, two_col()).await;

        // Build s1 from the PERSISTED s0 so field ids line up.
        let mut fields: Vec<iceberg::spec::NestedFieldRef> = s0.as_struct().fields().to_vec();
        let next_id = fields.iter().map(|f| f.id).max().unwrap() + 1;
        fields.push(
            NestedField::optional(
                next_id,
                "age",
                iceberg::spec::Type::Primitive(PrimitiveType::Int),
            )
            .into(),
        );
        let s1: std::sync::Arc<Schema> = Arc::new(
            Schema::builder()
                .with_schema_id(s0.schema_id() + 1)
                .with_identifier_field_ids(s0.identifier_field_ids().collect::<Vec<_>>())
                .with_fields(fields)
                .build()
                .unwrap(),
        );

        let mut txn = pool.begin().await.unwrap();
        crate::tabular::table::common::insert_schemas(std::iter::once(&s1), &mut txn, wh, table_id)
            .await
            .unwrap();
        txn.commit().await.unwrap();

        let mut txn = pool.begin().await.unwrap();
        let mut loaded = load_tables(
            wh,
            vec![table_id],
            false,
            &LoadTableFilters::default(),
            &mut txn,
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();

        let md = loaded.pop().unwrap().table_metadata;
        assert_eq!(md.schemas_iter().count(), 2);
        assert!(md.schema_by_id(s0.schema_id()).is_some());
        assert!(md.schema_by_id(s1.schema_id()).is_some());
        assert_eq!(
            md.schema_by_id(s1.schema_id()).unwrap().as_ref(),
            s1.as_ref()
        );
    }

    #[sqlx::test]
    async fn empty_non_current_schema_reloads_present(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, wh) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let (table_id, s0) = create_table_with_schema(state.clone(), wh, two_col()).await;

        // A legitimately-empty, retained non-current schema: anchor row, zero field rows.
        let empty: Arc<Schema> = Arc::new(
            Schema::builder()
                .with_schema_id(s0.schema_id() + 1)
                .build()
                .unwrap(),
        );
        let mut txn = pool.begin().await.unwrap();
        crate::tabular::table::common::insert_schemas(
            std::iter::once(&empty),
            &mut txn,
            wh,
            table_id,
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();

        let mut txn = pool.begin().await.unwrap();
        let mut loaded = load_tables(
            wh,
            vec![table_id],
            false,
            &LoadTableFilters::default(),
            &mut txn,
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();

        let md = loaded.pop().unwrap().table_metadata;
        assert_eq!(md.schemas_iter().count(), 2);
        let empty_loaded = md
            .schema_by_id(s0.schema_id() + 1)
            .expect("empty non-current schema must reload, not vanish");
        assert!(empty_loaded.as_struct().fields().is_empty());
    }

    #[sqlx::test]
    async fn empty_current_schema_fails_loud(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, wh) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let (table_id, s0) = create_table_with_schema(state.clone(), wh, two_col()).await;

        // Simulate lost schema_field rows for the CURRENT schema (its anchor row remains).
        sqlx::query("DELETE FROM schema_field WHERE warehouse_id = $1 AND tabular_id = $2 AND schema_id = $3")
            .bind(*wh)
            .bind(*table_id)
            .bind(s0.schema_id())
            .execute(&pool)
            .await
            .unwrap();

        let mut txn = pool.begin().await.unwrap();
        let result = load_tables(
            wh,
            vec![table_id],
            false,
            &LoadTableFilters::default(),
            &mut txn,
        )
        .await;

        let err = result.expect_err("current schema with no field rows must fail loud");
        assert!(
            format!("{err:?}").contains("has no fields"),
            "expected a 'current schema has no fields' error, got: {err:?}"
        );
    }

    // ── B. tabular_field refcount GC ───────────────────────────────────────

    #[sqlx::test]
    async fn gc_reaps_identity_when_last_schema_removed(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, wh) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let (table_id, s0) = create_table_with_schema(state.clone(), wh, two_col()).await;

        assert_eq!(row_count(&pool, "tabular_field", wh, table_id).await, 2);
        assert_eq!(row_count(&pool, "schema_field", wh, table_id).await, 2);

        let mut txn = pool.begin().await.unwrap();
        crate::tabular::table::common::remove_schemas(wh, table_id, vec![s0.schema_id()], &mut txn)
            .await
            .unwrap();
        txn.commit().await.unwrap();

        assert_eq!(row_count(&pool, "tabular_field", wh, table_id).await, 0);
        assert_eq!(row_count(&pool, "schema_field", wh, table_id).await, 0);
    }

    #[sqlx::test]
    async fn gc_keeps_identity_referenced_by_another_schema(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, wh) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let (table_id, s0) = create_table_with_schema(state.clone(), wh, two_col()).await;

        // Build s1 = s0's columns + one extra.
        let mut fields: Vec<iceberg::spec::NestedFieldRef> = s0.as_struct().fields().to_vec();
        let next_id = fields.iter().map(|f| f.id).max().unwrap() + 1;
        fields.push(
            NestedField::optional(
                next_id,
                "age",
                iceberg::spec::Type::Primitive(PrimitiveType::Int),
            )
            .into(),
        );
        let s1: std::sync::Arc<Schema> = Arc::new(
            Schema::builder()
                .with_schema_id(s0.schema_id() + 1)
                .with_identifier_field_ids(s0.identifier_field_ids().collect::<Vec<_>>())
                .with_fields(fields)
                .build()
                .unwrap(),
        );

        let mut txn = pool.begin().await.unwrap();
        crate::tabular::table::common::insert_schemas(std::iter::once(&s1), &mut txn, wh, table_id)
            .await
            .unwrap();
        txn.commit().await.unwrap();

        // id, name, age → 3 identities after both schemas exist.
        assert_eq!(row_count(&pool, "tabular_field", wh, table_id).await, 3);

        // Remove s0 — s1 still references id + name, so identities stay at 3.
        let mut txn = pool.begin().await.unwrap();
        crate::tabular::table::common::remove_schemas(wh, table_id, vec![s0.schema_id()], &mut txn)
            .await
            .unwrap();
        txn.commit().await.unwrap();
        assert_eq!(row_count(&pool, "tabular_field", wh, table_id).await, 3);

        // Remove s1 — last schema gone, all identities reaped.
        let mut txn = pool.begin().await.unwrap();
        crate::tabular::table::common::remove_schemas(wh, table_id, vec![s1.schema_id()], &mut txn)
            .await
            .unwrap();
        txn.commit().await.unwrap();
        assert_eq!(row_count(&pool, "tabular_field", wh, table_id).await, 0);
    }

    #[sqlx::test]
    async fn gc_whole_table_drop_cascades(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, wh) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let (table_id, _) = create_table_with_schema(state.clone(), wh, two_col()).await;

        assert_eq!(row_count(&pool, "tabular_field", wh, table_id).await, 2);

        sqlx::query(r#"DELETE FROM tabular WHERE warehouse_id=$1 AND tabular_id=$2"#)
            .bind(*wh)
            .bind(*table_id)
            .execute(&pool)
            .await
            .unwrap();

        assert_eq!(row_count(&pool, "schema_field", wh, table_id).await, 0);
        assert_eq!(row_count(&pool, "tabular_field", wh, table_id).await, 0);
    }

    /// Create-side invariant: the schema_field -> tabular_field FK rejects a field row with no
    /// identity anchor.
    #[sqlx::test]
    async fn schema_field_requires_tabular_field_anchor(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, wh) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let (table_id, _) = create_table_with_schema(state.clone(), wh, two_col()).await;

        // field_id 9999 has no tabular_field anchor; (warehouse_id, tabular_id) is valid and
        // (schema_id 0, field_id 9999) is free, so only the tabular_field FK can fail here.
        let err = sqlx::query(
            r#"INSERT INTO schema_field
                (warehouse_id, tabular_id, schema_id, field_id, ordinal, name, required,
                 type_kind, is_identifier)
               VALUES ($1, $2, 0, 9999, 0, 'ghost', false, 'int'::iceberg_type_kind, false)"#,
        )
        .bind(*wh)
        .bind(*table_id)
        .execute(&pool)
        .await
        .expect_err("schema_field insert with no tabular_field anchor must fail");

        let db_err = err.as_database_error().expect("must be a database error");
        assert_eq!(
            db_err.code().as_deref(),
            Some("23503"),
            "expected FK violation (23503 foreign_key_violation), got: {err:?}"
        );
    }

    // ── B2. Storage benchmark (ignored) ──────────────────────────────────────

    /// Rough old-vs-new gate: one JSONB blob per schema vs normalized schema_field rows, for a wide
    /// table with many overlapping schema versions. Run with:
    ///   cargo test -p lakekeeper-storage-postgres --all-features \
    ///     bench_old_jsonb_vs_new_normalized -- --ignored --nocapture
    #[sqlx::test]
    #[ignore = "benchmark: run explicitly with --ignored --nocapture"]
    async fn bench_old_jsonb_vs_new_normalized(pool: sqlx::PgPool) {
        use std::time::Instant;

        use iceberg::spec::{NestedField, NestedFieldRef, PrimitiveType, Schema, Type};
        use sqlx::Row;

        const NCOLS: i32 = 500;
        const NVERS: i32 = 20;
        const BASE: i32 = 1000;

        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, wh) = initialize_warehouse(state.clone(), None, None, None, true).await;

        // NVERS versions, each NCOLS columns, sharing field_ids 1..=NCOLS (overlapping — the worst
        // case for row count: NCOLS*NVERS schema_field rows against NCOLS tabular_field rows).
        let schemas: Vec<std::sync::Arc<Schema>> = (0..NVERS)
            .map(|v| {
                let fields: Vec<NestedFieldRef> = (1..=NCOLS)
                    .map(|i| {
                        NestedField::optional(
                            i,
                            format!("col_{i}"),
                            Type::Primitive(PrimitiveType::Long),
                        )
                        .into()
                    })
                    .collect();
                std::sync::Arc::new(
                    Schema::builder()
                        .with_schema_id(BASE + v)
                        .with_fields(fields)
                        .build()
                        .unwrap(),
                )
            })
            .collect();
        let expected: Vec<i32> = (0..NVERS).map(|v| BASE + v).collect();

        // ---- NEW: normalized schema_field (real write path) ----
        let (tbl_new, _) = create_table_with_schema(state.clone(), wh, two_col()).await;
        let t = Instant::now();
        let mut txn = pool.begin().await.unwrap();
        crate::tabular::table::common::insert_schemas(schemas.iter(), &mut txn, wh, tbl_new)
            .await
            .unwrap();
        txn.commit().await.unwrap();
        let new_write = t.elapsed();

        let new_rows: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM schema_field WHERE warehouse_id=$1 AND tabular_id=$2 AND schema_id>=$3",
        )
        .bind(*wh)
        .bind(*tbl_new)
        .bind(BASE)
        .fetch_one(&pool)
        .await
        .unwrap();

        let med = |mut v: Vec<std::time::Duration>| {
            v.sort();
            v[v.len() / 2]
        };
        let (mut new_fetch, mut new_decode_name, mut new_decode_pos, mut new_assemble) =
            (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        for _ in 0..5 {
            let q = r#"SELECT schema_id, field_id, parent_field_id, ordinal, name, required, doc,
                          type_kind::text AS type_kind, type_params, initial_default, write_default,
                          is_identifier
                   FROM schema_field WHERE warehouse_id=$1 AND tabular_id=$2 AND schema_id>=$3
                   ORDER BY schema_id, parent_field_id, ordinal"#;
            let t = Instant::now();
            let rows = sqlx::query(q)
                .bind(*wh)
                .bind(*tbl_new)
                .bind(BASE)
                .fetch_all(&pool)
                .await
                .unwrap();
            new_fetch.push(t.elapsed());

            // decode by column name (what this bench first used)
            let t = Instant::now();
            let by_name: Vec<normalized_schema::SchemaFieldRow> = rows
                .iter()
                .map(|r| normalized_schema::SchemaFieldRow {
                    schema_id: r.get("schema_id"),
                    field_id: r.get("field_id"),
                    parent_field_id: r.get("parent_field_id"),
                    ordinal: r.get("ordinal"),
                    name: r.get("name"),
                    required: r.get("required"),
                    doc: r.get("doc"),
                    type_kind: r.get("type_kind"),
                    type_params: r.get("type_params"),
                    initial_default: r.get("initial_default"),
                    write_default: r.get("write_default"),
                    is_identifier: r.get("is_identifier"),
                })
                .collect();
            new_decode_name.push(t.elapsed());

            // decode by column index (what the production `query!` path effectively does)
            let t = Instant::now();
            let by_pos: Vec<normalized_schema::SchemaFieldRow> = rows
                .iter()
                .map(|r| normalized_schema::SchemaFieldRow {
                    schema_id: r.get(0),
                    field_id: r.get(1),
                    parent_field_id: r.get(2),
                    ordinal: r.get(3),
                    name: r.get(4),
                    required: r.get(5),
                    doc: r.get(6),
                    type_kind: r.get(7),
                    type_params: r.get(8),
                    initial_default: r.get(9),
                    write_default: r.get(10),
                    is_identifier: r.get(11),
                })
                .collect();
            new_decode_pos.push(t.elapsed());
            assert_eq!(by_pos.len(), new_rows as usize);

            let t = Instant::now();
            let assembled = normalized_schema::assemble_schemas(by_name, &expected).unwrap();
            assert_eq!(assembled.len(), NVERS as usize);
            new_assemble.push(t.elapsed());
        }
        let (new_fetch, new_decode_name, new_decode_pos, new_assemble) = (
            med(new_fetch),
            med(new_decode_name),
            med(new_decode_pos),
            med(new_assemble),
        );
        // production-equivalent read = fetch + by-index decode + assemble
        let new_read = new_fetch + new_decode_pos + new_assemble;

        // ---- OLD: one JSONB blob per schema ----
        let (tbl_old, _) = create_table_with_schema(state.clone(), wh, two_col()).await;
        let blobs: Vec<serde_json::Value> = schemas
            .iter()
            .map(|s| serde_json::to_value(s).unwrap())
            .collect();
        let old_bytes: usize = blobs
            .iter()
            .map(|b| serde_json::to_vec(b).unwrap().len())
            .sum();
        let tblids = vec![*tbl_old; NVERS as usize];

        let t = Instant::now();
        let mut txn = pool.begin().await.unwrap();
        sqlx::query(
            r#"INSERT INTO table_schema(schema_id, table_id, warehouse_id, schema)
               SELECT sid, tid, $3, s FROM UNNEST($1::int[], $2::uuid[], $4::jsonb[]) u(sid, tid, s)"#,
        )
        .bind(&expected)
        .bind(&tblids)
        .bind(*wh)
        .bind(&blobs)
        .execute(&mut *txn)
        .await
        .unwrap();
        txn.commit().await.unwrap();
        let old_write = t.elapsed();

        let (mut old_fetch, mut old_deser) = (Vec::new(), Vec::new());
        for _ in 0..5 {
            let t = Instant::now();
            let got: Vec<serde_json::Value> = sqlx::query_scalar(
                "SELECT schema FROM table_schema WHERE warehouse_id=$1 AND table_id=$2 AND schema_id>=$3 AND schema IS NOT NULL",
            )
            .bind(*wh)
            .bind(*tbl_old)
            .bind(BASE)
            .fetch_all(&pool)
            .await
            .unwrap();
            old_fetch.push(t.elapsed());
            // from_value consumes the Value (no clone) — the fair deserialize cost.
            let t = Instant::now();
            let parsed: Vec<Schema> = got
                .into_iter()
                .map(|b| serde_json::from_value(b).unwrap())
                .collect();
            assert_eq!(parsed.len(), NVERS as usize);
            old_deser.push(t.elapsed());
        }
        let (old_fetch, old_deser) = (med(old_fetch), med(old_deser));
        let old_read = old_fetch + old_deser;

        println!(
            "\n=== read breakdown: {NCOLS} cols x {NVERS} versions (overlapping), medians of 5 ==="
        );
        println!("NEW normalized  ({new_rows} rows)");
        println!("  fetch (query round-trip):        {new_fetch:?}");
        println!("  row->struct decode by-NAME:      {new_decode_name:?}");
        println!("  row->struct decode by-INDEX:     {new_decode_pos:?}  (prod query! path)");
        println!("  assemble_schemas:                {new_assemble:?}");
        println!("  read total (fetch+by-index+asm): {new_read:?}");
        println!("OLD JSONB blob  (~{} KiB)", old_bytes / 1024);
        println!("  fetch (query round-trip):        {old_fetch:?}");
        println!("  from_value deserialize:          {old_deser:?}");
        println!("  read total:                      {old_read:?}");
        println!("write:  new {new_write:?}  vs old {old_write:?}");
        println!(
            "read ratio new/old = {:.1}x",
            new_read.as_secs_f64() / old_read.as_secs_f64()
        );
    }

    // ── C. Backfill + freeze ─────────────────────────────────────────────────

    #[sqlx::test]
    async fn backfill_reproduces_schema_from_jsonb(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, wh) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let (table_id, persisted) =
            create_table_with_schema(state.clone(), wh, nested_corpus_schema()).await;

        let jsonb = serde_json::to_value(&persisted).unwrap();
        let mut txn = pool.begin().await.unwrap();
        sqlx::query("DELETE FROM schema_field WHERE warehouse_id=$1 AND tabular_id=$2")
            .bind(*wh)
            .bind(*table_id)
            .execute(&mut *txn)
            .await
            .unwrap();
        sqlx::query("UPDATE table_schema SET schema=$3 WHERE warehouse_id=$1 AND table_id=$2")
            .bind(*wh)
            .bind(*table_id)
            .bind(&jsonb)
            .execute(&mut *txn)
            .await
            .unwrap();
        crate::migrations::normalize_schema::backfill(&mut txn)
            .await
            .unwrap();
        txn.commit().await.unwrap();

        let mut txn = pool.begin().await.unwrap();
        let md = load_tables(
            wh,
            vec![table_id],
            false,
            &LoadTableFilters::default(),
            &mut txn,
        )
        .await
        .unwrap()
        .pop()
        .unwrap()
        .table_metadata;
        txn.commit().await.unwrap();

        assert_eq!(md.current_schema().as_ref(), &persisted);
    }

    #[sqlx::test]
    async fn freeze_blocks_jsonb_but_allows_null_anchor(pool: sqlx::PgPool) {
        use crate::migrations::MigrationHook;

        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, wh) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let (table_id, _s) = create_table_with_schema(state.clone(), wh, two_col()).await;

        // Install the freeze via the real hook (also drops NOT NULL, after a no-op backfill).
        let mut txn = pool.begin().await.unwrap();
        crate::migrations::normalize_schema::NormalizeSchemaHook
            .apply(&mut txn)
            .await
            .unwrap();
        txn.commit().await.unwrap();

        // The new write path (schema = NULL anchor) is permitted under the freeze.
        sqlx::query(
            "INSERT INTO table_schema(warehouse_id, table_id, schema_id) VALUES ($1,$2,$3)",
        )
        .bind(*wh)
        .bind(*table_id)
        .bind(998_i32)
        .execute(&pool)
        .await
        .expect("NULL-schema anchor insert must be allowed under the freeze");

        // A legacy JSONB schema write is rejected with SQLSTATE object_not_in_prerequisite_state.
        let err = sqlx::query(
            "INSERT INTO table_schema(warehouse_id, table_id, schema_id, schema) \
             VALUES ($1,$2,$3,$4)",
        )
        .bind(*wh)
        .bind(*table_id)
        .bind(999_i32)
        .bind(serde_json::json!({"type":"struct","schema-id":999,"fields":[]}))
        .execute(&pool)
        .await
        .unwrap_err();

        assert_eq!(
            err.as_database_error().and_then(|e| e.code()).as_deref(),
            Some("55000")
        );
    }
}
