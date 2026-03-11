mod commit;
mod common;
mod create;

use std::{collections::HashMap, default::Default, ops::Deref, str::FromStr, sync::Arc};

pub(crate) use commit::commit_table_transaction;
pub(crate) use create::create_table;
use iceberg::{
    TableUpdate,
    spec::{
        BlobMetadata, EncryptedKey, FormatVersion, MAIN_BRANCH, PartitionSpec, Schema, SchemaId,
        SnapshotRetention, SortOrder, Summary,
    },
};
use iceberg_ext::spec::TableMetadata;
use sqlx::types::Json;
use uuid::Uuid;

use crate::{
    WarehouseId,
    api::iceberg::v1::tables::{LoadTableFilters, SnapshotsQuery},
    service::{
        ConversionError, InternalParseLocationError, InternalTableMetadataBuildFailed,
        LoadTableError, LoadTableResponse, RequiredTableComponentMissing, TableId,
        storage::join_location,
    },
};

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
    schemas: Option<Vec<Json<Schema>>>,
    schema_ids: Option<Vec<i32>>,
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
    fn into_table_metadata(self) -> Result<TableMetadata, LoadTableError> {
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

        let schemas = expect(self.schemas, "Schemas", &info)?
            .into_iter()
            .map(|s| (s.0.schema_id(), Arc::new(s.0)))
            .collect::<HashMap<SchemaId, _>>();

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
            ts.schema_ids,
            tcs.schema_id as "current_schema",
            tdps.partition_spec_id as "default_partition_spec_id",
            ts.schemas as "schemas: Vec<Json<Schema>>",
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
                          ARRAY_AGG(schema_id) as schema_ids,
                          ARRAY_AGG(schema) as schemas
                   FROM table_schema WHERE warehouse_id = $1 AND table_id = ANY($2)
                   GROUP BY table_id) ts ON ts.table_id = t.table_id
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
            let table_metadata = table.into_table_metadata()?;

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

#[cfg(test)]
pub(crate) mod tests {
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
    use lakekeeper_io::Location;
    use uuid::Uuid;

    use super::*;
    use crate::{
        api::{
            iceberg::{
                types::PageToken,
                v1::{PaginationQuery, tables::LoadTableFilters},
            },
            management::v1::{DeleteKind, warehouse::WarehouseStatus},
        },
        implementations::{
            CatalogState,
            postgres::{
                PostgresBackend,
                namespace::tests::initialize_namespace,
                tabular::{
                    drop_tabular, get_tabular_infos_by_idents, get_tabular_infos_by_ids,
                    get_tabular_infos_by_s3_location, list_tabulars, mark_tabular_as_deleted,
                    rename_tabular, table::create::create_table,
                },
                warehouse::{set_warehouse_status, test::initialize_warehouse},
            },
        },
        server::tables::create_table::create_table_request_into_table_metadata,
        service::{
            CreateTableError, NamedEntity, NamespaceId, RenameTabularError, TableCreation,
            TabularIdentBorrowed, TabularListFlags, ViewOrTableInfo,
            tasks::{
                ScheduleTaskMetadata, TaskEntity, WarehouseTaskEntityId,
                tabular_expiration_queue::{TabularExpirationPayload, TabularExpirationTask},
            },
        },
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

    pub(crate) struct InitializedTable {
        #[allow(dead_code)]
        pub(crate) namespace_id: NamespaceId,
        pub(crate) namespace: NamespaceIdent,
        pub(crate) table_id: TableId,
        pub(crate) table_ident: TableIdent,
    }

    /// Creates a table in the given warehouse.
    ///
    /// Parameters:
    ///
    /// * `namespace`: If provided creates the table in that namespace, otherwise creates new one.
    /// * `table_id`: If provided uses this as the table's id, otherwise creates a random id.
    pub(crate) async fn initialize_table(
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

        let table_metadata = create_table_request_into_table_metadata(table_id, request).unwrap();
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

        let table_metadata = create_table_request_into_table_metadata(table_id, request).unwrap();

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
        let table_metadata =
            create_table_request_into_table_metadata(staged_table_id, request).unwrap();

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

        let table_metadata =
            create_table_request_into_table_metadata(staged_table_id, request).unwrap();

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
        assert_eq!(load_result.table_metadata, table_metadata, "{diff}",);
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
        .unwrap();
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
        .unwrap();
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
        .unwrap();
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
        .unwrap();
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
        .unwrap();
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
        .unwrap();
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
        .unwrap();
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
        .unwrap();
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
        .unwrap();
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
        // Both queries should resolve to the same table, but we should get 2 results
        // (one for each queried identifier)
        assert_eq!(infos.len(), 2);
        let id_lower = infos[0].tabular_id();
        let id_upper = infos[1].tabular_id();
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
        .unwrap();
        assert_eq!(infos.len(), 0);

        let exists = get_tabular_infos_by_idents(
            warehouse_id,
            &[TabularIdentBorrowed::Table(&new_table_ident)],
            TabularListFlags::active(),
            &state.read_pool(),
        )
        .await
        .unwrap();
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
        .unwrap();
        assert_eq!(infos.len(), 0);

        let infos = get_tabular_infos_by_idents(
            warehouse_id,
            &[TabularIdentBorrowed::Table(&new_table_ident)],
            TabularListFlags::active(),
            &state.read_pool(),
        )
        .await
        .unwrap();
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
    #[tracing_test::traced_test]
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
            TabularExpirationPayload {
                deletion_kind: DeleteKind::Purge,
            },
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
}
