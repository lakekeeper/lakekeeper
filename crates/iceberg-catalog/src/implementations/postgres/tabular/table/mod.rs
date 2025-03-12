mod commit;
mod common;
mod create;

use std::{
    collections::{HashMap, HashSet},
    default::Default,
    ops::Deref,
    str::FromStr,
    sync::Arc,
};

pub(crate) use commit::commit_table_transaction;
pub(crate) use create::create_table;
use http::StatusCode;
use iceberg::{
    spec::{
        BlobMetadata, FormatVersion, PartitionSpec, Parts, Schema, SchemaId, SnapshotRetention,
        SortOrder, Summary, MAIN_BRANCH,
    },
    TableUpdate,
};
use iceberg_ext::{configs::Location, spec::TableMetadata, NamespaceIdent};
use sqlx::types::Json;
use uuid::Uuid;

use super::get_partial_fs_locations;
use crate::{
    api::iceberg::v1::{PaginatedMapping, PaginationQuery},
    implementations::postgres::{
        dbutils::DBErrorHandler as _,
        tabular::{
            drop_tabular, list_tabulars, try_parse_namespace_ident, TabularIdentBorrowed,
            TabularIdentOwned, TabularIdentUuid, TabularType,
        },
        CatalogState,
    },
    service::{
        storage::{join_location, split_location, StorageProfile},
        ErrorModel, GetTableMetadataResponse, LoadTableResponse, Result, TableIdent,
        TableIdentUuid, TabularDetails,
    },
    SecretIdent, WarehouseIdent,
};

const MAX_PARAMETERS: usize = 30000;

pub(crate) async fn resolve_table_ident<'e, 'c: 'e, E>(
    warehouse_id: WarehouseIdent,
    table: &TableIdent,
    list_flags: crate::service::ListFlags,
    catalog_state: E,
) -> Result<Option<TabularDetails>>
where
    E: 'e + sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    crate::implementations::postgres::tabular::tabular_ident_to_id(
        warehouse_id,
        &TabularIdentBorrowed::Table(table),
        list_flags,
        catalog_state,
    )
    .await?
    .map(|(id, location)| match id {
        TabularIdentUuid::Table(tab) => Ok(TabularDetails {
            ident: tab.into(),
            location,
        }),
        TabularIdentUuid::View(_) => Err(ErrorModel::builder()
            .code(StatusCode::INTERNAL_SERVER_ERROR.into())
            .message("DB returned a view when filtering for tables.".to_string())
            .r#type("InternalDatabaseError".to_string())
            .build()
            .into()),
    })
    .transpose()
}

pub(crate) async fn table_idents_to_ids<'e, 'c: 'e, E>(
    warehouse_id: WarehouseIdent,
    tables: HashSet<&TableIdent>,
    list_flags: crate::service::ListFlags,
    catalog_state: E,
) -> Result<HashMap<TableIdent, Option<TableIdentUuid>>>
where
    E: 'e + sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let table_map = crate::implementations::postgres::tabular::tabular_idents_to_ids(
        warehouse_id,
        tables
            .into_iter()
            .map(TabularIdentBorrowed::Table)
            .collect(),
        list_flags,
        catalog_state,
    )
    .await?
    .into_iter()
    .map(|(k, v)| match k {
        TabularIdentOwned::Table(t) => Ok((t, v.map(|v| TableIdentUuid::from(*v)))),
        TabularIdentOwned::View(_) => Err(ErrorModel::internal(
            "DB returned a view when filtering for tables.",
            "InternalDatabaseError",
            None,
        )
        .into()),
    })
    .collect::<Result<HashMap<_, Option<TableIdentUuid>>>>()?;

    Ok(table_map)
}

#[derive(Debug, sqlx::Type)]
#[sqlx(type_name = "table_format_version", rename_all = "kebab-case")]
pub enum DbTableFormatVersion {
    #[sqlx(rename = "1")]
    V1,
    #[sqlx(rename = "2")]
    V2,
}

impl From<DbTableFormatVersion> for FormatVersion {
    fn from(v: DbTableFormatVersion) -> Self {
        match v {
            DbTableFormatVersion::V1 => FormatVersion::V1,
            DbTableFormatVersion::V2 => FormatVersion::V2,
        }
    }
}

pub(crate) async fn load_tables_old(
    warehouse_id: WarehouseIdent,
    tables: impl IntoIterator<Item = TableIdentUuid>,
    include_deleted: bool,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<HashMap<TableIdentUuid, LoadTableResponse>> {
    let tables = sqlx::query!(
        r#"
        SELECT
            t."table_id",
            ti."namespace_id",
            t."metadata" as "metadata: Json<TableMetadata>",
            ti."metadata_location",
            w.storage_profile as "storage_profile: Json<StorageProfile>",
            w."storage_secret_id"
        FROM "table" t
        INNER JOIN tabular ti ON t.table_id = ti.tabular_id
        INNER JOIN namespace n ON ti.namespace_id = n.namespace_id
        INNER JOIN warehouse w ON n.warehouse_id = w.warehouse_id
        WHERE w.warehouse_id = $1
        AND w.status = 'active'
        AND (ti.deleted_at IS NULL OR $3)
        AND t."table_id" = ANY($2)
        "#,
        *warehouse_id,
        &tables.into_iter().map(Into::into).collect::<Vec<_>>(),
        include_deleted
    )
    .fetch_all(&mut **transaction)
    .await
    .map_err(|e| e.into_error_model("Error fetching table".to_string()))?;

    tables
        .into_iter()
        .map(|table| {
            let table_id = table.table_id.into();
            let metadata_location = table
                .metadata_location
                .as_deref()
                .map(FromStr::from_str)
                .transpose()
                .map_err(|e| {
                    ErrorModel::internal(
                        "Error parsing metadata location",
                        "InternalMetadataLocationParseError",
                        Some(Box::new(e)),
                    )
                })?;

            Ok((
                table_id,
                LoadTableResponse {
                    table_id,
                    namespace_id: table.namespace_id.into(),
                    table_metadata: table
                        .metadata
                        .ok_or(ErrorModel::internal(
                            "Table metadata jsonb not found",
                            "InternalTableMetadataNotFound",
                            None,
                        ))?
                        .0,
                    metadata_location,
                    storage_secret_ident: table.storage_secret_id.map(SecretIdent::from),
                    storage_profile: table.storage_profile.deref().clone(),
                },
            ))
        })
        .collect::<Result<HashMap<_, _>>>()
}

pub(crate) async fn list_tables<'e, 'c: 'e, E>(
    warehouse_id: WarehouseIdent,
    namespace: &NamespaceIdent,
    list_flags: crate::service::ListFlags,
    transaction: E,
    pagination_query: PaginationQuery,
) -> Result<PaginatedMapping<TableIdentUuid, TableIdent>>
where
    E: 'e + sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let tabulars = list_tabulars(
        warehouse_id,
        Some(namespace),
        None,
        list_flags,
        transaction,
        Some(TabularType::Table),
        pagination_query,
        false,
    )
    .await?;

    tabulars.map::<TableIdentUuid, TableIdent>(
        |k| match k {
            TabularIdentUuid::Table(t) => {
                let r: Result<TableIdentUuid> = Ok(TableIdentUuid::from(t));
                r
            }
            TabularIdentUuid::View(_) => Err(ErrorModel::internal(
                "DB returned a view when filtering for tables.",
                "InternalDatabaseError",
                None,
            )
            .into()),
        },
        |(v, _)| Ok(v.into_inner()),
    )
}

#[expect(dead_code)]
#[derive(sqlx::FromRow)]
struct TableQueryStruct {
    // TODO: clean up the options below once we've released this and can assume that migrations
    //       have happened
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
    metadata_location: Option<String>,
    table_fs_location: String,
    table_fs_protocol: String,
    storage_profile: Json<StorageProfile>,
    storage_secret_id: Option<Uuid>,
    table_properties_keys: Option<Vec<String>>,
    table_properties_values: Option<Vec<String>>,
    default_partition_spec_id: Option<i32>,
    partition_spec_ids: Option<Vec<i32>>,
    partition_specs: Option<Vec<Json<PartitionSpec>>>,
    current_schema: Option<i32>,
    schemas: Option<Vec<Json<Schema>>>,
    schema_ids: Option<Vec<i32>>,
    table_format_version: Option<DbTableFormatVersion>,
    last_sequence_number: Option<i64>,
    last_column_id: Option<i32>,
    last_updated_ms: Option<i64>,
    last_partition_id: Option<i32>,
    partition_stats_snapshot_ids: Option<Vec<i64>>,
    partition_stats_statistics_paths: Option<Vec<String>>,
    partition_stats_file_size_in_bytes: Option<Vec<i64>>,
    table_stats_snapshot_ids: Option<Vec<i64>>,
    table_stats_statistics_paths: Option<Vec<String>>,
    table_stats_file_size_in_bytes: Option<Vec<i64>>,
    table_stats_file_footer_size_in_bytes: Option<Vec<i64>>,
    table_stats_key_metadata: Option<Vec<Option<String>>>,
    table_stats_blob_metadata: Option<Vec<Json<Vec<BlobMetadata>>>>,
}

impl TableQueryStruct {
    #[expect(clippy::too_many_lines)]
    fn into_table_metadata(self) -> Result<Option<TableMetadata>> {
        macro_rules! expect {
            ($e:expr) => {
                match $e {
                    Some(v) => v,
                    None => return Ok(None),
                }
            };
        }
        let schemas = expect!(self.schemas)
            .into_iter()
            .map(|s| (s.0.schema_id(), Arc::new(s.0)))
            .collect::<HashMap<SchemaId, _>>();

        let partition_specs = expect!(self.partition_spec_ids)
            .into_iter()
            .zip(
                expect!(self.partition_specs)
                    .into_iter()
                    .map(|s| Arc::new(s.0)),
            )
            .collect::<HashMap<_, _>>();

        let default_spec = partition_specs
            .get(&expect!(self.default_partition_spec_id))
            .ok_or(ErrorModel::internal(
                "Default partition spec not found",
                "InternalDefaultPartitionSpecNotFound",
                None,
            ))?
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
        ))
        .map(
            |(snap_id, schema_id, summary, manifest, parent_snap, seq, timestamp_ms)| {
                (
                    snap_id,
                    Arc::new({
                        let builder = iceberg::spec::Snapshot::builder()
                            .with_manifest_list(manifest)
                            .with_parent_snapshot_id(parent_snap)
                            .with_sequence_number(seq)
                            .with_snapshot_id(snap_id)
                            .with_summary(summary.0)
                            .with_timestamp_ms(timestamp_ms);
                        if let Some(schema_id) = schema_id {
                            builder.with_schema_id(schema_id).build()
                        } else {
                            builder.build()
                        }
                    }),
                )
            },
        )
        .collect::<_>();

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

        let sort_orders =
            itertools::multizip((expect!(self.sort_order_ids), expect!(self.sort_orders)))
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

        Ok(Some(
            TableMetadata::try_from_parts(Parts {
                format_version: FormatVersion::from(expect!(self.table_format_version)),
                table_uuid: self.table_id,
                location: join_location(&self.table_fs_protocol, &self.table_fs_location),
                last_sequence_number: expect!(self.last_sequence_number),
                last_updated_ms: expect!(self.last_updated_ms),
                last_column_id: expect!(self.last_column_id),
                schemas,
                current_schema_id: expect!(self.current_schema),
                partition_specs,
                default_spec,
                last_partition_id: expect!(self.last_partition_id),
                properties,
                current_snapshot_id,
                snapshots,
                snapshot_log,
                metadata_log,
                sort_orders,
                default_sort_order_id: expect!(self.default_sort_order_id),
                refs,
                partition_statistics,
                statistics,
            })
            .map_err(|e| {
                ErrorModel::internal(
                    "Error parsing table metadata from DB",
                    "InternalTableMetadataParseError",
                    Some(Box::new(e)),
                )
            })?,
        ))
    }
}

pub(crate) async fn load_storage_profile(
    warehouse_id: WarehouseIdent,
    table: TableIdentUuid,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<(Option<SecretIdent>, StorageProfile)> {
    let secret = sqlx::query!(
        r#"
        SELECT w.storage_secret_id,
        w.storage_profile as "storage_profile: Json<StorageProfile>"
        FROM "table" t
        INNER JOIN tabular ti ON t.table_id = ti.tabular_id
        INNER JOIN namespace n ON ti.namespace_id = n.namespace_id
        INNER JOIN warehouse w ON n.warehouse_id = w.warehouse_id
        WHERE w.warehouse_id = $1
            AND t."table_id" = $2
            AND w.status = 'active'
        "#,
        *warehouse_id,
        *table
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|e| e.into_error_model("Error fetching storage secret".to_string()))?;

    Ok((
        secret.storage_secret_id.map(SecretIdent::from),
        secret.storage_profile.0,
    ))
}

#[allow(clippy::too_many_lines)]
pub(crate) async fn load_tables(
    warehouse_id: WarehouseIdent,
    tables: impl IntoIterator<Item = TableIdentUuid>,
    include_deleted: bool,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<HashMap<TableIdentUuid, LoadTableResponse>> {
    let table_ids = &tables.into_iter().map(Into::into).collect::<Vec<_>>();

    let table = sqlx::query_as!(
        TableQueryStruct,
        r#"
        SELECT
            t."table_id",
            t.last_sequence_number,
            t.last_column_id,
            t.last_updated_ms,
            t.last_partition_id,
            t.table_format_version as "table_format_version: DbTableFormatVersion",
            ti.name as "table_name",
            ti.fs_location as "table_fs_location",
            ti.fs_protocol as "table_fs_protocol",
            namespace_name,
            ti.namespace_id,
            ti."metadata_location",
            w.storage_profile as "storage_profile: Json<StorageProfile>",
            w."storage_secret_id",
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
            tstat.blob_metadatas as "table_stats_blob_metadata: Vec<Json<Vec<BlobMetadata>>>"
        FROM "table" t
        INNER JOIN tabular ti ON t.table_id = ti.tabular_id
        INNER JOIN namespace n ON ti.namespace_id = n.namespace_id
        INNER JOIN warehouse w ON n.warehouse_id = w.warehouse_id
        INNER JOIN table_current_schema tcs ON tcs.table_id = t.table_id
        LEFT JOIN table_default_partition_spec tdps ON tdps.table_id = t.table_id
        LEFT JOIN table_default_sort_order tdsort ON tdsort.table_id = t.table_id
        LEFT JOIN (SELECT table_id,
                          ARRAY_AGG(schema_id) as schema_ids,
                          ARRAY_AGG(schema) as schemas
                   FROM table_schema WHERE table_id = ANY($2)
                   GROUP BY table_id) ts ON ts.table_id = t.table_id
        LEFT JOIN (SELECT table_id,
                          ARRAY_AGG(partition_spec) as partition_spec,
                          ARRAY_AGG(partition_spec_id) as partition_spec_id
                   FROM table_partition_spec WHERE table_id = ANY($2)
                   GROUP BY table_id) tps ON tps.table_id = t.table_id
        LEFT JOIN (SELECT table_id,
                            ARRAY_AGG(key) as keys,
                            ARRAY_AGG(value) as values
                     FROM table_properties WHERE table_id = ANY($2)
                     GROUP BY table_id) tp ON tp.table_id = t.table_id
        LEFT JOIN (SELECT table_id,
                          ARRAY_AGG(snapshot_id) as snapshot_ids,
                          ARRAY_AGG(parent_snapshot_id) as parent_snapshot_ids,
                          ARRAY_AGG(sequence_number) as sequence_numbers,
                          ARRAY_AGG(manifest_list) as manifest_lists,
                          ARRAY_AGG(summary) as summaries,
                          ARRAY_AGG(schema_id) as schema_ids,
                          ARRAY_AGG(timestamp_ms) as timestamp
                   FROM table_snapshot WHERE table_id = ANY($2)
                   GROUP BY table_id) tsnap ON tsnap.table_id = t.table_id
        LEFT JOIN (SELECT table_id,
                          ARRAY_AGG(snapshot_id ORDER BY sequence_number) as snapshot_ids,
                          ARRAY_AGG(timestamp ORDER BY sequence_number) as timestamps
                     FROM table_snapshot_log WHERE table_id = ANY($2)
                     GROUP BY table_id) tsl ON tsl.table_id = t.table_id
        LEFT JOIN (SELECT table_id,
                          ARRAY_AGG(timestamp ORDER BY sequence_number) as timestamps,
                          ARRAY_AGG(metadata_file ORDER BY sequence_number) as metadata_files
                   FROM table_metadata_log WHERE table_id = ANY($2)
                   GROUP BY table_id) tml ON tml.table_id = t.table_id
        LEFT JOIN (SELECT table_id,
                          ARRAY_AGG(sort_order_id) as sort_order_ids,
                          ARRAY_AGG(sort_order) as sort_orders
                     FROM table_sort_order WHERE table_id = ANY($2)
                     GROUP BY table_id) tso ON tso.table_id = t.table_id
        LEFT JOIN (SELECT table_id,
                          ARRAY_AGG(table_ref_name) as table_ref_names,
                          ARRAY_AGG(snapshot_id) as snapshot_ids,
                          ARRAY_AGG(retention) as retentions
                   FROM table_refs WHERE table_id = ANY($2)
                   GROUP BY table_id) tr ON tr.table_id = t.table_id
        LEFT JOIN (SELECT table_id,
                          ARRAY_AGG(snapshot_id) as snapshot_ids,
                          ARRAY_AGG(statistics_path) as statistics_paths,
                          ARRAY_AGG(file_size_in_bytes) as file_size_in_bytes_s
                    FROM partition_statistics WHERE table_id = ANY($2)
                    GROUP BY table_id) pstat ON pstat.table_id = t.table_id
        LEFT JOIN (SELECT table_id,
                          ARRAY_AGG(snapshot_id) as snapshot_ids,
                          ARRAY_AGG(statistics_path) as statistics_paths,
                          ARRAY_AGG(file_size_in_bytes) as file_size_in_bytes_s,
                          ARRAY_AGG(file_footer_size_in_bytes) as file_footer_size_in_bytes_s,
                          ARRAY_AGG(key_metadata) as key_metadatas,
                          ARRAY_AGG(blob_metadata) as blob_metadatas
                    FROM table_statistics WHERE table_id = ANY($2)
                    GROUP BY table_id) tstat ON tstat.table_id = t.table_id
        WHERE w.warehouse_id = $1
            AND w.status = 'active'
            AND (ti.deleted_at IS NULL OR $3)
            AND t."table_id" = ANY($2)
        "#,
        *warehouse_id,
        &table_ids,
        include_deleted
    )
    .fetch_all(&mut **transaction)
    .await
    .unwrap();

    let mut tables = HashMap::new();
    let mut failed_to_fetch = HashSet::new();
    for table in table {
        let table_id = table.table_id.into();
        let metadata_location = match table
            .metadata_location
            .as_deref()
            .map(FromStr::from_str)
            .transpose()
        {
            Ok(location) => location,
            Err(e) => {
                return Err(ErrorModel::internal(
                    "Error parsing metadata location",
                    "InternalMetadataLocationParseError",
                    Some(Box::new(e)),
                )
                .into());
            }
        };
        let namespace_id = table.namespace_id.into();
        let storage_secret_ident = table.storage_secret_id.map(SecretIdent::from);
        let storage_profile = table.storage_profile.deref().clone();

        let Some(table_metadata) = table.into_table_metadata()? else {
            tracing::warn!(
                "Table metadata could not be fetched from tables, falling back to blob retrieval."
            );
            failed_to_fetch.insert(table_id);
            continue;
        };

        tables.insert(
            table_id,
            LoadTableResponse {
                table_id,
                namespace_id,
                table_metadata,
                metadata_location,
                storage_secret_ident,
                storage_profile,
            },
        );
    }

    for t in table_ids {
        if !tables.contains_key(&((*t).into())) {
            failed_to_fetch.insert((*t).into());
        }
    }
    if !failed_to_fetch.is_empty() {
        tracing::error!(
            "Failed to fetch the following tables: '{:?}'",
            failed_to_fetch
        );
    }

    Ok(tables)
}

pub(crate) async fn get_table_metadata_by_id(
    warehouse_id: WarehouseIdent,
    table: TableIdentUuid,
    list_flags: crate::service::ListFlags,
    catalog_state: CatalogState,
) -> Result<Option<GetTableMetadataResponse>> {
    let table = sqlx::query!(
        r#"
        SELECT
            t."table_id",
            ti.name as "table_name",
            ti.fs_location as "table_fs_location",
            ti.fs_protocol as "table_fs_protocol",
            namespace_name,
            ti.namespace_id,
            t."metadata" as "metadata: Json<TableMetadata>",
            ti."metadata_location",
            w.storage_profile as "storage_profile: Json<StorageProfile>",
            w."storage_secret_id"
        FROM "table" t
        INNER JOIN tabular ti ON t.table_id = ti.tabular_id
        INNER JOIN namespace n ON ti.namespace_id = n.namespace_id
        INNER JOIN warehouse w ON n.warehouse_id = w.warehouse_id
        WHERE w.warehouse_id = $1 AND t."table_id" = $2
            AND w.status = 'active'
            AND (ti.deleted_at IS NULL OR $3)
        "#,
        *warehouse_id,
        *table,
        list_flags.include_deleted
    )
    .fetch_one(&catalog_state.read_pool())
    .await;

    let table = match table {
        Ok(table) => table,
        Err(sqlx::Error::RowNotFound) => return Ok(None),
        Err(e) => {
            return Err(e
                .into_error_model("Error fetching table".to_string())
                .into());
        }
    };

    if !list_flags.include_staged && table.metadata_location.is_none() {
        return Ok(None);
    }

    let namespace = try_parse_namespace_ident(table.namespace_name)?;

    Ok(Some(GetTableMetadataResponse {
        table: TableIdent {
            namespace,
            name: table.table_name,
        },
        namespace_id: table.namespace_id.into(),
        table_id: table.table_id.into(),
        warehouse_id,
        location: join_location(&table.table_fs_protocol, &table.table_fs_location),
        metadata_location: table.metadata_location,
        storage_secret_ident: table.storage_secret_id.map(SecretIdent::from),
        storage_profile: table.storage_profile.deref().clone(),
    }))
}

pub(crate) async fn get_table_metadata_by_s3_location(
    warehouse_id: WarehouseIdent,
    location: &Location,
    list_flags: crate::service::ListFlags,
    catalog_state: CatalogState,
) -> Result<Option<GetTableMetadataResponse>> {
    let (fs_protocol, fs_location) = split_location(location.url().as_str())?;
    let partial_locations = get_partial_fs_locations(location)?;

    // Location might also be a subpath of the table location.
    // We need to make sure that the location starts with the table location.
    let table = sqlx::query!(
        r#"
         SELECT
             t."table_id",
             ti.name as "table_name",
             ti.fs_location as "fs_location",
             namespace_name,
             ti.namespace_id,
             t."metadata" as "metadata: Json<TableMetadata>",
             ti."metadata_location",
             w.storage_profile as "storage_profile: Json<StorageProfile>",
             w."storage_secret_id"
         FROM "table" t
         INNER JOIN tabular ti ON t.table_id = ti.tabular_id
         INNER JOIN namespace n ON ti.namespace_id = n.namespace_id
         INNER JOIN warehouse w ON n.warehouse_id = w.warehouse_id
         WHERE w.warehouse_id = $1
             AND ti.fs_location = ANY($2)
             AND LENGTH(ti.fs_location) <= $3
             AND w.status = 'active'
             AND (ti.deleted_at IS NULL OR $4)
         "#,
        *warehouse_id,
        partial_locations.as_slice(),
        i32::try_from(fs_location.len()).unwrap_or(i32::MAX) + 1, // account for maybe trailing
        list_flags.include_deleted
    )
    .fetch_one(&catalog_state.read_pool())
    .await;

    let table = match table {
        Ok(table) => table,
        Err(sqlx::Error::RowNotFound) => {
            tracing::debug!("Table at location {} not found", location);
            return Ok(None);
        }
        Err(e) => {
            tracing::warn!("Error fetching table: {}", e);
            return Err(e
                .into_error_model("Error fetching table".to_string())
                .into());
        }
    };

    if !list_flags.include_staged && table.metadata_location.is_none() {
        return Ok(None);
    }

    let namespace = try_parse_namespace_ident(table.namespace_name)?;

    Ok(Some(GetTableMetadataResponse {
        table: TableIdent {
            namespace,
            name: table.table_name,
        },
        table_id: table.table_id.into(),
        namespace_id: table.namespace_id.into(),
        warehouse_id,
        location: join_location(fs_protocol, &table.fs_location),
        metadata_location: table.metadata_location,
        storage_secret_ident: table.storage_secret_id.map(SecretIdent::from),
        storage_profile: table.storage_profile.deref().clone(),
    }))
}

/// Rename a table. Tables may be moved across namespaces.
pub(crate) async fn rename_table(
    warehouse_id: WarehouseIdent,
    source_id: TableIdentUuid,
    source: &TableIdent,
    destination: &TableIdent,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<()> {
    crate::implementations::postgres::tabular::rename_tabular(
        warehouse_id,
        TabularIdentUuid::Table(*source_id),
        source,
        destination,
        transaction,
    )
    .await?;

    Ok(())
}

pub(crate) async fn drop_table(
    table_id: TableIdentUuid,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<String> {
    drop_tabular(TabularIdentUuid::Table(*table_id), transaction).await
}

#[derive(Default)]
#[allow(clippy::struct_excessive_bools)]
struct TableUpdates {
    snapshot_refs: bool,
    properties: bool,
}

impl From<&[TableUpdate]> for TableUpdates {
    fn from(value: &[TableUpdate]) -> Self {
        let mut s = TableUpdates::default();
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
    // Desired behaviour:
    // - Stage-Create => Load fails with 404
    // - No Stage-Create => Next create fails with 409, load succeeds
    // - Stage-Create => Next stage-create works & overwrites
    // - Stage-Create => Next regular create works & overwrites

    use std::{default::Default, time::SystemTime};

    use iceberg::{
        spec::{
            NestedField, Operation, PrimitiveType, Schema, Snapshot, SnapshotReference,
            UnboundPartitionSpec,
        },
        NamespaceIdent,
    };
    use iceberg_ext::{catalog::rest::CreateTableRequest, configs::Location};
    use uuid::Uuid;

    use super::*;
    use crate::{
        api::{iceberg::types::PageToken, management::v1::warehouse::WarehouseStatus},
        catalog::tables::create_table_request_into_table_metadata,
        implementations::postgres::{
            namespace::tests::initialize_namespace,
            tabular::{mark_tabular_as_deleted, table::create::create_table},
            warehouse::{set_warehouse_status, test::initialize_warehouse},
        },
        service::{ListFlags, NamespaceIdentUuid, TableCreation},
    };

    fn create_request(
        stage_create: Option<bool>,
        table_name: Option<String>,
    ) -> (CreateTableRequest, Option<Location>) {
        let metadata_location = if let Some(stage_create) = stage_create {
            if stage_create {
                None
            } else {
                Some(
                    format!("s3://my_bucket/my_table/metadata/foo/{}", Uuid::now_v7())
                        .parse()
                        .unwrap(),
                )
            }
        } else {
            Some(
                format!("s3://my_bucket/my_table/metadata/foo/{}", Uuid::now_v7())
                    .parse()
                    .unwrap(),
            )
        };

        (
            CreateTableRequest {
                name: table_name.unwrap_or("my_table".to_string()),
                location: Some(format!("s3://my_bucket/my_table/{}", Uuid::now_v7())),
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
        warehouse_id: WarehouseIdent,
        namespace: &NamespaceIdent,
    ) -> NamespaceIdentUuid {
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
        pub(crate) namespace_id: NamespaceIdentUuid,
        pub(crate) namespace: NamespaceIdent,
        pub(crate) table_id: TableIdentUuid,
        pub(crate) table_ident: TableIdent,
    }

    pub(crate) async fn initialize_table(
        warehouse_id: WarehouseIdent,
        state: CatalogState,
        staged: bool,
        namespace: Option<NamespaceIdent>,
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
        let table_id = Uuid::now_v7().into();

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
            namespace_id,
            table_ident: &table_ident,
            table_metadata,
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

        let warehouse_id = initialize_warehouse(state.clone(), None, None, None, true).await;
        let namespace = NamespaceIdent::from_vec(vec!["my_namespace".to_string()]).unwrap();
        initialize_namespace(state.clone(), warehouse_id, &namespace, None).await;
        let namespace_id = get_namespace_id(state.clone(), warehouse_id, &namespace).await;

        let (request, metadata_location) = create_request(None, None);
        let table_ident = TableIdent {
            namespace: namespace.clone(),
            name: request.name.clone(),
        };

        let mut transaction = pool.begin().await.unwrap();
        let table_id = uuid::Uuid::now_v7().into();

        let table_metadata =
            crate::catalog::tables::create_table_request_into_table_metadata(table_id, request)
                .unwrap();

        let request = TableCreation {
            namespace_id,
            table_ident: &table_ident,
            table_metadata,
            metadata_location: metadata_location.as_ref(),
        };

        let create_result = create_table(request.clone(), &mut transaction)
            .await
            .unwrap();
        transaction.commit().await.unwrap();

        let mut transaction = pool.begin().await.unwrap();
        // Second create should fail
        let mut request = request;
        // exchange location else we fail on unique constraint there
        let location = format!("s3://my_bucket/my_table/other/{}", Uuid::now_v7())
            .as_str()
            .parse::<Location>()
            .unwrap();
        let build = request
            .table_metadata
            .into_builder(None)
            .set_location(location.to_string())
            .assign_uuid(Uuid::now_v7())
            .build()
            .unwrap()
            .metadata;
        request.table_metadata = build;
        let create_err = create_table(request, &mut transaction).await.unwrap_err();

        assert_eq!(
            create_err.error.code,
            StatusCode::CONFLICT,
            "{create_err:?}"
        );

        // Load should succeed
        let mut t = pool.begin().await.unwrap();
        let load_result = load_tables(warehouse_id, vec![table_id], false, &mut t)
            .await
            .unwrap();
        assert_eq!(load_result.len(), 1);
        assert_eq!(
            load_result.get(&table_id).unwrap().table_metadata,
            create_result.table_metadata
        );
    }

    #[sqlx::test]
    async fn test_stage_create(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let warehouse_id = initialize_warehouse(state.clone(), None, None, None, true).await;
        let namespace = NamespaceIdent::from_vec(vec!["my_namespace".to_string()]).unwrap();
        initialize_namespace(state.clone(), warehouse_id, &namespace, None).await;
        let namespace_id = get_namespace_id(state.clone(), warehouse_id, &namespace).await;

        let (request, metadata_location) = create_request(Some(true), None);
        let table_ident = TableIdent {
            namespace: namespace.clone(),
            name: request.name.clone(),
        };

        let mut transaction = pool.begin().await.unwrap();
        let table_id = uuid::Uuid::now_v7().into();
        let table_metadata = create_table_request_into_table_metadata(table_id, request).unwrap();

        let request = TableCreation {
            namespace_id,
            table_ident: &table_ident,
            table_metadata,
            metadata_location: metadata_location.as_ref(),
        };

        let _create_result = create_table(request.clone(), &mut transaction)
            .await
            .unwrap();
        transaction.commit().await.unwrap();

        // Its staged - should not have metadata_location
        let load = load_tables(
            warehouse_id,
            vec![table_id],
            false,
            &mut pool.begin().await.unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(load.len(), 1);
        assert!(load.get(&table_id).unwrap().metadata_location.is_none());

        // Second create should succeed, even with different id
        let mut transaction = pool.begin().await.unwrap();
        let mut request = request;
        request.table_metadata = request
            .table_metadata
            .into_builder(None)
            .assign_uuid(Uuid::now_v7())
            .build()
            .unwrap()
            .metadata;

        let create_result = create_table(request, &mut transaction).await.unwrap();
        transaction.commit().await.unwrap();

        assert_eq!(create_result.table_metadata, create_result.table_metadata);

        // We can overwrite the table with a regular create
        let (request, metadata_location) = create_request(Some(false), None);

        let table_metadata = create_table_request_into_table_metadata(table_id, request).unwrap();

        let request = TableCreation {
            namespace_id,
            table_ident: &table_ident,
            table_metadata,
            metadata_location: metadata_location.as_ref(),
        };
        let mut transaction = pool.begin().await.unwrap();
        let create_result = create_table(request, &mut transaction).await.unwrap();
        transaction.commit().await.unwrap();
        let load_result = load_tables(
            warehouse_id,
            vec![table_id],
            false,
            &mut pool.begin().await.unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(load_result.len(), 1);
        let s1 = format!("{:#?}", load_result.get(&table_id).unwrap().table_metadata);
        let s2 = format!("{:#?}", create_result.table_metadata);
        let diff = similar::TextDiff::from_lines(&s1, &s2);
        let diff = diff
            .unified_diff()
            .context_radius(15)
            .missing_newline_hint(false)
            .to_string();
        assert_eq!(
            load_result.get(&table_id).unwrap().table_metadata,
            create_result.table_metadata,
            "{diff}",
        );
        assert_eq!(
            load_result.get(&table_id).unwrap().metadata_location,
            metadata_location
        );
    }

    #[sqlx::test]
    async fn test_to_id(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let warehouse_id = initialize_warehouse(state.clone(), None, None, None, true).await;
        let namespace = NamespaceIdent::from_vec(vec!["my_namespace".to_string()]).unwrap();
        initialize_namespace(state.clone(), warehouse_id, &namespace, None).await;

        let table_ident = TableIdent {
            namespace: namespace.clone(),
            name: "my_table".to_string(),
        };

        let exists = resolve_table_ident(
            warehouse_id,
            &table_ident,
            ListFlags::default(),
            &state.read_pool(),
        )
        .await
        .unwrap();
        assert!(exists.is_none());
        drop(table_ident);

        let table = initialize_table(warehouse_id, state.clone(), true, None, None).await;

        // Table is staged - no result if include_staged is false
        let exists = resolve_table_ident(
            warehouse_id,
            &table.table_ident,
            ListFlags::default(),
            &state.read_pool(),
        )
        .await
        .unwrap();
        assert!(exists.is_none());

        let exists = resolve_table_ident(
            warehouse_id,
            &table.table_ident,
            ListFlags {
                include_staged: true,
                ..ListFlags::default()
            },
            &state.read_pool(),
        )
        .await
        .unwrap();
        assert_eq!(exists.map(|i| i.ident), Some(table.table_id));
    }

    #[sqlx::test]
    async fn test_to_ids(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let warehouse_id = initialize_warehouse(state.clone(), None, None, None, true).await;
        let namespace = NamespaceIdent::from_vec(vec!["my_namespace".to_string()]).unwrap();
        initialize_namespace(state.clone(), warehouse_id, &namespace, None).await;

        let table_ident = TableIdent {
            namespace: namespace.clone(),
            name: "my_table".to_string(),
        };

        let exists = table_idents_to_ids(
            warehouse_id,
            vec![&table_ident].into_iter().collect(),
            ListFlags::default(),
            &state.read_pool(),
        )
        .await
        .unwrap();
        assert!(exists.len() == 1 && exists.get(&table_ident).unwrap().is_none());

        let table_1 = initialize_table(warehouse_id, state.clone(), true, None, None).await;
        let mut tables = HashSet::new();
        tables.insert(&table_1.table_ident);

        // Table is staged - no result if include_staged is false
        let exists = table_idents_to_ids(
            warehouse_id,
            tables.clone(),
            ListFlags::default(),
            &state.read_pool(),
        )
        .await
        .unwrap();
        assert_eq!(exists.len(), 1);
        assert!(exists.get(&table_1.table_ident).unwrap().is_none());

        let exists = table_idents_to_ids(
            warehouse_id,
            tables.clone(),
            ListFlags {
                include_staged: true,
                ..ListFlags::default()
            },
            &state.read_pool(),
        )
        .await
        .unwrap();
        assert_eq!(exists.len(), 1);
        assert_eq!(
            exists.get(&table_1.table_ident).unwrap(),
            &Some(table_1.table_id)
        );

        // Second Table
        let table_2 = initialize_table(warehouse_id, state.clone(), false, None, None).await;
        tables.insert(&table_2.table_ident);

        let exists = table_idents_to_ids(
            warehouse_id,
            tables.clone(),
            ListFlags::default(),
            &state.read_pool(),
        )
        .await
        .unwrap();
        assert_eq!(exists.len(), 2);
        assert!(exists.get(&table_1.table_ident).unwrap().is_none());
        assert_eq!(
            exists.get(&table_2.table_ident).unwrap(),
            &Some(table_2.table_id)
        );

        let exists = table_idents_to_ids(
            warehouse_id,
            tables.clone(),
            ListFlags {
                include_staged: true,
                ..ListFlags::default()
            },
            &state.read_pool(),
        )
        .await
        .unwrap();
        assert_eq!(exists.len(), 2);
        assert_eq!(
            exists.get(&table_1.table_ident).unwrap(),
            &Some(table_1.table_id)
        );
        assert_eq!(
            exists.get(&table_2.table_ident).unwrap(),
            &Some(table_2.table_id)
        );
    }

    #[sqlx::test]
    async fn test_rename_without_namespace(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let warehouse_id = initialize_warehouse(state.clone(), None, None, None, true).await;
        let table = initialize_table(warehouse_id, state.clone(), false, None, None).await;

        let new_table_ident = TableIdent {
            namespace: table.namespace.clone(),
            name: "new_table".to_string(),
        };

        let mut transaction = pool.begin().await.unwrap();
        rename_table(
            warehouse_id,
            table.table_id,
            &table.table_ident,
            &new_table_ident,
            &mut transaction,
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();

        let exists = resolve_table_ident(
            warehouse_id,
            &table.table_ident,
            ListFlags::default(),
            &state.read_pool(),
        )
        .await
        .unwrap();
        assert!(exists.is_none());

        let exists = resolve_table_ident(
            warehouse_id,
            &new_table_ident,
            ListFlags::default(),
            &state.read_pool(),
        )
        .await
        .unwrap();
        // Table id should be the same
        assert_eq!(exists.map(|i| i.ident), Some(table.table_id));
    }

    #[sqlx::test]
    async fn test_rename_with_namespace(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let warehouse_id = initialize_warehouse(state.clone(), None, None, None, true).await;
        let table = initialize_table(warehouse_id, state.clone(), false, None, None).await;

        let new_namespace = NamespaceIdent::from_vec(vec!["new_namespace".to_string()]).unwrap();
        initialize_namespace(state.clone(), warehouse_id, &new_namespace, None).await;

        let new_table_ident = TableIdent {
            namespace: new_namespace.clone(),
            name: "new_table".to_string(),
        };

        let mut transaction = pool.begin().await.unwrap();
        rename_table(
            warehouse_id,
            table.table_id,
            &table.table_ident,
            &new_table_ident,
            &mut transaction,
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();

        let exists = resolve_table_ident(
            warehouse_id,
            &table.table_ident,
            ListFlags::default(),
            &state.read_pool(),
        )
        .await
        .unwrap();
        assert!(exists.is_none());

        let exists = resolve_table_ident(
            warehouse_id,
            &new_table_ident,
            ListFlags::default(),
            &state.read_pool(),
        )
        .await
        .unwrap();
        assert_eq!(exists.map(|i| i.ident), Some(table.table_id));
    }

    #[sqlx::test]
    async fn test_list_tables(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let warehouse_id = initialize_warehouse(state.clone(), None, None, None, true).await;
        let namespace = NamespaceIdent::from_vec(vec!["my_namespace".to_string()]).unwrap();
        initialize_namespace(state.clone(), warehouse_id, &namespace, None).await;
        let tables = list_tables(
            warehouse_id,
            &namespace,
            ListFlags::default(),
            &state.read_pool(),
            PaginationQuery::empty(),
        )
        .await
        .unwrap();
        assert_eq!(tables.len(), 0);

        let table1 = initialize_table(warehouse_id, state.clone(), false, None, None).await;

        let tables = list_tables(
            warehouse_id,
            &table1.namespace,
            ListFlags::default(),
            &state.read_pool(),
            PaginationQuery::empty(),
        )
        .await
        .unwrap();
        assert_eq!(tables.len(), 1);
        assert_eq!(tables.get(&table1.table_id), Some(&table1.table_ident));

        let table2 = initialize_table(warehouse_id, state.clone(), true, None, None).await;
        let tables = list_tables(
            warehouse_id,
            &table2.namespace,
            ListFlags::default(),
            &state.read_pool(),
            PaginationQuery::empty(),
        )
        .await
        .unwrap();
        assert_eq!(tables.len(), 0);
        let tables = list_tables(
            warehouse_id,
            &table2.namespace,
            ListFlags {
                include_staged: true,
                ..ListFlags::default()
            },
            &state.read_pool(),
            PaginationQuery::empty(),
        )
        .await
        .unwrap();
        assert_eq!(tables.len(), 1);
        assert_eq!(tables.get(&table2.table_id), Some(&table2.table_ident));
    }

    #[sqlx::test]
    async fn test_list_tables_pagination(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let warehouse_id = initialize_warehouse(state.clone(), None, None, None, true).await;
        let namespace = NamespaceIdent::from_vec(vec!["my_namespace".to_string()]).unwrap();
        initialize_namespace(state.clone(), warehouse_id, &namespace, None).await;
        let tables = list_tables(
            warehouse_id,
            &namespace,
            ListFlags::default(),
            &state.read_pool(),
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
            Some("t1".into()),
        )
        .await;
        let table2 = initialize_table(
            warehouse_id,
            state.clone(),
            true,
            Some(namespace.clone()),
            Some("t2".into()),
        )
        .await;
        let table3 = initialize_table(
            warehouse_id,
            state.clone(),
            true,
            Some(namespace.clone()),
            Some("t3".into()),
        )
        .await;

        let tables = list_tables(
            warehouse_id,
            &namespace,
            ListFlags {
                include_staged: true,
                ..ListFlags::default()
            },
            &state.read_pool(),
            PaginationQuery {
                page_token: PageToken::NotSpecified,
                page_size: Some(2),
            },
        )
        .await
        .unwrap();
        assert_eq!(tables.len(), 2);

        assert_eq!(tables.get(&table2.table_id), Some(&table2.table_ident));

        let tables = list_tables(
            warehouse_id,
            &namespace,
            ListFlags {
                include_staged: true,
                ..ListFlags::default()
            },
            &state.read_pool(),
            PaginationQuery {
                page_token: PageToken::Present(tables.next_token().unwrap().to_string()),
                page_size: Some(2),
            },
        )
        .await
        .unwrap();

        assert_eq!(tables.len(), 1);
        assert_eq!(tables.get(&table3.table_id), Some(&table3.table_ident));

        let tables = list_tables(
            warehouse_id,
            &namespace,
            ListFlags {
                include_staged: true,
                ..ListFlags::default()
            },
            &state.read_pool(),
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
    async fn test_get_id_by_location(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let warehouse_id = initialize_warehouse(state.clone(), None, None, None, true).await;
        let table = initialize_table(warehouse_id, state.clone(), false, None, None).await;

        let metadata = get_table_metadata_by_id(
            warehouse_id,
            table.table_id,
            ListFlags::default(),
            state.clone(),
        )
        .await
        .unwrap()
        .unwrap();
        let mut metadata_location = metadata.location.parse::<Location>().unwrap();
        // Exact path works
        let id = get_table_metadata_by_s3_location(
            warehouse_id,
            &metadata_location,
            ListFlags::default(),
            state.clone(),
        )
        .await
        .unwrap()
        .unwrap()
        .table_id;

        assert_eq!(id, table.table_id);

        let mut subpath = metadata_location.clone();
        subpath.push("data/foo.parquet");
        // Subpath works
        let id = get_table_metadata_by_s3_location(
            warehouse_id,
            &subpath,
            ListFlags::default(),
            state.clone(),
        )
        .await
        .unwrap()
        .unwrap()
        .table_id;

        assert_eq!(id, table.table_id);

        // Path without trailing slash works
        metadata_location.without_trailing_slash();
        get_table_metadata_by_s3_location(
            warehouse_id,
            &metadata_location,
            ListFlags::default(),
            state.clone(),
        )
        .await
        .unwrap();

        metadata_location.with_trailing_slash();
        // Path with trailing slash works
        get_table_metadata_by_s3_location(
            warehouse_id,
            &metadata_location,
            ListFlags::default(),
            state.clone(),
        )
        .await
        .unwrap();

        let shorter = metadata.location[0..metadata.location.len() - 2]
            .to_string()
            .parse()
            .unwrap();

        // Shorter path does not work
        assert!(get_table_metadata_by_s3_location(
            warehouse_id,
            &shorter,
            ListFlags::default(),
            state.clone(),
        )
        .await
        .unwrap()
        .is_none());
    }

    #[sqlx::test]
    async fn test_cannot_get_table_of_inactive_warehouse(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let warehouse_id = initialize_warehouse(state.clone(), None, None, None, true).await;
        let table = initialize_table(warehouse_id, state.clone(), false, None, None).await;
        let mut transaction = pool.begin().await.expect("Failed to start transaction");
        set_warehouse_status(warehouse_id, WarehouseStatus::Inactive, &mut transaction)
            .await
            .expect("Failed to set warehouse status");
        transaction.commit().await.unwrap();

        let r = get_table_metadata_by_id(
            warehouse_id,
            table.table_id,
            ListFlags::default(),
            state.clone(),
        )
        .await
        .unwrap();
        assert!(r.is_none());
    }

    #[sqlx::test]
    async fn test_drop_table_works(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let warehouse_id = initialize_warehouse(state.clone(), None, None, None, true).await;
        let table = initialize_table(warehouse_id, state.clone(), false, None, None).await;

        let mut transaction = pool.begin().await.unwrap();
        mark_tabular_as_deleted(
            TabularIdentUuid::Table(*table.table_id),
            None,
            &mut transaction,
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();

        assert!(get_table_metadata_by_id(
            warehouse_id,
            table.table_id,
            ListFlags::default(),
            state.clone(),
        )
        .await
        .unwrap()
        .is_none());

        let ok = get_table_metadata_by_id(
            warehouse_id,
            table.table_id,
            ListFlags {
                include_deleted: true,
                ..ListFlags::default()
            },
            state.clone(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(ok.table_id, table.table_id);

        let mut transaction = pool.begin().await.unwrap();

        drop_table(table.table_id, &mut transaction).await.unwrap();
        transaction.commit().await.unwrap();

        assert!(get_table_metadata_by_id(
            warehouse_id,
            table.table_id,
            ListFlags {
                include_deleted: true,
                ..ListFlags::default()
            },
            state.clone(),
        )
        .await
        .unwrap()
        .is_none());
    }
}
