use crate::implementations::postgres::{dbutils::DBErrorHandler as _, CatalogState};
use crate::service::{TableCommit, TableCreation};
use crate::{
    service::{
        storage::StorageProfile, CreateTableResponse, ErrorModel, GetTableMetadataResponse,
        LoadTableResponse, Result, TableIdent, TableIdentUuid,
    },
    SecretIdent, WarehouseIdent,
};

use http::StatusCode;
use iceberg_ext::{spec::TableMetadata, NamespaceIdent};

use crate::api::iceberg::v1::{PaginatedMapping, PaginationQuery};
use crate::implementations::postgres::tabular::{
    create_tabular, drop_tabular, list_tabulars, try_parse_namespace_ident, CreateTabular,
    TabularIdentBorrowed, TabularIdentOwned, TabularIdentUuid, TabularType,
};
use iceberg::spec::{
    FormatVersion, PartitionField, Schema, SnapshotRetention, SortOrder, StructType, Summary,
    TableMetadataBuilder, UnboundPartitionSpec,
};
use iceberg_ext::configs::Location;
use sqlx::types::Json;
use sqlx::PgConnection;
use std::default::Default;
use std::str::FromStr;
use std::{
    collections::{HashMap, HashSet},
    ops::Deref,
};
use uuid::Uuid;

const MAX_PARAMETERS: usize = 30000;

pub(crate) async fn table_ident_to_id<'e, 'c: 'e, E>(
    warehouse_id: WarehouseIdent,
    table: &TableIdent,
    list_flags: crate::service::ListFlags,
    catalog_state: E,
) -> Result<Option<TableIdentUuid>>
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
    .map(|id| match id {
        TabularIdentUuid::Table(tab) => Ok(tab.into()),
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

pub(crate) async fn create_table(
    TableCreation {
        namespace_id,
        table_ident,
        table_metadata,
        metadata_location,
    }: TableCreation<'_>,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<CreateTableResponse> {
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
            id: table_metadata.table_uuid,
            name,
            namespace_id: *namespace_id,
            typ: TabularType::Table,
            metadata_location,
            location: &Location::from_str(table_metadata.location.as_str()).map_err(|err| {
                ErrorModel::bad_request(
                    format!("Invalid location: '{}'", table_metadata.location.as_str()),
                    "InvalidLocation",
                    Some(Box::new(err)),
                )
            })?,
        },
        transaction,
    )
    .await?;

    let _update_result = sqlx::query!(
        r#"
        INSERT INTO "table" (table_id, metadata, table_format_version)
        (
            SELECT $1, $2, $3
            WHERE EXISTS (SELECT 1
                FROM active_tables
                WHERE active_tables.table_id = $1))
        ON CONFLICT ON CONSTRAINT "table_pkey"
        DO UPDATE SET "metadata" = $2
        RETURNING "table_id"
        "#,
        tabular_id,
        table_metadata_ser,
        match table_metadata.format_version {
            FormatVersion::V1 => DbTableFormatVersion::V1,
            FormatVersion::V2 => DbTableFormatVersion::V2,
        } as _
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|e| {
        tracing::warn!("Error creating table: {}", e);
        e.into_error_model("Error creating table".to_string())
    })?;

    let TableMetadata {
        format_version,
        table_uuid,
        location,
        last_sequence_number,
        last_updated_ms,
        last_column_id,
        schemas,
        current_schema_id,
        last_partition_id,
        properties,
        current_snapshot_id,
        snapshots,
        snapshot_log,
        metadata_log,
        sort_orders,
        default_sort_order_id,
        refs,
        ..
    } = &table_metadata;

    let partition_specs = table_metadata.partition_specs_iter();
    let default_spec = table_metadata.default_partition_spec();

    for (_, scheme) in schemas.into_iter() {
        let _ = sqlx::query!(
            r#"INSERT INTO table_schema(schema_id, table_id, schema) VALUES ($1, $2, $3)"#,
            scheme.schema_id(),
            tabular_id,
            serde_json::to_value(scheme).map_err(|er| ErrorModel::internal(
                "Error serializing schema",
                "SchemaSerializationError",
                Some(Box::new(er)),
            ))?
        )
        .execute(&mut **transaction)
        .await
        .map_err(|err| {
            tracing::warn!("Error creating table: {}", err);
            err.into_error_model("Error inserting table schema".to_string())
        })?;
    }

    let _ = sqlx::query!(
        r#"INSERT INTO table_current_schema (table_id, schema_id) VALUES ($1, $2)"#,
        tabular_id,
        *current_schema_id
    )
    .execute(&mut **transaction)
    .await
    .map_err(|err| {
        tracing::warn!("Error creating table: {}", err);
        err.into_error_model("Error inserting table current schema".to_string())
    })?;

    for part_spec in partition_specs {
        let _ = sqlx::query!(
            r#"INSERT INTO table_partition_spec(partition_spec_id, table_id, partition_spec) VALUES ($1, $2, $3)"#,
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
    let partition_type = serde_json::to_value(default_spec.partition_type()).map_err(|er| {
        ErrorModel::internal(
            "Error serializing partition type",
            "PartitionTypeSerializationError",
            Some(Box::new(er)),
        )
    })?;

    let fields = serde_json::to_value(default_spec.fields()).map_err(|er| {
        ErrorModel::internal(
            "Error serializing partition fields",
            "PartitionFieldsSerializationError",
            Some(Box::new(er)),
        )
    })?;

    // insert default part spec
    let _ = sqlx::query!(
        r#"INSERT INTO table_default_partition_spec(partition_spec_id, table_id, schema_id, partition_type, fields) VALUES ($1, $2, $3, $4, $5)"#,
        default_spec.spec_id(),
        tabular_id,
        default_spec.schema_ref().schema_id(),
        partition_type,
        fields
    )
    .execute(&mut **transaction)
    .await
    .map_err(|err| {
        tracing::warn!("Error creating table: {}", err);
        err.into_error_model("Error inserting table default partition spec".to_string())
    })?;

    set_table_properties(properties, tabular_id, &mut **transaction).await?;

    for snap in snapshots.values() {
        let _ = sqlx::query!(
            r#"INSERT INTO table_snapshot(snapshot_id,
                                          table_id,
                                          parent_snapshot_id,
                                          sequence_number,
                                          manifest_list,
                                          summary,
                                          schema_id)
            VALUES ($1, $2, $3, $4, $5, $6, $7)"#,
            snap.snapshot_id(),
            tabular_id,
            snap.parent_snapshot_id(),
            snap.sequence_number(),
            snap.manifest_list(),
            serde_json::to_value(&snap.summary()).map_err(|er| ErrorModel::internal(
                "Error serializing snapshot summary",
                "SnapshotSummarySerializationError",
                Some(Box::new(er)),
            ))?,
            snap.schema_id()
        )
        .execute(&mut **transaction)
        .await
        .map_err(|err| {
            tracing::warn!("Error creating table: {}", err);
            err.into_error_model("Error inserting table snapshot".to_string())
        })?;
    }

    // set current snap
    if let Some(current_snapshot_id) = current_snapshot_id {
        let _ = sqlx::query!(
            r#"INSERT INTO table_current_snapshot(snapshot_id, table_id) VALUES ($1, $2)"#,
            current_snapshot_id,
            tabular_id
        )
        .execute(&mut **transaction)
        .await
        .map_err(|err| {
            tracing::warn!("Error creating table: {}", err);
            err.into_error_model("Error inserting table current snapshot".to_string())
        })?;
    }

    for log in snapshot_log {
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

    for log in metadata_log {
        let _ = sqlx::query!(
            r#"INSERT INTO table_metadata_log(table_id, timestamp, metadata_file) VALUES ($1, $2, $3)"#,
            tabular_id,
            log.timestamp_ms,
            log.metadata_file
        )
            .execute(&mut **transaction)
            .await
            .map_err(|err| {
                tracing::warn!("Error creating table: {}", err);
                err.into_error_model("Error inserting table metadata log".to_string())
            })?;
    }

    for (refname, snapshot_ref) in refs {
        let retention = serde_json::to_value(&snapshot_ref.retention).map_err(|er| {
            ErrorModel::internal(
                "Error serializing retention",
                "RetentionSerializationError",
                Some(Box::new(er)),
            )
        })?;

        let _ = sqlx::query!(
            r#"INSERT INTO table_refs(table_id,
                                      table_ref_name,
                                      snapshot_id,
                                      retention)
            VALUES ($1, $2, $3, $4)"#,
            tabular_id,
            refname,
            snapshot_ref.snapshot_id,
            retention,
        )
        .execute(&mut **transaction)
        .await
        .map_err(|err| {
            tracing::warn!("Error creating table: {}", err);
            err.into_error_model("Error inserting table refs".to_string())
        })?;
    }

    Ok(CreateTableResponse { table_metadata })
}

#[derive(Debug, sqlx::Type)]
#[sqlx(type_name = "table_format_version", rename_all = "kebab-case")]
pub enum DbTableFormatVersion {
    #[sqlx(rename = "1")]
    V1,
    #[sqlx(rename = "2")]
    V2,
}

pub(crate) async fn set_table_properties(
    properties: &HashMap<String, String>,
    table_id: Uuid,
    transaction: &mut PgConnection,
) -> Result<()> {
    let (keys, vals): (Vec<String>, Vec<String>) = properties
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .unzip();
    sqlx::query!(
        r#"INSERT INTO table_properties (table_id, key, value)
           VALUES ($1, UNNEST($2::text[]), UNNEST($3::text[]))
              ON CONFLICT (table_id, key)
                DO UPDATE SET value = EXCLUDED.value
           ;"#,
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

pub(crate) async fn load_tables(
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
            ti.location as "table_location",
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
                    table_metadata: table.metadata.deref().clone(),
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
        list_flags,
        transaction,
        Some(TabularType::Table),
        pagination_query,
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

#[derive(sqlx::FromRow)]
struct TableQueryStruct {
    table_id: Uuid,
    table_name: String,
    namespace_name: Vec<String>,
    namespace_id: Uuid,
    metadata: Json<TableMetadata>,
    table_ref_names: Option<Vec<String>>,
    table_ref_snapshot_ids: Option<Vec<i64>>,
    table_ref_retention: Option<Vec<Json<SnapshotRetention>>>,
    default_sort_order_id: Option<i32>,
    sort_order_ids: Option<Vec<i32>>,
    sort_orders: Option<Vec<Json<SortOrder>>>,
    metadata_log_timestamps: Option<Vec<i64>>,
    metadata_log_files: Option<Vec<String>>,
    snapshot_log_timestamps: Option<Vec<i64>>,
    snapshot_log_ids: Option<Vec<i64>>,
    current_snapshot_id: Option<i64>,
    snapshot_ids: Option<Vec<i64>>,
    snapshot_parent_snapshot_id: Option<Vec<Option<i64>>>,
    snapshot_sequence_number: Option<Vec<i64>>,
    snapshot_manifest_list: Option<Vec<Vec<String>>>,
    snapshot_summary: Option<Vec<Json<Summary>>>,
    snapshot_schema_id: Option<Vec<i32>>,
    metadata_location: Option<String>,
    table_location: String,
    storage_profile: Json<StorageProfile>,
    storage_secret_id: Option<Uuid>,
    table_properties_keys: Option<Vec<String>>,
    table_properties_values: Option<Vec<String>>,
    default_partition_spec_id: Option<i32>,
    default_partition_schema_id: Option<i32>,
    default_partition_fields: Option<Json<Vec<PartitionField>>>,
    default_partition_struct_type: Option<serde_json::Value>,
    partition_spec_ids: Option<Vec<i32>>,
    partition_specs: Option<Vec<Json<UnboundPartitionSpec>>>,
    current_schema: Option<i32>,
    schemas: Option<Vec<Json<Schema>>>,
    schema_ids: Option<Vec<i32>>,
    table_format_version: Option<DbTableFormatVersion>,
}

impl TableQueryStruct {
    fn into_table_metadata(self) -> Result<TableMetadata> {
        todo!()
    }
}

pub(crate) async fn load_tables_2(
    warehouse_id: WarehouseIdent,
    tables: impl IntoIterator<Item = TableIdentUuid>,
    include_deleted: bool,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<HashMap<TableIdentUuid, LoadTableResponse>> {
    let table = sqlx::query_as!(
        TableQueryStruct,
        r#"
        SELECT
            t."table_id",
            t.table_format_version as "table_format_version: DbTableFormatVersion",
            ti.name as "table_name",
            ti.location as "table_location",
            namespace_name,
            ti.namespace_id,
            t."metadata" as "metadata: Json<TableMetadata>",
            ti."metadata_location",
            w.storage_profile as "storage_profile: Json<StorageProfile>",
            w."storage_secret_id",
            ts.schema_ids,
            tcs.schema_id as "current_schema",
            tdps.partition_spec_id as "default_partition_spec_id",
            tdps.schema_id as "default_partition_schema_id",
            tdps.fields as "default_partition_fields: Json<Vec<PartitionField>>",
            tdps.partition_type as "default_partition_struct_type",
            ts.schemas as "schemas: Vec<Json<Schema>>",
            tsnap.snapshot_ids,
            tcsnap.snapshot_id as "current_snapshot_id?",
            tsnap.parent_snapshot_ids as "snapshot_parent_snapshot_id: Vec<Option<i64>>",
            tsnap.sequence_numbers as "snapshot_sequence_number",
            tsnap.manifest_lists as "snapshot_manifest_list: Vec<Vec<String>>",
            tsnap.summaries as "snapshot_summary: Vec<Json<Summary>>",
            tsnap.schema_ids as "snapshot_schema_id",
            tdsort.sort_order_id as "default_sort_order_id?",
            tps.partition_spec_id as "partition_spec_ids",
            tps.partition_spec as "partition_specs: Vec<Json<UnboundPartitionSpec>>",
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
            tr.retentions as "table_ref_retention: Vec<Json<SnapshotRetention>>"
        FROM "table" t
        INNER JOIN tabular ti ON t.table_id = ti.tabular_id
        INNER JOIN namespace n ON ti.namespace_id = n.namespace_id
        INNER JOIN warehouse w ON n.warehouse_id = w.warehouse_id
        INNER JOIN table_current_schema tcs ON tcs.table_id = t.table_id
        LEFT JOIN table_default_partition_spec tdps ON tdps.table_id = t.table_id
        LEFT JOIN table_current_snapshot tcsnap ON tcsnap.table_id = t.table_id
        LEFT JOIN table_default_sort_order tdsort ON tdsort.table_id = t.table_id
        LEFT JOIN (SELECT table_id,
                          ARRAY_AGG(schema_id) as schema_ids,
                          ARRAY_AGG(schema) as schemas
                   FROM table_schema
                   GROUP BY table_id) ts ON ts.table_id = t.table_id
        LEFT JOIN (SELECT table_id,
                          ARRAY_AGG(partition_spec) as partition_spec,
                          ARRAY_AGG(partition_spec_id) as partition_spec_id
                   FROM table_partition_spec
                   GROUP BY table_id) tps ON tps.table_id = t.table_id
        LEFT JOIN (SELECT table_id,
                            ARRAY_AGG(key) as keys,
                            ARRAY_AGG(value) as values
                     FROM table_properties
                     GROUP BY table_id) tp ON tp.table_id = t.table_id
        LEFT JOIN (SELECT table_id,
                          ARRAY_AGG(snapshot_id) as snapshot_ids,
                          ARRAY_AGG(parent_snapshot_id) as parent_snapshot_ids,
                          ARRAY_AGG(sequence_number) as sequence_numbers,
                          ARRAY_AGG(manifest_list) as manifest_lists,
                          ARRAY_AGG(summary) as summaries,
                          ARRAY_AGG(schema_id) as schema_ids
                   FROM table_snapshot
                   GROUP BY table_id) tsnap ON tsnap.table_id = t.table_id
        LEFT JOIN (SELECT table_id,
                          ARRAY_AGG(snapshot_id) as snapshot_ids,
                          ARRAY_AGG(timestamp) as timestamps
                     FROM table_snapshot_log
                     GROUP BY table_id) tsl ON tsl.table_id = t.table_id
        LEFT JOIN (SELECT table_id,
                          ARRAY_AGG(timestamp) as timestamps,
                          ARRAY_AGG(metadata_file) as metadata_files
                   FROM table_metadata_log
                   GROUP BY table_id) tml ON tml.table_id = t.table_id
        LEFT JOIN (SELECT table_id,
                          ARRAY_AGG(sort_order_id) as sort_order_ids,
                          ARRAY_AGG(sort_order) as sort_orders
                     FROM table_sort_order
                        GROUP BY table_id) tso ON tso.table_id = t.table_id
        LEFT JOIN (SELECT table_id,
                          ARRAY_AGG(table_ref_name) as table_ref_names,
                          ARRAY_AGG(snapshot_id) as snapshot_ids,
                          ARRAY_AGG(retention) as retentions
                   FROM table_refs
                   GROUP BY table_id) tr ON tr.table_id = t.table_id
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
    .unwrap();

    table
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
                    table_metadata: table.metadata.deref().clone(),
                    metadata_location,
                    storage_secret_ident: table.storage_secret_id.map(SecretIdent::from),
                    storage_profile: table.storage_profile.deref().clone(),
                },
            ))
        })
        .collect::<Result<HashMap<_, _>>>()
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
            ti.location as "table_location",
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
        location: table.table_location,
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
    let query_strings = location
        .partial_locations()
        .into_iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();

    // Location might also be a subpath of the table location.
    // We need to make sure that the location starts with the table location.
    let table = sqlx::query!(
        r#"
         SELECT
             t."table_id",
             ti.name as "table_name",
             ti.location as "table_location",
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
             AND ti.location = ANY($2)
             AND LENGTH(ti.location) <= $3
             AND w.status = 'active'
             AND (ti.deleted_at IS NULL OR $4)
         "#,
        *warehouse_id,
        query_strings.as_slice(),
        i32::try_from(location.url().as_str().len()).unwrap_or(i32::MAX) + 1, // account for maybe trailing
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
        table_id: table.table_id.into(),
        namespace_id: table.namespace_id.into(),
        warehouse_id,
        location: table.table_location,
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

pub(crate) async fn drop_table<'a>(
    table_id: TableIdentUuid,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<String> {
    let _ = sqlx::query!(
        r#"
        DELETE FROM "table"
        WHERE table_id = $1 AND EXISTS (
            SELECT 1
            FROM active_tables
            WHERE active_tables.table_id = $1
        )
        RETURNING table_id
        "#,
        *table_id,
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|e| {
        if let sqlx::Error::RowNotFound = e {
            ErrorModel::not_found(
                "Table not found",
                "NoSuchTabularError".to_string(),
                Some(Box::new(e)),
            )
        } else {
            tracing::warn!("Error dropping table: {}", e);
            e.into_error_model("Error dropping table".into())
        }
    })?;

    drop_tabular(TabularIdentUuid::Table(*table_id), transaction).await
}

pub(crate) async fn commit_table_transaction<'a>(
    // We do not need the warehouse_id here, because table_ids are unique across warehouses
    _: WarehouseIdent,
    commits: impl IntoIterator<Item = TableCommit> + Send,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<()> {
    let commits: Vec<TableCommit> = commits.into_iter().collect();
    if commits.len() > (MAX_PARAMETERS / 4) {
        return Err(ErrorModel::builder()
            .code(StatusCode::BAD_REQUEST.into())
            .message("Too updates in single commit".to_string())
            .r#type("TooManyTablesForCommit".to_string())
            .build()
            .into());
    }

    let mut query_builder_table = sqlx::QueryBuilder::new(
        r#"
        UPDATE "table" as t
        SET "metadata" = c."metadata"
        FROM (VALUES
        "#,
    );

    let mut query_builder_tabular = sqlx::QueryBuilder::new(
        r#"
        UPDATE "tabular" as t
        SET "metadata_location" = c."metadata_location",
        "location" = c."location"
        FROM (VALUES
        "#,
    );

    for (i, commit) in commits.iter().enumerate() {
        let metadata_ser = serde_json::to_value(&commit.new_metadata).map_err(|e| {
            ErrorModel::internal(
                "Error serializing table metadata",
                "TableMetadataSerializationError",
                Some(Box::new(e)),
            )
        })?;

        query_builder_table.push("(");
        query_builder_table.push_bind(commit.new_metadata.uuid());
        query_builder_table.push(", ");
        query_builder_table.push_bind(metadata_ser);
        query_builder_table.push(")");

        query_builder_tabular.push("(");
        query_builder_tabular.push_bind(commit.new_metadata.uuid());
        query_builder_tabular.push(", ");
        query_builder_tabular.push_bind(commit.new_metadata_location.to_string());
        query_builder_tabular.push(", ");
        query_builder_tabular.push_bind(commit.new_metadata.location());
        query_builder_tabular.push(")");

        if i != commits.len() - 1 {
            query_builder_table.push(", ");
            query_builder_tabular.push(", ");
        }
    }

    query_builder_table.push(") as c(table_id, metadata) WHERE c.table_id = t.table_id");
    query_builder_tabular.push(
        ") as c(table_id, metadata_location, location) WHERE c.table_id = t.tabular_id AND t.typ = 'table'",
    );

    query_builder_table.push(" RETURNING t.table_id");
    query_builder_tabular.push(" RETURNING t.tabular_id");

    let query_meta_update = query_builder_table.build();
    let query_meta_location_update = query_builder_tabular.build();

    // futures::try_join didn't work due to concurrent mutable borrow of transaction
    let updated_meta = query_meta_update
        .fetch_all(&mut **transaction)
        .await
        .map_err(|e| e.into_error_model("Error committing tablemetadata updates".to_string()))?;

    let updated_meta_location = query_meta_location_update
        .fetch_all(&mut **transaction)
        .await
        .map_err(|e| {
            e.into_error_model("Error committing tablemetadata location updates".to_string())
        })?;

    if updated_meta.len() != commits.len() || updated_meta_location.len() != commits.len() {
        return Err(ErrorModel::builder()
            .code(StatusCode::INTERNAL_SERVER_ERROR.into())
            .message("Error committing table updates".to_string())
            .r#type("CommitTableUpdateError".to_string())
            .build()
            .into());
    }

    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    // Desired behaviour:
    // - Stage-Create => Load fails with 404
    // - No Stage-Create => Next create fails with 409, load succeeds
    // - Stage-Create => Next stage-create works & overwrites
    // - Stage-Create => Next regular create works & overwrites

    use super::*;

    use crate::api::iceberg::types::PageToken;
    use crate::api::management::v1::warehouse::WarehouseStatus;
    use crate::implementations::postgres::namespace::tests::initialize_namespace;
    use crate::implementations::postgres::warehouse::set_warehouse_status;
    use crate::implementations::postgres::warehouse::test::initialize_warehouse;
    use crate::service::{ListFlags, NamespaceIdentUuid, Transaction};

    use crate::catalog::tables::create_table_request_into_table_metadata;
    use crate::implementations::postgres::tabular::mark_tabular_as_deleted;
    use crate::implementations::postgres::PostgresTransaction;
    use iceberg::spec::{
        NestedField, PrimitiveType, Schema, TableMetadataBuilder, UnboundPartitionSpec,
    };
    use iceberg::NamespaceIdent;
    use iceberg_ext::catalog::rest::CreateTableRequest;
    use iceberg_ext::configs::Location;
    use uuid::Uuid;

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

        let table_metadata =
            crate::catalog::tables::create_table_request_into_table_metadata(table_id, request)
                .unwrap();
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
        request.table_metadata.location = location.to_string();
        request.table_metadata.table_uuid = Uuid::now_v7();
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
        request.table_metadata.table_uuid = Uuid::now_v7();
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
        assert_eq!(
            load_result.get(&table_id).unwrap().table_metadata,
            create_result.table_metadata
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

        let exists = table_ident_to_id(
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
        let exists = table_ident_to_id(
            warehouse_id,
            &table.table_ident,
            ListFlags::default(),
            &state.read_pool(),
        )
        .await
        .unwrap();
        assert!(exists.is_none());

        let exists = table_ident_to_id(
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
        assert_eq!(exists, Some(table.table_id));
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

        let exists = table_ident_to_id(
            warehouse_id,
            &table.table_ident,
            ListFlags::default(),
            &state.read_pool(),
        )
        .await
        .unwrap();
        assert!(exists.is_none());

        let exists = table_ident_to_id(
            warehouse_id,
            &new_table_ident,
            ListFlags::default(),
            &state.read_pool(),
        )
        .await
        .unwrap();
        // Table id should be the same
        assert_eq!(exists, Some(table.table_id));
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

        let exists = table_ident_to_id(
            warehouse_id,
            &table.table_ident,
            ListFlags::default(),
            &state.read_pool(),
        )
        .await
        .unwrap();
        assert!(exists.is_none());

        let exists = table_ident_to_id(
            warehouse_id,
            &new_table_ident,
            ListFlags::default(),
            &state.read_pool(),
        )
        .await
        .unwrap();
        assert_eq!(exists, Some(table.table_id));
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
    async fn test_commit_transaction(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let warehouse_id = initialize_warehouse(state.clone(), None, None, None, true).await;
        let table1 = initialize_table(warehouse_id, state.clone(), true, None, None).await;
        let table2 = initialize_table(warehouse_id, state.clone(), false, None, None).await;
        let _ = initialize_table(warehouse_id, state.clone(), false, None, None).await;

        let loaded_tables = load_tables(
            warehouse_id,
            vec![table1.table_id, table2.table_id],
            false,
            &mut pool.begin().await.unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(loaded_tables.len(), 2);
        assert!(loaded_tables
            .get(&table1.table_id)
            .unwrap()
            .metadata_location
            .is_none());
        assert!(loaded_tables
            .get(&table2.table_id)
            .unwrap()
            .metadata_location
            .is_some());

        let table1_metadata = &loaded_tables.get(&table1.table_id).unwrap().table_metadata;
        let table2_metadata = &loaded_tables.get(&table2.table_id).unwrap().table_metadata;

        let builder1 = TableMetadataBuilder::new_from_metadata(
            table1_metadata.clone(),
            Some("s3://my_bucket/table1/metadata/foo".to_string()),
        )
        .set_properties(HashMap::from_iter(vec![(
            "t1_key".to_string(),
            "t1_value".to_string(),
        )]))
        .unwrap();
        let builder2 = TableMetadataBuilder::new_from_metadata(
            table2_metadata.clone(),
            Some("s3://my_bucket/table2/metadata/foo".to_string()),
        )
        .set_properties(HashMap::from_iter(vec![(
            "t2_key".to_string(),
            "t2_value".to_string(),
        )]))
        .unwrap();
        let updated_metadata1 = builder1.build().unwrap().metadata;
        let updated_metadata2 = builder2.build().unwrap().metadata;

        let commits = vec![
            TableCommit {
                new_metadata: updated_metadata1.clone(),
                new_metadata_location: Location::from_str("s3://my_bucket/table1/metadata/foo")
                    .unwrap(),
            },
            TableCommit {
                new_metadata: updated_metadata2.clone(),
                new_metadata_location: Location::from_str("s3://my_bucket/table2/metadata/foo")
                    .unwrap(),
            },
        ];

        let mut transaction = pool.begin().await.unwrap();
        commit_table_transaction(warehouse_id, commits.clone(), &mut transaction)
            .await
            .unwrap();
        transaction.commit().await.unwrap();

        let loaded_tables = load_tables(
            warehouse_id,
            vec![table1.table_id, table2.table_id],
            false,
            &mut pool.begin().await.unwrap(),
        )
        .await
        .unwrap();

        assert_eq!(loaded_tables.len(), 2);

        let loaded_metadata1 = &loaded_tables.get(&table1.table_id).unwrap().table_metadata;
        let loaded_metadata2 = &loaded_tables.get(&table2.table_id).unwrap().table_metadata;

        assert_eq!(loaded_metadata1, &updated_metadata1);
        assert_eq!(loaded_metadata2, &updated_metadata2);
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
        mark_tabular_as_deleted(TabularIdentUuid::Table(*table.table_id), &mut transaction)
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

    #[sqlx::test]
    async fn test_get_by_id_2(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let warehouse_id = initialize_warehouse(state.clone(), None, None, None, true).await;
        let table = initialize_table(warehouse_id, state.clone(), false, None, None).await;
        let mut transaction = PostgresTransaction::begin_read(state).await.unwrap();
        let tt1 = load_tables(
            warehouse_id,
            [table.table_id],
            false,
            transaction.transaction(),
        )
        .await
        .unwrap();

        let tt2 = load_tables_2(
            warehouse_id,
            [table.table_id],
            false,
            transaction.transaction(),
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();

        assert_eq!(tt1, tt2);
    }
}
