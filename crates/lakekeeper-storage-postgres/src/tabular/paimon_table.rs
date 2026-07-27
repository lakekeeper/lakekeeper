use std::{collections::HashMap, str::FromStr as _};

use iceberg::TableIdent;
use lakekeeper::{
    CONFIG, WarehouseId,
    service::{
        CatalogBackendError, CreatePaimonTableError, CreateTabularError, DropPaimonTableError,
        ListPaimonTablesError, LoadPaimonTableError, LogicalSchema, PaimonCommitState,
        PaimonCommitStateUpdate, PaimonTableAlreadyExists, PaimonTableCreation, PaimonTableInfo,
        PaimonTableListEntry, PaimonTableNotFound, TableId, UpdatePaimonCommitStateError,
        storage::join_location,
    },
};
use serde_json::Value;
use sqlx::{FromRow, Row};
use uuid::Uuid;

use super::{
    CreateTabular, TabularType, create_tabular,
    table::{
        SchemaFieldBatch,
        normalized_schema::{self, SchemaFieldRow},
    },
};
use crate::{
    dbutils::DBErrorHandler as _,
    namespace::parse_namespace_identifier_from_vec,
    pagination::{PaginateToken, V1PaginateToken},
};

#[derive(Debug, sqlx::Type, Copy, Clone, PartialEq, Eq)]
#[sqlx(type_name = "paimon_commit_state", rename_all = "kebab-case")]
enum PaimonCommitStateRow {
    Stable,
    PendingPublish,
    PublishFailed,
}

impl From<PaimonCommitStateRow> for PaimonCommitState {
    fn from(value: PaimonCommitStateRow) -> Self {
        match value {
            PaimonCommitStateRow::Stable => PaimonCommitState::Stable,
            PaimonCommitStateRow::PendingPublish => PaimonCommitState::PendingPublish,
            PaimonCommitStateRow::PublishFailed => PaimonCommitState::PublishFailed,
        }
    }
}

impl From<PaimonCommitState> for PaimonCommitStateRow {
    fn from(value: PaimonCommitState) -> Self {
        match value {
            PaimonCommitState::Stable => PaimonCommitStateRow::Stable,
            PaimonCommitState::PendingPublish => PaimonCommitStateRow::PendingPublish,
            PaimonCommitState::PublishFailed => PaimonCommitStateRow::PublishFailed,
        }
    }
}

#[derive(Debug, FromRow)]
struct PaimonTableFullRow {
    tabular_id: Uuid,
    warehouse_version: i64,
    namespace_id: Uuid,
    namespace_version: i64,
    namespace_name: Vec<String>,
    name: String,
    fs_location: String,
    fs_protocol: String,
    shared_metadata_location: Option<String>,
    protected: bool,
    current_snapshot_id: Option<i64>,
    metadata_location: Option<String>,
    current_branch: String,
    table_options: Value,
    schema_id: Option<i32>,
    partition_keys: Vec<String>,
    primary_keys: Vec<String>,
    comment: Option<String>,
    #[sqlx(rename = "commit_state: PaimonCommitStateRow")]
    commit_state: PaimonCommitStateRow,
    pending_commit_token: Option<Uuid>,
    last_commit_error: Option<String>,
    updated_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, FromRow)]
struct PaimonTableListRow {
    tabular_id: Uuid,
    warehouse_id: Uuid,
    namespace_id: Uuid,
    name: String,
    #[sqlx(rename = "commit_state: PaimonCommitStateRow")]
    commit_state: PaimonCommitStateRow,
    protected: bool,
    created_at: chrono::DateTime<chrono::Utc>,
}

fn parse_options(value: Value) -> Result<HashMap<String, String>, CatalogBackendError> {
    serde_json::from_value(value)
        .map_err(|e| CatalogBackendError::new_unexpected(e).append_detail("Invalid table_options"))
}

async fn load_schema(
    warehouse_id: WarehouseId,
    tabular_id: TableId,
    schema_id: i32,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<LogicalSchema, CatalogBackendError> {
    let rows = sqlx::query_as::<_, SchemaFieldRow>(
        r#"SELECT schema_id,
                  field_id,
                  parent_field_id,
                  ordinal,
                  name,
                  required,
                  doc,
                  type_kind::text AS type_kind,
                  type_params,
                  initial_default,
                  write_default,
                  is_identifier
           FROM schema_field
           WHERE warehouse_id = $1 AND tabular_id = $2 AND schema_id = $3
           ORDER BY ordinal, field_id"#,
    )
    .bind(*warehouse_id)
    .bind(*tabular_id)
    .bind(schema_id)
    .fetch_all(&mut **transaction)
    .await
    .map_err(|e| {
        e.into_catalog_backend_error()
            .append_detail("Failed to load Paimon schema")
    })?;

    let mut schemas =
        normalized_schema::assemble_logical_schemas(rows, &[schema_id]).map_err(|e| {
            CatalogBackendError::new_unexpected(e).append_detail("Failed to assemble Paimon schema")
        })?;
    schemas.remove(&schema_id).ok_or_else(|| {
        CatalogBackendError::new_unexpected(std::io::Error::other(
            "Missing assembled Paimon schema",
        ))
    })
}

fn full_row_to_info(
    row: PaimonTableFullRow,
    warehouse_id: WarehouseId,
    schema: Option<LogicalSchema>,
) -> Result<PaimonTableInfo, CatalogBackendError> {
    let namespace_ident = parse_namespace_identifier_from_vec(
        &row.namespace_name,
        warehouse_id,
        Some(row.namespace_id),
    )
    .map_err(CatalogBackendError::new_unexpected)?;
    let location = join_location(&row.fs_protocol, &row.fs_location)
        .map_err(CatalogBackendError::new_unexpected)?;
    let metadata_location = row
        .metadata_location
        .or(row.shared_metadata_location)
        .map(|s| lakekeeper_io::Location::from_str(&s))
        .transpose()
        .map_err(CatalogBackendError::new_unexpected)?;
    let name = row.name;
    let tabular_ident = TableIdent {
        namespace: namespace_ident.clone(),
        name: name.clone(),
    };

    Ok(PaimonTableInfo {
        tabular_id: row.tabular_id.into(),
        warehouse_id,
        warehouse_version: row.warehouse_version.into(),
        namespace_id: row.namespace_id.into(),
        namespace_version: row.namespace_version.into(),
        namespace_ident,
        name,
        tabular_ident,
        location,
        metadata_location,
        protected: row.protected,
        current_snapshot_id: row.current_snapshot_id,
        current_branch: row.current_branch,
        schema,
        table_options: parse_options(row.table_options)?,
        partition_keys: row.partition_keys,
        primary_keys: row.primary_keys,
        comment: row.comment,
        commit_state: row.commit_state.into(),
        pending_commit_token: row.pending_commit_token,
        last_commit_error: row.last_commit_error,
        updated_at: row.updated_at,
    })
}

pub(crate) async fn create_paimon_table(
    creation: PaimonTableCreation,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<PaimonTableInfo, CreatePaimonTableError> {
    let tabular_id: Uuid = *creation.tabular_id;
    let metadata_location = creation.metadata_location.as_ref();

    create_tabular(
        CreateTabular {
            id: tabular_id,
            name: &creation.name,
            namespace_id: *creation.namespace_id,
            warehouse_id: *creation.warehouse_id,
            typ: TabularType::PaimonTable,
            metadata_location,
            location: &creation.location,
        },
        transaction,
    )
    .await
    .map_err(|e| match e {
        CreateTabularError::TabularAlreadyExists(_) => {
            CreatePaimonTableError::from(PaimonTableAlreadyExists::new())
        }
        CreateTabularError::CatalogBackendError(e) => CreatePaimonTableError::from(e),
        CreateTabularError::InternalParseLocationError(e) => CreatePaimonTableError::from(e),
        CreateTabularError::LocationAlreadyTaken(e) => CreatePaimonTableError::from(e),
        CreateTabularError::InvalidNamespaceIdentifier(e) => CreatePaimonTableError::from(e),
    })?;

    if let Some(schema) = &creation.schema {
        let flat = normalized_schema::flatten_logical_schema(schema)
            .map_err(|e| CreatePaimonTableError::from(CatalogBackendError::new_unexpected(e)))?;
        let mut batch = SchemaFieldBatch::default();
        batch.push_schema(*creation.warehouse_id, tabular_id, schema.schema_id, &flat);
        batch
            .flush(transaction)
            .await
            .map_err(CreatePaimonTableError::from)?;
    }

    sqlx::query(
        r#"INSERT INTO paimon_table
              (warehouse_id, tabular_id, current_snapshot_id, metadata_location, current_branch,
               table_options, schema_id, partition_keys, primary_keys, comment)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)"#,
    )
    .bind(*creation.warehouse_id)
    .bind(tabular_id)
    .bind(creation.current_snapshot_id)
    .bind(metadata_location.map(lakekeeper_io::Location::as_str))
    .bind(&creation.current_branch)
    .bind(
        serde_json::to_value(&creation.table_options)
            .map_err(|e| CreatePaimonTableError::from(CatalogBackendError::new_unexpected(e)))?,
    )
    .bind(creation.schema.as_ref().map(|schema| schema.schema_id))
    .bind(&creation.partition_keys)
    .bind(&creation.primary_keys)
    .bind(creation.comment.as_deref())
    .execute(&mut **transaction)
    .await
    .map_err(|e| CreatePaimonTableError::from(e.into_catalog_backend_error()))?;

    load_paimon_table_by_id(creation.warehouse_id, creation.tabular_id, transaction)
        .await
        .map_err(|e| match e {
            LoadPaimonTableError::PaimonTableNotFound(_) => {
                CreatePaimonTableError::from(CatalogBackendError::new_unexpected(
                    std::io::Error::other("Created Paimon table could not be reloaded"),
                ))
            }
            LoadPaimonTableError::CatalogBackendError(e) => CreatePaimonTableError::from(e),
        })
}

pub(crate) async fn load_paimon_table(
    warehouse_id: WarehouseId,
    namespace_id: lakekeeper::service::NamespaceId,
    table_name: &str,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<PaimonTableInfo, LoadPaimonTableError> {
    let row = sqlx::query_as::<_, PaimonTableFullRow>(
        r#"SELECT
                t.tabular_id,
                w.version AS warehouse_version,
                t.namespace_id,
                n.version AS namespace_version,
                t.tabular_namespace_name AS namespace_name,
                t.name,
                t.fs_location,
                t.fs_protocol,
                t.metadata_location AS shared_metadata_location,
                t.protected,
                pt.current_snapshot_id,
                pt.metadata_location,
                pt.current_branch,
                pt.table_options,
                pt.schema_id,
                pt.partition_keys,
                pt.primary_keys,
                pt.comment,
                pt.commit_state AS "commit_state: PaimonCommitStateRow",
                pt.pending_commit_token,
                pt.last_commit_error,
                pt.updated_at
           FROM tabular t
           INNER JOIN paimon_table pt
                   ON pt.warehouse_id = t.warehouse_id AND pt.tabular_id = t.tabular_id
           INNER JOIN warehouse w
                   ON w.warehouse_id = t.warehouse_id AND w.status = 'active'
           INNER JOIN namespace n
                   ON n.namespace_id = t.namespace_id AND n.warehouse_id = t.warehouse_id
           WHERE t.warehouse_id = $1
             AND t.namespace_id = $2
             AND t.name = $3
             AND t.typ = 'paimon-table'
             AND t.deleted_at IS NULL"#,
    )
    .bind(*warehouse_id)
    .bind(*namespace_id)
    .bind(table_name)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|e| LoadPaimonTableError::from(e.into_catalog_backend_error()))?
    .ok_or_else(|| LoadPaimonTableError::from(PaimonTableNotFound::new()))?;

    let schema = match row.schema_id {
        Some(schema_id) => {
            Some(load_schema(warehouse_id, row.tabular_id.into(), schema_id, transaction).await?)
        }
        None => None,
    };
    full_row_to_info(row, warehouse_id, schema).map_err(LoadPaimonTableError::from)
}

pub(crate) async fn load_paimon_table_by_id(
    warehouse_id: WarehouseId,
    tabular_id: TableId,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<PaimonTableInfo, LoadPaimonTableError> {
    let row = sqlx::query_as::<_, PaimonTableFullRow>(
        r#"SELECT
                t.tabular_id,
                w.version AS warehouse_version,
                t.namespace_id,
                n.version AS namespace_version,
                t.tabular_namespace_name AS namespace_name,
                t.name,
                t.fs_location,
                t.fs_protocol,
                t.metadata_location AS shared_metadata_location,
                t.protected,
                pt.current_snapshot_id,
                pt.metadata_location,
                pt.current_branch,
                pt.table_options,
                pt.schema_id,
                pt.partition_keys,
                pt.primary_keys,
                pt.comment,
                pt.commit_state AS "commit_state: PaimonCommitStateRow",
                pt.pending_commit_token,
                pt.last_commit_error,
                pt.updated_at
           FROM tabular t
           INNER JOIN paimon_table pt
                   ON pt.warehouse_id = t.warehouse_id AND pt.tabular_id = t.tabular_id
           INNER JOIN warehouse w
                   ON w.warehouse_id = t.warehouse_id AND w.status = 'active'
           INNER JOIN namespace n
                   ON n.namespace_id = t.namespace_id AND n.warehouse_id = t.warehouse_id
           WHERE t.warehouse_id = $1
             AND t.tabular_id = $2
             AND t.typ = 'paimon-table'
             AND t.deleted_at IS NULL"#,
    )
    .bind(*warehouse_id)
    .bind(*tabular_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|e| LoadPaimonTableError::from(e.into_catalog_backend_error()))?
    .ok_or_else(|| LoadPaimonTableError::from(PaimonTableNotFound::new()))?;
    let schema = match row.schema_id {
        Some(schema_id) => {
            Some(load_schema(warehouse_id, tabular_id, schema_id, transaction).await?)
        }
        None => None,
    };
    full_row_to_info(row, warehouse_id, schema).map_err(LoadPaimonTableError::from)
}

pub(crate) async fn list_paimon_tables(
    warehouse_id: WarehouseId,
    namespace_id: lakekeeper::service::NamespaceId,
    namespace_ident: &iceberg::NamespaceIdent,
    page_size: Option<i64>,
    page_token: Option<&str>,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<(Vec<PaimonTableListEntry>, Option<String>), ListPaimonTablesError> {
    let page_size = CONFIG.page_size_or_pagination_default(page_size);
    let token = page_token
        .map(PaginateToken::<Uuid>::try_from)
        .transpose()
        .map_err(|e| ListPaimonTablesError::from(CatalogBackendError::new_unexpected(e)))?;
    let (token_ts, token_id) = token
        .as_ref()
        .map(
            |PaginateToken::V1(V1PaginateToken { created_at, id }): &PaginateToken<Uuid>| {
                (Some(*created_at), Some(*id))
            },
        )
        .unwrap_or((None, None));

    let rows = sqlx::query_as::<_, PaimonTableListRow>(
        r#"SELECT
                t.tabular_id,
                t.warehouse_id,
                t.namespace_id,
                t.name,
                pt.commit_state AS "commit_state: PaimonCommitStateRow",
                t.protected,
                t.created_at
           FROM tabular t
           INNER JOIN paimon_table pt
                   ON pt.warehouse_id = t.warehouse_id AND pt.tabular_id = t.tabular_id
           INNER JOIN warehouse w
                   ON w.warehouse_id = t.warehouse_id AND w.status = 'active'
           WHERE t.warehouse_id = $1
             AND t.namespace_id = $2
             AND t.typ = 'paimon-table'
             AND t.deleted_at IS NULL
             AND (($3::timestamptz IS NULL) OR (t.created_at, t.tabular_id) > ($3, $4))
           ORDER BY t.created_at ASC, t.tabular_id ASC
           LIMIT $5"#,
    )
    .bind(*warehouse_id)
    .bind(*namespace_id)
    .bind(token_ts)
    .bind(token_id)
    .bind(page_size)
    .fetch_all(&mut **transaction)
    .await
    .map_err(|e| ListPaimonTablesError::from(e.into_catalog_backend_error()))?;

    let next_page_token = rows.last().map(|row| {
        PaginateToken::V1(V1PaginateToken {
            created_at: row.created_at,
            id: row.tabular_id,
        })
        .to_string()
    });

    let entries = rows
        .into_iter()
        .map(|row| {
            let tabular_ident = TableIdent::new(namespace_ident.clone(), row.name.clone());
            PaimonTableListEntry {
                tabular_id: row.tabular_id.into(),
                warehouse_id: row.warehouse_id.into(),
                namespace_id: row.namespace_id.into(),
                namespace_ident: namespace_ident.clone(),
                name: row.name,
                tabular_ident,
                commit_state: row.commit_state.into(),
                protected: row.protected,
                created_at: row.created_at,
            }
        })
        .collect();

    Ok((entries, next_page_token))
}

pub(crate) async fn update_paimon_commit_state(
    warehouse_id: WarehouseId,
    tabular_id: TableId,
    update: PaimonCommitStateUpdate,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<PaimonTableInfo, UpdatePaimonCommitStateError> {
    let metadata_location = update
        .metadata_location
        .as_ref()
        .map(lakekeeper_io::Location::as_str);
    let state: PaimonCommitStateRow = update.commit_state.into();

    let updated = sqlx::query(
        r#"UPDATE paimon_table
              SET commit_state = $3,
                  pending_commit_token = $4,
                  current_snapshot_id = $5,
                  metadata_location = COALESCE($6, metadata_location),
                  last_commit_error = $7
            WHERE warehouse_id = $1 AND tabular_id = $2"#,
    )
    .bind(*warehouse_id)
    .bind(*tabular_id)
    .bind(state)
    .bind(update.pending_commit_token)
    .bind(update.current_snapshot_id)
    .bind(metadata_location)
    .bind(update.last_commit_error.as_deref())
    .execute(&mut **transaction)
    .await
    .map_err(|e| UpdatePaimonCommitStateError::from(e.into_catalog_backend_error()))?
    .rows_affected();

    if updated == 0 {
        return Err(UpdatePaimonCommitStateError::from(
            PaimonTableNotFound::new(),
        ));
    }

    if let Some(metadata_location) = metadata_location {
        sqlx::query(
            r#"UPDATE tabular
                  SET metadata_location = $3
                WHERE warehouse_id = $1 AND tabular_id = $2"#,
        )
        .bind(*warehouse_id)
        .bind(*tabular_id)
        .bind(metadata_location)
        .execute(&mut **transaction)
        .await
        .map_err(|e| UpdatePaimonCommitStateError::from(e.into_catalog_backend_error()))?;
    }

    load_paimon_table_by_id(warehouse_id, tabular_id, transaction)
        .await
        .map_err(|e| match e {
            LoadPaimonTableError::PaimonTableNotFound(_) => {
                UpdatePaimonCommitStateError::from(PaimonTableNotFound::new())
            }
            LoadPaimonTableError::CatalogBackendError(e) => UpdatePaimonCommitStateError::from(e),
        })
}

pub(crate) async fn drop_paimon_table(
    warehouse_id: WarehouseId,
    namespace_id: lakekeeper::service::NamespaceId,
    table_name: &str,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<TableId, DropPaimonTableError> {
    let row = sqlx::query(
        r#"SELECT t.tabular_id
           FROM tabular t
           INNER JOIN warehouse w ON w.warehouse_id = t.warehouse_id AND w.status = 'active'
           WHERE t.warehouse_id = $1
             AND t.namespace_id = $2
             AND t.name = $3
             AND t.typ = 'paimon-table'
             AND t.deleted_at IS NULL"#,
    )
    .bind(*warehouse_id)
    .bind(*namespace_id)
    .bind(table_name)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|e| DropPaimonTableError::from(e.into_catalog_backend_error()))?
    .ok_or_else(|| DropPaimonTableError::from(PaimonTableNotFound::new()))?;

    let tabular_id: TableId = row
        .try_get::<Uuid, _>("tabular_id")
        .map_err(|e| DropPaimonTableError::from(CatalogBackendError::new_unexpected(e)))?
        .into();

    let row = sqlx::query(
        r#"
        WITH locked_tabular AS (
            SELECT protected, tabular_id
            FROM tabular
            WHERE tabular_id = $2
              AND warehouse_id = $1
              AND typ = $3
              AND tabular_id IN (
                  SELECT tabular_id FROM active_tabulars
                  WHERE warehouse_id = $1 AND tabular_id = $2
              )
            FOR UPDATE
        ),
        deleted AS (
            DELETE FROM tabular
            WHERE tabular_id IN (
                SELECT tabular_id FROM locked_tabular
                WHERE NOT protected
            )
            AND warehouse_id = $1
            RETURNING tabular_id
        )
        SELECT
            lt.protected,
            (SELECT tabular_id FROM deleted) IS NOT NULL AS was_deleted
        FROM locked_tabular lt"#,
    )
    .bind(*warehouse_id)
    .bind(*tabular_id)
    .bind(TabularType::PaimonTable)
    .fetch_one(&mut **transaction)
    .await
    .map_err(|e| {
        if let sqlx::Error::RowNotFound = e {
            DropPaimonTableError::from(PaimonTableNotFound::new())
        } else {
            DropPaimonTableError::from(e.into_catalog_backend_error())
        }
    })?;

    let protected = row
        .try_get::<bool, _>("protected")
        .map_err(|e| DropPaimonTableError::from(CatalogBackendError::new_unexpected(e)))?;
    if protected {
        return Err(DropPaimonTableError::from(
            lakekeeper::service::ProtectedTabularDeletionWithoutForce::new(
                warehouse_id,
                lakekeeper::service::TabularId::Table(tabular_id),
            ),
        ));
    }

    Ok(tabular_id)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use iceberg::NamespaceIdent;
    use lakekeeper::service::{CatalogKind, NamespaceId};
    use sqlx::PgPool;

    use super::*;
    use crate::{
        CatalogState, namespace::tests::initialize_namespace,
        warehouse::test::initialize_warehouse_with_catalog_kind,
    };

    async fn setup(pool: PgPool) -> (CatalogState, WarehouseId, NamespaceId, NamespaceIdent) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_project_id, warehouse_id) = initialize_warehouse_with_catalog_kind(
            state.clone(),
            None,
            None,
            None,
            true,
            Some(CatalogKind::Paimon),
        )
        .await;
        let namespace = NamespaceIdent::new(Uuid::now_v7().to_string());
        let ns = initialize_namespace(state.clone(), warehouse_id, &namespace, None).await;
        (state, warehouse_id, ns.namespace_id(), namespace)
    }

    fn nested_schema() -> LogicalSchema {
        LogicalSchema {
            schema_id: 7,
            root_fields: vec![lakekeeper::service::LogicalField {
                field_id: 1,
                name: "user".to_string(),
                required: true,
                doc: None,
                field_type: lakekeeper::service::LogicalType::Struct {
                    fields: vec![lakekeeper::service::LogicalField {
                        field_id: 2,
                        name: "tags".to_string(),
                        required: false,
                        doc: None,
                        field_type: lakekeeper::service::LogicalType::List {
                            element_field: Box::new(lakekeeper::service::LogicalField {
                                field_id: 3,
                                name: "element".to_string(),
                                required: true,
                                doc: None,
                                field_type: lakekeeper::service::LogicalType::Primitive(
                                    lakekeeper::service::LogicalPrimitiveType::String,
                                ),
                                initial_default: None,
                                write_default: None,
                                is_identity_hint: false,
                            }),
                        },
                        initial_default: None,
                        write_default: None,
                        is_identity_hint: false,
                    }],
                },
                initial_default: None,
                write_default: None,
                is_identity_hint: true,
            }],
        }
    }

    fn test_creation(
        warehouse_id: WarehouseId,
        namespace_id: NamespaceId,
        name: &str,
    ) -> PaimonTableCreation {
        PaimonTableCreation {
            tabular_id: TableId::new_random(),
            warehouse_id,
            namespace_id,
            name: name.to_string(),
            location: format!("s3://bucket/{name}/").parse().unwrap(),
            metadata_location: Some(
                format!("s3://bucket/{name}/metadata/1.json")
                    .parse()
                    .unwrap(),
            ),
            current_snapshot_id: Some(42),
            current_branch: "main".to_string(),
            schema: Some(nested_schema()),
            table_options: HashMap::from([("bucket".to_string(), "4".to_string())]),
            partition_keys: vec!["dt".to_string()],
            primary_keys: vec!["id".to_string()],
            comment: Some("hello".to_string()),
        }
    }

    #[sqlx::test]
    async fn test_create_load_list_update_drop(pool: PgPool) {
        let (_state, warehouse_id, namespace_id, namespace_ident) = setup(pool.clone()).await;
        let creation = test_creation(warehouse_id, namespace_id, "orders");

        let mut tx = pool.begin().await.unwrap();
        let created = create_paimon_table(creation.clone(), &mut tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();

        assert_eq!(created.commit_state, PaimonCommitState::Stable);
        assert_eq!(created.schema, creation.schema);

        let mut tx = pool.begin().await.unwrap();
        let loaded = load_paimon_table(warehouse_id, namespace_id, "orders", &mut tx)
            .await
            .unwrap();
        let (listed, next) = list_paimon_tables(
            warehouse_id,
            namespace_id,
            &namespace_ident,
            Some(10),
            None,
            &mut tx,
        )
        .await
        .unwrap();
        let updated = update_paimon_commit_state(
            warehouse_id,
            created.tabular_id,
            PaimonCommitStateUpdate {
                commit_state: PaimonCommitState::PendingPublish,
                pending_commit_token: Some(Uuid::now_v7()),
                current_snapshot_id: Some(43),
                metadata_location: Some("s3://bucket/orders/metadata/2.json".parse().unwrap()),
                last_commit_error: None,
            },
            &mut tx,
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();

        assert_eq!(loaded.schema, creation.schema);
        assert_eq!(listed.len(), 1);
        assert!(next.is_some());
        assert_eq!(updated.commit_state, PaimonCommitState::PendingPublish);
        assert_eq!(updated.current_snapshot_id, Some(43));

        let mut tx = pool.begin().await.unwrap();
        let dropped = drop_paimon_table(warehouse_id, namespace_id, "orders", &mut tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        assert_eq!(dropped, created.tabular_id);

        let mut tx = pool.begin().await.unwrap();
        let err = load_paimon_table_by_id(warehouse_id, dropped, &mut tx)
            .await
            .unwrap_err();
        assert!(matches!(err, LoadPaimonTableError::PaimonTableNotFound(_)));
    }

    #[sqlx::test]
    async fn test_drop_cascades_schema(pool: PgPool) {
        let (_state, warehouse_id, namespace_id, _namespace_ident) = setup(pool.clone()).await;
        let creation = test_creation(warehouse_id, namespace_id, "events");

        let mut tx = pool.begin().await.unwrap();
        let created = create_paimon_table(creation, &mut tx).await.unwrap();
        tx.commit().await.unwrap();

        let before: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM schema_field WHERE warehouse_id = $1 AND tabular_id = $2",
        )
        .bind(*warehouse_id)
        .bind(*created.tabular_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(before > 0);

        let mut tx = pool.begin().await.unwrap();
        drop_paimon_table(warehouse_id, namespace_id, "events", &mut tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();

        let after: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM schema_field WHERE warehouse_id = $1 AND tabular_id = $2",
        )
        .bind(*warehouse_id)
        .bind(*created.tabular_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(after, 0);
    }
}
