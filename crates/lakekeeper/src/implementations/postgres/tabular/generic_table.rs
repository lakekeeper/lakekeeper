use std::collections::HashMap;

use uuid::Uuid;

use super::super::dbutils::DBErrorHandler as _;
use crate::{
    CONFIG, WarehouseId,
    implementations::postgres::{
        namespace::parse_namespace_identifier_from_vec,
        pagination::{PaginateToken, V1PaginateToken},
    },
    service::{
        CatalogBackendError, CreateGenericTableError, DropGenericTableError,
        GenericTableAlreadyExists, GenericTableCreation, GenericTableId, GenericTableInfo,
        GenericTableListEntry, GenericTableNotFound, ListGenericTablesError, LoadGenericTableError,
        NamespaceId, NamespaceVersion, WarehouseVersion,
    },
};

struct GenericTableRow {
    generic_table_id: Uuid,
    warehouse_id: Uuid,
    warehouse_version: i64,
    namespace_id: Uuid,
    namespace_version: i64,
    namespace_name: Vec<String>,
    name: String,
    format: String,
    base_location: String,
    doc: Option<String>,
    schema_info: Option<serde_json::Value>,
    statistics: Option<serde_json::Value>,
    properties: serde_json::Value,
}

struct GenericTableListRow {
    generic_table_id: Uuid,
    name: String,
    format: String,
    created_at: chrono::DateTime<chrono::Utc>,
}

struct GenericTableDropRow {
    generic_table_id: Uuid,
}

fn row_to_info(row: GenericTableRow) -> Result<GenericTableInfo, CatalogBackendError> {
    let namespace_ident = parse_namespace_identifier_from_vec(
        &row.namespace_name,
        row.warehouse_id.into(),
        Some(row.namespace_id),
    )
    .map_err(CatalogBackendError::new_unexpected)?;

    let properties: HashMap<String, String> =
        serde_json::from_value(row.properties).map_err(CatalogBackendError::new_unexpected)?;

    Ok(GenericTableInfo {
        generic_table_id: row.generic_table_id.into(),
        warehouse_id: row.warehouse_id.into(),
        warehouse_version: WarehouseVersion::new(row.warehouse_version),
        namespace_id: row.namespace_id.into(),
        namespace_version: NamespaceVersion::new(row.namespace_version),
        namespace_ident,
        name: row.name,
        format: row.format,
        base_location: row.base_location,
        doc: row.doc,
        schema: row.schema_info,
        statistics: row.statistics,
        properties,
    })
}

pub(crate) async fn create_generic_table(
    creation: GenericTableCreation,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<GenericTableInfo, CreateGenericTableError> {
    let properties_json = serde_json::to_value(&creation.properties)
        .map_err(|e| CreateGenericTableError::from(CatalogBackendError::new_unexpected(e)))?;

    let row = sqlx::query_as!(
        GenericTableRow,
        r#"
        WITH inserted AS (
            INSERT INTO generic_table (namespace_id, warehouse_id, name, format, base_location, doc, schema_info, statistics, properties)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            RETURNING generic_table_id, warehouse_id, namespace_id, name, format, base_location, doc, schema_info, statistics, properties
        )
        SELECT
            i.generic_table_id,
            i.warehouse_id,
            w.version as "warehouse_version!",
            i.namespace_id,
            n.version as "namespace_version!",
            n.namespace_name as "namespace_name!",
            i.name,
            i.format,
            i.base_location,
            i.doc,
            i.schema_info,
            i.statistics,
            i.properties
        FROM inserted i
        INNER JOIN warehouse w ON w.warehouse_id = i.warehouse_id
        INNER JOIN namespace n ON n.namespace_id = i.namespace_id AND n.warehouse_id = i.warehouse_id
        "#,
        *creation.namespace_id,
        *creation.warehouse_id,
        &creation.name,
        &creation.format,
        &creation.base_location,
        creation.doc.as_deref(),
        creation.schema as Option<serde_json::Value>,
        creation.statistics as Option<serde_json::Value>,
        &properties_json,
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|e| match &e {
        sqlx::Error::Database(db_err)
            if db_err.constraint() == Some("unique_generic_table_name_per_namespace") =>
        {
            CreateGenericTableError::from(GenericTableAlreadyExists::new())
        }
        _ => CreateGenericTableError::from(e.into_catalog_backend_error()),
    })?;

    row_to_info(row).map_err(CreateGenericTableError::from)
}

pub(crate) async fn load_generic_table(
    warehouse_id: WarehouseId,
    namespace_id: NamespaceId,
    table_name: &str,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<GenericTableInfo, LoadGenericTableError> {
    let row = sqlx::query_as!(
        GenericTableRow,
        r#"
        SELECT
            g.generic_table_id,
            g.warehouse_id,
            w.version as "warehouse_version!",
            g.namespace_id,
            n.version as "namespace_version!",
            n.namespace_name as "namespace_name!",
            g.name,
            g.format,
            g.base_location,
            g.doc,
            g.schema_info,
            g.statistics,
            g.properties
        FROM generic_table g
        INNER JOIN warehouse w ON w.warehouse_id = g.warehouse_id AND w.status = 'active'
        INNER JOIN namespace n ON n.namespace_id = g.namespace_id AND n.warehouse_id = g.warehouse_id
        WHERE g.warehouse_id = $1 AND g.namespace_id = $2 AND g.name = $3
        "#,
        *warehouse_id,
        *namespace_id,
        table_name,
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|e| LoadGenericTableError::from(e.into_catalog_backend_error()))?
    .ok_or_else(|| LoadGenericTableError::from(GenericTableNotFound::new()))?;

    row_to_info(row).map_err(LoadGenericTableError::from)
}

pub(crate) async fn list_generic_tables(
    warehouse_id: WarehouseId,
    namespace_id: NamespaceId,
    namespace_ident: &iceberg::NamespaceIdent,
    page_size: Option<i64>,
    page_token: Option<&str>,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<(Vec<GenericTableListEntry>, Option<String>), ListGenericTablesError> {
    let page_size = CONFIG.page_size_or_pagination_default(page_size);

    let token = page_token
        .map(PaginateToken::<Uuid>::try_from)
        .transpose()
        .map_err(|e| ListGenericTablesError::from(CatalogBackendError::new_unexpected(e)))?;

    let (token_ts, token_id) = token
        .as_ref()
        .map(
            |PaginateToken::V1(V1PaginateToken { created_at, id }): &PaginateToken<Uuid>| {
                (created_at, id)
            },
        )
        .map_or((None, None), |(ts, id)| (Some(*ts), Some(*id)));

    let rows = sqlx::query_as!(
        GenericTableListRow,
        r#"
        SELECT g.generic_table_id, g.name, g.format, g.created_at
        FROM generic_table g
        INNER JOIN warehouse w ON w.warehouse_id = g.warehouse_id AND w.status = 'active'
        WHERE g.warehouse_id = $1 AND g.namespace_id = $2
          AND (
            ($3::timestamptz IS NULL)
            OR (g.created_at, g.generic_table_id) > ($3, $4)
          )
        ORDER BY g.created_at ASC, g.generic_table_id ASC
        LIMIT $5
        "#,
        *warehouse_id,
        *namespace_id,
        token_ts,
        token_id,
        page_size,
    )
    .fetch_all(&mut **transaction)
    .await
    .map_err(|e| ListGenericTablesError::from(e.into_catalog_backend_error()))?;

    let mut entries = Vec::with_capacity(rows.len());
    let mut next_page_token = None;

    for row in &rows {
        next_page_token = Some(
            PaginateToken::V1(V1PaginateToken {
                created_at: row.created_at,
                id: row.generic_table_id,
            })
            .to_string(),
        );

        entries.push(GenericTableListEntry {
            generic_table_id: row.generic_table_id.into(),
            name: row.name.clone(),
            format: row.format.clone(),
            namespace_ident: namespace_ident.clone(),
            created_at: row.created_at,
        });
    }

    Ok((entries, next_page_token))
}

pub(crate) async fn drop_generic_table(
    warehouse_id: WarehouseId,
    namespace_id: NamespaceId,
    table_name: &str,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<GenericTableId, DropGenericTableError> {
    let row = sqlx::query_as!(
        GenericTableDropRow,
        r#"
        DELETE FROM generic_table
        WHERE warehouse_id = $1 AND namespace_id = $2 AND name = $3
          AND EXISTS (SELECT 1 FROM warehouse w WHERE w.warehouse_id = $1 AND w.status = 'active')
        RETURNING generic_table_id
        "#,
        *warehouse_id,
        *namespace_id,
        table_name,
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|e| DropGenericTableError::from(e.into_catalog_backend_error()))?
    .ok_or_else(|| DropGenericTableError::from(GenericTableNotFound::new()))?;

    Ok(row.generic_table_id.into())
}
