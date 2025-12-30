use std::{collections::HashMap, sync::Arc};

use iceberg::TableIdent;
use itertools::izip;
use sqlx::types::Json;
use uuid::Uuid;

use super::dbutils::DBErrorHandler;
use crate::{
    CONFIG, WarehouseId,
    api::iceberg::v1::{PaginatedMapping, namespace::NamespaceDropFlags},
    implementations::postgres::{
        pagination::{PaginateToken, V1PaginateToken},
        tabular::TabularType,
    },
    server::namespace::MAX_NAMESPACE_DEPTH,
    service::{
        CatalogCreateNamespaceError, CatalogGetNamespaceError, CatalogListNamespaceError,
        CatalogListNamespacesResponse, CatalogNamespaceDropError,
        CatalogSetNamespaceProtectedError, CatalogUpdateNamespacePropertiesError,
        ChildNamespaceProtected, ChildTabularProtected, CreateNamespaceRequest,
        InternalParseLocationError, InvalidNamespaceIdentifier, ListNamespacesQuery, Namespace,
        NamespaceAlreadyExists, NamespaceDropInfo, NamespaceHasRunningTabularExpirations,
        NamespaceId, NamespaceIdent, NamespaceNotEmpty, NamespaceNotFound,
        NamespacePropertiesSerializationError, NamespaceProtected, NamespaceWithParent, Result,
        SerializationError, TabularId, WarehouseIdNotFound, storage::join_location, tasks::TaskId,
    },
};

#[derive(Debug)]
struct NamespaceRow {
    namespace_id: NamespaceId,
    namespace_name: Vec<String>,
    warehouse_id: WarehouseId,
    protected: bool,
    properties: Json<Option<HashMap<String, String>>>,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: Option<chrono::DateTime<chrono::Utc>>,
    version: i64,
}

impl NamespaceRow {
    fn into_namespace(
        self,
        warehouse_id: WarehouseId,
    ) -> std::result::Result<Namespace, InvalidNamespaceIdentifier> {
        Ok(Namespace {
            namespace_ident: parse_namespace_identifier_from_vec(
                &self.namespace_name,
                warehouse_id,
                Some(self.namespace_id),
            )?,
            protected: self.protected,
            properties: self.properties.0.filter(|p| !p.is_empty()),
            namespace_id: self.namespace_id,
            warehouse_id: self.warehouse_id,
            created_at: self.created_at,
            updated_at: self.updated_at,
            version: self.version.into(),
        })
    }
}

#[derive(Debug)]
struct NamespaceWithParentVersionRow {
    namespace_id: NamespaceId,
    namespace_name: Vec<String>,
    warehouse_id: WarehouseId,
    protected: bool,
    properties: Json<Option<HashMap<String, String>>>,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: Option<chrono::DateTime<chrono::Utc>>,
    version: i64,
    parent_namespace_id: Option<Uuid>,
    parent_version: Option<i64>,
}

impl NamespaceWithParentVersionRow {
    fn into_namespace_with_parent_version(
        self,
        warehouse_id: WarehouseId,
    ) -> std::result::Result<NamespaceWithParent, InvalidNamespaceIdentifier> {
        let parent = if let (Some(parent_id), Some(parent_version)) =
            (self.parent_namespace_id, self.parent_version)
        {
            Some((parent_id.into(), parent_version.into()))
        } else {
            None
        };

        let namespace = NamespaceRow {
            namespace_id: self.namespace_id,
            namespace_name: self.namespace_name,
            warehouse_id: self.warehouse_id,
            protected: self.protected,
            properties: self.properties,
            created_at: self.created_at,
            updated_at: self.updated_at,
            version: self.version,
        }
        .into_namespace(warehouse_id)?;

        Ok(NamespaceWithParent {
            namespace: Arc::new(namespace),
            parent,
        })
    }
}

pub(crate) async fn get_namespaces_by_id<
    'c,
    'e: 'c,
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
>(
    warehouse_id: WarehouseId,
    namespace_ids: &[NamespaceId],
    connection: E,
) -> std::result::Result<Vec<NamespaceWithParent>, CatalogGetNamespaceError> {
    let rows = sqlx::query_as!(
        NamespaceWithParentVersionRow,
        r#"
        with selected_ns as (
            select namespace_name
            from namespace
            where warehouse_id = $1 AND namespace_id = ANY($2)
        ),
        parent_paths as (
            SELECT DISTINCT namespace_name[1:generate_series(1, array_length(namespace_name, 1))] as parent_name
            FROM selected_ns
        ),
        relevant_namespaces AS (
            SELECT 
                n.namespace_id,
                n.namespace_name,
                n.warehouse_id,
                n.protected,
                n.namespace_properties,
                n.created_at,
                n.updated_at,
                n.version
            FROM namespace n
            INNER JOIN warehouse w ON w.warehouse_id = $1
            WHERE n.warehouse_id = $1
            AND w.status = 'active'
            AND n.namespace_name IN (SELECT parent_name FROM parent_paths)
        )
        SELECT
                n.namespace_id,
                n.namespace_name as "namespace_name: Vec<String>",
                n.warehouse_id,
                n.protected,
                n.namespace_properties as "properties: Json<Option<HashMap<String, String>>>",
                n.created_at,
                n.updated_at,
                n.version,
                p.namespace_id as "parent_namespace_id?",
                p.version as "parent_version?"
        FROM relevant_namespaces n
        LEFT JOIN relevant_namespaces p ON array_length(n.namespace_name, 1) = array_length(p.namespace_name, 1) + 1
            AND n.namespace_name[1:array_length(p.namespace_name, 1)] = p.namespace_name
        "#,
        *warehouse_id,
        &namespace_ids.iter().copied().map(Into::into).collect::<Vec<Uuid>>()
    )
    .fetch_all(connection)
    .await
    .map_err(DBErrorHandler::into_catalog_backend_error)?;

    rows.into_iter()
        .map(|row| row.into_namespace_with_parent_version(warehouse_id))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

pub(crate) async fn get_namespaces_by_name<
    'c,
    'e: 'c,
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
>(
    warehouse_id: WarehouseId,
    namespace: &[&NamespaceIdent],
    connection: E,
) -> std::result::Result<Vec<NamespaceWithParent>, CatalogGetNamespaceError> {
    // Encoding `ns_names` as json is a workaround for `sqlx` not supporting `Vec<Vec<String>>`.

    let ns_names_json = namespace
        .iter()
        .map(|ns| serde_json::to_value(*ns).map_err(|e| SerializationError::new("namespace", e)))
        .collect::<Result<Vec<_>, _>>()?;

    let rows = sqlx::query_as!(
        NamespaceWithParentVersionRow,
        r#"
        with requested_namespaces as (
            select array(select jsonb_array_elements_text(r))::text[] as namespace_name
            from unnest($2::jsonb[]) as r
        ),
        selected_ns as (
            select namespace_name
            from namespace
            where warehouse_id = $1 AND namespace_name = ANY(SELECT namespace_name FROM requested_namespaces)
        ),
        parent_paths as (
            SELECT DISTINCT namespace_name[1:generate_series(1, array_length(namespace_name, 1))] as parent_name
            FROM selected_ns
        ),
        relevant_namespaces AS (
            SELECT
                n.namespace_id,
                n.namespace_name,
                n.warehouse_id,
                n.protected,
                n.namespace_properties,
                n.created_at,
                n.updated_at,
                n.version
            FROM namespace n
            INNER JOIN warehouse w ON w.warehouse_id = $1
            WHERE n.warehouse_id = $1
            AND w.status = 'active'
            AND n.namespace_name IN (SELECT parent_name FROM parent_paths)
        )
        SELECT
                n.namespace_id,
                n.namespace_name as "namespace_name: Vec<String>",
                n.warehouse_id,
                n.protected,
                n.namespace_properties as "properties: Json<Option<HashMap<String, String>>>",
                n.created_at,
                n.updated_at,
                n.version,
                p.namespace_id as "parent_namespace_id?",
                p.version as "parent_version?"
        FROM relevant_namespaces n
        LEFT JOIN relevant_namespaces p ON array_length(n.namespace_name, 1) = array_length(p.namespace_name, 1) + 1
            AND n.namespace_name[1:array_length(p.namespace_name, 1)] = p.namespace_name
        "#,
        *warehouse_id,
        &ns_names_json
    )
    .fetch_all(connection)
    .await
    .map_err(DBErrorHandler::into_catalog_backend_error)?;

    rows.into_iter()
        .map(|row| row.into_namespace_with_parent_version(warehouse_id))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

struct ListNamespaceRow {
    namespace_id: NamespaceId,
    warehouse_id: WarehouseId,
    namespace_name: Vec<String>,
    protected: bool,
    properties: Json<Option<HashMap<String, String>>>,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: Option<chrono::DateTime<chrono::Utc>>,
    parent_namespace_id: Option<Uuid>,
    parent_version: Option<i64>,
    version: i64,
    include_in_list: bool,
}

impl From<ListNamespaceRow> for NamespaceWithParentVersionRow {
    fn from(row: ListNamespaceRow) -> Self {
        NamespaceWithParentVersionRow {
            namespace_id: row.namespace_id,
            namespace_name: row.namespace_name,
            warehouse_id: row.warehouse_id,
            protected: row.protected,
            properties: row.properties,
            created_at: row.created_at,
            updated_at: row.updated_at,
            parent_version: row.parent_version,
            parent_namespace_id: row.parent_namespace_id,
            version: row.version,
        }
    }
}

fn list_rows_into_hierarchy(
    rows: Vec<ListNamespaceRow>,
    warehouse_id: WarehouseId,
) -> std::result::Result<CatalogListNamespacesResponse, InvalidNamespaceIdentifier> {
    if rows.is_empty() {
        return Ok(CatalogListNamespacesResponse {
            parent_namespaces: HashMap::new(),
            namespaces: PaginatedMapping::with_capacity(0),
        });
    }

    let mut namespace_by_id: HashMap<NamespaceId, NamespaceWithParent> =
        HashMap::with_capacity(rows.len());

    // Track which namespaces should be included in the result, in order
    let mut result = PaginatedMapping::new();

    for row in rows {
        let include_this_row_in_list = row.include_in_list;

        let namespace = NamespaceWithParentVersionRow::from(row)
            .into_namespace_with_parent_version(warehouse_id)?;

        if include_this_row_in_list {
            let namespace_id = namespace.namespace_id();
            let created_at = namespace.created_at();

            let token = PaginateToken::V1(V1PaginateToken {
                id: namespace_id,
                created_at,
            })
            .to_string();

            result.insert(namespace_id, namespace.clone(), token);
        }

        namespace_by_id.insert(namespace.namespace_id(), namespace);
    }

    Ok(CatalogListNamespacesResponse {
        parent_namespaces: namespace_by_id,
        namespaces: result,
    })
}

#[allow(clippy::too_many_lines)]
pub(crate) async fn list_namespaces(
    warehouse_id: WarehouseId,
    ListNamespacesQuery {
        page_token,
        page_size,
        parent,
        return_uuids: _,
        return_protection_status: _,
    }: &ListNamespacesQuery,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> std::result::Result<CatalogListNamespacesResponse, CatalogListNamespaceError> {
    let page_size = CONFIG.page_size_or_pagination_max(*page_size);

    // Treat empty parent as None
    let parent = parent
        .as_ref()
        .and_then(|p| if p.is_empty() { None } else { Some(p.clone()) });
    let token = page_token
        .as_option()
        .map(PaginateToken::try_from)
        .transpose()?;

    let (token_ts, token_id) = token
        .as_ref()
        .map(
            |PaginateToken::V1(V1PaginateToken { created_at, id }): &PaginateToken<Uuid>| {
                (created_at, id)
            },
        )
        .unzip();

    let namespaces = if let Some(parent) = parent {
        // If it doesn't fit in a i32 it is way too large. Validation would have failed
        // already in the catalog.
        let parent_len: i32 = parent.len().try_into().unwrap_or(MAX_NAMESPACE_DEPTH + 1);

        // Namespace name field is an array.
        // Get all namespaces where the "name" array has
        // length(parent) + 1 elements, and the first length(parent)
        // elements are equal to parent.
        sqlx::query_as!(
            ListNamespaceRow,
            r#"
            WITH list_entries AS (
                SELECT
                    n.namespace_id,
                    n.namespace_name
                FROM namespace n
                INNER JOIN warehouse w ON w.warehouse_id = $1
                WHERE n.warehouse_id = $1
                AND w.status = 'active'
                AND array_length("namespace_name", 1) = $2 + 1
                AND "namespace_name"[1:$2] = $3
                --- PAGINATION
                AND ((n.created_at > $4 OR $4 IS NULL) OR (n.created_at = $4 AND n.namespace_id > $5))
                ORDER BY n.created_at, n.namespace_id ASC
                LIMIT $6
            ),
            parent_paths AS (
                SELECT DISTINCT
                    tn.namespace_name[1:generate_series(1, array_length(tn.namespace_name, 1))] as parent_name
                FROM list_entries tn
            ),
            relevant_namespaces AS (
                SELECT
                    n.namespace_id,
                    n.namespace_name,
                    n.warehouse_id,
                    n.protected,
                    n.namespace_properties,
                    n.created_at,
                    n.updated_at,
                    n.version,
                    n.namespace_id in (SELECT namespace_id FROM list_entries) AS "include_in_list"
                FROM namespace n
                WHERE n.warehouse_id = $1
                AND n.namespace_name IN (SELECT parent_name FROM parent_paths)
            )
            SELECT
                n.namespace_id,
                n.namespace_name as "namespace_name: Vec<String>",
                n.warehouse_id,
                n.protected,
                n.namespace_properties as "properties: Json<Option<HashMap<String, String>>>",
                n.created_at,
                n.updated_at,
                n.version,
                n.include_in_list AS "include_in_list!",
                p.namespace_id as "parent_namespace_id?",
                p.version as "parent_version?"
            FROM relevant_namespaces n
            LEFT JOIN relevant_namespaces p ON array_length(n.namespace_name, 1) = array_length(p.namespace_name, 1) + 1
                AND n.namespace_name[1:array_length(p.namespace_name, 1)] = p.namespace_name
            ORDER BY n.created_at, n.namespace_id ASC
            "#,
            *warehouse_id,
            parent_len,
            &*parent,
            token_ts,
            token_id,
            page_size
        )
        .fetch_all(&mut **transaction)
        .await
        .map_err(DBErrorHandler::into_catalog_backend_error)?
        .into_iter()
        .collect::<Vec<_>>()
    } else {
        sqlx::query_as!(
            ListNamespaceRow,
            r#"
            WITH list_entries AS (
                SELECT
                    n.namespace_id,
                    n.namespace_name
                FROM namespace n
                INNER JOIN warehouse w ON w.warehouse_id = $1
                WHERE n.warehouse_id = $1
                AND array_length("namespace_name", 1) = 1
                AND w.status = 'active'
                AND ((n.created_at > $2 OR $2 IS NULL) OR (n.created_at = $2 AND n.namespace_id > $3))
                ORDER BY n.created_at, n.namespace_id ASC
                LIMIT $4
            ),
            parent_paths AS (
                SELECT DISTINCT
                    tn.namespace_name[1:generate_series(1, array_length(tn.namespace_name, 1))] as parent_name
                FROM list_entries tn
            ),
            relevant_namespaces AS (
                SELECT
                    n.namespace_id,
                    n.namespace_name,
                    n.warehouse_id,
                    n.protected,
                    n.namespace_properties,
                    n.created_at,
                    n.updated_at,
                    n.version,
                    n.namespace_id in (SELECT namespace_id FROM list_entries) AS "include_in_list"
                FROM namespace n
                WHERE n.warehouse_id = $1
                AND n.namespace_name IN (SELECT parent_name FROM parent_paths)
            )
            SELECT
                n.namespace_id,
                n.namespace_name as "namespace_name: Vec<String>",
                n.warehouse_id,
                n.protected,
                n.namespace_properties as "properties: Json<Option<HashMap<String, String>>>",
                n.created_at,
                n.updated_at,
                n.version,
                n.include_in_list AS "include_in_list!",
                p.namespace_id as "parent_namespace_id?",
                p.version as "parent_version?"
            FROM relevant_namespaces n
            LEFT JOIN relevant_namespaces p ON array_length(n.namespace_name, 1) = array_length(p.namespace_name, 1) + 1
                AND n.namespace_name[1:array_length(p.namespace_name, 1)] = p.namespace_name
            ORDER BY n.created_at, n.namespace_id ASC
            "#,
            *warehouse_id,
            token_ts,
            token_id,
            page_size
        )
        .fetch_all(&mut **transaction)
        .await
        .map_err(DBErrorHandler::into_catalog_backend_error)?
        .into_iter()
        .collect()
    };

    let namespace_map = list_rows_into_hierarchy(namespaces, warehouse_id)?;

    Ok(namespace_map)
}

pub(crate) async fn create_namespace(
    warehouse_id: WarehouseId,
    namespace_id: NamespaceId,
    request: CreateNamespaceRequest,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> std::result::Result<NamespaceWithParent, CatalogCreateNamespaceError> {
    let CreateNamespaceRequest {
        namespace,
        properties,
    } = request;
    let parent = namespace.parent();
    let has_parent = parent.is_some();

    let row = sqlx::query_as!(
        NamespaceWithParentVersionRow,
        r#"
        WITH inserted_ns AS (
            INSERT INTO namespace (warehouse_id, namespace_id, namespace_name, namespace_properties)
            (
                SELECT $1, $2, $3, $4
                WHERE EXISTS (
                    SELECT 1
                    FROM warehouse
                    WHERE warehouse_id = $1
                    AND status = 'active'
            ))
            RETURNING
                namespace_id,
                namespace_name,
                warehouse_id,
                protected,
                namespace_properties,
                created_at,
                updated_at,
                version
        ),
        parent_ns AS (
            SELECT
                namespace_id,
                version
            FROM namespace
            WHERE warehouse_id = $1
            AND $6
            AND namespace_name = $5
        )
        SELECT
            i.namespace_id as "namespace_id!",
            i.namespace_name as "namespace_name!",
            i.warehouse_id as "warehouse_id!",
            i.protected as "protected!",
            i.namespace_properties as "properties!: Json<Option<HashMap<String, String>>>",
            i.created_at as "created_at!",
            i.updated_at,
            i.version as "version!",
            p.namespace_id as "parent_namespace_id?",
            p.version as "parent_version?"
        FROM inserted_ns i
        LEFT JOIN parent_ns p ON $6
        "#,
        *warehouse_id,
        *namespace_id,
        &*namespace,
        serde_json::to_value(properties.clone()).map_err(|e| {
            NamespacePropertiesSerializationError::new(warehouse_id, namespace.clone(), e)
        })?,
        parent.as_deref(),
        has_parent
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|e| match e {
        sqlx::Error::Database(ref db_error) if db_error.is_unique_violation() => {
            tracing::debug!("Namespace already exists: {db_error:?}");
            CatalogCreateNamespaceError::from(NamespaceAlreadyExists::new(
                warehouse_id,
                namespace.clone(),
            ))
        }
        sqlx::Error::Database(ref db_error) if db_error.is_foreign_key_violation() => {
            tracing::debug!("Namespace foreign key violation: {db_error:?}");
            WarehouseIdNotFound::new(warehouse_id).into()
        }
        e @ sqlx::Error::RowNotFound => {
            tracing::debug!("Warehouse not found: {e:?}");
            WarehouseIdNotFound::new(warehouse_id).into()
        }
        _ => {
            tracing::error!("Internal error creating namespace: {e:?}");
            e.into_catalog_backend_error().into()
        }
    })?;

    // Check if parent was expected but not found
    if let Some(parent) = parent
        && row.parent_namespace_id.is_none()
    {
        return Err(CatalogCreateNamespaceError::from(NamespaceNotFound::new(
            warehouse_id,
            parent,
        )));
    }

    row.into_namespace_with_parent_version(warehouse_id)
        .map_err(Into::into)
}

#[allow(clippy::too_many_lines)]
pub(crate) async fn drop_namespace(
    warehouse_id: WarehouseId,
    namespace_id: NamespaceId,
    NamespaceDropFlags {
        force,
        purge: _purge,
        recursive,
    }: NamespaceDropFlags,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> std::result::Result<NamespaceDropInfo, CatalogNamespaceDropError> {
    let info = sqlx::query!(r#"
        WITH namespace_info AS (
            SELECT namespace_name, namespace_id, protected
            FROM namespace
            WHERE warehouse_id = $1 AND namespace_id = $2
        ),
        child_namespaces AS (
            SELECT n.protected, n.namespace_id, n.namespace_name
            FROM namespace n
            INNER JOIN namespace_info ni ON n.namespace_name[1:array_length(ni.namespace_name, 1)] = ni.namespace_name
            WHERE n.warehouse_id = $1 AND n.namespace_id != $2
        ),
        tabulars AS (
            SELECT ta.tabular_id, ta.name as table_name, COALESCE(ni.namespace_name, cn.namespace_name) as namespace_name, fs_location, fs_protocol, ta.typ, ta.protected, deleted_at
            FROM tabular ta
            LEFT JOIN namespace_info ni ON ta.namespace_id = ni.namespace_id
            LEFT JOIN child_namespaces cn ON ta.namespace_id = cn.namespace_id
            WHERE warehouse_id = $1 AND metadata_location IS NOT NULL AND (ta.namespace_id = $2 OR (ta.namespace_id = ANY (SELECT namespace_id FROM child_namespaces)))
        ),
        tasks AS (
            SELECT t.task_id, t.queue_name, t.status as task_status from task t
            WHERE t.entity_id = ANY (SELECT tabular_id FROM tabulars) AND t.warehouse_id = $1 AND t.entity_type in ('table', 'view')
        )
        SELECT
            ni.protected AS "is_protected!",
            ni.namespace_name AS "namespace_name: Vec<String>",
            EXISTS (SELECT 1 FROM child_namespaces WHERE protected = true) AS "has_protected_namespaces!",
            EXISTS (SELECT 1 FROM tabulars WHERE protected = true) AS "has_protected_tabulars!",
            EXISTS (SELECT 1 FROM tasks WHERE task_status = 'running' AND queue_name = 'tabular_expiration') AS "has_running_expiration!",
            ARRAY(SELECT tabular_id FROM tabulars where deleted_at is NULL) AS "child_tabulars!",
            ARRAY(SELECT to_jsonb(namespace_name) FROM tabulars where deleted_at is NULL) AS "child_tabulars_namespace_names!: Vec<serde_json::Value>",
            ARRAY(SELECT table_name FROM tabulars where deleted_at is NULL) AS "child_tabulars_table_names!",
            ARRAY(SELECT fs_protocol FROM tabulars where deleted_at is NULL) AS "child_tabular_fs_protocol!",
            ARRAY(SELECT fs_location FROM tabulars where deleted_at is NULL) AS "child_tabular_fs_location!",
            ARRAY(SELECT typ FROM tabulars where deleted_at is NULL) AS "child_tabular_typ!: Vec<TabularType>",
            ARRAY(SELECT tabular_id FROM tabulars where deleted_at is not NULL) AS "child_tabulars_deleted!",
            ARRAY(SELECT namespace_id FROM child_namespaces) AS "child_namespaces!",
            ARRAY(SELECT task_id FROM tasks) AS "child_tabular_task_id!: Vec<Uuid>"
        FROM namespace_info ni
"#,
        *warehouse_id,
        *namespace_id,
    ).fetch_one(&mut **transaction).await.map_err(|e|
        if let sqlx::Error::RowNotFound = e {
            CatalogNamespaceDropError::from(NamespaceNotFound::new(warehouse_id, namespace_id))
         } else {
            e.into_catalog_backend_error().into()
        }
    )?;
    let namespace_ident = parse_namespace_identifier_from_vec(
        &info.namespace_name,
        warehouse_id,
        Some(namespace_id),
    )?;

    if !recursive && (!info.child_tabulars.is_empty() || !info.child_namespaces.is_empty()) {
        return Err(
            NamespaceNotEmpty::new(warehouse_id, namespace_ident.clone()).append_detail(format!("Contains {} tables/views, {} soft-deleted tables/views and {} child namespaces.", 
                info.child_tabulars.len(),
                info.child_tabulars_deleted.len(),
                info.child_namespaces.len()
        )

    ).append_detail("Use 'recursive' flag to delete all content.").into()
        );
    }

    if !force && info.is_protected {
        return Err(NamespaceProtected::new(warehouse_id, namespace_ident.clone()).into());
    }

    if !force && info.has_protected_namespaces {
        return Err(ChildNamespaceProtected::new(warehouse_id, namespace_ident.clone()).into());
    }

    if !force && info.has_protected_tabulars {
        return Err(ChildTabularProtected::new(warehouse_id, namespace_ident.clone()).into());
    }

    if info.has_running_expiration {
        return Err(NamespaceHasRunningTabularExpirations::new(
            warehouse_id,
            namespace_ident.clone(),
        )
        .into());
    }

    let record = sqlx::query!(
        r#"
        DELETE FROM namespace
            WHERE warehouse_id = $1
            -- If recursive is true, delete all child namespaces...
            AND (namespace_id = any($2) or namespace_id = $3)
            AND warehouse_id IN (
                SELECT warehouse_id FROM warehouse WHERE status = 'active'
                AND warehouse_id = $1
            )
        "#,
        *warehouse_id,
        &info.child_namespaces,
        *namespace_id,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| match &e {
        sqlx::Error::Database(db_error) if db_error.is_foreign_key_violation() => {
            CatalogNamespaceDropError::from(NamespaceNotEmpty::new(
                warehouse_id,
                namespace_ident.clone(),
            ))
        }
        _ => e.into_catalog_backend_error().into(),
    })?;

    tracing::debug!(
        "Deleted {deleted_count} namespaces while dropping namespace {namespace_ident} with id {namespace_id} in warehouse {warehouse_id}",
        deleted_count = record.rows_affected()
    );

    if record.rows_affected() == 0 {
        return Err(NamespaceNotFound::new(warehouse_id, namespace_ident.clone()).into());
    }

    Ok(NamespaceDropInfo {
        child_namespaces: info.child_namespaces.into_iter().map(Into::into).collect(),
        child_tables: izip!(
            info.child_tabulars,
            info.child_tabular_fs_protocol,
            info.child_tabular_fs_location,
            info.child_tabular_typ,
            info.child_tabulars_namespace_names,
            info.child_tabulars_table_names
        )
        .map(
            |(tabular_id, protocol, fs_location, typ, ns_name, t_name)| {
                let ns_ident = json_value_to_namespace_ident(warehouse_id, &ns_name)?;
                let table_ident = TableIdent::new(ns_ident, t_name);
                Ok::<_, CatalogNamespaceDropError>((
                    match typ {
                        TabularType::Table => TabularId::Table(tabular_id.into()),
                        TabularType::View => TabularId::View(tabular_id.into()),
                    },
                    join_location(protocol.as_str(), fs_location.as_str())
                        .map_err(InternalParseLocationError::from)?,
                    table_ident,
                ))
            },
        )
        .collect::<std::result::Result<Vec<_>, _>>()?,
        open_tasks: info
            .child_tabular_task_id
            .into_iter()
            .map(TaskId::from)
            .collect(),
    })
}

pub(super) fn parse_namespace_identifier_from_vec(
    namespace: &[String],
    warehouse_id: WarehouseId,
    namespace_id: Option<impl Into<NamespaceId>>,
) -> std::result::Result<NamespaceIdent, InvalidNamespaceIdentifier> {
    let namespace_id = namespace_id.map(Into::into);
    NamespaceIdent::from_vec(namespace.to_owned()).map_err(|_e| {
        let err = InvalidNamespaceIdentifier::new(warehouse_id, format!("{namespace:?}"))
            .append_detail("Namespace identifier can't be empty");
        if let Some(id) = namespace_id {
            err.with_id(id)
        } else {
            err
        }
    })
}

fn json_value_to_namespace_ident(
    warehouse_id: WarehouseId,
    v: &serde_json::Value,
) -> Result<NamespaceIdent, InvalidNamespaceIdentifier> {
    if let serde_json::Value::Array(arr) = v.clone() {
        let str_vec: Result<Vec<String>, InvalidNamespaceIdentifier> = arr
            .into_iter()
            .map(|item| {
                if let serde_json::Value::String(s) = item {
                    Ok(s)
                } else {
                    Err(
                        InvalidNamespaceIdentifier::new(warehouse_id, format!("{v:?}"))
                            .append_detail("Expected array of strings for namespace identifier"),
                    )
                }
            })
            .collect();
        NamespaceIdent::from_vec(str_vec?).map_err(|_e| {
            InvalidNamespaceIdentifier::new(warehouse_id, format!("{v:?}"))
                .append_detail("Namespace identifier can't be empty")
        })
    } else {
        Err(
            InvalidNamespaceIdentifier::new(warehouse_id, format!("{v:?}"))
                .append_detail("Expected array for namespace identifier"),
        )
    }
}

pub(crate) async fn set_namespace_protected(
    warehouse_id: WarehouseId,
    namespace_id: NamespaceId,
    protect: bool,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> std::result::Result<NamespaceWithParent, CatalogSetNamespaceProtectedError> {
    let row = sqlx::query_as!(
        NamespaceWithParentVersionRow,
        r#"
        WITH updated_ns AS (
            UPDATE namespace
            SET protected = $1
            WHERE namespace_id = $2 AND warehouse_id IN (
                SELECT warehouse_id FROM warehouse WHERE status = 'active'
            )
            RETURNING
                namespace_id,
                namespace_name,
                warehouse_id,
                protected,
                namespace_properties,
                created_at,
                updated_at,
                version
        ),
        parent_ns AS (
            SELECT
                p.namespace_id,
                p.version
            FROM updated_ns u
            INNER JOIN namespace p ON p.warehouse_id = u.warehouse_id
                AND p.namespace_name = u.namespace_name[1:array_length(u.namespace_name, 1) - 1]
            WHERE array_length(u.namespace_name, 1) > 1
        )
        SELECT
            u.namespace_id as "namespace_id!",
            u.namespace_name as "namespace_name!",
            u.warehouse_id as "warehouse_id!",
            u.protected as "protected!",
            u.namespace_properties as "properties!: Json<Option<HashMap<String, String>>>",
            u.created_at as "created_at!",
            u.updated_at,
            u.version as "version!",
            p.namespace_id as "parent_namespace_id?",
            p.version as "parent_version?"
        FROM updated_ns u
        LEFT JOIN parent_ns p ON TRUE
        "#,
        protect,
        *namespace_id
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|e| {
        if let sqlx::Error::RowNotFound = e {
            CatalogSetNamespaceProtectedError::from(NamespaceNotFound::new(
                warehouse_id,
                namespace_id,
            ))
        } else {
            tracing::error!("Error setting namespace protection: {e:?}");
            e.into_catalog_backend_error().into()
        }
    })?;

    row.into_namespace_with_parent_version(warehouse_id)
        .map_err(Into::into)
}

pub(crate) async fn update_namespace_properties(
    warehouse_id: WarehouseId,
    namespace_id: NamespaceId,
    properties: HashMap<String, String>,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> std::result::Result<NamespaceWithParent, CatalogUpdateNamespacePropertiesError> {
    let properties = serde_json::to_value(properties)
        .map_err(|e| NamespacePropertiesSerializationError::new(warehouse_id, namespace_id, e))?;

    let row = sqlx::query_as!(
        NamespaceWithParentVersionRow,
        r#"
        WITH updated_ns AS (
            UPDATE namespace
            SET namespace_properties = $1
            WHERE warehouse_id = $2 AND namespace_id = $3
            AND warehouse_id IN (
                SELECT warehouse_id FROM warehouse WHERE status = 'active'
            )
            RETURNING
                namespace_id,
                namespace_name,
                warehouse_id,
                protected,
                namespace_properties,
                created_at,
                updated_at,
                version
        ),
        parent_ns AS (
            SELECT
                p.namespace_id,
                p.version
            FROM updated_ns u
            INNER JOIN namespace p ON p.warehouse_id = u.warehouse_id
                AND p.namespace_name = u.namespace_name[1:array_length(u.namespace_name, 1) - 1]
            WHERE array_length(u.namespace_name, 1) > 1
        )
        SELECT
            u.namespace_id as "namespace_id!",
            u.namespace_name as "namespace_name!",
            u.warehouse_id as "warehouse_id!",
            u.protected as "protected!",
            u.namespace_properties as "properties!: Json<Option<HashMap<String, String>>>",
            u.created_at as "created_at!",
            u.updated_at,
            u.version as "version!",
            p.namespace_id as "parent_namespace_id?",
            p.version as "parent_version?"
        FROM updated_ns u
        LEFT JOIN parent_ns p ON TRUE
        "#,
        properties,
        *warehouse_id,
        *namespace_id
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|e| match e {
        sqlx::Error::RowNotFound => CatalogUpdateNamespacePropertiesError::from(
            NamespaceNotFound::new(warehouse_id, namespace_id),
        ),
        _ => e.into_catalog_backend_error().into(),
    })?;

    row.into_namespace_with_parent_version(warehouse_id)
        .map_err(Into::into)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::{
        super::{PostgresBackend, warehouse::test::initialize_warehouse},
        *,
    };
    use crate::{
        api::iceberg::{types::PageToken, v1::tables::LoadTableFilters},
        implementations::postgres::{
            CatalogState, PostgresTransaction,
            tabular::{
                set_tabular_protected,
                table::{load_tables, tests::initialize_table},
            },
        },
        service::{CachePolicy, CatalogNamespaceOps, Transaction as _},
    };

    pub(crate) async fn initialize_namespace(
        state: CatalogState,
        warehouse_id: WarehouseId,
        namespace: &NamespaceIdent,
        properties: Option<HashMap<String, String>>,
    ) -> NamespaceWithParent {
        let mut transaction = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();

        let namespace_id = NamespaceId::new_random();

        let response = PostgresBackend::create_namespace(
            warehouse_id,
            namespace_id,
            CreateNamespaceRequest {
                namespace: namespace.clone(),
                properties: properties.clone(),
            },
            transaction.transaction(),
        )
        .await
        .unwrap();

        transaction.commit().await.unwrap();

        response
    }

    #[sqlx::test]
    async fn test_namespace_lifecycle(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;

        let namespace = NamespaceIdent::from_vec(vec!["test".to_string()]).unwrap();
        let properties = HashMap::from_iter(vec![
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
        ]);

        let namespace_info = initialize_namespace(
            state.clone(),
            warehouse_id,
            &namespace,
            Some(properties.clone()),
        )
        .await;

        let namespace_hierarchy_by_name = PostgresBackend::get_namespace_cache_aware(
            warehouse_id,
            &namespace,
            CachePolicy::Skip,
            state.clone(),
        )
        .await
        .unwrap()
        .expect("Namespace should exist");
        assert_eq!(
            namespace_hierarchy_by_name.root(),
            &namespace_hierarchy_by_name.namespace
        );
        assert_eq!(namespace_hierarchy_by_name.depth(), 0);
        assert_eq!(*namespace_hierarchy_by_name.version(), 0);
        assert_eq!(namespace_hierarchy_by_name.parent(), None);
        assert_eq!(namespace_hierarchy_by_name.namespace.parent, None);
        let namespace_id = namespace_hierarchy_by_name.namespace_id();

        assert_eq!(&namespace_hierarchy_by_name.namespace, &namespace_info);

        let namespace_hierarchy_by_id = PostgresBackend::get_namespace_cache_aware(
            warehouse_id,
            namespace_id,
            CachePolicy::Skip,
            state.clone(),
        )
        .await
        .unwrap()
        .expect("Namespace should exist");

        assert_eq!(namespace_hierarchy_by_id, namespace_hierarchy_by_name);

        let mut transaction = PostgresTransaction::begin_read(state.clone())
            .await
            .unwrap();

        let _response = PostgresBackend::get_namespace_cache_aware(
            warehouse_id,
            &namespace,
            CachePolicy::Skip,
            state.clone(),
        )
        .await
        .unwrap()
        .expect("Namespace should exist");

        let response = PostgresBackend::list_namespaces(
            warehouse_id,
            &ListNamespacesQuery {
                page_token: crate::api::iceberg::v1::PageToken::NotSpecified,
                page_size: None,
                parent: None,
                return_uuids: false,
                return_protection_status: false,
            },
            transaction.transaction(),
        )
        .await
        .unwrap()
        .namespaces
        .into_hashmap();

        assert_eq!(response.len(), 1);
        assert_eq!(response[&namespace_id].namespace_ident(), &namespace);

        let mut transaction = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();

        let new_props = HashMap::from_iter(vec![
            ("key2".to_string(), "updated_value".to_string()),
            ("new_key".to_string(), "new_value".to_string()),
        ]);
        PostgresBackend::update_namespace_properties(
            warehouse_id,
            namespace_id,
            new_props.clone(),
            transaction.transaction(),
        )
        .await
        .unwrap();

        transaction.commit().await.unwrap();

        let response = PostgresBackend::get_namespace_cache_aware(
            warehouse_id,
            namespace_id,
            CachePolicy::Skip,
            state.clone(),
        )
        .await
        .unwrap()
        .expect("Namespace should exist");
        assert_eq!(response.properties().unwrap(), &new_props);

        let mut transaction = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();

        PostgresBackend::drop_namespace(
            warehouse_id,
            namespace_id,
            NamespaceDropFlags::default(),
            transaction.transaction(),
        )
        .await
        .expect("Error dropping namespace");
    }

    #[sqlx::test]
    async fn test_pagination(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let namespace = NamespaceIdent::from_vec(vec!["test".to_string()]).unwrap();
        let properties = Some(HashMap::from_iter(vec![
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
        ]));

        let namespace_info_1 =
            initialize_namespace(state.clone(), warehouse_id, &namespace, properties.clone()).await;

        let namespace = NamespaceIdent::from_vec(vec!["test2".to_string()]).unwrap();
        let properties = Some(HashMap::from_iter(vec![
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
        ]));
        let namespace_info_2 =
            initialize_namespace(state.clone(), warehouse_id, &namespace, properties.clone()).await;
        let namespace = NamespaceIdent::from_vec(vec!["test3".to_string()]).unwrap();
        let properties = Some(HashMap::from_iter(vec![
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
        ]));
        let namespace_info_3 =
            initialize_namespace(state.clone(), warehouse_id, &namespace, properties.clone()).await;

        let mut t = PostgresTransaction::begin_read(state.clone())
            .await
            .unwrap();

        let namespaces = PostgresBackend::list_namespaces(
            warehouse_id,
            &ListNamespacesQuery {
                page_token: crate::api::iceberg::v1::PageToken::NotSpecified,
                page_size: Some(1),
                parent: None,
                return_uuids: false,
                return_protection_status: false,
            },
            t.transaction(),
        )
        .await
        .unwrap()
        .namespaces;
        let next_page_token = namespaces.next_token().map(ToString::to_string);
        assert_eq!(namespaces.len(), 1);
        let namespaces = namespaces.into_hashmap();
        assert_eq!(
            namespaces[&namespace_info_1.namespace_id()].namespace_ident(),
            namespace_info_1.namespace_ident()
        );
        assert!(!namespaces[&namespace_info_1.namespace_id()].is_protected());
        // Root namespaces should have no parents
        assert!(namespaces[&namespace_info_1.namespace_id()].is_root());

        let mut t = PostgresTransaction::begin_read(state.clone())
            .await
            .unwrap();

        let namespaces = PostgresBackend::list_namespaces(
            warehouse_id,
            &ListNamespacesQuery {
                page_token: next_page_token.map_or(
                    crate::api::iceberg::v1::PageToken::Empty,
                    crate::api::iceberg::v1::PageToken::Present,
                ),
                page_size: Some(2),
                parent: None,
                return_uuids: false,
                return_protection_status: false,
            },
            t.transaction(),
        )
        .await
        .unwrap()
        .namespaces;
        let next_page_token = namespaces.next_token().map(ToString::to_string);
        assert_eq!(namespaces.len(), 2);
        assert!(next_page_token.is_some());
        let namespaces = namespaces.into_hashmap();

        assert_eq!(
            namespaces[&namespace_info_2.namespace_id()].namespace_ident(),
            namespace_info_2.namespace_ident()
        );
        assert!(!namespaces[&namespace_info_2.namespace_id()].is_protected());
        assert!(namespaces[&namespace_info_2.namespace_id()].is_root());
        assert_eq!(
            namespaces[&namespace_info_3.namespace_id()].namespace_ident(),
            namespace_info_3.namespace_ident()
        );
        assert!(!namespaces[&namespace_info_3.namespace_id()].is_protected());
        assert!(namespaces[&namespace_info_3.namespace_id()].is_root());

        // last page is empty
        let namespaces = PostgresBackend::list_namespaces(
            warehouse_id,
            &ListNamespacesQuery {
                page_token: next_page_token.map_or(
                    crate::api::iceberg::v1::PageToken::Empty,
                    crate::api::iceberg::v1::PageToken::Present,
                ),
                page_size: Some(3),
                parent: None,
                return_uuids: false,
                return_protection_status: false,
            },
            t.transaction(),
        )
        .await
        .unwrap()
        .namespaces;

        assert_eq!(namespaces.next_token(), None);
        assert_eq!(namespaces.into_hashmap(), HashMap::new());
    }

    #[sqlx::test]
    async fn test_get_nested_namespace(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;

        let parent_namespace_ident = NamespaceIdent::from_vec(vec!["parent".to_string()]).unwrap();
        let parent_namespace: NamespaceWithParent =
            initialize_namespace(state.clone(), warehouse_id, &parent_namespace_ident, None).await;

        let child_namespace_ident =
            NamespaceIdent::from_vec(vec!["parent".to_string(), "child".to_string()]).unwrap();
        let child_namespace =
            initialize_namespace(state.clone(), warehouse_id, &child_namespace_ident, None).await;

        let result = PostgresBackend::get_namespace_cache_aware(
            warehouse_id,
            &child_namespace_ident,
            CachePolicy::Skip,
            state.clone(),
        )
        .await
        .unwrap()
        .expect("Namespace should exist");
        assert_eq!(&result.namespace, &child_namespace);
        assert_eq!(result.depth(), 1);
        assert_eq!(result.root(), &parent_namespace);
        assert_eq!(result.parents.len(), 1);
        assert_eq!(&result.parents[0], &parent_namespace);
    }

    #[sqlx::test]
    async fn test_get_nonexistent_namespace(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;

        let result = PostgresBackend::get_namespace_cache_aware(
            warehouse_id,
            NamespaceId::new_random(),
            CachePolicy::Skip,
            state.clone(),
        )
        .await
        .unwrap();
        assert_eq!(result, None);
    }

    #[sqlx::test]
    async fn test_drop_nonexistent_namespace(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;

        let mut transaction = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let result = drop_namespace(
            warehouse_id,
            NamespaceId::new_random(),
            NamespaceDropFlags::default(),
            transaction.transaction(),
        )
        .await
        .unwrap_err();

        assert!(matches!(
            result,
            CatalogNamespaceDropError::NamespaceNotFound(_)
        ));
    }

    #[sqlx::test]
    async fn test_cannot_drop_nonempty_namespace(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let staged = false;
        let table = initialize_table(warehouse_id, state.clone(), staged, None, None, None).await;

        let namespace_id = PostgresBackend::get_namespace_cache_aware(
            warehouse_id,
            Into::<NamespaceIdent>::into(table.namespace),
            CachePolicy::Skip,
            state.clone(),
        )
        .await
        .unwrap()
        .expect("Namespace should exist")
        .namespace_id();
        let mut transaction = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let result = drop_namespace(
            warehouse_id,
            namespace_id,
            NamespaceDropFlags::default(),
            transaction.transaction(),
        )
        .await
        .unwrap_err();

        assert!(matches!(
            result,
            CatalogNamespaceDropError::NamespaceNotEmpty(_)
        ));
    }

    #[sqlx::test]
    async fn test_can_recursive_drop_nonempty_namespace(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let staged = false;
        let table = initialize_table(warehouse_id, state.clone(), staged, None, None, None).await;

        let namespace_id = PostgresBackend::get_namespace_cache_aware(
            warehouse_id,
            Into::<NamespaceIdent>::into(table.namespace),
            CachePolicy::Skip,
            state.clone(),
        )
        .await
        .unwrap()
        .expect("Namespace should exist")
        .namespace_id();

        let mut transaction = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let drop_info = drop_namespace(
            warehouse_id,
            namespace_id,
            NamespaceDropFlags {
                force: false,
                purge: false,
                recursive: true,
            },
            transaction.transaction(),
        )
        .await
        .unwrap();

        assert_eq!(drop_info.child_namespaces.len(), 0);
        assert_eq!(drop_info.child_tables.len(), 1);
        assert_eq!(drop_info.open_tasks.len(), 0);
        let r0 = &drop_info.child_tables[0];
        assert_eq!(r0.0, TabularId::Table(table.table_id));
        assert_eq!(r0.2, table.table_ident);

        transaction.commit().await.unwrap();

        let mut transaction = PostgresTransaction::begin_read(state.clone())
            .await
            .unwrap();
        let tables = load_tables(
            warehouse_id,
            [table.table_id].into_iter(),
            true,
            &LoadTableFilters::default(),
            transaction.transaction(),
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();

        assert_eq!(tables.len(), 0);
    }

    #[sqlx::test]
    async fn test_cannot_drop_namespace_with_sub_namespaces(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let namespace = NamespaceIdent::from_vec(vec!["test".to_string()]).unwrap();

        let response = initialize_namespace(state.clone(), warehouse_id, &namespace, None).await;

        let namespace =
            NamespaceIdent::from_vec(vec!["test".to_string(), "test2".to_string()]).unwrap();
        let response2 = initialize_namespace(state.clone(), warehouse_id, &namespace, None).await;

        let mut transaction = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();

        let result = drop_namespace(
            warehouse_id,
            response.namespace_id(),
            NamespaceDropFlags::default(),
            transaction.transaction(),
        )
        .await
        .unwrap_err();

        assert!(matches!(
            result,
            CatalogNamespaceDropError::NamespaceNotEmpty(_)
        ));

        drop_namespace(
            warehouse_id,
            response2.namespace_id(),
            NamespaceDropFlags::default(),
            transaction.transaction(),
        )
        .await
        .unwrap();

        drop_namespace(
            warehouse_id,
            response.namespace_id(),
            NamespaceDropFlags::default(),
            transaction.transaction(),
        )
        .await
        .unwrap();
    }

    #[sqlx::test]
    async fn test_can_recursive_drop_namespace_with_sub_namespaces(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let namespace = NamespaceIdent::from_vec(vec!["test".to_string()]).unwrap();

        let response = initialize_namespace(state.clone(), warehouse_id, &namespace, None).await;

        let namespace =
            NamespaceIdent::from_vec(vec!["test".to_string(), "test2".to_string()]).unwrap();
        let _ = initialize_namespace(state.clone(), warehouse_id, &namespace, None).await;

        let mut transaction = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();

        let drop_info = drop_namespace(
            warehouse_id,
            response.namespace_id(),
            NamespaceDropFlags {
                force: false,
                purge: false,
                recursive: true,
            },
            transaction.transaction(),
        )
        .await
        .unwrap();

        assert_eq!(drop_info.child_namespaces.len(), 1);
        assert_eq!(drop_info.child_tables.len(), 0);
        assert_eq!(drop_info.open_tasks.len(), 0);

        transaction.commit().await.unwrap();

        let mut transaction = PostgresTransaction::begin_read(state.clone())
            .await
            .unwrap();
        let ns = list_namespaces(
            warehouse_id,
            &ListNamespacesQuery {
                page_token: PageToken::NotSpecified,
                page_size: Some(100),
                parent: None,
                return_uuids: true,
                return_protection_status: false,
            },
            transaction.transaction(),
        )
        .await
        .unwrap()
        .namespaces;
        transaction.commit().await.unwrap();

        assert_eq!(ns.len(), 0);
    }

    #[sqlx::test]
    async fn test_case_insensitive_but_preserve_case(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let namespace_1 = NamespaceIdent::from_vec(vec!["Test".to_string()]).unwrap();
        let namespace_2 = NamespaceIdent::from_vec(vec!["test".to_string()]).unwrap();

        let mut transaction = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();

        let response = PostgresBackend::create_namespace(
            warehouse_id,
            NamespaceId::new_random(),
            CreateNamespaceRequest {
                namespace: namespace_1.clone(),
                properties: None,
            },
            transaction.transaction(),
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();

        // Check that the namespace is created with the correct case
        assert_eq!(response.namespace_ident(), &namespace_1);

        let mut transaction = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();

        let response = PostgresBackend::create_namespace(
            warehouse_id,
            NamespaceId::new_random(),
            CreateNamespaceRequest {
                namespace: namespace_2.clone(),
                properties: None,
            },
            transaction.transaction(),
        )
        .await
        .unwrap_err();

        assert!(matches!(
            response,
            CatalogCreateNamespaceError::NamespaceAlreadyExists(_)
        ));
    }

    #[sqlx::test]
    async fn test_cannot_drop_protected_namespace(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let namespace = NamespaceIdent::from_vec(vec!["test".to_string()]).unwrap();

        let response = initialize_namespace(state.clone(), warehouse_id, &namespace, None).await;
        assert_eq!(*response.version(), 0);

        let mut transaction = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();

        let protected_response = PostgresBackend::set_namespace_protected(
            warehouse_id,
            response.namespace_id(),
            true,
            transaction.transaction(),
        )
        .await
        .unwrap();
        assert_eq!(*protected_response.version(), 1);

        let result = drop_namespace(
            warehouse_id,
            response.namespace_id(),
            NamespaceDropFlags::default(),
            transaction.transaction(),
        )
        .await
        .unwrap_err();

        assert!(matches!(
            result,
            CatalogNamespaceDropError::NamespaceProtected(_)
        ));
    }

    #[sqlx::test]
    async fn test_can_force_drop_protected_namespace(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let namespace = NamespaceIdent::from_vec(vec!["test".to_string()]).unwrap();

        let response = initialize_namespace(state.clone(), warehouse_id, &namespace, None).await;

        let mut transaction = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();

        PostgresBackend::set_namespace_protected(
            warehouse_id,
            response.namespace_id(),
            true,
            transaction.transaction(),
        )
        .await
        .unwrap();

        let result = drop_namespace(
            warehouse_id,
            response.namespace_id(),
            NamespaceDropFlags {
                force: true,
                purge: false,
                recursive: false,
            },
            transaction.transaction(),
        )
        .await
        .unwrap();

        assert!(result.child_namespaces.is_empty());
        assert!(result.child_tables.is_empty());
        assert!(result.open_tasks.is_empty());
    }

    #[sqlx::test]
    async fn test_can_recursive_force_drop_nonempty_protected_namespace(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let outer_namespace = NamespaceIdent::from_vec(vec!["test".to_string()]).unwrap();

        let response =
            initialize_namespace(state.clone(), warehouse_id, &outer_namespace, None).await;
        let namespace_id = response.namespace_id();

        let namespace =
            NamespaceIdent::from_vec(vec!["test".to_string(), "test2".to_string()]).unwrap();
        let _ = initialize_namespace(state.clone(), warehouse_id, &namespace, None).await;

        let mut transaction = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();

        set_namespace_protected(warehouse_id, namespace_id, true, transaction.transaction())
            .await
            .unwrap();
        transaction.commit().await.unwrap();
        let mut transaction = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let err = drop_namespace(
            warehouse_id,
            namespace_id,
            NamespaceDropFlags {
                force: false,
                purge: false,
                recursive: true,
            },
            transaction.transaction(),
        )
        .await
        .unwrap_err();

        assert!(matches!(
            err,
            CatalogNamespaceDropError::NamespaceProtected(_)
        ));

        let drop_info = drop_namespace(
            warehouse_id,
            namespace_id,
            NamespaceDropFlags {
                force: true,
                recursive: true,
                purge: false,
            },
            transaction.transaction(),
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();

        assert_eq!(drop_info.child_namespaces.len(), 1);
        assert_eq!(drop_info.child_tables.len(), 0);
        assert_eq!(drop_info.open_tasks.len(), 0);
    }

    #[sqlx::test]
    async fn test_can_recursive_force_drop_namespace_with_protected_table(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let outer_namespace = NamespaceIdent::from_vec(vec!["test".to_string()]).unwrap();

        let response =
            initialize_namespace(state.clone(), warehouse_id, &outer_namespace, None).await;
        let namespace_id = response.namespace_id();
        let tab = initialize_table(
            warehouse_id,
            state.clone(),
            false,
            Some(outer_namespace),
            None,
            None,
        )
        .await;

        let mut transaction = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        set_tabular_protected(
            warehouse_id,
            TabularId::Table(tab.table_id),
            true,
            transaction.transaction(),
        )
        .await
        .unwrap();

        let err = drop_namespace(
            warehouse_id,
            namespace_id,
            NamespaceDropFlags {
                force: false,
                purge: false,
                recursive: true,
            },
            transaction.transaction(),
        )
        .await
        .unwrap_err();

        assert!(matches!(
            err,
            CatalogNamespaceDropError::ChildTabularProtected(_)
        ));

        let drop_info = drop_namespace(
            warehouse_id,
            namespace_id,
            NamespaceDropFlags {
                force: true,
                recursive: true,
                purge: false,
            },
            transaction.transaction(),
        )
        .await
        .unwrap();

        assert_eq!(drop_info.child_namespaces.len(), 0);
        assert_eq!(drop_info.child_tables.len(), 1);
        assert_eq!(drop_info.open_tasks.len(), 0);

        transaction.commit().await.unwrap();
    }

    #[sqlx::test]
    async fn test_list_namespaces_with_hierarchy(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;

        // Create a hierarchy: root, root.child, root.child.grandchild
        let root = NamespaceIdent::from_vec(vec!["root".to_string()]).unwrap();
        let root_ns = initialize_namespace(state.clone(), warehouse_id, &root, None).await;

        let child =
            NamespaceIdent::from_vec(vec!["root".to_string(), "child".to_string()]).unwrap();
        let child_ns = initialize_namespace(state.clone(), warehouse_id, &child, None).await;

        let grandchild = NamespaceIdent::from_vec(vec![
            "root".to_string(),
            "child".to_string(),
            "grandchild".to_string(),
        ])
        .unwrap();
        let grandchild_ns =
            initialize_namespace(state.clone(), warehouse_id, &grandchild, None).await;

        // List all root namespaces (no parent filter)
        let mut transaction = PostgresTransaction::begin_read(state.clone())
            .await
            .unwrap();

        let result = PostgresBackend::list_namespaces(
            warehouse_id,
            &ListNamespacesQuery {
                page_token: PageToken::NotSpecified,
                page_size: None,
                parent: None,
                return_uuids: false,
                return_protection_status: false,
            },
            transaction.transaction(),
        )
        .await
        .unwrap();

        // Should only return root namespace
        assert_eq!(result.parent_namespaces.len(), 1);
        let result = result.namespaces;
        assert_eq!(result.len(), 1);
        let result_map = result.into_hashmap();

        let root_hierarchy = &result_map[&root_ns.namespace_id()];
        assert_eq!(root_hierarchy.namespace_ident(), &root);
        assert!(root_hierarchy.is_root());

        // List children of root
        let mut transaction = PostgresTransaction::begin_read(state.clone())
            .await
            .unwrap();

        let result = PostgresBackend::list_namespaces(
            warehouse_id,
            &ListNamespacesQuery {
                page_token: PageToken::NotSpecified,
                page_size: None,
                parent: Some(root.clone()),
                return_uuids: false,
                return_protection_status: false,
            },
            transaction.transaction(),
        )
        .await
        .unwrap();

        // Should return child with root as parent
        assert_eq!(result.parent_namespaces.len(), 2);
        let result = result.namespaces;
        assert_eq!(result.len(), 1);
        let result_map = result.into_hashmap();

        let child_hierarchy = &result_map[&child_ns.namespace_id()];
        assert_eq!(child_hierarchy.namespace_ident(), &child);
        assert!(!child_hierarchy.is_root());

        // List children of root.child
        let mut transaction = PostgresTransaction::begin_read(state.clone())
            .await
            .unwrap();

        let result = PostgresBackend::list_namespaces(
            warehouse_id,
            &ListNamespacesQuery {
                page_token: PageToken::NotSpecified,
                page_size: None,
                parent: Some(child.clone()),
                return_uuids: false,
                return_protection_status: false,
            },
            transaction.transaction(),
        )
        .await
        .unwrap();

        // Should return grandchild with full hierarchy
        assert_eq!(result.parent_namespaces.len(), 3);
        let result = result.namespaces;
        assert_eq!(result.len(), 1);
        let result_map = result.into_hashmap();

        let grandchild_hierarchy = &result_map[&grandchild_ns.namespace_id()];
        assert_eq!(grandchild_hierarchy.namespace_ident(), &grandchild);
        assert!(!grandchild_hierarchy.is_root());
    }

    #[sqlx::test]
    async fn test_list_namespaces_multiple_hierarchies(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;

        // Create multiple root namespaces with children
        // Root A with child A.1
        let root_a = NamespaceIdent::from_vec(vec!["a".to_string()]).unwrap();
        let root_a_ns = initialize_namespace(state.clone(), warehouse_id, &root_a, None).await;

        let child_a1 = NamespaceIdent::from_vec(vec!["a".to_string(), "1".to_string()]).unwrap();
        let child_a1_ns = initialize_namespace(state.clone(), warehouse_id, &child_a1, None).await;

        // Root B with child B.1
        let root_b = NamespaceIdent::from_vec(vec!["b".to_string()]).unwrap();
        let root_b_ns = initialize_namespace(state.clone(), warehouse_id, &root_b, None).await;

        let child_b1 = NamespaceIdent::from_vec(vec!["b".to_string(), "1".to_string()]).unwrap();
        let child_b1_ns = initialize_namespace(state.clone(), warehouse_id, &child_b1, None).await;

        // List all root namespaces
        let mut transaction = PostgresTransaction::begin_read(state.clone())
            .await
            .unwrap();

        let result = PostgresBackend::list_namespaces(
            warehouse_id,
            &ListNamespacesQuery {
                page_token: PageToken::NotSpecified,
                page_size: None,
                parent: None,
                return_uuids: false,
                return_protection_status: false,
            },
            transaction.transaction(),
        )
        .await
        .unwrap();

        // Should return both roots, both with no parents
        assert_eq!(result.parent_namespaces.len(), 2);
        let result = result.namespaces;
        assert_eq!(result.len(), 2);
        let result_map = result.into_hashmap();

        assert!(result_map[&root_a_ns.namespace_id()].is_root());
        assert!(result_map[&root_b_ns.namespace_id()].is_root());

        // List children of root A
        let mut transaction = PostgresTransaction::begin_read(state.clone())
            .await
            .unwrap();

        let result = PostgresBackend::list_namespaces(
            warehouse_id,
            &ListNamespacesQuery {
                page_token: PageToken::NotSpecified,
                page_size: None,
                parent: Some(root_a.clone()),
                return_uuids: false,
                return_protection_status: false,
            },
            transaction.transaction(),
        )
        .await
        .unwrap();

        // Should only return A.1 with correct parent
        assert_eq!(result.parent_namespaces.len(), 2);
        let result = result.namespaces;
        assert_eq!(result.len(), 1);
        let result_map = result.into_hashmap();

        let a1_hierarchy = &result_map[&child_a1_ns.namespace_id()];
        assert_eq!(
            a1_hierarchy.parent.unwrap(),
            (root_a_ns.namespace_id(), root_a_ns.version())
        );

        // List children of root B
        let mut transaction = PostgresTransaction::begin_read(state.clone())
            .await
            .unwrap();

        let result = PostgresBackend::list_namespaces(
            warehouse_id,
            &ListNamespacesQuery {
                page_token: PageToken::NotSpecified,
                page_size: None,
                parent: Some(root_b.clone()),
                return_uuids: false,
                return_protection_status: false,
            },
            transaction.transaction(),
        )
        .await
        .unwrap();

        // Should only return B.1 with correct parent
        assert_eq!(result.parent_namespaces.len(), 2);
        let result = result.namespaces;
        assert_eq!(result.len(), 1);
        let result_map = result.into_hashmap();

        let b1_hierarchy = &result_map[&child_b1_ns.namespace_id()];
        assert_eq!(
            b1_hierarchy.parent.unwrap(),
            (root_b_ns.namespace_id(), root_b_ns.version())
        );
    }

    #[sqlx::test]
    async fn test_list_namespaces_pagination_with_hierarchy(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;

        // Create parent and multiple children
        let parent = NamespaceIdent::from_vec(vec!["parent".to_string()]).unwrap();
        let parent_ns = initialize_namespace(state.clone(), warehouse_id, &parent, None).await;

        let child1 =
            NamespaceIdent::from_vec(vec!["parent".to_string(), "child1".to_string()]).unwrap();
        let child1_ns = initialize_namespace(state.clone(), warehouse_id, &child1, None).await;

        let child2 =
            NamespaceIdent::from_vec(vec!["parent".to_string(), "child2".to_string()]).unwrap();
        let child2_ns = initialize_namespace(state.clone(), warehouse_id, &child2, None).await;

        let child3 =
            NamespaceIdent::from_vec(vec!["parent".to_string(), "child3".to_string()]).unwrap();
        let child3_ns = initialize_namespace(state.clone(), warehouse_id, &child3, None).await;

        // List children with pagination (page_size = 2)
        let mut transaction = PostgresTransaction::begin_read(state.clone())
            .await
            .unwrap();

        let result = PostgresBackend::list_namespaces(
            warehouse_id,
            &ListNamespacesQuery {
                page_token: PageToken::NotSpecified,
                page_size: Some(2),
                parent: Some(parent.clone()),
                return_uuids: false,
                return_protection_status: false,
            },
            transaction.transaction(),
        )
        .await
        .unwrap();

        // First page: 2 children
        assert_eq!(result.parent_namespaces.len(), 3);
        let result = result.namespaces;
        assert_eq!(result.len(), 2);
        let next_token = result.next_token().map(ToString::to_string);
        assert!(next_token.is_some());

        let result_map = result.into_hashmap();

        // All returned children should have parent hierarchy
        assert!(
            result_map.contains_key(&child1_ns.namespace_id())
                || result_map.contains_key(&child2_ns.namespace_id())
                || result_map.contains_key(&child3_ns.namespace_id())
        );

        for hierarchy in result_map.values() {
            assert_eq!(
                hierarchy.parent.unwrap(),
                (parent_ns.namespace_id(), parent_ns.version())
            );
        }

        // Get second page
        let mut transaction = PostgresTransaction::begin_read(state.clone())
            .await
            .unwrap();

        let result = PostgresBackend::list_namespaces(
            warehouse_id,
            &ListNamespacesQuery {
                page_token: next_token.map_or(PageToken::Empty, PageToken::Present),
                page_size: Some(2),
                parent: Some(parent.clone()),
                return_uuids: false,
                return_protection_status: false,
            },
            transaction.transaction(),
        )
        .await
        .unwrap()
        .namespaces;

        // Second page: 1 child
        assert_eq!(result.len(), 1);
        let result_map = result.into_hashmap();

        // This child should also have parent hierarchy
        for hierarchy in result_map.values() {
            assert_eq!(
                hierarchy.parent.unwrap(),
                (parent_ns.namespace_id(), parent_ns.version())
            );
        }
    }

    #[sqlx::test]
    async fn test_list_namespaces_deep_hierarchy(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;

        // Create a 4-level deep hierarchy
        let level1 = NamespaceIdent::from_vec(vec!["level1".to_string()]).unwrap();
        let _level1_ns = initialize_namespace(state.clone(), warehouse_id, &level1, None).await;

        let level2 =
            NamespaceIdent::from_vec(vec!["level1".to_string(), "level2".to_string()]).unwrap();
        let _level2_ns = initialize_namespace(state.clone(), warehouse_id, &level2, None).await;

        let level3 = NamespaceIdent::from_vec(vec![
            "level1".to_string(),
            "level2".to_string(),
            "level3".to_string(),
        ])
        .unwrap();
        let _level3_ns = initialize_namespace(state.clone(), warehouse_id, &level3, None).await;

        let level4 = NamespaceIdent::from_vec(vec![
            "level1".to_string(),
            "level2".to_string(),
            "level3".to_string(),
            "level4".to_string(),
        ])
        .unwrap();
        let level4_ns = initialize_namespace(state.clone(), warehouse_id, &level4, None).await;

        // List at level 4 (deepest)
        let mut transaction = PostgresTransaction::begin_read(state.clone())
            .await
            .unwrap();

        let result = PostgresBackend::list_namespaces(
            warehouse_id,
            &ListNamespacesQuery {
                page_token: PageToken::NotSpecified,
                page_size: None,
                parent: Some(level3.clone()),
                return_uuids: false,
                return_protection_status: false,
            },
            transaction.transaction(),
        )
        .await
        .unwrap();

        let parents = result.parent_namespaces;
        let result = result.namespaces;
        assert_eq!(result.len(), 1);
        let result_map = result.into_hashmap();

        let level4_hierarchy = &result_map[&level4_ns.namespace_id()];

        // Verify parent chain: level3 -> level2 -> level1
        assert_eq!(parents.len(), 4);
        let parent3 = parents
            .get(&level4_hierarchy.parent_namespaces_id().unwrap())
            .unwrap();
        assert_eq!(parent3.namespace_ident(), &level3);
        let parent2 = parents
            .get(&parent3.parent_namespaces_id().unwrap())
            .unwrap();
        assert_eq!(parent2.namespace_ident(), &level2);
        let parent1 = parents
            .get(&parent2.parent_namespaces_id().unwrap())
            .unwrap();
        assert_eq!(parent1.namespace_ident(), &level1);
        assert!(parent1.is_root());
    }
}
