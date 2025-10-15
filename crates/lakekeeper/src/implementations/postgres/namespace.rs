use std::{collections::HashMap, ops::Deref, sync::Arc};

use iceberg::TableIdent;
use itertools::izip;
use sqlx::types::Json;
use uuid::Uuid;

use super::dbutils::DBErrorHandler;
use crate::{
    api::iceberg::v1::{namespace::NamespaceDropFlags, PaginatedMapping},
    implementations::postgres::{
        pagination::{PaginateToken, V1PaginateToken},
        tabular::TabularType,
    },
    server::namespace::MAX_NAMESPACE_DEPTH,
    service::{
        storage::join_location, tasks::TaskId, CatalogCreateNamespaceError,
        CatalogGetNamespaceError, CatalogListNamespaceError, CatalogNamespaceDropError,
        CatalogSetNamespaceProtectedError, CatalogUpdateNamespacePropertiesError,
        ChildNamespaceProtected, ChildTabularProtected, CreateNamespaceRequest,
        InvalidNamespaceIdentifier, ListNamespacesQuery, Namespace, NamespaceAlreadyExists,
        NamespaceDropInfo, NamespaceHasRunningTabularExpirations, NamespaceId, NamespaceIdent,
        NamespaceIdentOrId, NamespaceNotEmpty, NamespaceNotFound,
        NamespacePropertiesSerializationError, NamespaceProtected, Result, TabularId,
        WarehouseIdNotFound,
    },
    WarehouseId, CONFIG,
};

pub(crate) async fn get_namespace<'c, 'e: 'c, E: sqlx::Executor<'c, Database = sqlx::Postgres>>(
    warehouse_id: WarehouseId,
    namespace: NamespaceIdentOrId,
    connection: E,
) -> std::result::Result<Namespace, CatalogGetNamespaceError> {
    match namespace {
        NamespaceIdentOrId::Id(id) => get_namespace_by_id(warehouse_id, id, connection).await,
        NamespaceIdentOrId::Name(name) => {
            get_namespace_by_name(warehouse_id, &name, connection).await
        }
    }
}

pub(crate) async fn get_namespace_by_id<
    'c,
    'e: 'c,
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
>(
    warehouse_id: WarehouseId,
    namespace_id: NamespaceId,
    connection: E,
) -> std::result::Result<Namespace, CatalogGetNamespaceError> {
    let row = sqlx::query!(
        r#"
        SELECT 
            namespace_name as "namespace_name: Vec<String>",
            n.warehouse_id,
            n.protected,
            namespace_properties as "properties: Json<Option<HashMap<String, String>>>",
            n.updated_at
        FROM namespace n
        INNER JOIN warehouse w ON w.warehouse_id = $1
        WHERE n.warehouse_id = $1 AND n.namespace_id = $2
        AND w.status = 'active'
        "#,
        *warehouse_id,
        *namespace_id
    )
    .fetch_one(connection)
    .await
    .map_err(|e| match e {
        sqlx::Error::RowNotFound => CatalogGetNamespaceError::not_found(warehouse_id, namespace_id),
        _ => e.into_catalog_backend_error().into(),
    })?;

    Ok(Namespace {
        namespace_ident: parse_namespace_identifier_from_vec(
            &row.namespace_name,
            warehouse_id,
            namespace_id,
        )?,
        protected: row.protected,
        properties: row.properties.deref().clone().map(Arc::new),
        namespace_id,
        warehouse_id: row.warehouse_id.into(),
        updated_at: row.updated_at,
    })
}

pub(crate) async fn get_namespace_by_name<
    'c,
    'e: 'c,
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
>(
    warehouse_id: WarehouseId,
    namespace: &NamespaceIdent,
    connection: E,
) -> std::result::Result<Namespace, CatalogGetNamespaceError> {
    let row = sqlx::query!(
        r#"
        SELECT 
            n.namespace_id,
            n.warehouse_id,
            n.protected,
            namespace_properties as "properties: Json<Option<HashMap<String, String>>>",
            n.updated_at
        FROM namespace n
        INNER JOIN warehouse w ON w.warehouse_id = $1
        WHERE n.warehouse_id = $1 AND n.namespace_name = $2
        AND w.status = 'active'
        "#,
        *warehouse_id,
        &**namespace
    )
    .fetch_one(connection)
    .await
    .map_err(|e| match e {
        sqlx::Error::RowNotFound => {
            CatalogGetNamespaceError::not_found(warehouse_id, namespace.clone())
        }
        _ => e.into_catalog_backend_error().into(),
    })?;

    Ok(Namespace {
        namespace_ident: namespace.clone(),
        protected: row.protected,
        properties: row.properties.deref().clone().map(Arc::new),
        namespace_id: row.namespace_id.into(),
        warehouse_id: row.warehouse_id.into(),
        updated_at: row.updated_at,
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
) -> std::result::Result<PaginatedMapping<NamespaceId, Namespace>, CatalogListNamespaceError> {
    let page_size = CONFIG.page_size_or_pagination_max(*page_size);

    // Treat empty parent as None
    let parent = parent
        .as_ref()
        .and_then(|p| if p.is_empty() { None } else { Some(p.clone()) });
    let token = page_token
        .as_option()
        .map(|s| {
            PaginateToken::try_from(s)
                .map_err(|e| CatalogListNamespaceError::invalid_pagination_token(e.message, s))
        })
        .transpose()?;

    let (token_ts, token_id) = token
        .as_ref()
        .map(
            |PaginateToken::V1(V1PaginateToken { created_at, id }): &PaginateToken<Uuid>| {
                (created_at, id)
            },
        )
        .unzip();

    let namespaces: Vec<_> = if let Some(parent) = parent {
        // If it doesn't fit in a i32 it is way too large. Validation would have failed
        // already in the catalog.
        let parent_len: i32 = parent.len().try_into().unwrap_or(MAX_NAMESPACE_DEPTH + 1);

        // Namespace name field is an array.
        // Get all namespaces where the "name" array has
        // length(parent) + 1 elements, and the first length(parent)
        // elements are equal to parent.
        sqlx::query!(
            r#"
            SELECT
                n.namespace_id,
                "namespace_name" as "namespace_name: Vec<String>",
                n.created_at,
                n.protected,
                namespace_properties as "properties: Json<Option<HashMap<String, String>>>",
                n.updated_at
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
        .map_err(super::dbutils::DBErrorHandler::into_catalog_backend_error)?
        .into_iter()
        .map(|r| {
            (
                r.namespace_id,
                r.namespace_name,
                r.created_at,
                r.protected,
                r.properties.deref().clone(),
                r.updated_at,
            )
        })
        .collect()
    } else {
        sqlx::query!(
            r#"
            SELECT
                n.namespace_id,
                "namespace_name" as "namespace_name: Vec<String>",
                n.created_at,
                n.protected,
                namespace_properties as "properties: Json<Option<HashMap<String, String>>>",
                n.updated_at
            FROM namespace n
            INNER JOIN warehouse w ON w.warehouse_id = $1
            WHERE n.warehouse_id = $1
            AND array_length("namespace_name", 1) = 1
            AND w.status = 'active'
            AND ((n.created_at > $2 OR $2 IS NULL) OR (n.created_at = $2 AND n.namespace_id > $3))
            ORDER BY n.created_at, n.namespace_id ASC
            LIMIT $4
            "#,
            *warehouse_id,
            token_ts,
            token_id,
            page_size
        )
        .fetch_all(&mut **transaction)
        .await
        .map_err(super::dbutils::DBErrorHandler::into_catalog_backend_error)?
        .into_iter()
        .map(|r| {
            (
                r.namespace_id,
                r.namespace_name,
                r.created_at,
                r.protected,
                r.properties.deref().clone(),
                r.updated_at,
            )
        })
        .collect()
    };

    // Convert Vec<Vec<String>> to Vec<NamespaceIdent>
    let mut namespace_map: PaginatedMapping<NamespaceId, Namespace> =
        PaginatedMapping::with_capacity(namespaces.len());
    for ns_result in namespaces
        .into_iter()
        .map(|(id, n, ts, protected, properties, updated_at)| {
            parse_namespace_identifier_from_vec(&n, warehouse_id, id.into()).map(|n| {
                (
                    id.into(),
                    Namespace {
                        warehouse_id,
                        namespace_id: id.into(),
                        namespace_ident: n,
                        protected,
                        properties: properties.map(Arc::new),
                        updated_at,
                    },
                    ts,
                )
            })
        })
    {
        let (id, ns, created_at) = ns_result?;
        namespace_map.insert(
            id,
            ns,
            PaginateToken::V1(V1PaginateToken {
                id: *id,
                created_at,
            })
            .to_string(),
        );
    }

    Ok(namespace_map)
}

pub(crate) async fn create_namespace(
    warehouse_id: WarehouseId,
    namespace_id: NamespaceId,
    request: CreateNamespaceRequest,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> std::result::Result<Namespace, CatalogCreateNamespaceError> {
    let CreateNamespaceRequest {
        namespace,
        properties,
    } = request;

    let r = sqlx::query!(
        r#"
        INSERT INTO namespace (warehouse_id, namespace_id, namespace_name, namespace_properties)
        (
            SELECT $1, $2, $3, $4
            WHERE EXISTS (
                SELECT 1
                FROM warehouse
                WHERE warehouse_id = $1
                AND status = 'active'
        ))
        RETURNING namespace_id, updated_at
        "#,
        *warehouse_id,
        *namespace_id,
        &*namespace,
        serde_json::to_value(properties.clone()).map_err(|e| {
            NamespacePropertiesSerializationError::new(warehouse_id, namespace.clone(), e)
        })?
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

    // If inner is empty, return None
    let properties = properties.and_then(|h| if h.is_empty() { None } else { Some(h) });
    Ok(Namespace {
        namespace_ident: namespace,
        properties: properties.filter(|p| !p.is_empty()).map(Arc::new),
        protected: false,
        namespace_id,
        warehouse_id,
        updated_at: r.updated_at,
    })
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
    let namespace_ident =
        parse_namespace_identifier_from_vec(&info.namespace_name, warehouse_id, namespace_id)?;

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
                    join_location(protocol.as_str(), fs_location.as_str()),
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

fn parse_namespace_identifier_from_vec(
    namespace: &[String],
    warehouse_id: WarehouseId,
    namespace_id: NamespaceId,
) -> std::result::Result<NamespaceIdent, InvalidNamespaceIdentifier> {
    NamespaceIdent::from_vec(namespace.to_owned()).map_err(|_e| {
        InvalidNamespaceIdentifier::new(warehouse_id, format!("{namespace:?}"))
            .with_id(namespace_id)
            .append_detail("Namespace identifier can't be empty")
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
) -> std::result::Result<Namespace, CatalogSetNamespaceProtectedError> {
    let row = sqlx::query!(
        r#"
        UPDATE namespace
        SET protected = $1
        WHERE namespace_id = $2 AND warehouse_id IN (
            SELECT warehouse_id FROM warehouse WHERE status = 'active'
        )
        returning protected, updated_at, namespace_name as "namespace_name: Vec<String>", namespace_properties as "properties: Json<Option<HashMap<String, String>>>"
        "#,
        protect,
        *namespace_id
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|e| {
        if let sqlx::Error::RowNotFound = e {
            CatalogSetNamespaceProtectedError::from(NamespaceNotFound::new(warehouse_id, namespace_id))
        } else {
            tracing::error!("Error setting namespace protection: {e:?}");
            e.into_catalog_backend_error().into()
        }
    })?;

    Ok(Namespace {
        namespace_ident: parse_namespace_identifier_from_vec(
            &row.namespace_name,
            warehouse_id,
            namespace_id,
        )?,
        protected: row.protected,
        properties: row.properties.deref().clone().map(Arc::new),
        namespace_id,
        warehouse_id,
        updated_at: row.updated_at,
    })
}

pub(crate) async fn update_namespace_properties(
    warehouse_id: WarehouseId,
    namespace_id: NamespaceId,
    properties: HashMap<String, String>,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> std::result::Result<Namespace, CatalogUpdateNamespacePropertiesError> {
    let properties = serde_json::to_value(properties)
        .map_err(|e| NamespacePropertiesSerializationError::new(warehouse_id, namespace_id, e))?;

    let row = sqlx::query!(
        r#"
        UPDATE namespace
        SET namespace_properties = $1
        WHERE warehouse_id = $2 AND namespace_id = $3
        AND warehouse_id IN (
            SELECT warehouse_id FROM warehouse WHERE status = 'active'
        )
        RETURNING namespace_name as "namespace_name: Vec<String>", protected, updated_at, namespace_properties as "properties: Json<Option<HashMap<String, String>>>"
        "#,
        properties,
        *warehouse_id,
        *namespace_id
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|e| match e {
        sqlx::Error::RowNotFound => CatalogUpdateNamespacePropertiesError::from(NamespaceNotFound::new(warehouse_id, namespace_id)),
        _ => e.into_catalog_backend_error().into(),
    })?;

    Ok(Namespace {
        namespace_ident: parse_namespace_identifier_from_vec(
            &row.namespace_name,
            warehouse_id,
            namespace_id,
        )?,
        protected: row.protected,
        properties: row.properties.deref().clone().map(Arc::new),
        namespace_id,
        warehouse_id,
        updated_at: row.updated_at,
    })
}

#[cfg(test)]
pub(crate) mod tests {
    use tracing_test::traced_test;

    use super::{
        super::{warehouse::test::initialize_warehouse, PostgresBackend},
        *,
    };
    use crate::{
        api::iceberg::{types::PageToken, v1::tables::LoadTableFilters},
        implementations::postgres::{
            tabular::{
                set_tabular_protected,
                table::{load_tables, tests::initialize_table},
            },
            CatalogState, PostgresTransaction,
        },
        service::{CatalogNamespaceOps, Transaction as _},
    };

    pub(crate) async fn initialize_namespace(
        state: CatalogState,
        warehouse_id: WarehouseId,
        namespace: &NamespaceIdent,
        properties: Option<HashMap<String, String>>,
    ) -> (NamespaceId, Namespace) {
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

        (namespace_id, response)
    }

    #[sqlx::test]
    async fn test_namespace_lifecycle(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let warehouse_id = initialize_warehouse(state.clone(), None, None, None, true).await;

        let namespace = NamespaceIdent::from_vec(vec!["test".to_string()]).unwrap();
        let properties = HashMap::from_iter(vec![
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
        ]);

        let response = initialize_namespace(
            state.clone(),
            warehouse_id,
            &namespace,
            Some(properties.clone()),
        )
        .await;

        let namespace_id =
            PostgresBackend::require_namespace(warehouse_id, &namespace, state.clone())
                .await
                .unwrap()
                .namespace_id;

        assert_eq!(response.1.namespace_ident, namespace);
        assert_eq!(response.1.properties.unwrap(), properties.clone().into());

        let response =
            PostgresBackend::require_namespace(warehouse_id, namespace_id, state.clone())
                .await
                .unwrap();

        assert_eq!(response.namespace_ident, namespace);
        assert_eq!(response.properties.unwrap(), properties.into());

        let mut transaction = PostgresTransaction::begin_read(state.clone())
            .await
            .unwrap();

        let _response = PostgresBackend::require_namespace(warehouse_id, &namespace, state.clone())
            .await
            .unwrap();

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
        .into_hashmap();

        assert_eq!(response.len(), 1);
        assert_eq!(response[&namespace_id].namespace_ident, namespace);

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

        let response =
            PostgresBackend::require_namespace(warehouse_id, namespace_id, state.clone())
                .await
                .unwrap();
        assert_eq!(&*response.properties.unwrap(), &new_props);

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

        let warehouse_id = initialize_warehouse(state.clone(), None, None, None, true).await;
        let namespace = NamespaceIdent::from_vec(vec!["test".to_string()]).unwrap();
        let properties = Some(HashMap::from_iter(vec![
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
        ]));

        let response1 =
            initialize_namespace(state.clone(), warehouse_id, &namespace, properties.clone()).await;

        let namespace = NamespaceIdent::from_vec(vec!["test2".to_string()]).unwrap();
        let properties = Some(HashMap::from_iter(vec![
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
        ]));
        let response2 =
            initialize_namespace(state.clone(), warehouse_id, &namespace, properties.clone()).await;
        let namespace = NamespaceIdent::from_vec(vec!["test3".to_string()]).unwrap();
        let properties = Some(HashMap::from_iter(vec![
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
        ]));
        let response3 =
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
        .unwrap();
        let next_page_token = namespaces.next_token().map(ToString::to_string);
        assert_eq!(namespaces.len(), 1);
        let namespaces = namespaces.into_hashmap();
        assert_eq!(
            namespaces[&response1.0].namespace_ident,
            response1.1.namespace_ident
        );
        assert!(!namespaces[&response1.0].protected);

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
        .unwrap();
        let next_page_token = namespaces.next_token().map(ToString::to_string);
        assert_eq!(namespaces.len(), 2);
        assert!(next_page_token.is_some());
        let namespaces = namespaces.into_hashmap();

        assert_eq!(
            namespaces[&response2.0].namespace_ident,
            response2.1.namespace_ident
        );
        assert!(!namespaces[&response2.0].protected);
        assert_eq!(
            namespaces[&response3.0].namespace_ident,
            response3.1.namespace_ident
        );
        assert!(!namespaces[&response3.0].protected);

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
        .unwrap();

        assert_eq!(namespaces.next_token(), None);
        assert_eq!(namespaces.into_hashmap(), HashMap::new());
    }

    #[sqlx::test]
    async fn test_get_nonexistent_namespace(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let warehouse_id = initialize_warehouse(state.clone(), None, None, None, true).await;

        let result = PostgresBackend::require_namespace(
            warehouse_id,
            NamespaceId::new_random(),
            state.clone(),
        )
        .await
        .unwrap_err();

        assert!(matches!(
            result,
            CatalogGetNamespaceError::NamespaceNotFound(_)
        ));
    }

    #[sqlx::test]
    async fn test_drop_nonexistent_namespace(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let warehouse_id = initialize_warehouse(state.clone(), None, None, None, true).await;

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

        let warehouse_id = initialize_warehouse(state.clone(), None, None, None, true).await;
        let staged = false;
        let table = initialize_table(warehouse_id, state.clone(), staged, None, None, None).await;

        let namespace_id = get_namespace(warehouse_id, table.namespace.into(), &state.read_pool())
            .await
            .unwrap()
            .namespace_id;
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

        let warehouse_id = initialize_warehouse(state.clone(), None, None, None, true).await;
        let staged = false;
        let table = initialize_table(warehouse_id, state.clone(), staged, None, None, None).await;

        let namespace_id = get_namespace(warehouse_id, table.namespace.into(), &state.read_pool())
            .await
            .unwrap()
            .namespace_id;

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

        let warehouse_id = initialize_warehouse(state.clone(), None, None, None, true).await;
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
            response.0,
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
            response2.0,
            NamespaceDropFlags::default(),
            transaction.transaction(),
        )
        .await
        .unwrap();

        drop_namespace(
            warehouse_id,
            response.0,
            NamespaceDropFlags::default(),
            transaction.transaction(),
        )
        .await
        .unwrap();
    }

    #[sqlx::test]
    async fn test_can_recursive_drop_namespace_with_sub_namespaces(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let warehouse_id = initialize_warehouse(state.clone(), None, None, None, true).await;
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
            response.0,
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
        .unwrap();
        transaction.commit().await.unwrap();

        assert_eq!(ns.len(), 0);
    }

    #[sqlx::test]
    async fn test_case_insensitive_but_preserve_case(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let warehouse_id = initialize_warehouse(state.clone(), None, None, None, true).await;
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
        assert_eq!(response.namespace_ident, namespace_1);

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

        let warehouse_id = initialize_warehouse(state.clone(), None, None, None, true).await;
        let namespace = NamespaceIdent::from_vec(vec!["test".to_string()]).unwrap();

        let response = initialize_namespace(state.clone(), warehouse_id, &namespace, None).await;

        let mut transaction = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();

        PostgresBackend::set_namespace_protected(
            warehouse_id,
            response.0,
            true,
            transaction.transaction(),
        )
        .await
        .unwrap();

        let result = drop_namespace(
            warehouse_id,
            response.0,
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

        let warehouse_id = initialize_warehouse(state.clone(), None, None, None, true).await;
        let namespace = NamespaceIdent::from_vec(vec!["test".to_string()]).unwrap();

        let response = initialize_namespace(state.clone(), warehouse_id, &namespace, None).await;

        let mut transaction = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();

        PostgresBackend::set_namespace_protected(
            warehouse_id,
            response.0,
            true,
            transaction.transaction(),
        )
        .await
        .unwrap();

        let result = drop_namespace(
            warehouse_id,
            response.0,
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
    #[traced_test]
    async fn test_can_recursive_force_drop_nonempty_protected_namespace(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let warehouse_id = initialize_warehouse(state.clone(), None, None, None, true).await;
        let outer_namespace = NamespaceIdent::from_vec(vec!["test".to_string()]).unwrap();

        let (namespace_id, _) =
            initialize_namespace(state.clone(), warehouse_id, &outer_namespace, None).await;

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

        let warehouse_id = initialize_warehouse(state.clone(), None, None, None, true).await;
        let outer_namespace = NamespaceIdent::from_vec(vec!["test".to_string()]).unwrap();

        let (namespace_id, _) =
            initialize_namespace(state.clone(), warehouse_id, &outer_namespace, None).await;
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
}
