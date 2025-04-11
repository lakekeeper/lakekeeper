use std::{collections::HashMap, ops::Deref};

use chrono::Utc;
use http::StatusCode;
use iceberg_ext::catalog::rest::IcebergErrorResponse;
use itertools::{izip, Itertools};
use sqlx::types::Json;
use uuid::Uuid;

use super::dbutils::DBErrorHandler;
use crate::{
    api::{
        iceberg::v1::{namespace::NamespaceDropFlags, PaginatedMapping, MAX_PAGE_SIZE},
        management::v1::ProtectionResponse,
    },
    catalog::namespace::MAX_NAMESPACE_DEPTH,
    implementations::postgres::{
        pagination::{PaginateToken, V1PaginateToken},
        tabular::TabularType,
    },
    service::{
        storage::join_location, task_queue::TaskId, CreateNamespaceRequest,
        CreateNamespaceResponse, ErrorModel, GetNamespaceResponse, ListNamespacesQuery,
        NamespaceDropInfo, NamespaceIdent, NamespaceIdentUuid, NamespaceInfo, Result,
        TabularIdentUuid,
    },
    WarehouseIdent,
};

pub(crate) async fn get_namespace(
    warehouse_id: WarehouseIdent,
    namespace_id: NamespaceIdentUuid,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<GetNamespaceResponse> {
    let row = sqlx::query!(
        r#"
        SELECT 
            namespace_name as "namespace_name: Vec<String>",
            n.namespace_id,
            n.warehouse_id,
            namespace_properties as "properties: Json<Option<HashMap<String, String>>>"
        FROM namespace n
        INNER JOIN warehouse w ON n.warehouse_id = w.warehouse_id
        WHERE n.warehouse_id = $1 AND n.namespace_id = $2
        AND w.status = 'active'
        "#,
        *warehouse_id,
        *namespace_id
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|e| match e {
        sqlx::Error::RowNotFound => ErrorModel::builder()
            .code(StatusCode::NOT_FOUND.into())
            .message(format!(
                "Namespace with id {warehouse_id} not found in warehouse {namespace_id}"
            ))
            .r#type("NamespaceNotFound".to_string())
            .build(),
        _ => e.into_error_model("Error fetching namespace".to_string()),
    })?;

    Ok(GetNamespaceResponse {
        namespace: NamespaceIdent::from_vec(row.namespace_name.clone()).map_err(|e| {
            ErrorModel::internal(
                "Error converting namespace",
                "NamespaceConversionError",
                Some(Box::new(e)),
            )
        })?,
        properties: row.properties.deref().clone(),
        namespace_id: row.namespace_id.into(),
        warehouse_id: row.warehouse_id.into(),
    })
}

#[allow(clippy::too_many_lines)]
pub(crate) async fn list_namespaces(
    warehouse_id: WarehouseIdent,
    ListNamespacesQuery {
        page_token,
        page_size,
        parent,
        return_uuids: _,
        return_protection_status: _,
    }: &ListNamespacesQuery,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<PaginatedMapping<NamespaceIdentUuid, NamespaceInfo>> {
    let page_size = page_size.map_or(MAX_PAGE_SIZE, |i| i.clamp(1, MAX_PAGE_SIZE));

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

    let namespaces: Vec<(Uuid, Vec<String>, chrono::DateTime<Utc>, bool)> =
        if let Some(parent) = parent {
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
                n.protected
            FROM namespace n
            INNER JOIN warehouse w ON n.warehouse_id = w.warehouse_id
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
            .map_err(|e| e.into_error_model("Error fetching Namespace"))?
            .into_iter()
            .map(|r| (r.namespace_id, r.namespace_name, r.created_at, r.protected))
            .collect()
        } else {
            sqlx::query!(
                r#"
            SELECT
                n.namespace_id,
                "namespace_name" as "namespace_name: Vec<String>",
                n.created_at,
                n.protected
            FROM namespace n
            INNER JOIN warehouse w ON n.warehouse_id = w.warehouse_id
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
            .map_err(|e| e.into_error_model("Error fetching Namespace"))?
            .into_iter()
            .map(|r| (r.namespace_id, r.namespace_name, r.created_at, r.protected))
            .collect()
        };

    // Convert Vec<Vec<String>> to Vec<NamespaceIdent>
    let mut namespace_map: PaginatedMapping<NamespaceIdentUuid, NamespaceInfo> =
        PaginatedMapping::with_capacity(namespaces.len());
    for ns_result in namespaces.into_iter().map(|(id, n, ts, protected)| {
        NamespaceIdent::from_vec(n.clone())
            .map_err(|e| {
                IcebergErrorResponse::from(ErrorModel::internal(
                    "Error converting namespace",
                    "NamespaceConversionError",
                    Some(Box::new(e)),
                ))
            })
            .map(|n| {
                (
                    id.into(),
                    NamespaceInfo {
                        namespace_ident: n,
                        protected,
                    },
                    ts,
                )
            })
    }) {
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
    warehouse_id: WarehouseIdent,
    namespace_id: NamespaceIdentUuid,
    request: CreateNamespaceRequest,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<CreateNamespaceResponse> {
    let CreateNamespaceRequest {
        namespace,
        properties,
    } = request;

    let _namespace_id = sqlx::query_scalar!(
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
        RETURNING namespace_id
        "#,
        *warehouse_id,
        *namespace_id,
        &*namespace,
        serde_json::to_value(properties.clone()).map_err(|e| {
            ErrorModel::internal(
                "Error serializing namespace properties",
                "NamespacePropertiesSerializationError",
                Some(Box::new(e)),
            )
        })?
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|e| match e {
        sqlx::Error::Database(db_error) => {
            if db_error.is_unique_violation() {
                tracing::debug!("Namespace already exists: {db_error:?}");
                ErrorModel::conflict(
                    "Namespace already exists",
                    "NamespaceAlreadyExists",
                    Some(Box::new(db_error)),
                )
            } else if db_error.is_foreign_key_violation() {
                tracing::debug!("Namespace foreign key violation: {db_error:?}");
                ErrorModel::not_found(
                    "Warehouse not found",
                    "WarehouseNotFound",
                    Some(Box::new(db_error)),
                )
            } else {
                tracing::error!("Internal error creating namespace: {db_error:?}");
                ErrorModel::internal(
                    "Error creating namespace",
                    "NamespaceCreateError",
                    Some(Box::new(db_error)),
                )
            }
        }
        e @ sqlx::Error::RowNotFound => {
            tracing::debug!("Warehouse not found: {e:?}");
            ErrorModel::not_found(
                "Warehouse not found",
                "WarehouseNotFound",
                Some(Box::new(e)),
            )
        }
        _ => {
            tracing::error!("Internal error creating namespace: {e:?}");
            e.into_error_model("Error creating Namespace")
        }
    })?;

    // If inner is empty, return None
    let properties = properties.and_then(|h| if h.is_empty() { None } else { Some(h) });
    Ok(CreateNamespaceResponse {
        namespace,
        // Return None if properties is empty
        properties,
    })
}

pub(crate) async fn namespace_to_id(
    warehouse_id: WarehouseIdent,
    namespace: &NamespaceIdent,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<Option<NamespaceIdentUuid>> {
    let namespace_id = sqlx::query_scalar!(
        r#"
        SELECT namespace_id
        FROM namespace n
        INNER JOIN warehouse w ON n.warehouse_id = w.warehouse_id
        WHERE n.warehouse_id = $1 AND namespace_name = $2
        AND w.status = 'active'
        "#,
        *warehouse_id,
        &**namespace
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|e| match e {
        sqlx::Error::RowNotFound => None,
        _ => Some(e.into_error_model("Error fetching namespace".to_string())),
    });

    match namespace_id {
        Ok(namespace_id) => Ok(Some(namespace_id.into())),
        Err(Some(e)) => Err(e.into()),
        Err(None) => Ok(None),
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) async fn drop_namespace(
    warehouse_id: WarehouseIdent,
    namespace_id: NamespaceIdentUuid,
    NamespaceDropFlags {
        force,
        purge: _purge,
        recursive,
    }: NamespaceDropFlags,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<NamespaceDropInfo> {
    let info = sqlx::query!(r#"
        WITH namespace_info AS (
            SELECT namespace_name, protected
            FROM namespace
            WHERE warehouse_id = $1 AND namespace_id = $2
        ),
        child_namespaces AS (
            SELECT n.protected, n.namespace_id
            FROM namespace n
            INNER JOIN namespace_info ni ON n.namespace_name[1:array_length(ni.namespace_name, 1)] = ni.namespace_name
            WHERE n.warehouse_id = $1 AND n.namespace_id != $2
        ),
        tabulars AS (
            SELECT ta.tabular_id, fs_location, fs_protocol, ta.typ, protected
            FROM tabular ta
            WHERE namespace_id = $2 OR (namespace_id = ANY (SELECT namespace_id FROM child_namespaces))
        ),
        tasks AS (
            SELECT te.task_id, t.status as task_status from tabular_expirations te join task t on te.task_id = t.task_id
            WHERE t.status = 'running' AND te.tabular_id = ANY (SELECT tabular_id FROM tabulars)
        )
        SELECT
            (SELECT protected FROM namespace_info) AS "is_protected!",
            EXISTS (SELECT 1 FROM child_namespaces WHERE protected = true) AS "has_protected_namespaces!",
            EXISTS (SELECT 1 FROM tabulars WHERE protected = true) AS "has_protected_tabulars!",
            EXISTS (SELECT 1 FROM tasks WHERE task_status = 'running') AS "has_running_tasks!",
            ARRAY(SELECT tabular_id FROM tabulars) AS "child_tabulars!",
            ARRAY(SELECT namespace_id FROM child_namespaces) AS "child_namespaces!",
            ARRAY(SELECT fs_protocol FROM tabulars) AS "child_tabular_fs_protocol!",
            ARRAY(SELECT fs_location FROM tabulars) AS "child_tabular_fs_location!",
            ARRAY(SELECT typ FROM tabulars) AS "child_tabular_typ!: Vec<TabularType>",
            ARRAY(SELECT task_id FROM tasks) AS "child_tabular_task_id!: Vec<Uuid>"
"#,
        *warehouse_id,
        *namespace_id,
    ).fetch_one(&mut **transaction).await.map_err(|e|
        if let sqlx::Error::RowNotFound = e { ErrorModel::not_found(
            format!("Namespace {namespace_id} not found in warehouse {warehouse_id}"),
            "NamespaceNotFound",
            None,
        ) } else {
            tracing::warn!("Error fetching namespace: {e:?}");
            e.into_error_model("Error fetching namespace".to_string())
        }
    )?;

    if !recursive && (!info.child_tabulars.is_empty() || !info.child_namespaces.is_empty()) {
        return Err(
            ErrorModel::conflict("Namespace is not empty", "NamespaceNotEmpty", None).into(),
        );
    }

    if !force && info.is_protected {
        return Err(
            ErrorModel::conflict("Namespace is protected", "NamespaceProtected", None).into(),
        );
    }

    if !force && info.has_protected_namespaces {
        return Err(ErrorModel::conflict(
            "Namespace has protected child namespaces",
            "NamespaceNotEmpty",
            None,
        )
        .into());
    }

    if !force && info.has_protected_tabulars {
        return Err(ErrorModel::conflict(
            "Namespace has protected tabulars",
            "NamespaceNotEmpty",
            None,
        )
        .into());
    }

    if info.has_running_tasks {
        return Err(
            ErrorModel::conflict("Namespace has a currently running tabular expiration, please retry after the expiration task is done.", "NamespaceNotEmpty", None).into(),
        );
    }

    // Return 404 not found if namespace does not exist
    let record = sqlx::query!(
        r#"
        DELETE FROM namespace
            WHERE warehouse_id = $1
            -- If recursive is true, delete all child namespaces...
            AND (namespace_id = any($2) or namespace_id = $3)
            AND warehouse_id IN (
                SELECT warehouse_id FROM warehouse WHERE status = 'active'
            )
        "#,
        *warehouse_id,
        &info.child_namespaces,
        *namespace_id,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| match &e {
        sqlx::Error::Database(db_error) => {
            if db_error.is_foreign_key_violation() {
                ErrorModel::conflict("Namespace is not empty", "NamespaceNotEmpty", None)
            } else {
                e.into_error_model("Error deleting namespace")
            }
        }
        _ => e.into_error_model("Error deleting namespace"),
    })?;

    tracing::debug!(
        "Deleted {deleted_count} namespaces",
        deleted_count = record.rows_affected()
    );

    if record.rows_affected() == 0 {
        return Err(ErrorModel::internal(
            format!("Namespace {namespace_id} naaot found in warehouse {warehouse_id}"),
            "NamespaceNotFound",
            None,
        )
        .into());
    }

    Ok(NamespaceDropInfo {
        child_namespaces: info.child_namespaces.into_iter().map(Into::into).collect(),
        child_tables: izip!(
            info.child_tabulars,
            info.child_tabular_fs_protocol,
            info.child_tabular_fs_location,
            info.child_tabular_typ
        )
        .map(|(id, protocol, fs_location, typ)| {
            (
                match typ {
                    TabularType::Table => TabularIdentUuid::Table(id),
                    TabularType::View => TabularIdentUuid::View(id),
                },
                join_location(protocol.as_str(), fs_location.as_str()),
            )
        })
        .collect_vec(),
        open_tasks: info
            .child_tabular_task_id
            .into_iter()
            .map(TaskId::from)
            .collect(),
    })
}

pub(crate) async fn set_namespace_protected(
    namespace_id: NamespaceIdentUuid,
    protect: bool,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<ProtectionResponse> {
    let row = sqlx::query!(
        r#"
        UPDATE namespace
        SET protected = $1
        WHERE namespace_id = $2 AND warehouse_id IN (
            SELECT warehouse_id FROM warehouse WHERE status = 'active'
        )
        returning protected, updated_at
        "#,
        protect,
        *namespace_id
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|e| {
        if let sqlx::Error::RowNotFound = e {
            ErrorModel::not_found(
                format!("Namespace {namespace_id} not found"),
                "NamespaceNotFound",
                None,
            )
        } else {
            tracing::error!("Error setting namespace protection: {e:?}");
            e.into_error_model("Error setting namespace protection".to_string())
        }
    })?;

    Ok(ProtectionResponse {
        protected: row.protected,
        updated_at: row.updated_at,
    })
}

pub(crate) async fn get_namespace_protected(
    namespace_id: NamespaceIdentUuid,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<ProtectionResponse> {
    let row = sqlx::query!(
        r#"
        SELECT protected, updated_at
        FROM namespace
        WHERE namespace_id = $1 AND warehouse_id IN (
            SELECT warehouse_id FROM warehouse WHERE status = 'active'
        )
        "#,
        *namespace_id
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|e| {
        if let sqlx::Error::RowNotFound = e {
            ErrorModel::not_found(
                format!("Namespace {namespace_id} not found"),
                "NamespaceNotFound",
                None,
            )
        } else {
            tracing::error!("Error getting namespace protection status: {e:?}");
            e.into_error_model("Error getting namespace protection status".to_string())
        }
    })?;

    Ok(ProtectionResponse {
        protected: row.protected,
        updated_at: row.updated_at,
    })
}

pub(crate) async fn update_namespace_properties(
    warehouse_id: WarehouseIdent,
    namespace_id: NamespaceIdentUuid,
    properties: HashMap<String, String>,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<()> {
    let properties = serde_json::to_value(properties).map_err(|e| {
        ErrorModel::builder()
            .code(StatusCode::INTERNAL_SERVER_ERROR.into())
            .message("Error serializing namespace properties".to_string())
            .r#type("NamespacePropertiesSerializationError".to_string())
            .source(Some(Box::new(e)))
            .build()
    })?;

    sqlx::query!(
        r#"
        UPDATE namespace
        SET namespace_properties = $1
        WHERE warehouse_id = $2 AND namespace_id = $3
        AND warehouse_id IN (
            SELECT warehouse_id FROM warehouse WHERE status = 'active'
        )
        "#,
        properties,
        *warehouse_id,
        *namespace_id
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| e.into_error_model("Error updating namespace properties".to_string()))?;

    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use tracing_test::traced_test;

    use super::{
        super::{warehouse::test::initialize_warehouse, PostgresCatalog},
        *,
    };
    use crate::{
        api::iceberg::types::PageToken,
        implementations::postgres::{
            tabular::{
                set_tabular_protected,
                table::{load_tables, tests::initialize_table},
            },
            CatalogState, PostgresTransaction,
        },
        service::{Catalog as _, Transaction as _},
    };

    pub(crate) async fn initialize_namespace(
        state: CatalogState,
        warehouse_id: WarehouseIdent,
        namespace: &NamespaceIdent,
        properties: Option<HashMap<String, String>>,
    ) -> (NamespaceIdentUuid, CreateNamespaceResponse) {
        let mut transaction = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();

        let namespace_id = NamespaceIdentUuid::default();

        let response = PostgresCatalog::create_namespace(
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
        let properties = Some(HashMap::from_iter(vec![
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
        ]));

        let response =
            initialize_namespace(state.clone(), warehouse_id, &namespace, properties.clone()).await;

        let mut transaction = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let namespace_id =
            PostgresCatalog::namespace_to_id(warehouse_id, &namespace, transaction.transaction())
                .await
                .unwrap()
                .expect("Namespace not found");

        assert_eq!(response.1.namespace, namespace);
        assert_eq!(response.1.properties, properties);

        let response =
            PostgresCatalog::get_namespace(warehouse_id, namespace_id, transaction.transaction())
                .await
                .unwrap();

        drop(transaction);

        assert_eq!(response.namespace, namespace);
        assert_eq!(response.properties, properties);

        let mut transaction = PostgresTransaction::begin_read(state.clone())
            .await
            .unwrap();

        let response =
            PostgresCatalog::namespace_to_id(warehouse_id, &namespace, transaction.transaction())
                .await
                .unwrap()
                .is_some();

        assert!(response);

        let response = PostgresCatalog::list_namespaces(
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
        .unwrap();

        assert_eq!(
            response.into_hashmap(),
            HashMap::from_iter(vec![(
                namespace_id,
                NamespaceInfo {
                    namespace_ident: namespace.clone(),
                    protected: false
                }
            )])
        );

        let mut transaction = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();

        let new_props = HashMap::from_iter(vec![
            ("key2".to_string(), "updated_value".to_string()),
            ("new_key".to_string(), "new_value".to_string()),
        ]);
        PostgresCatalog::update_namespace_properties(
            warehouse_id,
            namespace_id,
            new_props.clone(),
            transaction.transaction(),
        )
        .await
        .unwrap();

        transaction.commit().await.unwrap();

        let mut t = PostgresTransaction::begin_read(state.clone())
            .await
            .unwrap();
        let response = PostgresCatalog::get_namespace(warehouse_id, namespace_id, t.transaction())
            .await
            .unwrap();
        drop(t);
        assert_eq!(response.properties, Some(new_props));

        let mut transaction = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();

        PostgresCatalog::drop_namespace(
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

        let namespaces = PostgresCatalog::list_namespaces(
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
        assert_eq!(
            namespaces.into_hashmap(),
            HashMap::from_iter(vec![(
                response1.0,
                NamespaceInfo {
                    namespace_ident: response1.1.namespace,
                    protected: false
                }
            )])
        );

        let mut t = PostgresTransaction::begin_read(state.clone())
            .await
            .unwrap();

        let namespaces = PostgresCatalog::list_namespaces(
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
        assert_eq!(
            namespaces.into_hashmap(),
            HashMap::from_iter(vec![
                (
                    response2.0,
                    NamespaceInfo {
                        namespace_ident: response2.1.namespace,
                        protected: false
                    }
                ),
                (
                    response3.0,
                    NamespaceInfo {
                        namespace_ident: response3.1.namespace,
                        protected: false,
                    }
                )
            ])
        );

        // last page is empty
        let namespaces = PostgresCatalog::list_namespaces(
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
    async fn test_cannot_drop_nonempty_namespace(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let warehouse_id = initialize_warehouse(state.clone(), None, None, None, true).await;
        let staged = false;
        let table = initialize_table(warehouse_id, state.clone(), staged, None, None).await;

        let mut transaction = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let namespace_id =
            namespace_to_id(warehouse_id, &table.namespace, transaction.transaction())
                .await
                .unwrap()
                .expect("Namespace not found");
        let result = drop_namespace(
            warehouse_id,
            namespace_id,
            NamespaceDropFlags::default(),
            transaction.transaction(),
        )
        .await
        .unwrap_err();

        assert_eq!(result.error.code, StatusCode::CONFLICT);
    }

    #[sqlx::test]
    async fn test_can_recursive_drop_nonempty_namespace(pool: sqlx::PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let warehouse_id = initialize_warehouse(state.clone(), None, None, None, true).await;
        let staged = false;
        let table = initialize_table(warehouse_id, state.clone(), staged, None, None).await;

        let mut transaction = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        let namespace_id =
            namespace_to_id(warehouse_id, &table.namespace, transaction.transaction())
                .await
                .unwrap()
                .expect("Namespace not found");
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

        transaction.commit().await.unwrap();

        let mut transaction = PostgresTransaction::begin_read(state.clone())
            .await
            .unwrap();
        let tables = load_tables(
            warehouse_id,
            [table.table_id].into_iter(),
            true,
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

        assert_eq!(result.error.code, StatusCode::CONFLICT);

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

        let response = PostgresCatalog::create_namespace(
            warehouse_id,
            NamespaceIdentUuid::default(),
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
        assert_eq!(response.namespace, namespace_1);

        let mut transaction = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();

        let response = PostgresCatalog::create_namespace(
            warehouse_id,
            NamespaceIdentUuid::default(),
            CreateNamespaceRequest {
                namespace: namespace_2.clone(),
                properties: None,
            },
            transaction.transaction(),
        )
        .await
        .unwrap_err();

        assert_eq!(response.error.code, StatusCode::CONFLICT);
        assert_eq!(response.error.r#type, "NamespaceAlreadyExists");
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

        PostgresCatalog::set_namespace_protected(response.0, true, transaction.transaction())
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

        assert_eq!(result.error.code, StatusCode::CONFLICT);
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

        PostgresCatalog::set_namespace_protected(response.0, true, transaction.transaction())
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

        set_namespace_protected(namespace_id, true, transaction.transaction())
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

        assert_eq!(err.error.code, StatusCode::CONFLICT);

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
        )
        .await;

        let mut transaction = PostgresTransaction::begin_write(state.clone())
            .await
            .unwrap();
        set_tabular_protected(
            TabularIdentUuid::Table(*tab.table_id),
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

        assert_eq!(err.error.code, StatusCode::CONFLICT);

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
