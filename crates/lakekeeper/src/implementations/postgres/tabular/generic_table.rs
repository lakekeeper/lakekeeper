use std::collections::HashMap;

use iceberg::TableIdent;
use uuid::Uuid;

use super::{super::dbutils::DBErrorHandler as _, CreateTabular, TabularType};
use crate::{
    CONFIG, WarehouseId,
    implementations::postgres::{
        namespace::parse_namespace_identifier_from_vec,
        pagination::{PaginateToken, V1PaginateToken},
    },
    service::{
        CatalogBackendError, CreateGenericTableError, DropGenericTableError,
        GenericTableAlreadyExists, GenericTableCreation, GenericTableFormat, GenericTableId,
        GenericTableInfo, GenericTableListEntry, GenericTableNotFound, ListGenericTablesError,
        LoadGenericTableError, NamespaceId, NamespaceVersion, TabularId, WarehouseVersion,
        storage::join_location,
    },
};

struct GenericTableFullRow {
    generic_table_id: Uuid,
    warehouse_version: i64,
    namespace_id: Uuid,
    namespace_version: i64,
    namespace_name: Vec<String>,
    name: String,
    format: String,
    fs_location: String,
    fs_protocol: String,
    protected: bool,
    doc: Option<String>,
    schema_info: Option<serde_json::Value>,
    statistics: Option<serde_json::Value>,
    property_keys: Option<Vec<String>>,
    property_values: Option<Vec<String>>,
}

struct GenericTableListRow {
    generic_table_id: Uuid,
    warehouse_id: Uuid,
    namespace_id: Uuid,
    name: String,
    format: String,
    protected: bool,
    created_at: chrono::DateTime<chrono::Utc>,
}

fn full_row_to_info(
    row: GenericTableFullRow,
    warehouse_id: WarehouseId,
) -> Result<GenericTableInfo, CatalogBackendError> {
    let namespace_ident = parse_namespace_identifier_from_vec(
        &row.namespace_name,
        warehouse_id,
        Some(row.namespace_id),
    )
    .map_err(CatalogBackendError::new_unexpected)?;

    let location = join_location(&row.fs_protocol, &row.fs_location)
        .map_err(CatalogBackendError::new_unexpected)?;

    let properties = if let (Some(keys), Some(values)) = (row.property_keys, row.property_values) {
        keys.into_iter().zip(values).collect()
    } else {
        std::collections::HashMap::new()
    };

    let name = row.name;
    let tabular_ident = TableIdent {
        namespace: namespace_ident.clone(),
        name: name.clone(),
    };

    Ok(GenericTableInfo {
        generic_table_id: row.generic_table_id.into(),
        warehouse_id,
        warehouse_version: WarehouseVersion::new(row.warehouse_version),
        namespace_id: row.namespace_id.into(),
        namespace_version: NamespaceVersion::new(row.namespace_version),
        namespace_ident,
        name,
        tabular_ident,
        location,
        properties,
        protected: row.protected,
        format: GenericTableFormat::from(row.format),
        doc: row.doc,
        schema: row.schema_info,
        statistics: row.statistics,
    })
}

pub(crate) async fn create_generic_table(
    creation: GenericTableCreation,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<GenericTableInfo, CreateGenericTableError> {
    let id: Uuid = *creation.generic_table_id;

    let tabular_info = super::create_tabular(
        CreateTabular {
            id,
            name: &creation.name,
            namespace_id: *creation.namespace_id,
            warehouse_id: *creation.warehouse_id,
            typ: TabularType::GenericTable,
            metadata_location: None,
            location: &creation.location,
        },
        transaction,
    )
    .await
    .map_err(|e| {
        use crate::service::CreateTabularError;
        match e {
            CreateTabularError::TabularAlreadyExists(_) => {
                CreateGenericTableError::from(GenericTableAlreadyExists::new())
            }
            CreateTabularError::CatalogBackendError(e) => CreateGenericTableError::from(e),
            other => CreateGenericTableError::from(CatalogBackendError::new_unexpected(other)),
        }
    })?;

    let format_str = creation.format.as_str();
    let schema_clone = creation.schema.clone();
    let statistics_clone = creation.statistics.clone();
    sqlx::query!(
        r#"INSERT INTO generic_table (warehouse_id, generic_table_id, format, doc, schema_info, statistics)
        VALUES ($1, $2, $3, $4, $5, $6)"#,
        *creation.warehouse_id,
        id,
        format_str,
        creation.doc.as_deref(),
        schema_clone as Option<serde_json::Value>,
        statistics_clone as Option<serde_json::Value>,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| CreateGenericTableError::from(e.into_catalog_backend_error()))?;

    if !creation.properties.is_empty() {
        let keys: Vec<&str> = creation.properties.keys().map(String::as_str).collect();
        let values: Vec<&str> = creation.properties.values().map(String::as_str).collect();
        sqlx::query!(
            r#"INSERT INTO generic_table_properties (warehouse_id, generic_table_id, key, value)
            SELECT $1, $2, UNNEST($3::text[]), UNNEST($4::text[])"#,
            *creation.warehouse_id,
            id,
            &keys as &[&str],
            &values as &[&str],
        )
        .execute(&mut **transaction)
        .await
        .map_err(|e| CreateGenericTableError::from(e.into_catalog_backend_error()))?;
    }

    let generic_tabular = tabular_info
        .into_generic_table_info()
        .expect("create_tabular returned GenericTable type");

    let tabular_ident = TableIdent {
        namespace: generic_tabular.tabular_ident.namespace.clone(),
        name: generic_tabular.tabular_ident.name.clone(),
    };

    Ok(GenericTableInfo {
        generic_table_id: id.into(),
        warehouse_id: generic_tabular.warehouse_id,
        warehouse_version: generic_tabular.warehouse_version,
        namespace_id: generic_tabular.namespace_id,
        namespace_version: generic_tabular.namespace_version,
        namespace_ident: tabular_ident.namespace.clone(),
        name: tabular_ident.name.clone(),
        tabular_ident,
        location: generic_tabular.location,
        properties: creation.properties,
        protected: generic_tabular.protected,
        format: creation.format,
        doc: creation.doc,
        schema: creation.schema,
        statistics: creation.statistics,
    })
}

pub(crate) async fn load_generic_table(
    warehouse_id: WarehouseId,
    namespace_id: NamespaceId,
    table_name: &str,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<GenericTableInfo, LoadGenericTableError> {
    let row = sqlx::query_as!(
        GenericTableFullRow,
        r#"
        SELECT
            t.tabular_id as generic_table_id,
            w.version as "warehouse_version!",
            t.namespace_id,
            n.version as "namespace_version!",
            t.tabular_namespace_name as "namespace_name!",
            t.name,
            gt.format,
            t.fs_location,
            t.fs_protocol,
            t.protected,
            gt.doc,
            gt.schema_info,
            gt.statistics,
            gtp.keys as property_keys,
            gtp.values as property_values
        FROM tabular t
        INNER JOIN generic_table gt ON gt.warehouse_id = t.warehouse_id AND gt.generic_table_id = t.tabular_id
        INNER JOIN warehouse w ON w.warehouse_id = t.warehouse_id AND w.status = 'active'
        INNER JOIN namespace n ON n.namespace_id = t.namespace_id AND n.warehouse_id = t.warehouse_id
        LEFT JOIN (
            SELECT generic_table_id,
                   ARRAY_AGG(key) as keys,
                   ARRAY_AGG(value) as values
            FROM generic_table_properties
            WHERE warehouse_id = $1
            GROUP BY generic_table_id
        ) gtp ON t.tabular_id = gtp.generic_table_id
        WHERE t.warehouse_id = $1
          AND t.namespace_id = $2
          AND t.name = $3
          AND t.typ = 'generic-table'
          AND t.deleted_at IS NULL
        "#,
        *warehouse_id,
        *namespace_id,
        table_name,
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|e| LoadGenericTableError::from(e.into_catalog_backend_error()))?
    .ok_or_else(|| LoadGenericTableError::from(GenericTableNotFound::new()))?;

    full_row_to_info(row, warehouse_id).map_err(LoadGenericTableError::from)
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
        SELECT
            t.tabular_id as generic_table_id,
            t.warehouse_id,
            t.namespace_id,
            t.name,
            gt.format,
            t.protected,
            t.created_at
        FROM tabular t
        INNER JOIN generic_table gt ON gt.warehouse_id = t.warehouse_id AND gt.generic_table_id = t.tabular_id
        INNER JOIN warehouse w ON w.warehouse_id = t.warehouse_id AND w.status = 'active'
        WHERE t.warehouse_id = $1
          AND t.namespace_id = $2
          AND t.typ = 'generic-table'
          AND t.deleted_at IS NULL
          AND (
            ($3::timestamptz IS NULL)
            OR (t.created_at, t.tabular_id) > ($3, $4)
          )
        ORDER BY t.created_at ASC, t.tabular_id ASC
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

        let tabular_ident = TableIdent::new(namespace_ident.clone(), row.name.clone());
        entries.push(GenericTableListEntry {
            generic_table_id: row.generic_table_id.into(),
            warehouse_id: row.warehouse_id.into(),
            namespace_id: row.namespace_id.into(),
            name: row.name.clone(),
            tabular_ident,
            format: GenericTableFormat::from(row.format.clone()),
            namespace_ident: namespace_ident.clone(),
            protected: row.protected,
            properties: HashMap::new(),
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
    let row = sqlx::query!(
        r#"
        SELECT t.tabular_id as generic_table_id
        FROM tabular t
        INNER JOIN warehouse w ON w.warehouse_id = t.warehouse_id AND w.status = 'active'
        WHERE t.warehouse_id = $1
          AND t.namespace_id = $2
          AND t.name = $3
          AND t.typ = 'generic-table'
          AND t.deleted_at IS NULL
        "#,
        *warehouse_id,
        *namespace_id,
        table_name,
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|e| DropGenericTableError::from(e.into_catalog_backend_error()))?
    .ok_or_else(|| DropGenericTableError::from(GenericTableNotFound::new()))?;

    let generic_table_id: GenericTableId = row.generic_table_id.into();

    super::drop_tabular(
        warehouse_id,
        TabularId::GenericTable(generic_table_id),
        false,
        None,
        transaction,
    )
    .await
    .map_err(|e| {
        use crate::service::DropTabularError;
        match e {
            DropTabularError::TabularNotFound(_) => {
                DropGenericTableError::from(GenericTableNotFound::new())
            }
            DropTabularError::CatalogBackendError(e) => DropGenericTableError::from(e),
            other => DropGenericTableError::from(CatalogBackendError::new_unexpected(other)),
        }
    })?;

    Ok(generic_table_id)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use iceberg::NamespaceIdent;
    use sqlx::PgPool;
    use uuid::Uuid;

    use super::*;
    use crate::{
        implementations::postgres::{
            CatalogState, namespace::tests::initialize_namespace,
            warehouse::test::initialize_warehouse,
        },
        service::{GenericTableCreation, GenericTableFormat, GenericTableId, NamespaceId},
    };

    async fn setup(pool: PgPool) -> (CatalogState, PgPool, crate::WarehouseId, NamespaceId) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_project_id, warehouse_id) =
            initialize_warehouse(state.clone(), None, None, None, true).await;
        let namespace = NamespaceIdent::new(Uuid::now_v7().to_string());
        let ns = initialize_namespace(state.clone(), warehouse_id, &namespace, None).await;
        let namespace_id = ns.namespace_id();
        (state, pool, warehouse_id, namespace_id)
    }

    fn test_creation(
        warehouse_id: crate::WarehouseId,
        namespace_id: NamespaceId,
        name: &str,
    ) -> GenericTableCreation {
        GenericTableCreation {
            generic_table_id: GenericTableId::from(Uuid::now_v7()),
            namespace_id,
            warehouse_id,
            name: name.to_string(),
            format: GenericTableFormat::Unknown("lance".to_string()),
            location: format!("s3://bucket/path/{name}").parse().unwrap(),
            doc: Some("test doc".to_string()),
            schema: None,
            statistics: None,
            properties: HashMap::from([("key".to_string(), "value".to_string())]),
        }
    }

    #[sqlx::test]
    async fn test_create_and_load(pool: PgPool) {
        let (_state, pool, warehouse_id, namespace_id) = setup(pool).await;
        let creation = test_creation(warehouse_id, namespace_id, "test-gt");

        let mut t = pool.begin().await.unwrap();
        let info = create_generic_table(creation.clone(), &mut t)
            .await
            .unwrap();
        t.commit().await.unwrap();

        assert_eq!(info.name, "test-gt");
        assert_eq!(
            info.format,
            GenericTableFormat::Unknown("lance".to_string())
        );
        assert_eq!(info.doc, Some("test doc".to_string()));
        assert_eq!(info.properties.get("key").unwrap(), "value");

        // Load it back
        let mut t = pool.begin().await.unwrap();
        let loaded = load_generic_table(warehouse_id, namespace_id, "test-gt", &mut t)
            .await
            .unwrap();
        t.commit().await.unwrap();

        assert_eq!(loaded.generic_table_id, info.generic_table_id);
        assert_eq!(loaded.name, "test-gt");
        assert_eq!(loaded.properties.get("key").unwrap(), "value");
    }

    #[sqlx::test]
    async fn test_create_duplicate_fails(pool: PgPool) {
        let (_state, pool, warehouse_id, namespace_id) = setup(pool).await;

        let mut t = pool.begin().await.unwrap();
        create_generic_table(test_creation(warehouse_id, namespace_id, "dup"), &mut t)
            .await
            .unwrap();
        t.commit().await.unwrap();

        let mut t = pool.begin().await.unwrap();
        let err = create_generic_table(test_creation(warehouse_id, namespace_id, "dup"), &mut t)
            .await
            .expect_err("duplicate should fail");
        t.rollback().await.ok();

        assert!(
            matches!(err, CreateGenericTableError::GenericTableAlreadyExists(_)),
            "expected AlreadyExists, got: {err:?}"
        );
    }

    #[sqlx::test]
    async fn test_list_with_pagination(pool: PgPool) {
        let (_state, pool, warehouse_id, namespace_id) = setup(pool).await;
        let ns_ident = NamespaceIdent::new("test".to_string());

        // Create 3 tables
        for name in ["a", "b", "c"] {
            let mut t = pool.begin().await.unwrap();
            create_generic_table(test_creation(warehouse_id, namespace_id, name), &mut t)
                .await
                .unwrap();
            t.commit().await.unwrap();
        }

        // List with page_size=2
        let mut t = pool.begin().await.unwrap();
        let (page1, token) =
            list_generic_tables(warehouse_id, namespace_id, &ns_ident, Some(2), None, &mut t)
                .await
                .unwrap();
        t.commit().await.unwrap();

        assert_eq!(page1.len(), 2);
        assert!(token.is_some());

        // Second page
        let mut t = pool.begin().await.unwrap();
        let (page2, token2) = list_generic_tables(
            warehouse_id,
            namespace_id,
            &ns_ident,
            Some(2),
            token.as_deref(),
            &mut t,
        )
        .await
        .unwrap();
        t.commit().await.unwrap();

        assert_eq!(page2.len(), 1);
        // token2 may be Some (pointing past the last item) — fetch next page to confirm empty
        if token2.is_some() {
            let mut t = pool.begin().await.unwrap();
            let (page3, _) = list_generic_tables(
                warehouse_id,
                namespace_id,
                &ns_ident,
                Some(2),
                token2.as_deref(),
                &mut t,
            )
            .await
            .unwrap();
            t.commit().await.unwrap();
            assert!(page3.is_empty());
        }
    }

    #[sqlx::test]
    async fn test_drop_and_verify_gone(pool: PgPool) {
        let (_state, pool, warehouse_id, namespace_id) = setup(pool).await;

        let mut t = pool.begin().await.unwrap();
        create_generic_table(test_creation(warehouse_id, namespace_id, "to-drop"), &mut t)
            .await
            .unwrap();
        t.commit().await.unwrap();

        let mut t = pool.begin().await.unwrap();
        let dropped_id = drop_generic_table(warehouse_id, namespace_id, "to-drop", &mut t)
            .await
            .unwrap();
        t.commit().await.unwrap();
        assert_ne!(*dropped_id, Uuid::nil());

        // Verify load fails
        let mut t = pool.begin().await.unwrap();
        let err = load_generic_table(warehouse_id, namespace_id, "to-drop", &mut t)
            .await
            .expect_err("should be gone");
        t.commit().await.unwrap();

        assert!(
            matches!(err, LoadGenericTableError::GenericTableNotFound(_)),
            "expected NotFound, got: {err:?}"
        );
    }

    #[sqlx::test]
    async fn test_drop_not_found(pool: PgPool) {
        let (_state, pool, warehouse_id, namespace_id) = setup(pool).await;

        let mut t = pool.begin().await.unwrap();
        let err = drop_generic_table(warehouse_id, namespace_id, "ghost", &mut t)
            .await
            .expect_err("should not exist");
        t.rollback().await.ok();

        assert!(
            matches!(err, DropGenericTableError::GenericTableNotFound(_)),
            "expected NotFound, got: {err:?}"
        );
    }

    #[sqlx::test]
    async fn test_load_not_found(pool: PgPool) {
        let (_state, pool, warehouse_id, namespace_id) = setup(pool).await;

        let mut t = pool.begin().await.unwrap();
        let err = load_generic_table(warehouse_id, namespace_id, "nope", &mut t)
            .await
            .expect_err("should not exist");
        t.commit().await.unwrap();

        assert!(
            matches!(err, LoadGenericTableError::GenericTableNotFound(_)),
            "expected NotFound, got: {err:?}"
        );
    }
}
