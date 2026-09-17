use iceberg::TableIdent;
use lakekeeper::{
    CONFIG, WarehouseId,
    service::{
        CatalogBackendError, CreateDatasetError, CreateTabularError, DatasetAlreadyExists,
        DatasetConstraints, DatasetCreation, DatasetId, DatasetInfo, DatasetListEntry,
        DatasetNotFound, DatasetOwnership, DropDatasetError, DropTabularError, ListDatasetsError,
        LoadDatasetError, NamespaceId, NamespaceVersion, TabularId, WarehouseVersion,
        storage::join_location,
    },
};
use uuid::Uuid;

use super::{super::dbutils::DBErrorHandler as _, CreateTabular, TabularType};
use crate::{
    namespace::parse_namespace_identifier_from_vec,
    pagination::{PaginateToken, V1PaginateToken},
};

struct DatasetFullRow {
    dataset_id: Uuid,
    warehouse_version: i64,
    namespace_id: Uuid,
    namespace_version: i64,
    namespace_name: Vec<String>,
    name: String,
    fs_location: String,
    fs_protocol: String,
    protected: bool,
    managed: bool,
    constraints: Option<serde_json::Value>,
}

struct DatasetListRow {
    dataset_id: Uuid,
    warehouse_id: Uuid,
    namespace_id: Uuid,
    name: String,
    protected: bool,
    managed: bool,
    created_at: chrono::DateTime<chrono::Utc>,
}

fn ownership_of(managed: bool) -> DatasetOwnership {
    if managed {
        DatasetOwnership::Managed
    } else {
        DatasetOwnership::Imported
    }
}

/// Constraints are stored as a single JSONB document rather than columns: the set
/// grows with the commit contract (content types, size, later checksum policy), and
/// nothing in the database branches on them — the commit path is their only reader.
fn parse_constraints(
    constraints: Option<serde_json::Value>,
) -> Result<DatasetConstraints, CatalogBackendError> {
    let Some(value) = constraints else {
        return Ok(DatasetConstraints::default());
    };
    serde_json::from_value(value).map_err(CatalogBackendError::new_unexpected)
}

fn full_row_to_info(
    row: DatasetFullRow,
    warehouse_id: WarehouseId,
) -> Result<DatasetInfo, CatalogBackendError> {
    let namespace_ident = parse_namespace_identifier_from_vec(
        &row.namespace_name,
        warehouse_id,
        Some(row.namespace_id),
    )
    .map_err(CatalogBackendError::new_unexpected)?;

    let location = join_location(&row.fs_protocol, &row.fs_location)
        .map_err(CatalogBackendError::new_unexpected)?;

    let constraints = parse_constraints(row.constraints)?;

    let name = row.name;
    let tabular_ident = TableIdent {
        namespace: namespace_ident.clone(),
        name: name.clone(),
    };

    Ok(DatasetInfo {
        dataset_id: row.dataset_id.into(),
        warehouse_id,
        warehouse_version: WarehouseVersion::new(row.warehouse_version),
        namespace_id: row.namespace_id.into(),
        namespace_version: NamespaceVersion::new(row.namespace_version),
        namespace_ident,
        name,
        tabular_ident,
        location,
        // Datasets have no per-type properties table; `constraints` carries the
        // configuration and is surfaced separately.
        properties: std::collections::HashMap::new(),
        protected: row.protected,
        ownership: ownership_of(row.managed),
        constraints,
    })
}

pub(crate) async fn create_dataset(
    creation: DatasetCreation,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<DatasetInfo, CreateDatasetError> {
    let id: Uuid = *creation.dataset_id;

    // The tabular row is what makes the name collide with tables and views in the
    // same namespace, so it is inserted first and the dataset row hangs off it.
    let tabular_info = super::create_tabular(
        CreateTabular {
            id,
            name: &creation.name,
            namespace_id: *creation.namespace_id,
            warehouse_id: *creation.warehouse_id,
            typ: TabularType::Dataset,
            metadata_location: None,
            location: &creation.location,
        },
        transaction,
    )
    .await
    .map_err(|e| match e {
        CreateTabularError::TabularAlreadyExists(_) => {
            CreateDatasetError::from(DatasetAlreadyExists::new())
        }
        CreateTabularError::CatalogBackendError(e) => CreateDatasetError::from(e),
        CreateTabularError::InternalParseLocationError(e) => CreateDatasetError::from(e),
        CreateTabularError::LocationAlreadyTaken(e) => CreateDatasetError::from(e),
        CreateTabularError::InvalidNamespaceIdentifier(e) => CreateDatasetError::from(e),
    })?;

    let constraints_json = if creation.constraints.is_empty() {
        None
    } else {
        Some(
            serde_json::to_value(&creation.constraints)
                .map_err(|e| CreateDatasetError::from(CatalogBackendError::new_unexpected(e)))?,
        )
    };

    sqlx::query!(
        r#"INSERT INTO dataset (warehouse_id, dataset_id, location, managed, constraints)
        VALUES ($1, $2, $3, $4, $5)"#,
        *creation.warehouse_id,
        id,
        creation.location.to_string(),
        creation.ownership.is_managed(),
        constraints_json as Option<serde_json::Value>,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| CreateDatasetError::from(e.into_catalog_backend_error()))?;

    // Every dataset has `main` from the moment it exists, so there is always a
    // commit target. It starts pointing at nothing: the first commit's
    // compare-and-swap is against NULL.
    sqlx::query!(
        r#"INSERT INTO dataset_ref (warehouse_id, dataset_id, name, typ, snapshot_id)
        VALUES ($1, $2, 'main', 'branch', NULL)"#,
        *creation.warehouse_id,
        id,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| CreateDatasetError::from(e.into_catalog_backend_error()))?;

    let dataset_tabular = tabular_info
        .into_dataset_info()
        .expect("create_tabular returned Dataset type");

    let tabular_ident = TableIdent {
        namespace: dataset_tabular.tabular_ident.namespace.clone(),
        name: dataset_tabular.tabular_ident.name.clone(),
    };

    Ok(DatasetInfo {
        dataset_id: id.into(),
        warehouse_id: dataset_tabular.warehouse_id,
        warehouse_version: dataset_tabular.warehouse_version,
        namespace_id: dataset_tabular.namespace_id,
        namespace_version: dataset_tabular.namespace_version,
        namespace_ident: tabular_ident.namespace.clone(),
        name: tabular_ident.name.clone(),
        tabular_ident,
        location: dataset_tabular.location,
        properties: std::collections::HashMap::new(),
        protected: dataset_tabular.protected,
        ownership: creation.ownership,
        constraints: creation.constraints,
    })
}

pub(crate) async fn load_dataset(
    warehouse_id: WarehouseId,
    namespace_id: NamespaceId,
    dataset_name: &str,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<DatasetInfo, LoadDatasetError> {
    let row = sqlx::query_as!(
        DatasetFullRow,
        r#"
        SELECT
            t.tabular_id as dataset_id,
            w.version as "warehouse_version!",
            t.namespace_id,
            n.version as "namespace_version!",
            t.tabular_namespace_name as "namespace_name!",
            t.name,
            t.fs_location,
            t.fs_protocol,
            t.protected,
            d.managed,
            d.constraints
        FROM tabular t
        INNER JOIN dataset d ON d.warehouse_id = t.warehouse_id AND d.dataset_id = t.tabular_id
        INNER JOIN warehouse w ON w.warehouse_id = t.warehouse_id AND w.status = 'active'
        INNER JOIN namespace n ON n.namespace_id = t.namespace_id AND n.warehouse_id = t.warehouse_id
        WHERE t.warehouse_id = $1
          AND t.namespace_id = $2
          AND t.name = $3
          AND t.typ = 'dataset'
          AND t.deleted_at IS NULL
        "#,
        *warehouse_id,
        *namespace_id,
        dataset_name,
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|e| LoadDatasetError::from(e.into_catalog_backend_error()))?
    .ok_or_else(|| LoadDatasetError::from(DatasetNotFound::new()))?;

    full_row_to_info(row, warehouse_id).map_err(LoadDatasetError::from)
}

/// Load a dataset by its stable id. Use this when the caller already holds an
/// authorized identity (e.g. after a successful authz check); it closes the TOCTOU
/// window where a concurrent rename + create-with-same-name between authz and load
/// would let the caller read a different row than the one their grant applied to.
pub(crate) async fn load_dataset_by_id(
    warehouse_id: WarehouseId,
    dataset_id: DatasetId,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<DatasetInfo, LoadDatasetError> {
    let row = sqlx::query_as!(
        DatasetFullRow,
        r#"
        SELECT
            t.tabular_id as dataset_id,
            w.version as "warehouse_version!",
            t.namespace_id,
            n.version as "namespace_version!",
            t.tabular_namespace_name as "namespace_name!",
            t.name,
            t.fs_location,
            t.fs_protocol,
            t.protected,
            d.managed,
            d.constraints
        FROM tabular t
        INNER JOIN dataset d ON d.warehouse_id = t.warehouse_id AND d.dataset_id = t.tabular_id
        INNER JOIN warehouse w ON w.warehouse_id = t.warehouse_id AND w.status = 'active'
        INNER JOIN namespace n ON n.namespace_id = t.namespace_id AND n.warehouse_id = t.warehouse_id
        WHERE t.warehouse_id = $1
          AND t.tabular_id = $2
          AND t.typ = 'dataset'
          AND t.deleted_at IS NULL
        "#,
        *warehouse_id,
        *dataset_id,
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|e| LoadDatasetError::from(e.into_catalog_backend_error()))?
    .ok_or_else(|| LoadDatasetError::from(DatasetNotFound::new()))?;

    full_row_to_info(row, warehouse_id).map_err(LoadDatasetError::from)
}

/// The `managed` flag alone, readable after the tabular row is soft-deleted.
///
/// [`load_dataset_by_id`] filters those out, and expiry runs after the row is
/// already marked deleted.
pub(crate) async fn load_dataset_ownership(
    warehouse_id: WarehouseId,
    dataset_id: DatasetId,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<DatasetOwnership, LoadDatasetError> {
    let managed = sqlx::query_scalar!(
        r#"SELECT managed FROM dataset WHERE warehouse_id = $1 AND dataset_id = $2"#,
        *warehouse_id,
        *dataset_id,
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|e| LoadDatasetError::from(e.into_catalog_backend_error()))?
    .ok_or_else(|| LoadDatasetError::from(DatasetNotFound::new()))?;

    Ok(ownership_of(managed))
}

pub(crate) async fn list_datasets(
    warehouse_id: WarehouseId,
    namespace_id: NamespaceId,
    namespace_ident: &iceberg::NamespaceIdent,
    page_size: Option<i64>,
    page_token: Option<&str>,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<(Vec<DatasetListEntry>, Option<String>), ListDatasetsError> {
    let page_size = CONFIG.page_size_or_pagination_default(page_size);

    let token = page_token
        .map(PaginateToken::<Uuid>::try_from)
        .transpose()
        .map_err(|e| ListDatasetsError::from(CatalogBackendError::new_unexpected(e)))?;

    // `v1_parts` refuses a V2 token, which pins a subtree walk's ceiling. Listing a
    // namespace's datasets is a plain keyset walk, so such a token belongs to a
    // different listing and must not silently resume here.
    let (token_ts, token_id) = match token.as_ref() {
        Some(token) => {
            let (created_at, id) = token.v1_parts()?;
            (Some(*created_at), Some(*id))
        }
        None => (None, None),
    };

    let rows = sqlx::query_as!(
        DatasetListRow,
        r#"
        SELECT
            t.tabular_id as dataset_id,
            t.warehouse_id,
            t.namespace_id,
            t.name,
            t.protected,
            d.managed,
            t.created_at
        FROM tabular t
        INNER JOIN dataset d ON d.warehouse_id = t.warehouse_id AND d.dataset_id = t.tabular_id
        INNER JOIN warehouse w ON w.warehouse_id = t.warehouse_id AND w.status = 'active'
        WHERE t.warehouse_id = $1
          AND t.namespace_id = $2
          AND t.typ = 'dataset'
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
    .map_err(|e| ListDatasetsError::from(e.into_catalog_backend_error()))?;

    let mut entries = Vec::with_capacity(rows.len());
    let mut next_page_token = None;

    for row in &rows {
        next_page_token = Some(
            PaginateToken::V1(V1PaginateToken {
                created_at: row.created_at,
                id: row.dataset_id,
            })
            .to_string(),
        );

        let tabular_ident = TableIdent::new(namespace_ident.clone(), row.name.clone());
        entries.push(DatasetListEntry {
            dataset_id: row.dataset_id.into(),
            warehouse_id: row.warehouse_id.into(),
            namespace_id: row.namespace_id.into(),
            name: row.name.clone(),
            tabular_ident,
            namespace_ident: namespace_ident.clone(),
            ownership: ownership_of(row.managed),
            protected: row.protected,
            created_at: row.created_at,
        });
    }

    Ok((entries, next_page_token))
}

pub(crate) async fn drop_dataset(
    warehouse_id: WarehouseId,
    namespace_id: NamespaceId,
    dataset_name: &str,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<DatasetId, DropDatasetError> {
    let row = sqlx::query!(
        r#"
        SELECT t.tabular_id as dataset_id
        FROM tabular t
        INNER JOIN warehouse w ON w.warehouse_id = t.warehouse_id AND w.status = 'active'
        WHERE t.warehouse_id = $1
          AND t.namespace_id = $2
          AND t.name = $3
          AND t.typ = 'dataset'
          AND t.deleted_at IS NULL
        "#,
        *warehouse_id,
        *namespace_id,
        dataset_name,
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|e| DropDatasetError::from(e.into_catalog_backend_error()))?
    .ok_or_else(|| DropDatasetError::from(DatasetNotFound::new()))?;

    let dataset_id: DatasetId = row.dataset_id.into();

    super::drop_tabular(
        warehouse_id,
        TabularId::Dataset(dataset_id),
        false,
        None,
        transaction,
    )
    .await
    .map_err(|e| match e {
        DropTabularError::TabularNotFound(_) => DropDatasetError::from(DatasetNotFound::new()),
        DropTabularError::CatalogBackendError(e) => DropDatasetError::from(e),
        DropTabularError::InvalidNamespaceIdentifier(e) => DropDatasetError::from(e),
        DropTabularError::InternalParseLocationError(e) => DropDatasetError::from(e),
        DropTabularError::ProtectedTabularDeletionWithoutForce(e) => DropDatasetError::from(e),
        DropTabularError::ConcurrentUpdateError(e) => DropDatasetError::from(e),
    })?;

    Ok(dataset_id)
}
