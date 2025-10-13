use std::{collections::HashMap, sync::Arc};

use http::StatusCode;
use iceberg::NamespaceIdent;
use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};

use crate::{
    service::{
        impl_error_stack_methods, impl_from_with_detail, tasks::TaskId, CatalogBackendError,
        CatalogStore, DatabaseIntegrityError, NamespaceId, TableIdent, TabularId,
    },
    WarehouseId,
};

#[derive(Debug, PartialEq, Clone)]
pub struct GetNamespaceResponse {
    /// Reference to one or more levels of a namespace
    pub namespace_ident: NamespaceIdent,
    pub protected: bool,
    pub namespace_id: NamespaceId,
    pub warehouse_id: WarehouseId,
    pub properties: Option<Arc<std::collections::HashMap<String, String>>>,
    pub updated_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ListNamespacesResponse {
    pub next_page_tokens: Vec<(NamespaceId, String)>,
    pub namespaces: HashMap<NamespaceId, NamespaceIdent>,
}

#[derive(Debug)]
pub struct NamespaceDropInfo {
    pub child_namespaces: Vec<NamespaceId>,
    // table-id, location, table-ident
    pub child_tables: Vec<(TabularId, String, TableIdent)>,
    pub open_tasks: Vec<TaskId>,
}

// --------------------------- GENERAL ERROR ---------------------------
#[derive(Debug, Clone, PartialEq, Eq, derive_more::From)]
pub enum NamespaceIdentOrId {
    Id(NamespaceId),
    Name(NamespaceIdent),
}
impl std::fmt::Display for NamespaceIdentOrId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NamespaceIdentOrId::Id(id) => write!(f, "id '{id}'"),
            NamespaceIdentOrId::Name(name) => write!(f, "name '{name}'"),
        }
    }
}
impl From<&NamespaceIdent> for NamespaceIdentOrId {
    fn from(value: &NamespaceIdent) -> Self {
        value.clone().into()
    }
}

#[derive(thiserror::Error, Debug, PartialEq)]
#[error("Namespace with {namespace} does not exist in warehouse '{warehouse_id}'")]
pub struct NamespaceNotFound {
    pub warehouse_id: WarehouseId,
    pub namespace: NamespaceIdentOrId,
    pub stack: Vec<String>,
}
impl NamespaceNotFound {
    #[must_use]
    pub fn new(warehouse_id: WarehouseId, namespace: impl Into<NamespaceIdentOrId>) -> Self {
        Self {
            warehouse_id,
            namespace: namespace.into(),
            stack: Vec::new(),
        }
    }
}
impl_error_stack_methods!(NamespaceNotFound);

impl From<NamespaceNotFound> for ErrorModel {
    fn from(err: NamespaceNotFound) -> Self {
        ErrorModel {
            r#type: "NoSuchNamespaceException".to_string(),
            code: StatusCode::NOT_FOUND.as_u16(),
            message: err.to_string(),
            stack: err.stack,
            source: None,
        }
    }
}

// --------------------------- GET ERROR ---------------------------
#[derive(thiserror::Error, Debug, PartialEq)]
pub enum CatalogGetNamespaceError {
    #[error(transparent)]
    CatalogBackendError(CatalogBackendError),
    #[error(transparent)]
    NamespaceNotFound(NamespaceNotFound),
    #[error(transparent)]
    DatabaseIntegrityError(DatabaseIntegrityError),
}

impl CatalogGetNamespaceError {
    #[must_use]
    pub fn append_detail(mut self, detail: String) -> Self {
        match &mut self {
            CatalogGetNamespaceError::CatalogBackendError(e) => {
                e.append_detail_mut(detail);
            }
            CatalogGetNamespaceError::DatabaseIntegrityError(e) => {
                e.append_detail_mut(detail);
            }
            CatalogGetNamespaceError::NamespaceNotFound(e) => {
                e.append_detail_mut(detail);
            }
        }
        self
    }

    #[must_use]
    pub fn not_found(warehouse_id: WarehouseId, namespace: impl Into<NamespaceIdentOrId>) -> Self {
        Self::NamespaceNotFound(NamespaceNotFound::new(warehouse_id, namespace))
    }

    #[must_use]
    pub fn database_integrity(message: impl Into<String>) -> Self {
        Self::DatabaseIntegrityError(DatabaseIntegrityError::new(message))
    }

    #[must_use]
    pub fn is_not_found(&self) -> bool {
        matches!(self, CatalogGetNamespaceError::NamespaceNotFound(_))
    }
}
const GET_BY_NAME_ERROR_STACK: &str = "Error getting namespace in catalog";
impl_from_with_detail!(CatalogBackendError => CatalogGetNamespaceError::CatalogBackendError, GET_BY_NAME_ERROR_STACK);
impl_from_with_detail!(NamespaceNotFound => CatalogGetNamespaceError::NamespaceNotFound, GET_BY_NAME_ERROR_STACK);

impl From<CatalogGetNamespaceError> for ErrorModel {
    fn from(err: CatalogGetNamespaceError) -> Self {
        match err {
            CatalogGetNamespaceError::CatalogBackendError(e) => e.into(),
            CatalogGetNamespaceError::NamespaceNotFound(e) => e.into(),
            CatalogGetNamespaceError::DatabaseIntegrityError(e) => e.into(),
        }
    }
}
impl From<CatalogGetNamespaceError> for IcebergErrorResponse {
    fn from(err: CatalogGetNamespaceError) -> Self {
        ErrorModel::from(err).into()
    }
}

#[async_trait::async_trait]
pub trait CatalogNamespaceOps
where
    Self: CatalogStore,
{
    /// Get a namespace by its ID or name.
    async fn get_namespace<'a>(
        warehouse_id: WarehouseId,
        namespace: impl Into<NamespaceIdentOrId> + Send,
        catalog_state: Self::State,
    ) -> Result<Option<GetNamespaceResponse>, CatalogGetNamespaceError> {
        let ns = Self::get_namespace_impl(warehouse_id, namespace.into(), catalog_state).await;

        match ns {
            Ok(ns) => Ok(Some(ns)),
            Err(CatalogGetNamespaceError::NamespaceNotFound { .. }) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Get a namespace by its ID or name.
    /// Only returns the namespace if the warehouse is active.
    async fn require_namespace<'a>(
        warehouse_id: WarehouseId,
        namespace: impl Into<NamespaceIdentOrId> + Send,
        catalog_state: Self::State,
    ) -> Result<GetNamespaceResponse, CatalogGetNamespaceError> {
        let ns = Self::get_namespace_impl(warehouse_id, namespace.into(), catalog_state).await?;
        Ok(ns)
    }
}

impl<T> CatalogNamespaceOps for T where T: CatalogStore {}
