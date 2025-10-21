use std::{collections::HashMap, sync::Arc};

use http::StatusCode;
use iceberg::NamespaceIdent;
use iceberg_ext::catalog::rest::{CreateNamespaceRequest, ErrorModel, IcebergErrorResponse};
use lakekeeper_io::Location;

use crate::{
    api::iceberg::v1::{namespace::NamespaceDropFlags, PaginatedMapping},
    service::{
        define_transparent_error, impl_error_stack_methods, impl_from_with_detail, tasks::TaskId,
        CatalogBackendError, CatalogStore, InternalParseLocationError, InvalidPaginationToken,
        ListNamespacesQuery, NamespaceId, TableIdent, TabularId, Transaction, WarehouseIdNotFound,
    },
    WarehouseId,
};

#[derive(Debug, PartialEq, Clone)]
pub struct Namespace {
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
    pub child_tables: Vec<(TabularId, Location, TableIdent)>,
    pub open_tasks: Vec<TaskId>,
}

macro_rules! define_simple_namespace_err {
    ($error_name:ident, $error_message:literal) => {
        #[derive(thiserror::Error, Debug, PartialEq)]
        #[error($error_message)]
        pub struct $error_name {
            pub warehouse_id: $crate::WarehouseId,
            pub namespace: NamespaceIdentOrId,
            pub stack: Vec<String>,
        }

        impl $error_name {
            #[must_use]
            pub fn new(
                warehouse_id: $crate::WarehouseId,
                namespace: impl Into<NamespaceIdentOrId>,
            ) -> Self {
                Self {
                    warehouse_id,
                    namespace: namespace.into(),
                    stack: Vec::new(),
                }
            }
        }

        impl_error_stack_methods!($error_name);
    };
}

// --------------------------- GENERAL ERROR ---------------------------
#[derive(thiserror::Error, Debug)]
#[error("Error serializing properties of namespace {namespace}: {source}")]
pub struct NamespacePropertiesSerializationError {
    warehouse_id: WarehouseId,
    namespace: NamespaceIdentOrId,
    source: serde_json::Error,
    stack: Vec<String>,
}
impl NamespacePropertiesSerializationError {
    #[must_use]
    pub fn new(
        warehouse_id: WarehouseId,
        namespace: impl Into<NamespaceIdentOrId>,
        source: serde_json::Error,
    ) -> Self {
        Self {
            warehouse_id,
            namespace: namespace.into(),
            source,
            stack: Vec::new(),
        }
    }
}
impl_error_stack_methods!(NamespacePropertiesSerializationError);
impl From<NamespacePropertiesSerializationError> for ErrorModel {
    fn from(err: NamespacePropertiesSerializationError) -> Self {
        let message = err.to_string();
        let NamespacePropertiesSerializationError { stack, source, .. } = err;

        ErrorModel {
            r#type: "NamespacePropertiesSerializationError".to_string(),
            code: StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
            message,
            stack,
            source: Some(Box::new(source)),
        }
    }
}

#[derive(thiserror::Error, Debug)]
#[error("Encountered invalid namespace identifier in warehouse {warehouse_id}: {found}")]
pub struct InvalidNamespaceIdentifier {
    warehouse_id: WarehouseId,
    namespace_id: Option<NamespaceId>,
    found: String,
    stack: Vec<String>,
}
impl InvalidNamespaceIdentifier {
    #[must_use]
    pub fn new(warehouse_id: WarehouseId, found: impl Into<String>) -> Self {
        Self {
            warehouse_id,
            namespace_id: None,
            found: found.into(),
            stack: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_id(mut self, namespace_id: NamespaceId) -> Self {
        self.namespace_id = Some(namespace_id);
        self
    }
}
impl_error_stack_methods!(InvalidNamespaceIdentifier);

impl From<InvalidNamespaceIdentifier> for ErrorModel {
    fn from(err: InvalidNamespaceIdentifier) -> Self {
        let message = err.to_string();
        let InvalidNamespaceIdentifier { stack, .. } = err;

        ErrorModel {
            r#type: "InvalidNamespaceIdentifier".to_string(),
            code: StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
            message,
            stack,
            source: None,
        }
    }
}

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

define_simple_namespace_err!(
    NamespaceNotFound,
    "Namespace with {namespace} does not exist in warehouse '{warehouse_id}'"
);
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
define_transparent_error! {
    pub enum CatalogGetNamespaceError,
    stack_message: "Error getting namespace in catalog",
    variants: [
        CatalogBackendError,
        InvalidNamespaceIdentifier,
    ]
}

// --------------------------- List Error ---------------------------
define_transparent_error! {
    pub enum CatalogListNamespaceError,
    stack_message: "Error listing namespaces in catalog",
    variants: [
        CatalogBackendError,
        InvalidNamespaceIdentifier,
        InvalidPaginationToken,
    ]
}

// --------------------------- Create Error ---------------------------
define_transparent_error! {
    pub enum CatalogCreateNamespaceError,
    stack_message: "Error creating Namespace in catalog",
    variants: [
        CatalogBackendError,
        NamespacePropertiesSerializationError,
        NamespaceAlreadyExists,
        WarehouseIdNotFound
    ]
}

#[derive(thiserror::Error, Debug, PartialEq)]
#[error("Namespace name '{namespace}' already exist in warehouse '{warehouse_id}'")]
pub struct NamespaceAlreadyExists {
    pub warehouse_id: WarehouseId,
    pub namespace: NamespaceIdent,
    pub stack: Vec<String>,
}
impl NamespaceAlreadyExists {
    #[must_use]
    pub fn new(warehouse_id: WarehouseId, namespace: NamespaceIdent) -> Self {
        Self {
            warehouse_id,
            namespace,
            stack: Vec::new(),
        }
    }
}
impl_error_stack_methods!(NamespaceAlreadyExists);

impl From<NamespaceAlreadyExists> for ErrorModel {
    fn from(err: NamespaceAlreadyExists) -> Self {
        ErrorModel {
            r#type: "AlreadyExistsException".to_string(),
            code: StatusCode::CONFLICT.as_u16(),
            message: err.to_string(),
            stack: err.stack,
            source: None,
        }
    }
}

// --------------------------- Drop Error ---------------------------
define_transparent_error! {
    pub enum CatalogNamespaceDropError,
    stack_message: "Error dropping Namespace in catalog",
    variants: [
        CatalogBackendError,
        NamespaceNotFound,
        InvalidNamespaceIdentifier,
        NamespaceProtected,
        NamespaceNotEmpty,
        ChildNamespaceProtected,
        ChildTabularProtected,
        NamespaceHasRunningTabularExpirations,
        InternalParseLocationError
    ]
}

define_simple_namespace_err!(
    NamespaceProtected,
    "Namespace with {namespace} is protected and force flag not set. Cannot delete protected namespace."
);

impl From<NamespaceProtected> for ErrorModel {
    fn from(err: NamespaceProtected) -> Self {
        ErrorModel {
            r#type: "NamespaceProtected".to_string(),
            code: StatusCode::CONFLICT.as_u16(),
            message: err.to_string(),
            stack: err.stack,
            source: None,
        }
    }
}

define_simple_namespace_err!(
    ChildNamespaceProtected,
    "Namespace with {namespace} has protected child namespaces and force flag was not specified."
);

impl From<ChildNamespaceProtected> for ErrorModel {
    fn from(err: ChildNamespaceProtected) -> Self {
        ErrorModel {
            r#type: "ChildNamespaceProtected".to_string(),
            code: StatusCode::CONFLICT.as_u16(),
            message: err.to_string(),
            stack: err.stack,
            source: None,
        }
    }
}

define_simple_namespace_err!(
    ChildTabularProtected,
    "Namespace with {namespace} has protected child tables or views and force flag was not specified."
);

impl From<ChildTabularProtected> for ErrorModel {
    fn from(err: ChildTabularProtected) -> Self {
        ErrorModel {
            r#type: "ChildTabularProtected".to_string(),
            code: StatusCode::CONFLICT.as_u16(),
            message: err.to_string(),
            stack: err.stack,
            source: None,
        }
    }
}

define_simple_namespace_err!(
    NamespaceNotEmpty,
    "Namespace with {namespace} is not empty."
);

impl From<NamespaceNotEmpty> for ErrorModel {
    fn from(err: NamespaceNotEmpty) -> Self {
        ErrorModel {
            r#type: "NamespaceNotEmptyException".to_string(),
            code: StatusCode::CONFLICT.as_u16(),
            message: err.to_string(),
            stack: err.stack,
            source: None,
        }
    }
}

define_simple_namespace_err!(
    NamespaceHasRunningTabularExpirations,
    "Namespace with {namespace} has a running tabular expiration, please retry after the expiration task is done."
);

impl From<NamespaceHasRunningTabularExpirations> for ErrorModel {
    fn from(err: NamespaceHasRunningTabularExpirations) -> Self {
        ErrorModel {
            r#type: "NamespaceHasRunningTabularExpirations".to_string(),
            code: StatusCode::CONFLICT.as_u16(),
            message: err.to_string(),
            stack: err.stack,
            source: None,
        }
    }
}

// --------------------------- Update Properties Error ---------------------------
define_transparent_error! {
    pub enum CatalogUpdateNamespacePropertiesError,
    stack_message: "Error updating Namespace properties in catalog",
    variants: [
        CatalogBackendError,
        NamespacePropertiesSerializationError,
        NamespaceNotFound,
        InvalidNamespaceIdentifier,
    ]
}

// --------------------------- Set Namespace Protected Error ---------------------------
define_transparent_error! {
    pub enum CatalogSetNamespaceProtectedError,
    stack_message: "Error setting Namespace protection in catalog",
    variants: [
        CatalogBackendError,
        NamespaceNotFound,
        InvalidNamespaceIdentifier,
    ]
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
    ) -> Result<Option<Namespace>, CatalogGetNamespaceError> {
        Self::get_namespace_impl(warehouse_id, namespace.into(), catalog_state).await
    }

    async fn list_namespaces<'a>(
        warehouse_id: WarehouseId,
        query: &ListNamespacesQuery,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<PaginatedMapping<NamespaceId, Namespace>, CatalogListNamespaceError>
    {
        Self::list_namespaces_impl(warehouse_id, query, transaction).await
    }

    async fn create_namespace<'a>(
        warehouse_id: WarehouseId,
        namespace_id: NamespaceId,
        request: CreateNamespaceRequest,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<Namespace, CatalogCreateNamespaceError> {
        Self::create_namespace_impl(warehouse_id, namespace_id, request, transaction).await
    }

    async fn drop_namespace<'a>(
        warehouse_id: WarehouseId,
        namespace_id: NamespaceId,
        flags: NamespaceDropFlags,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<NamespaceDropInfo, CatalogNamespaceDropError> {
        Self::drop_namespace_impl(warehouse_id, namespace_id, flags, transaction).await
    }

    async fn update_namespace_properties<'a>(
        warehouse_id: WarehouseId,
        namespace_id: NamespaceId,
        properties: HashMap<String, String>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> std::result::Result<Namespace, CatalogUpdateNamespacePropertiesError> {
        Self::update_namespace_properties_impl(warehouse_id, namespace_id, properties, transaction)
            .await
    }

    async fn set_namespace_protected(
        warehouse_id: WarehouseId,
        namespace_id: NamespaceId,
        protect: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> std::result::Result<Namespace, CatalogSetNamespaceProtectedError> {
        Self::set_namespace_protected_impl(warehouse_id, namespace_id, protect, transaction).await
    }
}

impl<T> CatalogNamespaceOps for T where T: CatalogStore {}
