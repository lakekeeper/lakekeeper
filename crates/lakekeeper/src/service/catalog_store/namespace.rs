use std::{collections::HashMap, sync::Arc};

use http::StatusCode;
use iceberg::NamespaceIdent;
use iceberg_ext::catalog::rest::{CreateNamespaceRequest, ErrorModel, IcebergErrorResponse};
use lakekeeper_io::Location;

use crate::{
    api::iceberg::v1::{namespace::NamespaceDropFlags, PaginatedMapping},
    service::{
        define_transparent_error, define_version_newtype, impl_error_stack_methods,
        impl_from_with_detail,
        namespace_cache::{
            namespace_cache_get_by_id, namespace_cache_get_by_ident,
            namespace_cache_insert_hierarchy,
        },
        tasks::TaskId,
        CachePolicy, CatalogBackendError, CatalogStore, InternalParseLocationError,
        InvalidPaginationToken, ListNamespacesQuery, NamespaceId, TableIdent, TabularId,
        Transaction, WarehouseIdNotFound,
    },
    WarehouseId,
};

define_version_newtype!(NamespaceVersion);

#[derive(Debug, PartialEq, Clone)]
pub struct Namespace {
    pub namespace_ident: NamespaceIdent,
    pub protected: bool,
    pub namespace_id: NamespaceId,
    pub warehouse_id: WarehouseId,
    pub properties: Option<std::collections::HashMap<String, String>>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: Option<chrono::DateTime<chrono::Utc>>,
    pub version: NamespaceVersion,
}

#[derive(Debug, PartialEq, Clone)]
pub struct NamespaceWithParentVersion {
    pub namespace: Arc<Namespace>,
    pub parent: Option<(NamespaceId, NamespaceVersion)>,
}

impl NamespaceWithParentVersion {
    #[must_use]
    pub fn namespace_id(&self) -> NamespaceId {
        self.namespace.namespace_id
    }

    #[must_use]
    pub fn namespace_ident(&self) -> &NamespaceIdent {
        &self.namespace.namespace_ident
    }

    #[must_use]
    pub fn warehouse_id(&self) -> WarehouseId {
        self.namespace.warehouse_id
    }

    #[must_use]
    pub fn is_protected(&self) -> bool {
        self.namespace.protected
    }

    #[must_use]
    pub fn properties(&self) -> Option<&HashMap<String, String>> {
        self.namespace.properties.as_ref()
    }

    #[must_use]
    pub fn version(&self) -> NamespaceVersion {
        self.namespace.version
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NamespaceHierarchy {
    /// The target namespace (leaf in the hierarchy)
    pub namespace: Arc<Namespace>,
    /// Parent namespaces ordered from immediate parent to root.
    /// Empty if this namespace is a root namespace (i.e., directly in the warehouse).
    /// Root namespace = a namespace that is directly contained in the warehouse with no parent.
    pub parents: Vec<Arc<Namespace>>,
}

impl NamespaceHierarchy {
    /// Get the immediate parent namespace, if any.
    /// Returns None if this is a root namespace (directly in the warehouse).
    #[must_use]
    pub fn parent(&self) -> Option<&Arc<Namespace>> {
        self.parents.first()
    }

    /// Get the root namespace (furthest ancestor in the hierarchy).
    /// A root namespace is one that is directly contained in the warehouse.
    /// If this namespace is itself a root namespace, returns itself.
    #[must_use]
    pub fn root(&self) -> &Arc<Namespace> {
        self.parents.last().unwrap_or(&self.namespace)
    }

    /// Check if this is a root namespace (directly in the warehouse, no parents)
    #[must_use]
    pub fn is_root(&self) -> bool {
        self.parents.is_empty()
    }

    /// Get the depth in the hierarchy.
    /// - 0 = root namespace (directly in warehouse)
    /// - 1 = one level deep
    /// - 2 = two levels deep, etc.
    #[must_use]
    pub fn depth(&self) -> usize {
        self.parents.len()
    }

    #[must_use]
    pub fn namespace_ident(&self) -> &NamespaceIdent {
        &self.namespace.namespace_ident
    }

    #[must_use]
    pub fn namespace_id(&self) -> NamespaceId {
        self.namespace.namespace_id
    }

    #[must_use]
    pub fn warehouse_id(&self) -> WarehouseId {
        self.namespace.warehouse_id
    }

    #[must_use]
    pub fn is_protected(&self) -> bool {
        self.namespace.protected
    }

    #[must_use]
    pub fn properties(&self) -> Option<&HashMap<String, String>> {
        self.namespace.properties.as_ref()
    }

    #[must_use]
    pub fn version(&self) -> NamespaceVersion {
        self.namespace.version
    }

    #[must_use]
    pub fn updated_at(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        self.namespace.updated_at
    }
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
        NamespaceNotFound, // for parent namespace check
        CatalogBackendError,
        NamespacePropertiesSerializationError,
        NamespaceAlreadyExists,
        WarehouseIdNotFound,
        InvalidNamespaceIdentifier
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
    ) -> Result<Option<NamespaceHierarchy>, CatalogGetNamespaceError> {
        let namespace = namespace.into();
        let cached = match namespace {
            NamespaceIdentOrId::Id(namespace_id) => namespace_cache_get_by_id(namespace_id).await,
            NamespaceIdentOrId::Name(ref namespace_ident) => {
                namespace_cache_get_by_ident(namespace_ident, warehouse_id).await
            }
        };

        if let Some(cached_namespace) = cached {
            return Ok(Some(cached_namespace));
        }
        let namespace_hierarchy =
            Self::get_namespace_impl(warehouse_id, namespace, catalog_state).await?;

        if let Some(namespace_hierarchy) = &namespace_hierarchy {
            namespace_cache_insert_hierarchy(namespace_hierarchy).await;
        }

        Ok(namespace_hierarchy)
    }

    /// Get warehouse by ID, invalidating cache if it's older than the provided timestamp
    async fn get_namespace_cache_aware(
        warehouse_id: WarehouseId,
        namespace: impl Into<NamespaceIdentOrId> + Send,
        cache_policy: CachePolicy,
        state: Self::State,
    ) -> Result<Option<NamespaceHierarchy>, CatalogGetNamespaceError> {
        let provided_namespace = namespace.into();
        let namespace = match cache_policy {
            CachePolicy::Skip => {
                // Skip cache entirely
                let namespace =
                    Self::get_namespace_impl(warehouse_id, provided_namespace, state).await?;

                // Update cache with fresh data
                if let Some(namespace) = &namespace {
                    namespace_cache_insert_hierarchy(namespace).await;
                }

                namespace
            }
            CachePolicy::Use => {
                // Use cache if available
                Self::get_namespace(warehouse_id, provided_namespace, state).await?
            }
            CachePolicy::RequireMinimumVersion(require_min_version) => {
                // Check cache first
                let cached = match provided_namespace {
                    NamespaceIdentOrId::Id(namespace_id) => {
                        namespace_cache_get_by_id(namespace_id).await
                    }
                    NamespaceIdentOrId::Name(ref namespace_ident) => {
                        namespace_cache_get_by_ident(namespace_ident, warehouse_id).await
                    }
                };

                if let Some(namespace) = cached {
                    // Determine if cache is valid based on version
                    let cache_is_valid = namespace.version().0 >= require_min_version;

                    if cache_is_valid {
                        Some(namespace)
                    } else {
                        tracing::debug!(
                            "Detected stale cache for namespace {}: cached={:?}, required={:?}. Refreshing.",
                            provided_namespace,
                            namespace.version(),
                            require_min_version
                        );
                        // Cache is stale: fetch fresh data
                        let namespace =
                            Self::get_namespace_impl(warehouse_id, provided_namespace, state)
                                .await?;
                        // Update cache with fresh data
                        if let Some(namespace) = &namespace {
                            namespace_cache_insert_hierarchy(namespace).await;
                        }
                        namespace
                    }
                } else {
                    // No cache entry: fetch fresh data
                    let namespace =
                        Self::get_namespace_impl(warehouse_id, provided_namespace, state).await?;
                    // Update cache with fresh data
                    if let Some(namespace) = &namespace {
                        namespace_cache_insert_hierarchy(namespace).await;
                    }
                    namespace
                }
            }
        };

        Ok(namespace)
    }

    async fn list_namespaces<'a>(
        warehouse_id: WarehouseId,
        query: &ListNamespacesQuery,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<PaginatedMapping<NamespaceId, NamespaceHierarchy>, CatalogListNamespaceError> {
        let namespaces = Self::list_namespaces_impl(warehouse_id, query, transaction).await?;

        let mut tasks = Vec::with_capacity(namespaces.len());
        for (_namespace_id, namespace) in &namespaces {
            tasks.push(namespace_cache_insert_hierarchy(namespace));
        }

        futures::future::join_all(tasks).await;

        Ok(namespaces)
    }

    async fn create_namespace<'a>(
        warehouse_id: WarehouseId,
        namespace_id: NamespaceId,
        request: CreateNamespaceRequest,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<NamespaceWithParentVersion, CatalogCreateNamespaceError> {
        Self::create_namespace_impl(warehouse_id, namespace_id, request, transaction).await
    }

    async fn drop_namespace<'a>(
        warehouse_id: WarehouseId,
        namespace_id: NamespaceId,
        flags: NamespaceDropFlags,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<NamespaceDropInfo, CatalogNamespaceDropError> {
        Self::drop_namespace_impl(warehouse_id, namespace_id, flags, transaction).await
    }

    async fn update_namespace_properties<'a>(
        warehouse_id: WarehouseId,
        namespace_id: NamespaceId,
        properties: HashMap<String, String>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<NamespaceWithParentVersion, CatalogUpdateNamespacePropertiesError> {
        Self::update_namespace_properties_impl(warehouse_id, namespace_id, properties, transaction)
            .await
    }

    async fn set_namespace_protected(
        warehouse_id: WarehouseId,
        namespace_id: NamespaceId,
        protect: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<NamespaceWithParentVersion, CatalogSetNamespaceProtectedError> {
        Self::set_namespace_protected_impl(warehouse_id, namespace_id, protect, transaction).await
    }
}

impl<T> CatalogNamespaceOps for T where T: CatalogStore {}
