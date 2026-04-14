use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use http::StatusCode;
use iceberg::NamespaceIdent;
use iceberg_ext::catalog::rest::{CreateNamespaceRequest, ErrorModel};
use lakekeeper_io::Location;

use crate::{
    WarehouseId,
    api::iceberg::v1::{PaginatedMapping, namespace::NamespaceDropFlags},
    service::{
        BasicTabularInfo, CachePolicy, CatalogBackendError, CatalogStore,
        InternalParseLocationError, InvalidPaginationToken, ListNamespacesQuery, NamespaceId,
        SerializationError, StateOrTransaction, TableIdent, TabularId, Transaction,
        WarehouseIdNotFound,
        authz::AuthZCannotSeeNamespace,
        define_transparent_error, define_version_newtype,
        events::impl_authorization_failure_source,
        impl_error_stack_methods, impl_from_with_detail,
        namespace_cache::{
            namespace_cache_get_by_id, namespace_cache_get_by_ident,
            namespace_cache_insert_multiple,
        },
        storage::storage_layout::NamespaceNameContext,
        tasks::TaskId,
    },
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
pub struct NamespaceWithParent {
    /// Canonical (stored) namespace data. Always in the case that was used at creation time.
    pub namespace: Arc<Namespace>,
    pub parent: Option<(NamespaceId, NamespaceVersion)>,
    /// User-requested namespace ident for this row. When a caller looks up a namespace
    /// by name (e.g. `foo.bar`), the DB matches case-insensitively (via ICU collation)
    /// but the response should carry the caller's case. This field stores the caller's
    /// case for the leaf AND for any parent levels that were derived from the request.
    ///
    /// When `None`, `namespace_ident()` returns the canonical case. Caches always store
    /// canonical (this field is stripped before insert) so that ID-based lookups
    /// return deterministic case independent of which earlier caller populated the cache.
    pub requested_ident: Option<NamespaceIdent>,
}
pub trait AuthZNamespaceInfo: Send + Sync {
    fn namespace(&self) -> &Namespace;
    fn namespace_id(&self) -> NamespaceId {
        self.namespace().namespace_id
    }
    fn parent(&self) -> Option<(NamespaceId, NamespaceVersion)>;
    fn warehouse_id(&self) -> WarehouseId {
        self.namespace().warehouse_id
    }
}
impl AuthZNamespaceInfo for NamespaceWithParent {
    fn namespace(&self) -> &Namespace {
        &self.namespace
    }
    fn parent(&self) -> Option<(NamespaceId, NamespaceVersion)> {
        self.parent
    }
}

impl NamespaceWithParent {
    #[must_use]
    pub fn namespace_id(&self) -> NamespaceId {
        self.namespace.namespace_id
    }

    /// Returns the user-requested ident if set; otherwise the canonical (stored) ident.
    #[must_use]
    pub fn namespace_ident(&self) -> &NamespaceIdent {
        self.requested_ident
            .as_ref()
            .unwrap_or(&self.namespace.namespace_ident)
    }

    /// Returns the canonical (stored, creation-time) ident, ignoring any user-requested override.
    #[must_use]
    pub fn canonical_ident(&self) -> &NamespaceIdent {
        &self.namespace.namespace_ident
    }

    /// Returns a copy with `requested_ident` set to `ident`. Used to overlay the caller's
    /// case on a canonical entry fetched from the cache.
    #[must_use]
    pub fn with_requested_ident(&self, ident: NamespaceIdent) -> Self {
        Self {
            namespace: self.namespace.clone(),
            parent: self.parent,
            requested_ident: if ident == self.namespace.namespace_ident {
                None
            } else {
                Some(ident)
            },
        }
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
    pub fn parent_namespaces_id(&self) -> Option<NamespaceId> {
        self.parent.as_ref().map(|(id, _)| *id)
    }

    #[must_use]
    pub fn updated_at(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        self.namespace.updated_at
    }

    #[must_use]
    pub fn created_at(&self) -> chrono::DateTime<chrono::Utc> {
        self.namespace.created_at
    }

    #[must_use]
    pub fn is_root(&self) -> bool {
        self.parent.is_none()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NamespaceHierarchy {
    /// The target namespace (leaf in the hierarchy)
    pub namespace: NamespaceWithParent,
    /// Parent namespaces ordered from immediate parent to root.
    /// Empty if this namespace is a root namespace (i.e., directly in the warehouse).
    /// Root namespace = a namespace that is directly contained in the warehouse with no parent.
    pub parents: Vec<NamespaceWithParent>,
}

impl NamespaceHierarchy {
    /// Get the immediate parent namespace, if any.
    /// Returns None if this is a root namespace (directly in the warehouse).
    #[must_use]
    pub fn parent(&self) -> Option<&NamespaceWithParent> {
        self.parents.first()
    }

    /// Get the root namespace (furthest ancestor in the hierarchy).
    /// A root namespace is one that is directly contained in the warehouse.
    /// If this namespace is itself a root namespace, returns itself.
    #[must_use]
    pub fn root(&self) -> &NamespaceWithParent {
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
        self.namespace.namespace_ident()
    }

    /// Returns a copy of this hierarchy with the user's requested case applied to
    /// the leaf and every parent level. Each level gets a prefix of `user_ident`
    /// corresponding to its depth.
    ///
    /// Used when the hierarchy came from the cache (which always stores canonical
    /// case) but we want to return the caller's case. The DB path already sets
    /// `requested_ident` correctly, so this is a no-op for DB-sourced hierarchies
    /// when `user_ident` matches what was passed to the SQL.
    #[must_use]
    pub fn with_user_ident(mut self, user_ident: &NamespaceIdent) -> Self {
        let parts = user_ident.as_ref();
        // Leaf depth equals full length of user_ident.
        if parts.len() == self.namespace.namespace.namespace_ident.len() {
            self.namespace = self.namespace.with_requested_ident(user_ident.clone());
        }
        // Each parent is one level shallower than its child. self.parents[0] is
        // the direct parent of the leaf, self.parents[1] is the grandparent, etc.
        for (i, parent) in self.parents.iter_mut().enumerate() {
            let depth = parts.len().saturating_sub(i + 1);
            if depth == 0 || depth != parent.namespace.namespace_ident.len() {
                // Depth mismatch: don't substitute (shouldn't normally happen).
                continue;
            }
            if let Ok(prefix_ident) = NamespaceIdent::from_vec(parts[..depth].to_vec()) {
                *parent = parent.with_requested_ident(prefix_ident);
            }
        }
        self
    }

    #[must_use]
    pub fn namespace_id(&self) -> NamespaceId {
        self.namespace.namespace_id()
    }

    #[must_use]
    pub fn warehouse_id(&self) -> WarehouseId {
        self.namespace.warehouse_id()
    }

    #[must_use]
    pub fn is_protected(&self) -> bool {
        self.namespace.is_protected()
    }

    #[must_use]
    pub fn properties(&self) -> Option<&HashMap<String, String>> {
        self.namespace.properties()
    }

    #[must_use]
    pub fn version(&self) -> NamespaceVersion {
        self.namespace.version()
    }

    #[must_use]
    pub fn updated_at(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        self.namespace.updated_at()
    }

    #[cfg(feature = "test-utils")]
    #[must_use]
    pub fn new_with_id(warehouse_id: WarehouseId, namespace_id: NamespaceId) -> Self {
        Self {
            namespace: NamespaceWithParent {
                namespace: Arc::new(Namespace {
                    namespace_ident: NamespaceIdent::new(format!("ns-{namespace_id}")),
                    protected: false,
                    namespace_id,
                    warehouse_id,
                    properties: None,
                    created_at: chrono::Utc::now(),
                    updated_at: None,
                    version: 0.into(),
                }),
                parent: None,
                requested_ident: None,
            },
            parents: Vec::new(),
        }
    }
}

impl TryFrom<&NamespaceWithParent> for NamespaceNameContext {
    type Error = ErrorModel;

    fn try_from(value: &NamespaceWithParent) -> Result<Self, Self::Error> {
        Ok(Self {
            name: value
                .namespace_ident()
                .last()
                .ok_or_else(|| {
                    ErrorModel::internal("Namespace must have a name", "NamespaceNameMissing", None)
                })?
                .clone(),
            uuid: value.namespace_id().into(),
        })
    }
}

#[derive(Debug)]
pub struct CatalogListNamespacesResponse {
    pub parent_namespaces: HashMap<NamespaceId, NamespaceWithParent>,
    pub namespaces: PaginatedMapping<NamespaceId, NamespaceWithParent>,
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

        ErrorModel::builder()
            .r#type("NamespacePropertiesSerializationError")
            .code(StatusCode::INTERNAL_SERVER_ERROR.as_u16())
            .message(message)
            .stack(stack)
            .source(Some(Box::new(source)))
            .build()
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
impl_authorization_failure_source!(InvalidNamespaceIdentifier => InternalCatalogError);
impl From<InvalidNamespaceIdentifier> for ErrorModel {
    fn from(err: InvalidNamespaceIdentifier) -> Self {
        let message = err.to_string();
        let InvalidNamespaceIdentifier { stack, .. } = err;

        ErrorModel::builder()
            .r#type("InvalidNamespaceIdentifier")
            .code(StatusCode::INTERNAL_SERVER_ERROR.as_u16())
            .message(message)
            .stack(stack)
            .build()
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
        ErrorModel::builder()
            .r#type("NoSuchNamespaceException")
            .code(StatusCode::NOT_FOUND.as_u16())
            .message(err.to_string())
            .stack(err.stack)
            .build()
    }
}
impl_authorization_failure_source!(NamespaceNotFound => ResourceNotFound);

// --------------------------- GET ERROR ---------------------------
define_transparent_error! {
    pub enum CatalogGetNamespaceError,
    stack_message: "Error getting namespace in catalog",
    variants: [
        CatalogBackendError,
        InvalidNamespaceIdentifier,
        SerializationError,
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
        ErrorModel::builder()
            .r#type("AlreadyExistsException")
            .code(StatusCode::CONFLICT.as_u16())
            .message(err.to_string())
            .stack(err.stack)
            .build()
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
        ErrorModel::builder()
            .r#type("NamespaceProtected")
            .code(StatusCode::CONFLICT.as_u16())
            .message(err.to_string())
            .stack(err.stack)
            .build()
    }
}

define_simple_namespace_err!(
    ChildNamespaceProtected,
    "Namespace with {namespace} has protected child namespaces and force flag was not specified."
);

impl From<ChildNamespaceProtected> for ErrorModel {
    fn from(err: ChildNamespaceProtected) -> Self {
        ErrorModel::builder()
            .r#type("ChildNamespaceProtected")
            .code(StatusCode::CONFLICT.as_u16())
            .message(err.to_string())
            .stack(err.stack)
            .build()
    }
}

define_simple_namespace_err!(
    ChildTabularProtected,
    "Namespace with {namespace} has protected child tables or views and force flag was not specified."
);

impl From<ChildTabularProtected> for ErrorModel {
    fn from(err: ChildTabularProtected) -> Self {
        ErrorModel::builder()
            .r#type("ChildTabularProtected")
            .code(StatusCode::CONFLICT.as_u16())
            .message(err.to_string())
            .stack(err.stack)
            .build()
    }
}

define_simple_namespace_err!(
    NamespaceNotEmpty,
    "Namespace with {namespace} is not empty."
);

impl From<NamespaceNotEmpty> for ErrorModel {
    fn from(err: NamespaceNotEmpty) -> Self {
        ErrorModel::builder()
            .r#type("NamespaceNotEmptyException")
            .code(StatusCode::CONFLICT.as_u16())
            .message(err.to_string())
            .stack(err.stack)
            .build()
    }
}

define_simple_namespace_err!(
    NamespaceHasRunningTabularExpirations,
    "Namespace with {namespace} has a running tabular expiration, please retry after the expiration task is done."
);

impl From<NamespaceHasRunningTabularExpirations> for ErrorModel {
    fn from(err: NamespaceHasRunningTabularExpirations) -> Self {
        ErrorModel::builder()
            .r#type("NamespaceHasRunningTabularExpirations")
            .code(StatusCode::CONFLICT.as_u16())
            .message(err.to_string())
            .stack(err.stack)
            .build()
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

/// Input must contain full parent chain up to root namespace.
/// Builds the full `NamespaceHierarchy` by following parent IDs using the provided lookup map.
/// Starts from the namespace with the longest ident (deepest in hierarchy).
fn build_namespace_hierarchy_from_vec(
    namespaces: &[NamespaceWithParent],
) -> Option<NamespaceHierarchy> {
    if namespaces.is_empty() {
        return None;
    }

    let parent_lookup = namespaces
        .iter()
        .map(|ns| (ns.namespace_id(), ns.clone()))
        .collect();

    // namespace with longest ident
    let target_namespace = namespaces
        .iter()
        .max_by_key(|ns| ns.namespace_ident().len())?;
    Some(build_namespace_hierarchy(target_namespace, &parent_lookup))
}

/// Build a `NamespaceHierarchy` from a `NamespaceWithParent` and a lookup map of parent namespaces.
/// This follows the parent chain using `parent_namespaces_id` to look up parents.
/// Returns None if a required parent cannot be found (logs warning in that case).
pub(crate) fn build_namespace_hierarchy(
    namespace: &NamespaceWithParent,
    parent_lookup: &HashMap<NamespaceId, NamespaceWithParent>,
) -> NamespaceHierarchy {
    let mut parents = Vec::new();
    let mut current_parent_id = namespace.parent_namespaces_id();

    while let Some(parent_id) = current_parent_id {
        // Find the parent in the lookup map
        if let Some(parent_ns) = parent_lookup.get(&parent_id) {
            parents.push((*parent_ns).clone());
            current_parent_id = parent_ns.parent_namespaces_id();
        } else {
            // Parent not found - log warning and abort hierarchy build
            #[cfg(debug_assertions)]
            {
                debug_assert!(
                    false,
                    "Parent namespace with id {parent_id} not found in parent_namespaces for namespace {}",
                    namespace.namespace_id()
                );
            }
            tracing::warn!(
                "Parent namespace with id {parent_id} not found in parent_namespaces for namespace {}. Aborting hierarchy build.",
                namespace.namespace_id()
            );
            break;
        }
    }

    let hierarchy = NamespaceHierarchy {
        namespace: namespace.clone(),
        parents,
    };

    #[cfg(debug_assertions)]
    {
        debug_assert!(
            hierarchy.root().namespace_ident().len() == 1,
            "Root namespace should have ident length 1, got {} as root for namespace {}",
            hierarchy.root().namespace_ident(),
            namespace.namespace_ident()
        );
    }

    hierarchy
}

/// Helper function to fetch namespace from database and convert to hierarchy
async fn fetch_namespace<'a, S: CatalogStore, SOT>(
    warehouse_id: WarehouseId,
    namespace: NamespaceIdentOrId,
    state_or_transaction: &mut SOT,
) -> Result<Vec<NamespaceWithParent>, CatalogGetNamespaceError>
where
    SOT: StateOrTransaction<S::State, <S::Transaction as Transaction<S::State>>::Transaction<'a>>,
{
    match namespace {
        NamespaceIdentOrId::Id(namespace_id) => {
            S::get_namespaces_by_id_impl(warehouse_id, &[namespace_id], state_or_transaction).await
        }
        NamespaceIdentOrId::Name(ref namespace_ident) => {
            S::get_namespaces_by_ident_impl(warehouse_id, &[namespace_ident], state_or_transaction)
                .await
        }
    }
}

/// Helper function to check for version conflicts between cached and DB namespaces
/// Returns true if conflicts are detected and a full refetch is needed
fn check_namespace_version_conflicts(
    namespaces_from_cache: &HashMap<NamespaceId, NamespaceWithParent>,
    db_namespaces: &[NamespaceWithParent],
    warehouse_id: WarehouseId,
) -> bool {
    for db_namespace in db_namespaces {
        if let Some(ns_cached) = namespaces_from_cache.get(&db_namespace.namespace_id()) {
            // Check if namespace ident matches
            if db_namespace.namespace_ident() != ns_cached.namespace_ident() {
                tracing::debug!(
                    "Cached Namespace ident mismatch for namespace ID {} in warehouse {warehouse_id}: cached='{}', db='{}'. Refetching all namespaces.",
                    db_namespace.namespace_id(),
                    ns_cached.namespace_ident(),
                    db_namespace.namespace_ident()
                );
                return true;
            }

            // Check if DB version >= cached version
            if db_namespace.version() < ns_cached.version() {
                tracing::debug!(
                    "Cached Namespace version is newer than DB for namespace {} in warehouse {warehouse_id}: cached={:?}, db={:?}. Refetching all namespaces.",
                    db_namespace.namespace_ident(),
                    ns_cached.version(),
                    db_namespace.version()
                );
                return true;
            }
        }
    }
    false
}

/// Helper to add a namespace hierarchy to the cache map
fn add_hierarchy_to_cache_map(
    hierarchy: NamespaceHierarchy,
    cache_map: &mut HashMap<NamespaceId, NamespaceWithParent>,
) {
    cache_map.insert(hierarchy.namespace_id(), hierarchy.namespace);
    hierarchy.parents.into_iter().for_each(|parent_ns| {
        cache_map.insert(parent_ns.namespace_id(), parent_ns);
    });
}

/// Generic helper to get namespaces with caching, conflict detection, and optional refetch.
///
/// `apply_user_case_on_cache_hit` overlays the caller's case onto a cache-hit hierarchy
/// using the lookup key. For by-ident lookups it applies the caller's case; for by-id
/// lookups it is the identity function (no case to apply). This keeps cache semantics
/// consistent with the DB path (which sets `requested_ident` via the SQL COALESCE).
async fn get_namespaces_with_cache<'a, SOT, S, K, F, Apply>(
    warehouse_id: WarehouseId,
    keys: &[K],
    get_from_cache: impl Fn(&K) -> F,
    fetch_from_db: impl for<'b> Fn(
        WarehouseId,
        Vec<K>,
        &'b mut SOT,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<Vec<NamespaceWithParent>, CatalogGetNamespaceError>,
                > + Send
                + 'b,
        >,
    >,
    apply_user_case_on_cache_hit: Apply,
    state_or_transaction: &mut SOT,
) -> Result<HashMap<NamespaceId, NamespaceWithParent>, CatalogGetNamespaceError>
where
    S: CatalogStore,
    K: Clone + Eq + std::hash::Hash,
    F: std::future::Future<Output = Option<NamespaceHierarchy>>,
    Apply: Fn(&K, NamespaceHierarchy) -> NamespaceHierarchy,
    SOT: StateOrTransaction<S::State, <S::Transaction as Transaction<S::State>>::Transaction<'a>>,
{
    // Step 1: Deduplicate and get from cache
    let keys = keys
        .iter()
        .collect::<HashSet<_>>()
        .into_iter()
        .cloned()
        .collect::<Vec<_>>();

    let mut namespaces_from_cache = HashMap::new();
    let mut missing_keys = Vec::new();

    for key in &keys {
        match get_from_cache(key).await {
            Some(hierarchy) => {
                // Overlay caller's case onto the canonical cache entry.
                let hierarchy = apply_user_case_on_cache_hit(key, hierarchy);
                add_hierarchy_to_cache_map(hierarchy, &mut namespaces_from_cache);
            }
            None => missing_keys.push(key.clone()),
        }
    }

    // Step 2: Fetch missing from DB & Update Cache
    let db_namespaces = if missing_keys.is_empty() {
        Vec::new()
    } else {
        let fetched = fetch_from_db(warehouse_id, missing_keys, state_or_transaction).await?;
        namespace_cache_insert_multiple(fetched.clone()).await;
        fetched
    };

    // Step 3: Check for conflicts between cache and DB versions
    let version_conflicts =
        check_namespace_version_conflicts(&namespaces_from_cache, &db_namespaces, warehouse_id);

    // Step 4: If conflicts detected, refetch everything from DB
    let final_namespaces = if version_conflicts {
        let refetched = fetch_from_db(warehouse_id, keys, state_or_transaction).await?;
        namespace_cache_insert_multiple(refetched.clone()).await;
        refetched
            .into_iter()
            .map(|ns| (ns.namespace_id(), ns))
            .collect()
    } else {
        // Merge cached and DB hierarchies, preferring DB versions on conflict
        namespaces_from_cache.extend(db_namespaces.into_iter().map(|ns| (ns.namespace_id(), ns)));
        namespaces_from_cache
    };

    Ok(final_namespaces)
}

pub(crate) fn require_namespace_for_tabular<'a>(
    namespaces: &'a std::collections::HashMap<NamespaceId, NamespaceWithParent>,
    tabular: &impl BasicTabularInfo,
) -> Result<&'a NamespaceWithParent, AuthZCannotSeeNamespace> {
    namespaces.get(&tabular.namespace_id()).ok_or_else(|| {
        AuthZCannotSeeNamespace::new_not_found(
            tabular.warehouse_id(),
            tabular.tabular_ident().namespace.clone(),
        )
    })
}

#[async_trait::async_trait]
pub trait CatalogNamespaceOps
where
    Self: CatalogStore,
{
    /// Get a namespace by its ID or name.
    async fn get_namespace<'a, SOT>(
        warehouse_id: WarehouseId,
        namespace: impl Into<NamespaceIdentOrId> + Send,
        mut state_or_transaction: SOT,
    ) -> Result<Option<NamespaceHierarchy>, CatalogGetNamespaceError>
    where
        SOT: StateOrTransaction<
                Self::State,
                <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
            >,
    {
        let namespace = namespace.into();
        let cached = match namespace {
            NamespaceIdentOrId::Id(namespace_id) => namespace_cache_get_by_id(namespace_id).await,
            NamespaceIdentOrId::Name(ref namespace_ident) => {
                namespace_cache_get_by_ident(namespace_ident, warehouse_id).await
            }
        };

        if let Some(cached_namespace) = cached {
            // Cache always stores canonical case. If the lookup was by name, overlay
            // the caller's case onto the returned hierarchy so the response reflects
            // the caller's request — not whatever case a previous caller used.
            let result = match &namespace {
                NamespaceIdentOrId::Name(user_ident) => {
                    cached_namespace.with_user_ident(user_ident)
                }
                NamespaceIdentOrId::Id(_) => cached_namespace,
            };
            return Ok(Some(result));
        }

        let namespaces =
            fetch_namespace::<Self, _>(warehouse_id, namespace, &mut state_or_transaction).await?;
        namespace_cache_insert_multiple(namespaces.clone()).await;
        // DB rows already carry `requested_ident` per the SQL's COALESCE against
        // requested_parent_paths, so no further substitution is needed here.
        let namespace_hierarchy = build_namespace_hierarchy_from_vec(&namespaces);
        Ok(namespace_hierarchy)
    }

    /// Get all namespaces including their parents.
    /// If a namespace is not found, it is not in the returned Vec.
    async fn get_namespaces_by_id<'a, SOT>(
        warehouse_id: WarehouseId,
        namespaces: &[NamespaceId],
        mut state_or_transaction: SOT,
    ) -> Result<HashMap<NamespaceId, NamespaceWithParent>, CatalogGetNamespaceError>
    where
        SOT: StateOrTransaction<
                Self::State,
                <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
            >,
    {
        get_namespaces_with_cache::<SOT, Self, _, _, _>(
            warehouse_id,
            namespaces,
            |namespace_id| namespace_cache_get_by_id(*namespace_id),
            |wh_id, ns_ids, state| {
                Box::pin(
                    async move { Self::get_namespaces_by_id_impl(wh_id, &ns_ids, state).await },
                )
            },
            // By-id: no user-provided case; return canonical as-is.
            |_, hierarchy| hierarchy,
            &mut state_or_transaction,
        )
        .await
    }

    /// Get all namespaces including their parents.
    /// If a namespace is not found, it is not in the returned Vec.
    async fn get_namespaces_by_ident<'a, SOT>(
        warehouse_id: WarehouseId,
        namespaces: &[&NamespaceIdent],
        mut state_or_transaction: SOT,
    ) -> Result<HashMap<NamespaceId, NamespaceWithParent>, CatalogGetNamespaceError>
    where
        SOT: StateOrTransaction<
                Self::State,
                <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
            >,
    {
        get_namespaces_with_cache::<SOT, Self, _, _, _>(
            warehouse_id,
            namespaces,
            |namespace_ident| namespace_cache_get_by_ident(namespace_ident, warehouse_id),
            |wh_id, ns_ids, state| {
                let ns_ids = ns_ids.iter().map(|ns| (*ns).clone()).collect::<Vec<_>>();
                Box::pin(async move {
                    let ns_ids_refs = ns_ids.iter().collect::<Vec<_>>();
                    Self::get_namespaces_by_ident_impl(wh_id, &ns_ids_refs, state).await
                })
            },
            // By-ident: overlay the caller's case from the lookup key onto the
            // cached canonical hierarchy. This ensures cache-hit responses carry
            // the same case as DB-fetched responses.
            |user_ident, hierarchy| hierarchy.with_user_ident(user_ident),
            &mut state_or_transaction,
        )
        .await
    }

    /// Get warehouse by ID, invalidating cache if it's older than the provided timestamp
    async fn get_namespace_cache_aware<'a, SOT>(
        warehouse_id: WarehouseId,
        namespace: impl Into<NamespaceIdentOrId> + Send,
        cache_policy: CachePolicy,
        mut state_or_transaction: SOT,
    ) -> Result<Option<NamespaceHierarchy>, CatalogGetNamespaceError>
    where
        SOT: StateOrTransaction<
                Self::State,
                <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
            >,
    {
        let provided_namespace = namespace.into();
        let namespace = match cache_policy {
            CachePolicy::Skip => {
                // Skip cache entirely
                let namespaces = fetch_namespace::<Self, _>(
                    warehouse_id,
                    provided_namespace,
                    &mut state_or_transaction,
                )
                .await?;
                // Update cache with fresh data
                namespace_cache_insert_multiple(namespaces.clone()).await;
                build_namespace_hierarchy_from_vec(&namespaces)
            }
            CachePolicy::Use => {
                // Use cache if available
                Self::get_namespace(warehouse_id, provided_namespace, state_or_transaction).await?
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
                        let namespaces = fetch_namespace::<Self, _>(
                            warehouse_id,
                            provided_namespace,
                            &mut state_or_transaction,
                        )
                        .await?;
                        // Update cache with fresh data
                        namespace_cache_insert_multiple(namespaces.clone()).await;
                        build_namespace_hierarchy_from_vec(&namespaces)
                    }
                } else {
                    // No cache entry: fetch fresh data
                    let namespace = fetch_namespace::<Self, _>(
                        warehouse_id,
                        provided_namespace,
                        &mut state_or_transaction,
                    )
                    .await?;
                    namespace_cache_insert_multiple(namespace.clone()).await;
                    build_namespace_hierarchy_from_vec(&namespace)
                }
            }
        };

        Ok(namespace)
    }

    async fn list_namespaces<'a>(
        warehouse_id: WarehouseId,
        query: &ListNamespacesQuery,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<CatalogListNamespacesResponse, CatalogListNamespaceError> {
        let list_response = Self::list_namespaces_impl(warehouse_id, query, transaction).await?;

        let namespaces_for_cache = list_response
            .namespaces
            .iter()
            .map(|(_, ns)| ns.clone())
            .collect::<Vec<_>>();
        namespace_cache_insert_multiple(namespaces_for_cache).await;

        Ok(list_response)
    }

    async fn create_namespace<'a>(
        warehouse_id: WarehouseId,
        namespace_id: NamespaceId,
        request: CreateNamespaceRequest,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<NamespaceWithParent, CatalogCreateNamespaceError> {
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
    ) -> Result<NamespaceWithParent, CatalogUpdateNamespacePropertiesError> {
        Self::update_namespace_properties_impl(warehouse_id, namespace_id, properties, transaction)
            .await
    }

    async fn set_namespace_protected(
        warehouse_id: WarehouseId,
        namespace_id: NamespaceId,
        protect: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> Result<NamespaceWithParent, CatalogSetNamespaceProtectedError> {
        Self::set_namespace_protected_impl(warehouse_id, namespace_id, protect, transaction).await
    }
}

impl<T> CatalogNamespaceOps for T where T: CatalogStore {}
