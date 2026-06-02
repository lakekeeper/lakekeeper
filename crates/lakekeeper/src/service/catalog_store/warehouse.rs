use std::sync::Arc;

use http::StatusCode;
use iceberg::spec::FormatVersion;
use iceberg_ext::catalog::rest::ErrorModel;

use super::{CatalogStore, Transaction};
use crate::{
    ProjectId, SecretId, WarehouseId,
    api::management::v1::{DeleteWarehouseQuery, warehouse::TabularDeleteProfile},
    service::{
        ArcProjectId, DatabaseIntegrityError,
        catalog_store::{
            CatalogBackendError, define_transparent_error, impl_error_stack_methods,
            impl_from_with_detail,
            warehouse_cache::{
                warehouse_cache_get_by_id, warehouse_cache_get_by_name, warehouse_cache_insert,
            },
        },
        define_simple_error, define_version_newtype,
        storage::StorageProfile,
    },
};

/// Status of a warehouse
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    strum_macros::Display,
    strum_macros::EnumIter,
    serde::Serialize,
    serde::Deserialize,
)]
#[serde(rename_all = "kebab-case")]
#[strum(serialize_all = "kebab-case")]
#[cfg_attr(feature = "sqlx", derive(sqlx::Type))]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[cfg_attr(
    feature = "sqlx",
    sqlx(type_name = "warehouse_status", rename_all = "kebab-case")
)]
pub enum WarehouseStatus {
    /// The warehouse is active and can be used
    Active,
    /// The warehouse is inactive and cannot be used.
    Inactive,
}

impl WarehouseStatus {
    #[must_use]
    pub fn active_and_inactive() -> &'static [WarehouseStatus] {
        &[WarehouseStatus::Active, WarehouseStatus::Inactive]
    }

    #[must_use]
    pub fn active() -> &'static [WarehouseStatus] {
        &[WarehouseStatus::Active]
    }

    #[must_use]
    pub fn inactive() -> &'static [WarehouseStatus] {
        &[WarehouseStatus::Inactive]
    }
}

define_version_newtype!(WarehouseVersion);

/// The set of Iceberg table format versions that may be created in, or upgraded
/// to, within a warehouse. Always non-empty; deduplicated and sorted ascending.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AllowedFormatVersions(Vec<FormatVersion>);

impl Default for AllowedFormatVersions {
    /// All format versions supported by Lakekeeper.
    fn default() -> Self {
        Self(vec![
            FormatVersion::V1,
            FormatVersion::V2,
            FormatVersion::V3,
        ])
    }
}

impl AllowedFormatVersions {
    /// Build from an iterator of versions, deduplicating and sorting ascending.
    ///
    /// # Errors
    /// Returns [`EmptyAllowedFormatVersionsError`] if no versions are provided.
    pub fn try_new(
        versions: impl IntoIterator<Item = FormatVersion>,
    ) -> Result<Self, EmptyAllowedFormatVersionsError> {
        let mut versions: Vec<FormatVersion> = versions.into_iter().collect();
        versions.sort_unstable();
        versions.dedup();
        if versions.is_empty() {
            return Err(EmptyAllowedFormatVersionsError::new());
        }
        Ok(Self(versions))
    }

    /// Whether `version` is permitted.
    #[must_use]
    pub fn contains(&self, version: FormatVersion) -> bool {
        self.0.contains(&version)
    }

    /// Highest allowed format version. The set is non-empty, so this always
    /// returns a value; `V2` is only a defensive fallback.
    #[must_use]
    pub fn max(&self) -> FormatVersion {
        self.0.iter().copied().max().unwrap_or(FormatVersion::V2)
    }

    /// Resolve the effective format version for a create-table request that does
    /// not specify one: the configured `default_format_version` if set, otherwise
    /// `V2` if allowed, else the highest allowed version.
    #[must_use]
    pub fn resolve_default(&self, configured: Option<FormatVersion>) -> FormatVersion {
        configured.unwrap_or_else(|| {
            if self.contains(FormatVersion::V2) {
                FormatVersion::V2
            } else {
                self.max()
            }
        })
    }

    #[must_use]
    pub fn as_slice(&self) -> &[FormatVersion] {
        &self.0
    }

    #[must_use]
    pub fn to_vec(&self) -> Vec<FormatVersion> {
        self.0.clone()
    }
}

define_simple_error!(
    EmptyAllowedFormatVersionsError,
    "allowed_format_versions must contain at least one format version."
);

impl From<EmptyAllowedFormatVersionsError> for ErrorModel {
    fn from(err: EmptyAllowedFormatVersionsError) -> Self {
        ErrorModel::builder()
            .r#type("EmptyAllowedFormatVersions")
            .code(StatusCode::BAD_REQUEST.as_u16())
            .message(err.to_string())
            .stack(err.stack)
            .build()
    }
}

/// Per-warehouse Iceberg table format version policy: which versions may be
/// created in / upgraded to, and the default applied when a create-table request
/// omits one.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WarehouseFormatVersionPolicy {
    /// Versions that may be created in, or upgraded to, within the warehouse.
    pub allowed_format_versions: AllowedFormatVersions,
    /// Default version used when a create-table request omits one. When `None`,
    /// resolves to `V2` if allowed, otherwise the highest allowed version.
    pub default_format_version: Option<FormatVersion>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedWarehouse {
    /// ID of the warehouse.
    pub warehouse_id: WarehouseId,
    /// Name of the warehouse.
    pub name: String,
    /// Project ID in which the warehouse is created.
    pub project_id: ArcProjectId,
    /// Storage profile used for the warehouse.
    pub storage_profile: StorageProfile,
    /// Storage secret ID used for the warehouse.
    pub storage_secret_id: Option<SecretId>,
    /// Whether the warehouse is active.
    pub status: WarehouseStatus,
    /// Tabular delete profile used for the warehouse.
    pub tabular_delete_profile: TabularDeleteProfile,
    /// Whether the warehouse is protected from being deleted.
    pub protected: bool,
    /// Iceberg table format versions that may be created in, or upgraded to,
    /// within this warehouse.
    pub allowed_format_versions: AllowedFormatVersions,
    /// Default Iceberg table format version used when a create-table request
    /// does not specify one. When `None`, resolves to `V2` if allowed, otherwise
    /// the highest allowed version. Always a member of `allowed_format_versions`.
    pub default_format_version: Option<FormatVersion>,
    /// Timestamp when the warehouse metadata was last updated.
    pub updated_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Version of the warehouse entity.
    /// Increments on each update to the warehouse.
    pub version: WarehouseVersion,
}

impl ResolvedWarehouse {
    #[cfg(feature = "test-utils")]
    #[must_use]
    pub fn new_random() -> Self {
        use crate::service::storage::MemoryProfile;
        let warehouse_id = WarehouseId::new_random();
        let name = format!("warehouse_{}", warehouse_id.as_u128());

        Self {
            warehouse_id,
            name,
            project_id: Arc::new(ProjectId::new_random()),
            storage_profile: MemoryProfile::default().into(),
            storage_secret_id: None,
            status: WarehouseStatus::Active,
            tabular_delete_profile: TabularDeleteProfile::default(),
            protected: false,
            allowed_format_versions: AllowedFormatVersions::default(),
            default_format_version: None,
            updated_at: None,
            version: WarehouseVersion(0),
        }
    }

    #[cfg(feature = "test-utils")]
    #[must_use]
    pub fn new_with_id(warehouse_id: WarehouseId) -> Self {
        use crate::service::storage::MemoryProfile;
        let name = format!("warehouse_{}", warehouse_id.as_u128());

        Self {
            warehouse_id,
            name,
            project_id: Arc::new(ProjectId::new_random()),
            storage_profile: MemoryProfile::default().into(),
            storage_secret_id: None,
            status: WarehouseStatus::Active,
            tabular_delete_profile: TabularDeleteProfile::default(),
            protected: false,
            allowed_format_versions: AllowedFormatVersions::default(),
            default_format_version: None,
            updated_at: None,
            version: WarehouseVersion(0),
        }
    }
}

// --------------------------- GENERAL ERROR ---------------------------
#[derive(thiserror::Error, Debug, PartialEq)]
#[error("A warehouse with id '{warehouse_id}' does not exist")]
pub struct WarehouseIdNotFound {
    pub warehouse_id: WarehouseId,
    pub stack: Vec<String>,
}
impl WarehouseIdNotFound {
    #[must_use]
    pub fn new(warehouse_id: WarehouseId) -> Self {
        Self {
            warehouse_id,
            stack: Vec::new(),
        }
    }
}
impl_error_stack_methods!(WarehouseIdNotFound);

impl From<WarehouseIdNotFound> for ErrorModel {
    fn from(err: WarehouseIdNotFound) -> Self {
        ErrorModel::builder()
            .r#type("NoSuchWarehouseException")
            .code(StatusCode::NOT_FOUND.as_u16())
            .message(err.to_string())
            .stack(err.stack)
            .build()
    }
}

#[derive(thiserror::Error, Debug, PartialEq)]
#[error("Warehouse id is missing")]
pub struct WarehouseIdMissing {
    pub stack: Vec<String>,
}
impl Default for WarehouseIdMissing {
    fn default() -> Self {
        Self::new()
    }
}

impl WarehouseIdMissing {
    #[must_use]
    pub fn new() -> Self {
        Self { stack: Vec::new() }
    }
}
impl_error_stack_methods!(WarehouseIdMissing);

impl From<WarehouseIdMissing> for ErrorModel {
    fn from(err: WarehouseIdMissing) -> Self {
        ErrorModel::builder()
            .r#type("WarehouseIdMissing")
            .code(StatusCode::NOT_FOUND.as_u16())
            .message(err.to_string())
            .stack(err.stack)
            .build()
    }
}

#[derive(thiserror::Error, Debug, PartialEq)]
#[error("A warehouse '{warehouse_name}' does not exist")]
pub struct WarehouseNameNotFound {
    pub warehouse_name: String,
    pub stack: Vec<String>,
}
impl WarehouseNameNotFound {
    #[must_use]
    pub fn new(warehouse_name: impl Into<String>) -> Self {
        Self {
            warehouse_name: warehouse_name.into(),
            stack: Vec::new(),
        }
    }
}
impl_error_stack_methods!(WarehouseNameNotFound);

impl From<WarehouseNameNotFound> for ErrorModel {
    fn from(err: WarehouseNameNotFound) -> Self {
        ErrorModel::builder()
            .r#type("NoSuchWarehouseException")
            .code(StatusCode::NOT_FOUND.as_u16())
            .message(err.to_string())
            .stack(err.stack)
            .build()
    }
}

#[derive(thiserror::Error, Debug)]
#[error("Error serializing storage profile: {source}")]
pub struct StorageProfileSerializationError {
    source: serde_json::Error,
    stack: Vec<String>,
}
impl_error_stack_methods!(StorageProfileSerializationError);
impl From<serde_json::Error> for StorageProfileSerializationError {
    fn from(source: serde_json::Error) -> Self {
        Self {
            source,
            stack: Vec::new(),
        }
    }
}
impl PartialEq for StorageProfileSerializationError {
    fn eq(&self, other: &Self) -> bool {
        self.source.to_string() == other.source.to_string() && self.stack == other.stack
    }
}
impl From<StorageProfileSerializationError> for ErrorModel {
    fn from(err: StorageProfileSerializationError) -> Self {
        let message = err.to_string();
        let StorageProfileSerializationError { source, stack } = err;

        ErrorModel::builder()
            .r#type("StorageProfileSerializationError")
            .code(StatusCode::INTERNAL_SERVER_ERROR.as_u16())
            .message(message)
            .stack(stack)
            .source(Some(Box::new(source)))
            .build()
    }
}

// --------------------------- CREATE ERROR ---------------------------
define_transparent_error! {
    pub enum CatalogCreateWarehouseError,
    stack_message: "Error creating warehouse in catalog",
    variants: [
        WarehouseAlreadyExists,
        CatalogBackendError,
        StorageProfileSerializationError,
        ProjectIdNotFoundError,
        DatabaseIntegrityError,
    ]
}

#[derive(thiserror::Error, PartialEq, Debug)]
#[error(
    "A warehouse with the name '{warehouse_name}' already exists in project with id '{project_id}'"
)]
pub struct WarehouseAlreadyExists {
    pub warehouse_name: String,
    pub project_id: ProjectId,
    pub stack: Vec<String>,
}
impl WarehouseAlreadyExists {
    #[must_use]
    pub fn new(warehouse_name: String, project_id: ProjectId) -> Self {
        Self {
            warehouse_name,
            project_id,
            stack: Vec::new(),
        }
    }
}
impl_error_stack_methods!(WarehouseAlreadyExists);

impl From<WarehouseAlreadyExists> for ErrorModel {
    fn from(err: WarehouseAlreadyExists) -> Self {
        ErrorModel::builder()
            .r#type("WarehouseAlreadyExists")
            .code(StatusCode::CONFLICT.as_u16())
            .message(err.to_string())
            .stack(err.stack)
            .build()
    }
}

#[derive(thiserror::Error, PartialEq, Debug)]
#[error("Project with id '{project_id}' not found")]
pub struct ProjectIdNotFoundError {
    project_id: ProjectId,
    stack: Vec<String>,
}
impl_error_stack_methods!(ProjectIdNotFoundError);
impl ProjectIdNotFoundError {
    #[must_use]
    pub fn new(project_id: ProjectId) -> Self {
        Self {
            project_id,
            stack: Vec::new(),
        }
    }
}
impl From<ProjectIdNotFoundError> for ErrorModel {
    fn from(err: ProjectIdNotFoundError) -> Self {
        ErrorModel::builder()
            .r#type("ProjectNotFound")
            .code(StatusCode::NOT_FOUND.as_u16())
            .message(err.to_string())
            .stack(err.stack)
            .build()
    }
}

// --------------------------- DELETE ERROR ---------------------------
define_transparent_error! {
    pub enum CatalogDeleteWarehouseError,
    stack_message: "Error deleting warehouse in catalog",
    variants: [
        CatalogBackendError,
        WarehouseHasUnfinishedTasks,
        WarehouseIdNotFound,
        WarehouseNotEmpty,
        WarehouseProtected,
    ]
}

define_simple_error!(
    WarehouseHasUnfinishedTasks,
    "Warehouse has unfinished tasks. Cannot delete warehouse until all tasks are finished."
);

impl From<WarehouseHasUnfinishedTasks> for ErrorModel {
    fn from(err: WarehouseHasUnfinishedTasks) -> Self {
        ErrorModel::builder()
            .r#type("WarehouseHasUnfinishedTasks")
            .code(StatusCode::CONFLICT.as_u16())
            .message(err.to_string())
            .stack(err.stack)
            .build()
    }
}

define_simple_error!(
    WarehouseNotEmpty,
    "Warehouse is not empty. Cannot delete a non-empty warehouse."
);
define_simple_error!(
    WarehouseProtected,
    "Warehouse is protected and force flag not set. Cannot delete protected warehouse."
);

impl From<WarehouseNotEmpty> for ErrorModel {
    fn from(err: WarehouseNotEmpty) -> Self {
        ErrorModel::builder()
            .r#type("WarehouseNotEmpty")
            .code(StatusCode::CONFLICT.as_u16())
            .message(err.to_string())
            .stack(err.stack)
            .build()
    }
}
impl From<WarehouseProtected> for ErrorModel {
    fn from(err: WarehouseProtected) -> Self {
        ErrorModel::builder()
            .r#type("WarehouseProtected")
            .code(StatusCode::CONFLICT.as_u16())
            .message(err.to_string())
            .stack(err.stack)
            .build()
    }
}

// --------------------------- RENAME ERROR ---------------------------
define_transparent_error! {
    pub enum CatalogRenameWarehouseError,
    stack_message: "Error renaming warehouse in catalog",
    variants: [
        CatalogBackendError,
        WarehouseIdNotFound,
        DatabaseIntegrityError,
    ]
}

// --------------------------- LIST ERROR ---------------------------
define_transparent_error! {
    pub enum CatalogListWarehousesError,
    stack_message: "Error listing warehouses in catalog",
    variants: [
        CatalogBackendError,
        DatabaseIntegrityError,
    ]
}

// --------------------------- GET ERROR ---------------------------
define_transparent_error! {
    pub enum CatalogGetWarehouseByIdError,
    stack_message: "Error getting warehouse by id in catalog",
    variants: [
        CatalogBackendError,
        DatabaseIntegrityError,
    ]
}

define_transparent_error! {
    pub enum CatalogGetWarehouseByNameError,
    stack_message: "Error getting warehouse by name in catalog",
    variants: [
        CatalogBackendError,
        DatabaseIntegrityError,
        WarehouseNameNotFound,
    ]
}

// --------------------------- Set Warehouse Delete Profile Error ---------------------------
define_transparent_error! {
    pub enum SetWarehouseDeletionProfileError,
    stack_message: "Error setting warehouse deletion profile in catalog",
    variants: [
        CatalogBackendError,
        WarehouseIdNotFound,
        DatabaseIntegrityError,
    ]
}

// --------------------------- Set Warehouse Status Error ---------------------------
define_transparent_error! {
    pub enum SetWarehouseStatusError,
    stack_message: "Error setting warehouse status in catalog",
    variants: [
        CatalogBackendError,
        WarehouseIdNotFound,
        DatabaseIntegrityError,
    ]
}

// --------------------------- Update Warehouse Storage Profile ----------------------
define_transparent_error! {
    pub enum UpdateWarehouseStorageProfileError,
    stack_message: "Error updating warehouse storage profile in catalog",
    variants: [
        CatalogBackendError,
        WarehouseIdNotFound,
        StorageProfileSerializationError,
        DatabaseIntegrityError,
    ]
}

// --------------------------- Set Warehouse Protected Error ---------------------------
define_transparent_error! {
    pub enum SetWarehouseProtectedError,
    stack_message: "Error setting warehouse protection in catalog",
    variants: [
        CatalogBackendError,
        WarehouseIdNotFound,
        DatabaseIntegrityError,
    ]
}

// --------------------- Set Warehouse Format Version Policy Error ---------------------
define_transparent_error! {
    pub enum SetWarehouseFormatVersionPolicyError,
    stack_message: "Error setting warehouse format version policy in catalog",
    variants: [
        CatalogBackendError,
        WarehouseIdNotFound,
        DatabaseIntegrityError,
    ]
}

#[derive(Debug, Clone, Default, Copy)]
pub enum CachePolicy {
    /// Use cached data if available
    #[default]
    Use,
    /// Only use cached data newer or equal to the specified version
    RequireMinimumVersion(i64),
    /// Skip the cache and always fetch from the database
    Skip,
}

#[async_trait::async_trait]
pub trait CatalogWarehouseOps
where
    Self: CatalogStore,
{
    /// Create a warehouse.
    async fn create_warehouse<'a>(
        warehouse_name: String,
        project_id: &ProjectId,
        storage_profile: StorageProfile,
        tabular_delete_profile: TabularDeleteProfile,
        storage_secret_id: Option<SecretId>,
        format_version_policy: WarehouseFormatVersionPolicy,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<Arc<ResolvedWarehouse>, CatalogCreateWarehouseError> {
        let warehouse = Self::create_warehouse_impl(
            warehouse_name,
            project_id,
            storage_profile,
            tabular_delete_profile,
            storage_secret_id,
            format_version_policy,
            transaction,
        )
        .await?;
        let warehouse_ref = Arc::new(warehouse);
        Ok(warehouse_ref)
    }

    /// Delete a warehouse.
    async fn delete_warehouse<'a>(
        warehouse_id: WarehouseId,
        query: DeleteWarehouseQuery,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<(), CatalogDeleteWarehouseError> {
        Self::delete_warehouse_impl(warehouse_id, query, transaction).await?;
        Ok(())
    }

    /// Rename a warehouse.
    async fn rename_warehouse<'a>(
        warehouse_id: WarehouseId,
        new_name: &str,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<Arc<ResolvedWarehouse>, CatalogRenameWarehouseError> {
        Self::rename_warehouse_impl(warehouse_id, new_name, transaction)
            .await
            .map(Arc::new)
    }

    /// Return a list of all warehouse in a project
    async fn list_warehouses(
        project_id: &ProjectId,
        // If None, returns active warehouses
        // If Some, returns warehouses with any of the statuses in the set
        include_inactive: Option<Vec<WarehouseStatus>>,
        state: Self::State,
    ) -> Result<Vec<Arc<ResolvedWarehouse>>, CatalogListWarehousesError> {
        let warehouses = Self::list_warehouses_impl(project_id, include_inactive, state)
            .await?
            .into_iter()
            .map(Arc::new)
            .collect::<Vec<_>>();

        let mut tasks = Vec::with_capacity(warehouses.len());
        for warehouse in &warehouses {
            tasks.push(warehouse_cache_insert(warehouse.clone()));
        }

        futures::future::join_all(tasks).await;

        Ok(warehouses)
    }

    /// Get the warehouse metadata.
    ///
    /// Return Ok(None) if the warehouse does not exist.
    async fn get_warehouse_by_id<'a>(
        warehouse_id: WarehouseId,
        status_filter: &[WarehouseStatus],
        state: Self::State,
    ) -> Result<Option<Arc<ResolvedWarehouse>>, CatalogGetWarehouseByIdError> {
        let cached_warehouse = warehouse_cache_get_by_id(warehouse_id).await;
        if let Some(warehouse) = cached_warehouse {
            let warehouse = Some(warehouse).filter(|w| status_filter.contains(&w.status));
            return Ok(warehouse);
        }

        let warehouse = Self::get_warehouse_by_id_impl(warehouse_id, state)
            .await?
            .map(Arc::new);

        if let Some(warehouse) = warehouse.clone() {
            warehouse_cache_insert(warehouse).await;
        }

        let warehouse = warehouse.filter(|w| status_filter.contains(&w.status));

        Ok(warehouse)
    }

    async fn get_active_warehouse_by_id(
        warehouse_id: WarehouseId,
        state: Self::State,
    ) -> Result<Option<Arc<ResolvedWarehouse>>, CatalogGetWarehouseByIdError> {
        Self::get_warehouse_by_id(warehouse_id, WarehouseStatus::active(), state).await
    }

    /// Get warehouse by ID, invalidating cache if it's older than the provided timestamp
    async fn get_warehouse_by_id_cache_aware(
        warehouse_id: WarehouseId,
        status_filter: &[WarehouseStatus],
        cache_policy: CachePolicy,
        state: Self::State,
    ) -> Result<Option<Arc<ResolvedWarehouse>>, CatalogGetWarehouseByIdError> {
        let warehouse = match cache_policy {
            CachePolicy::Skip => {
                // Skip cache entirely
                let warehouse = Self::get_warehouse_by_id_impl(warehouse_id, state)
                    .await?
                    .map(Arc::new);

                // Update cache with fresh data
                if let Some(warehouse) = warehouse.clone() {
                    warehouse_cache_insert(warehouse).await;
                }

                warehouse
            }
            CachePolicy::Use => {
                // Use cache if available
                Self::get_warehouse_by_id(warehouse_id, status_filter, state).await?
            }
            CachePolicy::RequireMinimumVersion(require_min_version) => {
                // Check cache first
                let cached_warehouse = warehouse_cache_get_by_id(warehouse_id).await;

                if let Some(warehouse) = cached_warehouse {
                    // Determine if cache is valid based on version
                    let cache_is_valid = warehouse.version.0 >= require_min_version;

                    if cache_is_valid {
                        Some(warehouse)
                    } else {
                        tracing::debug!(
                            "Detected stale cache for warehouse {}: cached={:?}, required={:?}. Refreshing.",
                            warehouse_id,
                            warehouse.version,
                            require_min_version
                        );
                        // Cache is stale: fetch fresh data
                        let warehouse = Self::get_warehouse_by_id_impl(warehouse_id, state)
                            .await?
                            .map(Arc::new);
                        // Update cache with fresh data
                        if let Some(warehouse) = warehouse.clone() {
                            warehouse_cache_insert(warehouse).await;
                        }
                        warehouse
                    }
                } else {
                    // No cache entry: fetch fresh data
                    let warehouse = Self::get_warehouse_by_id_impl(warehouse_id, state)
                        .await?
                        .map(Arc::new);
                    // Update cache with fresh data
                    if let Some(warehouse) = warehouse.clone() {
                        warehouse_cache_insert(warehouse).await;
                    }
                    warehouse
                }
            }
        }.filter(|w| status_filter.contains(&w.status));

        Ok(warehouse)
    }

    async fn get_warehouse_by_name(
        warehouse_name: &str,
        project_id: &ArcProjectId,
        status_filter: &[WarehouseStatus],
        catalog_state: Self::State,
    ) -> Result<Option<Arc<ResolvedWarehouse>>, CatalogGetWarehouseByNameError> {
        let cached_warehouse = warehouse_cache_get_by_name(warehouse_name, project_id).await;
        if let Some(warehouse) = cached_warehouse {
            let warehouse = Some(warehouse).filter(|w| status_filter.contains(&w.status));
            return Ok(warehouse);
        }

        let warehouse = Self::get_warehouse_by_name_impl(warehouse_name, project_id, catalog_state)
            .await?
            .map(Arc::new);
        if let Some(warehouse) = warehouse.clone() {
            warehouse_cache_insert(warehouse).await;
        }

        let warehouse = warehouse.filter(|w| status_filter.contains(&w.status));

        Ok(warehouse)
    }

    // /// Wrapper around `get_warehouse_by_name` that returns
    // /// not found error if the warehouse does not exist.
    // async fn require_warehouse_by_name(
    //     warehouse_name: &str,
    //     project_id: &ProjectId,
    //     catalog_state: Self::State,
    // ) -> Result<Arc<ResolvedWarehouse>, CatalogGetWarehouseByNameError> {
    //     Self::get_warehouse_by_name(warehouse_name, project_id, catalog_state)
    //         .await?
    //         .ok_or(WarehouseNameNotFound::new(warehouse_name.to_string()).into())
    // }

    /// Set warehouse deletion profile
    async fn set_warehouse_deletion_profile<'a>(
        warehouse_id: WarehouseId,
        deletion_profile: &TabularDeleteProfile,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<Arc<ResolvedWarehouse>, SetWarehouseDeletionProfileError> {
        Self::set_warehouse_deletion_profile_impl(warehouse_id, deletion_profile, transaction)
            .await
            .map(Arc::new)
    }

    async fn set_warehouse_status<'a>(
        warehouse_id: WarehouseId,
        status: WarehouseStatus,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<Arc<ResolvedWarehouse>, SetWarehouseStatusError> {
        Self::set_warehouse_status_impl(warehouse_id, status, transaction)
            .await
            .map(Arc::new)
    }

    async fn update_storage_profile<'a>(
        warehouse_id: WarehouseId,
        storage_profile: StorageProfile,
        storage_secret_id: Option<SecretId>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<Arc<ResolvedWarehouse>, UpdateWarehouseStorageProfileError> {
        Self::update_storage_profile_impl(
            warehouse_id,
            storage_profile,
            storage_secret_id,
            transaction,
        )
        .await
        .map(Arc::new)
    }

    async fn set_warehouse_protected(
        warehouse_id: WarehouseId,
        protect: bool,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> std::result::Result<Arc<ResolvedWarehouse>, SetWarehouseProtectedError> {
        Self::set_warehouse_protected_impl(warehouse_id, protect, transaction)
            .await
            .map(Arc::new)
    }

    /// Set the per-warehouse Iceberg table format version policy.
    async fn set_warehouse_format_version_policy(
        warehouse_id: WarehouseId,
        policy: &WarehouseFormatVersionPolicy,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> std::result::Result<Arc<ResolvedWarehouse>, SetWarehouseFormatVersionPolicyError> {
        Self::set_warehouse_format_version_policy_impl(warehouse_id, policy, transaction)
            .await
            .map(Arc::new)
    }
}

impl<T> CatalogWarehouseOps for T where T: CatalogStore {}

#[cfg(test)]
mod allowed_format_versions_tests {
    use iceberg::spec::FormatVersion;

    use super::AllowedFormatVersions;

    #[test]
    fn try_new_rejects_empty() {
        assert!(AllowedFormatVersions::try_new([]).is_err());
    }

    #[test]
    fn try_new_dedups_and_sorts() {
        let allowed = AllowedFormatVersions::try_new([
            FormatVersion::V3,
            FormatVersion::V2,
            FormatVersion::V3,
        ])
        .unwrap();
        assert_eq!(allowed.as_slice(), &[FormatVersion::V2, FormatVersion::V3]);
    }

    #[test]
    fn contains_and_max() {
        let allowed =
            AllowedFormatVersions::try_new([FormatVersion::V1, FormatVersion::V2]).unwrap();
        assert!(allowed.contains(FormatVersion::V1));
        assert!(!allowed.contains(FormatVersion::V3));
        assert_eq!(allowed.max(), FormatVersion::V2);
    }

    #[test]
    fn resolve_default_prefers_configured() {
        let allowed = AllowedFormatVersions::default();
        assert_eq!(
            allowed.resolve_default(Some(FormatVersion::V1)),
            FormatVersion::V1
        );
    }

    #[test]
    fn resolve_default_falls_back_to_v2_when_allowed() {
        let allowed = AllowedFormatVersions::try_new([
            FormatVersion::V1,
            FormatVersion::V2,
            FormatVersion::V3,
        ])
        .unwrap();
        assert_eq!(allowed.resolve_default(None), FormatVersion::V2);
    }

    #[test]
    fn resolve_default_falls_back_to_max_when_v2_disallowed() {
        let allowed =
            AllowedFormatVersions::try_new([FormatVersion::V1, FormatVersion::V3]).unwrap();
        assert_eq!(allowed.resolve_default(None), FormatVersion::V3);
    }
}
