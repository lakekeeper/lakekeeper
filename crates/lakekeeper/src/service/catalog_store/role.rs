use std::{collections::HashSet, sync::Arc};

use http::StatusCode;
use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};

use crate::{
    CONFIG, ProjectId,
    api::{
        iceberg::{types::PageToken, v1::PaginationQuery},
        management::v1::role::UpdateRoleSourceSystemRequest,
    },
    service::{
        ArcProjectId, CachePolicy, CatalogBackendError, CatalogCreateRoleRequest, CatalogStore,
        InvalidPaginationToken, ProjectIdNotFoundError, ResultCountMismatch, RoleId, RoleIdent,
        RoleProviderId, RoleSourceId, Transaction,
        catalog_store::{
            define_version_newtype,
            role_cache::{role_cache_get_by_id, role_cache_get_by_ident, role_cache_insert},
        },
        define_transparent_error,
        identifier::role::ArcRoleIdent,
        impl_error_stack_methods, impl_from_with_detail,
    },
};

define_version_newtype!(RoleVersion);

/// Reference to a [`Role`]
pub type ArcRole = Arc<Role>;

#[derive(Debug, PartialEq, Clone, Eq)]
pub struct Role {
    /// Global unique identifier for the role.
    pub id: RoleId,
    pub ident: ArcRoleIdent,
    /// Name of the role
    pub name: String,
    /// Description of the role
    pub description: Option<String>,
    /// Project ID in which the role is created.
    pub project_id: ArcProjectId,
    /// Timestamp when the role was created
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// Timestamp when the role was last updated
    pub updated_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Monotonically increasing version counter, incremented on every update.
    pub version: RoleVersion,
}

impl std::fmt::Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Role(name={}, provider_id={}, source_id={}, project_id={})",
            self.name,
            self.ident.provider_id(),
            self.ident.source_id(),
            self.project_id
        )
    }
}

impl Role {
    #[must_use]
    pub fn source_id(&self) -> &RoleSourceId {
        self.ident.source_id()
    }

    #[must_use]
    pub fn provider_id(&self) -> &RoleProviderId {
        self.ident.provider_id()
    }

    #[must_use]
    pub fn project_id(&self) -> &ProjectId {
        &self.project_id
    }

    #[must_use]
    pub fn project_id_arc(&self) -> ArcProjectId {
        self.project_id.clone()
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub fn id(&self) -> RoleId {
        self.id
    }

    #[must_use]
    pub fn ident(&self) -> &RoleIdent {
        &self.ident
    }

    #[must_use]
    pub fn ident_arc(&self) -> ArcRoleIdent {
        self.ident.clone()
    }

    #[cfg(feature = "test-utils")]
    #[must_use]
    pub fn new_random() -> Self {
        let id = RoleId::new_random();
        Self::new_random_with_id(id)
    }

    #[cfg(feature = "test-utils")]
    #[must_use]
    pub fn new_random_with_id(id: RoleId) -> Self {
        let ident = Arc::new(crate::service::RoleIdent::new_internal_with_role_id(id));
        Self {
            name: format!("role-{id}"),
            id,
            ident,
            description: Some("A randomly generated role".to_string()),
            project_id: Arc::new(ProjectId::new_random()),
            created_at: chrono::Utc::now(),
            updated_at: None,
            version: RoleVersion::new(0),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct ListRolesResponse {
    pub roles: Vec<ArcRole>,
    pub next_page_token: Option<String>,
}

#[derive(Debug, PartialEq)]
pub struct SearchRoleResponse {
    /// List of roles matching the search criteria
    pub roles: Vec<ArcRole>,
}

#[derive(thiserror::Error, Debug, PartialEq)]
#[error("A role with id '{role_id}' does not exist in project with id '{project_id}'")]
pub struct RoleIdNotFoundInProject {
    pub role_id: RoleId,
    pub project_id: ProjectId,
    pub stack: Vec<String>,
}
impl RoleIdNotFoundInProject {
    #[must_use]
    pub fn new(role_id: RoleId, project_id: ProjectId) -> Self {
        Self {
            role_id,
            project_id,
            stack: Vec::new(),
        }
    }
}
impl_error_stack_methods!(RoleIdNotFoundInProject);

impl From<RoleIdNotFoundInProject> for ErrorModel {
    fn from(err: RoleIdNotFoundInProject) -> Self {
        ErrorModel::builder()
            .r#type("RoleNotFoundInProject")
            .code(StatusCode::NOT_FOUND.as_u16())
            .message(err.to_string())
            .stack(err.stack)
            .build()
    }
}

#[derive(thiserror::Error, Debug, PartialEq)]
#[error("A role with id '{role_id}' does not exist")]
pub struct RoleIdNotFound {
    pub role_id: RoleId,
    pub stack: Vec<String>,
}
impl RoleIdNotFound {
    #[must_use]
    pub fn new(role_id: RoleId) -> Self {
        Self {
            role_id,
            stack: Vec::new(),
        }
    }
}
impl_error_stack_methods!(RoleIdNotFound);

impl From<RoleIdNotFound> for ErrorModel {
    fn from(err: RoleIdNotFound) -> Self {
        ErrorModel::builder()
            .r#type("RoleIdNotFound")
            .code(StatusCode::NOT_FOUND.as_u16())
            .message(err.to_string())
            .stack(err.stack)
            .build()
    }
}

// --------------------------- CREATE ERROR ---------------------------
define_transparent_error! {
    pub enum CreateRoleError,
    stack_message: "Error creating role in catalog",
    variants: [
        RoleNameAlreadyExists,
        CatalogBackendError,
        ProjectIdNotFoundError,
        RoleSourceIdConflict,
        ResultCountMismatch
    ]
}

#[derive(thiserror::Error, PartialEq, Debug, Default)]
#[error("A role with the specified name already exists in the specified project")]
pub struct RoleNameAlreadyExists {
    pub stack: Vec<String>,
}
impl RoleNameAlreadyExists {
    #[must_use]
    pub fn new() -> Self {
        Self { stack: Vec::new() }
    }
}
impl_error_stack_methods!(RoleNameAlreadyExists);
impl From<RoleNameAlreadyExists> for ErrorModel {
    fn from(err: RoleNameAlreadyExists) -> Self {
        ErrorModel::builder()
            .r#type("RoleNameAlreadyExists")
            .code(StatusCode::CONFLICT.as_u16())
            .message(err.to_string())
            .stack(err.stack)
            .build()
    }
}

#[derive(thiserror::Error, PartialEq, Debug, Default)]
#[error(
    "A role with the specified combination of (project_id, provider_id, source_id) already exists"
)]
pub struct RoleSourceIdConflict {
    pub stack: Vec<String>,
}
impl RoleSourceIdConflict {
    #[must_use]
    pub fn new() -> Self {
        Self { stack: Vec::new() }
    }
}
impl_error_stack_methods!(RoleSourceIdConflict);
impl From<RoleSourceIdConflict> for ErrorModel {
    fn from(err: RoleSourceIdConflict) -> Self {
        ErrorModel::builder()
            .r#type("RoleSourceIdConflict")
            .code(StatusCode::CONFLICT.as_u16())
            .message(err.to_string())
            .stack(err.stack)
            .build()
    }
}

// --------------------------- LIST ERROR ---------------------------
define_transparent_error! {
    pub enum ListRolesError,
    stack_message: "Error listing Roles catalog",
    variants: [
        CatalogBackendError,
        InvalidPaginationToken
    ]
}

// --------------------------- GET ROLE ERROR ---------------------------
define_transparent_error! {
    pub enum GetRoleInProjectError,
    stack_message: "Error getting Role from catalog",
    variants: [
        CatalogBackendError,
        InvalidPaginationToken,
        RoleIdNotFoundInProject,
    ]
}

impl From<ListRolesError> for GetRoleInProjectError {
    fn from(err: ListRolesError) -> Self {
        match err {
            ListRolesError::CatalogBackendError(e) => e.into(),
            ListRolesError::InvalidPaginationToken(e) => e.into(),
        }
    }
}

define_transparent_error! {
    pub enum GetRoleAcrossProjectsError,
    stack_message: "Error getting Role from catalog",
    variants: [
        CatalogBackendError,
        InvalidPaginationToken,
        RoleIdNotFound,
    ]
}

impl From<ListRolesError> for GetRoleAcrossProjectsError {
    fn from(err: ListRolesError) -> Self {
        match err {
            ListRolesError::CatalogBackendError(e) => e.into(),
            ListRolesError::InvalidPaginationToken(e) => e.into(),
        }
    }
}

// --------------------------- GET ROLE BY IDENT ERROR ---------------------------

#[derive(thiserror::Error, Debug, PartialEq)]
#[error("A role with ident '{ident}' does not exist in project '{project_id}'")]
pub struct RoleIdentNotFoundInProject {
    pub ident: ArcRoleIdent,
    pub project_id: ArcProjectId,
    pub stack: Vec<String>,
}
impl RoleIdentNotFoundInProject {
    #[must_use]
    pub fn new(ident: ArcRoleIdent, project_id: ArcProjectId) -> Self {
        Self {
            ident,
            project_id,
            stack: Vec::new(),
        }
    }
}
impl_error_stack_methods!(RoleIdentNotFoundInProject);

impl From<RoleIdentNotFoundInProject> for ErrorModel {
    fn from(err: RoleIdentNotFoundInProject) -> Self {
        ErrorModel::builder()
            .r#type("RoleIdentNotFoundInProject")
            .code(StatusCode::NOT_FOUND.as_u16())
            .message(err.to_string())
            .stack(err.stack)
            .build()
    }
}

define_transparent_error! {
    pub enum GetRoleByIdentError,
    stack_message: "Error getting Role by ident from catalog",
    variants: [
        CatalogBackendError,
        RoleIdentNotFoundInProject,
    ]
}

// --------------------------- DELETE ERROR ---------------------------
define_transparent_error! {
    pub enum DeleteRoleError,
    stack_message: "Error deleting role in catalog",
    variants: [
        CatalogBackendError,
        RoleIdNotFoundInProject
    ]
}

// --------------------------- UPDATE ERROR ----------------------
define_transparent_error! {
    pub enum UpdateRoleError,
    stack_message: "Error updating role in catalog",
    variants: [
        CatalogBackendError,
        RoleSourceIdConflict,
        RoleNameAlreadyExists,
        RoleIdNotFoundInProject,
    ]
}

// --------------------------- LIST ERROR ---------------------------
define_transparent_error! {
    pub enum SearchRolesError,
    stack_message: "Error searching Roles catalog",
    variants: [
        CatalogBackendError
    ]
}

#[derive(Debug, PartialEq, typed_builder::TypedBuilder)]
pub struct CatalogListRolesByIdFilter<'a> {
    #[builder(default)]
    pub role_ids: Option<&'a [RoleId]>,
    #[builder(default)]
    pub source_ids: Option<&'a [&'a RoleSourceId]>,
    #[builder(default)]
    pub provider_ids: Option<&'a [&'a RoleProviderId]>,
}

/// Try to serve a `role_ids` list query entirely from cache.
///
/// Returns `Some(ListRolesResponse)` if every requested ID was found in cache.
/// Cached roles that do not match the `project_id`, `source_ids`, or `provider_ids`
/// filters are excluded from the result but do not cause a fallback to DB.
/// Returns `None` on any cache miss or when a continuation token is present or more
/// IDs are requested than fit on a single page.
async fn try_list_roles_from_cache(
    filter: &CatalogListRolesByIdFilter<'_>,
    pagination: &PaginationQuery,
    project_id: Option<&ArcProjectId>,
) -> Option<ListRolesResponse> {
    // Decompose filter fields explicitly so new fields are not accidentally overlooked.
    let CatalogListRolesByIdFilter {
        role_ids,
        source_ids,
        provider_ids,
    } = filter;

    let role_ids = (*role_ids)?;

    // Can't reconstruct a paginated result from cache.
    if matches!(pagination.page_token, PageToken::Present(_)) {
        return None;
    }
    if role_ids.len()
        > CONFIG
            .page_size_or_pagination_default(pagination.page_size)
            .try_into()
            .unwrap_or(usize::MAX)
    {
        return None;
    }

    // Build filter sets once for O(1) membership checks.
    let source_id_set: Option<HashSet<&RoleSourceId>> =
        source_ids.map(|sids| sids.iter().copied().collect());
    let provider_id_set: Option<HashSet<&RoleProviderId>> =
        provider_ids.map(|pids| pids.iter().copied().collect());

    // Fetch all IDs from cache in parallel; abort early on first None.
    let mut join_set = tokio::task::JoinSet::new();
    for &role_id in role_ids {
        join_set.spawn(role_cache_get_by_id(role_id));
    }

    let mut cached = Vec::with_capacity(role_ids.len());
    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(Some(role)) => cached.push(role),
            Ok(None) => {
                join_set.abort_all();
                return None;
            }
            Err(_) => {
                // JoinError shouldn't happen for these tasks
                join_set.abort_all();
                return None;
            }
        }
    }

    let mut roles = Vec::with_capacity(role_ids.len());
    for role in cached {
        // Apply filters: roles that don't match are simply excluded.
        if let Some(pid) = project_id
            && role.project_id != *pid
        {
            continue;
        }
        if let Some(ref sids) = source_id_set
            && !sids.contains(role.source_id())
        {
            continue;
        }
        if let Some(ref pids) = provider_id_set
            && !pids.contains(role.provider_id())
        {
            continue;
        }
        roles.push(role);
    }
    Some(ListRolesResponse {
        roles,
        next_page_token: None,
    })
}

#[async_trait::async_trait]
pub trait CatalogRoleOps
where
    Self: CatalogStore,
{
    async fn create_role<'a>(
        project_id: &ProjectId,
        role_to_create: CatalogCreateRoleRequest<'_>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<Arc<Role>, CreateRoleError> {
        let roles = Self::create_roles(project_id, vec![role_to_create], transaction).await?;
        let n_roles = roles.len();
        if n_roles != 1 {
            return Err(ResultCountMismatch::new(1, n_roles, "Create Role").into());
        }

        Ok(roles.into_iter().next().expect("length checked above"))
    }

    async fn create_roles<'a>(
        project_id: &ProjectId,
        roles_to_create: Vec<CatalogCreateRoleRequest<'_>>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<Vec<Arc<Role>>, CreateRoleError> {
        let roles = Self::create_roles_impl(project_id, roles_to_create, transaction)
            .await?
            .into_iter()
            .map(Arc::new)
            .collect::<Vec<_>>();
        Ok(roles)
    }

    async fn delete_role<'a>(
        project_id: &ProjectId,
        role_id: RoleId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<(), DeleteRoleError> {
        let deleted_roles =
            Self::delete_roles_impl(project_id, Some(&[role_id]), transaction).await?;
        if deleted_roles.is_empty() {
            Err(RoleIdNotFoundInProject::new(role_id, project_id.clone()).into())
        } else {
            Ok(())
        }
    }

    /// If description is None, the description must be removed.
    async fn update_role<'a>(
        project_id: &ProjectId,
        role_id: RoleId,
        role_name: &str,
        description: Option<&str>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<Arc<Role>, UpdateRoleError> {
        Self::update_role_impl(project_id, role_id, role_name, description, transaction)
            .await
            .map(Arc::new)
    }

    /// Update the external ID of the role.
    async fn set_role_source_system<'a>(
        project_id: &ProjectId,
        role_id: RoleId,
        request: &UpdateRoleSourceSystemRequest,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<Arc<Role>, UpdateRoleError> {
        Self::set_role_source_system_impl(project_id, role_id, request, transaction)
            .await
            .map(Arc::new)
    }

    async fn list_roles(
        project_id: ArcProjectId,
        filter: CatalogListRolesByIdFilter<'_>,
        pagination: PaginationQuery,
        catalog_state: Self::State,
    ) -> Result<ListRolesResponse, ListRolesError> {
        if let Some(cached) =
            try_list_roles_from_cache(&filter, &pagination, Some(&project_id)).await
        {
            return Ok(cached);
        }
        let populate_cache = filter.role_ids.is_some();
        let result =
            Self::list_roles_impl(Some(&*project_id), filter, pagination, catalog_state).await?;
        if populate_cache {
            for role in &result.roles {
                role_cache_insert(role.clone()).await;
            }
        }
        Ok(result)
    }

    async fn list_roles_across_projects(
        filter: CatalogListRolesByIdFilter<'_>,
        pagination: PaginationQuery,
        catalog_state: Self::State,
    ) -> Result<ListRolesResponse, ListRolesError> {
        if let Some(cached) = try_list_roles_from_cache(&filter, &pagination, None).await {
            return Ok(cached);
        }
        let populate_cache = filter.role_ids.is_some();
        let result = Self::list_roles_impl(None, filter, pagination, catalog_state).await?;
        if populate_cache {
            for role in &result.roles {
                role_cache_insert(role.clone()).await;
            }
        }
        Ok(result)
    }

    async fn get_role_by_id_across_projects(
        role_id: RoleId,
        catalog_state: Self::State,
    ) -> Result<Arc<Role>, GetRoleAcrossProjectsError> {
        if let Some(role) = role_cache_get_by_id(role_id).await {
            return Ok(role);
        }
        let roles = Self::list_roles_impl(
            None,
            CatalogListRolesByIdFilter::builder()
                .role_ids(Some(&[role_id]))
                .build(),
            PaginationQuery::new_with_page_size(1),
            catalog_state,
        )
        .await?;
        let role = roles
            .roles
            .into_iter()
            .next()
            .ok_or_else(|| RoleIdNotFound::new(role_id))?;
        role_cache_insert(role.clone()).await;
        Ok(role)
    }

    async fn get_role_by_id(
        project_id: &ProjectId,
        role_id: RoleId,
        catalog_state: Self::State,
    ) -> Result<Arc<Role>, GetRoleInProjectError> {
        if let Some(role) = role_cache_get_by_id(role_id).await {
            // Verify the cached role belongs to the requested project
            if role.project_id.as_ref() == project_id {
                return Ok(role);
            }
            // Cache hit for wrong project - treat as cache miss
        }
        let roles = Self::list_roles_impl(
            Some(project_id),
            CatalogListRolesByIdFilter::builder()
                .role_ids(Some(&[role_id]))
                .build(),
            PaginationQuery::new_with_page_size(1),
            catalog_state,
        )
        .await?;
        let role = roles
            .roles
            .into_iter()
            .next()
            .ok_or_else(|| RoleIdNotFoundInProject::new(role_id, project_id.clone()))?;
        role_cache_insert(role.clone()).await;
        Ok(role)
    }

    async fn search_role(
        project_id: &ProjectId,
        search_term: &str,
        catalog_state: Self::State,
    ) -> Result<SearchRoleResponse, SearchRolesError> {
        Self::search_role_impl(project_id, search_term, catalog_state).await
    }

    /// Returns all roles in `project_id` whose `(provider_id, source_id)` matches one of the
    /// provided idents. No pagination — returns all matches at once.
    async fn list_roles_by_idents(
        project_id: &ProjectId,
        idents: &[&RoleIdent],
        catalog_state: Self::State,
    ) -> Result<Vec<Arc<Role>>, CatalogBackendError> {
        Ok(
            Self::list_roles_by_idents_impl(project_id, idents, catalog_state)
                .await?
                .into_iter()
                .map(Arc::new)
                .collect(),
        )
    }

    /// Returns the single role in `project_id` with the given `ident`, or an error if not found.
    async fn get_role_by_ident(
        arc_project_id: ArcProjectId,
        arc_ident: ArcRoleIdent,
        catalog_state: Self::State,
    ) -> Result<Arc<Role>, GetRoleByIdentError> {
        if let Some(role) = role_cache_get_by_ident(arc_project_id.clone(), arc_ident.clone()).await
        {
            // Verify the cached role belongs to the requested project
            if role.project_id == arc_project_id {
                return Ok(role);
            }
            // Cache hit for wrong project - treat as cache miss
        }
        let roles =
            Self::list_roles_by_idents_impl(&arc_project_id, &[&*arc_ident], catalog_state).await?;
        let role = roles
            .into_iter()
            .next()
            .map(Arc::new)
            .ok_or_else(|| RoleIdentNotFoundInProject::new(arc_ident, arc_project_id))?;
        role_cache_insert(role.clone()).await;
        Ok(role)
    }

    async fn get_role_by_id_cache_aware(
        project_id: &ProjectId,
        role_id: RoleId,
        cache_policy: CachePolicy,
        catalog_state: Self::State,
    ) -> Result<Arc<Role>, GetRoleInProjectError> {
        match cache_policy {
            CachePolicy::Use => Self::get_role_by_id(project_id, role_id, catalog_state).await,
            CachePolicy::Skip => {
                let roles = Self::list_roles_impl(
                    Some(project_id),
                    CatalogListRolesByIdFilter::builder()
                        .role_ids(Some(&[role_id]))
                        .build(),
                    PaginationQuery::new_with_page_size(1),
                    catalog_state,
                )
                .await?;
                let role = roles
                    .roles
                    .into_iter()
                    .next()
                    .ok_or_else(|| RoleIdNotFoundInProject::new(role_id, project_id.clone()))?;
                role_cache_insert(role.clone()).await;
                Ok(role)
            }
            CachePolicy::RequireMinimumVersion(min_version) => {
                if let Some(role) = role_cache_get_by_id(role_id).await
                    && role.project_id.as_ref() == project_id
                    && *role.version >= min_version
                {
                    return Ok(role);
                }
                let roles = Self::list_roles_impl(
                    Some(project_id),
                    CatalogListRolesByIdFilter::builder()
                        .role_ids(Some(&[role_id]))
                        .build(),
                    PaginationQuery::new_with_page_size(1),
                    catalog_state,
                )
                .await?;
                let role = roles
                    .roles
                    .into_iter()
                    .next()
                    .ok_or_else(|| RoleIdNotFoundInProject::new(role_id, project_id.clone()))?;
                role_cache_insert(role.clone()).await;
                Ok(role)
            }
        }
    }

    async fn get_role_by_id_across_projects_cache_aware(
        role_id: RoleId,
        cache_policy: CachePolicy,
        catalog_state: Self::State,
    ) -> Result<Arc<Role>, GetRoleAcrossProjectsError> {
        match cache_policy {
            CachePolicy::Use => Self::get_role_by_id_across_projects(role_id, catalog_state).await,
            CachePolicy::Skip => {
                let roles = Self::list_roles_impl(
                    None,
                    CatalogListRolesByIdFilter::builder()
                        .role_ids(Some(&[role_id]))
                        .build(),
                    PaginationQuery::new_with_page_size(1),
                    catalog_state,
                )
                .await?;
                let role = roles
                    .roles
                    .into_iter()
                    .next()
                    .ok_or_else(|| RoleIdNotFound::new(role_id))?;
                role_cache_insert(role.clone()).await;
                Ok(role)
            }
            CachePolicy::RequireMinimumVersion(min_version) => {
                if let Some(role) = role_cache_get_by_id(role_id).await
                    && *role.version >= min_version
                {
                    return Ok(role);
                }
                let roles = Self::list_roles_impl(
                    None,
                    CatalogListRolesByIdFilter::builder()
                        .role_ids(Some(&[role_id]))
                        .build(),
                    PaginationQuery::new_with_page_size(1),
                    catalog_state,
                )
                .await?;
                let role = roles
                    .roles
                    .into_iter()
                    .next()
                    .ok_or_else(|| RoleIdNotFound::new(role_id))?;
                role_cache_insert(role.clone()).await;
                Ok(role)
            }
        }
    }

    async fn get_role_by_ident_cache_aware(
        project_id: ArcProjectId,
        ident: ArcRoleIdent,
        cache_policy: CachePolicy,
        catalog_state: Self::State,
    ) -> Result<Arc<Role>, GetRoleByIdentError> {
        match cache_policy {
            CachePolicy::Use => Self::get_role_by_ident(project_id, ident, catalog_state).await,
            CachePolicy::Skip => {
                let roles =
                    Self::list_roles_by_idents_impl(&project_id, &[&ident], catalog_state).await?;
                let role = roles
                    .into_iter()
                    .next()
                    .map(Arc::new)
                    .ok_or_else(|| RoleIdentNotFoundInProject::new(ident, project_id))?;
                role_cache_insert(role.clone()).await;
                Ok(role)
            }
            CachePolicy::RequireMinimumVersion(min_version) => {
                if let Some(role) = role_cache_get_by_ident(project_id.clone(), ident.clone()).await
                    && role.project_id == project_id
                    && *role.version >= min_version
                {
                    return Ok(role);
                }
                let roles =
                    Self::list_roles_by_idents_impl(&project_id, &[&ident], catalog_state).await?;
                let role = roles
                    .into_iter()
                    .next()
                    .map(Arc::new)
                    .ok_or_else(|| RoleIdentNotFoundInProject::new(ident, project_id))?;
                role_cache_insert(role.clone()).await;
                Ok(role)
            }
        }
    }
}

impl<T> CatalogRoleOps for T where T: CatalogStore {}

// --------------------------- AuthorizationFailureSource implementations ---------------------------
use crate::service::events::impl_authorization_failure_source;

impl_authorization_failure_source!(CreateRoleError => InternalCatalogError);
impl_authorization_failure_source!(ListRolesError => InternalCatalogError);
impl_authorization_failure_source!(GetRoleAcrossProjectsError => InternalCatalogError);
impl_authorization_failure_source!(GetRoleByIdentError => InternalCatalogError);
impl_authorization_failure_source!(DeleteRoleError => InternalCatalogError);
impl_authorization_failure_source!(UpdateRoleError => InternalCatalogError);
impl_authorization_failure_source!(SearchRolesError => InternalCatalogError);
