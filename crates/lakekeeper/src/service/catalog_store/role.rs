use std::sync::Arc;

use http::StatusCode;
use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};

use crate::{
    ProjectId,
    api::{
        iceberg::v1::PaginationQuery,
        management::v1::role::{
            ListRolesResponse, Role, SearchRoleResponse, UpdateRoleSourceSystemRequest,
        },
    },
    service::{
        CatalogBackendError, CatalogCreateRoleRequest, CatalogStore, InvalidPaginationToken,
        ProjectIdNotFoundError, ResultCountMismatch, RoleId, Transaction, define_transparent_error,
        impl_error_stack_methods, impl_from_with_detail,
    },
};

#[derive(thiserror::Error, Debug, PartialEq)]
#[error("A role with id '{role_id}' does not exist in project with id '{project_id}'")]
pub struct RoleIdNotFound {
    pub role_id: RoleId,
    pub project_id: ProjectId,
    pub stack: Vec<String>,
}
impl RoleIdNotFound {
    #[must_use]
    pub fn new(role_id: RoleId, project_id: ProjectId) -> Self {
        Self {
            role_id,
            project_id,
            stack: Vec::new(),
        }
    }
}
impl_error_stack_methods!(RoleIdNotFound);

impl From<RoleIdNotFound> for ErrorModel {
    fn from(err: RoleIdNotFound) -> Self {
        ErrorModel {
            r#type: "RoleNotFound".to_string(),
            code: StatusCode::NOT_FOUND.as_u16(),
            message: err.to_string(),
            stack: err.stack,
            source: None,
        }
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
        ErrorModel {
            r#type: "RoleNameAlreadyExists".to_string(),
            code: StatusCode::CONFLICT.as_u16(),
            message: err.to_string(),
            stack: err.stack,
            source: None,
        }
    }
}

#[derive(thiserror::Error, PartialEq, Debug, Default)]
#[error("A role with the specified combination of (project_id, source_id) already exists")]
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
        ErrorModel {
            r#type: "RoleSourceIdConflict".to_string(),
            code: StatusCode::CONFLICT.as_u16(),
            message: err.to_string(),
            stack: err.stack,
            source: None,
        }
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
    pub enum GetRoleError,
    stack_message: "Error getting Role from catalog",
    variants: [
        CatalogBackendError,
        InvalidPaginationToken,
        RoleIdNotFound,
    ]
}

impl From<ListRolesError> for GetRoleError {
    fn from(err: ListRolesError) -> Self {
        match err {
            ListRolesError::CatalogBackendError(e) => e.into(),
            ListRolesError::InvalidPaginationToken(e) => e.into(),
        }
    }
}

// --------------------------- DELETE ERROR ---------------------------
define_transparent_error! {
    pub enum DeleteRoleError,
    stack_message: "Error deleting role in catalog",
    variants: [
        CatalogBackendError,
        RoleIdNotFound
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
        RoleIdNotFound,
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
pub struct CatalogListRolesFilter<'a> {
    #[builder(default)]
    pub role_ids: Option<&'a [RoleId]>,
    #[builder(default)]
    pub source_ids: Option<&'a [&'a str]>,
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
            Self::delete_roles_impl(project_id, Some(&[role_id]), None, transaction).await?;
        if deleted_roles.is_empty() {
            Err(RoleIdNotFound::new(role_id, project_id.clone()).into())
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
        project_id: &ProjectId,
        filter: CatalogListRolesFilter<'_>,
        pagination: PaginationQuery,
        catalog_state: Self::State,
    ) -> Result<ListRolesResponse, ListRolesError> {
        Self::list_roles_impl(project_id, filter, pagination, catalog_state).await
    }

    async fn get_role_by_id(
        project_id: &ProjectId,
        role_id: RoleId,
        catalog_state: Self::State,
    ) -> Result<Arc<Role>, GetRoleError> {
        let roles = Self::list_roles(
            project_id,
            CatalogListRolesFilter::builder()
                .role_ids(Some(&[role_id]))
                .build(),
            PaginationQuery::new_with_page_size(1),
            catalog_state,
        )
        .await?;

        if let Some(role) = roles.roles.into_iter().next() {
            Ok(role)
        } else {
            Err(RoleIdNotFound::new(role_id, project_id.clone()).into())
        }
    }

    async fn search_role(
        project_id: &ProjectId,
        search_term: &str,
        catalog_state: Self::State,
    ) -> Result<SearchRoleResponse, SearchRolesError> {
        Self::search_role_impl(project_id, search_term, catalog_state).await
    }
}

impl<T> CatalogRoleOps for T where T: CatalogStore {}
