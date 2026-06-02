//! Role-assignment management API: assign, revoke, and list `(user, role)`
//! grants.
//!
//! Design invariants:
//!
//! - **Authorizer-agnostic dispatch.** Every operation routes through the active
//!   authorizer, which is the single source of truth. `manages_role_assignments()`
//!   selects the backend: when it returns `true` (OpenFGA) assignments are stored
//!   as relationship tuples; otherwise (Cedar / `AllowAll`) they are rows in the
//!   `role_assignment` table. There is **no dual-write** — exactly one store is
//!   touched per request.
//! - **Catalog-managed only.** Only roles whose provider is `lakekeeper` or
//!   `system` may be mutated here; externally-provided roles (LDAP/OIDC/SCIM) are
//!   owned by their sync and rejected with `409 RoleNotManuallyAssignable`.
//! - **User must exist.** Assigning to an unknown user returns
//!   `404 RoleAssignmentUserNotFound`; this endpoint never creates users. Revoke
//!   skips the check (it is idempotent).
//! - **Idempotent** assign and revoke; **`INSTANCE_ADMIN`s bypass** the authz
//!   checks (control-plane provisioning).

use chrono::{DateTime, Utc};
use http::StatusCode;
use iceberg_ext::catalog::rest::ErrorModel;
use serde::{Deserialize, Serialize};

use crate::{
    api::{
        ApiContext,
        iceberg::{types::PageToken, v1::PaginationQuery},
    },
    request_metadata::RequestMetadata,
    service::{
        CatalogRoleAssignmentOps, CatalogRoleOps, CatalogStore, ListRoleAssignmentsResultPage,
        ProjectId, RoleAssignmentFilter, RoleAssignmentRow, RoleId, RoleProviderId, SecretStore,
        State, Transaction,
        authn::UserId,
        authz::{AuthZRoleOps, AuthZUserOps, Authorizer},
        role_assignments_cache,
    },
};

// ============================================================================
// DTOs
// ============================================================================

/// Request body for assigning a role to a user.
#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct CreateRoleAssignmentRequest {
    /// The user that should be granted the role.
    #[cfg_attr(feature = "open-api", schema(value_type = String))]
    pub user_id: UserId,
    /// The role that should be granted to the user.
    #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
    pub role_id: RoleId,
}

/// A single `(user_id, role_id)` assignment.
#[derive(Debug, Clone, Serialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct RoleAssignmentResponse {
    /// The user that holds the role.
    #[cfg_attr(feature = "open-api", schema(value_type = String))]
    pub user_id: UserId,
    /// The role held by the user.
    #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
    pub role_id: RoleId,
    /// When the assignment was created. `None` when the authorizer (e.g.
    /// OpenFGA) is the source of truth and does not track a timestamp.
    pub created_at: Option<DateTime<Utc>>,
}

impl From<RoleAssignmentRow> for RoleAssignmentResponse {
    fn from(row: RoleAssignmentRow) -> Self {
        Self {
            user_id: row.user_id,
            role_id: row.role_id,
            created_at: row.created_at,
        }
    }
}

/// Query parameters for listing role assignments. Exactly one of `user_id` or
/// `role_id` must be provided.
#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::IntoParams))]
#[serde(rename_all = "camelCase")]
pub struct ListRoleAssignmentsQuery {
    /// List all assignments held by this user.
    #[serde(default)]
    #[cfg_attr(feature = "open-api", param(value_type = Option<String>))]
    pub user_id: Option<UserId>,
    /// List all assignments to this role.
    #[serde(default)]
    #[cfg_attr(feature = "open-api", param(value_type = Option<uuid::Uuid>))]
    pub role_id: Option<RoleId>,
    /// Next page token.
    #[serde(default)]
    pub page_token: Option<String>,
    /// Signals an upper bound of the number of results a client will receive.
    #[serde(default)]
    pub page_size: Option<i64>,
}

impl ListRoleAssignmentsQuery {
    #[must_use]
    pub fn pagination_query(&self) -> PaginationQuery {
        PaginationQuery {
            page_token: self
                .page_token
                .clone()
                .map_or(PageToken::Empty, PageToken::Present),
            page_size: self.page_size,
        }
    }
}

/// Response body for listing role assignments.
#[derive(Debug, Clone, Serialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct ListRoleAssignmentsResponse {
    pub assignments: Vec<RoleAssignmentResponse>,
    #[serde(alias = "next_page_token")]
    pub next_page_token: Option<String>,
}

impl From<ListRoleAssignmentsResultPage> for ListRoleAssignmentsResponse {
    fn from(page: ListRoleAssignmentsResultPage) -> Self {
        Self {
            assignments: page.assignments.into_iter().map(Into::into).collect(),
            next_page_token: page.next_page_token,
        }
    }
}

// ============================================================================
// Role-assignment errors
// ============================================================================

/// Raised when a role-assignment mutation targets a role that is not
/// catalog-managed. Only roles whose provider is `lakekeeper` (the default) or
/// `system` may be assigned/revoked manually through this endpoint; roles owned
/// by any other provider (LDAP, OIDC, SCIM, …) are the authoritative
/// responsibility of that provider's sync and must not be mutated here.
#[derive(thiserror::Error, Debug)]
#[error(
    "role {role_id} is owned by provider '{provider_id}' and cannot be assigned \
     via this endpoint; only catalog-managed (lakekeeper/system) roles are \
     manually assignable."
)]
pub struct RoleNotManuallyAssignable {
    pub role_id: RoleId,
    pub provider_id: RoleProviderId,
}

impl From<RoleNotManuallyAssignable> for ErrorModel {
    fn from(err: RoleNotManuallyAssignable) -> Self {
        let message = err.to_string();
        ErrorModel::builder()
            .r#type("RoleNotManuallyAssignable")
            .code(StatusCode::CONFLICT.as_u16())
            .message(message)
            .build()
    }
}

/// Raised when a role assignment targets a user that does not yet exist.
/// Users must be provisioned (e.g. via the user management API or token
/// login) before roles can be assigned to them.
#[derive(thiserror::Error, Debug)]
#[error("user {user_id} does not exist; provision the user before assigning roles.")]
pub struct RoleAssignmentUserNotFound {
    pub user_id: UserId,
}

impl From<RoleAssignmentUserNotFound> for ErrorModel {
    fn from(err: RoleAssignmentUserNotFound) -> Self {
        let message = err.to_string();
        ErrorModel::builder()
            .r#type("RoleAssignmentUserNotFound")
            .code(StatusCode::NOT_FOUND.as_u16())
            .message(message)
            .build()
    }
}

// ============================================================================
// Authz-result error
// ============================================================================

// Single error type threaded through the `Result` handed to `emit_authz`.
// `emit_authz` accepts any `E: AuthorizationFailureSource`; this wraps the
// role-action / user-action authz failures (so the audit log records the exact
// reason), the managed-provider 409, and any catalog / authorizer-backend error
// folded into an `ErrorModel`.
#[derive(Debug)]
enum RoleAssignmentAuthZError {
    RequireRole(crate::service::authz::RequireRoleActionError),
    RequireUser(crate::service::authz::RequireUserActionError),
    NotManuallyAssignable(RoleNotManuallyAssignable),
    UserNotFound(RoleAssignmentUserNotFound),
    /// Catalog-backend / authorizer-backend failure already shaped into an
    /// `ErrorModel`. Recorded as an internal catalog failure in the audit log.
    Catalog(ErrorModel),
}

impl From<crate::service::authz::RequireRoleActionError> for RoleAssignmentAuthZError {
    fn from(e: crate::service::authz::RequireRoleActionError) -> Self {
        Self::RequireRole(e)
    }
}

impl From<crate::service::authz::RequireUserActionError> for RoleAssignmentAuthZError {
    fn from(e: crate::service::authz::RequireUserActionError) -> Self {
        Self::RequireUser(e)
    }
}

impl From<RoleNotManuallyAssignable> for RoleAssignmentAuthZError {
    fn from(e: RoleNotManuallyAssignable) -> Self {
        Self::NotManuallyAssignable(e)
    }
}

impl From<RoleAssignmentUserNotFound> for RoleAssignmentAuthZError {
    fn from(e: RoleAssignmentUserNotFound) -> Self {
        Self::UserNotFound(e)
    }
}

impl crate::service::events::AuthorizationFailureSource for RoleAssignmentAuthZError {
    fn to_failure_reason(&self) -> crate::service::events::AuthorizationFailureReason {
        use crate::service::events::AuthorizationFailureReason;
        match self {
            Self::RequireRole(e) => e.to_failure_reason(),
            Self::RequireUser(e) => e.to_failure_reason(),
            Self::NotManuallyAssignable(_) | Self::UserNotFound(_) => {
                AuthorizationFailureReason::InvalidRequestData
            }
            Self::Catalog(_) => AuthorizationFailureReason::InternalCatalogError,
        }
    }

    fn into_error_model(self) -> ErrorModel {
        match self {
            Self::RequireRole(e) => e.into_error_model(),
            Self::RequireUser(e) => e.into_error_model(),
            Self::NotManuallyAssignable(e) => e.into(),
            Self::UserNotFound(e) => e.into(),
            Self::Catalog(e) => e,
        }
    }
}

// ============================================================================
// Service
// ============================================================================

use crate::api::management::v1::ApiServer;

impl<C: CatalogStore, A: Authorizer + Clone, S: SecretStore> Service<C, A, S>
    for ApiServer<C, A, S>
{
}

#[async_trait::async_trait]
pub trait Service<C: CatalogStore, A: Authorizer, S: SecretStore> {
    /// Assign a role to a user. Idempotent.
    async fn create_role_assignment(
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        request: CreateRoleAssignmentRequest,
    ) -> crate::api::Result<RoleAssignmentResponse> {
        let CreateRoleAssignmentRequest { user_id, role_id } = request;
        let project_id = request_metadata.require_project_id(None)?;

        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;

        let event_ctx = crate::service::events::APIEventContext::for_role(
            request_metadata.into(),
            context.v1_state.events.clone(),
            role_id,
            crate::service::authz::CatalogRoleAction::ManageRoleAssignments,
        );

        let metadata = event_ctx.request_metadata();
        let authz_result: Result<RoleAssignmentResponse, RoleAssignmentAuthZError> = async {
            let role = C::get_role_by_id_cache_aware(
                &project_id,
                role_id,
                crate::service::CachePolicy::Skip,
                catalog_state.clone(),
            )
            .await;
            let role = authorizer
                .require_role_action(
                    metadata,
                    role,
                    crate::service::authz::CatalogRoleAction::ManageRoleAssignments,
                )
                .await?;

            // Allowlist: only catalog-managed roles (lakekeeper / system) may be
            // assigned manually. Roles owned by any other provider are synced.
            if !(role.ident.is_lakekeeper() || role.ident.is_system()) {
                return Err(RoleNotManuallyAssignable {
                    role_id,
                    provider_id: role.ident.provider_id().clone(),
                }
                .into());
            }

            // The target user must already exist (provision-then-assign).
            let users = C::list_user(
                Some(vec![user_id.clone()]),
                None,
                PaginationQuery {
                    page_token: PageToken::Empty,
                    page_size: Some(1),
                },
                catalog_state.clone(),
            )
            .await
            .map_err(into_authz)?;
            if users.users.is_empty() {
                return Err(RoleAssignmentUserNotFound {
                    user_id: user_id.clone(),
                }
                .into());
            }

            let manages = authorizer.manages_role_assignments();
            let pairs = vec![(user_id.clone(), role_id)];

            let created_at = if manages {
                authorizer
                    .add_role_assignments(metadata, project_id.clone(), &pairs)
                    .await
                    .map_err(into_authz)?;
                None
            } else {
                let mut t = C::Transaction::begin_write(catalog_state.clone())
                    .await
                    .map_err(into_authz)?;
                let rows = C::add_role_assignments(&project_id, &pairs, t.transaction())
                    .await
                    .map_err(into_authz)?;
                t.commit().await.map_err(into_authz)?;
                role_assignments_cache::user_assignments_cache_invalidate(&user_id).await;
                role_assignments_cache::role_members_cache_invalidate(role_id).await;
                rows.into_iter().next().and_then(|r| r.created_at)
            };

            Ok(RoleAssignmentResponse {
                user_id,
                role_id,
                created_at,
            })
        }
        .await;

        let (_event_ctx, response) = event_ctx.emit_authz(authz_result)?;
        Ok(response)
    }

    /// Revoke a role from a user. Idempotent: revoking an absent assignment is
    /// a no-op.
    async fn delete_role_assignment(
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        user_id: UserId,
        role_id: RoleId,
    ) -> crate::api::Result<()> {
        let project_id = request_metadata.require_project_id(None)?;

        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;

        let event_ctx = crate::service::events::APIEventContext::for_role(
            request_metadata.into(),
            context.v1_state.events.clone(),
            role_id,
            crate::service::authz::CatalogRoleAction::ManageRoleAssignments,
        );

        let metadata = event_ctx.request_metadata();
        let authz_result: Result<(), RoleAssignmentAuthZError> = async {
            let role = C::get_role_by_id_cache_aware(
                &project_id,
                role_id,
                crate::service::CachePolicy::Skip,
                catalog_state.clone(),
            )
            .await;
            let role = authorizer
                .require_role_action(
                    metadata,
                    role,
                    crate::service::authz::CatalogRoleAction::ManageRoleAssignments,
                )
                .await?;

            // Allowlist: only catalog-managed roles (lakekeeper / system) may be
            // revoked manually. Roles owned by any other provider are synced.
            if !(role.ident.is_lakekeeper() || role.ident.is_system()) {
                return Err(RoleNotManuallyAssignable {
                    role_id,
                    provider_id: role.ident.provider_id().clone(),
                }
                .into());
            }

            // No user-exists check: DELETE is idempotent (unknown user / absent
            // assignment → 204).
            let manages = authorizer.manages_role_assignments();
            let pairs = vec![(user_id.clone(), role_id)];

            if manages {
                authorizer
                    .remove_role_assignments(metadata, project_id.clone(), &pairs)
                    .await
                    .map_err(into_authz)?;
            } else {
                let mut t = C::Transaction::begin_write(catalog_state.clone())
                    .await
                    .map_err(into_authz)?;
                C::remove_role_assignments(&pairs, t.transaction())
                    .await
                    .map_err(into_authz)?;
                t.commit().await.map_err(into_authz)?;
                role_assignments_cache::user_assignments_cache_invalidate(&user_id).await;
                role_assignments_cache::role_members_cache_invalidate(role_id).await;
            }

            Ok(())
        }
        .await;

        let (_event_ctx, ()) = event_ctx.emit_authz(authz_result)?;
        Ok(())
    }

    /// List role assignments filtered by exactly one of `user_id` or `role_id`.
    async fn list_role_assignments(
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        query: ListRoleAssignmentsQuery,
    ) -> crate::api::Result<ListRoleAssignmentsResponse> {
        let project_id = request_metadata.require_project_id(None)?;
        let pagination = query.pagination_query();

        let filter = match (query.user_id.clone(), query.role_id) {
            (Some(user_id), None) => RoleAssignmentFilter::ByUser(user_id),
            (None, Some(role_id)) => RoleAssignmentFilter::ByRole(role_id),
            _ => {
                return Err(ErrorModel::bad_request(
                    "Exactly one of `userId` or `roleId` must be provided.",
                    "RoleAssignmentFilterRequired",
                    None,
                )
                .into());
            }
        };

        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;

        match filter {
            RoleAssignmentFilter::ByRole(role_id) => {
                let event_ctx = crate::service::events::APIEventContext::for_role(
                    request_metadata.into(),
                    context.v1_state.events.clone(),
                    role_id,
                    crate::service::authz::CatalogRoleAction::ReadRoleAssignments,
                );
                let metadata = event_ctx.request_metadata();
                let authz_result: Result<ListRoleAssignmentsResultPage, RoleAssignmentAuthZError> =
                    async {
                        let role = C::get_role_by_id_cache_aware(
                            &project_id,
                            role_id,
                            crate::service::CachePolicy::Skip,
                            catalog_state.clone(),
                        )
                        .await;
                        authorizer
                            .require_role_action(
                                metadata,
                                role,
                                crate::service::authz::CatalogRoleAction::ReadRoleAssignments,
                            )
                            .await?;
                        list_assignments::<C, A>(
                            &authorizer,
                            metadata,
                            &project_id,
                            RoleAssignmentFilter::ByRole(role_id),
                            pagination,
                            catalog_state.clone(),
                        )
                        .await
                    }
                    .await;
                let (_event_ctx, page) = event_ctx.emit_authz(authz_result)?;
                Ok(page.into())
            }
            RoleAssignmentFilter::ByUser(user_id) => {
                let event_ctx = crate::service::events::APIEventContext::for_user(
                    request_metadata.into(),
                    context.v1_state.events.clone(),
                    std::sync::Arc::new(user_id.clone()),
                    crate::service::authz::CatalogUserAction::ReadRoleAssignments,
                );
                let metadata = event_ctx.request_metadata();
                let authz_result: Result<ListRoleAssignmentsResultPage, RoleAssignmentAuthZError> =
                    async {
                        authorizer
                            .require_user_action(
                                metadata,
                                &user_id,
                                crate::service::authz::CatalogUserAction::ReadRoleAssignments,
                            )
                            .await?;
                        list_assignments::<C, A>(
                            &authorizer,
                            metadata,
                            &project_id,
                            RoleAssignmentFilter::ByUser(user_id.clone()),
                            pagination,
                            catalog_state.clone(),
                        )
                        .await
                    }
                    .await;
                let (_event_ctx, page) = event_ctx.emit_authz(authz_result)?;
                Ok(page.into())
            }
        }
    }
}

async fn list_assignments<C: CatalogStore, A: Authorizer>(
    authorizer: &A,
    metadata: &RequestMetadata,
    project_id: &ProjectId,
    filter: RoleAssignmentFilter,
    pagination: PaginationQuery,
    catalog_state: C::State,
) -> Result<ListRoleAssignmentsResultPage, RoleAssignmentAuthZError> {
    if authorizer.manages_role_assignments() {
        Ok(authorizer
            .list_role_assignments(
                metadata,
                std::sync::Arc::new(project_id.clone()),
                filter,
                pagination,
            )
            .await
            .map_err(into_authz)?)
    } else {
        let mut t = C::Transaction::begin_read(catalog_state)
            .await
            .map_err(into_authz)?;
        let page = C::list_role_assignments(project_id, filter, pagination, t.transaction())
            .await
            .map_err(into_authz)?;
        t.commit().await.map_err(into_authz)?;
        Ok(page)
    }
}

// Helper: any error convertible into `ErrorModel` (catalog errors, authorizer
// hook `IcebergErrorResponse`, `begin_write`/`commit` failures) → folded into
// the `Catalog` variant so `emit_authz` records the failure with its original
// type/code.
fn into_authz<E>(e: E) -> RoleAssignmentAuthZError
where
    E: Into<ErrorModel>,
{
    RoleAssignmentAuthZError::Catalog(e.into())
}
