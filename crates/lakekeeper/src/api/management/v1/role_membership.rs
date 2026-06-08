//! Role-membership management API: the direct `/role/{id}/members`,
//! `/role/{id}/member-of` and `/user/{id}/roles` surface.
//!
//! A role's members are **polymorphic** — a member is either a user (a direct
//! user→role assignment) or another role (a role→role membership edge). Both are
//! surfaced through one `/members` collection discriminated by [`RoleMemberType`].
//!
//! Write dispatch is **mutually exclusive / single-source-of-truth**: when the
//! authorizer manages assignments (e.g. OpenFGA, via
//! [`Authorizer::role_assignments`]) the edges live in the authorizer's store;
//! otherwise (Cedar/AllowAll) they live in the catalog tables. Reads currently
//! implement the catalog arm only — under an assignment-managing authorizer they
//! return an explicit `NotImplemented` until the authorizer-backed listing lands
//! (see the transitive-listing task). The hot authz path is untouched; all of
//! this is cold-path management code.
//!
//! Error/emit shape follows `user.rs::get_user`: emit the authorization decision,
//! then run business logic with `?`.

use axum::{Json, response::IntoResponse};
use http::StatusCode;
use iceberg_ext::catalog::rest::ErrorModel;
use serde::{Deserialize, Serialize};

use crate::{
    api::{
        ApiContext,
        iceberg::{types::PageToken, v1::PaginationQuery},
        management::v1::ApiServer,
    },
    request_metadata::RequestMetadata,
    service::{
        ArcRoleIdent, CachePolicy, CatalogRoleAssignmentOps, CatalogRoleOps, CatalogStore, Result,
        RoleId, RoleMemberKind, RoleMembershipEntry, SecretStore, State, UserId,
        authz::{
            AuthZRoleOps, AuthZUserOps, Authorizer, CatalogRoleAction, CatalogUserAction,
            RoleAssignmentRow, UserOrRoleId,
        },
        events::{APIEventContext, AuthorizationFailureSource},
    },
};

// ─── DTOs ───────────────────────────────────────────────────────────────────

/// Kind of a role member. Serializes as `"user"` / `"role"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum RoleMemberType {
    User,
    Role,
}

impl From<RoleMemberType> for RoleMemberKind {
    fn from(t: RoleMemberType) -> Self {
        match t {
            RoleMemberType::User => RoleMemberKind::User,
            RoleMemberType::Role => RoleMemberKind::Role,
        }
    }
}

/// A member of a role on responses: a user or another role (`type` + opaque `id`),
/// with the membership-edge timestamp. The request twin is [`RoleMemberRef`].
///
/// `created_at` is `None` when no timestamp is available — on the add response
/// (which only confirms membership) and under authorizer backends that do not
/// record an edge timestamp; it is populated by the `GET /role/{id}/members`
/// listing.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct RoleMember {
    pub r#type: RoleMemberType,
    pub id: String,
    pub created_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl From<UserOrRoleId> for RoleMember {
    fn from(subject: UserOrRoleId) -> Self {
        let (r#type, id) = match subject {
            UserOrRoleId::User(user_id) => (RoleMemberType::User, user_id.to_string()),
            UserOrRoleId::Role(role_id) => (RoleMemberType::Role, role_id.to_string()),
        };
        RoleMember {
            r#type,
            id,
            created_at: None,
        }
    }
}

impl From<RoleAssignmentRow> for RoleMember {
    fn from(row: RoleAssignmentRow) -> Self {
        // Identity from the subject; overlay the membership-edge timestamp the
        // catalog listing carries (`None` under a timestamp-less authorizer).
        RoleMember {
            created_at: row.created_at,
            ..RoleMember::from(row.subject)
        }
    }
}

/// A member to add: a user or another role, by identity (`type` + opaque `id`).
/// The request twin of [`RoleMember`] — no timestamp, since the membership edge's
/// `created_at` is server-assigned and read-only, so it lives only on responses.
#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct RoleMemberRef {
    pub r#type: RoleMemberType,
    pub id: String,
}

/// Request body for `POST /role/{id}/members`. Batch: adds every listed member to
/// the role atomically (all-or-nothing).
#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct AddRoleMembersRequest {
    pub members: Vec<RoleMemberRef>,
}

/// Response for `POST /role/{id}/members`: the requested members, confirmed
/// present after the add (idempotent — already-present members are included).
/// Each member's `created_at` is `None` (membership confirmation only) — use
/// `GET /role/{id}/members` to retrieve membership timestamps.
#[derive(Debug, Clone, Serialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct AddRoleMembersResponse {
    pub members: Vec<RoleMember>,
}

/// One page of a role's direct members (users ∪ member roles).
#[derive(Debug, Clone, Serialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct ListRoleMembersResponse {
    pub members: Vec<RoleMember>,
    #[serde(alias = "next_page_token")]
    pub next_page_token: Option<String>,
}

/// A role reached through a membership edge — used by `member-of` and
/// `user/{id}/roles`. `id`, `ident`, and `name` are always present; `created_at`
/// (the membership-edge timestamp) is `None` under authorizer backends that do not
/// record one.
//
// Internal rationale (not part of the API contract): identity is always present
// because a reader that walks an authorizer's membership graph must drop entries
// whose role id no longer resolves in the catalog (an orphaned/dangling edge) and
// log it, rather than surface a null identity — see
// `listing_not_supported_under_authorizer` for the full policy. Contrast a user
// member, where an id absent from the catalog is a legitimately-unprovisioned user
// and is tolerated.
#[derive(Debug, Clone, Serialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct RoleMembership {
    #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
    pub id: RoleId,
    #[cfg_attr(feature = "open-api", schema(value_type = String))]
    pub ident: ArcRoleIdent,
    pub name: String,
    pub created_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl From<RoleMembershipEntry> for RoleMembership {
    fn from(entry: RoleMembershipEntry) -> Self {
        RoleMembership {
            id: entry.role_id,
            ident: entry.role_ident,
            name: entry.name,
            // The catalog reader always has the edge timestamp; see field docs.
            created_at: Some(entry.created_at),
        }
    }
}

/// One page of roles (the `member-of` set, or a user's directly-assigned roles).
#[derive(Debug, Clone, Serialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct ListRoleMembershipsResponse {
    pub roles: Vec<RoleMembership>,
    #[serde(alias = "next_page_token")]
    pub next_page_token: Option<String>,
}

/// Query parameters for `GET /role/{id}/members`.
#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::IntoParams))]
#[serde(rename_all = "camelCase")]
pub struct ListMembersQuery {
    /// Restrict to one member kind (`user` or `role`). Both kinds when omitted.
    #[serde(default)]
    pub r#type: Option<RoleMemberType>,
    #[serde(default)]
    pub page_token: Option<String>,
    /// Upper bound on the number of results returned. Default: 100.
    #[serde(default)]
    pub page_size: Option<i64>,
}

impl ListMembersQuery {
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

/// Query parameters for the role-listing endpoints (`member-of`, `user roles`).
#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::IntoParams))]
#[serde(rename_all = "camelCase")]
pub struct ListRolesPageQuery {
    #[serde(default)]
    pub page_token: Option<String>,
    /// Upper bound on the number of results returned. Default: 100.
    #[serde(default)]
    pub page_size: Option<i64>,
}

impl ListRolesPageQuery {
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

impl IntoResponse for AddRoleMembersResponse {
    fn into_response(self) -> axum::response::Response {
        (StatusCode::OK, Json(self)).into_response()
    }
}
impl IntoResponse for ListRoleMembersResponse {
    fn into_response(self) -> axum::response::Response {
        (StatusCode::OK, Json(self)).into_response()
    }
}
impl IntoResponse for ListRoleMembershipsResponse {
    fn into_response(self) -> axum::response::Response {
        (StatusCode::OK, Json(self)).into_response()
    }
}

// ─── helpers ──────────────────────────────────────────────────────────────────

/// Parse a member id into its typed identifier.
fn parse_member(r#type: RoleMemberType, id: &str) -> Result<UserOrRoleId> {
    Ok(match r#type {
        RoleMemberType::User => UserOrRoleId::User(UserId::try_from(id.to_string())?),
        RoleMemberType::Role => UserOrRoleId::Role(RoleId::from_str_or_bad_request(id)?),
    })
}

/// Cold-path listing is not yet implemented for assignment-managing authorizers
/// (e.g. OpenFGA); the authorizer-backed reader lands with the transitive work.
/// When it does, it must resolve role identity from the catalog and **drop orphan
/// roles** (a reachable id with no catalog row) with a `warn!`, while
/// **tolerating** unprovisioned user members — see [`RoleMembership`] (roles
/// are catalog-owned) vs [`RoleMember`] (users are IdP-owned, lazily cached). It
/// must also refill dropped rows so a partly-orphaned page never reads as empty
/// (the house convention treats an empty page as end-of-listing).
fn listing_not_supported_under_authorizer() -> ErrorModel {
    ErrorModel::not_implemented(
        "Listing role members is not yet supported under this authorizer backend.",
        "RoleMembershipListingNotImplemented",
        None,
    )
}

impl<C: CatalogStore, A: Authorizer + Clone, S: SecretStore> Service<C, A, S>
    for ApiServer<C, A, S>
{
}

#[async_trait::async_trait]
pub trait Service<C: CatalogStore, A: Authorizer, S: SecretStore> {
    /// `GET /role/{id}/members` — direct members (users ∪ member roles), one
    /// merged keyset-paginated page, optionally filtered to one kind.
    async fn list_role_members(
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        role_id: RoleId,
        query: ListMembersQuery,
    ) -> Result<ListRoleMembersResponse> {
        let project_id = request_metadata.require_project_id(None)?;
        let authorizer = context.v1_state.authz;

        let event_ctx = APIEventContext::for_role(
            request_metadata.into(),
            context.v1_state.events.clone(),
            role_id,
            CatalogRoleAction::ReadRoleAssignments,
        );
        let role = C::get_role_by_id_cache_aware(
            &project_id,
            role_id,
            CachePolicy::Skip,
            context.v1_state.catalog.clone(),
        )
        .await;
        let authz_result = authorizer
            .require_role_action(
                event_ctx.request_metadata(),
                role,
                CatalogRoleAction::ReadRoleAssignments,
            )
            .await;
        event_ctx.emit_authz(authz_result)?;

        if authorizer.role_assignments().is_some() {
            return Err(listing_not_supported_under_authorizer().into());
        }
        let page = C::list_direct_role_members_page(
            &project_id,
            role_id,
            query.r#type.map(RoleMemberKind::from),
            query.pagination_query(),
            context.v1_state.catalog,
        )
        .await?;
        Ok(ListRoleMembersResponse {
            members: page.assignments.into_iter().map(RoleMember::from).collect(),
            next_page_token: page.next_page_token,
        })
    }

    /// `POST /role/{id}/members` — batch add members. Atomic (all-or-nothing).
    /// This is an **idempotent add/confirm, not a create**: it returns `200` with
    /// the requested members confirmed present (re-adding an existing member is
    /// accepted), never `201`.
    async fn add_role_members(
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        role_id: RoleId,
        request: AddRoleMembersRequest,
    ) -> Result<AddRoleMembersResponse> {
        let project_id = request_metadata.require_project_id(None)?;
        let authorizer = context.v1_state.authz;

        let event_ctx = APIEventContext::for_role(
            request_metadata.into(),
            context.v1_state.events.clone(),
            role_id,
            CatalogRoleAction::ManageRoleAssignments,
        );
        let role = C::get_role_by_id_cache_aware(
            &project_id,
            role_id,
            CachePolicy::Skip,
            context.v1_state.catalog.clone(),
        )
        .await;
        let authz_result = authorizer
            .require_role_action(
                event_ctx.request_metadata(),
                role,
                CatalogRoleAction::ManageRoleAssignments,
            )
            .await;
        let (event_ctx, _role) = event_ctx.emit_authz(authz_result)?;

        // Parse each requested member to its canonical id and dedup on THAT (not the
        // raw string): two case spellings of a role UUID collapse to one, so the
        // echoed response carries no duplicate rows. Order preserved.
        let mut seen = std::collections::HashSet::new();
        let mut subjects: Vec<UserOrRoleId> = Vec::new();
        for member in request.members {
            let subject = parse_member(member.r#type, &member.id)?;
            if seen.insert(subject.clone()) {
                subjects.push(subject);
            }
        }

        match authorizer.role_assignments() {
            None => {
                let mut user_ids = Vec::new();
                let mut role_ids = Vec::new();
                for subject in &subjects {
                    match subject {
                        UserOrRoleId::User(user_id) => user_ids.push(user_id.clone()),
                        UserOrRoleId::Role(role_id) => role_ids.push(*role_id),
                    }
                }
                C::add_role_members_mixed_and_invalidate(
                    &project_id,
                    role_id,
                    &user_ids,
                    &role_ids,
                    context.v1_state.catalog,
                )
                .await?;
            }
            Some(capability) => {
                // Id-only: no per-member role resolution (which would be N round-trips
                // and would wrongly 404 an as-yet-unprovisioned role member — the
                // managing-authorizer path tolerates that for users too).
                let assignments: Vec<(UserOrRoleId, RoleId)> =
                    subjects.iter().map(|s| (s.clone(), role_id)).collect();
                capability
                    .add_role_assignments(
                        event_ctx.request_metadata(),
                        project_id.clone(),
                        &assignments,
                    )
                    .await
                    .map_err(ErrorModel::from)?;
            }
        }

        Ok(AddRoleMembersResponse {
            members: subjects.into_iter().map(RoleMember::from).collect(),
        })
    }

    /// `DELETE /role/{id}/members/{type}/{member_id}` — remove a single member.
    /// Idempotent: removing an absent member is a no-op (`204`).
    async fn remove_role_member(
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        role_id: RoleId,
        member_type: RoleMemberType,
        member_id: String,
    ) -> Result<()> {
        let project_id = request_metadata.require_project_id(None)?;
        let authorizer = context.v1_state.authz;

        let event_ctx = APIEventContext::for_role(
            request_metadata.into(),
            context.v1_state.events.clone(),
            role_id,
            CatalogRoleAction::ManageRoleAssignments,
        );
        let role = C::get_role_by_id_cache_aware(
            &project_id,
            role_id,
            CachePolicy::Skip,
            context.v1_state.catalog.clone(),
        )
        .await;
        let authz_result = authorizer
            .require_role_action(
                event_ctx.request_metadata(),
                role,
                CatalogRoleAction::ManageRoleAssignments,
            )
            .await;
        let (event_ctx, _role) = event_ctx.emit_authz(authz_result)?;

        let subject = parse_member(member_type, &member_id)?;
        match authorizer.role_assignments() {
            None => match subject {
                UserOrRoleId::User(user_id) => {
                    C::remove_user_role_assignments_and_invalidate(
                        role_id,
                        &[user_id],
                        context.v1_state.catalog,
                    )
                    .await?;
                }
                UserOrRoleId::Role(member_role_id) => {
                    C::remove_role_members_and_invalidate(
                        role_id,
                        &[member_role_id],
                        context.v1_state.catalog,
                    )
                    .await?;
                }
            },
            Some(capability) => {
                // Id-only: no role resolution — removal needs only the id and must stay
                // idempotent even if the member role was already deleted while a
                // dangling `<role>#assignee` tuple lingers (resolving would 404 and
                // leave the grant un-removable).
                capability
                    .remove_role_assignments(
                        event_ctx.request_metadata(),
                        project_id.clone(),
                        &[(subject, role_id)],
                    )
                    .await
                    .map_err(AuthorizationFailureSource::into_error_model)?;
            }
        }
        Ok(())
    }

    /// `GET /role/{id}/member-of` — the roles `role_id` is a direct member of.
    async fn list_role_member_of(
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        role_id: RoleId,
        query: ListRolesPageQuery,
    ) -> Result<ListRoleMembershipsResponse> {
        let project_id = request_metadata.require_project_id(None)?;
        let authorizer = context.v1_state.authz;

        let event_ctx = APIEventContext::for_role(
            request_metadata.into(),
            context.v1_state.events.clone(),
            role_id,
            CatalogRoleAction::ReadRoleAssignments,
        );
        let role = C::get_role_by_id_cache_aware(
            &project_id,
            role_id,
            CachePolicy::Skip,
            context.v1_state.catalog.clone(),
        )
        .await;
        let authz_result = authorizer
            .require_role_action(
                event_ctx.request_metadata(),
                role,
                CatalogRoleAction::ReadRoleAssignments,
            )
            .await;
        event_ctx.emit_authz(authz_result)?;

        if authorizer.role_assignments().is_some() {
            return Err(listing_not_supported_under_authorizer().into());
        }
        let page = C::list_direct_role_member_of_page(
            &project_id,
            role_id,
            query.pagination_query(),
            context.v1_state.catalog,
        )
        .await?;
        Ok(ListRoleMembershipsResponse {
            roles: page.entries.into_iter().map(RoleMembership::from).collect(),
            next_page_token: page.next_page_token,
        })
    }

    /// `GET /user/{id}/roles` — the roles a user is directly assigned to.
    async fn list_user_roles(
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        user_id: UserId,
        query: ListRolesPageQuery,
    ) -> Result<ListRoleMembershipsResponse> {
        let project_id = request_metadata.require_project_id(None)?;
        let authorizer = context.v1_state.authz;

        let event_ctx = APIEventContext::for_user(
            request_metadata.into(),
            context.v1_state.events.clone(),
            std::sync::Arc::new(user_id.clone()),
            CatalogUserAction::ReadRoleAssignments,
        );
        let authz_result = authorizer
            .require_user_action(
                event_ctx.request_metadata(),
                &user_id,
                CatalogUserAction::ReadRoleAssignments,
            )
            .await;
        event_ctx.emit_authz(authz_result)?;

        if authorizer.role_assignments().is_some() {
            return Err(listing_not_supported_under_authorizer().into());
        }
        // The catalog reader returns `None` for a user with no catalog row → 404;
        // `Some(page)` is a user that exists (page may be empty → 200). Under OpenFGA
        // (the `Some(_)` arm above, deferred) the reader can't prove non-existence,
        // so that arm will be tolerant (no 404).
        let page = C::list_direct_user_roles_page(
            &project_id,
            &user_id,
            query.pagination_query(),
            context.v1_state.catalog,
        )
        .await?
        .ok_or_else(|| {
            ErrorModel::not_found(
                format!("User with id {user_id} not found or not provisioned."),
                "UserNotFound",
                None,
            )
        })?;
        Ok(ListRoleMembershipsResponse {
            roles: page.entries.into_iter().map(RoleMembership::from).collect(),
            next_page_token: page.next_page_token,
        })
    }
}
