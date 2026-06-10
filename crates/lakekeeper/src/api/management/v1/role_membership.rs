//! Role-membership management API: the direct `/role/{id}/members`,
//! `/role/{id}/member-of` and `/user/{id}/roles` surface.
//!
//! A role's members are **polymorphic** — a member is either a user (a direct
//! user→role assignment) or another role (a role→role membership edge). Both are
//! surfaced through one `/members` collection discriminated by [`RoleMemberType`].
//!
//! Read & write dispatch is **mutually exclusive / single-source-of-truth**: when
//! the authorizer manages assignments (e.g. OpenFGA, via
//! [`Authorizer::role_assignments`]) the edges live in the authorizer's store;
//! otherwise (Cedar/AllowAll) they live in the catalog tables. The authorizer arm
//! reads id-only rows and hydrates role identity from the catalog. The hot authz
//! path is untouched; all of this is cold-path management code.
//!
//! **Direct vs transitive coverage.** The three *direct* reads (`/members`,
//! `/member-of`, `/user/{id}/roles`) are implemented on both arms. The three
//! *transitive* reads are implemented on the **catalog arm only** (a bounded,
//! lazily-paginated recursive SQL CTE); under an assignment-managing authorizer
//! they return `501` — see [`transitive_listing_not_supported_under_authorizer`]
//! for why OpenFGA's graph-listing APIs make a correct, bounded transitive
//! listing impractical today.
//!
//! **Authorization scope.** The *direct* role reads check the per-role
//! `ReadRoleAssignments` — they expose only the named role's own edges, which that
//! check covers. The *transitive* role reads (`/members/transitive`,
//! `/member-of/transitive`) instead require the PROJECT-scoped `ListRoles`
//! capability, because they return a whole closure of *other* roles' members /
//! ancestors. Gating that on one per-role check would, under a per-role authorizer
//! (Cedar), let a grant on the entry role authorize disclosure of nested/ancestor
//! roles the caller cannot read individually. `ListRoles` is the right gate: in the
//! OpenFGA model `role.can_read_assignments` already resolves to
//! `can_list_roles from project`, so reading role assignments is project-uniform by
//! design (no per-role read grant exists) — see `authz/openfga/.../role.fga`. The
//! `/user/{id}/roles/transitive` read stays on the per-user `ReadRoleAssignments`
//! (server-level under OpenFGA): its closure is the *user's own* effective roles,
//! which is exactly the data that check authorizes.
//!
//! **Cross-project nesting divergence.** The catalog forbids role-in-role
//! membership across projects at write time (`add_role_members` →
//! `RoleIdNotFoundInProject`) and its readers are project-scoped. An
//! assignment-managing authorizer (OpenFGA) writes id-only edges with no such
//! check, so a cross-project edge can exist there; the authorizer-arm readers
//! therefore hydrate identity **across projects** (see [`fetch_roles_by_ids`])
//! and faithfully return cross-project members, dropping only ids with no catalog
//! row anywhere (a truly dangling tuple). The listing reflects the authorizer's
//! actual graph rather than re-imposing the catalog's project scoping.
//!
//! Error/emit shape follows `user.rs::get_user`: emit the authorization decision,
//! then run business logic with `?`.

use std::collections::HashMap;

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
        ArcProjectId, ArcRole, ArcRoleIdent, CachePolicy, CatalogListRolesByIdFilter,
        CatalogRoleAssignmentOps, CatalogRoleOps, CatalogStore, Result, RoleId, RoleMemberKind,
        RoleMembershipEntry, SecretStore, State, UserId,
        authz::{
            AuthZError, AuthZProjectOps, AuthZRoleOps, AuthZUserOps, Authorizer,
            CatalogProjectAction, CatalogRoleAction, CatalogUserAction, ManagesRoleAssignments,
            RoleAssignmentFilter, RoleAssignmentRow, UserOrRoleId,
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
///
/// `name` carries the role's display name for **role** members and is `None` for
/// **user** members (a user has no role-name; users are IdP-owned and not resolved
/// here). On the listing endpoints a role member always has a name (an unresolvable
/// role is dropped, not surfaced); it is `None` on the add response (confirmation
/// only, no hydration).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct RoleMember {
    pub r#type: RoleMemberType,
    pub id: String,
    /// Display name for a role member; `None` for a user member or where unresolved.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
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
            name: None,
            created_at: None,
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
    /// Token for the next page; `null`/absent once the listing is exhausted.
    /// Note for SDK authors: **stop when `next_page_token` is null/absent.** The
    /// final page of results may itself return a null token, so don't rely on
    /// receiving a separate trailing empty page — keep requesting until the token
    /// is null.
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
// log it, rather than surface a null identity — see `read_assignee_roles_hydrated`.
// Contrast a user member, where an id absent from the catalog is a
// legitimately-unprovisioned user and is tolerated.
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
    /// Token for the next page; `null`/absent once the listing is exhausted.
    /// Note for SDK authors: **stop when `next_page_token` is null/absent.** The
    /// final page of results may itself return a null token, so don't rely on
    /// receiving a separate trailing empty page — keep requesting until the token
    /// is null.
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

/// Resolve `role_ids` to their catalog rows, keyed by id, **across all projects**.
///
/// An id absent from the result has no catalog row anywhere — a truly dangling
/// authorizer tuple (e.g. a since-deleted role) — and the caller drops it.
/// Cross-project member roles ARE resolved and returned: an assignment-managing
/// authorizer (OpenFGA) permits role-in-role membership across projects, whereas
/// the catalog forbids it at write time (`RoleIdNotFoundInProject`). The listing
/// therefore faithfully reflects the authorizer's graph rather than re-imposing
/// the catalog's project scoping. Role ids are globally-unique UUIDs, so the
/// id-keyed lookup is unambiguous.
///
/// Drains every catalog page: `list_roles` clamps the page size to
/// `pagination_size_max` (default 1000), so a large id set spans several pages —
/// a single sized request would silently truncate.
async fn fetch_roles_by_ids<C: CatalogStore>(
    role_ids: &[RoleId],
    catalog: C::State,
) -> Result<HashMap<RoleId, ArcRole>> {
    let mut roles = HashMap::with_capacity(role_ids.len());
    if role_ids.is_empty() {
        return Ok(roles);
    }
    let mut page_token = PageToken::Empty;
    loop {
        let response = C::list_roles_across_projects(
            CatalogListRolesByIdFilter::builder()
                .role_ids(Some(role_ids))
                .build(),
            PaginationQuery {
                page_token,
                page_size: None,
            },
            catalog.clone(),
        )
        .await
        .map_err(ErrorModel::from)?;
        for role in response.roles {
            roles.insert(role.id, role);
        }
        match response.next_page_token {
            Some(token) if !token.is_empty() => page_token = PageToken::Present(token),
            _ => return Ok(roles),
        }
    }
}

/// Map id-only catalog member rows to `RoleMember`s, attaching each role member's
/// display name (users carry none). A role member always resolves on the catalog
/// arm (the FK guarantees it), so its name is always present — consistent with the
/// authorizer arm's drop-or-name rule.
async fn catalog_members_with_names<C: CatalogStore>(
    assignments: Vec<RoleAssignmentRow>,
    catalog: C::State,
) -> Result<Vec<RoleMember>> {
    let role_member_ids: Vec<RoleId> = assignments
        .iter()
        .filter_map(|r| match r.subject {
            UserOrRoleId::Role(role_id) => Some(role_id),
            UserOrRoleId::User(_) => None,
        })
        .collect();
    let names = fetch_roles_by_ids::<C>(&role_member_ids, catalog).await?;
    Ok(assignments
        .into_iter()
        .map(|row| {
            let name = match &row.subject {
                UserOrRoleId::Role(role_id) => names.get(role_id).map(|r| r.name.clone()),
                UserOrRoleId::User(_) => None,
            };
            RoleMember {
                name,
                created_at: row.created_at,
                ..RoleMember::from(row.subject)
            }
        })
        .collect())
}

/// Read one hydrated page from an assignment-managing authorizer (e.g. OpenFGA).
///
/// The authorizer returns id-only rows. `collect_role_ids` names the role ids on
/// a page that need a catalog row; they are resolved via [`fetch_roles_by_ids`],
/// and `build` turns the raw rows + resolved roles into output items — dropping
/// any role with no catalog row anywhere (a dangling tuple) and keeping IdP-owned
/// user subjects. If dropping orphans empties an otherwise non-final
/// page, the next authorizer page is pulled, so a returned empty page always
/// means the listing is exhausted (the house convention treats empty as end).
#[allow(clippy::too_many_arguments)]
async fn read_hydrated_assignments_page<C, T>(
    assignments: &dyn ManagesRoleAssignments,
    metadata: &RequestMetadata,
    project_id: ArcProjectId,
    filter: RoleAssignmentFilter,
    mut pagination: PaginationQuery,
    catalog: C::State,
    collect_role_ids: impl Fn(&[RoleAssignmentRow]) -> Vec<RoleId>,
    build: impl Fn(&[RoleAssignmentRow], &HashMap<RoleId, ArcRole>) -> Vec<T>,
) -> Result<(Vec<T>, Option<String>)>
where
    C: CatalogStore,
{
    loop {
        let page = assignments
            .list_role_assignments(
                metadata,
                project_id.clone(),
                filter.clone(),
                pagination.clone(),
            )
            .await
            .map_err(ErrorModel::from)?;

        let mut ids = collect_role_ids(&page.assignments);
        ids.sort_unstable();
        ids.dedup();
        let roles = fetch_roles_by_ids::<C>(&ids, catalog.clone()).await?;

        let items = build(&page.assignments, &roles);
        match page.next_page_token {
            // The whole page was orphan role tuples but more pages remain; pull the
            // next so an empty page never masquerades as end-of-listing.
            Some(token) if items.is_empty() => {
                pagination = PaginationQuery {
                    page_token: PageToken::Present(token),
                    page_size: pagination.page_size,
                };
            }
            next_page_token => return Ok((items, next_page_token)),
        }
    }
}

/// Read the roles a `subject` is directly assigned to from an assignment-managing
/// authorizer (the `member-of` and `user-roles` listings), hydrating each target
/// role's identity from the catalog and dropping any with no catalog row in the
/// project. Returns `RoleMembership` rows carrying the tuple's write timestamp.
async fn read_assignee_roles_hydrated<C: CatalogStore>(
    assignments: &dyn ManagesRoleAssignments,
    metadata: &RequestMetadata,
    project_id: ArcProjectId,
    subject: UserOrRoleId,
    pagination: PaginationQuery,
    catalog: C::State,
) -> Result<(Vec<RoleMembership>, Option<String>)> {
    read_hydrated_assignments_page::<C, RoleMembership>(
        assignments,
        metadata,
        project_id,
        RoleAssignmentFilter::ByAssignee(subject),
        pagination,
        catalog,
        |rows| rows.iter().map(|r| r.role_id).collect(),
        |rows, roles| {
            rows.iter()
                .filter_map(|r| {
                    let Some(role) = roles.get(&r.role_id) else {
                        tracing::warn!(
                            role_id = %r.role_id,
                            "Dropping assigned role with no catalog row (dangling assignment tuple)."
                        );
                        return None;
                    };
                    Some(RoleMembership {
                        id: role.id,
                        ident: role.ident_arc(),
                        name: role.name.clone(),
                        created_at: r.created_at,
                    })
                })
                .collect()
        },
    )
    .await
}

/// The **transitive** listings (`…/members/transitive`, `…/roles/transitive`,
/// `…/member-of/transitive`) are not implemented under an assignment-managing
/// authorizer (OpenFGA). OpenFGA's graph-listing APIs (`ListObjects`/`ListUsers`)
/// silently truncate at a server-side cap with no continuation token and no
/// completeness signal (see openfga/openfga#1961), and a hand-rolled `Read`-based
/// graph walk would be unbounded and must materialize the whole closure to
/// paginate. Rather than ship a best-effort transitive listing on this backend,
/// these endpoints return `501` here; full transitive support remains on
/// catalog-backed authorizers (Cedar/AllowAll), whose recursive SQL paginates and
/// bounds depth natively. The **direct** reads ARE implemented under OpenFGA.
fn transitive_listing_not_supported_under_authorizer() -> ErrorModel {
    ErrorModel::not_implemented(
        "Transitive role-membership listing is not supported under this authorizer \
         backend; use the direct listings, or a catalog-backed authorizer.",
        "TransitiveRoleMembershipListingNotImplemented",
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
        let (event_ctx, _role) = event_ctx.emit_authz(authz_result)?;

        // Authorizer arm (OpenFGA): members live as `assignee` tuples; read them
        // id-only and hydrate role identity from the catalog. Users are kept
        // verbatim (IdP-owned); role members with no catalog row are dropped. The
        // `?type=` filter is applied here since the authorizer read is unfiltered.
        if let Some(assignments) = authorizer.role_assignments() {
            let want_users = query.r#type != Some(RoleMemberType::Role);
            let want_roles = query.r#type != Some(RoleMemberType::User);
            let (members, next_page_token) = read_hydrated_assignments_page::<C, RoleMember>(
                assignments,
                event_ctx.request_metadata(),
                project_id.clone(),
                RoleAssignmentFilter::ByRole(role_id),
                query.pagination_query(),
                context.v1_state.catalog,
                |rows| {
                    if want_roles {
                        rows.iter()
                            .filter_map(|r| match r.subject {
                                UserOrRoleId::Role(rid) => Some(rid),
                                UserOrRoleId::User(_) => None,
                            })
                            .collect()
                    } else {
                        Vec::new()
                    }
                },
                |rows, roles| {
                    rows.iter()
                        .filter_map(|r| match &r.subject {
                            UserOrRoleId::User(uid) => want_users.then(|| RoleMember {
                                r#type: RoleMemberType::User,
                                id: uid.to_string(),
                                name: None,
                                created_at: r.created_at,
                            }),
                            UserOrRoleId::Role(rid) => {
                                if !want_roles {
                                    return None;
                                }
                                let Some(role) = roles.get(rid) else {
                                    tracing::warn!(
                                        role_id = %rid,
                                        "Dropping role member with no catalog row \
                                         (dangling assignment tuple)."
                                    );
                                    return None;
                                };
                                Some(RoleMember {
                                    r#type: RoleMemberType::Role,
                                    id: rid.to_string(),
                                    name: Some(role.name.clone()),
                                    created_at: r.created_at,
                                })
                            }
                        })
                        .collect()
                },
            )
            .await?;
            return Ok(ListRoleMembersResponse {
                members,
                next_page_token,
            });
        }
        let catalog = context.v1_state.catalog;
        let page = C::list_direct_role_members_page(
            &project_id,
            role_id,
            query.r#type.map(RoleMemberKind::from),
            query.pagination_query(),
            catalog.clone(),
        )
        .await?;
        Ok(ListRoleMembersResponse {
            members: catalog_members_with_names::<C>(page.assignments, catalog).await?,
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
        let (event_ctx, _role) = event_ctx.emit_authz(authz_result)?;

        // Authorizer arm (OpenFGA): the parent roles are the targets of this
        // role's `assignee` tuples; read them id-only and hydrate from the catalog.
        if let Some(assignments) = authorizer.role_assignments() {
            let (roles, next_page_token) = read_assignee_roles_hydrated::<C>(
                assignments,
                event_ctx.request_metadata(),
                project_id.clone(),
                UserOrRoleId::Role(role_id),
                query.pagination_query(),
                context.v1_state.catalog,
            )
            .await?;
            return Ok(ListRoleMembershipsResponse {
                roles,
                next_page_token,
            });
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
        let (event_ctx, ()) = event_ctx.emit_authz(authz_result)?;

        // Authorizer arm (OpenFGA): the assigned roles are the targets of the
        // user's `assignee` tuples; read them id-only and hydrate from the catalog.
        // Unlike the catalog reader, OpenFGA cannot prove a user does not exist, so
        // this arm is tolerant: an unknown user yields an empty page (200), never a
        // 404 — a user is just a subject with no assignment tuples here.
        if let Some(assignments) = authorizer.role_assignments() {
            let (roles, next_page_token) = read_assignee_roles_hydrated::<C>(
                assignments,
                event_ctx.request_metadata(),
                project_id.clone(),
                UserOrRoleId::User(user_id.clone()),
                query.pagination_query(),
                context.v1_state.catalog,
            )
            .await?;
            return Ok(ListRoleMembershipsResponse {
                roles,
                next_page_token,
            });
        }
        // The catalog reader returns `None` for a user with no catalog row → 404;
        // `Some(page)` is a user that exists (page may be empty → 200).
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

    /// `GET /role/{id}/members/transitive` — the role's transitive members: users
    /// assigned to the role or any role in its downward membership closure, plus
    /// every role in that closure. One keyset-paginated page, optionally filtered
    /// to one kind. Transitive rows carry no `created_at` (no single edge).
    async fn list_role_transitive_members(
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        role_id: RoleId,
        query: ListMembersQuery,
    ) -> Result<ListRoleMembersResponse> {
        let project_id = request_metadata.require_project_id(None)?;
        let authorizer = context.v1_state.authz;

        let catalog = context.v1_state.catalog;

        // Transitive listings traverse a CLOSURE of roles, so they are authorized by
        // the PROJECT-scoped `ListRoles` capability, not per-role `ReadRoleAssignments`.
        // Under OpenFGA the two coincide (`can_read_assignments` resolves to
        // `can_list_roles from project`); under a per-role authorizer (Cedar) the
        // project gate is what stops one role's read grant from authorizing disclosure
        // of nested roles in the closure. The named role must still exist (404),
        // checked AFTER the capability so a forbidden caller can't probe existence.
        let event_ctx = APIEventContext::for_project_arc(
            request_metadata.into(),
            context.v1_state.events.clone(),
            project_id.clone(),
            std::sync::Arc::new(CatalogProjectAction::ListRoles),
        );
        let authz_result: std::result::Result<(), AuthZError> = async {
            authorizer
                .require_project_action(
                    event_ctx.request_metadata(),
                    event_ctx.user_provided_entity_arc_ref(),
                    event_ctx.action().clone(),
                )
                .await?;
            let role = C::get_role_by_id_cache_aware(
                &project_id,
                role_id,
                CachePolicy::Skip,
                catalog.clone(),
            )
            .await;
            authorizer.require_role_presence(role)?;
            Ok(())
        }
        .await;
        event_ctx.emit_authz(authz_result)?;

        // Transitive listing is catalog-only; see the helper for why OpenFGA is
        // excluded. The direct `/members` listing IS supported under OpenFGA.
        if authorizer.role_assignments().is_some() {
            return Err(transitive_listing_not_supported_under_authorizer().into());
        }
        let page = C::list_transitive_role_members_page(
            &project_id,
            role_id,
            query.r#type.map(RoleMemberKind::from),
            query.pagination_query(),
            catalog.clone(),
        )
        .await?;
        // Hydrate role-member display names (users carry none), exactly as the
        // direct `/members` listing does — a transitive role member always resolves
        // in the catalog, so its name is always present.
        Ok(ListRoleMembersResponse {
            members: catalog_members_with_names::<C>(page.assignments, catalog).await?,
            next_page_token: page.next_page_token,
        })
    }

    /// `GET /user/{id}/roles/transitive` — the full effective (transitive) role
    /// set a user holds (direct assignments plus every role reachable upward
    /// through membership). Transitive rows carry no `created_at` (no single edge).
    async fn list_user_transitive_roles(
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

        // Transitive listing is catalog-only; see the helper. The direct
        // `/user/{id}/roles` listing IS supported under OpenFGA.
        if authorizer.role_assignments().is_some() {
            return Err(transitive_listing_not_supported_under_authorizer().into());
        }
        let page = C::list_transitive_user_roles_page(
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
            // Transitive: a role reached through the closure has no single defining
            // membership edge, so `created_at` is None (not the `From` impl, which
            // would surface the keyset key — the role's own creation time).
            roles: page
                .entries
                .into_iter()
                .map(|e| RoleMembership {
                    id: e.role_id,
                    ident: e.role_ident,
                    name: e.name,
                    created_at: None,
                })
                .collect(),
            next_page_token: page.next_page_token,
        })
    }

    /// `GET /role/{id}/member-of/transitive` — the full transitive member-of set
    /// of a role: every role it effectively belongs to, reachable upward through
    /// membership. Transitive rows carry no `created_at` (no single defining edge).
    async fn list_role_transitive_member_of(
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        role_id: RoleId,
        query: ListRolesPageQuery,
    ) -> Result<ListRoleMembershipsResponse> {
        let project_id = request_metadata.require_project_id(None)?;
        let authorizer = context.v1_state.authz;

        let catalog = context.v1_state.catalog;

        // Transitive listings traverse a CLOSURE of roles, so they are authorized by
        // the PROJECT-scoped `ListRoles` capability, not per-role `ReadRoleAssignments`
        // (see `list_role_transitive_members` for the full rationale). The named role
        // must still exist (404), checked AFTER the capability.
        let event_ctx = APIEventContext::for_project_arc(
            request_metadata.into(),
            context.v1_state.events.clone(),
            project_id.clone(),
            std::sync::Arc::new(CatalogProjectAction::ListRoles),
        );
        let authz_result: std::result::Result<(), AuthZError> = async {
            authorizer
                .require_project_action(
                    event_ctx.request_metadata(),
                    event_ctx.user_provided_entity_arc_ref(),
                    event_ctx.action().clone(),
                )
                .await?;
            let role = C::get_role_by_id_cache_aware(
                &project_id,
                role_id,
                CachePolicy::Skip,
                catalog.clone(),
            )
            .await;
            authorizer.require_role_presence(role)?;
            Ok(())
        }
        .await;
        event_ctx.emit_authz(authz_result)?;

        // Transitive listing is catalog-only; see the helper. The direct
        // `/role/{id}/member-of` listing IS supported under OpenFGA.
        if authorizer.role_assignments().is_some() {
            return Err(transitive_listing_not_supported_under_authorizer().into());
        }
        let page = C::list_transitive_role_member_of_page(
            &project_id,
            role_id,
            query.pagination_query(),
            catalog,
        )
        .await?;
        Ok(ListRoleMembershipsResponse {
            // Transitive: an ancestor role reached through the closure has no
            // single defining membership edge, so `created_at` is None (not the
            // `From` impl, which would surface the role's own creation time).
            roles: page
                .entries
                .into_iter()
                .map(|e| RoleMembership {
                    id: e.role_id,
                    ident: e.role_ident,
                    name: e.name,
                    created_at: None,
                })
                .collect(),
            next_page_token: page.next_page_token,
        })
    }
}
