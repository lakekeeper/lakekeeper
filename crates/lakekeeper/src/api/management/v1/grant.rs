//! The grants management API: who holds which privilege on which resource.
//!
//! **Preview.** This surface may change in a backward-incompatible way in a future
//! release, as the tags API may.
//!
//! One shape for every resource level — `GET .../grants` lists, `POST .../grants`
//! applies a `{writes, deletes}` diff. There is no `DELETE .../grants/{id}`: a
//! grant's identity is its `(principal, privilege, resource)` triple, so a
//! revocation is just the delete side of a diff. That also keeps the API expressible
//! by authorizers that store grants themselves and have no row ids to hand out.
//!
//! Every handler follows the same order, and the order matters:
//!
//! 1. **Validate the request** — size, duplicates, and (for writes only) that the
//!    privilege is in the authorizer's vocabulary. A bad request is a 400 and must
//!    not be reported as an authorization failure.
//! 2. **Authorize**, inside the single `Result` handed to `emit_authz`, so the audit
//!    event records exactly what was checked.
//! 3. **Write**, after `emit_authz` — folding the write into the authorization
//!    result would mislabel a storage failure as a denial.
//! 4. **Emit** one event per grant that actually changed.
//!
//! ## What the write and vocabulary paths disclose
//!
//! Listings resolve the resource with the level's can-see action, so one the caller
//! cannot see reads as absent — with one deliberate exception: the grant-read action
//! doubles as visibility (see `require_warehouse_action`), because a principal holding
//! only direct `manage_grants` has grant-read without can-see, and masking it would
//! let them apply grants they can never read back. The apply and grantable-privileges
//! paths check no can-see at all: grant authority is independent of visibility, and a
//! principal can hold `manage_grants` directly on a resource it cannot otherwise see.
//!
//! The consequence is that, for a caller who already knows a resource's id, those two
//! paths distinguish "exists but you may not" (403, or a vocabulary with nothing
//! allowed) from "does not exist" (404). Accepted: ids are UUIDs, the request is already
//! confined to the caller's own project, and no privilege, principal or name is
//! revealed — only that the id resolves.
//!
//! ## One explicit body per level
//!
//! The per-level duplication is deliberate, matching `lakekeeper_actions.rs`: the
//! bodies differ in exactly what a shared abstraction would have to parameterize
//! (resolution, can-see action, event context). A ninth level is the trigger to
//! revisit.

use std::{collections::HashMap, sync::Arc};

use iceberg_ext::catalog::rest::ErrorModel;
use serde::{Deserialize, Serialize};

use crate::{
    api::{
        ApiContext, RequestMetadata,
        iceberg::v1::{PaginationQuery, Result},
        management::v1::{
            ApiServer,
            check::UserOrRole,
            lakekeeper_actions::{resolve_principal, set_for_user},
        },
    },
    service::{
        ArcRole, CachePolicy, CatalogGrantOps, CatalogNamespaceOps, CatalogRoleOps, CatalogStore,
        CatalogTagOps, CatalogWarehouseOps, GenericTableId, GrantRevokeBatchTooLarge,
        GrantSubtreeTooLarge, ListGrantsStoreError, ListRolesError, NamespaceHierarchy,
        NamespaceId, NamespaceIdentOrId, ProjectId, ResolvedWarehouse, RoleId, SecretStore, State,
        TableId, TabularListFlags, TagDefinitionId, ViewId, WarehouseId, WarehouseStatus,
        authn::UserId,
        authz::{
            ActionDescriptor, AuthZCannotSeeNamespace, AuthZCannotSeeTag,
            AuthZCannotUseWarehouseId, AuthZError, AuthZGenericTableOps, AuthZGrantActionForbidden,
            AuthZGrantOps, AuthZProjectOps, AuthZServerOps, AuthZTableOps, AuthZTagActionForbidden,
            AuthZTagOps, AuthZViewOps, AuthorizationDecision, Authorizer, AuthzNamespaceOps,
            AuthzWarehouseOps, CatalogGenericTableAction, CatalogNamespaceAction,
            CatalogProjectAction, CatalogServerAction, CatalogTableAction, CatalogTagAction,
            CatalogViewAction, CatalogWarehouseAction, GrantAuthorityCheck, GrantFilter, GrantOp,
            GrantResource, GrantRevokeCandidates, GrantRow, GrantSpec, GrantSubtreeFilter,
            GrantSubtreeRoot, GrantTarget, PrivilegeDescriptor, RequireTagActionError,
            ResourceType, RoleAssignee as AuthzRoleAssignee, UserOrRole as AuthzUserOrRole,
            UserOrRoleId,
        },
        events::{
            APIEventContext, GrantsChangedEvent,
            context::{APIEventActions, IntrospectPermissions},
        },
    },
};

/// Upper bound on a single diff.
///
/// Matches the maximum number of records an authorizer-managed store can write in
/// one atomic batch, so a diff that is accepted here always applies as a unit.
///
/// Public so a store can assert at startup that it can actually write a diff this
/// large. The relationship is real but invisible: raising this without raising the
/// store's batch limit turns every large apply into a runtime backend failure.
pub const MAX_GRANTS_PER_REQUEST: usize = 100;

/// Upper bound on a privilege name, mirroring the storage constraint.
const MAX_PRIVILEGE_LENGTH: usize = 256;

// ---------------------------------------------------------------------------
// Wire types
// ---------------------------------------------------------------------------

/// One entry of a grant diff.
///
/// Unknown fields are rejected so that feeding a `GrantResponse` from a listing
/// straight back into an apply fails loudly instead of silently dropping its
/// `resource` — which would apply the grant to whatever resource the endpoint names.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct GrantEntry {
    /// A privilege from the authorizer's grantable vocabulary. Which values are legal
    /// differs between authorizers and is published by the server.
    #[cfg_attr(feature = "open-api", schema(min_length = 1, max_length = 256))]
    pub privilege: String,
    pub principal: UserOrRole,
}

// An apply returns `204 No Content` and reports no delta.
//
// It once returned the entries it changed, which only one store can actually determine:
// the catalog store diffs inside its transaction, while an authorizer that owns its
// grants writes idempotently and cannot see prior state, so its "created" was an echo of
// the request. One response field meaning two different things per deployment is worse
// than no field, and the delta's real job — emitting an event per genuine change — is
// internal and unaffected. The post-state is implied by a success: everything in
// `writes` is held, everything in `deletes` is not. Which grants exist, with their
// timestamps, is `GET .../grants`; what changed is the `grant_created`/`grant_revoked`
// audit events. This matches the older `/permissions/…/assignments` diff endpoints,
// which are the only comparable multi-edge mutation and also return 204.

/// A grant diff: create `writes`, remove `deletes`.
///
/// Applied atomically, and safe to retry: applying the same diff twice has the same
/// effect as applying it once. The same entry may not appear in both lists — that is
/// rejected rather than resolved, because either reading of it would be a guess.
///
/// Unknown fields are rejected: a misspelled `deletes` would otherwise silently apply
/// the writes and drop the revocations.
///
/// At least one entry is required, and `writes` and `deletes` together may not exceed
/// 100. Neither rule is expressible as a per-field schema constraint — a `maxItems` on
/// each array would say 200 are acceptable — so both are stated here and enforced by
/// the server.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct ApplyGrantsRequest {
    /// Grants to create. Counts against the shared 100-entry limit with `deletes`.
    // max_items is sound but not tight: the 100-entry cap is shared across both
    // arrays, which a per-array bound cannot express.
    #[serde(default)]
    #[cfg_attr(feature = "open-api", schema(max_items = 100))]
    pub writes: Vec<GrantEntry>,
    /// Grants to remove. Counts against the shared 100-entry limit with `writes`.
    #[serde(default)]
    #[cfg_attr(feature = "open-api", schema(max_items = 100))]
    pub deletes: Vec<GrantEntry>,
}

impl ApplyGrantsRequest {
    fn entries(&self) -> impl Iterator<Item = &GrantEntry> {
        self.writes.iter().chain(self.deletes.iter())
    }

    fn is_empty(&self) -> bool {
        self.writes.is_empty() && self.deletes.is_empty()
    }
}

/// The resource a grant is held on, as it appears in a response.
///
/// `type` carries the same spelling as `ResourceType`, which is also the URL segment
/// that addresses the resource — so a client can look a listed grant up in the
/// vocabulary, or build a request path from it, without a translation table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum GrantResourceResponse {
    #[cfg_attr(feature = "open-api", schema(title = "GrantResourceServer"))]
    Server,
    #[cfg_attr(feature = "open-api", schema(title = "GrantResourceProject"))]
    Project {
        #[serde(rename = "project-id")]
        #[cfg_attr(feature = "open-api", schema(value_type = String))]
        project_id: ProjectId,
    },
    #[cfg_attr(feature = "open-api", schema(title = "GrantResourceWarehouse"))]
    Warehouse {
        #[serde(rename = "warehouse-id")]
        #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
        warehouse_id: WarehouseId,
    },
    #[cfg_attr(feature = "open-api", schema(title = "GrantResourceNamespace"))]
    Namespace {
        #[serde(rename = "warehouse-id")]
        #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
        warehouse_id: WarehouseId,
        #[serde(rename = "namespace-id")]
        #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
        namespace_id: NamespaceId,
    },
    #[cfg_attr(feature = "open-api", schema(title = "GrantResourceTable"))]
    Table {
        #[serde(rename = "warehouse-id")]
        #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
        warehouse_id: WarehouseId,
        #[serde(rename = "table-id")]
        #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
        table_id: TableId,
    },
    #[cfg_attr(feature = "open-api", schema(title = "GrantResourceView"))]
    View {
        #[serde(rename = "warehouse-id")]
        #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
        warehouse_id: WarehouseId,
        #[serde(rename = "view-id")]
        #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
        view_id: ViewId,
    },
    #[cfg_attr(feature = "open-api", schema(title = "GrantResourceGenericTable"))]
    GenericTable {
        #[serde(rename = "warehouse-id")]
        #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
        warehouse_id: WarehouseId,
        #[serde(rename = "generic-table-id")]
        #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
        generic_table_id: GenericTableId,
    },
    #[cfg_attr(feature = "open-api", schema(title = "GrantResourceTag"))]
    /// Spelled `tag-definition`, as everywhere else. `kebab-case` alone would render it
    /// `tag`, which matches neither `ResourceType` nor the URL segment.
    #[serde(rename = "tag-definition")]
    Tag {
        #[serde(rename = "tag-definition-id")]
        #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
        tag_definition_id: TagDefinitionId,
    },
}

impl From<GrantResource> for GrantResourceResponse {
    fn from(resource: GrantResource) -> Self {
        match resource {
            GrantResource::Server => Self::Server,
            GrantResource::Project(project_id) => Self::Project { project_id },
            GrantResource::Warehouse(warehouse_id) => Self::Warehouse { warehouse_id },
            GrantResource::Namespace {
                warehouse_id,
                namespace_id,
            } => Self::Namespace {
                warehouse_id,
                namespace_id,
            },
            GrantResource::Table {
                warehouse_id,
                table_id,
            } => Self::Table {
                warehouse_id,
                table_id,
            },
            GrantResource::View {
                warehouse_id,
                view_id,
            } => Self::View {
                warehouse_id,
                view_id,
            },
            GrantResource::GenericTable {
                warehouse_id,
                generic_table_id,
            } => Self::GenericTable {
                warehouse_id,
                generic_table_id,
            },
            GrantResource::Tag(tag_definition_id) => Self::Tag { tag_definition_id },
        }
    }
}

/// One grant in a listing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct GrantResponse {
    pub principal: UserOrRole,
    pub resource: GrantResourceResponse,
    pub privilege: String,
    /// Whether `privilege` is still in the authorizer's vocabulary. Where grants live
    /// in the catalog, a `false` value is surfaced rather than hidden: the grant
    /// enforces nothing today but is still there, and can still be revoked. An
    /// authorizer that owns its grants may instead omit unrecognized grants from
    /// listings entirely, so `false` never appears there — see its documentation.
    pub recognized: bool,
    pub created_at: Option<chrono::DateTime<chrono::Utc>>,
    // No grantor. Who granted a privilege is a question about a past event, not about
    // the grant that exists now, and only some stores can answer it at all — publishing
    // it here made the field's meaning depend on the configured authorizer. The
    // `grant_created` audit event carries it, per grant, for every store.
}

/// A page of grants.
///
/// The order is **unspecified** and differs by authorizer, so do not rely on it. Use the
/// page token to walk a listing, and sort client-side if you need a stable presentation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct ListGrantsResponse {
    pub grants: Vec<GrantResponse>,
    /// Present when another page may follow. Follow the token until it is **absent**:
    /// depending on the authorizer, a page can come back short or even empty while
    /// more grants remain, so neither a short page nor an empty one signals the end.
    ///
    /// The project-scoped listing pages like the rest, but only where grants live in
    /// the catalog. An authorizer that owns its grants may not implement that listing
    /// at all — see its `501` response.
    pub next_page_token: Option<String>,
}

impl axum::response::IntoResponse for ListGrantsResponse {
    fn into_response(self) -> axum::response::Response {
        (http::StatusCode::OK, axum::Json(self)).into_response()
    }
}

/// The privileges this server's authorizer will accept, per resource type.
///
/// Which privileges exist is decided by the configured authorizer, not by the API, so
/// this endpoint is the only reliable way to learn them: sending a name the authorizer
/// does not know is a 400.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct GrantablePrivilegesResponse {
    /// Keyed by resource type. Every resource type the API knows is present, so an
    /// empty list distinguishes "nothing is grantable here" from "unknown type" — an
    /// authorizer that manages no grants reports every list empty.
    // Borrowed from the authorizer's `LazyLock` vocabulary rather than rebuilt per call.
    #[cfg_attr(
        feature = "open-api",
        schema(value_type = std::collections::BTreeMap<String, Vec<PrivilegeDescriptor>>)
    )]
    pub privileges: std::collections::BTreeMap<&'static str, &'static [PrivilegeDescriptor]>,
}

impl axum::response::IntoResponse for GrantablePrivilegesResponse {
    fn into_response(self) -> axum::response::Response {
        (http::StatusCode::OK, axum::Json(self)).into_response()
    }
}

/// One privilege of a resource's vocabulary, and whether the principal may grant it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct GrantablePrivilege {
    /// The privilege itself: static vocabulary, identical for every caller, and the same
    /// object `GET /management/v1/grants/grantable-privileges` publishes. Nested rather
    /// than flattened so the cacheable description and the per-caller decision below stay
    /// distinct — and so a new descriptor field can never collide with `allowed`.
    #[cfg_attr(feature = "open-api", schema(value_type = PrivilegeDescriptor))]
    pub privilege: &'static PrivilegeDescriptor,
    /// Whether the principal may grant this privilege here. Advisory: an apply checks
    /// each of its entries on its own.
    ///
    /// Granting only. Revoking is authorized separately and may be refused where this
    /// says `true` — under the OpenFGA authorizer, delegated grant authority
    /// (`pass_grants`) hands a privilege out without taking it back. There is no
    /// discovery question for the revoke direction; a removal is authorized as it is
    /// applied.
    pub allowed: bool,
}

/// This resource's whole vocabulary, each entry marked with whether the principal may
/// grant it.
///
/// The deployment-wide vocabulary answers "what does this server understand"; this
/// answers "what may I do here", which is the question a grant dialog asks. Grant
/// authority is a right of its own, invisible to action introspection, so neither
/// `.../actions` nor the vocabulary can substitute for it.
///
/// Deliberately **not** filtered to the permitted subset — unlike `allowed-actions` on
/// the action-introspection endpoints. A picker chooses from a closed, published set, so
/// a silently shortened list reads as a missing privilege rather than a withheld one;
/// the caller needs to render `ownership` greyed out, not to wonder where it went.
/// Nothing is disclosed by listing them: the same names are public at
/// `/management/v1/grants/grantable-privileges`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct ResourceGrantablePrivilegesResponse {
    /// Every privilege this resource level publishes, in the authorizer's own order.
    pub privileges: Vec<GrantablePrivilege>,
}

impl axum::response::IntoResponse for ResourceGrantablePrivilegesResponse {
    fn into_response(self) -> axum::response::Response {
        (http::StatusCode::OK, axum::Json(self)).into_response()
    }
}

/// Query parameters shared by every grant listing.
///
/// Two separate parameters rather than one encoded principal, matching the
/// `principalUser`/`principalRole` pair the action-introspection endpoints already use.
///
/// Unlike [`GetGrantAccessQuery`] this cannot carry `deny_unknown_fields`: every listing
/// extracts this and [`PaginationQuery`](crate::api::iceberg::v1::PaginationQuery) from
/// the same query string, so rejecting unknown keys here would reject `pageSize`. The
/// misspelling that attribute guards against — `?principal_user=…` deserializing to
/// `None` — therefore answers unnarrowed here, gated by `ReadGrants` rather than by the
/// self-read rule, so it fails closed on authority.
#[derive(Debug, Clone, Default, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::IntoParams))]
#[serde(rename_all = "camelCase")]
pub struct ListGrantsQuery {
    /// List only the grants held by this user. Mutually exclusive with `principalRole`.
    /// A resource's own listing accepts neither and then lists every principal's; the
    /// project-wide listing requires one of the two.
    #[serde(default)]
    #[cfg_attr(feature = "open-api", param(required = false, value_type = Option<String>))]
    pub principal_user: Option<UserId>,
    /// List only the grants held by this role. Mutually exclusive with `principalUser`,
    /// and subject to the same requirement on the project-wide listing.
    #[serde(default)]
    #[cfg_attr(feature = "open-api", param(required = false, value_type = Option<uuid::Uuid>))]
    pub principal_role: Option<crate::service::RoleId>,
}

impl ListGrantsQuery {
    /// Which principal to narrow to, or `None` for all of them.
    fn try_principal(&self) -> Result<Option<UserOrRoleId>> {
        principal_from_params(self.principal_user.as_ref(), self.principal_role)
    }

    /// The principal to narrow to, refusing a listing that names none.
    ///
    /// Only the project-wide listing requires one. A resource's own listing is bounded
    /// by the resource, so listing every principal's grants there costs what the caller
    /// asked for; the project-wide listing is bounded by the project, so an unnarrowed
    /// answer grows with the deployment instead.
    fn require_principal(&self) -> Result<UserOrRoleId> {
        self.try_principal()?.ok_or_else(|| {
            bad_request(
                "Specify `principalUser` or `principalRole`: this listing answers for \
                 one principal at a time. To read every grant held on a single resource, \
                 use that resource's own listing.",
                "MissingGrantPrincipal",
            )
            .into()
        })
    }
}

/// At most one of the two parameters, never both.
///
/// Shared so that the same mistake reports the same error type everywhere on the grant
/// surface, rather than one per endpoint.
fn principal_from_params(
    principal_user: Option<&UserId>,
    principal_role: Option<crate::service::RoleId>,
) -> Result<Option<UserOrRoleId>> {
    match (principal_user, principal_role) {
        (Some(user), None) => Ok(Some(UserOrRoleId::User(user.clone()))),
        (None, Some(role)) => Ok(Some(UserOrRoleId::Role(role))),
        (Some(_), Some(_)) => Err(bad_request(
            "Specify at most one of `principalUser` and `principalRole`",
            "AmbiguousGrantPrincipal",
        )
        .into()),
        (None, None) => Ok(None),
    }
}

fn default_true() -> bool {
    true
}

/// Upper bound on one subtree revoke call.
///
/// Distinct from [`MAX_GRANTS_PER_REQUEST`], which encodes what an authorizer-managed
/// store can write in one atomic batch — a property of a backend that does not serve
/// these routes at all. This bound exists for the audit payload: every removed grant is
/// announced individually, and this is the largest batch that fits one dispatch.
///
/// Matches the maximum page size, so a subtree revoke needs no configuration knob of its
/// own.
pub const MAX_GRANTS_PER_SUBTREE_REVOKE: usize = 1000;

/// How many namespaces a namespace-rooted subtree operation reads.
///
/// Its cost is proportional to the namespaces the subtree spans, and a page of a subtree
/// this wide stays inside a few hundred milliseconds. Beyond it the request is refused
/// with the size it would have read, so an operator learns the shape of their catalog
/// instead of watching a call run. The warehouse-rooted operations carry no such bound:
/// an index answers them in page-proportional time whatever the warehouse holds.
pub const MAX_SUBTREE_NAMESPACES: u64 = 5000;

/// Query parameters of the subtree listings.
///
/// Every parameter narrows; none is required, because the subtree already bounds the
/// answer. Cannot carry `deny_unknown_fields` for the same reason [`ListGrantsQuery`]
/// cannot: it shares a query string with `PaginationQuery`.
#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::IntoParams))]
#[serde(rename_all = "camelCase")]
pub struct ListSubtreeGrantsQuery {
    /// List only the grants held by this user. Mutually exclusive with `principalRole`.
    #[serde(default)]
    #[cfg_attr(feature = "open-api", param(required = false, value_type = Option<String>))]
    pub principal_user: Option<UserId>,
    /// List only the grants held by this role. Mutually exclusive with `principalUser`.
    #[serde(default)]
    #[cfg_attr(feature = "open-api", param(required = false, value_type = Option<uuid::Uuid>))]
    pub principal_role: Option<crate::service::RoleId>,
    /// List only grants carrying one of these privileges. Repeat the parameter for
    /// several. A name this server's authorizer does not publish is rejected, so a
    /// misspelling cannot read as "nobody holds that".
    #[serde(default)]
    #[cfg_attr(feature = "open-api", param(required = false, value_type = Vec<String>))]
    pub privilege: Vec<String>,
    /// List only grants on resources of these kinds. Repeat the parameter for several.
    /// `table` does not imply `view` or `generic-table`; name each kind you want.
    #[serde(default)]
    #[cfg_attr(feature = "open-api", param(required = false, value_type = Vec<ResourceType>))]
    pub resource_type: Vec<ResourceType>,
    /// Include grants on tables, views and generic tables that are in the recycle bin.
    /// On by default, so that a listing previews exactly what the matching revoke would
    /// remove — an undrop restores a table together with its grants.
    #[serde(default = "default_true")]
    #[cfg_attr(feature = "open-api", param(required = false, default = true))]
    pub include_soft_deleted: bool,
    /// Read only grants created at or before this instant. Meaningful on the first page:
    /// the page token pins the walk's ceiling, and a later value that disagrees with the
    /// token is refused. Omit it to have the server bind the ceiling and report it as
    /// `as-of`.
    #[serde(default)]
    #[cfg_attr(feature = "open-api", param(required = false))]
    pub created_before: Option<chrono::DateTime<chrono::Utc>>,
    /// Include the grants held on the addressed resource itself. On by default: a grant
    /// on a container confers access beneath it, so a listing meant to preview a revoke
    /// must show it. Pass `false` for strictly-below.
    #[serde(default = "default_true")]
    #[cfg_attr(feature = "open-api", param(required = false, default = true))]
    pub include_root_level: bool,
}

impl Default for ListSubtreeGrantsQuery {
    fn default() -> Self {
        Self {
            principal_user: None,
            principal_role: None,
            privilege: Vec::new(),
            resource_type: Vec::new(),
            include_soft_deleted: true,
            created_before: None,
            include_root_level: true,
        }
    }
}

/// A page of a subtree grant listing.
///
/// A page of grants from a subtree, and the instant it was read under.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct ListSubtreeGrantsResponse {
    /// Grants held anywhere under the addressed resource. Reading a subtree is
    /// authorized once, at that resource, and covers every member — so pages come back
    /// full.
    pub grants: Vec<GrantResponse>,
    /// Present when another page may follow; follow it until it is absent. The token
    /// also pins the walk's ceiling, so every page reads under the instant page one
    /// bound.
    pub next_page_token: Option<String>,
    /// The ceiling this walk reads under. Pass it to the matching revoke as
    /// `created-before` to bound the revoke to what was previewed.
    pub as_of: chrono::DateTime<chrono::Utc>,
}

impl axum::response::IntoResponse for ListSubtreeGrantsResponse {
    fn into_response(self) -> axum::response::Response {
        (http::StatusCode::OK, axum::Json(self)).into_response()
    }
}

/// Which of a subtree's grants to revoke, and how far one call may go.
///
/// Unknown fields are rejected: a misspelled `privilege` would otherwise widen the
/// request from "these privileges" to "every grant under here".
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
// Each flag turns a different guard off and they compose freely, so grouping them into a
// sub-object would only change the wire shape without removing a decision.
#[allow(clippy::struct_excessive_bools)]
pub struct RevokeSubtreeGrantsRequest {
    /// Revoke only the grants held by this principal.
    #[serde(default)]
    pub principal: Option<UserOrRole>,
    /// Revoke only grants carrying one of these privileges, each of which must be one
    /// this server's authorizer publishes. Empty matches every privilege, including any
    /// the authorizer no longer knows — which is how those are cleared in bulk.
    #[serde(default)]
    pub privilege: Vec<String>,
    /// Revoke only grants on resources of these kinds. Empty matches every kind under
    /// the addressed resource.
    #[serde(default)]
    pub resource_type: Vec<ResourceType>,
    /// Revoke only grants created at or before this instant. The first call may omit it
    /// and echo back the `created-before` it is answered with; every later call of the
    /// same operation repeats that value. This is what makes the loop terminate:
    /// grants made after the operation began are deliberately left alone.
    #[serde(default)]
    pub created_before: Option<chrono::DateTime<chrono::Utc>>,
    /// At most this many grants are removed by one call. A value outside 1–1000 is
    /// clamped into it, so the response's `revoked` and `has-more` describe what the
    /// server actually did.
    #[serde(default)]
    pub limit: Option<usize>,
    /// Allow the call to leave matching grants behind, to be removed by repeating the
    /// request. Without it, a filter matching more than `limit` grants is refused and
    /// nothing is removed — so nobody starts a multi-call revoke without knowing that
    /// each call is its own transaction and the whole is not atomic.
    #[serde(default)]
    pub allow_partial: bool,
    /// Also revoke the grants held on the addressed resource itself. On by default: a
    /// grant on a container confers access beneath it, so a revoke that skipped the root
    /// would leave standing the access it names. Pass `false` to keep the root's own
    /// grants — its administration plane — untouched, including the one that gives the
    /// caller authority here.
    #[serde(default = "default_true")]
    #[cfg_attr(feature = "open-api", schema(default = true))]
    pub include_root_level: bool,
    /// Compute and return what this request would revoke, removing nothing. The same
    /// read and the same authority checks as the live call; the response's `preview`
    /// carries the batch. A dry run is never refused for size — `has-more: true` says
    /// the live call needs `allow-partial`, or several calls.
    #[serde(default)]
    pub dry_run: bool,
}

/// What one revoke call removed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct RevokeSubtreeGrantsResponse {
    /// How many grants this call removed.
    pub revoked: usize,
    /// Whether grants matching the same filter were still visible to this call. Repeat
    /// the request — with `created-before` set to the value below — until this is
    /// `false`. There is no continuation token: the filter is the continuation, and a
    /// cursor over a delete would permanently skip a grant that sorts below it.
    ///
    /// `false` reports that this call saw nothing further, not that the subtree is clear.
    /// A grant whose transaction committed after the call began can carry a timestamp
    /// below the ceiling and outlive the loop. Re-run the operation with a fresh ceiling
    /// to confirm.
    pub has_more: bool,
    /// The ceiling this call ran under. Pass it back on every later call of the same
    /// operation.
    pub created_before: chrono::DateTime<chrono::Utc>,
    /// The grants a dry run would revoke, rendered as the listing renders them. Present
    /// exactly when the request set `dry-run`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preview: Option<Vec<GrantResponse>>,
}

impl axum::response::IntoResponse for RevokeSubtreeGrantsResponse {
    fn into_response(self) -> axum::response::Response {
        (http::StatusCode::OK, axum::Json(self)).into_response()
    }
}

/// Query parameters of the per-resource vocabulary endpoints.
///
/// Wire-identical to the action-introspection endpoints' query, but described in terms
/// of privileges: reusing their struct published "the user to show *actions* for" on an
/// endpoint that shows nothing of the kind.
/// Unknown parameters are rejected: `?principal_user=…` (or any other misspelling) would
/// otherwise deserialize to `None` and answer for the *caller*, which needs no authority,
/// while the client believed it had asked about someone else.
#[derive(Debug, Clone, Default, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::IntoParams))]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GetGrantAccessQuery {
    /// Report which privileges this user may administer, instead of the caller. Requires
    /// authority to read the resource's grants, since it discloses another principal's
    /// access. Mutually exclusive with `principalRole`.
    #[serde(default)]
    #[cfg_attr(feature = "open-api", param(required = false, value_type = Option<String>))]
    pub principal_user: Option<UserId>,
    /// Report which privileges this role may administer, instead of the caller. Same
    /// authority requirement as `principalUser`, and mutually exclusive with it.
    #[serde(default)]
    #[cfg_attr(feature = "open-api", param(required = false, value_type = Option<uuid::Uuid>))]
    pub principal_role: Option<crate::service::RoleId>,
}

impl GetGrantAccessQuery {
    /// The principal to answer for, in the API's own principal shape, or `None` for the
    /// caller.
    fn try_principal(&self) -> Result<Option<UserOrRole>> {
        Ok(
            principal_from_params(self.principal_user.as_ref(), self.principal_role)?
                .as_ref()
                .map(UserOrRole::from),
        )
    }
}

// ---------------------------------------------------------------------------
// Request validation
// ---------------------------------------------------------------------------

fn bad_request(message: impl Into<String>, r#type: &'static str) -> ErrorModel {
    ErrorModel::bad_request(message, r#type, None)
}

/// Structural validation, independent of the resource and the authorizer.
fn validate_request_shape(request: &ApplyGrantsRequest) -> Result<()> {
    if request.is_empty() {
        return Err(bad_request(
            "A grant diff must contain at least one write or delete",
            "EmptyGrantDiff",
        )
        .into());
    }
    let total = request.writes.len() + request.deletes.len();
    if total > MAX_GRANTS_PER_REQUEST {
        return Err(bad_request(
            format!(
                "A grant diff may contain at most {MAX_GRANTS_PER_REQUEST} entries, got {total}"
            ),
            "GrantDiffTooLarge",
        )
        .into());
    }
    // Each check names every entry that violates it, not just the first. A diff may
    // carry up to MAX_GRANTS_PER_REQUEST entries, and fixing them one round trip at a
    // time is the difference between one correction and a hundred.
    let bad_length = paths_where(request, |entry| {
        let length = entry.privilege.chars().count();
        length == 0 || length > MAX_PRIVILEGE_LENGTH
    });
    if !bad_length.is_empty() {
        return Err(bad_request(
            format!(
                "A privilege must be between 1 and {MAX_PRIVILEGE_LENGTH} characters; violated by {}",
                summarize(&bad_length)
            ),
            "InvalidGrantPrivilegeLength",
        )
        .into());
    }
    // The same grant cannot be both created and removed by one request: the caller
    // is asking for two contradictory outcomes, and stores that apply a diff
    // natively reject it outright.
    let contradictory = indexed(&request.writes, "writes")
        .filter(|(_, entry)| request.deletes.contains(entry))
        .map(|(path, _)| path)
        .collect::<Vec<_>>();
    if !contradictory.is_empty() {
        return Err(bad_request(
            format!(
                "The same grant appears in both writes and deletes: {}",
                summarize(&contradictory)
            ),
            "ContradictoryGrantDiff",
        )
        .into());
    }
    let repeated = [
        duplicate_paths(&request.writes, "writes"),
        duplicate_paths(&request.deletes, "deletes"),
    ]
    .concat();
    if !repeated.is_empty() {
        return Err(bad_request(
            format!(
                "A grant diff must not repeat the same entry: {}",
                summarize(&repeated)
            ),
            "DuplicateGrantEntry",
        )
        .into());
    }
    Ok(())
}

/// `("writes[3]", entry)` for each entry of one side, so an error can point at it.
fn indexed<'a>(
    entries: &'a [GrantEntry],
    side: &'static str,
) -> impl Iterator<Item = (String, &'a GrantEntry)> {
    entries
        .iter()
        .enumerate()
        .map(move |(i, entry)| (format!("{side}[{i}]"), entry))
}

/// Paths of every entry in the diff failing `predicate`, both sides.
/// How many offending paths an error message names before it stops counting.
const MAX_REPORTED_OFFENDERS: usize = 5;

/// Name the offending paths, bounded.
///
/// Every check reports all of its offenders so one round trip fixes them all, but a
/// 100-entry diff would otherwise produce an error message longer than the request:
/// past a handful the list stops being something a human reads. Paths only — echoing
/// the rejected values back would put up to 100 caller-supplied strings into the
/// response and the audit log.
fn summarize(paths: &[String]) -> String {
    if paths.len() <= MAX_REPORTED_OFFENDERS {
        return paths.join(", ");
    }
    format!(
        "{} and {} more",
        paths[..MAX_REPORTED_OFFENDERS].join(", "),
        paths.len() - MAX_REPORTED_OFFENDERS
    )
}

fn paths_where(
    request: &ApplyGrantsRequest,
    predicate: impl Fn(&GrantEntry) -> bool,
) -> Vec<String> {
    indexed(&request.writes, "writes")
        .chain(indexed(&request.deletes, "deletes"))
        .filter(|(_, entry)| predicate(entry))
        .map(|(path, _)| format!("{path}.privilege"))
        .collect()
}

/// Paths of the later occurrence of each repeated entry; the first is not the error.
fn duplicate_paths(entries: &[GrantEntry], side: &'static str) -> Vec<String> {
    indexed(entries, side)
        .enumerate()
        .filter(|(i, (_, entry))| entries[..*i].contains(entry))
        .map(|(_, (path, _))| path)
        .collect()
}

/// Reject privileges the authorizer cannot grant — on **writes only**.
///
/// Deletes stay unvalidated on purpose: a privilege that has left the vocabulary, or
/// that arrived from another authorizer, must remain revocable or its grants would
/// be permanently stuck.
fn validate_write_privileges<A: Authorizer>(
    authorizer: &A,
    resource_type: ResourceType,
    request: &ApplyGrantsRequest,
) -> Result<()> {
    let mut rejected = Vec::new();
    let mut first_error = None;
    for (path, entry) in indexed(&request.writes, "writes") {
        if let Err(e) = authorizer.validate_grant_privilege(resource_type, &entry.privilege) {
            rejected.push(format!("{path}.privilege"));
            first_error.get_or_insert(e);
        }
    }
    let Some(first_error) = first_error else {
        return Ok(());
    };
    // Keep the authorizer's own wording and error type; add where the offenders are.
    let mut model = ErrorModel::from(first_error);
    model.message = format!("{}; rejected {}", model.message, summarize(&rejected));
    Err(model.into())
}

/// Every role named as a grantee anywhere in the diff, resolved to the role itself.
///
/// The authority check carries resolved grantees, so an authorizer can read a grantee
/// role's identity and attributes instead of a `RoleId` it has no way to look up. One
/// listing covers both sides of the diff.
///
/// Across projects, not within the request's: the server level has no project to scope to,
/// and scoping here would make a cross-project role indistinguishable from a missing one,
/// which is [`validate_write_principals`]'s distinction to draw. Nothing is refused here —
/// a role that does not resolve is simply absent, and the check then asks without a
/// grantee, which narrows nothing.
///
/// Runs before the authority gate, which is safe only because it cannot fail: the
/// project-membership *judgement* stays after the gate, so an unauthorized caller still
/// cannot use it to learn which roles exist.
async fn resolve_grantee_roles<C: CatalogRoleOps>(
    request: &ApplyGrantsRequest,
    catalog_state: C::State,
) -> Result<HashMap<RoleId, ArcRole>> {
    let mut role_ids: Vec<_> = request
        .entries()
        .filter_map(|entry| match &entry.principal {
            UserOrRole::Role(assignee) => Some(assignee.role_id()),
            UserOrRole::User(_) => None,
        })
        .collect();
    role_ids.sort_unstable();
    role_ids.dedup();
    Ok(resolve_roles::<C>(role_ids, catalog_state).await?)
}

/// Look up each of `role_ids`, dropping the ones that do not resolve. `role_ids` must
/// already be sorted and deduplicated; the page is sized to it, so one call covers them
/// all.
async fn resolve_roles<C: CatalogRoleOps>(
    role_ids: Vec<RoleId>,
    catalog_state: C::State,
) -> std::result::Result<HashMap<RoleId, ArcRole>, ListRolesError> {
    if role_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let found = C::list_roles_across_projects(
        crate::service::CatalogListRolesByIdFilter::builder()
            .role_ids(Some(role_ids.as_slice()))
            .build(),
        PaginationQuery::new(
            crate::api::iceberg::v1::PageToken::Empty,
            Some(i64::try_from(role_ids.len()).unwrap_or(i64::MAX)),
        ),
        catalog_state,
    )
    .await?
    .roles;
    Ok(found.into_iter().map(|role| (role.id(), role)).collect())
}

/// Roles are project-scoped while their ids are global, so a caller could otherwise
/// grant a privilege to a role from a different project. Checked on writes only, for
/// the same reason as the privilege vocabulary: a grant to a role that has since
/// moved or vanished must stay revocable.
///
/// Reads the resolution [`resolve_grantee_roles`] already performed rather than querying
/// again — a role from another project resolves but reports a different project, which is
/// the same answer a project-scoped listing gave by omitting it.
fn validate_write_principals(
    request: &ApplyGrantsRequest,
    project_id: &ProjectId,
    grantee_roles: &HashMap<RoleId, ArcRole>,
) -> Result<()> {
    let missing = request
        .writes
        .iter()
        .find_map(|entry| match &entry.principal {
            UserOrRole::Role(assignee) => {
                let role_id = assignee.role_id();
                match grantee_roles.get(&role_id) {
                    Some(role) if role.project_id() == project_id => None,
                    _ => Some(role_id),
                }
            }
            UserOrRole::User(_) => None,
        });
    if let Some(missing) = missing {
        return Err(bad_request(
            format!("Role `{missing}` does not exist in this project"),
            "GrantRoleNotInProject",
        )
        .into());
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Shared apply / list bodies
// ---------------------------------------------------------------------------

/// The grant-authority questions a diff asks, one per entry, in `writes`-then-`deletes`
/// order: which privilege, destined for which principal, moving which way.
///
/// One question per entry rather than one per privilege: a diff handing one privilege to a
/// hundred principals asks a hundred questions. Whether the grantee or the direction
/// changes the answer belongs to the authorizer's model, so this layer collapses neither.
/// An authorizer that resolves authority from the privilege alone collapses them itself,
/// where that is sound — the tuple-based one does.
///
/// The direction each question carries is what lets an authorizer hold a principal to
/// taking access away without letting it hand access out.
///
/// Nothing is deduplicated here because nothing can repeat: [`validate_request_shape`]
/// rejects an entry repeated within a side and an entry appearing on both sides, and the
/// side is what fixes the direction.
fn authority_questions<'a>(
    request: &'a ApplyGrantsRequest,
    grantee_roles: &HashMap<RoleId, ArcRole>,
) -> Vec<(Option<AuthzUserOrRole>, &'a str, GrantOp)> {
    let sides = [
        (request.writes.iter(), GrantOp::Grant),
        (request.deletes.iter(), GrantOp::Revoke),
    ];
    sides
        .into_iter()
        .flat_map(|(entries, op)| {
            entries.map(move |entry| {
                let grantee = match &entry.principal {
                    UserOrRole::User(user_id) => Some(AuthzUserOrRole::User(user_id.clone())),
                    // Absent when the role does not resolve; see `resolve_grantee_roles`.
                    UserOrRole::Role(assignee) => {
                        grantee_roles.get(&assignee.role_id()).map(|role| {
                            AuthzUserOrRole::Role(AuthzRoleAssignee::from_role(role.clone()))
                        })
                    }
                };
                (grantee, entry.privilege.as_str(), op)
            })
        })
        .collect()
}

/// What a refusal is about: every question `decisions` refused, with the direction it
/// asked about, in the order they were refused.
///
/// `decisions` answer `questions` positionally. Refusal order is not request order once an
/// authorizer answers per grantee: a privilege allowed for the first principal that asks
/// for it and refused for the second is reported where the refusal is. Repeats are left
/// in — naming each privilege once is the error's own presentation choice, and it needs
/// every refused direction to say which were refused.
fn refused_privileges<'a>(
    questions: &[(Option<AuthzUserOrRole>, &'a str, GrantOp)],
    decisions: &[AuthorizationDecision],
) -> Vec<(GrantOp, &'a str)> {
    questions
        .iter()
        .zip(decisions)
        .filter(|(_, decision)| !decision.allowed)
        .map(|((_, privilege, op), _)| (*op, *privilege))
        .collect()
}

/// Check grant authority for every entry of the diff, including the deletes: taking a
/// privilege away is administering it too, so it is asked rather than assumed. Whether an
/// authorizer answers the two directions alike is its own business.
async fn require_grant_authority<A: Authorizer>(
    authorizer: &A,
    metadata: &RequestMetadata,
    target: &GrantTarget<'_>,
    request: &ApplyGrantsRequest,
    grantee_roles: &HashMap<RoleId, ArcRole>,
) -> std::result::Result<(), AuthZError> {
    // Materialized first so each check can borrow its grantee: the request carries the
    // API principal type, and resolving it allocates.
    let questions = authority_questions(request, grantee_roles);
    let checks: Vec<GrantAuthorityCheck<'_>> = questions
        .iter()
        .map(|(grantee, privilege, op)| {
            GrantAuthorityCheck::entry(privilege, grantee.as_ref(), *op)
        })
        .collect();
    let decisions = authorizer
        .are_allowed_grants(metadata, None, target, &checks)
        .await?;
    // Every refused privilege is named, matching the validation errors above: one round
    // trip should surface everything the caller must remove.
    let refused = refused_privileges(&questions, &decisions);
    if !refused.is_empty() {
        return Err(AuthZGrantActionForbidden::new(&target.resource(), refused).into());
    }
    Ok(())
}

/// The vocabulary entries the principal may administer on `target`.
///
/// One batch call, so eight or nine privileges cost one round trip to the authorizer.
///
/// `for_user` must already have passed the read gate: answering for another principal
/// discloses that principal's access, and each caller enforces that with its own
/// resource's `ReadGrants` action, which it is the only one holding the resolved entity
/// for. See [`grant_read_is_delegated`].
async fn allowed_privileges<A: Authorizer>(
    authorizer: &A,
    request_metadata: &RequestMetadata,
    for_user: Option<AuthzUserOrRole>,
    target: &GrantTarget<'_>,
) -> std::result::Result<Vec<GrantablePrivilege>, AuthZError> {
    let vocabulary = authorizer.grantable_privileges(target.resource_type());
    // Asks about granting, with no grantee: may the subject hand each privilege out here
    // at all? Advisory - the apply path asks again per entry, naming grantee and
    // direction - so an authorizer that distinguishes grantees need not enumerate anyone
    // to answer.
    let checks: Vec<GrantAuthorityCheck<'_>> = vocabulary
        .iter()
        .map(|privilege| GrantAuthorityCheck::grantable(privilege.name.as_str()))
        .collect();
    let decisions = authorizer
        .are_allowed_grants(request_metadata, for_user.as_ref(), target, &checks)
        .await?;
    Ok(vocabulary
        .iter()
        .zip(decisions)
        .map(|(privilege, decision)| GrantablePrivilege {
            privilege,
            allowed: decision.allowed,
        })
        .collect())
}

fn to_specs(entries: &[GrantEntry], resource: &GrantResource) -> Vec<GrantSpec> {
    entries
        .iter()
        .map(|entry| GrantSpec {
            principal: UserOrRoleId::from(&entry.principal),
            resource: resource.clone(),
            privilege: entry.privilege.clone(),
        })
        .collect()
}

/// Persist the diff and emit one event per grant that actually changed.
///
/// Runs after `emit_authz`: a storage failure here is a storage failure, not a
/// denial.
async fn apply_and_emit<A: Authorizer, C: CatalogStore>(
    authorizer: &A,
    catalog_state: C::State,
    events: &crate::service::events::EventDispatcher,
    request_metadata: Arc<RequestMetadata>,
    resource: &GrantResource,
    request: &ApplyGrantsRequest,
) -> Result<()> {
    let writes = to_specs(&request.writes, resource);
    let deletes = to_specs(&request.deletes, resource);

    let applied = if let Some(store) = authorizer.grants() {
        store
            .apply_grants(&request_metadata, &writes, &deletes)
            .await
            .map_err(ErrorModel::from)?
    } else {
        C::apply_grants(&writes, &deletes, catalog_state).await?
    };

    let event = GrantsChangedEvent::new(applied.removed, applied.created, request_metadata);
    if !event.is_empty() {
        events.grants_changed(event).await;
    }
    Ok(())
}

/// Refuse a namespace root that spans more namespaces than the subtree read covers.
///
/// Asked after the authorization gate: the size of a subtree is information about it, so
/// a caller who may not read it does not learn this either.
async fn require_bounded_subtree<C: CatalogStore>(
    warehouse_id: WarehouseId,
    namespace_id: NamespaceId,
    catalog_state: C::State,
) -> std::result::Result<(), ListGrantsStoreError> {
    let namespaces =
        C::count_subtree_namespaces_impl(warehouse_id, namespace_id, catalog_state).await?;
    if namespaces > MAX_SUBTREE_NAMESPACES {
        return Err(GrantSubtreeTooLarge::new(namespaces, MAX_SUBTREE_NAMESPACES).into());
    }
    Ok(())
}

/// How many grants one revoke call may remove: the caller's `limit`, clamped.
fn revoke_limit(request: &RevokeSubtreeGrantsRequest) -> usize {
    request
        .limit
        .unwrap_or(MAX_GRANTS_PER_SUBTREE_REVOKE)
        .clamp(1, MAX_GRANTS_PER_SUBTREE_REVOKE)
}

/// The store filter a revoke request asks for.
fn subtree_filter(request: &RevokeSubtreeGrantsRequest) -> GrantSubtreeFilter {
    GrantSubtreeFilter {
        include_root_level: request.include_root_level,
        principal: request.principal.as_ref().map(UserOrRoleId::from),
        privileges: request.privilege.clone(),
        resource_types: request.resource_type.clone(),
        // Not a choice on the revoke: see `GrantSubtreeFilter::include_soft_deleted`.
        include_soft_deleted: true,
        created_before: request.created_before,
    }
}

/// Read one page of the subtree and render it.
///
/// `ReadSubtreeGrants` on the root covers every member, so the page is returned as read:
/// full pages, nothing filtered after the fact.
async fn list_subtree_and_render<A: Authorizer, C: CatalogStore>(
    authorizer: &A,
    catalog_state: C::State,
    root: GrantSubtreeRoot,
    query: &ListSubtreeGrantsQuery,
    principal: Option<UserOrRoleId>,
    pagination: PaginationQuery,
) -> Result<ListSubtreeGrantsResponse> {
    let filter = GrantSubtreeFilter {
        include_root_level: query.include_root_level,
        principal,
        privileges: query.privilege.clone(),
        resource_types: query.resource_type.clone(),
        include_soft_deleted: query.include_soft_deleted,
        created_before: query.created_before,
    };
    let page = C::list_grants_in_subtree(root, &filter, pagination, catalog_state).await?;
    Ok(ListSubtreeGrantsResponse {
        grants: page
            .grants
            .into_iter()
            .map(|row| render_grant(authorizer, row))
            .collect(),
        next_page_token: page.next_page_token,
        as_of: page.as_of,
    })
}

/// Remove the authorized candidates and announce every grant that actually went.
///
/// One event per removed grant, not a summary: a revoked grant is deleted outright, so
/// its `grant_revoked` record is the only remaining evidence that the access ever
/// existed, and a summary cannot answer when a given principal lost a given privilege.
///
/// Runs after `emit_authz` and after the transaction commits, like every other grant
/// write: emitting before the commit would record removals a rollback then undid.
async fn revoke_and_emit<C: CatalogStore>(
    catalog_state: C::State,
    events: &crate::service::events::EventDispatcher,
    request_metadata: Arc<RequestMetadata>,
    root: GrantSubtreeRoot,
    candidates: GrantRevokeCandidates,
    request: &RevokeSubtreeGrantsRequest,
) -> Result<RevokeSubtreeGrantsResponse> {
    // Refused before anything is removed, so a caller who has not said they understand
    // that each call is its own transaction cannot start a multi-call operation by
    // accident. The one who has loops until `has-more` is false.
    //
    // After the gate, and still a 400 rather than a denial — the same placement as
    // `validate_write_principals` on the apply path, for the same reason: what it checks
    // is not knowable until the authorized set is.
    if candidates.has_more && !request.allow_partial {
        return Err(ErrorModel::from(GrantRevokeBatchTooLarge::new(revoke_limit(request))).into());
    }

    let removed = C::revoke_grant_candidates(root, &candidates.candidates, catalog_state).await?;
    let revoked = removed.len();
    let event = GrantsChangedEvent::new(removed, Vec::new(), request_metadata);
    if !event.is_empty() {
        events.grants_changed(event).await;
    }
    Ok(RevokeSubtreeGrantsResponse {
        revoked,
        has_more: candidates.has_more,
        created_before: candidates.as_of,
        preview: None,
    })
}

/// What the live call would remove, removing nothing.
///
/// No size gate: `GrantRevokeBatchTooLarge` guards a removal a caller did not know was
/// partial, and a dry run removes nothing — `has-more` in the answer is how the caller
/// learns the live call needs `allow-partial`. No events either: nothing changed.
fn preview_response<A: Authorizer>(
    authorizer: &A,
    candidates: GrantRevokeCandidates,
) -> RevokeSubtreeGrantsResponse {
    RevokeSubtreeGrantsResponse {
        revoked: 0,
        has_more: candidates.has_more,
        created_before: candidates.as_of,
        preview: Some(
            candidates
                .candidates
                .into_iter()
                .map(|candidate| render_grant(authorizer, candidate.grant))
                .collect(),
        ),
    }
}

/// Read one page of grants, marking each row's privilege as recognized or not.
async fn list_and_render<A: Authorizer, C: CatalogStore>(
    authorizer: &A,
    catalog_state: C::State,
    request_metadata: &RequestMetadata,
    filter: GrantFilter,
    pagination: PaginationQuery,
) -> Result<ListGrantsResponse> {
    let page = if let Some(store) = authorizer.grants() {
        store
            .list_grants(request_metadata, filter, pagination)
            .await
            .map_err(ErrorModel::from)?
    } else {
        C::list_grants(&filter, pagination, catalog_state).await?
    };
    Ok(ListGrantsResponse {
        grants: page
            .grants
            .into_iter()
            .map(|row| render_grant(authorizer, row))
            .collect(),
        next_page_token: page.next_page_token,
    })
}

/// Whether a listing narrowed to `principal` asks only about the caller's own grants.
///
/// Such a listing needs no grant-read authority: reading your own access discloses
/// nothing the caller does not already have. The per-resource listings still require the
/// level's *can-see* action instead, so that the authorizer — not this endpoint — decides
/// whether the resource exists for this caller. Every other listing reads someone else's
/// access and needs authority to read the resource's grants.
///
/// Self is the *acting* identity, the same one [`Authorizer::are_allowed_grants`] folds to
/// `None`. Under an assumed role that is the role, not the user behind it: a token narrowed
/// to a role must not read the whole user's access, and asking about the role it is acting
/// as is the one role question that discloses nothing new. Resolving self from
/// [`RequestMetadata::user_id`] instead would make this endpoint decide on a different
/// identity than the authorizer does.
fn is_self_read(principal: Option<&UserOrRoleId>, request_metadata: &RequestMetadata) -> bool {
    let Some(principal) = principal else {
        return false;
    };
    request_metadata
        .actor()
        .to_user_or_role()
        .is_some_and(|actor| UserOrRoleId::from(&actor) == *principal)
}

/// Whether this request asks about a principal other than the acting one.
///
/// Answering for another principal discloses that principal's access, so the actor must
/// hold grant-read authority on the resource. Naming the acting identity — or naming
/// nobody, which on these endpoints asks about the actor — discloses nothing new and is
/// exempt. Note the contrast with [`is_self_read`]: on a *listing*, naming nobody asks
/// about everybody and is the case that most needs the gate.
///
/// Self is the *acting* identity, the same one
/// [`Authorizer::are_allowed_grants`](crate::service::authz::AuthZGrantOps::are_allowed_grants)
/// folds to `None`, so this endpoint and the authorizer decide on one identity.
fn grant_read_is_delegated(
    for_user: Option<&AuthzUserOrRole>,
    request_metadata: &RequestMetadata,
) -> bool {
    for_user.is_some() && request_metadata.actor().to_user_or_role().as_ref() != for_user
}

fn render_grant<A: Authorizer>(authorizer: &A, row: GrantRow) -> GrantResponse {
    let resource_type = row.resource.resource_type();
    // Asked as a predicate, not as a discarded `Result`: this runs once per listed row,
    // and the error variant would allocate the privilege name only to drop it.
    let recognized = authorizer.is_grantable_privilege(resource_type, &row.privilege);
    GrantResponse {
        principal: UserOrRole::from(&row.principal),
        resource: row.resource.into(),
        privilege: row.privilege,
        recognized,
        created_at: row.created_at,
    }
}

/// A resource id from another project must read as absent, not as a denial: the
/// caller has no business learning that it exists.
fn ensure_warehouse_in_project(
    warehouse_id: WarehouseId,
    resource_project: &ProjectId,
    request_project: &ProjectId,
) -> std::result::Result<(), AuthZError> {
    if resource_project == request_project {
        return Ok(());
    }
    Err(AuthZCannotUseWarehouseId::new_not_found(warehouse_id).into())
}

/// Tabulars are addressed by id here, and soft-deleted ones stay addressable: their
/// grants are intact and an undrop restores them, so the recycle bin must not make a
/// tabular's grants unreachable. Matches how tag management resolves the same ids.
const TABULAR_FLAGS: TabularListFlags = TabularListFlags {
    include_active: true,
    include_staged: true,
    include_deleted: true,
};

/// Event-action marker for a grant write. Grant authority is resolved by the authorizer
/// from the privilege name rather than modelled as a `Catalog*Action`, so the audit event
/// carries its own marker instead of borrowing a resource action.
///
/// Carries what was asked for, because a *denied* apply emits no per-grant
/// `GrantCreated`/`GrantRevoked` event — without this the audit log would record only
/// that someone was refused here, not what they tried to do.
///
/// Both the privileges and the principals they were destined for are recorded: a refused
/// attempt to escalate is only attributable if the log names the intended beneficiary,
/// not merely the actor and the privilege. Each list is deduplicated, and the request's
/// 100-entry cap bounds what either can grow to, so one event stays bounded whatever the
/// diff's size. The counts are kept because deduplication loses them.
#[derive(Clone, Debug)]
pub struct ApplyGrants {
    principals: Vec<String>,
    privileges: Vec<String>,
    writes: usize,
    deletes: usize,
}

impl ApplyGrants {
    fn of(request: &ApplyGrantsRequest) -> Self {
        let mut privileges: Vec<String> = request
            .entries()
            .map(|entry| entry.privilege.clone())
            .collect();
        privileges.sort_unstable();
        privileges.dedup();
        // Prefixed by kind, matching the wire discriminator, so a user id and a role id
        // that happen to coincide stay distinguishable in the log.
        let mut principals: Vec<String> = request
            .entries()
            .map(|entry| match &entry.principal {
                UserOrRole::User(user_id) => format!("user:{user_id}"),
                UserOrRole::Role(assignee) => format!("role:{}", assignee.role_id()),
            })
            .collect();
        principals.sort_unstable();
        principals.dedup();
        Self {
            principals,
            privileges,
            writes: request.writes.len(),
            deletes: request.deletes.len(),
        }
    }
}

impl APIEventActions for ApplyGrants {
    fn event_actions(&self) -> Vec<ActionDescriptor> {
        vec![
            ActionDescriptor::builder()
                .action_name("apply_grants")
                .context_list("principals", self.principals.clone())
                .context_list("privileges", self.privileges.clone())
                .context_string("writes", self.writes.to_string())
                .context_string("deletes", self.deletes.to_string())
                .build(),
        ]
    }
}

/// The resource kinds a namespace root covers.
const NAMESPACE_SUBTREE_KINDS: &[ResourceType] = &[
    ResourceType::Namespace,
    ResourceType::Table,
    ResourceType::View,
    ResourceType::GenericTable,
];

/// The resource kinds a warehouse root covers once the warehouse level is in scope.
const WAREHOUSE_SUBTREE_KINDS: &[ResourceType] = &[
    ResourceType::Warehouse,
    ResourceType::Namespace,
    ResourceType::Table,
    ResourceType::View,
    ResourceType::GenericTable,
];

/// The kinds an operation can actually return, so both filter validators are answered from
/// one place.
///
/// Keyed on whether the warehouse level is in scope rather than on the root's kind: a
/// warehouse-rooted operation that has not opted the warehouse in cannot return a
/// warehouse grant, so accepting `warehouse` from either filter would match nothing and
/// report a clean subtree — the failure both validators exist to prevent.
const fn subtree_kinds(include_warehouse_level: bool) -> &'static [ResourceType] {
    if include_warehouse_level {
        WAREHOUSE_SUBTREE_KINDS
    } else {
        NAMESPACE_SUBTREE_KINDS
    }
}

/// Subtree operations answer only where grants live in the catalog.
///
/// An authorizer that owns its grant store indexes them by object, so "every grant under
/// this namespace" would mean walking its store one resource at a time — the same reason
/// the project-wide listing reports `501` there. Checked before the authorization gate
/// and deliberately outside the single `Result` the other paths fold everything into: a
/// `501` is about the deployment, not about this caller, and recording an authorization
/// decision for a request nothing can decide is noise.
fn require_catalog_grants<A: Authorizer>(authorizer: &A) -> Result<()> {
    if authorizer.grants().is_none() {
        return Ok(());
    }
    Err(ErrorModel::not_implemented(
        "Subtree grant operations are not available under the configured authorization \
         backend, which stores grants itself. Read and change grants through each \
         resource's own grant endpoints.",
        "SubtreeGrantsNotImplemented",
        None,
    )
    .into())
}

/// Reject a `privilege` filter naming something no resource kind under this root
/// publishes.
///
/// The write path validates privileges and the delete path deliberately does not — a
/// privilege that has left the vocabulary must stay revocable through the per-resource
/// diff, which names it outright. A *filter* is neither: `?privilege=slect` would match
/// nothing, answer `200`, and report zero revoked, which an operator reads as "their
/// access is gone". So a filter fails closed. An unfiltered request still sweeps
/// unrecognized privileges, which is how a subtree is cleared of them.
fn validate_filter_privileges<A: Authorizer>(
    authorizer: &A,
    kinds: &[ResourceType],
    privileges: &[String],
) -> Result<()> {
    let unknown: Vec<&str> = privileges
        .iter()
        .filter(|privilege| {
            !kinds
                .iter()
                .any(|kind| authorizer.is_grantable_privilege(*kind, privilege))
        })
        .map(String::as_str)
        .collect();
    if unknown.is_empty() {
        return Ok(());
    }
    Err(bad_request(
        format!(
            "No resource under this subtree has the privilege {}. \
             `GET /management/v1/grants/grantable-privileges` publishes the names this \
             server accepts.",
            unknown
                .iter()
                .map(|name| format!("`{name}`"))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        "UnknownGrantPrivilege",
    )
    .into())
}

/// Reject a `resourceType` filter naming a kind this root does not cover.
///
/// Same reasoning as [`validate_filter_privileges`], and the same failure it prevents:
/// `resource-type: ["tag-definition"]` on a namespace revoke would match nothing, answer
/// `200` with zero removed, and read as "already clean". A tag definition belongs to the
/// project, and the warehouse itself is not under a namespace, so neither is a narrowing
/// of this request — it is a different request.
fn validate_filter_resource_types(
    kinds: &[ResourceType],
    requested: &[ResourceType],
) -> Result<()> {
    let outside: Vec<&str> = requested
        .iter()
        .filter(|kind| !kinds.contains(kind))
        .map(|kind| kind.as_str())
        .collect();
    if outside.is_empty() {
        return Ok(());
    }
    Err(bad_request(
        format!(
            "Nothing under this subtree is a {}. Grants on it are read and changed \
             through that resource's own endpoints.",
            outside
                .iter()
                .map(|kind| format!("`{kind}`"))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        "GrantSubtreeScopeMismatch",
    )
    .into())
}

/// Gate a subtree revoke at its root, before any candidate is read.
///
/// `ReadSubtreeGrants` first: reading the batch is a read, and without it the refusal
/// itself would report whether the subtree holds a matching grant — so a caller who may
/// not read is answered as if the root did not exist. `RevokeSubtreeGrants` then covers
/// the whole batch with one content-independent question, which an authorizer must
/// answer as authority over everything beneath the root (see the action docs). Both are
/// asked bare, without the level's can-see action, so an authority holder passes without
/// needing `describe`.
async fn require_subtree_revoke_actions<A: Authorizer>(
    authorizer: &A,
    metadata: &RequestMetadata,
    warehouse: &ResolvedWarehouse,
    namespace: Option<&NamespaceHierarchy>,
) -> std::result::Result<(), AuthZError> {
    let (read, revoke) = if let Some(namespace) = namespace {
        tokio::try_join!(
            authorizer.is_allowed_namespace_action(
                metadata,
                None,
                warehouse,
                &namespace.parents,
                &namespace.namespace,
                CatalogNamespaceAction::ReadSubtreeGrants,
            ),
            authorizer.is_allowed_namespace_action(
                metadata,
                None,
                warehouse,
                &namespace.parents,
                &namespace.namespace,
                CatalogNamespaceAction::RevokeSubtreeGrants,
            ),
        )?
    } else {
        tokio::try_join!(
            authorizer.is_allowed_warehouse_action(
                metadata,
                None,
                warehouse,
                CatalogWarehouseAction::ReadSubtreeGrants,
            ),
            authorizer.is_allowed_warehouse_action(
                metadata,
                None,
                warehouse,
                CatalogWarehouseAction::RevokeSubtreeGrants,
            ),
        )?
    };
    if !read.into_inner() {
        // The refusal an unreachable root gives, so a caller without authority cannot
        // tell a subtree holding nothing from one they may not look into.
        return Err(match namespace {
            Some(namespace) => AuthZCannotSeeNamespace::new_forbidden(
                warehouse.warehouse_id,
                NamespaceIdentOrId::Id(namespace.namespace_id()),
            )
            .into(),
            None => AuthZCannotUseWarehouseId::new_access_denied(warehouse.warehouse_id).into(),
        });
    }
    if !revoke.into_inner() {
        // A reader already sees the subtree, so this refusal masks nothing. No privilege
        // names: the refused action is the whole operation, not any pair in the batch.
        let resource = match namespace {
            Some(namespace) => GrantResource::Namespace {
                warehouse_id: warehouse.warehouse_id,
                namespace_id: namespace.namespace_id(),
            },
            None => GrantResource::Warehouse(warehouse.warehouse_id),
        };
        let refused: Vec<(GrantOp, &str)> = Vec::new();
        return Err(AuthZGrantActionForbidden::new(&resource, refused).into());
    }
    Ok(())
}

/// Event-action marker for a subtree revoke.
///
/// Carries the **filter**, not the matched grants: what was asked for is fixed when the
/// request arrives, while what matches is not known until after the gate and is bounded
/// only by the batch size. Every removed grant gets its own `grant_revoked` record, so
/// nothing is lost by keeping this event to the request.
#[derive(Clone, Debug)]
// Mirrors the request's flags one-to-one; see the note on the request struct.
#[allow(clippy::struct_excessive_bools)]
pub struct RevokeSubtreeGrants {
    principal: Option<String>,
    privileges: Vec<String>,
    resource_types: Vec<String>,
    created_before: Option<String>,
    allow_partial: bool,
    include_root_level: bool,
    dry_run: bool,
}

impl RevokeSubtreeGrants {
    fn of(request: &RevokeSubtreeGrantsRequest) -> Self {
        let mut privileges = request.privilege.clone();
        privileges.sort_unstable();
        privileges.dedup();
        let mut resource_types: Vec<String> = request
            .resource_type
            .iter()
            .map(|kind| kind.as_str().to_string())
            .collect();
        resource_types.sort_unstable();
        resource_types.dedup();
        Self {
            // Prefixed by kind, matching the wire discriminator, so a user id and a role
            // id that coincide stay distinguishable in the log.
            principal: request.principal.as_ref().map(|principal| match principal {
                UserOrRole::User(user_id) => format!("user:{user_id}"),
                UserOrRole::Role(assignee) => format!("role:{}", assignee.role_id()),
            }),
            privileges,
            resource_types,
            created_before: request.created_before.map(|at| at.to_rfc3339()),
            allow_partial: request.allow_partial,
            include_root_level: request.include_root_level,
            dry_run: request.dry_run,
        }
    }
}

impl APIEventActions for RevokeSubtreeGrants {
    fn event_actions(&self) -> Vec<ActionDescriptor> {
        let mut descriptor = ActionDescriptor::builder()
            .action_name("revoke_subtree_grants")
            .context_list("privileges", self.privileges.clone())
            .context_list("resource-types", self.resource_types.clone())
            .context_string("allow-partial", self.allow_partial.to_string())
            .context_string("include-root-level", self.include_root_level.to_string())
            .context_string("dry-run", self.dry_run.to_string());
        if let Some(principal) = &self.principal {
            descriptor = descriptor.context_string("principal", principal.clone());
        }
        if let Some(created_before) = &self.created_before {
            descriptor = descriptor.context_string("created-before", created_before.clone());
        }
        vec![descriptor.build()]
    }
}

#[async_trait::async_trait]
pub trait Service<C: CatalogStore, A: Authorizer, S: SecretStore> {
    /// The grantable privilege vocabulary, for every resource type.
    ///
    /// Ungated, and emits no authorization event. The vocabulary is static deployment
    /// configuration — identical for every caller, already published in the docs, and
    /// needed by anyone who may use the grants API at all in order to form a valid
    /// request. Gating it would make discovery stricter than the endpoints it
    /// describes, and there is no per-caller decision to record.
    async fn get_grantable_privileges(
        context: ApiContext<State<A, C, S>>,
        _request_metadata: RequestMetadata,
    ) -> Result<GrantablePrivilegesResponse> {
        let authorizer = context.v1_state.authz;
        let privileges = <ResourceType as strum::VariantArray>::VARIANTS
            .iter()
            .map(|resource_type| {
                (
                    resource_type.as_str(),
                    authorizer.grantable_privileges(*resource_type),
                )
            })
            .collect();
        Ok(GrantablePrivilegesResponse { privileges })
    }

    /// List the grants held on a warehouse.
    async fn list_warehouse_grants(
        warehouse_id: WarehouseId,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        query: ListGrantsQuery,
        pagination: PaginationQuery,
    ) -> Result<ListGrantsResponse> {
        let project_id = request_metadata.require_project_id(None)?;
        let principal = query.try_principal()?;
        let is_self = is_self_read(principal.as_ref(), &request_metadata);
        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;

        let required = if is_self {
            CatalogWarehouseAction::IncludeInList
        } else {
            CatalogWarehouseAction::ReadGrants
        };
        let mut event_ctx = APIEventContext::for_warehouse(
            request_metadata.into(),
            context.v1_state.events.clone(),
            warehouse_id,
            required.clone(),
        );
        event_ctx.push_extra_context("self-read", if is_self { "true" } else { "false" });
        let event_ctx = event_ctx;
        let warehouse = C::get_warehouse_by_id_cache_aware(
            warehouse_id,
            WarehouseStatus::active_and_inactive(),
            CachePolicy::Use,
            catalog_state.clone(),
        )
        .await;
        let authz_result = async {
            let resolved = authorizer
                .require_warehouse_action(
                    event_ctx.request_metadata(),
                    warehouse_id,
                    warehouse,
                    required,
                )
                .await?;
            ensure_warehouse_in_project(warehouse_id, &resolved.project_id, &project_id)
        }
        .await;
        let (event_ctx, ()) = event_ctx.emit_authz(authz_result)?;

        list_and_render::<A, C>(
            &authorizer,
            catalog_state,
            event_ctx.request_metadata(),
            GrantFilter::on(GrantResource::Warehouse(warehouse_id), principal),
            pagination,
        )
        .await
    }

    /// List the direct grants held anywhere under a namespace, the namespace's own
    /// included.
    ///
    /// One `ReadSubtreeGrants` check on the root covers every member, so pages come
    /// back full.
    async fn list_namespace_subtree_grants(
        warehouse_id: WarehouseId,
        namespace_id: NamespaceId,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        query: ListSubtreeGrantsQuery,
        pagination: PaginationQuery,
    ) -> Result<ListSubtreeGrantsResponse> {
        let project_id = request_metadata.require_project_id(None)?;
        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;
        require_catalog_grants(&authorizer)?;
        let principal = principal_from_params(query.principal_user.as_ref(), query.principal_role)?;
        let kinds = subtree_kinds(false);
        validate_filter_privileges(&authorizer, kinds, &query.privilege)?;
        validate_filter_resource_types(kinds, &query.resource_type)?;

        let event_ctx = APIEventContext::for_namespace(
            request_metadata.into(),
            context.v1_state.events.clone(),
            warehouse_id,
            namespace_id,
            CatalogNamespaceAction::ReadSubtreeGrants,
        );
        let authz_result = async {
            let (warehouse, _) = authorizer
                .load_and_authorize_namespace_action::<C>(
                    event_ctx.request_metadata(),
                    event_ctx.user_provided_entity().clone(),
                    CatalogNamespaceAction::ReadSubtreeGrants,
                    CachePolicy::Use,
                    catalog_state.clone(),
                )
                .await?;
            ensure_warehouse_in_project(warehouse_id, &warehouse.project_id, &project_id)?;
            Ok::<_, AuthZError>(warehouse)
        }
        .await;
        let (_event_ctx, _warehouse) = event_ctx.emit_authz(authz_result)?;
        require_bounded_subtree::<C>(warehouse_id, namespace_id, catalog_state.clone()).await?;

        list_subtree_and_render::<A, C>(
            &authorizer,
            catalog_state,
            GrantSubtreeRoot::Namespace {
                warehouse_id,
                namespace_id,
            },
            &query,
            principal,
            pagination,
        )
        .await
    }

    /// List the direct grants held anywhere under a warehouse, the warehouse's own
    /// included.
    async fn list_warehouse_subtree_grants(
        warehouse_id: WarehouseId,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        query: ListSubtreeGrantsQuery,
        pagination: PaginationQuery,
    ) -> Result<ListSubtreeGrantsResponse> {
        let project_id = request_metadata.require_project_id(None)?;
        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;
        require_catalog_grants(&authorizer)?;
        let principal = principal_from_params(query.principal_user.as_ref(), query.principal_role)?;
        let kinds = subtree_kinds(query.include_root_level);
        validate_filter_privileges(&authorizer, kinds, &query.privilege)?;
        validate_filter_resource_types(kinds, &query.resource_type)?;

        let event_ctx = APIEventContext::for_warehouse(
            request_metadata.into(),
            context.v1_state.events.clone(),
            warehouse_id,
            CatalogWarehouseAction::ReadSubtreeGrants,
        );
        // Active only, unlike the warehouse's own grant listing. Deactivating a warehouse
        // is documented to hide its children's grants, and a listing that walks every
        // child would surface exactly those.
        let warehouse = C::get_active_warehouse_by_id(warehouse_id, catalog_state.clone()).await;
        let authz_result = async {
            let resolved = authorizer
                .require_warehouse_action(
                    event_ctx.request_metadata(),
                    warehouse_id,
                    warehouse,
                    CatalogWarehouseAction::ReadSubtreeGrants,
                )
                .await?;
            ensure_warehouse_in_project(warehouse_id, &resolved.project_id, &project_id)?;
            Ok::<_, AuthZError>(resolved)
        }
        .await;
        let (_event_ctx, _warehouse) = event_ctx.emit_authz(authz_result)?;

        list_subtree_and_render::<A, C>(
            &authorizer,
            catalog_state,
            GrantSubtreeRoot::Warehouse { warehouse_id },
            &query,
            principal,
            pagination,
        )
        .await
    }

    /// Revoke the direct grants held anywhere under a namespace, bounded per call.
    async fn revoke_namespace_subtree_grants(
        warehouse_id: WarehouseId,
        namespace_id: NamespaceId,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        request: RevokeSubtreeGrantsRequest,
    ) -> Result<RevokeSubtreeGrantsResponse> {
        let project_id = request_metadata.require_project_id(None)?;
        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;
        let events = context.v1_state.events.clone();
        require_catalog_grants(&authorizer)?;
        let kinds = subtree_kinds(false);
        validate_filter_privileges(&authorizer, kinds, &request.privilege)?;
        validate_filter_resource_types(kinds, &request.resource_type)?;

        let root = GrantSubtreeRoot::Namespace {
            warehouse_id,
            namespace_id,
        };
        let event_ctx = APIEventContext::for_namespace(
            request_metadata.into(),
            events.clone(),
            warehouse_id,
            namespace_id,
            RevokeSubtreeGrants::of(&request),
        );
        let authz_result = async {
            // Presence, then the root's subtree actions. Both before anything else, so a
            // root the caller may not reach is a 404 rather than a query, and neither the
            // refusal nor the work done to reach it reports what the subtree holds.
            let (warehouse, namespace) = tokio::join!(
                C::get_active_warehouse_by_id(warehouse_id, catalog_state.clone()),
                C::get_namespace_cache_aware(
                    warehouse_id,
                    NamespaceIdentOrId::Id(namespace_id),
                    CachePolicy::Skip,
                    catalog_state.clone()
                )
            );
            let warehouse = authorizer.require_warehouse_presence(warehouse_id, warehouse)?;
            let namespace =
                authorizer.require_namespace_presence(warehouse_id, namespace_id, namespace)?;
            ensure_warehouse_in_project(warehouse_id, &warehouse.project_id, &project_id)?;
            require_subtree_revoke_actions(
                &authorizer,
                event_ctx.request_metadata(),
                &warehouse,
                Some(&namespace),
            )
            .await?;
            // Inside this one result, so a store failure is recorded and reported rather
            // than escaping the gate it happened under.
            require_bounded_subtree::<C>(warehouse_id, namespace_id, catalog_state.clone()).await?;
            let filter = subtree_filter(&request);
            let candidates = C::select_subtree_grant_candidates_impl(
                root,
                &filter,
                revoke_limit(&request),
                catalog_state.clone(),
            )
            .await?;
            Ok::<_, AuthZError>(candidates)
        }
        .await;
        let (event_ctx, candidates) = event_ctx.emit_authz(authz_result)?;
        if request.dry_run {
            return Ok(preview_response(&authorizer, candidates));
        }

        revoke_and_emit::<C>(
            catalog_state,
            &events,
            event_ctx.request_metadata_arc(),
            root,
            candidates,
            &request,
        )
        .await
    }

    /// Revoke the direct grants held anywhere under a warehouse, bounded per call.
    ///
    /// Gated on the warehouse's `ReadSubtreeGrants` and `RevokeSubtreeGrants`.
    async fn revoke_warehouse_subtree_grants(
        warehouse_id: WarehouseId,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        request: RevokeSubtreeGrantsRequest,
    ) -> Result<RevokeSubtreeGrantsResponse> {
        let project_id = request_metadata.require_project_id(None)?;
        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;
        let events = context.v1_state.events.clone();
        require_catalog_grants(&authorizer)?;
        let kinds = subtree_kinds(request.include_root_level);
        validate_filter_privileges(&authorizer, kinds, &request.privilege)?;
        validate_filter_resource_types(kinds, &request.resource_type)?;

        let root = GrantSubtreeRoot::Warehouse { warehouse_id };
        let event_ctx = APIEventContext::for_warehouse(
            request_metadata.into(),
            events.clone(),
            warehouse_id,
            RevokeSubtreeGrants::of(&request),
        );
        // As `apply_warehouse_grants` resolves it: inactive included, because stripping
        // access from a deactivated warehouse is the point of the operation, and uncached,
        // because both the project scope and the gate are decided from this row.
        let warehouse = C::get_warehouse_by_id_cache_aware(
            warehouse_id,
            WarehouseStatus::active_and_inactive(),
            CachePolicy::Skip,
            catalog_state.clone(),
        )
        .await;
        let authz_result = async {
            let filter = subtree_filter(&request);
            // Presence, then the root's subtree actions; see the namespace-rooted twin.
            let resolved = authorizer.require_warehouse_presence(warehouse_id, warehouse)?;
            ensure_warehouse_in_project(warehouse_id, &resolved.project_id, &project_id)?;
            require_subtree_revoke_actions(
                &authorizer,
                event_ctx.request_metadata(),
                &resolved,
                None,
            )
            .await?;
            let candidates = C::select_subtree_grant_candidates_impl(
                root,
                &filter,
                revoke_limit(&request),
                catalog_state.clone(),
            )
            .await?;
            Ok::<_, AuthZError>(candidates)
        }
        .await;
        let (event_ctx, candidates) = event_ctx.emit_authz(authz_result)?;
        if request.dry_run {
            return Ok(preview_response(&authorizer, candidates));
        }

        revoke_and_emit::<C>(
            catalog_state,
            &events,
            event_ctx.request_metadata_arc(),
            root,
            candidates,
            &request,
        )
        .await
    }

    /// List one principal's grants across a whole project.
    ///
    /// This is the "what does this principal hold here" view. A principal is required:
    /// see [`ListGrantsQuery::require_principal`]. Server grants are excluded: they
    /// belong to no project, so `GET /server/grants` covers them.
    ///
    /// Grants are reported at the layer they are held: a grant a role holds is listed
    /// under that role, not under the users who have the role, and a grant on an
    /// ancestor is listed under the ancestor. Use the action-check endpoints to ask what
    /// a principal may effectively do.
    async fn list_grants(
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        query: ListGrantsQuery,
        pagination: PaginationQuery,
    ) -> Result<ListGrantsResponse> {
        let project_id = request_metadata.require_project_id(None)?;
        // Before the gate, as the two-principals rejection is: a request that names no
        // principal has no listing to authorize.
        let principal = query.require_principal()?;
        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;

        // Asking about yourself is free — the same self-introspection allowance the
        // action endpoints make. Any other principal reads someone else's access and
        // needs the project-level gate. Unlike the per-resource listings there is no
        // resource to be allowed to see, so the self path falls back to the project's
        // can-see action rather than to no check at all: `project_id` comes from
        // `x-project-id`, and an unchecked path would answer for a project the caller
        // has no relation to.
        let is_self = is_self_read(Some(&principal), &request_metadata);

        let required = if is_self {
            CatalogProjectAction::GetMetadata
        } else {
            CatalogProjectAction::ReadGrants
        };
        let mut event_ctx = APIEventContext::for_project(
            request_metadata.into(),
            context.v1_state.events.clone(),
            (*project_id).clone(),
            required.clone(),
        );
        // Record which path was taken. Without this an auditor cannot tell which action
        // the check actually required.
        event_ctx.push_extra_context("self-read", if is_self { "true" } else { "false" });
        let event_ctx = event_ctx;
        let authz_result = authorizer
            .require_project_action(event_ctx.request_metadata(), &project_id, required)
            .await
            .map_err(AuthZError::from);
        let (event_ctx, ()) = event_ctx.emit_authz(authz_result)?;

        let filter = GrantFilter::ByPrincipal {
            principal,
            project_id: (*project_id).clone(),
        };
        list_and_render::<A, C>(
            &authorizer,
            catalog_state,
            event_ctx.request_metadata(),
            filter,
            pagination,
        )
        .await
    }

    /// List the grants held on the server itself.
    ///
    /// This is also the only way to read a principal's server grants: they belong to no
    /// project, so the project-scoped listing excludes them.
    async fn list_server_grants(
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        query: ListGrantsQuery,
        pagination: PaginationQuery,
    ) -> Result<ListGrantsResponse> {
        let principal = query.try_principal()?;
        let is_self = is_self_read(principal.as_ref(), &request_metadata);
        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;

        // Action before id, unlike every other level: `for_server` takes the action
        // first because the server is ambient rather than user-provided.
        let mut event_ctx = APIEventContext::for_server(
            request_metadata.into(),
            context.v1_state.events.clone(),
            CatalogServerAction::ReadGrants,
            authorizer.server_id(),
        );
        event_ctx.push_extra_context("self-read", if is_self { "true" } else { "false" });
        let event_ctx = event_ctx;
        let authz_result = async {
            // The server has no can-see action — every caller reaches it — so a
            // self-read checks nothing, as on the project-scoped listing.
            if is_self {
                return Ok(());
            }
            authorizer
                .require_server_action(
                    event_ctx.request_metadata(),
                    None,
                    CatalogServerAction::ReadGrants,
                )
                .await
                .map_err(AuthZError::from)
        }
        .await;
        let (event_ctx, ()) = event_ctx.emit_authz(authz_result)?;

        list_and_render::<A, C>(
            &authorizer,
            catalog_state,
            event_ctx.request_metadata(),
            GrantFilter::on(GrantResource::Server, principal),
            pagination,
        )
        .await
    }

    /// Apply a grant diff on the server itself.
    async fn apply_server_grants(
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        request: ApplyGrantsRequest,
    ) -> Result<()> {
        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;
        let events = context.v1_state.events.clone();
        let resource = GrantResource::Server;

        validate_request_shape(&request)?;
        validate_write_privileges(&authorizer, ResourceType::Server, &request)?;
        // Before the gate, and deliberately cannot fail: the authority check carries
        // resolved grantees. See `resolve_grantee_roles`.
        let grantee_roles = resolve_grantee_roles::<C>(&request, catalog_state.clone()).await?;
        // No principal validation: roles are project-scoped and the server is not in a
        // project, so there is no project to resolve a role against. Role principals
        // stay allowed because the assignment API already grants server relations to
        // roles, and refusing them here would lose that capability.

        let event_ctx = APIEventContext::for_server(
            request_metadata.into(),
            events.clone(),
            ApplyGrants::of(&request),
            authorizer.server_id(),
        );
        let authz_result = require_grant_authority(
            &authorizer,
            event_ctx.request_metadata(),
            &GrantTarget::Server,
            &request,
            &grantee_roles,
        )
        .await;
        let (event_ctx, ()) = event_ctx.emit_authz(authz_result)?;

        apply_and_emit::<A, C>(
            &authorizer,
            catalog_state,
            &events,
            event_ctx.request_metadata_arc(),
            &resource,
            &request,
        )
        .await
    }

    /// List the grants held on a project.
    async fn list_project_grants(
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        query: ListGrantsQuery,
        pagination: PaginationQuery,
    ) -> Result<ListGrantsResponse> {
        let project_id = request_metadata.require_project_id(None)?;
        let principal = query.try_principal()?;
        let is_self = is_self_read(principal.as_ref(), &request_metadata);
        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;

        let required = if is_self {
            CatalogProjectAction::GetMetadata
        } else {
            CatalogProjectAction::ReadGrants
        };
        let mut event_ctx = APIEventContext::for_project(
            request_metadata.into(),
            context.v1_state.events.clone(),
            (*project_id).clone(),
            required.clone(),
        );
        event_ctx.push_extra_context("self-read", if is_self { "true" } else { "false" });
        let event_ctx = event_ctx;
        let authz_result = authorizer
            .require_project_action(event_ctx.request_metadata(), &project_id, required)
            .await
            .map_err(AuthZError::from);
        let (event_ctx, ()) = event_ctx.emit_authz(authz_result)?;

        list_and_render::<A, C>(
            &authorizer,
            catalog_state,
            event_ctx.request_metadata(),
            GrantFilter::on(GrantResource::Project((*project_id).clone()), principal),
            pagination,
        )
        .await
    }

    /// Apply a grant diff on a project.
    async fn apply_project_grants(
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        request: ApplyGrantsRequest,
    ) -> Result<()> {
        let project_id = request_metadata.require_project_id(None)?;
        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;
        let events = context.v1_state.events.clone();
        let resource = GrantResource::Project((*project_id).clone());

        validate_request_shape(&request)?;
        validate_write_privileges(&authorizer, ResourceType::Project, &request)?;
        // Before the gate, and deliberately cannot fail: the authority check carries
        // resolved grantees. See `resolve_grantee_roles`.
        let grantee_roles = resolve_grantee_roles::<C>(&request, catalog_state.clone()).await?;
        let event_ctx = APIEventContext::for_project(
            request_metadata.into(),
            events.clone(),
            (*project_id).clone(),
            ApplyGrants::of(&request),
        );
        let authz_result = require_grant_authority(
            &authorizer,
            event_ctx.request_metadata(),
            &GrantTarget::Project(&project_id),
            &request,
            &grantee_roles,
        )
        .await;
        let (event_ctx, ()) = event_ctx.emit_authz(authz_result)?;

        // After the gate: before it, the role lookup let an unauthorized caller probe
        // which roles exist in a project, and a catalog failure swallowed the
        // authorization event entirely. Still a 400, not an authorization failure.
        validate_write_principals(&request, &project_id, &grantee_roles)?;

        apply_and_emit::<A, C>(
            &authorizer,
            catalog_state,
            &events,
            event_ctx.request_metadata_arc(),
            &resource,
            &request,
        )
        .await
    }

    /// List the grants held on a namespace.
    async fn list_namespace_grants(
        warehouse_id: WarehouseId,
        namespace_id: NamespaceId,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        query: ListGrantsQuery,
        pagination: PaginationQuery,
    ) -> Result<ListGrantsResponse> {
        let project_id = request_metadata.require_project_id(None)?;
        let principal = query.try_principal()?;
        let is_self = is_self_read(principal.as_ref(), &request_metadata);
        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;

        let required = if is_self {
            CatalogNamespaceAction::IncludeInList
        } else {
            CatalogNamespaceAction::ReadGrants
        };
        let mut event_ctx = APIEventContext::for_namespace(
            request_metadata.into(),
            context.v1_state.events.clone(),
            warehouse_id,
            namespace_id,
            required.clone(),
        );
        event_ctx.push_extra_context("self-read", if is_self { "true" } else { "false" });
        let event_ctx = event_ctx;
        let authz_result = async {
            let (warehouse, _) = authorizer
                .load_and_authorize_namespace_action::<C>(
                    event_ctx.request_metadata(),
                    event_ctx.user_provided_entity().clone(),
                    required,
                    CachePolicy::Use,
                    catalog_state.clone(),
                )
                .await?;
            ensure_warehouse_in_project(warehouse_id, &warehouse.project_id, &project_id)
        }
        .await;
        let (event_ctx, ()) = event_ctx.emit_authz(authz_result)?;

        list_and_render::<A, C>(
            &authorizer,
            catalog_state,
            event_ctx.request_metadata(),
            GrantFilter::on(
                GrantResource::Namespace {
                    warehouse_id,
                    namespace_id,
                },
                principal,
            ),
            pagination,
        )
        .await
    }

    /// Apply a grant diff on a namespace.
    async fn apply_namespace_grants(
        warehouse_id: WarehouseId,
        namespace_id: NamespaceId,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        request: ApplyGrantsRequest,
    ) -> Result<()> {
        let project_id = request_metadata.require_project_id(None)?;
        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;
        let events = context.v1_state.events.clone();
        let resource = GrantResource::Namespace {
            warehouse_id,
            namespace_id,
        };

        validate_request_shape(&request)?;
        validate_write_privileges(&authorizer, ResourceType::Namespace, &request)?;
        // Before the gate, and deliberately cannot fail: the authority check carries
        // resolved grantees. See `resolve_grantee_roles`.
        let grantee_roles = resolve_grantee_roles::<C>(&request, catalog_state.clone()).await?;
        let event_ctx = APIEventContext::for_namespace(
            request_metadata.into(),
            events.clone(),
            warehouse_id,
            namespace_id,
            ApplyGrants::of(&request),
        );
        let authz_result = async {
            // Presence only, as at every other level.
            let (warehouse, namespace) = tokio::join!(
                C::get_active_warehouse_by_id(warehouse_id, catalog_state.clone()),
                C::get_namespace_cache_aware(
                    warehouse_id,
                    NamespaceIdentOrId::Id(namespace_id),
                    CachePolicy::Skip,
                    catalog_state.clone()
                )
            );
            let warehouse = authorizer.require_warehouse_presence(warehouse_id, warehouse)?;
            let namespace =
                authorizer.require_namespace_presence(warehouse_id, namespace_id, namespace)?;
            ensure_warehouse_in_project(warehouse_id, &warehouse.project_id, &project_id)?;
            require_grant_authority(
                &authorizer,
                event_ctx.request_metadata(),
                &GrantTarget::Namespace {
                    warehouse: &warehouse,
                    namespace: &namespace,
                },
                &request,
                &grantee_roles,
            )
            .await
        }
        .await;
        let (event_ctx, ()) = event_ctx.emit_authz(authz_result)?;

        // After the gate: before it, the role lookup let an unauthorized caller probe
        // which roles exist in a project, and a catalog failure swallowed the
        // authorization event entirely. Still a 400, not an authorization failure.
        validate_write_principals(&request, &project_id, &grantee_roles)?;

        apply_and_emit::<A, C>(
            &authorizer,
            catalog_state,
            &events,
            event_ctx.request_metadata_arc(),
            &resource,
            &request,
        )
        .await
    }

    /// List the grants held on a tag definition.
    async fn list_tag_grants(
        tag_definition_id: TagDefinitionId,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        query: ListGrantsQuery,
        pagination: PaginationQuery,
    ) -> Result<ListGrantsResponse> {
        let project_id = request_metadata.require_project_id(None)?;
        let principal = query.try_principal()?;
        let is_self = is_self_read(principal.as_ref(), &request_metadata);
        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;

        let required = if is_self {
            CatalogTagAction::Read
        } else {
            CatalogTagAction::ReadGrants
        };
        let mut event_ctx = APIEventContext::for_tag(
            request_metadata.into(),
            context.v1_state.events.clone(),
            tag_definition_id,
            required.clone(),
        );
        event_ctx.push_extra_context("self-read", if is_self { "true" } else { "false" });
        let event_ctx = event_ctx;
        let authz_result = async {
            // Fetched within the request's project, so a definition in another
            // project is simply absent — no separate cross-project check needed.
            let definition =
                C::get_tag_definition(&project_id, tag_definition_id, catalog_state.clone()).await;
            let definition = authorizer.require_tag_presence(tag_definition_id, definition)?;
            // Unlike the other levels, tag resolution has no can-see action folded into
            // it, so a definition the caller cannot see would answer `403` naming the id
            // while an id that does not exist answers `404` — an existence oracle. Mask
            // explicitly, matching `require_*_action` elsewhere — including its rule
            // that the grant-read action doubles as visibility. One batched call: on
            // the self path `required` is `Read`, so the pair decides everything.
            let [can_see, required_ok, read_grants] = authorizer
                .are_allowed_tag_actions_arr(
                    event_ctx.request_metadata(),
                    None,
                    &[
                        (&definition, CatalogTagAction::Read),
                        (&definition, required.clone()),
                        (&definition, CatalogTagAction::ReadGrants),
                    ],
                )
                .await?
                .into_inner();
            if read_grants || (can_see && required_ok) {
                return Ok::<(), AuthZError>(());
            }
            if can_see {
                return Err(RequireTagActionError::from(AuthZTagActionForbidden::new(
                    tag_definition_id,
                    &required,
                ))
                .into());
            }
            Err(
                RequireTagActionError::from(AuthZCannotSeeTag::new_forbidden(tag_definition_id))
                    .into(),
            )
        }
        .await;
        let (event_ctx, ()) = event_ctx.emit_authz(authz_result)?;

        list_and_render::<A, C>(
            &authorizer,
            catalog_state,
            event_ctx.request_metadata(),
            GrantFilter::on(GrantResource::Tag(tag_definition_id), principal),
            pagination,
        )
        .await
    }

    /// Apply a grant diff on a tag definition.
    async fn apply_tag_grants(
        tag_definition_id: TagDefinitionId,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        request: ApplyGrantsRequest,
    ) -> Result<()> {
        let project_id = request_metadata.require_project_id(None)?;
        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;
        let events = context.v1_state.events.clone();
        let resource = GrantResource::Tag(tag_definition_id);

        validate_request_shape(&request)?;
        validate_write_privileges(&authorizer, ResourceType::Tag, &request)?;
        // Before the gate, and deliberately cannot fail: the authority check carries
        // resolved grantees. See `resolve_grantee_roles`.
        let grantee_roles = resolve_grantee_roles::<C>(&request, catalog_state.clone()).await?;
        let event_ctx = APIEventContext::for_tag(
            request_metadata.into(),
            events.clone(),
            tag_definition_id,
            ApplyGrants::of(&request),
        );
        let authz_result = async {
            let definition =
                C::get_tag_definition(&project_id, tag_definition_id, catalog_state.clone()).await;
            let definition = authorizer.require_tag_presence(tag_definition_id, definition)?;
            require_grant_authority(
                &authorizer,
                event_ctx.request_metadata(),
                &GrantTarget::Tag(&definition),
                &request,
                &grantee_roles,
            )
            .await
        }
        .await;
        let (event_ctx, ()) = event_ctx.emit_authz(authz_result)?;

        // After the gate: before it, the role lookup let an unauthorized caller probe
        // which roles exist in a project, and a catalog failure swallowed the
        // authorization event entirely. Still a 400, not an authorization failure.
        validate_write_principals(&request, &project_id, &grantee_roles)?;

        apply_and_emit::<A, C>(
            &authorizer,
            catalog_state,
            &events,
            event_ctx.request_metadata_arc(),
            &resource,
            &request,
        )
        .await
    }

    /// List the grants held on a table.
    async fn list_table_grants(
        warehouse_id: WarehouseId,
        table_id: TableId,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        query: ListGrantsQuery,
        pagination: PaginationQuery,
    ) -> Result<ListGrantsResponse> {
        let project_id = request_metadata.require_project_id(None)?;
        let principal = query.try_principal()?;
        let is_self = is_self_read(principal.as_ref(), &request_metadata);
        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;

        let required = if is_self {
            CatalogTableAction::IncludeInList
        } else {
            CatalogTableAction::ReadGrants
        };
        let mut event_ctx = APIEventContext::for_table(
            request_metadata.into(),
            context.v1_state.events.clone(),
            warehouse_id,
            table_id,
            required.clone(),
        );
        event_ctx.push_extra_context("self-read", if is_self { "true" } else { "false" });
        let event_ctx = event_ctx;
        let authz_result = async {
            let (warehouse, _, _) = authorizer
                .load_and_authorize_table_operation::<C>(
                    event_ctx.request_metadata(),
                    event_ctx.user_provided_entity(),
                    TABULAR_FLAGS,
                    required,
                    catalog_state.clone(),
                )
                .await?;
            ensure_warehouse_in_project(warehouse_id, &warehouse.project_id, &project_id)
        }
        .await;
        let (event_ctx, ()) = event_ctx.emit_authz(authz_result)?;

        list_and_render::<A, C>(
            &authorizer,
            catalog_state,
            event_ctx.request_metadata(),
            GrantFilter::on(
                GrantResource::Table {
                    warehouse_id,
                    table_id,
                },
                principal,
            ),
            pagination,
        )
        .await
    }

    /// Apply a grant diff on a table.
    async fn apply_table_grants(
        warehouse_id: WarehouseId,
        table_id: TableId,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        request: ApplyGrantsRequest,
    ) -> Result<()> {
        let project_id = request_metadata.require_project_id(None)?;
        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;
        let events = context.v1_state.events.clone();
        let resource = GrantResource::Table {
            warehouse_id,
            table_id,
        };

        validate_request_shape(&request)?;
        validate_write_privileges(&authorizer, ResourceType::Table, &request)?;
        // Before the gate, and deliberately cannot fail: the authority check carries
        // resolved grantees. See `resolve_grantee_roles`.
        let grantee_roles = resolve_grantee_roles::<C>(&request, catalog_state.clone()).await?;
        let event_ctx = APIEventContext::for_table(
            request_metadata.into(),
            events.clone(),
            warehouse_id,
            table_id,
            ApplyGrants::of(&request),
        );
        let authz_result = async {
            // Presence only, as at every other level: grant authority is its own
            // right, so requiring a table action too would be a second, wrong gate.
            let (warehouse, namespace, table) =
                crate::service::authz::fetch_warehouse_namespace_table_by_id::<C, A>(
                    &authorizer,
                    warehouse_id,
                    table_id,
                    TABULAR_FLAGS,
                    catalog_state.clone(),
                )
                .await?;
            ensure_warehouse_in_project(warehouse_id, &warehouse.project_id, &project_id)?;
            require_grant_authority(
                &authorizer,
                event_ctx.request_metadata(),
                &GrantTarget::Table {
                    warehouse: &warehouse,
                    namespace: &namespace,
                    table: &table,
                },
                &request,
                &grantee_roles,
            )
            .await
        }
        .await;
        let (event_ctx, ()) = event_ctx.emit_authz(authz_result)?;

        // After the gate: before it, the role lookup let an unauthorized caller probe
        // which roles exist in a project, and a catalog failure swallowed the
        // authorization event entirely. Still a 400, not an authorization failure.
        validate_write_principals(&request, &project_id, &grantee_roles)?;

        apply_and_emit::<A, C>(
            &authorizer,
            catalog_state,
            &events,
            event_ctx.request_metadata_arc(),
            &resource,
            &request,
        )
        .await
    }

    /// List the grants held on a view.
    async fn list_view_grants(
        warehouse_id: WarehouseId,
        view_id: ViewId,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        query: ListGrantsQuery,
        pagination: PaginationQuery,
    ) -> Result<ListGrantsResponse> {
        let project_id = request_metadata.require_project_id(None)?;
        let principal = query.try_principal()?;
        let is_self = is_self_read(principal.as_ref(), &request_metadata);
        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;

        let required = if is_self {
            CatalogViewAction::IncludeInList
        } else {
            CatalogViewAction::ReadGrants
        };
        let mut event_ctx = APIEventContext::for_view(
            request_metadata.into(),
            context.v1_state.events.clone(),
            warehouse_id,
            view_id,
            required.clone(),
        );
        event_ctx.push_extra_context("self-read", if is_self { "true" } else { "false" });
        let event_ctx = event_ctx;
        let authz_result = async {
            let (warehouse, _, _) = authorizer
                .load_and_authorize_view_operation::<C>(
                    event_ctx.request_metadata(),
                    event_ctx.user_provided_entity(),
                    TABULAR_FLAGS,
                    required,
                    catalog_state.clone(),
                )
                .await?;
            ensure_warehouse_in_project(warehouse_id, &warehouse.project_id, &project_id)
        }
        .await;
        let (event_ctx, ()) = event_ctx.emit_authz(authz_result)?;

        list_and_render::<A, C>(
            &authorizer,
            catalog_state,
            event_ctx.request_metadata(),
            GrantFilter::on(
                GrantResource::View {
                    warehouse_id,
                    view_id,
                },
                principal,
            ),
            pagination,
        )
        .await
    }

    /// Apply a grant diff on a view.
    async fn apply_view_grants(
        warehouse_id: WarehouseId,
        view_id: ViewId,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        request: ApplyGrantsRequest,
    ) -> Result<()> {
        let project_id = request_metadata.require_project_id(None)?;
        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;
        let events = context.v1_state.events.clone();
        let resource = GrantResource::View {
            warehouse_id,
            view_id,
        };

        validate_request_shape(&request)?;
        validate_write_privileges(&authorizer, ResourceType::View, &request)?;
        // Before the gate, and deliberately cannot fail: the authority check carries
        // resolved grantees. See `resolve_grantee_roles`.
        let grantee_roles = resolve_grantee_roles::<C>(&request, catalog_state.clone()).await?;
        let event_ctx = APIEventContext::for_view(
            request_metadata.into(),
            events.clone(),
            warehouse_id,
            view_id,
            ApplyGrants::of(&request),
        );
        let authz_result = async {
            let (warehouse, namespace, view) =
                crate::service::authz::fetch_warehouse_namespace_view_by_id::<C, A>(
                    &authorizer,
                    warehouse_id,
                    view_id,
                    TABULAR_FLAGS,
                    catalog_state.clone(),
                )
                .await?;
            ensure_warehouse_in_project(warehouse_id, &warehouse.project_id, &project_id)?;
            require_grant_authority(
                &authorizer,
                event_ctx.request_metadata(),
                &GrantTarget::View {
                    warehouse: &warehouse,
                    namespace: &namespace,
                    view: &view,
                },
                &request,
                &grantee_roles,
            )
            .await
        }
        .await;
        let (event_ctx, ()) = event_ctx.emit_authz(authz_result)?;

        // After the gate: before it, the role lookup let an unauthorized caller probe
        // which roles exist in a project, and a catalog failure swallowed the
        // authorization event entirely. Still a 400, not an authorization failure.
        validate_write_principals(&request, &project_id, &grantee_roles)?;

        apply_and_emit::<A, C>(
            &authorizer,
            catalog_state,
            &events,
            event_ctx.request_metadata_arc(),
            &resource,
            &request,
        )
        .await
    }

    /// List the grants held on a generic table.
    async fn list_generic_table_grants(
        warehouse_id: WarehouseId,
        generic_table_id: GenericTableId,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        query: ListGrantsQuery,
        pagination: PaginationQuery,
    ) -> Result<ListGrantsResponse> {
        let project_id = request_metadata.require_project_id(None)?;
        let principal = query.try_principal()?;
        let is_self = is_self_read(principal.as_ref(), &request_metadata);
        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;

        let required = if is_self {
            CatalogGenericTableAction::IncludeInList
        } else {
            CatalogGenericTableAction::ReadGrants
        };
        let mut event_ctx = APIEventContext::for_generic_table(
            request_metadata.into(),
            context.v1_state.events.clone(),
            warehouse_id,
            generic_table_id,
            required.clone(),
        );
        event_ctx.push_extra_context("self-read", if is_self { "true" } else { "false" });
        let event_ctx = event_ctx;
        let authz_result = async {
            let (warehouse, _, _) = authorizer
                .load_and_authorize_generic_table_operation::<C>(
                    event_ctx.request_metadata(),
                    event_ctx.user_provided_entity(),
                    TABULAR_FLAGS,
                    required,
                    catalog_state.clone(),
                )
                .await?;
            ensure_warehouse_in_project(warehouse_id, &warehouse.project_id, &project_id)
        }
        .await;
        let (event_ctx, ()) = event_ctx.emit_authz(authz_result)?;

        list_and_render::<A, C>(
            &authorizer,
            catalog_state,
            event_ctx.request_metadata(),
            GrantFilter::on(
                GrantResource::GenericTable {
                    warehouse_id,
                    generic_table_id,
                },
                principal,
            ),
            pagination,
        )
        .await
    }

    /// Apply a grant diff on a generic table.
    async fn apply_generic_table_grants(
        warehouse_id: WarehouseId,
        generic_table_id: GenericTableId,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        request: ApplyGrantsRequest,
    ) -> Result<()> {
        let project_id = request_metadata.require_project_id(None)?;
        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;
        let events = context.v1_state.events.clone();
        let resource = GrantResource::GenericTable {
            warehouse_id,
            generic_table_id,
        };

        validate_request_shape(&request)?;
        validate_write_privileges(&authorizer, ResourceType::GenericTable, &request)?;
        // Before the gate, and deliberately cannot fail: the authority check carries
        // resolved grantees. See `resolve_grantee_roles`.
        let grantee_roles = resolve_grantee_roles::<C>(&request, catalog_state.clone()).await?;
        let event_ctx = APIEventContext::for_generic_table(
            request_metadata.into(),
            events.clone(),
            warehouse_id,
            generic_table_id,
            ApplyGrants::of(&request),
        );
        let authz_result = async {
            let (warehouse, namespace, generic_table) =
                crate::service::authz::fetch_warehouse_namespace_generic_table_by_id::<C, A>(
                    &authorizer,
                    warehouse_id,
                    generic_table_id,
                    TABULAR_FLAGS,
                    catalog_state.clone(),
                )
                .await?;
            ensure_warehouse_in_project(warehouse_id, &warehouse.project_id, &project_id)?;
            require_grant_authority(
                &authorizer,
                event_ctx.request_metadata(),
                &GrantTarget::GenericTable {
                    warehouse: &warehouse,
                    namespace: &namespace,
                    generic_table: &generic_table,
                },
                &request,
                &grantee_roles,
            )
            .await
        }
        .await;
        let (event_ctx, ()) = event_ctx.emit_authz(authz_result)?;

        // After the gate: before it, the role lookup let an unauthorized caller probe
        // which roles exist in a project, and a catalog failure swallowed the
        // authorization event entirely. Still a 400, not an authorization failure.
        validate_write_principals(&request, &project_id, &grantee_roles)?;

        apply_and_emit::<A, C>(
            &authorizer,
            catalog_state,
            &events,
            event_ctx.request_metadata_arc(),
            &resource,
            &request,
        )
        .await
    }

    /// Apply a grant diff on a warehouse.
    async fn apply_warehouse_grants(
        warehouse_id: WarehouseId,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        request: ApplyGrantsRequest,
    ) -> Result<()> {
        let project_id = request_metadata.require_project_id(None)?;
        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;
        let events = context.v1_state.events.clone();
        let resource = GrantResource::Warehouse(warehouse_id);

        // (1) Request validation: a malformed diff is a 400, never a denial.
        validate_request_shape(&request)?;
        validate_write_privileges(&authorizer, ResourceType::Warehouse, &request)?;
        // Before the gate, and deliberately cannot fail: the authority check carries
        // resolved grantees. See `resolve_grantee_roles`.
        let grantee_roles = resolve_grantee_roles::<C>(&request, catalog_state.clone()).await?;
        // (2) Authorization, folded into one result so the audit event is faithful.
        let event_ctx = APIEventContext::for_warehouse(
            request_metadata.into(),
            events.clone(),
            warehouse_id,
            ApplyGrants::of(&request),
        );
        let warehouse = C::get_warehouse_by_id_cache_aware(
            warehouse_id,
            WarehouseStatus::active_and_inactive(),
            CachePolicy::Skip,
            catalog_state.clone(),
        )
        .await;
        let authz_result = async {
            // Presence only, deliberately: grant authority is independent of visibility.
            // `security_admin` holds `manage_grants` on every warehouse in its project
            // without holding `describe`, so requiring a can-see action here would lock
            // the role built to manage grants out of managing them. A caller who cannot
            // see the warehouse and has no authority on it is refused rather than told
            // it does not exist — see the module docs on what that discloses.
            let resolved = authorizer.require_warehouse_presence(warehouse_id, warehouse)?;
            ensure_warehouse_in_project(warehouse_id, &resolved.project_id, &project_id)?;
            require_grant_authority(
                &authorizer,
                event_ctx.request_metadata(),
                &GrantTarget::Warehouse(&resolved),
                &request,
                &grantee_roles,
            )
            .await
        }
        .await;
        let (event_ctx, ()) = event_ctx.emit_authz(authz_result)?;

        // After the gate: before it, the role lookup let an unauthorized caller probe
        // which roles exist in a project, and a catalog failure swallowed the
        // authorization event entirely. Still a 400, not an authorization failure.
        validate_write_principals(&request, &project_id, &grantee_roles)?;

        // (3)+(4) Write, then emit — after the authorization event, so a storage
        // failure is not recorded as a denial.
        apply_and_emit::<A, C>(
            &authorizer,
            catalog_state,
            &events,
            event_ctx.request_metadata_arc(),
            &resource,
            &request,
        )
        .await
    }
    /// Which server privileges the caller may administer.
    async fn get_server_grantable_privileges(
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        query: GetGrantAccessQuery,
    ) -> Result<ResourceGrantablePrivilegesResponse> {
        let for_user_api = query.try_principal()?;
        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;

        let mut event_ctx = APIEventContext::for_server(
            request_metadata.into(),
            context.v1_state.events.clone(),
            IntrospectPermissions {},
            authorizer.server_id(),
        );
        set_for_user(&mut event_ctx, for_user_api.as_ref());
        let event_ctx = event_ctx;

        let authz_result = async {
            let for_user = resolve_principal::<C>(for_user_api, catalog_state).await?;
            if grant_read_is_delegated(for_user.as_ref(), event_ctx.request_metadata()) {
                authorizer
                    .require_server_action(
                        event_ctx.request_metadata(),
                        None,
                        CatalogServerAction::ReadGrants,
                    )
                    .await?;
            }
            allowed_privileges(
                &authorizer,
                event_ctx.request_metadata(),
                for_user,
                &GrantTarget::Server,
            )
            .await
        }
        .await;
        let (_event_ctx, allowed) = event_ctx.emit_authz(authz_result)?;
        Ok(ResourceGrantablePrivilegesResponse {
            privileges: allowed,
        })
    }

    /// Which project privileges the caller may administer in the request's project.
    async fn get_project_grantable_privileges(
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        query: GetGrantAccessQuery,
    ) -> Result<ResourceGrantablePrivilegesResponse> {
        let project_id = request_metadata.require_project_id(None)?;
        let for_user_api = query.try_principal()?;
        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;

        let mut event_ctx = APIEventContext::for_project(
            request_metadata.into(),
            context.v1_state.events.clone(),
            (*project_id).clone(),
            IntrospectPermissions {},
        );
        set_for_user(&mut event_ctx, for_user_api.as_ref());
        let event_ctx = event_ctx;

        let authz_result = async {
            let for_user = resolve_principal::<C>(for_user_api, catalog_state).await?;
            if grant_read_is_delegated(for_user.as_ref(), event_ctx.request_metadata()) {
                authorizer
                    .require_project_action(
                        event_ctx.request_metadata(),
                        &project_id,
                        CatalogProjectAction::ReadGrants,
                    )
                    .await?;
            }
            allowed_privileges(
                &authorizer,
                event_ctx.request_metadata(),
                for_user,
                &GrantTarget::Project(&project_id),
            )
            .await
        }
        .await;
        let (_event_ctx, allowed) = event_ctx.emit_authz(authz_result)?;
        Ok(ResourceGrantablePrivilegesResponse {
            privileges: allowed,
        })
    }

    /// Which warehouse privileges the caller may administer on a warehouse.
    async fn get_warehouse_grantable_privileges(
        warehouse_id: WarehouseId,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        query: GetGrantAccessQuery,
    ) -> Result<ResourceGrantablePrivilegesResponse> {
        let project_id = request_metadata.require_project_id(None)?;
        let for_user_api = query.try_principal()?;
        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;

        let mut event_ctx = APIEventContext::for_warehouse(
            request_metadata.into(),
            context.v1_state.events.clone(),
            warehouse_id,
            IntrospectPermissions {},
        );
        set_for_user(&mut event_ctx, for_user_api.as_ref());
        let event_ctx = event_ctx;

        let warehouse = C::get_warehouse_by_id_cache_aware(
            warehouse_id,
            WarehouseStatus::active_and_inactive(),
            CachePolicy::Use,
            catalog_state.clone(),
        )
        .await;
        let authz_result = async {
            // Presence only, as on the apply path: grant authority is its own right.
            let resolved = authorizer.require_warehouse_presence(warehouse_id, warehouse)?;
            ensure_warehouse_in_project(warehouse_id, &resolved.project_id, &project_id)?;
            let for_user = resolve_principal::<C>(for_user_api, catalog_state).await?;
            if grant_read_is_delegated(for_user.as_ref(), event_ctx.request_metadata()) {
                authorizer
                    .require_warehouse_action(
                        event_ctx.request_metadata(),
                        warehouse_id,
                        Ok(Some(resolved.clone())),
                        CatalogWarehouseAction::ReadGrants,
                    )
                    .await?;
            }
            allowed_privileges(
                &authorizer,
                event_ctx.request_metadata(),
                for_user,
                &GrantTarget::Warehouse(&resolved),
            )
            .await
        }
        .await;
        let (_event_ctx, allowed) = event_ctx.emit_authz(authz_result)?;
        Ok(ResourceGrantablePrivilegesResponse {
            privileges: allowed,
        })
    }

    /// Which namespace privileges the caller may administer on a namespace.
    async fn get_namespace_grantable_privileges(
        warehouse_id: WarehouseId,
        namespace_id: NamespaceId,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        query: GetGrantAccessQuery,
    ) -> Result<ResourceGrantablePrivilegesResponse> {
        let project_id = request_metadata.require_project_id(None)?;
        let for_user_api = query.try_principal()?;
        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;

        let mut event_ctx = APIEventContext::for_namespace(
            request_metadata.into(),
            context.v1_state.events.clone(),
            warehouse_id,
            namespace_id,
            IntrospectPermissions {},
        );
        set_for_user(&mut event_ctx, for_user_api.as_ref());
        let event_ctx = event_ctx;

        let authz_result = async {
            let (warehouse, namespace) = tokio::join!(
                C::get_active_warehouse_by_id(warehouse_id, catalog_state.clone()),
                C::get_namespace_cache_aware(
                    warehouse_id,
                    NamespaceIdentOrId::Id(namespace_id),
                    CachePolicy::Use,
                    catalog_state.clone()
                )
            );
            let warehouse = authorizer.require_warehouse_presence(warehouse_id, warehouse)?;
            let namespace =
                authorizer.require_namespace_presence(warehouse_id, namespace_id, namespace)?;
            ensure_warehouse_in_project(warehouse_id, &warehouse.project_id, &project_id)?;
            let for_user = resolve_principal::<C>(for_user_api, catalog_state.clone()).await?;
            if grant_read_is_delegated(for_user.as_ref(), event_ctx.request_metadata()) {
                authorizer
                    .load_and_authorize_namespace_action::<C>(
                        event_ctx.request_metadata(),
                        event_ctx.user_provided_entity().clone(),
                        CatalogNamespaceAction::ReadGrants,
                        CachePolicy::Use,
                        catalog_state.clone(),
                    )
                    .await?;
            }
            allowed_privileges(
                &authorizer,
                event_ctx.request_metadata(),
                for_user,
                &GrantTarget::Namespace {
                    warehouse: &warehouse,
                    namespace: &namespace,
                },
            )
            .await
        }
        .await;
        let (_event_ctx, allowed) = event_ctx.emit_authz(authz_result)?;
        Ok(ResourceGrantablePrivilegesResponse {
            privileges: allowed,
        })
    }

    /// Which table privileges the caller may administer on a table.
    async fn get_table_grantable_privileges(
        warehouse_id: WarehouseId,
        table_id: TableId,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        query: GetGrantAccessQuery,
    ) -> Result<ResourceGrantablePrivilegesResponse> {
        let project_id = request_metadata.require_project_id(None)?;
        let for_user_api = query.try_principal()?;
        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;

        let mut event_ctx = APIEventContext::for_table(
            request_metadata.into(),
            context.v1_state.events.clone(),
            warehouse_id,
            table_id,
            IntrospectPermissions {},
        );
        set_for_user(&mut event_ctx, for_user_api.as_ref());
        let event_ctx = event_ctx;

        let authz_result = async {
            let (warehouse, namespace, table) =
                crate::service::authz::fetch_warehouse_namespace_table_by_id::<C, A>(
                    &authorizer,
                    warehouse_id,
                    table_id,
                    TABULAR_FLAGS,
                    catalog_state.clone(),
                )
                .await?;
            ensure_warehouse_in_project(warehouse_id, &warehouse.project_id, &project_id)?;
            let for_user = resolve_principal::<C>(for_user_api, catalog_state.clone()).await?;
            if grant_read_is_delegated(for_user.as_ref(), event_ctx.request_metadata()) {
                authorizer
                    .load_and_authorize_table_operation::<C>(
                        event_ctx.request_metadata(),
                        event_ctx.user_provided_entity(),
                        TABULAR_FLAGS,
                        CatalogTableAction::ReadGrants,
                        catalog_state.clone(),
                    )
                    .await?;
            }
            allowed_privileges(
                &authorizer,
                event_ctx.request_metadata(),
                for_user,
                &GrantTarget::Table {
                    warehouse: &warehouse,
                    namespace: &namespace,
                    table: &table,
                },
            )
            .await
        }
        .await;
        let (_event_ctx, allowed) = event_ctx.emit_authz(authz_result)?;
        Ok(ResourceGrantablePrivilegesResponse {
            privileges: allowed,
        })
    }

    /// Which view privileges the caller may administer on a view.
    async fn get_view_grantable_privileges(
        warehouse_id: WarehouseId,
        view_id: ViewId,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        query: GetGrantAccessQuery,
    ) -> Result<ResourceGrantablePrivilegesResponse> {
        let project_id = request_metadata.require_project_id(None)?;
        let for_user_api = query.try_principal()?;
        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;

        let mut event_ctx = APIEventContext::for_view(
            request_metadata.into(),
            context.v1_state.events.clone(),
            warehouse_id,
            view_id,
            IntrospectPermissions {},
        );
        set_for_user(&mut event_ctx, for_user_api.as_ref());
        let event_ctx = event_ctx;

        let authz_result = async {
            let (warehouse, namespace, view) =
                crate::service::authz::fetch_warehouse_namespace_view_by_id::<C, A>(
                    &authorizer,
                    warehouse_id,
                    view_id,
                    TABULAR_FLAGS,
                    catalog_state.clone(),
                )
                .await?;
            ensure_warehouse_in_project(warehouse_id, &warehouse.project_id, &project_id)?;
            let for_user = resolve_principal::<C>(for_user_api, catalog_state.clone()).await?;
            if grant_read_is_delegated(for_user.as_ref(), event_ctx.request_metadata()) {
                authorizer
                    .load_and_authorize_view_operation::<C>(
                        event_ctx.request_metadata(),
                        event_ctx.user_provided_entity(),
                        TABULAR_FLAGS,
                        CatalogViewAction::ReadGrants,
                        catalog_state.clone(),
                    )
                    .await?;
            }
            allowed_privileges(
                &authorizer,
                event_ctx.request_metadata(),
                for_user,
                &GrantTarget::View {
                    warehouse: &warehouse,
                    namespace: &namespace,
                    view: &view,
                },
            )
            .await
        }
        .await;
        let (_event_ctx, allowed) = event_ctx.emit_authz(authz_result)?;
        Ok(ResourceGrantablePrivilegesResponse {
            privileges: allowed,
        })
    }

    /// Which generic-table privileges the caller may administer on a generic table.
    async fn get_generic_table_grantable_privileges(
        warehouse_id: WarehouseId,
        generic_table_id: GenericTableId,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        query: GetGrantAccessQuery,
    ) -> Result<ResourceGrantablePrivilegesResponse> {
        let project_id = request_metadata.require_project_id(None)?;
        let for_user_api = query.try_principal()?;
        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;

        let mut event_ctx = APIEventContext::for_generic_table(
            request_metadata.into(),
            context.v1_state.events.clone(),
            warehouse_id,
            generic_table_id,
            IntrospectPermissions {},
        );
        set_for_user(&mut event_ctx, for_user_api.as_ref());
        let event_ctx = event_ctx;

        let authz_result = async {
            let (warehouse, namespace, generic_table) =
                crate::service::authz::fetch_warehouse_namespace_generic_table_by_id::<C, A>(
                    &authorizer,
                    warehouse_id,
                    generic_table_id,
                    TABULAR_FLAGS,
                    catalog_state.clone(),
                )
                .await?;
            ensure_warehouse_in_project(warehouse_id, &warehouse.project_id, &project_id)?;
            let for_user = resolve_principal::<C>(for_user_api, catalog_state.clone()).await?;
            if grant_read_is_delegated(for_user.as_ref(), event_ctx.request_metadata()) {
                authorizer
                    .load_and_authorize_generic_table_operation::<C>(
                        event_ctx.request_metadata(),
                        event_ctx.user_provided_entity(),
                        TABULAR_FLAGS,
                        CatalogGenericTableAction::ReadGrants,
                        catalog_state.clone(),
                    )
                    .await?;
            }
            allowed_privileges(
                &authorizer,
                event_ctx.request_metadata(),
                for_user,
                &GrantTarget::GenericTable {
                    warehouse: &warehouse,
                    namespace: &namespace,
                    generic_table: &generic_table,
                },
            )
            .await
        }
        .await;
        let (_event_ctx, allowed) = event_ctx.emit_authz(authz_result)?;
        Ok(ResourceGrantablePrivilegesResponse {
            privileges: allowed,
        })
    }

    /// Which tag privileges the caller may administer on a tag definition.
    async fn get_tag_grantable_privileges(
        tag_definition_id: TagDefinitionId,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        query: GetGrantAccessQuery,
    ) -> Result<ResourceGrantablePrivilegesResponse> {
        let project_id = request_metadata.require_project_id(None)?;
        let for_user_api = query.try_principal()?;
        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;

        let mut event_ctx = APIEventContext::for_tag(
            request_metadata.into(),
            context.v1_state.events.clone(),
            tag_definition_id,
            IntrospectPermissions {},
        );
        set_for_user(&mut event_ctx, for_user_api.as_ref());
        let event_ctx = event_ctx;

        let authz_result = async {
            let definition =
                C::get_tag_definition(&project_id, tag_definition_id, catalog_state.clone()).await;
            let definition = authorizer.require_tag_presence(tag_definition_id, definition)?;
            let for_user = resolve_principal::<C>(for_user_api, catalog_state).await?;
            if grant_read_is_delegated(for_user.as_ref(), event_ctx.request_metadata()) {
                authorizer
                    .require_tag_action(
                        event_ctx.request_metadata(),
                        tag_definition_id,
                        Ok(Some(definition.clone())),
                        CatalogTagAction::ReadGrants,
                    )
                    .await?;
            }
            allowed_privileges(
                &authorizer,
                event_ctx.request_metadata(),
                for_user,
                &GrantTarget::Tag(&definition),
            )
            .await
        }
        .await;
        let (_event_ctx, allowed) = event_ctx.emit_authz(authz_result)?;
        Ok(ResourceGrantablePrivilegesResponse {
            privileges: allowed,
        })
    }
}

impl<C: CatalogStore, A: Authorizer, S: SecretStore> Service<C, A, S> for ApiServer<C, A, S> {}

#[cfg(test)]
mod resource_type_wire_agreement {
    use super::*;
    use crate::service::{
        GenericTableId, NamespaceId, TableId, TagDefinitionId, ViewId, WarehouseId,
        authz::ResourceType,
    };

    /// A listed grant's `type` must be the `ResourceType` spelling, which is also the
    /// URL segment. Checked for every variant rather than a sample: `kebab-case` happens
    /// to agree for seven of the eight, so a spot check would have missed the one that
    /// does not, and a new variant with an acronym or a digit could diverge silently.
    #[test]
    fn every_resource_response_names_its_resource_type() {
        let uuid = uuid::Uuid::nil();
        let cases: Vec<(GrantResourceResponse, ResourceType)> = vec![
            (GrantResourceResponse::Server, ResourceType::Server),
            (
                GrantResourceResponse::Project {
                    project_id: ProjectId::from(uuid),
                },
                ResourceType::Project,
            ),
            (
                GrantResourceResponse::Warehouse {
                    warehouse_id: WarehouseId::new(uuid),
                },
                ResourceType::Warehouse,
            ),
            (
                GrantResourceResponse::Namespace {
                    warehouse_id: WarehouseId::new(uuid),
                    namespace_id: NamespaceId::new(uuid),
                },
                ResourceType::Namespace,
            ),
            (
                GrantResourceResponse::Table {
                    warehouse_id: WarehouseId::new(uuid),
                    table_id: TableId::from(uuid),
                },
                ResourceType::Table,
            ),
            (
                GrantResourceResponse::View {
                    warehouse_id: WarehouseId::new(uuid),
                    view_id: ViewId::from(uuid),
                },
                ResourceType::View,
            ),
            (
                GrantResourceResponse::GenericTable {
                    warehouse_id: WarehouseId::new(uuid),
                    generic_table_id: GenericTableId::from(uuid),
                },
                ResourceType::GenericTable,
            ),
            (
                GrantResourceResponse::Tag {
                    tag_definition_id: TagDefinitionId::new(uuid),
                },
                ResourceType::Tag,
            ),
        ];
        assert_eq!(
            cases.len(),
            <ResourceType as strum::VariantArray>::VARIANTS.len(),
            "every resource type needs a case here"
        );
        for (response, resource_type) in cases {
            let json = serde_json::to_value(&response).expect("serializes");
            assert_eq!(
                json.get("type").and_then(serde_json::Value::as_str),
                Some(resource_type.as_str()),
                "`{response:?}` must report the `{}` spelling",
                resource_type.as_str()
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        api::management::v1::{RoleId, check::RoleAssignee},
        request_metadata::RequestMetadataTestBuilder,
        service::{
            authz::{AllowAllAuthorizer, tests::HidingAuthorizer},
            events::types::authorization::AuthorizationFailureSource,
        },
    };

    /// A caller-supplied limit is clamped, never rejected: the bound is the server's, and
    /// a request that asks for more is answered with what the server will do.
    #[test]
    fn a_revoke_limit_is_clamped_to_the_bound() {
        let with = |limit: Option<usize>| {
            revoke_limit(&RevokeSubtreeGrantsRequest {
                principal: None,
                privilege: Vec::new(),
                resource_type: Vec::new(),
                created_before: None,
                limit,
                allow_partial: false,
                include_root_level: true,
                dry_run: false,
            })
        };
        assert_eq!(with(None), MAX_GRANTS_PER_SUBTREE_REVOKE);
        assert_eq!(with(Some(0)), 1);
        assert_eq!(with(Some(10)), 10);
        assert_eq!(with(Some(999_999)), MAX_GRANTS_PER_SUBTREE_REVOKE);
    }

    /// A filter naming a privilege no kind under the root publishes is a typo, and
    /// answering `200` with nothing would read as "that access is gone".
    #[test]
    fn a_filter_privilege_outside_the_vocabulary_is_refused() {
        let authorizer = AllowAllAuthorizer::default();
        let known = authorizer.grantable_privileges(ResourceType::Table)[0]
            .name
            .clone();

        assert!(validate_filter_privileges(&authorizer, NAMESPACE_SUBTREE_KINDS, &[known]).is_ok());

        let err = validate_filter_privileges(
            &authorizer,
            NAMESPACE_SUBTREE_KINDS,
            &["not_a_privilege".to_string()],
        )
        .unwrap_err();
        assert_eq!(err.error.code, 400);
        assert_eq!(err.error.r#type, "UnknownGrantPrivilege");
    }

    fn alice() -> UserOrRole {
        UserOrRole::User(UserId::try_from("oidc~alice").expect("valid test user id"))
    }

    fn entry(privilege: &str, principal: UserOrRole) -> GrantEntry {
        GrantEntry {
            privilege: privilege.to_string(),
            principal,
        }
    }

    fn write_request(privileges: &[&str]) -> ApplyGrantsRequest {
        ApplyGrantsRequest {
            writes: privileges
                .iter()
                .map(|privilege| entry(privilege, alice()))
                .collect(),
            deletes: Vec::new(),
        }
    }

    /// A refused apply emits no per-grant event, so this action is the only record of
    /// it. Naming the privilege but not its intended holder would leave an attempted
    /// escalation unattributable to the principal who would have benefited.
    #[test]
    fn the_audit_action_names_the_principals_the_grant_was_destined_for() {
        let role_id = RoleId::new_random();
        let request = ApplyGrantsRequest {
            writes: vec![GrantEntry {
                privilege: "modify".to_string(),
                principal: UserOrRole::User(
                    UserId::try_from("oidc~alice").expect("valid test user id"),
                ),
            }],
            deletes: vec![GrantEntry {
                privilege: "modify".to_string(),
                principal: UserOrRole::Role(RoleAssignee::from_role(role_id)),
            }],
        };

        let actions = ApplyGrants::of(&request).event_actions();
        let [action] = actions.as_slice() else {
            panic!("apply emits exactly one action descriptor, got {actions:?}");
        };
        assert_eq!(action.action_name, "apply_grants");

        let context: std::collections::HashMap<&str, String> = action
            .context
            .iter()
            .map(|(key, value)| (*key, value.to_string()))
            .collect();
        assert_eq!(
            context["principals"],
            format!("[role:{role_id}, user:oidc~alice]")
        );
        // The privilege is shared by both entries, so dedup collapses it while the
        // counts still distinguish the grant from the revoke.
        assert_eq!(context["privileges"], "[modify]");
        assert_eq!(context["writes"], "1");
        assert_eq!(context["deletes"], "1");
    }

    #[tokio::test]
    async fn a_denied_privilege_makes_the_write_gate_forbid() {
        // The gate every apply handler funnels through. `HidingAuthorizer` opts out of
        // the grant surface, so `are_allowed_grants` takes the fail-closed default.
        let authorizer = HidingAuthorizer::new();
        let metadata = RequestMetadataTestBuilder::builder().build();
        let warehouse = crate::service::ResolvedWarehouse::new_random();

        let err = require_grant_authority(
            &authorizer,
            &metadata,
            &GrantTarget::Warehouse(&warehouse),
            &write_request(&["get_metadata"]),
            &HashMap::new(),
        )
        .await
        .expect_err("a deny-all authorizer must not confer grant authority");

        let err = err.into_error_model();
        assert_eq!(err.code, 403);
        assert_eq!(err.r#type, "GrantActionForbidden");
        assert_eq!(
            err.message,
            "Granting `get_metadata` on this warehouse is forbidden"
        );
    }

    #[tokio::test]
    async fn the_write_gate_names_every_denied_privilege() {
        // One decision per privilege, in order: the error must name each privilege
        // that was actually refused, in request order, so one round trip surfaces
        // everything the caller must remove — matching the validation errors.
        let authorizer = HidingAuthorizer::new();
        let metadata = RequestMetadataTestBuilder::builder().build();

        let err = require_grant_authority(
            &authorizer,
            &metadata,
            &GrantTarget::Server,
            &write_request(&["provision_users", "list_users"]),
            &HashMap::new(),
        )
        .await
        .expect_err("a deny-all authorizer must not confer grant authority")
        .into_error_model();

        assert_eq!(
            err.message,
            "Granting `provision_users`, `list_users` on this server is forbidden"
        );
    }

    #[test]
    fn every_entry_asks_its_own_question_tagged_with_its_side() {
        // One question per entry, writes first, each carrying the direction its side
        // implies. Nothing collapses: an authorizer may answer differently depending on who
        // receives the privilege and on which way it moves.
        //
        // A role grantee arrives resolved, so an authorizer can read its identity rather
        // than an id it cannot look up. The second role is deliberately left out of the
        // resolution: an unresolvable grantee asks without one rather than inventing a
        // role, and a write naming it is refused after the gate.
        let resolved = Arc::new(crate::service::Role::new_random());
        let resolved_id = resolved.id();
        let unresolved_id = RoleId::new_random();
        let request = ApplyGrantsRequest {
            writes: vec![
                entry("modify", alice()),
                entry(
                    "select",
                    UserOrRole::Role(RoleAssignee::from_role(unresolved_id)),
                ),
            ],
            deletes: vec![entry(
                "modify",
                UserOrRole::Role(RoleAssignee::from_role(resolved_id)),
            )],
        };
        let grantee_roles = HashMap::from([(resolved_id, resolved.clone())]);

        let alice_grantee =
            AuthzUserOrRole::User(UserId::try_from("oidc~alice").expect("valid test user id"));
        assert_eq!(
            authority_questions(&request, &grantee_roles),
            vec![
                (Some(alice_grantee), "modify", GrantOp::Grant),
                (None, "select", GrantOp::Grant),
                (
                    Some(AuthzUserOrRole::Role(AuthzRoleAssignee::from_role(
                        resolved
                    ))),
                    "modify",
                    GrantOp::Revoke
                ),
            ]
        );
    }

    /// The direction has to reach the authorizer, or separating the two is a fiction: this
    /// authorizer has authority over revokes only, so the grant in the same diff is the one
    /// refused — and the refusal says which.
    ///
    /// Fails if the apply path stopped passing the direction (both entries would be
    /// refused), or if the sides were tagged the wrong way round (the revoke would be).
    #[tokio::test]
    async fn the_gate_asks_with_the_direction_each_entry_moves() {
        let authorizer = HidingAuthorizer::new().with_grant_authority(&[GrantOp::Revoke]);
        let metadata = RequestMetadataTestBuilder::builder().build();
        let request = ApplyGrantsRequest {
            writes: vec![entry("select", alice())],
            deletes: vec![entry("modify", alice())],
        };

        let err = require_grant_authority(
            &authorizer,
            &metadata,
            &GrantTarget::Server,
            &request,
            &HashMap::new(),
        )
        .await
        .expect_err("granting is not this authorizer's to allow")
        .into_error_model();
        assert_eq!(err.message, "Granting `select` on this server is forbidden");
    }

    #[test]
    fn a_refusal_reports_every_refused_question_with_its_direction() {
        // Mixed decisions, which no authorizer in this workspace produces: the answers are
        // positional, so a pairing that slipped by one would report a question the
        // authorizer allowed - and stay invisible to every all-deny or all-allow test.
        let alice =
            AuthzUserOrRole::User(UserId::try_from("oidc~alice").expect("valid test user id"));
        let role = AuthzUserOrRole::Role(AuthzRoleAssignee::from_role(Arc::new(
            crate::service::Role::new_random(),
        )));
        let questions = vec![
            (Some(alice.clone()), "select", GrantOp::Grant),
            (Some(role), "select", GrantOp::Grant),
            (Some(alice), "modify", GrantOp::Revoke),
        ];

        // `select` allowed for alice but refused for the role: reported from the position
        // of the refusal rather than of the first request for it, and the revoke of
        // `modify` keeps its direction so the message can name it.
        assert_eq!(
            refused_privileges(
                &questions,
                &[
                    AuthorizationDecision::allow(),
                    AuthorizationDecision::deny(),
                    AuthorizationDecision::deny(),
                ]
            ),
            vec![(GrantOp::Grant, "select"), (GrantOp::Revoke, "modify")]
        );
        // Only the first question refused, so `modify` must not be reported.
        assert_eq!(
            refused_privileges(
                &questions,
                &[
                    AuthorizationDecision::deny(),
                    AuthorizationDecision::allow(),
                    AuthorizationDecision::allow(),
                ]
            ),
            vec![(GrantOp::Grant, "select")]
        );
    }

    #[tokio::test]
    async fn the_write_gate_names_a_privilege_refused_for_several_principals_once() {
        // The gate asks per pair, but the message speaks about privileges: one name
        // refused for two principals is named once, and still in request order.
        let authorizer = HidingAuthorizer::new();
        let metadata = RequestMetadataTestBuilder::builder().build();
        let role = UserOrRole::Role(RoleAssignee::from_role(RoleId::new_random()));

        let err = require_grant_authority(
            &authorizer,
            &metadata,
            &GrantTarget::Server,
            &ApplyGrantsRequest {
                writes: vec![
                    entry("provision_users", alice()),
                    entry("provision_users", role),
                    entry("list_users", alice()),
                ],
                deletes: Vec::new(),
            },
            &HashMap::new(),
        )
        .await
        .expect_err("a deny-all authorizer must not confer grant authority")
        .into_error_model();

        assert_eq!(
            err.message,
            "Granting `provision_users`, `list_users` on this server is forbidden"
        );
    }

    #[tokio::test]
    async fn the_write_gate_passes_when_every_privilege_is_allowed() {
        // The control case: the same gate, the same request, an allow-all authorizer.
        let authorizer = AllowAllAuthorizer::default();
        let metadata = RequestMetadataTestBuilder::builder().build();
        let warehouse = crate::service::ResolvedWarehouse::new_random();

        require_grant_authority(
            &authorizer,
            &metadata,
            &GrantTarget::Warehouse(&warehouse),
            &write_request(&["get_metadata", "list_namespaces"]),
            &HashMap::new(),
        )
        .await
        .expect("allow-all confers grant authority on every privilege");
    }
}
