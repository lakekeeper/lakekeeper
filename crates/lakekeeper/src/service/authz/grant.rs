//! Grants: direct `principal -> privilege -> resource` permissions.
//!
//! A grant is the enumerable, click-driven counterpart to expression-based policy.
//! Two things about it are owned by the authorizer, not by this layer:
//!
//! * **Storage.** [`Authorizer::grants`](super::Authorizer::grants) returns `Some`
//!   when the authorizer keeps grant edges in its own store, and `None` when the
//!   catalog's `grant_assignment` table is authoritative — the same split as
//!   [`ManagesRoleAssignments`](super::ManagesRoleAssignments).
//! * **Vocabulary.** Each authorizer publishes its own closed set of grantable
//!   privileges per resource type. The catalog stores the privilege *by name* and
//!   never interprets it, which is what lets the API be uniform while the
//!   privilege sets differ.
//!
//! A grant's identity is the `(principal, privilege, resource)` triple. There is no
//! grant id: an authorizer-managed store cannot mint one, and revocation is
//! expressed as a diff of triples, so nothing needs one.
//!
//! Grants do **not** inherit. A grant held by a role is the role's grant, and a
//! grant on a parent resource is a grant on the parent. Resolving what a principal
//! may *effectively* do is a separate question answered by the action-check API.

use std::fmt::Debug;

use chrono::{DateTime, Utc};
use iceberg_ext::catalog::rest::ErrorModel;
use serde::{Deserialize, Serialize};

use super::{
    AuthorizationBackendUnavailable, AuthorizationCountMismatch, AuthorizationDecision, Authorizer,
    IsAllowedActionError, UserOrRole, UserOrRoleId,
};
use crate::{
    api::{RequestMetadata, iceberg::v1::PaginationQuery},
    service::{
        GenericTableId, NamespaceId, ProjectId, TableId, TagDefinitionId, ViewId, WarehouseId,
        events::types::authorization::{AuthorizationFailureReason, AuthorizationFailureSource},
    },
};

/// The kinds of resource a grant can be held on.
///
/// Every value is also the URL segment that addresses that kind of resource, so a
/// client can build request paths straight from a vocabulary response. Kept link-free:
/// this doc comment is published verbatim in the `OpenAPI` description, where an
/// intra-doc link would render as a raw Rust module path.
///
/// This is the vocabulary the API speaks. A store is free to persist a coarser one —
/// tables, views and generic tables are one kind to a catalog that already records
/// which of the three an id refers to — so this deliberately carries no storage
/// mapping.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    strum::VariantArray,
    strum::EnumString,
    strum::IntoStaticStr,
)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
#[strum(serialize_all = "kebab-case")]
pub enum ResourceType {
    Server,
    Project,
    Warehouse,
    Namespace,
    Table,
    View,
    GenericTable,
    /// Spelled `tag-definition` on the wire, matching the path segment tag definitions
    /// are addressed by, so every key of the vocabulary map names its own URL segment.
    /// The only spelling `kebab-case` does not already produce.
    #[serde(rename = "tag-definition")]
    #[strum(serialize = "tag-definition")]
    Tag,
}

impl ResourceType {
    /// The label used on the wire.
    ///
    /// Derived from the variant names, so this spelling and `serde`'s cannot drift apart
    /// silently — the round-trip test below pins that they agree.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        self.into()
    }

    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        <Self as std::str::FromStr>::from_str(s).ok()
    }
}

/// Which kind of principal holds a grant.
///
/// Mirrors [`UserOrRoleId`]'s variants without their ids, so a stored row can name
/// its principal kind independently of which id column carries it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "sqlx-postgres", derive(sqlx::Type))]
#[cfg_attr(
    feature = "sqlx-postgres",
    sqlx(type_name = "grant_principal_type", rename_all = "snake_case")
)]
pub enum PrincipalType {
    User,
    Role,
}

impl UserOrRoleId {
    #[must_use]
    pub fn principal_type(&self) -> PrincipalType {
        match self {
            UserOrRoleId::User(_) => PrincipalType::User,
            UserOrRoleId::Role(_) => PrincipalType::Role,
        }
    }
}

/// A specific resource a grant is held on.
///
/// Tabular variants carry their warehouse because the catalog keys tabulars by
/// `(warehouse_id, tabular_id)`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum GrantResource {
    Server,
    Project(ProjectId),
    Warehouse(WarehouseId),
    Namespace {
        warehouse_id: WarehouseId,
        namespace_id: NamespaceId,
    },
    Table {
        warehouse_id: WarehouseId,
        table_id: TableId,
    },
    View {
        warehouse_id: WarehouseId,
        view_id: ViewId,
    },
    GenericTable {
        warehouse_id: WarehouseId,
        generic_table_id: GenericTableId,
    },
    Tag(TagDefinitionId),
}

impl GrantResource {
    #[must_use]
    pub fn resource_type(&self) -> ResourceType {
        match self {
            GrantResource::Server => ResourceType::Server,
            GrantResource::Project(_) => ResourceType::Project,
            GrantResource::Warehouse(_) => ResourceType::Warehouse,
            GrantResource::Namespace { .. } => ResourceType::Namespace,
            GrantResource::Table { .. } => ResourceType::Table,
            GrantResource::View { .. } => ResourceType::View,
            GrantResource::GenericTable { .. } => ResourceType::GenericTable,
            GrantResource::Tag(_) => ResourceType::Tag,
        }
    }

    /// The warehouse this resource lives under, if any. `None` for server, project
    /// and tag resources, which are not warehouse-scoped.
    #[must_use]
    pub fn warehouse_id(&self) -> Option<WarehouseId> {
        match self {
            GrantResource::Warehouse(warehouse_id)
            | GrantResource::Namespace { warehouse_id, .. }
            | GrantResource::Table { warehouse_id, .. }
            | GrantResource::View { warehouse_id, .. }
            | GrantResource::GenericTable { warehouse_id, .. } => Some(*warehouse_id),
            GrantResource::Server | GrantResource::Project(_) | GrantResource::Tag(_) => None,
        }
    }
}

/// One grant, identified entirely by its `(principal, privilege, resource)` triple.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GrantSpec {
    pub principal: UserOrRoleId,
    pub resource: GrantResource,
    /// A name from the authorizer's grantable vocabulary. Never interpreted here.
    pub privilege: String,
}

/// One row of a grant listing: a [`GrantSpec`] plus whatever provenance the store
/// can supply.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrantRow {
    pub principal: UserOrRoleId,
    pub resource: GrantResource,
    pub privilege: String,
    /// When the grant was created, if the source can supply it. `None` means the
    /// backend returned no usable timestamp — not that it has no notion of time.
    pub created_at: Option<DateTime<Utc>>,
    // No grantor. Only some stores could answer it, nothing reads it, and the
    // `grant_created` audit event already records who acted — including the assumed
    // role, which this row could not have expressed.
}

/// What a grant listing is scoped to.
///
/// Every variant lists **direct** grants only; see the module docs on inheritance.
#[derive(Debug, Clone)]
pub enum GrantFilter {
    /// Grants held on one resource: "who can do what here", or, narrowed to a
    /// principal, "what does this principal hold here".
    ByResource {
        resource: GrantResource,
        /// `None` for every principal.
        principal: Option<UserOrRoleId>,
    },
    /// All grants held by one principal within a project ("what does this
    /// principal hold"). Project-scoped because a principal's grants otherwise
    /// span every project. Server grants are not project-scoped and are excluded;
    /// name the server resource to read those.
    ///
    /// Only the catalog store answers this. An authorizer that owns its grants may
    /// have no index by principal or by project, and is expected to report
    /// [`GrantListingNotImplemented`] rather than walk its store.
    ByPrincipal {
        principal: UserOrRoleId,
        project_id: ProjectId,
    },
    /// Every grant in a project, for export and audit. Server grants are excluded
    /// for the same reason as above.
    ///
    /// No endpoint constructs this: the project-wide listing requires a principal,
    /// because an unnarrowed answer is sized by the deployment rather than by the
    /// request. Kept as a catalog-store capability for an export surface that can
    /// stream a whole project.
    ByProject(ProjectId),
}

impl GrantFilter {
    /// Grants on one resource, optionally narrowed to a single principal.
    #[must_use]
    pub fn on(resource: GrantResource, principal: Option<UserOrRoleId>) -> Self {
        Self::ByResource {
            resource,
            principal,
        }
    }
}

/// One page of a grant listing, with an opaque continuation token.
#[derive(Debug, Clone)]
pub struct ListGrantsResultPage {
    pub grants: Vec<GrantRow>,
    pub next_page_token: Option<String>,
}

/// One grantable privilege, as published by an authorizer for discovery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct PrivilegeDescriptor {
    /// The name to send back in a grant request.
    pub name: String,
    /// Short human-readable label for pickers.
    pub display_name: String,
    /// What the privilege permits, when the authorizer supplies an explanation.
    /// `null` rather than a guess: a wrong description of a permission is worse than
    /// none, so an authorizer that has not written one reports nothing.
    pub description: Option<String>,
    /// Which group of privileges this one belongs to, so a picker can lay out columns
    /// instead of one long list.
    ///
    /// Authorizer-supplied and open, because the vocabulary it groups is too. Treat an
    /// unrecognized value as its own group rather than an error, and `null` as
    /// ungrouped. Lakekeeper's own authorizers use `metadata`, `read`, `write`,
    /// `create`, `security` and `administration`; a client that wants a fixed column
    /// order should key off those and fall back for anything else.
    pub category: Option<String>,
    pub resource_type: ResourceType,
}

/// The privilege is not in the authorizer's grantable vocabulary for this resource
/// type. A client error (the vocabulary is discoverable), not an authorization
/// failure.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
#[error("`{privilege}` is not a grantable privilege for {}", resource_type.as_str())]
pub struct InvalidGrantPrivilege {
    pub resource_type: ResourceType,
    pub privilege: String,
}

impl From<InvalidGrantPrivilege> for ErrorModel {
    fn from(err: InvalidGrantPrivilege) -> Self {
        ErrorModel::bad_request(err.to_string(), "InvalidGrantPrivilege", None)
    }
}

/// The caller may not grant or revoke this privilege on this resource.
///
/// Returned without a visibility check: grant authority is independent of visibility,
/// so this `403` can reach a caller who cannot otherwise see the resource and thereby
/// confirm the id resolves. That disclosure is a recorded decision — see "What the
/// write and vocabulary paths disclose" in `api::management::v1::grant`.
#[derive(Debug, PartialEq, Eq)]
pub struct AuthZGrantActionForbidden {
    resource_type: ResourceType,
    privileges: Vec<String>,
}

impl AuthZGrantActionForbidden {
    /// How many refused privileges the message names before it stops counting,
    /// mirroring the request-validation errors: every offender is reported so one
    /// round trip fixes them all, but bounded so the message stays readable.
    const MAX_NAMED: usize = 5;

    #[must_use]
    pub fn new(
        resource: &GrantResource,
        privileges: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            resource_type: resource.resource_type(),
            privileges: privileges.into_iter().map(Into::into).collect(),
        }
    }
}

impl AuthorizationFailureSource for AuthZGrantActionForbidden {
    fn into_error_model(self) -> ErrorModel {
        let AuthZGrantActionForbidden {
            resource_type,
            privileges,
        } = self;
        let named = privileges
            .iter()
            .take(Self::MAX_NAMED)
            .map(|p| format!("`{p}`"))
            .collect::<Vec<_>>()
            .join(", ");
        let listed = if privileges.len() > Self::MAX_NAMED {
            format!("{named} and {} more", privileges.len() - Self::MAX_NAMED)
        } else {
            named
        };
        ErrorModel::forbidden(
            format!(
                "Granting or revoking {listed} on this {} is forbidden",
                resource_type.as_str()
            ),
            "GrantActionForbidden",
            None,
        )
    }

    fn to_failure_reason(&self) -> AuthorizationFailureReason {
        AuthorizationFailureReason::ActionForbidden
    }
}

/// The grant store returned a record Lakekeeper cannot interpret. Lakekeeper wrote
/// these records, so this is an internal invariant violation (500), not the backend
/// being *unavailable* (503).
#[derive(Debug, thiserror::Error)]
#[error("{reason}")]
pub struct MalformedGrant {
    reason: String,
    #[source]
    source: Box<dyn std::error::Error + Send + Sync + 'static>,
}

impl MalformedGrant {
    pub fn new(
        reason: impl Into<String>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self {
            reason: reason.into(),
            source: Box::new(source),
        }
    }
}

impl AuthorizationFailureSource for MalformedGrant {
    fn into_error_model(self) -> ErrorModel {
        ErrorModel::internal(self.reason, "MalformedGrant", Some(self.source))
    }

    fn to_failure_reason(&self) -> AuthorizationFailureReason {
        AuthorizationFailureReason::InternalAuthorizationError
    }
}

/// The authorizer's model cannot express this grant. Distinct from a denial: no
/// additional authority makes it succeed, so it is a 400 rather than a 403.
#[derive(Debug, thiserror::Error)]
#[error("{reason}")]
pub struct GrantNotSupported {
    reason: String,
}

impl GrantNotSupported {
    #[must_use]
    pub fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
        }
    }
}

impl AuthorizationFailureSource for GrantNotSupported {
    fn into_error_model(self) -> ErrorModel {
        ErrorModel::bad_request(self.reason, "GrantNotSupported", None)
    }

    fn to_failure_reason(&self) -> AuthorizationFailureReason {
        AuthorizationFailureReason::InvalidRequestData
    }
}

/// This authorizer cannot answer a listing that is not scoped to one resource.
///
/// A statement about the deployment, not the request, so 501 rather than 4xx: the same
/// question is answerable on a resource's own listing, and on a deployment whose grants
/// live in the catalog it is answerable here too. Reported rather than approximated —
/// an authorizer that has to walk its whole store to answer would have to choose
/// between a request sized by the deployment and a silently short result.
#[derive(Debug, thiserror::Error)]
#[error("{reason}")]
pub struct GrantListingNotImplemented {
    reason: String,
}

impl GrantListingNotImplemented {
    #[must_use]
    pub fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
        }
    }
}

impl AuthorizationFailureSource for GrantListingNotImplemented {
    fn into_error_model(self) -> ErrorModel {
        ErrorModel::not_implemented(self.reason, "GrantListingNotImplemented", None)
    }

    fn to_failure_reason(&self) -> AuthorizationFailureReason {
        AuthorizationFailureReason::InvalidRequestData
    }
}

/// Error from [`ManagesGrants::apply_grants`].
///
/// No not-found case: authorizers reference principals by id and tolerate ones the
/// catalog has not provisioned. The catalog store enforces existence with foreign
/// keys and reports it separately.
#[derive(Debug, derive_more::From)]
pub enum ApplyGrantsError {
    BackendUnavailable(AuthorizationBackendUnavailable),
    NotSupported(GrantNotSupported),
}

impl From<ApplyGrantsError> for ErrorModel {
    fn from(err: ApplyGrantsError) -> Self {
        match err {
            ApplyGrantsError::BackendUnavailable(e) => e.into_error_model(),
            ApplyGrantsError::NotSupported(e) => e.into_error_model(),
        }
    }
}

/// Error from [`ManagesGrants::list_grants`]: the backend is unavailable (503), or
/// it returned a record we cannot interpret (500). Deliberately distinct — a parse
/// failure is not a transient availability problem.
#[derive(Debug, derive_more::From)]
pub enum ListGrantsError {
    BackendUnavailable(AuthorizationBackendUnavailable),
    Malformed(MalformedGrant),
    NotImplemented(GrantListingNotImplemented),
}

impl From<ListGrantsError> for ErrorModel {
    fn from(err: ListGrantsError) -> Self {
        match err {
            ListGrantsError::BackendUnavailable(e) => e.into_error_model(),
            ListGrantsError::Malformed(e) => e.into_error_model(),
            ListGrantsError::NotImplemented(e) => e.into_error_model(),
        }
    }
}

/// Authorizers that are the source of truth for grants implement this and expose it
/// via [`Authorizer::grants`](super::Authorizer::grants). When an authorizer does
/// not manage grants, Lakekeeper persists them to the catalog instead and this
/// facet is absent.
#[async_trait::async_trait]
pub trait ManagesGrants: Send + Sync {
    /// Apply a grant diff: create `writes`, remove `deletes`. Idempotent.
    ///
    /// One method rather than separate add/remove because the diff must land
    /// atomically — a half-applied diff would emit events for changes that were
    /// then rolled back.
    ///
    /// Returns the grants actually created and actually removed, so callers can
    /// emit events only for real changes. A store that cannot report a per-record
    /// delta may over-report; it must document that.
    async fn apply_grants(
        &self,
        metadata: &RequestMetadata,
        writes: &[GrantSpec],
        deletes: &[GrantSpec],
    ) -> std::result::Result<AppliedGrants, ApplyGrantsError>;

    /// List direct grants matching `filter`.
    async fn list_grants(
        &self,
        metadata: &RequestMetadata,
        filter: GrantFilter,
        pagination: PaginationQuery,
    ) -> std::result::Result<ListGrantsResultPage, ListGrantsError>;
}

/// What an [`apply_grants`](ManagesGrants::apply_grants) call actually changed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AppliedGrants {
    pub created: Vec<GrantSpec>,
    pub removed: Vec<GrantSpec>,
}

impl AppliedGrants {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.created.is_empty() && self.removed.is_empty()
    }
}

/// One grant-authority question: may the subject grant and revoke `privilege` on the
/// resource, to `grantee`?
///
/// Extensible on purpose — construct with [`new`](Self::new). A question may grow a term
/// (whether the entry is a grant or a revoke, say) without breaking authorizers that live
/// outside this workspace.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct GrantAuthorityCheck<'a> {
    /// A name from the authorizer's own vocabulary. An unrecognized name is a deny, not
    /// an error: it may belong to a different authorizer's vocabulary.
    pub privilege: &'a str,
    /// Who would come to hold the privilege — or lose it, for a revoke.
    ///
    /// `None` leaves the grantee out of the question: the grantable-privileges endpoint
    /// asks whether the subject has authority over the privilege here at all. That
    /// answer is advisory, since the apply path asks again for each grantee, so an
    /// authorizer that does distinguish grantees may still answer this form from the
    /// privilege alone.
    ///
    /// An authorizer whose grant authority never depends on who receives the privilege
    /// ignores this field and answers the same either way.
    pub grantee: Option<&'a UserOrRoleId>,
}

impl<'a> GrantAuthorityCheck<'a> {
    #[must_use]
    pub fn new(privilege: &'a str, grantee: Option<&'a UserOrRoleId>) -> Self {
        Self { privilege, grantee }
    }
}

/// The checked entry point to grant authority. Blanket-implemented, so an authorizer
/// cannot replace it and lose the guards — implement
/// [`are_allowed_grants_impl`](Authorizer::are_allowed_grants_impl) instead.
#[async_trait::async_trait]
pub trait AuthZGrantOps: Authorizer {
    /// May the actor (or `for_user`, when given) grant and revoke each of `checks` on
    /// `resource`? Returns exactly one decision per check, in order.
    ///
    /// Grant *authority* is resolved here rather than modelled as a `Catalog*Action`
    /// because the privilege is a name from this authorizer's own vocabulary: it cannot
    /// be a variant of a catalog-wide action enum, and resolving it may fail (an unknown
    /// name is a deny, not a panic).
    ///
    /// Asking about yourself is normalized away before the authorizer is consulted, as
    /// every `are_allowed_*_actions_vec` does. The control-plane bypass is **not**
    /// applied — see the body.
    async fn are_allowed_grants(
        &self,
        metadata: &RequestMetadata,
        mut for_user: Option<&UserOrRole>,
        resource: &GrantResource,
        checks: &[GrantAuthorityCheck<'_>],
    ) -> std::result::Result<Vec<AuthorizationDecision>, IsAllowedActionError> {
        // Naming yourself asks the same question as naming nobody, so it must not trip
        // the read-assignments guard that answering for someone else requires.
        if metadata.actor().to_user_or_role().as_ref() == for_user {
            for_user = None;
        }
        // No `bypasses_control_plane_authz` here, unlike every other control-plane
        // check. Granting is permission administration: an instance admin holds a
        // static config credential, and letting it write grants would let a leaked one
        // escalate any principal to admin — under an authorizer that owns its grants,
        // by writing the very records its own permission API refuses them. Instance
        // admins provision; they do not administer permissions.
        let decisions = self
            .are_allowed_grants_impl(metadata, for_user, resource, checks)
            .await?;
        // Callers zip decisions against the checks, and `zip` stops at the shorter side —
        // a short vector would silently authorize the tail rather than deny it.
        if decisions.len() != checks.len() {
            return Err(
                AuthorizationCountMismatch::new(checks.len(), decisions.len(), "grant").into(),
            );
        }
        Ok(decisions)
    }
}

#[async_trait::async_trait]
impl<T> AuthZGrantOps for T where T: Authorizer {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_type_round_trips_through_its_stored_spelling() {
        for resource_type in <ResourceType as strum::VariantArray>::VARIANTS {
            assert_eq!(
                ResourceType::parse(resource_type.as_str()),
                Some(*resource_type),
                "`{}` must parse back to itself",
                resource_type.as_str()
            );
        }
        assert_eq!(ResourceType::parse("nope"), None);
    }

    #[test]
    fn resource_type_serializes_as_its_wire_spelling() {
        // `as_str` and serde must agree: both are the wire contract.
        assert_eq!(
            serde_json::to_string(&ResourceType::GenericTable).unwrap(),
            "\"generic-table\""
        );
        assert_eq!(ResourceType::GenericTable.as_str(), "generic-table");
        assert_eq!(ResourceType::Tag.as_str(), "tag-definition");
    }

    #[test]
    fn grant_resource_reports_its_type_and_warehouse() {
        let warehouse_id = WarehouseId::new_random();
        assert_eq!(GrantResource::Server.resource_type(), ResourceType::Server);
        assert_eq!(GrantResource::Server.warehouse_id(), None);
        assert_eq!(
            GrantResource::Tag(TagDefinitionId::new_random()).warehouse_id(),
            None
        );

        let table = GrantResource::Table {
            warehouse_id,
            table_id: TableId::new_random(),
        };
        assert_eq!(table.resource_type(), ResourceType::Table);
        assert_eq!(table.warehouse_id(), Some(warehouse_id));
    }

    #[test]
    fn invalid_privilege_is_a_client_error() {
        let err = ErrorModel::from(InvalidGrantPrivilege {
            resource_type: ResourceType::Warehouse,
            privilege: "nope".to_string(),
        });
        assert_eq!(err.code, 400);
        assert_eq!(err.r#type, "InvalidGrantPrivilege");
    }

    #[test]
    fn forbidden_grant_is_a_403_that_does_not_name_the_resource() {
        let resource = GrantResource::Warehouse(WarehouseId::new_random());
        let err = AuthZGrantActionForbidden::new(&resource, ["select"]).into_error_model();
        assert_eq!(err.code, 403);
        assert_eq!(err.r#type, "GrantActionForbidden");
        assert_eq!(
            err.message,
            "Granting or revoking `select` on this warehouse is forbidden"
        );
    }

    #[test]
    fn forbidden_grant_names_every_refused_privilege_bounded() {
        let resource = GrantResource::Warehouse(WarehouseId::new_random());
        let err =
            AuthZGrantActionForbidden::new(&resource, ["select", "modify"]).into_error_model();
        assert_eq!(
            err.message,
            "Granting or revoking `select`, `modify` on this warehouse is forbidden"
        );
        let err = AuthZGrantActionForbidden::new(&resource, ["a", "b", "c", "d", "e", "f", "g"])
            .into_error_model();
        assert_eq!(
            err.message,
            "Granting or revoking `a`, `b`, `c`, `d`, `e` and 2 more on this warehouse is forbidden"
        );
    }
}
