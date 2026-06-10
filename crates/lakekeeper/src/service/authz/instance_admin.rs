//! Instance-admin authorization: a small, non-pluggable authority for
//! capabilities granted by server configuration (`LAKEKEEPER__INSTANCE_ADMINS`)
//! or in-process callers, rather than by the pluggable resource [`Authorizer`].
//!
//! [`Authorizer`]: crate::service::authz::Authorizer
//!
//! Two shapes of capability share one predicate
//! ([`InstanceAdminAuthorizer::has_bypass`]):
//!
//! * **Override** — an instance admin may perform any control-plane action that
//!   others could be granted. Applied via
//!   [`RequestMetadata::bypasses_control_plane_authz`], which short-circuits the
//!   resource-authorizer checks (see the `are_allowed_*_actions_vec` defaults).
//! * **Exclusive** — a few operations are instance-admin-only and not grantable
//!   at all (e.g. setting a warehouse's managed-by marker). These are modelled as
//!   [`InstanceAdminAction`] and authorized here, **never** through the resource
//!   authorizer — so they never appear in OpenFGA, `/actions`, or batch-check.
//!
//! Instance-admin membership is resolved once in authn and carried on
//! [`RequestMetadata`]; this layer is therefore stateless.

use std::collections::HashSet;

use http::StatusCode;
use iceberg_ext::catalog::rest::ErrorModel;

use crate::{
    CONFIG,
    request_metadata::RequestMetadata,
    service::{
        UserId,
        authz::{ActionDescriptor, CatalogAction},
        events::{AuthorizationFailureReason, AuthorizationFailureSource},
    },
};

/// Capabilities authorized solely by instance-admin privilege (a configured
/// instance admin or an in-process caller) — never by the pluggable resource
/// authorizer, and never represented in OpenFGA, `/actions`, or batch-check.
///
/// Add a variant here for each new instance-admin-only operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum_macros::Display, strum_macros::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum InstanceAdminAction {
    /// Set or clear a warehouse's managed-by marker.
    SetWarehouseManagedBy,
}

impl CatalogAction for InstanceAdminAction {
    fn action_descriptor(&self) -> ActionDescriptor {
        ActionDescriptor::builder().action_name(self.into()).build()
    }
}

/// The instance-admin authority. Stateless — the membership decision is resolved
/// in authn and carried on [`RequestMetadata`]. Distinct from the pluggable
/// resource [`Authorizer`](crate::service::authz::Authorizer), which decides
/// resource grants.
#[derive(Debug, Clone, Copy)]
pub struct InstanceAdminAuthorizer;

impl InstanceAdminAuthorizer {
    /// Whether the caller holds instance-admin bypass: a configured instance
    /// admin (`Actor::Principal` in `LAKEKEEPER__INSTANCE_ADMINS`) or an
    /// in-process (`LakekeeperInternal`) caller.
    ///
    /// This is the single definition of the control-plane bypass predicate;
    /// [`RequestMetadata::bypasses_control_plane_authz`] builds on it. Note this
    /// does not consider data-plane actions — callers that bypass data-plane
    /// must additionally check [`RequestMetadata::is_lakekeeper_internal`].
    #[must_use]
    pub fn has_bypass(metadata: &RequestMetadata) -> bool {
        metadata.is_lakekeeper_internal() || metadata.is_instance_admin()
    }

    /// Whether `metadata` may perform `action`.
    #[must_use]
    pub fn is_allowed(metadata: &RequestMetadata, _action: InstanceAdminAction) -> bool {
        Self::has_bypass(metadata)
    }

    /// Authorize an instance-admin-only action. The returned
    /// [`InstanceAdminForbidden`] is an [`AuthorizationFailureSource`], so denials
    /// flow through the normal audit emit path (carrying
    /// `privilege_source = instance_admin`/`authorizer`).
    ///
    /// # Errors
    /// [`InstanceAdminForbidden`] if the caller is not an instance admin.
    pub fn require(
        metadata: &RequestMetadata,
        action: InstanceAdminAction,
    ) -> Result<(), InstanceAdminForbidden> {
        if Self::is_allowed(metadata, action) {
            Ok(())
        } else {
            Err(InstanceAdminForbidden { action })
        }
    }
}

/// Resolves whether an [`Actor`] holds instance-admin (break-glass) status.
///
/// The decision is made **once per request** on the authn path and cached on
/// [`RequestMetadata`] as a binary flag ([`RequestMetadata::is_instance_admin`]);
/// only the *source* of that decision is pluggable, never the capabilities it
/// grants. The bypass is intentionally all-or-nothing — granularity belongs on
/// the exclusive-action side ([`InstanceAdminAction`]), not here.
///
/// The default [`ConfiguredInstanceAdmins`] reads the static
/// `LAKEKEEPER__INSTANCE_ADMINS` configuration, preserving zero-config bootstrap.
/// Deployments that need runtime promote/demote (e.g. a control-plane UI backed by
/// a database) inject their own implementation when building the router. The
/// method is `async` because it is consulted on the already-async authn path and a
/// non-config source typically performs (cached) I/O.
#[async_trait::async_trait]
pub trait InstanceAdminMembership: Send + Sync + std::fmt::Debug {
    /// Whether `user_id` is an instance admin.
    ///
    /// The boundary deliberately takes a [`UserId`], not an [`Actor`]: callers must
    /// only invoke this for an authenticated principal ([`Actor::Principal`]), so an
    /// implementation can never be reached for [`Actor::Role`] or [`Actor::Anonymous`].
    /// Role assumption is an explicit opt-in to a narrower scope and never inherits
    /// instance-admin — keeping that exclusion at the caller means no implementation
    /// can grant it by mistake.
    ///
    /// [`Actor`]: crate::service::authn::Actor
    /// [`Actor::Principal`]: crate::service::authn::Actor::Principal
    /// [`Actor::Role`]: crate::service::authn::Actor::Role
    /// [`Actor::Anonymous`]: crate::service::authn::Actor::Anonymous
    async fn is_instance_admin(&self, user_id: &UserId) -> bool;
}

/// Default [`InstanceAdminMembership`]: membership is a fixed set of principals,
/// normally sourced from the static `LAKEKEEPER__INSTANCE_ADMINS` configuration
/// via [`ConfiguredInstanceAdmins::from_config`].
#[derive(Debug)]
pub struct ConfiguredInstanceAdmins {
    admins: HashSet<UserId>,
}

impl ConfiguredInstanceAdmins {
    /// Build from an explicit admin set.
    #[must_use]
    pub fn new(admins: HashSet<UserId>) -> Self {
        Self { admins }
    }

    /// Snapshot the configured admin set (`LAKEKEEPER__INSTANCE_ADMINS`). Config is
    /// loaded once at process start, so this is a stable, process-lifetime set.
    #[must_use]
    pub fn from_config() -> Self {
        Self::new(CONFIG.instance_admins.clone())
    }
}

#[async_trait::async_trait]
impl InstanceAdminMembership for ConfiguredInstanceAdmins {
    async fn is_instance_admin(&self, user_id: &UserId) -> bool {
        self.admins.contains(user_id)
    }
}

/// Returned when a non-instance-admin attempts an [`InstanceAdminAction`]. 403.
#[derive(Debug, thiserror::Error)]
#[error("Action `{action}` requires instance-admin privilege.")]
pub struct InstanceAdminForbidden {
    pub action: InstanceAdminAction,
}

impl From<InstanceAdminForbidden> for ErrorModel {
    fn from(err: InstanceAdminForbidden) -> Self {
        ErrorModel::builder()
            .r#type("InstanceAdminRequired")
            .code(StatusCode::FORBIDDEN.as_u16())
            .message(err.to_string())
            .build()
    }
}

impl AuthorizationFailureSource for InstanceAdminForbidden {
    fn to_failure_reason(&self) -> AuthorizationFailureReason {
        AuthorizationFailureReason::ActionForbidden
    }

    fn into_error_model(self) -> ErrorModel {
        self.into()
    }
}

#[cfg(test)]
mod tests {
    use http::StatusCode;

    use super::*;
    use crate::service::UserId;

    const ACTION: InstanceAdminAction = InstanceAdminAction::SetWarehouseManagedBy;

    #[test]
    fn instance_admin_is_allowed() {
        let md = RequestMetadata::test_instance_admin(UserId::new_unchecked("oidc", "admin-1"));
        assert!(InstanceAdminAuthorizer::has_bypass(&md));
        assert!(InstanceAdminAuthorizer::require(&md, ACTION).is_ok());
    }

    #[test]
    fn internal_is_allowed() {
        let md = RequestMetadata::new_lakekeeper_internal(uuid::Uuid::now_v7());
        assert!(InstanceAdminAuthorizer::has_bypass(&md));
        assert!(InstanceAdminAuthorizer::require(&md, ACTION).is_ok());
    }

    #[test]
    fn ordinary_user_is_denied_with_403() {
        // A normal authenticated user (not an instance admin) is rejected.
        let md = RequestMetadata::test_user(UserId::new_unchecked("oidc", "user-1"));
        assert!(!InstanceAdminAuthorizer::has_bypass(&md));

        let err = InstanceAdminAuthorizer::require(&md, ACTION)
            .expect_err("ordinary user must not set the managed-by marker");
        assert_eq!(
            err.to_failure_reason(),
            AuthorizationFailureReason::ActionForbidden
        );

        let model = ErrorModel::from(err);
        assert_eq!(model.code, StatusCode::FORBIDDEN.as_u16());
        assert_eq!(model.r#type, "InstanceAdminRequired");
    }

    #[tokio::test]
    async fn configured_admins_match_only_the_configured_user_ids() {
        let alice = UserId::try_from("oidc~alice").unwrap();
        let bob = UserId::try_from("oidc~bob").unwrap();
        let source = ConfiguredInstanceAdmins::new([alice.clone()].into_iter().collect());

        assert!(source.is_instance_admin(&alice).await);
        assert!(!source.is_instance_admin(&bob).await);

        let empty = ConfiguredInstanceAdmins::new(HashSet::new());
        assert!(!empty.is_instance_admin(&bob).await);
    }
}
