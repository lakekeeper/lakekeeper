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

use http::StatusCode;
use iceberg_ext::catalog::rest::ErrorModel;

use crate::{
    request_metadata::RequestMetadata,
    service::{
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
}
