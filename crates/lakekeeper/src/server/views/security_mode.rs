//! Who may switch a view between running as its owner (an engine owner property is set) and
//! running as the caller. The commit decides its switches on the stored view before
//! authorization. Each switch needs [`CatalogViewAction::ChangeSecurityMode`] for the identity
//! that sends the commit; one that adds the property also needs it for the user the property
//! names. Inside the commit transaction, the switches must be the ones that were decided.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};

use http::StatusCode;
use iceberg::spec::ViewMetadata;
use iceberg_ext::catalog::rest::ErrorModel;

use crate::{
    config::TRINO_OWNER_PROPERTY,
    request_metadata::{ArcRequestMetadata, RequestMetadata},
    server::tables::authorize_load::definer_owner_user_id,
    service::{
        NamespaceHierarchy, ResolvedWarehouse, UserId, ViewInfo,
        authz::{
            ActionOnView, AuthZViewOps, Authorizer, AuthzBadRequest, CatalogViewAction,
            RequireViewActionError, UserOrRole, UserOrRoleId,
        },
        events::{
            APIEventContext, EventDispatcher,
            context::{Unresolved, UserProvidedView},
            delegate_authorization_failure_source, impl_authorization_failure_source,
        },
    },
};

/// How a commit switches the security mode of a view for one owner property.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SecurityModeSwitch {
    /// The commit adds the owner property: engines run the view as `owner`.
    ToOwner { owner: String },
    /// The commit removes the owner property: engines run the view as the caller.
    ToCaller,
}

/// The switches a commit makes, by owner property.
pub(super) type SecurityModeSwitches = BTreeMap<String, SecurityModeSwitch>;

/// The switches that turning `previous` into `new` makes: each protected owner property that
/// appears or disappears. Another owner in the same property keeps the mode.
pub(super) fn security_mode_switches(
    previous: &ViewMetadata,
    new: &ViewMetadata,
    protected_properties: &HashSet<String>,
) -> SecurityModeSwitches {
    protected_properties
        .iter()
        .filter_map(|property| {
            let switch = match (
                previous.properties().contains_key(property),
                new.properties().get(property),
            ) {
                (false, Some(owner)) => SecurityModeSwitch::ToOwner {
                    owner: owner.clone(),
                },
                (true, None) => SecurityModeSwitch::ToCaller,
                (false, None) | (true, Some(_)) => return None,
            };
            Some((property.clone(), switch))
        })
        .collect()
}

/// Asks whether the commit may make `switches`: first for the identity that sends the commit,
/// then for each new owner, checked as the view will run. Each verdict is recorded as its own
/// `change_security_mode` record on the view; an owner's record names the owner. Stops at the
/// first refusal.
pub(super) async fn authorize_security_mode_switches<A: Authorizer>(
    authorizer: &A,
    records: SwitchRecords<'_>,
    target: SwitchTarget<'_>,
    switches: &SecurityModeSwitches,
) -> Result<(), ErrorModel> {
    let Some((first_property, first_switch)) = switches.iter().next() else {
        return Ok(());
    };
    let request_metadata = records.request_metadata.as_ref();

    let sender = request_metadata.user_id().cloned();
    let decision = is_allowed(authorizer, request_metadata, target, None)
        .await
        .and_then(|allowed| {
            refuse_unless(
                allowed,
                first_property,
                first_switch,
                RefusedIdentity::Sender(sender),
            )
        });
    records.record(None).emit_authz(decision)?;

    let token_idp_id = request_metadata
        .authentication()
        .and_then(|a| a.subject().idp_id())
        .map(String::as_str);
    let mut checked_owners = HashSet::new();
    for (property, switch) in switches {
        let SecurityModeSwitch::ToOwner { owner } = switch else {
            continue;
        };
        if !checked_owners.insert(owner) {
            continue;
        }
        let (for_principal, decision) = match definer_owner_user_id(token_idp_id, owner) {
            Ok(user) => {
                let decision = is_allowed(
                    authorizer,
                    request_metadata,
                    target,
                    Some(&UserOrRole::User(user.clone())),
                )
                .await
                .and_then(|allowed| {
                    refuse_unless(
                        allowed,
                        property,
                        switch,
                        RefusedIdentity::NewOwner(user.clone()),
                    )
                });
                (Some(UserOrRoleId::User(user)), decision)
            }
            // An owner that names no valid user may not switch the mode.
            Err(e) => (None, Err(e.into())),
        };
        records.record(for_principal).emit_authz(decision)?;
    }
    Ok(())
}

/// The view whose security mode a commit switches.
#[derive(Clone, Copy)]
pub(super) struct SwitchTarget<'a> {
    pub(super) warehouse: &'a ResolvedWarehouse,
    pub(super) namespace: &'a NamespaceHierarchy,
    pub(super) view: &'a ViewInfo,
}

/// Whether `user`, or the request's principal if `None`, may switch the security mode of the
/// view.
async fn is_allowed<A: Authorizer>(
    authorizer: &A,
    request_metadata: &RequestMetadata,
    target: SwitchTarget<'_>,
    user: Option<&UserOrRole>,
) -> Result<bool, ChangeSecurityModeError> {
    let parent_namespaces: HashMap<_, _> = target
        .namespace
        .parents
        .iter()
        .map(|ns| (ns.namespace_id(), ns.clone()))
        .collect();
    let [allowed] = authorizer
        .are_allowed_view_actions_arr(
            request_metadata,
            target.warehouse,
            &parent_namespaces,
            &[(
                &target.namespace.namespace,
                ActionOnView {
                    info: target.view,
                    action: CatalogViewAction::ChangeSecurityMode,
                    user,
                    // The owner's rights are checked as the view will run, as in a DEFINER chain.
                    is_delegated_execution: user.is_some(),
                },
            )],
        )
        .await
        .map_err(RequireViewActionError::from)?
        .into_inner();
    Ok(allowed)
}

fn refuse_unless(
    allowed: bool,
    property: &str,
    switch: &SecurityModeSwitch,
    refused: RefusedIdentity,
) -> Result<(), ChangeSecurityModeError> {
    if allowed {
        return Ok(());
    }
    Err(SecurityModeChangeForbidden {
        owner_property: property.to_string(),
        switch: switch.clone(),
        refused,
    }
    .into())
}

/// What the `change_security_mode` records of a commit share with the commit's own record.
#[derive(Clone, Copy)]
pub(super) struct SwitchRecords<'a> {
    pub(super) request_metadata: &'a ArcRequestMetadata,
    pub(super) dispatcher: &'a EventDispatcher,
    pub(super) view: &'a Arc<UserProvidedView>,
}

impl SwitchRecords<'_> {
    /// A `change_security_mode` record on the view, for `for_principal` if given.
    fn record(
        self,
        for_principal: Option<UserOrRoleId>,
    ) -> APIEventContext<UserProvidedView, Unresolved, CatalogViewAction> {
        let mut record = APIEventContext::new_arc(
            self.request_metadata.clone(),
            self.dispatcher.clone(),
            self.view.clone(),
            Arc::new(CatalogViewAction::ChangeSecurityMode),
        );
        if let Some(principal) = for_principal {
            record.set_for_principal(principal);
        }
        record
    }
}

/// Why a `change_security_mode` check of a commit failed.
#[derive(Debug, derive_more::From)]
pub(crate) enum ChangeSecurityModeError {
    SecurityModeChangeForbidden(SecurityModeChangeForbidden),
    RequireViewActionError(RequireViewActionError),
    /// The owner property names no valid user.
    AuthzBadRequest(AuthzBadRequest),
}
delegate_authorization_failure_source!(ChangeSecurityModeError => {
    SecurityModeChangeForbidden,
    RequireViewActionError,
    AuthzBadRequest,
});

/// Requires that a commit makes exactly the `decided` switches: the stored view may have changed
/// since they were decided and authorized.
pub(super) fn require_decided_switches(
    decided: &SecurityModeSwitches,
    actual: &SecurityModeSwitches,
) -> Result<(), SecurityModeChangedDuringCommit> {
    if decided == actual {
        Ok(())
    } else {
        Err(SecurityModeChangedDuringCommit)
    }
}

/// The identity that may not switch the security mode of the view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RefusedIdentity {
    /// The identity that sends the commit; `None` if it is no user.
    Sender(Option<UserId>),
    /// The user that the owner property names, in the identity provider of the request.
    NewOwner(UserId),
}

/// A commit adds or removes an engine owner property, and the identity that sends it or the user
/// the property names may not switch the security mode of the view.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) struct SecurityModeChangeForbidden {
    pub(crate) owner_property: String,
    pub(crate) switch: SecurityModeSwitch,
    pub(crate) refused: RefusedIdentity,
}

// `Display` by hand: the message and the Trino advice depend on the switch and the key.
impl std::fmt::Display for SecurityModeChangeForbidden {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            owner_property,
            switch,
            refused,
        } = self;
        match switch {
            SecurityModeSwitch::ToOwner { owner } => write!(
                f,
                "This commit makes query engines run the view as the user in view property \
                 '{owner_property}' ('{owner}'), where they ran it as the calling user. \
                 Switching a view between these two needs the view action \
                 'change_security_mode' for the identity that sends the commit and for the new \
                 owner."
            )?,
            SecurityModeSwitch::ToCaller => write!(
                f,
                "This commit makes query engines run the view as the calling user, where they \
                 ran it as the user in view property '{owner_property}'. Switching a view \
                 between these two needs the view action 'change_security_mode' for the \
                 identity that sends the commit."
            )?,
        }
        match refused {
            RefusedIdentity::NewOwner(user) => write!(
                f,
                " Lakekeeper checks the new owner as user '{user}', who does not have it. Keep \
                 the security mode of the view, or give '{user}' this permission on the view."
            )?,
            RefusedIdentity::Sender(Some(user)) => write!(
                f,
                " The identity that sends the commit is '{user}', and it does not have it. Keep \
                 the security mode of the view, or give '{user}' this permission on the view."
            )?,
            RefusedIdentity::Sender(None) => write!(
                f,
                " The identity that sends the commit does not have it. Keep the security mode \
                 of the view."
            )?,
        }
        if owner_property == TRINO_OWNER_PROPERTY {
            match switch {
                SecurityModeSwitch::ToOwner { .. } => write!(
                    f,
                    " In Trino, keep it with SECURITY INVOKER: SECURITY DEFINER is the default."
                )?,
                SecurityModeSwitch::ToCaller => {
                    write!(f, " In Trino, keep it with SECURITY DEFINER.")?;
                }
            }
        }
        Ok(())
    }
}

impl From<SecurityModeChangeForbidden> for ErrorModel {
    fn from(err: SecurityModeChangeForbidden) -> Self {
        ErrorModel::builder()
            .code(StatusCode::FORBIDDEN.as_u16())
            .r#type("SecurityModeChangeForbidden")
            .message(err.to_string())
            .build()
    }
}

impl_authorization_failure_source!(SecurityModeChangeForbidden => ActionForbidden);

/// The stored view changed between the decision on a commit's security mode switches and the
/// commit transaction, so the commit would switch the mode in a way nobody authorized.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error(
    "The view changed while Lakekeeper checked whether this commit switches its security mode. \
     Load the view and commit again."
)]
pub(crate) struct SecurityModeChangedDuringCommit;

impl From<SecurityModeChangedDuringCommit> for ErrorModel {
    fn from(err: SecurityModeChangedDuringCommit) -> Self {
        ErrorModel::builder()
            .code(StatusCode::CONFLICT.as_u16())
            .r#type("CommitFailedException")
            .message(err.to_string())
            .build()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use iceberg::{
        NamespaceIdent,
        spec::{
            NestedField, PrimitiveType, Schema, SqlViewRepresentation, Type as IcebergType,
            ViewFormatVersion, ViewMetadata, ViewMetadataBuilder, ViewRepresentation,
            ViewRepresentations, ViewVersion,
        },
    };

    use super::{
        RefusedIdentity, SecurityModeChangeForbidden, SecurityModeChangedDuringCommit,
        SecurityModeSwitch, SecurityModeSwitches, require_decided_switches, security_mode_switches,
    };
    use crate::service::{UserId, events::AuthorizationFailureSource as _};

    const OWNER: &str = "trino.run-as-owner";
    const CUSTOM_OWNER: &str = "custom.run-as-owner";

    fn view(properties: &[(&str, &str)]) -> ViewMetadata {
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(1, "id", IcebergType::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .unwrap();
        let version = ViewVersion::builder()
            .with_schema_id(1)
            .with_timestamp_ms(0)
            .with_default_namespace(NamespaceIdent::new("ns".to_string()))
            .with_representations(
                ViewRepresentations::builder()
                    .add_representation(ViewRepresentation::Sql(SqlViewRepresentation {
                        sql: "select 1".to_string(),
                        dialect: "trino".to_string(),
                    }))
                    .build()
                    .unwrap(),
            )
            .build();
        ViewMetadataBuilder::new(
            "s3://bucket/view".to_string(),
            schema,
            version,
            ViewFormatVersion::V1,
            properties
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect::<HashMap<_, _>>(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata
    }

    fn switches(previous: &[(&str, &str)], new: &[(&str, &str)]) -> SecurityModeSwitches {
        let protected = HashSet::from([OWNER.to_string(), CUSTOM_OWNER.to_string()]);
        security_mode_switches(&view(previous), &view(new), &protected)
    }

    fn to_owner(owner: &str) -> SecurityModeSwitch {
        SecurityModeSwitch::ToOwner {
            owner: owner.to_string(),
        }
    }

    #[test]
    fn adding_or_removing_an_owner_property_switches_the_mode() {
        assert_eq!(
            switches(&[], &[(OWNER, "alice")]),
            SecurityModeSwitches::from([(OWNER.to_string(), to_owner("alice"))])
        );
        assert_eq!(
            switches(&[(OWNER, "alice"), ("comment", "c")], &[("comment", "c")]),
            SecurityModeSwitches::from([(OWNER.to_string(), SecurityModeSwitch::ToCaller)])
        );
        // Each owner property on its own: Trino's key stays, the custom engine's key appears.
        assert_eq!(
            switches(
                &[(OWNER, "alice")],
                &[(OWNER, "alice"), (CUSTOM_OWNER, "bob")]
            ),
            SecurityModeSwitches::from([(CUSTOM_OWNER.to_string(), to_owner("bob"))])
        );
    }

    #[test]
    fn another_owner_or_another_property_keeps_the_mode() {
        assert_eq!(
            switches(&[(OWNER, "alice")], &[(OWNER, "bob")]),
            SecurityModeSwitches::new()
        );
        assert_eq!(
            switches(&[(OWNER, "alice")], &[(OWNER, "alice")]),
            SecurityModeSwitches::new()
        );
        assert_eq!(
            switches(&[("comment", "a")], &[("comment", "b")]),
            SecurityModeSwitches::new()
        );
    }

    /// The decisions were made for the switches the stored view showed before the commit
    /// transaction. Any other switch inside the transaction was not authorized.
    #[test]
    fn a_commit_makes_only_the_decided_switches() {
        let to_alice = switches(&[], &[(OWNER, "alice")]);
        let to_bob = switches(&[], &[(OWNER, "bob")]);
        let to_caller = switches(&[(OWNER, "alice")], &[]);
        let none = SecurityModeSwitches::new();
        assert_eq!(require_decided_switches(&to_alice, &to_alice), Ok(()));
        assert_eq!(require_decided_switches(&none, &none), Ok(()));
        for (decided, actual) in [
            (&none, &to_alice),
            (&none, &to_caller),
            (&to_bob, &to_alice),
            (&to_alice, &none),
        ] {
            assert_eq!(
                require_decided_switches(decided, actual),
                Err(SecurityModeChangedDuringCommit)
            );
        }
        let error = iceberg_ext::catalog::rest::ErrorModel::from(SecurityModeChangedDuringCommit);
        assert_eq!(error.code, 409);
        assert_eq!(error.r#type, "CommitFailedException");
    }

    #[test]
    fn refusals_name_the_switch_the_identity_to_grant_and_the_trino_statement() {
        let message = |property: &str, switch, refused| {
            SecurityModeChangeForbidden {
                owner_property: property.to_string(),
                switch,
                refused,
            }
            .into_error_model()
            .message
        };
        let alice = UserId::new_unchecked("oidc", "alice");
        let trino = UserId::new_unchecked("oidc", "trino-sa");
        assert_eq!(
            message(
                OWNER,
                to_owner("alice"),
                RefusedIdentity::NewOwner(alice.clone())
            ),
            "This commit makes query engines run the view as the user in view property \
             'trino.run-as-owner' ('alice'), where they ran it as the calling user. Switching a \
             view between these two needs the view action 'change_security_mode' for the \
             identity that sends the commit and for the new owner. Lakekeeper checks the new \
             owner as user 'oidc~alice', who does not have it. Keep the security mode of the \
             view, or give 'oidc~alice' this permission on the view. In Trino, keep it with \
             SECURITY INVOKER: SECURITY DEFINER is the default."
        );
        assert_eq!(
            message(
                OWNER,
                SecurityModeSwitch::ToCaller,
                RefusedIdentity::Sender(Some(trino))
            ),
            "This commit makes query engines run the view as the calling user, where they ran \
             it as the user in view property 'trino.run-as-owner'. Switching a view between \
             these two needs the view action 'change_security_mode' for the identity that sends \
             the commit. The identity that sends the commit is 'oidc~trino-sa', and it does not \
             have it. Keep the security mode of the view, or give 'oidc~trino-sa' this \
             permission on the view. In Trino, keep it with SECURITY DEFINER."
        );
        assert_eq!(
            message(CUSTOM_OWNER, to_owner("bob"), RefusedIdentity::Sender(None)),
            "This commit makes query engines run the view as the user in view property \
             'custom.run-as-owner' ('bob'), where they ran it as the calling user. Switching a \
             view between these two needs the view action 'change_security_mode' for the \
             identity that sends the commit and for the new owner. The identity that sends the \
             commit does not have it. Keep the security mode of the view."
        );
        let error = SecurityModeChangeForbidden {
            owner_property: OWNER.to_string(),
            switch: SecurityModeSwitch::ToCaller,
            refused: RefusedIdentity::NewOwner(alice),
        }
        .into_error_model();
        assert_eq!(error.code, 403);
        assert_eq!(error.r#type, "SecurityModeChangeForbidden");
    }
}
