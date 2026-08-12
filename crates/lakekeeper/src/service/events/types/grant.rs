use std::sync::Arc;

use crate::{api::RequestMetadata, service::authz::GrantSpec};

// ===== Grant Events =====

/// Event emitted when a request changed grants: everything it created and everything it
/// removed, in one event.
///
/// One event per request rather than one per grant. Both emitters are batch — the apply
/// endpoint and the cascade when a user is deleted — and the latter is unbounded, so
/// per-grant events would make dispatch cost scale with a number nothing caps. A
/// listener that wants per-grant granularity iterates; the audit backend does exactly
/// that, emitting one record per triple.
///
/// Each entry carries the whole `(principal, privilege, resource)` triple because that
/// triple *is* the grant's identity — there is no grant id to reference, and a revoked
/// grant is hard-deleted, so nothing remains to look up afterwards.
///
/// `removed` is listed before `created` to match the order storage applies them: a diff
/// that revokes and re-grants the same privilege ends granted.
#[derive(Clone, Debug)]
pub struct GrantsChangedEvent {
    pub removed: Vec<GrantSpec>,
    pub created: Vec<GrantSpec>,
    pub request_metadata: Arc<RequestMetadata>,
}

impl GrantsChangedEvent {
    #[must_use]
    pub fn new(
        removed: Vec<GrantSpec>,
        created: Vec<GrantSpec>,
        request_metadata: Arc<RequestMetadata>,
    ) -> Self {
        Self {
            removed,
            created,
            request_metadata,
        }
    }

    /// Nothing changed, so there is nothing to dispatch. Checked by the caller so an
    /// unchanged re-apply costs no listener work at all.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.removed.is_empty() && self.created.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fmt::{Display, Formatter},
        sync::Mutex,
    };

    use super::*;
    use crate::{
        request_metadata::RequestMetadataTestBuilder,
        service::{
            RoleId, TableId, WarehouseId,
            authz::{GrantResource, UserOrRoleId},
            events::{EventDispatcher, EventListener},
        },
    };

    #[derive(Debug, Default)]
    struct CapturingListener {
        changed: Mutex<Vec<GrantsChangedEvent>>,
    }

    impl Display for CapturingListener {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "CapturingListener")
        }
    }

    #[async_trait::async_trait]
    impl EventListener for CapturingListener {
        async fn grants_changed(&self, event: GrantsChangedEvent) -> anyhow::Result<()> {
            self.changed.lock().unwrap().push(event);
            Ok(())
        }
    }

    #[derive(Debug)]
    struct DefaultListener;

    impl Display for DefaultListener {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "DefaultListener")
        }
    }

    #[async_trait::async_trait]
    impl EventListener for DefaultListener {}

    fn request_metadata() -> Arc<RequestMetadata> {
        Arc::new(RequestMetadataTestBuilder::builder().build())
    }

    fn table_spec(privilege: &str) -> (GrantSpec, RoleId, WarehouseId, TableId) {
        let role_id = RoleId::new_random();
        let warehouse_id = WarehouseId::new_random();
        let table_id = TableId::new_random();
        let spec = GrantSpec {
            principal: UserOrRoleId::Role(role_id),
            resource: GrantResource::Table {
                warehouse_id,
                table_id,
            },
            privilege: privilege.to_string(),
        };
        (spec, role_id, warehouse_id, table_id)
    }

    /// Both sides of one diff reach the listener in one event, each entry with its whole
    /// triple intact — the triple is the grant's identity, so a dropped field makes the
    /// event unusable for attribution.
    #[tokio::test]
    async fn one_event_carries_both_sides_of_a_diff_with_their_full_triples() {
        let listener = Arc::new(CapturingListener::default());
        let dispatcher = EventDispatcher::new(vec![listener.clone()]);
        let (created_spec, role_id, warehouse_id, table_id) = table_spec("select");
        let user_id = crate::service::authn::UserId::new_unchecked("oidc", "u1");
        let removed_spec = GrantSpec {
            principal: UserOrRoleId::User(user_id.clone()),
            resource: GrantResource::Warehouse(warehouse_id),
            privilege: "modify".to_string(),
        };

        dispatcher
            .grants_changed(GrantsChangedEvent::new(
                vec![removed_spec],
                vec![created_spec],
                request_metadata(),
            ))
            .await;

        let changed = listener.changed.lock().unwrap();
        assert_eq!(changed.len(), 1, "one request must dispatch one event");
        let event = &changed[0];

        assert_eq!(event.created.len(), 1);
        assert_eq!(event.created[0].privilege, "select");
        assert_eq!(event.created[0].principal, UserOrRoleId::Role(role_id));
        assert_eq!(
            event.created[0].resource,
            GrantResource::Table {
                warehouse_id,
                table_id
            }
        );

        assert_eq!(event.removed.len(), 1);
        assert_eq!(event.removed[0].privilege, "modify");
        assert_eq!(event.removed[0].principal, UserOrRoleId::User(user_id));
        assert_eq!(
            event.removed[0].resource,
            GrantResource::Warehouse(warehouse_id)
        );
    }

    /// A batch is one dispatch however many grants it carries — the property the shape
    /// exists for, since the cascade on user deletion is unbounded.
    #[tokio::test]
    async fn many_grants_still_dispatch_once() {
        let listener = Arc::new(CapturingListener::default());
        let dispatcher = EventDispatcher::new(vec![listener.clone()]);
        let removed: Vec<GrantSpec> = (0..250).map(|_| table_spec("select").0).collect();

        dispatcher
            .grants_changed(GrantsChangedEvent::new(
                removed,
                Vec::new(),
                request_metadata(),
            ))
            .await;

        let changed = listener.changed.lock().unwrap();
        assert_eq!(changed.len(), 1);
        assert_eq!(changed[0].removed.len(), 250);
        assert_eq!(changed[0].created.len(), 0);
    }

    #[tokio::test]
    async fn default_listener_impls_are_successful_no_ops() {
        let listener = DefaultListener;
        let (spec, ..) = table_spec("select");

        assert_eq!(
            listener
                .grants_changed(GrantsChangedEvent::new(
                    Vec::new(),
                    vec![spec],
                    request_metadata()
                ))
                .await
                .map_err(|e| e.to_string()),
            Ok(())
        );
    }

    /// `is_empty` is what keeps an unchanged re-apply from costing any listener work, so
    /// it must be false the moment either side has an entry.
    #[test]
    fn an_event_is_empty_only_when_both_sides_are() {
        let (spec, ..) = table_spec("create_table");
        let metadata = request_metadata();

        assert!(GrantsChangedEvent::new(Vec::new(), Vec::new(), metadata.clone()).is_empty());
        assert!(
            !GrantsChangedEvent::new(Vec::new(), vec![spec.clone()], metadata.clone()).is_empty()
        );
        assert!(
            !GrantsChangedEvent::new(vec![spec.clone()], Vec::new(), metadata.clone()).is_empty()
        );

        let event = GrantsChangedEvent::new(vec![spec.clone()], vec![spec], metadata.clone());
        assert!(!event.is_empty());
        assert_eq!(event.request_metadata.request_id(), metadata.request_id());
    }
}
