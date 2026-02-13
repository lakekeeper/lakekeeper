use std::fmt::Display;

use valuable::Valuable;

use crate::service::events::{
    AuthorizationFailedEvent, AuthorizationSucceededEvent, EventListener,
};

#[derive(Debug)]
pub struct AuditEventListener;

impl Display for AuditEventListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AuditEventListener")
    }
}

#[async_trait::async_trait]
impl EventListener for AuditEventListener {
    async fn authorization_failed(&self, event: AuthorizationFailedEvent) -> anyhow::Result<()> {
        tracing::info!(
            event_source = "audit",
            action = event.action,
            entity = event.entity.as_value(),
            actor = event.request_metadata.actor().as_value(),
            request_id = event.request_metadata.request_id().to_string(),
            failure_reason = event.failure_reason.as_value(),
            error = event.error.as_value(),
            extra_context = event.extra_context.as_value(),
            "Authorization failed event"
        );
        Ok(())
    }

    async fn authorization_succeeded(
        &self,
        event: AuthorizationSucceededEvent,
    ) -> anyhow::Result<()> {
        tracing::info!(
            event_source = "audit",
            action = event.action,
            entity = event.entity.as_value(),
            actor = event.request_metadata.actor().as_value(),
            request_id = event.request_metadata.request_id().to_string(),
            extra_context = event.extra_context.as_value(),
            "Authorization succeeded event"
        );
        Ok(())
    }
}
