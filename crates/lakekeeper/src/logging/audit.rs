use crate::api::RequestMetadata;

pub const AUDIT_LOG_EVENT_SOURCE: &str = "audit";

pub trait AuditEvent {
    fn action(&self) -> &'static str;
    fn log<D: AuditContextData>(&self, ctx: &D);
}

pub trait AuditContext {
    fn log_audit<E: AuditEvent>(&self, event: E);
}

pub trait AuditContextData {
    fn request_metadata(&self) -> &RequestMetadata;
}
