use crate::{
    api::management::v1::user::UserType,
    logging::audit::{AUDIT_LOG_EVENT_SOURCE, AuditContextData, AuditEvent},
    service::UserId,
};

#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct AccessEndpointEvent {
    pub endpoint: String,
    pub method: String,
}

#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct BootstrapFailedEvent {
    pub error: String
}

#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct BootstrapCreateUserEvent {
    pub user_name: String,
    pub user_id: UserId,
    pub user_type: UserType,
    pub user_email: String,
}
