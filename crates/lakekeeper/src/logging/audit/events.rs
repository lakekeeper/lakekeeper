use crate::{
    WarehouseId,
    api::management::v1::user::UserType,
    logging::audit::{AUDIT_LOG_EVENT_SOURCE, AuditContextData, AuditEvent},
    service::{TableId, UserId, authz::CatalogTableAction},
};

#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct AccessEndpointEvent {
    pub endpoint: String,
    pub method: String,
}

#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct BootstrapFailedEvent {
    pub error: String,
}

#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct BootstrapCreateUserEvent {
    pub user_name: String,
    pub user_id: UserId,
    pub user_type: UserType,
    pub user_email: String,
}

#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct CommitTablesAccessTabularEvent {
    pub warehouse_id: WarehouseId,
    pub table_id: TableId,
    pub action: CatalogTableAction,
}

#[derive(Debug, lakekeeper_logging_derive::AuditEvent)]
pub struct CommitTablesFailedEvent {
    pub error: String,
}
