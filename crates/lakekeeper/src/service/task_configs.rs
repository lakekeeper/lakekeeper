use crate::{WarehouseId, service::ArcProjectId};

#[derive(Debug, Clone, PartialEq)]
pub enum TaskQueueConfigFilter {
    WarehouseId { warehouse_id: WarehouseId },
    ProjectId { project_id: ArcProjectId },
}
