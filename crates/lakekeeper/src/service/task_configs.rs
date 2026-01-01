use crate::{ProjectId, WarehouseId};

#[derive(Debug, Clone, PartialEq)]
pub enum TaskQueueConfigFilter {
    WarehouseId { warehouse_id: WarehouseId },
    ProjectId { project_id: ProjectId },
}
