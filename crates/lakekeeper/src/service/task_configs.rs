use crate::{ProjectId, WarehouseId};

#[derive(Debug, Clone, PartialEq)]
pub enum TaskQueueConfigFilter {
    WarehouseId {
        warehouse_id: WarehouseId,
        project_id: ProjectId,
    },
    ProjectId {
        project_id: ProjectId,
    },
}
