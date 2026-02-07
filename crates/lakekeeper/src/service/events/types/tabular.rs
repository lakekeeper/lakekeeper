use std::sync::Arc;

use crate::{
    WarehouseId,
    api::{RequestMetadata, management::v1::warehouse::UndropTabularsRequest},
    service::ViewOrTableInfo,
};

// ===== Tabular Events =====

/// Event emitted when tables or views are undeleted
#[derive(Clone, Debug)]
pub struct UndropTabularEvent {
    pub warehouse_id: WarehouseId,
    pub request: Arc<UndropTabularsRequest>,
    pub responses: Arc<Vec<ViewOrTableInfo>>,
    pub request_metadata: Arc<RequestMetadata>,
}
