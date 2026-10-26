pub mod data;
pub mod iceberg;
pub mod management;

pub mod endpoints;
#[cfg(feature = "router")]
pub mod maintenance;
#[cfg(feature = "router")]
pub mod router;
pub use iceberg_ext::catalog::rest::*;

#[cfg(any(test, feature = "test-utils"))]
pub use crate::request_metadata::RequestMetadataTestBuilder;
pub use crate::request_metadata::{
    ArcRequestMetadata, RequestMetadata, X_BREAK_GLASS_HEADER, X_PROJECT_ID_HEADER,
    X_REQUEST_ID_HEADER,
};

// Used only to group required traits for a State
pub trait ThreadSafe: Clone + Send + Sync + 'static {}

#[derive(Debug, Clone)]
pub struct ApiContext<S: ThreadSafe> {
    pub v1_state: S,
}

pub type Result<T, E = IcebergErrorResponse> = std::result::Result<T, E>;
