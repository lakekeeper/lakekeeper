//! Data plane API.
//!
//! Catalog-style endpoints for non-Iceberg table formats. Mounted at
//! `/lakekeeper/v1/*` (see [`crate::api::router`]). Parallel to
//! [`crate::api::iceberg`] (the Iceberg REST API at `/catalog/v1/*`) and
//! [`crate::api::management`] (Lakekeeper administrative API at
//! `/management/v1/*`).
pub mod v1;
