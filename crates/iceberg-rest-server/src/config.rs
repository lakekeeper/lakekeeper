//! Contains Configuration of the SAGA Module
use clap::Parser;
use std::collections::HashSet;
use std::convert::Infallible;
use std::ops::Deref;
use std::str::FromStr;

use crate::WarehouseIdent;

#[derive(Debug, Clone, serde::Deserialize, PartialEq, Parser)]
#[allow(clippy::module_name_repetitions)]
/// Configuration of this Module
pub struct DynAppConfig {
    /// Base URL for this REST Catalog.
    /// This is used as the "uri" and "s3.signer.url"
    /// while generating the Catalog Config
    #[clap(
        long,
        env = "ICEBERG_REST__BASE_URI",
        default_value = "https://localhost:8080/catalog/"
    )]
    pub base_uri: url::Url,
    /// Template to obtain the "prefix" for a warehouse,
    /// may contain `{warehouse_id}` placeholder.
    ///
    /// If this prefix contains more path segments than the
    /// `warehouse_id`, make sure to strip them using a
    /// reverse proxy before routing to the catalog service.
    /// Example value: `{warehouse_id}`
    #[clap(
        long,
        env = "ICEBERG_REST__PREFIX_TEMPLATE",
        default_value = "{warehouse_id}"
    )]
    prefix_template: String,
    /// Reserved namespaces that cannot be created by users.
    /// This is used to prevent users to create certain
    /// (sub)-namespaces. By default, `system` and `examples` are
    /// reserved. More namespaces can be added here.
    #[clap(
        long,
        env = "ICEBERG_REST__RESERVED_NAMESPACES",
        default_value = "system,examples"
    )]
    pub reserved_namespaces: ReservedNamespaces,
    // ------------- POSTGRES IMPLEMENTATION -------------
    #[clap(
        long,
        env = "ICEBERG_REST__PG_ENCRYPTION_KEY",
        default_value = "<This is unsafe, please set a proper key>"
    )]
    pub(crate) pg_encryption_key: String,
    #[clap(
        long,
        env = "ICEBERG_REST__PG_DATABASE_URL_READ",
        default_value = "postgres://postgres:password@localhost:5432/iceberg"
    )]
    pub(crate) pg_database_url_read: String,
    #[clap(
        long,
        env = "ICEBERG_REST__PG_DATABASE_URL_WRITE",
        default_value = "postgres://postgres:password@localhost:5432/iceberg"
    )]
    pub(crate) pg_database_url_write: String,
    #[clap(
        long,
        env = "ICEBERG_REST__PG_READ_POOL_CONNECTIONS",
        default_value = "10"
    )]
    pub pg_read_pool_connections: u32,
    #[clap(
        long,
        env = "ICEBERG_REST__PG_WRITE_POOL_CONNECTIONS",
        default_value = "5"
    )]
    pub pg_write_pool_connections: u32,
}
#[derive(Debug, Clone, serde::Deserialize, PartialEq)]
pub struct ReservedNamespaces(HashSet<String>);
impl Deref for ReservedNamespaces {
    type Target = HashSet<String>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl FromStr for ReservedNamespaces {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(ReservedNamespaces(
            s.split(',').map(str::to_string).collect(),
        ))
    }
}

impl DynAppConfig {
    // pub fn s3_signer_uri_for_table(
    //     &self,
    //     warehouse_id: &WarehouseIdent,
    //     namespace_id: &NamespaceIdentUuid,
    //     table_id: &TableIdentUuid,
    // ) -> url::Url {
    //     self.base_uri
    //         .join(&format!(
    //             "v1/{warehouse_id}/namespace/{namespace_id}/table/{table_id}"
    //         ))
    //         .expect("Valid URL")
    // }

    pub fn s3_signer_uri_for_warehouse(&self, warehouse_id: &WarehouseIdent) -> url::Url {
        self.base_uri
            .join(&format!("v1/{warehouse_id}"))
            .expect("Valid URL")
    }

    pub fn warehouse_prefix(&self, warehouse_id: &WarehouseIdent) -> String {
        self.prefix_template
            .replace("{warehouse_id}", warehouse_id.to_string().as_str())
    }
}

lazy_static::lazy_static! {
    #[derive(Debug)]
    /// Configuration of the SAGA Module
    pub static ref CONFIG: DynAppConfig = {
        DynAppConfig::parse()
    };
}

#[cfg(test)]
mod test {
    #[allow(unused_imports)]
    use super::*;

    #[test]
    fn test_default() {
        let _ = &CONFIG.base_uri;
    }

    #[test]
    fn reserved_namespaces_should_contains_default_values() {
        assert!(CONFIG.reserved_namespaces.contains("system"));
        assert!(CONFIG.reserved_namespaces.contains("examples"));
    }
}
