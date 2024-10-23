//! Contains Configuration of the service Module
use anyhow::anyhow;
use itertools::Itertools;
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::HashSet;
use std::convert::Infallible;
use std::ops::{Deref, DerefMut};
use std::path::PathBuf;
use std::str::FromStr;
use url::Url;
use veil::Redact;

use crate::service::event_publisher::KafkaConfig;
use crate::service::task_queue::TaskQueueConfig;
use crate::WarehouseIdent;
const DEFAULT_RESERVED_NAMESPACES: [&str; 2] = ["system", "examples"];
const DEFAULT_ENCRYPTION_KEY: &str = "<This is unsafe, please set a proper key>";

lazy_static::lazy_static! {
    /// Configuration of the service module.
    pub static ref CONFIG: DynAppConfig = {
        let defaults = figment::providers::Serialized::defaults(DynAppConfig::default());
        let mut config = figment::Figment::from(defaults)
            .merge(figment::providers::Env::prefixed("ICEBERG_REST__").split("__"))
            .extract::<DynAppConfig>()
            .expect("Valid Configuration");

        config.reserved_namespaces.extend(DEFAULT_RESERVED_NAMESPACES.into_iter().map(str::to_string));

        // Fail early if the base_uri is not a valid URL
        config.s3_signer_uri_for_warehouse(WarehouseIdent::from(uuid::Uuid::new_v4()));
        config.base_uri_catalog();
        config.base_uri_management();
        if config.secret_backend == SecretBackend::Postgres && config.pg_encryption_key == DEFAULT_ENCRYPTION_KEY {
            tracing::warn!("THIS IS UNSAFE! Using default encryption key for secrets in postgres, please set a proper key using ICEBERG_REST__PG_ENCRYPTION_KEY environment variable.");
        }

        config
    };
}

#[derive(Clone, Deserialize, Serialize, PartialEq, Redact)]
/// Configuration of this Module
pub struct DynAppConfig {
    /// Base URL for this REST Catalog.
    /// This is used as the "uri" and "s3.signer.url"
    /// while generating the Catalog Config
    pub base_uri: url::Url,
    /// Port under which we serve metrics
    pub metrics_port: u16,
    /// Port to listen on.
    pub listen_port: u16,
    /// The default Project ID to use. We recommend setting this
    /// only for singe-project deployments. A single project
    /// can still contain multiple warehouses.
    pub default_project_id: Option<uuid::Uuid>,
    /// Template to obtain the "prefix" for a warehouse,
    /// may contain `{warehouse_id}` placeholder.
    ///
    /// If this prefix contains more path segments than the
    /// `warehouse_id`, make sure to strip them using a
    /// reverse proxy before routing to the catalog service.
    /// Example value: `{warehouse_id}`
    prefix_template: String,
    /// Reserved namespaces that cannot be created by users.
    /// This is used to prevent users to create certain
    /// (sub)-namespaces. By default, `system` and `examples` are
    /// reserved. More namespaces can be added here.
    #[serde(
        deserialize_with = "deserialize_reserved_namespaces",
        serialize_with = "serialize_reserved_namespaces"
    )]
    pub reserved_namespaces: ReservedNamespaces,
    // ------------- POSTGRES IMPLEMENTATION -------------
    #[redact]
    pub(crate) pg_encryption_key: String,
    pub(crate) pg_database_url_read: Option<String>,
    pub(crate) pg_database_url_write: Option<String>,
    pub(crate) pg_host_r: Option<String>,
    pub(crate) pg_host_w: Option<String>,
    pub(crate) pg_port: Option<u16>,
    pub(crate) pg_user: Option<String>,
    #[redact]
    pub(crate) pg_password: Option<String>,
    pub(crate) pg_database: Option<String>,
    pub(crate) pg_ssl_mode: Option<PgSslMode>,
    pub(crate) pg_ssl_root_cert: Option<PathBuf>,
    pub(crate) pg_enable_statement_logging: bool,
    pub(crate) pg_test_before_acquire: bool,
    pub(crate) pg_connection_max_lifetime: Option<u64>,
    pub pg_read_pool_connections: u32,
    pub pg_write_pool_connections: u32,

    // ------------- NATS CLOUDEVENTS -------------
    pub nats_address: Option<Url>,
    pub nats_topic: Option<String>,
    pub nats_creds_file: Option<PathBuf>,
    pub nats_user: Option<String>,
    #[redact]
    pub nats_password: Option<String>,
    #[redact]
    pub nats_token: Option<String>,

    // ------------- KAFKA CLOUDEVENTS -------------
    pub kafka_topic: Option<String>,
    pub kafka_key: Option<String>,
    pub kafka_config: Option<KafkaConfig>,
    // ------------- AUTHORIZATION -------------
    pub openid_provider_uri: Option<Url>,

    // ------------- Health -------------
    pub health_check_frequency_seconds: u64,
    pub health_check_jitter_millis: u64,

    // ------------- KV2 -------------
    pub kv2: Option<KV2Config>,
    // ------------- Secrets -------------
    pub secret_backend: SecretBackend,

    // ------------- Queues -------------
    pub queue_config: TaskQueueConfig,

    // ------------- Tabular -------------
    /// Delay in seconds after which a tabular will be deleted
    // TODO: make this an enum?
    #[serde(
        deserialize_with = "seconds_to_duration",
        serialize_with = "duration_to_seconds"
    )]
    pub default_tabular_expiration_delay_seconds: chrono::Duration,
}

pub(crate) fn seconds_to_duration<'de, D>(deserializer: D) -> Result<chrono::Duration, D::Error>
where
    D: Deserializer<'de>,
{
    let buf = String::deserialize(deserializer)?;

    Ok(chrono::Duration::seconds(
        i64::from_str(&buf).map_err(serde::de::Error::custom)?,
    ))
}

pub(crate) fn duration_to_seconds<S>(
    duration: &chrono::Duration,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    duration.num_seconds().to_string().serialize(serializer)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum SecretBackend {
    #[serde(alias = "kv2", alias = "Kv2")]
    KV2,
    #[serde(alias = "postgres")]
    Postgres,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Redact)]
pub struct KV2Config {
    pub url: Url,
    pub user: String,
    #[redact]
    pub password: String,
    pub secret_mount: String,
}

impl Default for DynAppConfig {
    fn default() -> Self {
        Self {
            base_uri: "https://localhost:8080".parse().expect("Valid URL"),
            metrics_port: 9000,
            default_project_id: None,
            prefix_template: "{warehouse_id}".to_string(),
            reserved_namespaces: ReservedNamespaces(HashSet::from([
                "system".to_string(),
                "examples".to_string(),
            ])),
            pg_encryption_key: DEFAULT_ENCRYPTION_KEY.to_string(),
            pg_database_url_read: None,
            pg_database_url_write: None,
            pg_host_r: None,
            pg_host_w: None,
            pg_port: None,
            pg_user: None,
            pg_password: None,
            pg_database: None,
            pg_ssl_mode: None,
            pg_ssl_root_cert: None,
            pg_enable_statement_logging: false,
            pg_test_before_acquire: false,
            pg_connection_max_lifetime: None,
            pg_read_pool_connections: 10,
            pg_write_pool_connections: 5,
            nats_address: None,
            nats_topic: None,
            nats_creds_file: None,
            nats_user: None,
            nats_password: None,
            nats_token: None,
            kafka_config: None,
            kafka_key: None,
            kafka_topic: None,
            openid_provider_uri: None,
            listen_port: 8080,
            health_check_frequency_seconds: 10,
            health_check_jitter_millis: 500,
            kv2: None,
            secret_backend: SecretBackend::Postgres,
            queue_config: TaskQueueConfig::default(),
            default_tabular_expiration_delay_seconds: chrono::Duration::days(7),
        }
    }
}

impl DynAppConfig {
    pub fn s3_signer_uri_for_warehouse(&self, warehouse_id: WarehouseIdent) -> url::Url {
        self.base_uri
            .join(&format!("catalog/v1/{warehouse_id}"))
            .expect("Valid URL")
    }

    pub fn base_uri_catalog(&self) -> url::Url {
        self.base_uri.join("catalog").expect("Valid URL")
    }

    pub fn base_uri_management(&self) -> url::Url {
        self.base_uri.join("management").expect("Valid URL")
    }

    pub fn warehouse_prefix(&self, warehouse_id: WarehouseIdent) -> String {
        self.prefix_template
            .replace("{warehouse_id}", warehouse_id.to_string().as_str())
    }

    pub fn tabular_expiration_delay(&self) -> chrono::Duration {
        self.default_tabular_expiration_delay_seconds
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum PgSslMode {
    Disable,
    Allow,
    Prefer,
    Require,
    VerifyCa,
    VerifyFull,
}

impl From<PgSslMode> for sqlx::postgres::PgSslMode {
    fn from(value: PgSslMode) -> Self {
        match value {
            PgSslMode::Disable => sqlx::postgres::PgSslMode::Disable,
            PgSslMode::Allow => sqlx::postgres::PgSslMode::Allow,
            PgSslMode::Prefer => sqlx::postgres::PgSslMode::Prefer,
            PgSslMode::Require => sqlx::postgres::PgSslMode::Require,
            PgSslMode::VerifyCa => sqlx::postgres::PgSslMode::VerifyCa,
            PgSslMode::VerifyFull => sqlx::postgres::PgSslMode::VerifyFull,
        }
    }
}

impl FromStr for PgSslMode {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_ref() {
            "disabled" => Ok(Self::Disable),
            "allow" => Ok(Self::Allow),
            "prefer" => Ok(Self::Prefer),
            "require" => Ok(Self::Require),
            "verifyca" => Ok(Self::VerifyCa),
            "verifyfull" => Ok(Self::VerifyFull),
            _ => Err(anyhow!("PgSslMode not supported: '{}'", s)),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReservedNamespaces(HashSet<String>);
impl Deref for ReservedNamespaces {
    type Target = HashSet<String>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for ReservedNamespaces {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
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

fn deserialize_reserved_namespaces<'de, D>(deserializer: D) -> Result<ReservedNamespaces, D::Error>
where
    D: Deserializer<'de>,
{
    let buf = String::deserialize(deserializer)?;

    ReservedNamespaces::from_str(&buf).map_err(serde::de::Error::custom)
}

fn serialize_reserved_namespaces<S>(
    value: &ReservedNamespaces,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    value.0.iter().join(",").serialize(serializer)
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
