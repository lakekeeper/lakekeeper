//! Contains Configuration of the service Module
use anyhow::{anyhow, Context};
use http::HeaderValue;
use std::collections::HashSet;
use std::convert::Infallible;
use std::ops::{Deref, DerefMut};
use std::path::PathBuf;
use std::str::FromStr;
use url::Url;

use crate::service::task_queue::TaskQueueConfig;
use crate::{ProjectIdent, WarehouseIdent};
use itertools::Itertools;
use serde::{Deserialize, Deserializer, Serialize};
use veil::Redact;

const DEFAULT_RESERVED_NAMESPACES: [&str; 2] = ["system", "examples"];
const DEFAULT_ENCRYPTION_KEY: &str = "<This is unsafe, please set a proper key>";

lazy_static::lazy_static! {
    /// Configuration of the service module.
    pub static ref CONFIG: DynAppConfig = {
        get_config()
    };
    pub static ref DEFAULT_PROJECT_ID: Option<ProjectIdent> = {
        CONFIG.enable_default_project.then_some(uuid::Uuid::nil().into())
    };
}

fn get_config() -> DynAppConfig {
    let defaults = figment::providers::Serialized::defaults(DynAppConfig::default());

    #[cfg(not(test))]
    let prefixes = &["ICEBERG_REST__", "LAKEKEEPER__"];
    #[cfg(test)]
    let prefixes = &["LAKEKEEPER_TEST__"];

    let mut config = figment::Figment::from(defaults);
    for prefix in prefixes {
        config = config.merge(figment::providers::Env::prefixed(prefix).split("__"));
    }

    let mut config = config
        .extract::<DynAppConfig>()
        .expect("Valid Configuration");

    config
        .reserved_namespaces
        .extend(DEFAULT_RESERVED_NAMESPACES.into_iter().map(str::to_string));

    // Fail early if the base_uri is not a valid URL
    config.s3_signer_uri_for_warehouse(WarehouseIdent::from(uuid::Uuid::new_v4()));
    config.base_uri_catalog();
    config.base_uri_management();
    if config.secret_backend == SecretBackend::Postgres
        && config.pg_encryption_key == DEFAULT_ENCRYPTION_KEY
    {
        tracing::warn!("THIS IS UNSAFE! Using default encryption key for secrets in postgres, please set a proper key using ICEBERG_REST__PG_ENCRYPTION_KEY environment variable.");
    }

    config
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
    /// If true (default), the NIL uuid is used as default project id.
    pub enable_default_project: bool,
    /// Template to obtain the "prefix" for a warehouse,
    /// may contain `{warehouse_id}` placeholder.
    ///
    /// If this prefix contains more path segments than the
    /// `warehouse_id`, make sure to strip them using a
    /// reverse proxy before routing to the catalog service.
    /// Example value: `{warehouse_id}`
    prefix_template: String,
    /// CORS allowed origins. If not set, CORS is disabled.
    #[serde(
        deserialize_with = "deserialize_origin",
        serialize_with = "serialize_origin"
    )]
    pub allow_origin: Option<Vec<HeaderValue>>,
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

    // ------------- AUTHENTICATION -------------
    pub openid_provider_uri: Option<Url>,

    // ------------- AUTHORIZATION - OPENFGA -------------
    #[serde(default)]
    pub authz_backend: AuthZBackend,
    #[serde(
        deserialize_with = "deserialize_openfga_config",
        serialize_with = "serialize_openfga_config"
    )]
    pub openfga: Option<OpenFGAConfig>,

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
    #[serde(
        deserialize_with = "seconds_to_duration",
        serialize_with = "duration_to_seconds"
    )]
    pub default_tabular_expiration_delay_seconds: chrono::Duration,

    // ------------- Internal -------------
    /// Optional server id. We recommend to not change this unless multiple catalogs
    /// are sharing the same Authorization system.
    /// If not specified, 00000000-0000-0000-0000-000000000000 is used.
    /// This ID may not be changed after start!
    #[serde(default = "uuid::Uuid::nil")]
    pub server_id: uuid::Uuid,
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

fn deserialize_origin<'de, D>(deserializer: D) -> Result<Option<Vec<HeaderValue>>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::deserialize(deserializer)?
        .map(|buf: String| {
            buf.split(',')
                .map(|s| HeaderValue::from_str(s).map_err(serde::de::Error::custom))
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()
}

fn serialize_origin<S>(value: &Option<Vec<HeaderValue>>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    value
        .as_deref()
        .map(|value| {
            value
                .iter()
                .map(|hv| hv.to_str().context("Couldn't serialize cors header"))
                .collect::<anyhow::Result<Vec<_>>>()
                .map(|inner| inner.join(","))
        })
        .transpose()
        .map_err(serde::ser::Error::custom)?
        .serialize(serializer)
}

#[derive(Clone, Serialize, Deserialize, PartialEq, veil::Redact)]
#[serde(rename_all = "snake_case")]
pub enum OpenFGAAuth {
    Anonymous,
    ClientCredentials {
        client_id: String,
        #[redact]
        client_secret: String,
        token_endpoint: String,
    },
    #[redact(all)]
    ApiKey(String),
}

impl Default for OpenFGAAuth {
    fn default() -> Self {
        Self::Anonymous
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
pub struct OpenFGAConfig {
    /// GRPC Endpoint Url
    pub endpoint: Url,
    /// Store Name - if not specified, `lakekeeper` is used.
    #[serde(default = "default_openfga_store_name")]
    pub store_name: String,
    /// Authentication configuration
    #[serde(default)]
    pub auth: OpenFGAAuth,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum AuthZBackend {
    #[serde(alias = "allowall", alias = "AllowAll", alias = "ALLOWALL")]
    AllowAll,
    #[serde(alias = "openfga", alias = "OpenFGA", alias = "OPENFGA")]
    OpenFGA,
}

impl Default for AuthZBackend {
    fn default() -> Self {
        Self::AllowAll
    }
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
            enable_default_project: true,
            prefix_template: "{warehouse_id}".to_string(),
            allow_origin: None,
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
            openid_provider_uri: None,
            listen_port: 8080,
            health_check_frequency_seconds: 10,
            health_check_jitter_millis: 500,
            kv2: None,
            authz_backend: AuthZBackend::AllowAll,
            openfga: None,
            secret_backend: SecretBackend::Postgres,
            queue_config: TaskQueueConfig::default(),
            default_tabular_expiration_delay_seconds: chrono::Duration::days(7),
            server_id: uuid::Uuid::nil(),
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

#[derive(Serialize, Deserialize, PartialEq, veil::Redact)]
struct OpenFGAConfigSerde {
    /// GRPC Endpoint Url
    endpoint: Url,
    /// Store Name - if not specified, `lakekeeper` is used.
    #[serde(default = "default_openfga_store_name")]
    store_name: String,
    /// API-Key. If client-id is specified, this is ignored.
    api_key: Option<String>,
    /// Client id
    client_id: Option<String>,
    #[redact]
    /// Client secret
    client_secret: Option<String>,
    /// Token Endpoint to use when exchanging client credentials for an access token.
    token_endpoint: Option<String>,
}

fn default_openfga_store_name() -> String {
    "lakekeeper".to_string()
}

fn deserialize_openfga_config<'de, D>(deserializer: D) -> Result<Option<OpenFGAConfig>, D::Error>
where
    D: Deserializer<'de>,
{
    let Some(OpenFGAConfigSerde {
        client_id,
        client_secret,
        token_endpoint,
        api_key,
        endpoint,
        store_name,
    }) = Option::<OpenFGAConfigSerde>::deserialize(deserializer)?
    else {
        return Ok(None);
    };

    let auth = if let Some(client_id) = client_id {
        let client_secret = client_secret.ok_or_else(|| {
            serde::de::Error::custom(
                "openfga client_secret is required when client_id is specified",
            )
        })?;
        let token_endpoint = token_endpoint.ok_or_else(|| {
            serde::de::Error::custom(
                "openfga token_endpoint is required when client_id is specified",
            )
        })?;
        OpenFGAAuth::ClientCredentials {
            client_id,
            client_secret,
            token_endpoint,
        }
    } else {
        api_key.map_or(OpenFGAAuth::Anonymous, OpenFGAAuth::ApiKey)
    };

    Ok(Some(OpenFGAConfig {
        endpoint,
        store_name,
        auth,
    }))
}

fn serialize_openfga_config<S>(
    value: &Option<OpenFGAConfig>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let Some(value) = value else {
        return None::<OpenFGAConfigSerde>.serialize(serializer);
    };

    let (client_id, client_secret, token_endpoint, api_key) = match &value.auth {
        OpenFGAAuth::ClientCredentials {
            client_id,
            client_secret,
            token_endpoint,
        } => (
            Some(client_id),
            Some(client_secret),
            Some(token_endpoint),
            None,
        ),
        OpenFGAAuth::ApiKey(api_key) => (None, None, None, Some(api_key.clone())),
        OpenFGAAuth::Anonymous => (None, None, None, None),
    };

    OpenFGAConfigSerde {
        client_id: client_id.cloned(),
        client_secret: client_secret.cloned(),
        token_endpoint: token_endpoint.cloned(),
        api_key,
        endpoint: value.endpoint.clone(),
        store_name: value.store_name.clone(),
    }
    .serialize(serializer)
}

#[cfg(test)]
mod test {
    #[allow(unused_imports)]
    use super::*;

    #[test]
    fn test_wildcard_allow_origin() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__ALLOW_ORIGIN", "*");
            let config = get_config();
            assert_eq!(
                config.allow_origin,
                Some(vec![HeaderValue::from_str("*").unwrap()])
            );
            Ok(())
        });
    }

    #[test]
    fn test_default() {
        let _ = &CONFIG.base_uri;
    }

    #[test]
    fn reserved_namespaces_should_contains_default_values() {
        assert!(CONFIG.reserved_namespaces.contains("system"));
        assert!(CONFIG.reserved_namespaces.contains("examples"));
    }

    #[test]
    fn test_openfga_config_no_auth() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__AUTHZ_BACKEND", "openfga");
            jail.set_env("LAKEKEEPER_TEST__OPENFGA__STORE_NAME", "store_name");
            let config = get_config();
            let authz_config = config.openfga.unwrap();
            assert_eq!(config.authz_backend, AuthZBackend::OpenFGA);
            assert_eq!(authz_config.store_name, "store_name");

            assert_eq!(authz_config.auth, OpenFGAAuth::Anonymous);

            Ok(())
        });
    }

    #[test]
    fn test_openfga_config_api_key() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__AUTHZ_BACKEND", "openfga");
            jail.set_env("LAKEKEEPER_TEST__OPENFGA__API_KEY", "api_key");
            let config = get_config();
            let authz_config = config.openfga.unwrap();
            assert_eq!(config.authz_backend, AuthZBackend::OpenFGA);
            assert_eq!(authz_config.store_name, "lakekeeper");

            assert_eq!(
                authz_config.auth,
                OpenFGAAuth::ApiKey("api_key".to_string())
            );
            Ok(())
        });
    }

    #[test]
    #[should_panic(expected = "openfga client_secret is required when client_id is specified")]
    fn test_openfga_client_config_fails_without_token() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__AUTHZ_BACKEND", "openfga");
            jail.set_env("LAKEKEEPER_TEST__OPENFGA__CLIENT_ID", "client_id");
            jail.set_env("LAKEKEEPER_TEST__OPENFGA__STORE_NAME", "store_name");
            get_config();
            Ok(())
        });
    }

    #[test]
    fn test_openfga_client_credentials() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__AUTHZ_BACKEND", "openfga");
            jail.set_env("LAKEKEEPER_TEST__OPENFGA__CLIENT_ID", "client_id");
            jail.set_env("LAKEKEEPER_TEST__OPENFGA__CLIENT_SECRET", "client_secret");
            jail.set_env("LAKEKEEPER_TEST__OPENFGA__TOKEN_ENDPOINT", "token_endpoint");
            let config = get_config();
            let authz_config = config.openfga.unwrap();
            assert_eq!(config.authz_backend, AuthZBackend::OpenFGA);
            assert_eq!(authz_config.store_name, "lakekeeper");

            assert_eq!(
                authz_config.auth,
                OpenFGAAuth::ClientCredentials {
                    client_id: "client_id".to_string(),
                    client_secret: "client_secret".to_string(),
                    token_endpoint: "token_endpoint".to_string()
                }
            );
            Ok(())
        });
    }
}
