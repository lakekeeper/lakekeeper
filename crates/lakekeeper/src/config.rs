//! Contains Configuration of the service Module
#![allow(clippy::ref_option)]

use core::result::Result::Ok;
use std::{
    collections::{HashMap, HashSet},
    convert::Infallible,
    net::{IpAddr, Ipv4Addr},
    ops::{Deref, DerefMut},
    path::PathBuf,
    str::FromStr,
    sync::{Arc, LazyLock},
    time::Duration,
};

use anyhow::{Context, anyhow};
use figment::value::Uncased;
use http::HeaderValue;
use itertools::Itertools;
use serde::{Deserialize, Deserializer, Serialize};
use url::Url;
use veil::Redact;

use crate::{
    WarehouseId,
    service::{ArcProjectId, UserId},
};

const DEFAULT_RESERVED_NAMESPACES: [&str; 3] = ["system", "examples", "information_schema"];
const DEFAULT_ENCRYPTION_KEY: &str = "<This is unsafe, please set a proper key>";

pub static CONFIG: LazyLock<DynAppConfig> = LazyLock::new(get_config);
pub static DEFAULT_PROJECT_ID: LazyLock<Option<ArcProjectId>> = LazyLock::new(|| {
    CONFIG
        .enable_default_project
        .then_some(Arc::new(uuid::Uuid::nil().into()))
});

fn get_config() -> DynAppConfig {
    let defaults = figment::providers::Serialized::defaults(DynAppConfig::default());

    #[cfg(not(test))]
    let prefixes = &["ICEBERG_REST__", "LAKEKEEPER__"];
    #[cfg(test)]
    let prefixes = &["LAKEKEEPER_TEST__"];

    let file_keys = &["kafka_config"];

    let config_keys_map = &[("METRICS_PORT", "METRICS__PORT")];

    let mut config = figment::Figment::from(defaults);
    for prefix in prefixes {
        let env = figment::providers::Env::prefixed(prefix)
            .map(|env_key| {
                config_keys_map
                    .iter()
                    .find_map(|(k, v)| {
                        if *k == env_key {
                            Some(Uncased::from_borrowed(v))
                        } else {
                            None
                        }
                    })
                    .unwrap_or(env_key.into())
            })
            .split("__");
        config = config
            .merge(figment_file_provider_adapter::FileAdapter::wrap(env.clone()).only(file_keys))
            .merge(env);
    }

    let mut config = config
        .extract::<DynAppConfig>()
        .expect("Valid Configuration");

    // Ensure base_uri has a trailing slash
    if let Some(base_uri) = config.base_uri.as_mut() {
        let base_uri_path = base_uri.path().to_string();
        base_uri.set_path(&format!("{}/", base_uri_path.trim_end_matches('/')));
    }

    config
        .reserved_namespaces
        .extend(DEFAULT_RESERVED_NAMESPACES.into_iter().map(str::to_string));

    for (name, engine) in &config.trusted_engines {
        assert!(
            !engine.owner_property().trim().is_empty(),
            "Invalid trusted engine '{name}': owner_property must not be empty"
        );
        for (idp_id, identity) in engine.identities() {
            assert!(
                !idp_id.trim().is_empty(),
                "Invalid trusted engine '{name}': identity IdP ID must not be empty"
            );
            assert!(
                !identity.audiences.is_empty() || !identity.subjects.is_empty(),
                "Invalid trusted engine '{name}', identity '{idp_id}': \
                 at least one audience or subject must be configured"
            );
            assert!(
                identity.audiences.iter().all(|a| !a.trim().is_empty()),
                "Invalid trusted engine '{name}', identity '{idp_id}': \
                 audiences must not contain empty strings"
            );
            assert!(
                identity.subjects.iter().all(|s| !s.trim().is_empty()),
                "Invalid trusted engine '{name}', identity '{idp_id}': \
                 subjects must not contain empty strings"
            );
        }
    }
    config.protected_properties = config
        .trusted_engines
        .values()
        .map(|e| e.owner_property().to_string())
        .collect();

    // Fail early if the base_uri is not a valid URL
    if let Some(uri) = &config.base_uri {
        uri.join("catalog").expect("Valid URL");
        uri.join("management").expect("Valid URL");
    }

    if config.secret_backend == SecretBackend::Postgres
        && config.pg_encryption_key == DEFAULT_ENCRYPTION_KEY
    {
        tracing::warn!(
            "THIS IS UNSAFE! Using default encryption key for secrets in postgres, please set a proper key using ICEBERG_REST__PG_ENCRYPTION_KEY environment variable."
        );
    }

    // `UserAssignmentsCache` entries may reference roles that are still live
    // in the role cache.  If `user_assignments.time_to_live_secs` exceeds
    // `role.time_to_live_secs` a deleted role can remain visible through
    // user-assignment cache entries after it has been evicted from the role
    // cache, violating the documented invariant.
    // The constraint is only meaningful when both caches are active; if either
    // is disabled the TTL relationship has no effect at runtime.
    if config.cache.user_assignments.enabled && config.cache.role.enabled {
        assert!(
            config.cache.user_assignments.time_to_live_secs <= config.cache.role.time_to_live_secs,
            "Invalid cache configuration: user_assignments.time_to_live_secs ({}) must not exceed role.time_to_live_secs ({})",
            config.cache.user_assignments.time_to_live_secs,
            config.cache.role.time_to_live_secs,
        );
    }

    config
}

/// Identifies who is trusted to act as this engine from a specific `IdP`.
///
/// The map key (not part of this struct) is the `IdP` ID.
/// A token matches if:
/// - the map key matches the token's `IdP` ID, AND
/// - any configured `audience` appears in the token's audiences,
///   OR any configured `subject` matches the token's subject.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct EngineIdentity {
    #[serde(default)]
    pub audiences: Vec<String>,
    #[serde(default)]
    pub subjects: Vec<String>,
}

impl EngineIdentity {
    /// Check whether a token with the given audiences and subject matches this identity.
    #[must_use]
    pub fn matches(&self, token_audiences: &HashSet<&str>, token_subject: Option<&str>) -> bool {
        let audience_match = self
            .audiences
            .iter()
            .any(|a| token_audiences.contains(a.as_str()));
        let subject_match = token_subject.is_some_and(|sub| self.subjects.iter().any(|s| s == sub));
        audience_match || subject_match
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct TrinoEngineConfig {
    pub owner_property: String,
    /// Map from `IdP` ID to identity configuration.
    #[serde(default)]
    pub identities: HashMap<String, EngineIdentity>,
}

impl TrinoEngineConfig {
    #[must_use]
    pub fn determine_security_model(&self, properties: &HashMap<String, String>) -> SecurityModel {
        if let Some(owner) = properties.get(&self.owner_property) {
            SecurityModel::Definer(owner.clone())
        } else {
            SecurityModel::Invoker
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum TrustedEngine {
    Trino(TrinoEngineConfig),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SecurityModel {
    Invoker,
    Definer(String),
}

/// Multiple matched engines resolved to different owners for the same view.
#[derive(Debug, Clone, thiserror::Error)]
#[error("Ambiguous security model: multiple engines resolve to different owners")]
pub struct AmbiguousSecurityModel {
    pub owners: Vec<String>,
}

impl TrustedEngine {
    #[must_use]
    pub fn determine_security_model(&self, properties: &HashMap<String, String>) -> SecurityModel {
        match self {
            TrustedEngine::Trino(c) => c.determine_security_model(properties),
        }
    }

    #[must_use]
    pub fn owner_property(&self) -> &str {
        match self {
            TrustedEngine::Trino(c) => &c.owner_property,
        }
    }

    #[must_use]
    pub fn identities(&self) -> &HashMap<String, EngineIdentity> {
        match self {
            TrustedEngine::Trino(c) => &c.identities,
        }
    }
}

/// The set of trusted engines that matched the current request's token.
///
/// Consumers should use the high-level methods instead of iterating over engines.
#[derive(Debug, Clone, Default)]
pub struct MatchedEngines {
    engines: Vec<TrustedEngine>,
}

impl MatchedEngines {
    #[must_use]
    pub fn new(engines: Vec<TrustedEngine>) -> Self {
        Self { engines }
    }

    #[must_use]
    pub fn single(engine: TrustedEngine) -> Self {
        Self {
            engines: vec![engine],
        }
    }

    /// Whether the request comes from any trusted engine.
    #[must_use]
    pub fn is_trusted(&self) -> bool {
        !self.engines.is_empty()
    }

    /// Determine security model from view properties.
    ///
    /// Returns `Definer` if any matched engine's owner property is set.
    /// Returns an error if multiple engines resolve to different owners
    /// (ambiguous delegation).
    pub fn determine_security_model(
        &self,
        properties: &HashMap<String, String>,
    ) -> Result<SecurityModel, AmbiguousSecurityModel> {
        let mut found_owner: Option<String> = None;
        for engine in &self.engines {
            if let SecurityModel::Definer(owner) = engine.determine_security_model(properties) {
                if let Some(ref prev) = found_owner {
                    if *prev != owner {
                        return Err(AmbiguousSecurityModel {
                            owners: vec![prev.clone(), owner],
                        });
                    }
                } else {
                    found_owner = Some(owner);
                }
            }
        }
        Ok(found_owner.map_or(SecurityModel::Invoker, SecurityModel::Definer))
    }

    /// Whether this request is allowed to modify the given property.
    /// True if any matched engine's security model property matches.
    #[must_use]
    pub fn owns_property(&self, property: &str) -> bool {
        self.engines.iter().any(|e| e.owner_property() == property)
    }
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Deserialize, Serialize, Redact)]
/// Configuration of this Module
pub struct DynAppConfig {
    /// Base URL for this REST Catalog.
    /// This is used as the "uri" and "s3.signer.url"
    /// while generating the Catalog Config
    pub base_uri: Option<url::Url>,
    /// Port to listen on.
    pub listen_port: u16,
    /// Bind IP the server listens on.
    /// Defaults to 0.0.0.0
    pub bind_ip: IpAddr,
    /// If x-forwarded-x headers should be respected.
    /// Defaults to true
    pub use_x_forwarded_headers: bool,
    /// If true (default), the NIL uuid is used as default project id.
    pub enable_default_project: bool,
    /// If true, the swagger UI is served at /swagger-ui
    pub serve_swagger_ui: bool,
    /// Template to obtain the "prefix" for a warehouse,
    /// may contain `{warehouse_id}` placeholder.
    ///
    /// If this prefix contains more path segments than the
    /// `warehouse_id`, make sure to strip them using a
    /// reverse proxy before routing to the catalog service.
    /// Example value: `{warehouse_id}`
    prefix_template: String,
    /// CORS allowed origins.
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
    // ------------- STORAGE OPTIONS -------------
    /// If true, can create Warehouses with using System Identities.
    pub(crate) enable_aws_system_credentials: bool,
    /// If false, System Identities cannot be used directly to access files.
    /// Instead, `assume_role_arn` must be provided by the user if `SystemIdentities` are used.
    pub(crate) s3_enable_direct_system_credentials: bool,
    /// If true, users must set `external_id` when using system identities with
    /// `assume_role_arn`.
    pub(crate) s3_require_external_id_for_system_credentials: bool,

    /// Enable Azure System Identities
    pub(crate) enable_azure_system_credentials: bool,

    /// Enable GCP System Identities
    pub(crate) enable_gcp_system_credentials: bool,

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
    pub pg_acquire_timeout: u64,

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
    #[cfg(feature = "kafka")]
    pub kafka_config: Option<crate::service::events::backends::kafka::KafkaConfig>,

    // ------------- TRACING CLOUDEVENTS ----------
    pub log_cloudevents: Option<bool>,

    // ------------- AUTHENTICATION -------------
    pub openid_provider_uri: Option<Url>,
    /// Expected audience for the provided token.
    /// Specify multiple audiences as a comma-separated list.
    #[serde(
        deserialize_with = "deserialize_comma_separated",
        serialize_with = "serialize_comma_separated"
    )]
    pub openid_audience: Option<Vec<String>>,
    /// Additional issuers to trust for `OpenID` Connect
    #[serde(
        deserialize_with = "deserialize_comma_separated",
        serialize_with = "serialize_comma_separated"
    )]
    pub openid_additional_issuers: Option<Vec<String>>,
    /// A scope that must be present in provided tokens
    pub openid_scope: Option<String>,
    pub enable_kubernetes_authentication: bool,
    /// Audience expected in provided JWT tokens.
    #[serde(
        deserialize_with = "deserialize_comma_separated",
        serialize_with = "serialize_comma_separated"
    )]
    pub kubernetes_authentication_audience: Option<Vec<String>>,
    /// Accept legacy k8s token without audience and issuer
    /// set to kubernetes/serviceaccount or `https://kubernetes.default.svc.cluster.local`
    pub kubernetes_authentication_accept_legacy_serviceaccount: bool,
    /// Claim(s) to use in provided JWT tokens as the subject.
    /// Accepts a comma-separated list of claim names; the first claim present
    /// in the token is used. A single claim name (without a comma) is also
    /// accepted for backward compatibility.
    #[serde(
        deserialize_with = "deserialize_comma_separated",
        serialize_with = "serialize_comma_separated"
    )]
    pub openid_subject_claim: Option<Vec<String>>,
    /// Claim to use in provided JWT tokens to extract roles.
    /// The field should contain an array of strings or a single string.
    /// Supports nested claims using dot notation, e.g., `resource_access.account.roles`
    pub openid_roles_claim: Option<String>,

    // ------------- AUTHORIZATION - OPENFGA -------------
    #[serde(default)]
    pub authz_backend: AuthZBackend,

    /// Principals granted instance-admin privileges via deployment config.
    ///
    /// Instance admins bypass authorization for all control-plane actions
    /// (bootstrap, project/warehouse/role/namespace/table/view management) but
    /// NOT for data-plane actions (`CatalogTableAction::ReadData` /
    /// `WriteData`). The privilege cannot be revoked from within Lakekeeper at
    /// runtime; change the deployment config to add or remove admins.
    ///
    /// Accepts a TOML inline array of user IDs (each of form
    /// `<idp_id>~<subject>`) — for simple string arrays this is syntactically
    /// identical to JSON:
    ///
    /// ```text
    /// LAKEKEEPER__INSTANCE_ADMINS=["kubernetes~system:serviceaccount:lk:op","oidc~alice"]
    /// ```
    ///
    /// A bare string (e.g. `oidc~alice`) is rejected — even a single admin
    /// must be wrapped in brackets: `["oidc~alice"]`.
    #[serde(default)]
    pub instance_admins: HashSet<UserId>,
    // ------------- TRUSTED ENGINES -------------
    #[serde(default)]
    pub trusted_engines: HashMap<String, TrustedEngine>,
    /// Owner properties from all trusted engines, pre-computed at startup.
    #[serde(skip)]
    pub protected_properties: HashSet<String>,
    // ------------- Health -------------
    pub health_check_frequency_seconds: u64,

    // ------------- KV2 -------------
    pub kv2: Option<KV2Config>,
    // ------------- Secrets -------------
    pub secret_backend: SecretBackend,
    #[serde(
        deserialize_with = "crate::config::seconds_to_std_duration",
        serialize_with = "crate::config::serialize_std_duration_as_ms"
    )]
    // ------------- Tasks -------------
    /// Duration to wait after no new task was found before polling for new tasks again.
    pub task_poll_interval: std::time::Duration,
    /// Number of workers to spawn for expiring tabulars. (default: 2)
    pub task_tabular_expiration_workers: usize,
    /// Number of workers to spawn for purging tabulars. (default: 2)
    pub task_tabular_purge_workers: usize,
    /// Number of workers to spawn for cleaning task logs. (default: 2)
    pub task_log_cleanup_workers: usize,
    // ------------- Tabular -------------
    /// Delay in seconds after which a tabular will be deleted
    #[serde(
        deserialize_with = "seconds_to_duration",
        serialize_with = "duration_to_seconds"
    )]
    pub default_tabular_expiration_delay_seconds: chrono::Duration,

    // ------------- Page size for paginated queries -------------
    pub pagination_size_default: u32,
    pub pagination_size_max: u32,

    // ------------- Metrics -------------
    #[serde(default)]
    pub(crate) metrics: Metrics,

    // ------------- Stats -------------
    /// Interval to wait before writing the latest accumulated endpoint statistics into the database.
    ///
    /// Accepts a string of format "{number}{ms|s}", e.g. "30s" for 30 seconds or "500ms" for 500
    /// milliseconds.
    #[serde(
        deserialize_with = "seconds_to_std_duration",
        serialize_with = "serialize_std_duration_as_ms"
    )]
    pub endpoint_stat_flush_interval: Duration,

    // ------------- Caching -------------
    #[serde(default)]
    pub(crate) cache: Cache,

    // ------------- Audit logging -------------
    pub(crate) audit: AuditConfig,

    // ------------- Testing -------------
    pub skip_storage_validation: bool,

    // ------------- Idempotency -------------
    #[serde(default)]
    pub idempotency: IdempotencyConfig,

    // ------------- Debug -------------
    #[serde(default)]
    pub debug: DebugConfig,

    // ------------- Request Limits -------------
    /// Maximum request body size in bytes. Defaults to 2 MB.
    pub max_request_body_size: usize,
    /// Maximum request time. Defaults to 30 seconds.
    #[serde(
        deserialize_with = "seconds_to_std_duration",
        serialize_with = "serialize_std_duration_as_ms"
    )]
    pub max_request_time: Duration,
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

pub(crate) fn seconds_to_std_duration<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    let buf = String::deserialize(deserializer)?;
    Ok(if buf.ends_with("ms") {
        Duration::from_millis(
            u64::from_str(&buf[..buf.len() - 2]).map_err(serde::de::Error::custom)?,
        )
    } else if buf.ends_with('s') {
        Duration::from_secs(u64::from_str(&buf[..buf.len() - 1]).map_err(serde::de::Error::custom)?)
    } else {
        Duration::from_secs(u64::from_str(&buf).map_err(serde::de::Error::custom)?)
    })
}

pub(crate) fn serialize_std_duration_as_ms<S>(
    duration: &Duration,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    format!("{}ms", duration.as_millis()).serialize(serializer)
}

fn deserialize_comma_separated<'de, D>(deserializer: D) -> Result<Option<Vec<String>>, D::Error>
where
    D: Deserializer<'de>,
{
    let buf = Option::<serde_json::Value>::deserialize(deserializer)?;
    buf.map(|buf| {
        buf.as_str()
            .map(str::to_string)
            .or(buf.as_i64().map(|i| i.to_string()))
            .map(|s| s.split(',').map(str::to_string).collect::<Vec<_>>())
            .ok_or_else(|| serde::de::Error::custom("Expected a string"))
    })
    .transpose()
}

fn serialize_comma_separated<S>(
    value: &Option<Vec<String>>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    value
        .as_deref()
        .map(|value| value.join(","))
        .serialize(serializer)
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

#[allow(clippy::ref_option)]
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

#[derive(Debug, Default, Clone, PartialEq)]
pub enum AuthZBackend {
    #[default]
    AllowAll,
    External(String),
}

// Add a custom deserializer to handle the special cases
impl<'de> Deserialize<'de> for AuthZBackend {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        let normalized = raw.trim().to_lowercase();
        if normalized == "allowall" || normalized == "allow-all" {
            Ok(Self::AllowAll)
        } else {
            Ok(Self::External(normalized))
        }
    }
}

impl Serialize for AuthZBackend {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            AuthZBackend::AllowAll => "allowall".serialize(serializer),
            AuthZBackend::External(s) => s.to_lowercase().serialize(serializer),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum SecretBackend {
    #[serde(alias = "kv2", alias = "Kv2")]
    KV2,
    #[serde(alias = "postgres")]
    Postgres,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
pub struct IdempotencyConfig {
    /// Whether idempotency key support is enabled.
    /// When enabled, `idempotency-key-lifetime` is advertised in getConfig.
    pub enabled: bool,
    /// How long idempotency records are kept (ISO-8601 duration).
    /// This value is advertised to clients via getConfig.
    /// Default: PT30M (30 minutes)
    #[serde(with = "crate::utils::time_conversion::iso8601_std_duration_serde")]
    pub lifetime: Duration,
    /// Grace period added on top of lifetime for clock skew / transit delays (ISO-8601 duration).
    /// Default: PT5M (5 minutes)
    #[serde(with = "crate::utils::time_conversion::iso8601_std_duration_serde")]
    pub grace_period: Duration,
    /// Maximum time a background cleanup task may run before being considered dead.
    /// If a cleanup exceeds this, the next attempt takes over.
    /// Default: PT30S (30 seconds)
    #[serde(with = "crate::utils::time_conversion::iso8601_std_duration_serde")]
    pub cleanup_timeout: Duration,
}

impl Default for IdempotencyConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            lifetime: Duration::from_mins(30),
            grace_period: Duration::from_mins(5),
            cleanup_timeout: Duration::from_secs(30),
        }
    }
}

impl IdempotencyConfig {
    /// Returns the lifetime as an ISO-8601 duration string for advertising in getConfig.
    #[must_use]
    pub fn lifetime_iso8601(&self) -> String {
        crate::utils::time_conversion::std_duration_to_iso_8601_string(&self.lifetime)
    }

    /// Total retention duration (lifetime + grace).
    #[must_use]
    pub fn total_retention(&self) -> Duration {
        self.lifetime + self.grace_period
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, Default)]
pub struct DebugConfig {
    /// If true, log all request bodies to the debug log for debugging purposes.
    /// This is expensive and should only be used for debugging.
    pub log_request_bodies: bool,
    /// If true, log the Authorization header in request spans for debugging purposes.
    /// This exposes sensitive credentials and should never be enabled in production.
    pub log_authorization_header: bool,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Redact)]
pub struct KV2Config {
    pub url: Url,
    pub user: String,
    #[redact]
    pub password: String,
    pub secret_mount: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct AuditConfig {
    pub tracing: AuditTracingConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct AuditTracingConfig {
    pub enabled: bool,
}

/// Cache for `UserId → ListUserRoleAssignmentsResult` lookups.
///
/// Hot path: checked on every authorisation request.
/// `time_to_live_secs` must not exceed `role.time_to_live_secs` to bound
/// the window where a deleted role can appear in user assignment results.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub(crate) struct UserAssignmentsCache {
    pub(crate) enabled: bool,
    pub(crate) capacity: u64,
    pub(crate) time_to_live_secs: u64,
}

impl Default for UserAssignmentsCache {
    fn default() -> Self {
        Self {
            enabled: true,
            capacity: 50_000,
            time_to_live_secs: 120,
        }
    }
}

/// Cache for `RoleId → ListRoleMembersResult` lookups.
///
/// Cold path: admin / provider queries only. Keep capacity low —
/// each entry holds an unbounded `Vec<AssignedUser>`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub(crate) struct RoleMembersCache {
    pub(crate) enabled: bool,
    pub(crate) capacity: u64,
    pub(crate) time_to_live_secs: u64,
}

impl Default for RoleMembersCache {
    fn default() -> Self {
        Self {
            enabled: true,
            capacity: 1_000,
            time_to_live_secs: 120,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub(crate) struct Cache {
    /// Short‑Term Credentials cache configuration.
    pub(crate) stc: STCCache,
    /// Warehouse cache configuration.
    pub(crate) warehouse: WarehouseCache,
    /// Namespace cache configuration.
    pub(crate) namespace: NamespaceCache,
    /// Secrets cache configuration.
    pub(crate) secrets: SecretsCache,
    /// Role cache configuration.
    pub(crate) role: RoleCache,
    /// User-assignments cache: `UserId → roles`.
    pub(crate) user_assignments: UserAssignmentsCache,
    /// Role-members cache: `RoleId → members`.
    pub(crate) role_members: RoleMembersCache,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub(crate) struct STCCache {
    pub(crate) enabled: bool,
    pub(crate) capacity: u64,
}

impl std::default::Default for STCCache {
    fn default() -> Self {
        Self {
            enabled: true,
            capacity: 10_000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub(crate) struct WarehouseCache {
    pub(crate) enabled: bool,
    pub(crate) capacity: u64,
    /// Time-to-live for cache entries in seconds. Defaults to 60 seconds.
    pub(crate) time_to_live_secs: u64,
}

impl std::default::Default for WarehouseCache {
    fn default() -> Self {
        Self {
            enabled: true,
            capacity: 1000,
            time_to_live_secs: 60,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub(crate) struct NamespaceCache {
    pub(crate) enabled: bool,
    pub(crate) capacity: u64,
    /// Time-to-live for cache entries in seconds. Defaults to 60 seconds.
    pub(crate) time_to_live_secs: u64,
}

impl std::default::Default for NamespaceCache {
    fn default() -> Self {
        Self {
            enabled: true,
            capacity: 1000,
            time_to_live_secs: 60,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub(crate) struct SecretsCache {
    pub(crate) enabled: bool,
    pub(crate) capacity: u64,
    /// Time-to-live for cache entries in seconds. Defaults to 60 seconds.
    pub(crate) time_to_live_secs: u64,
}

impl std::default::Default for SecretsCache {
    fn default() -> Self {
        Self {
            enabled: true,
            capacity: 500,
            time_to_live_secs: 600,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub(crate) struct RoleCache {
    pub(crate) enabled: bool,
    pub(crate) capacity: u64,
    /// Time-to-live for cache entries in seconds. Defaults to 120 seconds.
    pub(crate) time_to_live_secs: u64,
}

impl std::default::Default for RoleCache {
    fn default() -> Self {
        Self {
            enabled: true,
            capacity: 10_000,
            time_to_live_secs: 120,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub(crate) struct Metrics {
    /// Port under which to serve metrics
    ///
    /// default: 9000
    pub(crate) port: u16,

    pub(crate) tokio: Tokio,
}

impl std::default::Default for Metrics {
    fn default() -> Self {
        Self {
            port: 9000,
            tokio: Tokio::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub(crate) struct Tokio {
    /// Interval to report Tokio Runtime metrics
    ///
    /// Accepts a string of format "{number}{ms|s}", e. g. "30s" for 30 seconds or "500ms" for 500
    /// milliseconds
    ///
    /// default: 30s
    #[serde(
        deserialize_with = "seconds_to_std_duration",
        serialize_with = "serialize_std_duration_as_ms"
    )]
    pub(crate) report_interval: Duration,
}

impl std::default::Default for Tokio {
    fn default() -> Self {
        Tokio {
            report_interval: Duration::from_secs(30),
        }
    }
}

impl Default for DynAppConfig {
    fn default() -> Self {
        Self {
            base_uri: None,
            enable_default_project: true,
            use_x_forwarded_headers: true,
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
            pg_acquire_timeout: 5,
            enable_azure_system_credentials: false,
            enable_aws_system_credentials: false,
            s3_enable_direct_system_credentials: false,
            s3_require_external_id_for_system_credentials: true,
            enable_gcp_system_credentials: false,
            nats_address: None,
            nats_topic: None,
            nats_creds_file: None,
            nats_user: None,
            nats_password: None,
            nats_token: None,
            #[cfg(feature = "kafka")]
            kafka_config: None,
            kafka_topic: None,
            log_cloudevents: None,
            authz_backend: AuthZBackend::default(),
            instance_admins: HashSet::new(),
            trusted_engines: HashMap::new(),
            protected_properties: HashSet::new(),
            openid_provider_uri: None,
            openid_audience: None,
            openid_additional_issuers: None,
            openid_scope: None,
            enable_kubernetes_authentication: false,
            kubernetes_authentication_audience: None,
            kubernetes_authentication_accept_legacy_serviceaccount: false,
            openid_subject_claim: None,
            openid_roles_claim: None,
            listen_port: 8181,
            bind_ip: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            health_check_frequency_seconds: 10,
            kv2: None,
            secret_backend: SecretBackend::Postgres,
            task_poll_interval: Duration::from_secs(10),
            task_tabular_expiration_workers: 2,
            task_tabular_purge_workers: 2,
            task_log_cleanup_workers: 2,
            default_tabular_expiration_delay_seconds: chrono::Duration::days(7),
            pagination_size_default: 100,
            pagination_size_max: 1000,
            metrics: Metrics::default(),
            endpoint_stat_flush_interval: Duration::from_secs(30),
            serve_swagger_ui: true,
            skip_storage_validation: false,
            idempotency: IdempotencyConfig::default(),
            debug: DebugConfig::default(),
            cache: Cache::default(),
            max_request_body_size: 2 * 1024 * 1024, // 2 MB
            max_request_time: Duration::from_secs(30),
            audit: AuditConfig {
                tracing: AuditTracingConfig { enabled: true },
            },
        }
    }
}

impl DynAppConfig {
    pub fn warehouse_prefix(&self, warehouse_id: WarehouseId) -> String {
        self.prefix_template
            .replace("{warehouse_id}", warehouse_id.to_string().as_str())
    }

    pub fn tabular_expiration_delay(&self) -> chrono::Duration {
        self.default_tabular_expiration_delay_seconds
    }

    pub fn authn_enabled(&self) -> bool {
        self.openid_provider_uri.is_some()
    }

    /// Helper for common conversion of optional page size to `i64`.
    pub fn page_size_or_pagination_max(&self, page_size: Option<i64>) -> i64 {
        page_size.map_or(self.pagination_size_max.into(), |i| {
            i.clamp(1, self.pagination_size_max.into())
        })
    }

    pub fn page_size_or_pagination_default(&self, page_size: Option<i64>) -> i64 {
        page_size
            .unwrap_or(self.pagination_size_default.into())
            .clamp(1, self.pagination_size_max.into())
    }
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq)]
pub enum PgSslMode {
    Disable,
    Allow,
    Prefer,
    Require,
    VerifyCa,
    VerifyFull,
}

#[cfg(feature = "sqlx-postgres")]
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
            "disabled" | "disable" => Ok(Self::Disable),
            "allow" => Ok(Self::Allow),
            "prefer" => Ok(Self::Prefer),
            "require" => Ok(Self::Require),
            "verifyca" | "verify-ca" | "verify_ca" => Ok(Self::VerifyCa),
            "verifyfull" | "verify-full" | "verify_full" => Ok(Self::VerifyFull),
            _ => Err(anyhow!("PgSslMode not supported: '{s}'")),
        }
    }
}

impl<'de> Deserialize<'de> for PgSslMode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        PgSslMode::from_str(&s).map_err(serde::de::Error::custom)
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

/// Deserialize a comma-separated string or a sequence into `Vec<String>`.
#[cfg(test)]
#[allow(clippy::result_large_err)]
mod test {
    use std::net::Ipv6Addr;

    #[allow(unused_imports)]
    use super::*;
    #[cfg(feature = "kafka")]
    use crate::service::events::backends::kafka::KafkaConfig;

    #[test]
    fn test_authz_backend_default() {
        let config = get_config();
        assert_eq!(config.authz_backend, AuthZBackend::AllowAll);
    }

    #[test]
    fn test_external_authz_backend() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__AUTHZ_BACKEND", "my-authz");
            let config = get_config();
            assert_eq!(
                config.authz_backend,
                AuthZBackend::External("my-authz".to_string())
            );
            Ok(())
        });
    }

    #[test]
    fn test_allow_all_authz_backend() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__AUTHZ_BACKEND", "allowall");
            let config = get_config();
            assert_eq!(config.authz_backend, AuthZBackend::AllowAll);
            Ok(())
        });
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__AUTHZ_BACKEND", "AllowAll");
            let config = get_config();
            assert_eq!(config.authz_backend, AuthZBackend::AllowAll);
            Ok(())
        });
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__AUTHZ_BACKEND", "ALLOWALL");
            let config = get_config();
            assert_eq!(config.authz_backend, AuthZBackend::AllowAll);
            Ok(())
        });
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__AUTHZ_BACKEND", "allow-all");
            let config = get_config();
            assert_eq!(config.authz_backend, AuthZBackend::AllowAll);
            Ok(())
        });
    }

    #[test]
    fn test_instance_admins_default_empty() {
        assert!(DynAppConfig::default().instance_admins.is_empty());
    }

    #[test]
    fn test_instance_admins_parses_json_array() {
        figment::Jail::expect_with(|jail| {
            jail.set_env(
                "LAKEKEEPER_TEST__INSTANCE_ADMINS",
                r#"["oidc~alice","kubernetes~system:serviceaccount:lk:op"]"#,
            );
            let config = get_config();
            assert_eq!(config.instance_admins.len(), 2);
            assert!(
                config
                    .instance_admins
                    .contains(&UserId::try_from("oidc~alice").unwrap())
            );
            assert!(config.instance_admins.contains(
                &UserId::try_from("kubernetes~system:serviceaccount:lk:op").unwrap(),
            ));
            Ok(())
        });
    }

    #[test]
    fn test_instance_admins_single_element() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__INSTANCE_ADMINS", r#"["oidc~solo"]"#);
            let config = get_config();
            assert_eq!(config.instance_admins.len(), 1);
            assert!(
                config
                    .instance_admins
                    .contains(&UserId::try_from("oidc~solo").unwrap())
            );
            Ok(())
        });
    }

    #[test]
    fn test_instance_admins_accepts_whitespace_in_array() {
        figment::Jail::expect_with(|jail| {
            jail.set_env(
                "LAKEKEEPER_TEST__INSTANCE_ADMINS",
                r#"[ "oidc~alice" ,  "oidc~bob" ]"#,
            );
            let config = get_config();
            assert_eq!(config.instance_admins.len(), 2);
            Ok(())
        });
    }

    #[test]
    fn test_instance_admins_rejects_bare_string() {
        // `FOO=oidc~alice` must NOT parse as a single-element admin list:
        // figment reads it as a scalar string, not a sequence. Operators
        // must use the inline-array form (`["..."]`) even for one admin.
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__INSTANCE_ADMINS", "oidc~alice");
            let defaults = figment::providers::Serialized::defaults(DynAppConfig::default());
            let env = figment::providers::Env::prefixed("LAKEKEEPER_TEST__").split("__");
            let result = figment::Figment::from(defaults)
                .merge(env)
                .extract::<DynAppConfig>();
            assert!(
                result.is_err(),
                "bare string must not be accepted as a single-element list, got {result:?}",
            );
            Ok(())
        });
    }

    #[test]
    fn test_instance_admins_rejects_missing_idp_prefix() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__INSTANCE_ADMINS", r#"["no-idp-prefix"]"#);
            let defaults = figment::providers::Serialized::defaults(DynAppConfig::default());
            let env = figment::providers::Env::prefixed("LAKEKEEPER_TEST__").split("__");
            let result = figment::Figment::from(defaults)
                .merge(env)
                .extract::<DynAppConfig>();
            assert!(
                result.is_err(),
                "expected parsing to fail for user id without idp prefix, got {result:?}",
            );
            Ok(())
        });
    }

    #[test]
    fn test_pg_ssl_mode_case_insensitive() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__PG_SSL_MODE", "DISABLED");
            let config = get_config();
            assert_eq!(config.pg_ssl_mode, Some(PgSslMode::Disable));
            Ok(())
        });
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__PG_SSL_MODE", "DisaBled");
            let config = get_config();
            assert_eq!(config.pg_ssl_mode, Some(PgSslMode::Disable));
            Ok(())
        });
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__PG_SSL_MODE", "disabled");
            let config = get_config();
            assert_eq!(config.pg_ssl_mode, Some(PgSslMode::Disable));
            Ok(())
        });
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__PG_SSL_MODE", "disable");
            let config = get_config();
            assert_eq!(config.pg_ssl_mode, Some(PgSslMode::Disable));
            Ok(())
        });
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__PG_SSL_MODE", "Disable");
            let config = get_config();
            assert_eq!(config.pg_ssl_mode, Some(PgSslMode::Disable));
            Ok(())
        });
    }

    #[test]
    fn test_base_uri_trailing_slash_stripped() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__BASE_URI", "https://localhost:8181/a/b/");
            let config = get_config();
            assert_eq!(
                config.base_uri.as_ref().unwrap().to_string(),
                "https://localhost:8181/a/b/"
            );
            assert_eq!(config.base_uri.as_ref().unwrap().path(), "/a/b/");
            Ok(())
        });
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__BASE_URI", "https://localhost:8181/a/b");
            let config = get_config();
            assert_eq!(
                config.base_uri.as_ref().unwrap().to_string(),
                "https://localhost:8181/a/b/"
            );
            assert_eq!(config.base_uri.as_ref().unwrap().path(), "/a/b/");
            Ok(())
        });
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__BASE_URI", "https://localhost:8181");
            let config = get_config();
            assert_eq!(
                config.base_uri.as_ref().unwrap().to_string(),
                "https://localhost:8181/"
            );
            assert_eq!(config.base_uri.as_ref().unwrap().path(), "/");
            Ok(())
        });
    }

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
    fn test_single_audience() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__OPENID_AUDIENCE", "abc");
            let config = get_config();
            assert_eq!(config.openid_audience, Some(vec!["abc".to_string()]));
            Ok(())
        });
    }

    #[test]
    fn test_audience_only_numbers() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__OPENID_AUDIENCE", "123456");
            let config = get_config();
            assert_eq!(config.openid_audience, Some(vec!["123456".to_string()]));
            Ok(())
        });
    }

    #[test]
    fn test_multiple_allow_origin() {
        figment::Jail::expect_with(|jail| {
            jail.set_env(
                "LAKEKEEPER_TEST__ALLOW_ORIGIN",
                "http://localhost,http://example.com",
            );
            let config = get_config();
            assert_eq!(
                config.allow_origin,
                Some(vec![
                    HeaderValue::from_str("http://localhost").unwrap(),
                    HeaderValue::from_str("http://example.com").unwrap()
                ])
            );
            Ok(())
        });
    }

    #[test]
    fn test_default() {
        let _ = &CONFIG.base_uri;
    }

    #[test]
    fn test_queue_config() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__TASK_POLL_INTERVAL", "5s");
            let config = get_config();
            assert_eq!(config.task_poll_interval, Duration::from_secs(5));
            Ok(())
        });
    }

    #[test]
    fn reserved_namespaces_should_contains_default_values() {
        assert!(CONFIG.reserved_namespaces.contains("system"));
        assert!(CONFIG.reserved_namespaces.contains("examples"));
    }

    #[test]
    fn test_task_queue_config_millis() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__TASK_POLL_INTERVAL", "5ms");
            let config = get_config();
            assert_eq!(
                config.task_poll_interval,
                std::time::Duration::from_millis(5)
            );
            Ok(())
        });
    }

    #[test]
    fn test_task_queue_config_seconds() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__TASK_POLL_INTERVAL", "5s");
            let config = get_config();
            assert_eq!(config.task_poll_interval, std::time::Duration::from_secs(5));
            Ok(())
        });
    }

    #[test]
    fn test_task_queue_config_legacy_seconds() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__TASK_POLL_INTERVAL", "\"5\"");
            let config = get_config();
            assert_eq!(config.task_poll_interval, std::time::Duration::from_secs(5));
            Ok(())
        });
    }

    #[test]
    fn test_bind_ip_address_v4_all() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__BIND_IP", "0.0.0.0");
            let config = get_config();
            assert_eq!(config.bind_ip, IpAddr::V4(Ipv4Addr::UNSPECIFIED));
            Ok(())
        });
    }

    #[test]
    fn test_bind_ip_address_v4_localhost() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__BIND_IP", "127.0.0.1");
            let config = get_config();
            assert_eq!(config.bind_ip, IpAddr::V4(Ipv4Addr::LOCALHOST));
            Ok(())
        });
    }

    #[test]
    fn test_bind_ip_address_v6_loopback() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__BIND_IP", "::1");
            let config = get_config();
            assert_eq!(config.bind_ip, IpAddr::V6(Ipv6Addr::LOCALHOST));
            Ok(())
        });
    }

    #[test]
    fn test_bind_ip_address_v6_all() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__BIND_IP", "::");
            let config = get_config();
            assert_eq!(config.bind_ip, IpAddr::V6(Ipv6Addr::UNSPECIFIED));
            Ok(())
        });
    }

    #[test]
    fn test_legacy_service_account_acceptance() {
        figment::Jail::expect_with(|jail| {
            jail.set_env(
                "LAKEKEEPER_TEST__KUBERNETES_AUTHENTICATION_ACCEPT_LEGACY_SERVICEACCOUNT",
                "true",
            );
            let config = get_config();
            assert!(config.kubernetes_authentication_accept_legacy_serviceaccount);
            Ok(())
        });
    }

    #[test]
    fn test_s3_disable_system_credentials() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__ENABLE_AWS_SYSTEM_CREDENTIALS", "true");
            let config = get_config();
            assert!(config.enable_aws_system_credentials);
            assert!(!config.s3_enable_direct_system_credentials);
            Ok(())
        });
    }

    #[test]
    fn test_use_x_forwarded_headers() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__USE_X_FORWARDED_HEADERS", "true");
            let config = get_config();
            assert!(config.use_x_forwarded_headers);
            Ok(())
        });

        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__USE_X_FORWARDED_HEADERS", "false");
            let config = get_config();
            assert!(!config.use_x_forwarded_headers);
            Ok(())
        });
    }

    #[cfg(feature = "kafka")]
    #[test]
    fn test_kafka_config_env_var() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__KAFKA_TOPIC", "test_topic");
            jail.set_env(
                "LAKEKEEPER_TEST__KAFKA_CONFIG",
                r#"{"sasl.password"="my_pw","bootstrap.servers"="host1:port,host2:port","security.protocol"="SSL"}"#,
            );
            jail.set_env(
                "LAKEKEEPER_TEST__KAFKA_CONFIG_FILE",
                r#"{"sasl.password"="my_pw","bootstrap.servers"="host1:port,host2:port","security.protocol"="SSL"}"#,
            );
            let config = get_config();
            assert_eq!(config.kafka_topic, Some("test_topic".to_string()));
            assert_eq!(
                config.kafka_config,
                Some(KafkaConfig {
                    sasl_password: Some("my_pw".to_string()),
                    sasl_oauthbearer_client_secret: None,
                    ssl_key_password: None,
                    ssl_keystore_password: None,
                    conf: std::collections::HashMap::from_iter([
                        (
                            "bootstrap.servers".to_string(),
                            "host1:port,host2:port".to_string()
                        ),
                        ("security.protocol".to_string(), "SSL".to_string()),
                    ]),
                })
            );
            Ok(())
        });
    }

    #[cfg(feature = "kafka")]
    #[test]
    fn test_kafka_config_file() {
        let named_tmp_file = tempfile::NamedTempFile::new().unwrap();
        std::io::Write::write_all(&mut named_tmp_file
            .as_file(), r#"{"sasl.password"="my_pw","bootstrap.servers"="host1:port,host2:port","security.protocol"="SSL"}"#.as_bytes())
            .unwrap();
        figment::Jail::expect_with(|jail| {
            use std::collections::HashMap;

            jail.set_env("LAKEKEEPER_TEST__KAFKA_TOPIC", "test_topic");
            jail.set_env(
                "LAKEKEEPER_TEST__KAFKA_CONFIG_FILE",
                named_tmp_file.path().to_str().unwrap(),
            );
            let config = get_config();
            assert_eq!(config.kafka_topic, Some("test_topic".to_string()));
            assert_eq!(
                config.kafka_config,
                Some(KafkaConfig {
                    sasl_password: Some("my_pw".to_string()),
                    sasl_oauthbearer_client_secret: None,
                    ssl_key_password: None,
                    ssl_keystore_password: None,
                    conf: HashMap::from_iter([
                        (
                            "bootstrap.servers".to_string(),
                            "host1:port,host2:port".to_string()
                        ),
                        ("security.protocol".to_string(), "SSL".to_string()),
                    ]),
                })
            );
            Ok(())
        });
    }

    #[test]
    fn test_disable_storage_validation() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__SKIP_STORAGE_VALIDATION", "true");
            let config = get_config();
            assert!(config.skip_storage_validation);
            Ok(())
        });

        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__SKIP_STORAGE_VALIDATION", "false");
            let config = get_config();
            assert!(!config.skip_storage_validation);
            Ok(())
        });
    }

    #[test]
    fn test_debug_log_request_bodies() {
        // Test default value (should be false)
        figment::Jail::expect_with(|_jail| {
            let config = get_config();
            assert!(!config.debug.log_request_bodies);
            Ok(())
        });

        // Test setting to true
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__DEBUG__LOG_REQUEST_BODIES", "true");
            let config = get_config();
            assert!(config.debug.log_request_bodies);
            Ok(())
        });

        // Test setting to false explicitly
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__DEBUG__LOG_REQUEST_BODIES", "false");
            let config = get_config();
            assert!(!config.debug.log_request_bodies);
            Ok(())
        });
    }

    #[test]
    fn test_debug_log_authorization_header() {
        // Test default value (should be false)
        figment::Jail::expect_with(|_jail| {
            let config = get_config();
            assert!(!config.debug.log_authorization_header);
            Ok(())
        });

        // Test setting to true
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__DEBUG__LOG_AUTHORIZATION_HEADER", "true");
            let config = get_config();
            assert!(config.debug.log_authorization_header);
            Ok(())
        });

        // Test setting to false explicitly
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__DEBUG__LOG_AUTHORIZATION_HEADER", "false");
            let config = get_config();
            assert!(!config.debug.log_authorization_header);
            Ok(())
        });
    }

    #[test]
    fn test_stc_cache() {
        figment::Jail::expect_with(|_jail| {
            let config = get_config();
            assert!(config.cache.stc.enabled);
            assert_eq!(config.cache.stc.capacity, 10_000);
            Ok(())
        });

        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__CACHE__STC__ENABLED", "false");
            let config = get_config();
            assert!(!config.cache.stc.enabled);
            Ok(())
        });

        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__CACHE__STC__ENABLED", "true");
            jail.set_env("LAKEKEEPER_TEST__CACHE__STC__CAPACITY", "5000");
            let config = get_config();
            assert!(config.cache.stc.enabled);
            assert_eq!(config.cache.stc.capacity, 5000);
            Ok(())
        });
    }

    #[test]
    fn test_warehouse_cache() {
        figment::Jail::expect_with(|_jail| {
            let config = get_config();
            assert!(config.cache.warehouse.enabled);
            assert_eq!(config.cache.warehouse.capacity, 1000);
            Ok(())
        });

        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__CACHE__WAREHOUSE__ENABLED", "false");
            let config = get_config();
            assert!(!config.cache.warehouse.enabled);
            Ok(())
        });

        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__CACHE__WAREHOUSE__ENABLED", "true");
            jail.set_env("LAKEKEEPER_TEST__CACHE__WAREHOUSE__CAPACITY", "2000");
            let config = get_config();
            assert!(config.cache.warehouse.enabled);
            assert_eq!(config.cache.warehouse.capacity, 2000);
            Ok(())
        });
    }

    #[test]
    fn test_namespace_cache() {
        figment::Jail::expect_with(|_jail| {
            let config = get_config();
            assert!(config.cache.namespace.enabled);
            assert_eq!(config.cache.namespace.capacity, 1000);
            Ok(())
        });

        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__CACHE__NAMESPACE__ENABLED", "false");
            let config = get_config();
            assert!(!config.cache.namespace.enabled);
            Ok(())
        });

        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__CACHE__NAMESPACE__ENABLED", "true");
            jail.set_env("LAKEKEEPER_TEST__CACHE__NAMESPACE__CAPACITY", "2000");
            let config = get_config();
            assert!(config.cache.namespace.enabled);
            assert_eq!(config.cache.namespace.capacity, 2000);
            Ok(())
        });
    }

    #[test]
    fn test_role_cache() {
        figment::Jail::expect_with(|_jail| {
            let config = get_config();
            assert!(config.cache.role.enabled);
            assert_eq!(config.cache.role.capacity, 10_000);
            Ok(())
        });

        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__CACHE__ROLE__ENABLED", "false");
            let config = get_config();
            assert!(!config.cache.role.enabled);
            Ok(())
        });

        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__CACHE__ROLE__ENABLED", "true");
            jail.set_env("LAKEKEEPER_TEST__CACHE__ROLE__CAPACITY", "5000");
            let config = get_config();
            assert!(config.cache.role.enabled);
            assert_eq!(config.cache.role.capacity, 5000);
            Ok(())
        });
    }

    #[test]
    fn test_user_assignments_cache() {
        figment::Jail::expect_with(|_jail| {
            let config = get_config();
            assert!(config.cache.user_assignments.enabled);
            assert_eq!(config.cache.user_assignments.capacity, 50_000);
            assert_eq!(config.cache.user_assignments.time_to_live_secs, 120);
            Ok(())
        });

        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__CACHE__USER_ASSIGNMENTS__ENABLED", "false");
            let config = get_config();
            assert!(!config.cache.user_assignments.enabled);
            Ok(())
        });

        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__CACHE__USER_ASSIGNMENTS__ENABLED", "true");
            jail.set_env(
                "LAKEKEEPER_TEST__CACHE__USER_ASSIGNMENTS__CAPACITY",
                "100000",
            );
            jail.set_env(
                "LAKEKEEPER_TEST__CACHE__USER_ASSIGNMENTS__TIME_TO_LIVE_SECS",
                "60",
            );
            let config = get_config();
            assert!(config.cache.user_assignments.enabled);
            assert_eq!(config.cache.user_assignments.capacity, 100_000);
            assert_eq!(config.cache.user_assignments.time_to_live_secs, 60);
            Ok(())
        });
    }

    #[test]
    fn test_role_members_cache() {
        figment::Jail::expect_with(|_jail| {
            let config = get_config();
            assert!(config.cache.role_members.enabled);
            assert_eq!(config.cache.role_members.capacity, 1_000);
            assert_eq!(config.cache.role_members.time_to_live_secs, 120);
            Ok(())
        });

        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__CACHE__ROLE_MEMBERS__ENABLED", "false");
            let config = get_config();
            assert!(!config.cache.role_members.enabled);
            Ok(())
        });

        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__CACHE__ROLE_MEMBERS__ENABLED", "true");
            jail.set_env("LAKEKEEPER_TEST__CACHE__ROLE_MEMBERS__CAPACITY", "5000");
            jail.set_env(
                "LAKEKEEPER_TEST__CACHE__ROLE_MEMBERS__TIME_TO_LIVE_SECS",
                "30",
            );
            let config = get_config();
            assert!(config.cache.role_members.enabled);
            assert_eq!(config.cache.role_members.capacity, 5000);
            assert_eq!(config.cache.role_members.time_to_live_secs, 30);
            Ok(())
        });
    }

    #[test]
    #[should_panic(expected = "user_assignments.time_to_live_secs")]
    fn test_user_assignments_ttl_exceeds_role_ttl_is_rejected() {
        figment::Jail::expect_with(|jail| {
            jail.set_env(
                "LAKEKEEPER_TEST__CACHE__USER_ASSIGNMENTS__TIME_TO_LIVE_SECS",
                "300",
            );
            jail.set_env(
                "LAKEKEEPER_TEST__CACHE__ROLE_MEMBERS__TIME_TO_LIVE_SECS",
                "60",
            );
            let _config = get_config(); // must panic – user_assignments TTL > role TTL
            Ok(())
        });
    }

    #[test]
    fn openid_subject_claims() {
        figment::Jail::expect_with(|_jail| {
            let config = get_config();
            assert!(config.openid_subject_claim.is_none());
            Ok(())
        });

        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__OPENID_SUBJECT_CLAIM", "custom_sub");
            let config = get_config();
            assert_eq!(
                config.openid_subject_claim,
                Some(vec!["custom_sub".to_string()])
            );
            Ok(())
        });

        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__OPENID_SUBJECT_CLAIM", "custom_sub,oid");
            let config = get_config();
            assert_eq!(
                config.openid_subject_claim,
                Some(vec!["custom_sub".to_string(), "oid".to_string()])
            );
            Ok(())
        });
    }

    #[test]
    fn test_audit_tracing_enabled() {
        // Test default value is true
        figment::Jail::expect_with(|_jail| {
            let config = get_config();
            assert!(config.audit.tracing.enabled);
            Ok(())
        });

        // Test can be disabled
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__AUDIT__TRACING__ENABLED", "false");
            let config = get_config();
            assert!(!config.audit.tracing.enabled);
            Ok(())
        });

        // Test can be explicitly enabled
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__AUDIT__TRACING__ENABLED", "true");
            let config = get_config();
            assert!(config.audit.tracing.enabled);
            Ok(())
        });
    }

    #[test]
    fn test_trusted_engine_configuration() {
        figment::Jail::expect_with(|_jail| {
            let config = get_config();
            assert!(config.trusted_engines.is_empty());
            Ok(())
        });

        // Verify full env var configuration including identities
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__TRUSTED_ENGINES__TRINO__TYPE", "trino");
            jail.set_env(
                "LAKEKEEPER_TEST__TRUSTED_ENGINES__TRINO__OWNER_PROPERTY",
                "trino.run-as-owner",
            );
            jail.set_env(
                "LAKEKEEPER_TEST__TRUSTED_ENGINES__TRINO__IDENTITIES__OIDC__AUDIENCES",
                "[trino_dev, trino_prod]",
            );
            jail.set_env(
                "LAKEKEEPER_TEST__TRUSTED_ENGINES__TRINO__IDENTITIES__KUBERNETES__SUBJECTS",
                "[trino-sa, trino-sa-2]",
            );

            let config = get_config();
            let engine = config.trusted_engines.get("trino").unwrap();
            let TrustedEngine::Trino(c) = engine;
            assert_eq!(c.owner_property, "trino.run-as-owner");
            assert_eq!(c.identities.len(), 2);

            let oidc = c.identities.get("oidc").unwrap();
            assert_eq!(oidc.audiences, vec!["trino_dev", "trino_prod"]);
            assert!(oidc.subjects.is_empty());

            let k8s = c.identities.get("kubernetes").unwrap();
            assert!(k8s.audiences.is_empty());
            assert_eq!(k8s.subjects, vec!["trino-sa", "trino-sa-2"]);

            // protected_properties should be pre-computed
            assert!(config.protected_properties.contains("trino.run-as-owner"));

            Ok(())
        });

        // Single-value audiences still require bracket syntax
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__TRUSTED_ENGINES__TRINO__TYPE", "trino");
            jail.set_env(
                "LAKEKEEPER_TEST__TRUSTED_ENGINES__TRINO__OWNER_PROPERTY",
                "trino.run-as-owner",
            );
            jail.set_env(
                "LAKEKEEPER_TEST__TRUSTED_ENGINES__TRINO__IDENTITIES__OIDC__AUDIENCES",
                "[trino]",
            );
            let config = get_config();
            let engine = config.trusted_engines.get("trino").unwrap();
            let TrustedEngine::Trino(c) = engine;
            let oidc = c.identities.get("oidc").unwrap();
            assert_eq!(oidc.audiences, vec!["trino"]);
            Ok(())
        });
    }

    #[test]
    fn test_idempotency_defaults() {
        figment::Jail::expect_with(|_jail| {
            let config = get_config();
            assert!(config.idempotency.enabled);
            assert_eq!(config.idempotency.lifetime, Duration::from_mins(30));
            assert_eq!(config.idempotency.grace_period, Duration::from_mins(5));
            assert_eq!(config.idempotency.lifetime_iso8601(), "PT30M");
            assert_eq!(
                config.idempotency.total_retention(),
                Duration::from_mins(35)
            );
            Ok(())
        });
    }

    #[test]
    fn test_idempotency_env_vars() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__IDEMPOTENCY__ENABLED", "false");
            jail.set_env("LAKEKEEPER_TEST__IDEMPOTENCY__LIFETIME", "PT1H");
            jail.set_env("LAKEKEEPER_TEST__IDEMPOTENCY__GRACE_PERIOD", "PT10M");
            let config = get_config();
            assert!(!config.idempotency.enabled);
            assert_eq!(config.idempotency.lifetime, Duration::from_hours(1));
            assert_eq!(config.idempotency.grace_period, Duration::from_mins(10));
            assert_eq!(config.idempotency.lifetime_iso8601(), "PT1H");
            assert_eq!(
                config.idempotency.total_retention(),
                Duration::from_mins(70)
            );
            Ok(())
        });
    }

    #[test]
    fn test_idempotency_partial_override() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__IDEMPOTENCY__LIFETIME", "PT15M");
            let config = get_config();
            // lifetime overridden, grace_period keeps default
            assert!(config.idempotency.enabled);
            assert_eq!(config.idempotency.lifetime, Duration::from_mins(15));
            assert_eq!(config.idempotency.grace_period, Duration::from_mins(5));
            Ok(())
        });
    }

    #[test]
    fn test_metrics_default_values_as_expected() {
        figment::Jail::expect_with(|_| {
            let config = get_config();
            assert_eq!(config.metrics.port, 9000);
            assert_eq!(
                config.metrics.tokio.report_interval,
                Duration::from_secs(30),
            );
            Ok(())
        });
    }

    #[test]
    fn test_metrics_env_vars() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__METRICS__PORT", "2");
            jail.set_env("LAKEKEEPER_TEST__METRICS__TOKIO__REPORT_INTERVAL", "100ms");
            let config = get_config();
            assert_eq!(config.metrics.port, 2);
            assert_eq!(
                config.metrics.tokio.report_interval,
                Duration::from_millis(100),
            );
            Ok(())
        });
    }

    #[test]
    fn test_flat_metrics_port_config_is_mapped() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__METRICS_PORT", "1");
            let config = get_config();
            assert_eq!(config.metrics.port, 1);
            Ok(())
        });
    }

    #[test]
    fn test_nested_metrics_port_config_takes_precedence() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__METRICS_PORT", "1");
            jail.set_env("LAKEKEEPER_TEST__METRICS__PORT", "2");
            let config = get_config();
            assert_eq!(config.metrics.port, 2);
            Ok(())
        });
    }

    fn test_engine(property: &str) -> TrustedEngine {
        TrustedEngine::Trino(TrinoEngineConfig {
            owner_property: property.to_string(),
            identities: HashMap::new(),
        })
    }

    #[test]
    fn test_determine_security_model_returns_definer_when_property_set() {
        let config = TrinoEngineConfig {
            owner_property: "trino.run-as-owner".to_string(),
            identities: HashMap::new(),
        };
        let properties = HashMap::from([("trino.run-as-owner".to_string(), "alice".to_string())]);
        assert_eq!(
            config.determine_security_model(&properties),
            SecurityModel::Definer("alice".to_string())
        );
    }

    #[test]
    fn test_determine_security_model_returns_invoker_when_property_absent() {
        let config = TrinoEngineConfig {
            owner_property: "trino.run-as-owner".to_string(),
            identities: HashMap::new(),
        };
        assert_eq!(
            config.determine_security_model(&HashMap::new()),
            SecurityModel::Invoker
        );
    }

    #[test]
    fn test_determine_security_model_ignores_unrelated_properties() {
        let config = TrinoEngineConfig {
            owner_property: "trino.run-as-owner".to_string(),
            identities: HashMap::new(),
        };
        let properties = HashMap::from([("some.other.property".to_string(), "value".to_string())]);
        assert_eq!(
            config.determine_security_model(&properties),
            SecurityModel::Invoker
        );
    }

    #[test]
    fn test_trusted_engine_delegates_to_trino_config() {
        let engine = test_engine("trino.run-as-owner");
        assert_eq!(engine.owner_property(), "trino.run-as-owner");

        let properties = HashMap::from([("trino.run-as-owner".to_string(), "bob".to_string())]);
        assert_eq!(
            engine.determine_security_model(&properties),
            SecurityModel::Definer("bob".to_string())
        );
    }

    #[test]
    fn test_matched_engines_default_is_not_trusted() {
        let m = MatchedEngines::default();
        assert!(!m.is_trusted());
        assert!(!m.owns_property("anything"));
        assert_eq!(
            m.determine_security_model(&HashMap::new()).unwrap(),
            SecurityModel::Invoker
        );
    }

    #[test]
    fn test_matched_engines_single() {
        let m = MatchedEngines::single(test_engine("trino.run-as-owner"));
        assert!(m.is_trusted());
        assert!(m.owns_property("trino.run-as-owner"));
        assert!(!m.owns_property("spark.run-as-owner"));
    }

    #[test]
    fn test_matched_engines_multiple_determine_security_model() {
        let m = MatchedEngines::new(vec![
            test_engine("trino.run-as-owner"),
            test_engine("spark.run-as-owner"),
        ]);

        let props = HashMap::from([("spark.run-as-owner".to_string(), "alice".to_string())]);
        assert_eq!(
            m.determine_security_model(&props).unwrap(),
            SecurityModel::Definer("alice".to_string())
        );

        assert!(m.owns_property("trino.run-as-owner"));
        assert!(m.owns_property("spark.run-as-owner"));
        assert!(!m.owns_property("other.property"));
    }

    #[test]
    fn test_matched_engines_invoker_when_no_property_matches() {
        let m = MatchedEngines::single(test_engine("trino.run-as-owner"));
        let props = HashMap::from([("unrelated".to_string(), "value".to_string())]);
        assert_eq!(
            m.determine_security_model(&props).unwrap(),
            SecurityModel::Invoker
        );
    }

    #[test]
    fn test_matched_engines_same_owner_across_engines_is_ok() {
        let m = MatchedEngines::new(vec![
            test_engine("trino.run-as-owner"),
            test_engine("spark.run-as-owner"),
        ]);
        let props = HashMap::from([
            ("trino.run-as-owner".to_string(), "alice".to_string()),
            ("spark.run-as-owner".to_string(), "alice".to_string()),
        ]);
        assert_eq!(
            m.determine_security_model(&props).unwrap(),
            SecurityModel::Definer("alice".to_string())
        );
    }

    #[test]
    fn test_matched_engines_different_owners_is_ambiguous() {
        let m = MatchedEngines::new(vec![
            test_engine("trino.run-as-owner"),
            test_engine("spark.run-as-owner"),
        ]);
        let props = HashMap::from([
            ("trino.run-as-owner".to_string(), "alice".to_string()),
            ("spark.run-as-owner".to_string(), "bob".to_string()),
        ]);
        assert!(m.determine_security_model(&props).is_err());
    }

    #[test]
    fn test_identities_accessor() {
        let engine = TrustedEngine::Trino(TrinoEngineConfig {
            owner_property: "trino.run-as-owner".to_string(),
            identities: HashMap::from([
                (
                    "oidc".to_string(),
                    EngineIdentity {
                        audiences: vec!["trino_dev".to_string()],
                        subjects: Vec::new(),
                    },
                ),
                (
                    "kubernetes".to_string(),
                    EngineIdentity {
                        audiences: Vec::new(),
                        subjects: vec!["trino-sa".to_string()],
                    },
                ),
            ]),
        });
        assert_eq!(engine.identities().len(), 2);
        assert!(engine.identities().contains_key("oidc"));
        assert!(engine.identities().contains_key("kubernetes"));
    }

    #[test]
    fn test_engine_identity_matches_audience() {
        let id = EngineIdentity {
            audiences: vec!["trino".to_string()],
            subjects: Vec::new(),
        };
        let auds: HashSet<&str> = ["trino"].into_iter().collect();
        assert!(id.matches(&auds, None));
        assert!(!id.matches(&HashSet::new(), None));
    }

    #[test]
    fn test_engine_identity_matches_subject() {
        let id = EngineIdentity {
            audiences: Vec::new(),
            subjects: vec!["trino-sa".to_string()],
        };
        assert!(id.matches(&HashSet::new(), Some("trino-sa")));
        assert!(!id.matches(&HashSet::new(), Some("other")));
        assert!(!id.matches(&HashSet::new(), None));
    }

    #[test]
    fn test_engine_identity_matches_audience_or_subject() {
        let id = EngineIdentity {
            audiences: vec!["trino".to_string()],
            subjects: vec!["admin-sa".to_string()],
        };
        let auds: HashSet<&str> = ["other_aud"].into_iter().collect();
        // Subject matches even though audience doesn't
        assert!(id.matches(&auds, Some("admin-sa")));
        // Audience matches even though subject doesn't
        let auds: HashSet<&str> = ["trino"].into_iter().collect();
        assert!(id.matches(&auds, Some("other")));
    }
}
