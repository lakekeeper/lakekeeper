use std::{sync::LazyLock, time::Duration};

use lakekeeper::AuthZBackend;
use serde::{Deserialize, Deserializer, Serialize};
use url::Url;
use veil::Redact;

pub static CONFIG: LazyLock<DynAppConfig> = LazyLock::new(get_config);

#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Deserialize, Serialize, Debug, Default)]
pub struct DynAppConfig {
    // ------------- AUTHORIZATION - OPENFGA -------------
    #[serde(default)]
    pub authz_backend: AuthZBackend,
    #[serde(
        deserialize_with = "deserialize_openfga_config",
        serialize_with = "serialize_openfga_config"
    )]
    pub openfga: Option<OpenFGAConfig>,
}

impl DynAppConfig {
    pub fn is_openfga_enabled(&self) -> bool {
        self.authz_backend == AuthZBackend::External("openfga".to_string())
    }
}

fn get_config() -> DynAppConfig {
    let defaults = figment::providers::Serialized::defaults(DynAppConfig::default());

    #[cfg(not(test))]
    let prefixes = &["ICEBERG_REST__", "LAKEKEEPER__"];
    #[cfg(test)]
    let prefixes = &["LAKEKEEPER_TEST__"];

    let mut config = figment::Figment::from(defaults);
    for prefix in prefixes {
        let env = figment::providers::Env::prefixed(prefix).split("__");
        config = config.merge(env);
    }

    match config.extract::<DynAppConfig>() {
        Ok(c) => c,
        Err(e) => {
            panic!("Failed to extract OpenFGA config: {e}");
        }
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
    /// Explicitly set the Authorization model prefix.
    /// Defaults to `collaboration` if not set.
    /// We recommend to use this setting only in combination with
    /// `authorization_model_version`
    #[serde(default = "default_openfga_model_prefix")]
    pub authorization_model_prefix: String,
    /// Version of the model to use, in the format `<major>.<minor>`.
    /// If specified, the model version must already exist. Under the default prefix
    /// it must be the version this release installs. Under another prefix (externally
    /// managed models) it must contain every type and relation that the server checks
    /// or writes; only the names are compared. Otherwise the server does not start.
    /// Migration is disabled if the model version is set.
    pub authorization_model_version: Option<String>,
    /// The maximum number of checks than can be handled by a batch check
    /// request. This is a [configuration option] of the `OpenFGA` server
    /// with default value 50.
    ///
    /// [configuration option]: https://openfga.dev/docs/getting-started/setup-openfga/configuration#OPENFGA_MAX_CHECKS_PER_BATCH_CHECK
    #[serde(default = "default_openfga_max_batch_check_size")]
    pub max_batch_check_size: usize,
    /// How the authorizer answers the caller's warehouse actions.
    #[serde(default)]
    pub warehouse_checks: WarehouseChecks,
    /// The `OpenFGA` server's list-objects deadline (`OPENFGA_LIST_OBJECTS_DEADLINE`).
    /// `OpenFGA` ends a `StreamedListObjects` stream without an error when this deadline
    /// passes, so a listing that ends near it is not trusted as a deny. Never set it higher
    /// than the server's value. Only used with `warehouse_checks = streamed-list`.
    #[serde(
        default = "default_openfga_server_deadline",
        deserialize_with = "deserialize_duration",
        serialize_with = "serialize_duration_as_ms"
    )]
    pub server_list_objects_deadline: Duration,
    /// The `OpenFGA` server's request timeout (`OPENFGA_REQUEST_TIMEOUT`). It also ends a
    /// `StreamedListObjects` stream without an error. Never set it higher than the
    /// server's value. Only used with `warehouse_checks = streamed-list`.
    #[serde(
        default = "default_openfga_server_deadline",
        deserialize_with = "deserialize_duration",
        serialize_with = "serialize_duration_as_ms"
    )]
    pub server_request_timeout: Duration,
}

impl OpenFGAConfig {
    /// The first of the `OpenFGA` deadlines that can cut a `StreamedListObjects` stream.
    pub(crate) fn streamed_list_deadline(&self) -> Duration {
        self.server_list_objects_deadline
            .min(self.server_request_timeout)
    }
}

/// How the `OpenFGA` authorizer answers the caller's warehouse actions.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WarehouseChecks {
    /// A `Check` per action. For a principal whose only grants are below the warehouse,
    /// `OpenFGA` expands the whole warehouse top-down to answer it.
    #[default]
    Check,
    /// One `StreamedListObjects(warehouse, <action>, caller)` per action asked, all at the
    /// same time, each read until every warehouse asked about has arrived. `OpenFGA`
    /// expands from the caller's own tuples, so the cost follows the caller's grants, not
    /// the size of the warehouse.
    StreamedList,
}

#[derive(Clone, Default, Serialize, Deserialize, PartialEq, veil::Redact)]
#[serde(rename_all = "snake_case")]
pub enum OpenFGAAuth {
    #[default]
    Anonymous,
    ClientCredentials {
        client_id: String,
        #[redact]
        client_secret: String,
        token_endpoint: Url,
        scope: Option<String>,
    },
    #[redact(all)]
    ApiKey(String),
}

fn deserialize_openfga_config<'de, D>(deserializer: D) -> Result<Option<OpenFGAConfig>, D::Error>
where
    D: Deserializer<'de>,
{
    let Some(OpenFGAConfigSerde {
        client_id,
        client_secret,
        scope,
        token_endpoint,
        api_key,
        endpoint,
        store_name,
        authorization_model_prefix,
        authorization_model_version,
        max_batch_check_size,
        warehouse_checks,
        server_list_objects_deadline,
        server_request_timeout,
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
            scope,
        }
    } else {
        api_key.map_or(OpenFGAAuth::Anonymous, OpenFGAAuth::ApiKey)
    };

    if max_batch_check_size == 0 {
        return Err(serde::de::Error::custom(
            "openfga max_batch_check_size must be greater than zero",
        ));
    }

    if server_list_objects_deadline.is_zero() {
        return Err(serde::de::Error::custom(
            "openfga server_list_objects_deadline must be greater than zero",
        ));
    }

    if server_request_timeout.is_zero() {
        return Err(serde::de::Error::custom(
            "openfga server_request_timeout must be greater than zero",
        ));
    }

    Ok(Some(OpenFGAConfig {
        endpoint,
        store_name,
        auth,
        authorization_model_prefix,
        authorization_model_version,
        max_batch_check_size,
        warehouse_checks,
        server_list_objects_deadline,
        server_request_timeout,
    }))
}

#[allow(clippy::ref_option)]
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

    let (client_id, client_secret, token_endpoint, scope, api_key) = match &value.auth {
        OpenFGAAuth::ClientCredentials {
            client_id,
            client_secret,
            token_endpoint,
            scope,
        } => (
            Some(client_id),
            Some(client_secret),
            Some(token_endpoint),
            scope.clone(),
            None,
        ),
        OpenFGAAuth::ApiKey(api_key) => (None, None, None, None, Some(api_key.clone())),
        OpenFGAAuth::Anonymous => (None, None, None, None, None),
    };

    OpenFGAConfigSerde {
        client_id: client_id.cloned(),
        client_secret: client_secret.cloned(),
        token_endpoint: token_endpoint.cloned(),
        scope,
        api_key,
        endpoint: value.endpoint.clone(),
        store_name: value.store_name.clone(),
        authorization_model_prefix: value.authorization_model_prefix.clone(),
        authorization_model_version: value.authorization_model_version.clone(),
        max_batch_check_size: value.max_batch_check_size,
        warehouse_checks: value.warehouse_checks,
        server_list_objects_deadline: value.server_list_objects_deadline,
        server_request_timeout: value.server_request_timeout,
    }
    .serialize(serializer)
}

#[derive(Serialize, Deserialize, PartialEq, Redact)]
#[allow(clippy::result_large_err)]
struct OpenFGAConfigSerde {
    /// GRPC Endpoint Url
    endpoint: Url,
    /// Store Name - if not specified, `lakekeeper` is used.
    #[serde(default = "default_openfga_store_name")]
    store_name: String,
    #[serde(default = "default_openfga_model_prefix")]
    authorization_model_prefix: String,
    authorization_model_version: Option<String>,
    /// API-Key. If client-id is specified, this is ignored.
    api_key: Option<String>,
    /// Client id
    client_id: Option<String>,
    #[redact]
    /// Client secret
    client_secret: Option<String>,
    /// Scope for the client credentials
    scope: Option<String>,
    /// Token Endpoint to use when exchanging client credentials for an access token.
    token_endpoint: Option<Url>,
    #[serde(default = "default_openfga_max_batch_check_size")]
    max_batch_check_size: usize,
    #[serde(default)]
    warehouse_checks: WarehouseChecks,
    #[serde(
        default = "default_openfga_server_deadline",
        deserialize_with = "deserialize_duration",
        serialize_with = "serialize_duration_as_ms"
    )]
    server_list_objects_deadline: Duration,
    #[serde(
        default = "default_openfga_server_deadline",
        deserialize_with = "deserialize_duration",
        serialize_with = "serialize_duration_as_ms"
    )]
    server_request_timeout: Duration,
}

/// `OpenFGA`'s default `OPENFGA_LIST_OBJECTS_DEADLINE` and `OPENFGA_REQUEST_TIMEOUT`.
fn default_openfga_server_deadline() -> Duration {
    Duration::from_secs(3)
}

/// A duration like `3s` or `500ms`, the units Lakekeeper's other durations accept. A
/// number without a unit is seconds.
fn deserialize_duration<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Raw {
        Seconds(u64),
        Text(String),
    }
    let text = match Raw::deserialize(deserializer)? {
        Raw::Seconds(secs) => return Ok(Duration::from_secs(secs)),
        Raw::Text(text) => text,
    };
    let text = text.trim();
    let parsed = if let Some(millis) = text.strip_suffix("ms") {
        millis.parse().map(Duration::from_millis)
    } else if let Some(secs) = text.strip_suffix('s') {
        secs.parse().map(Duration::from_secs)
    } else {
        text.parse().map(Duration::from_secs)
    };
    parsed.map_err(|e| {
        serde::de::Error::custom(format!(
            "invalid duration `{text}` ({e}); use for example `3s` or `500ms`"
        ))
    })
}

fn serialize_duration_as_ms<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    format!("{}ms", duration.as_millis()).serialize(serializer)
}

fn default_openfga_store_name() -> String {
    "lakekeeper".to_string()
}

fn default_openfga_model_prefix() -> String {
    crate::migration::OWN_MODEL_PREFIX.to_string()
}

fn default_openfga_max_batch_check_size() -> usize {
    50
}

#[cfg(test)]
#[allow(clippy::result_large_err)]
mod test {
    use super::*;

    #[test]
    fn test_openfga_config_warehouse_checks() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__AUTHZ_BACKEND", "openfga");
            jail.set_env("LAKEKEEPER_TEST__OPENFGA__ENDPOINT", "http://localhost");
            let authz_config = get_config().openfga.unwrap();
            assert_eq!(authz_config.warehouse_checks, WarehouseChecks::Check);
            assert_eq!(
                authz_config.server_list_objects_deadline,
                Duration::from_secs(3)
            );
            assert_eq!(authz_config.server_request_timeout, Duration::from_secs(3));

            jail.set_env(
                "LAKEKEEPER_TEST__OPENFGA__WAREHOUSE_CHECKS",
                "streamed-list",
            );
            jail.set_env(
                "LAKEKEEPER_TEST__OPENFGA__SERVER_LIST_OBJECTS_DEADLINE",
                "5s",
            );
            let authz_config = get_config().openfga.unwrap();
            assert_eq!(authz_config.warehouse_checks, WarehouseChecks::StreamedList);
            assert_eq!(
                authz_config.server_list_objects_deadline,
                Duration::from_secs(5)
            );
            // The request timeout cuts the stream first.
            assert_eq!(
                authz_config.streamed_list_deadline(),
                Duration::from_secs(3)
            );

            jail.set_env(
                "LAKEKEEPER_TEST__OPENFGA__SERVER_REQUEST_TIMEOUT",
                "10000ms",
            );
            let authz_config = get_config().openfga.unwrap();
            assert_eq!(authz_config.server_request_timeout, Duration::from_secs(10));
            assert_eq!(
                authz_config.streamed_list_deadline(),
                Duration::from_secs(5)
            );

            // A number without a unit is seconds.
            jail.set_env("LAKEKEEPER_TEST__OPENFGA__SERVER_REQUEST_TIMEOUT", "4");
            let authz_config = get_config().openfga.unwrap();
            assert_eq!(authz_config.server_request_timeout, Duration::from_secs(4));
            Ok(())
        });
    }

    #[test]
    fn test_openfga_config_rejects_bad_server_deadlines() {
        for value in ["0s", "3m", "fast"] {
            figment::Jail::expect_with(|jail| {
                jail.set_env("LAKEKEEPER_TEST__AUTHZ_BACKEND", "openfga");
                jail.set_env("LAKEKEEPER_TEST__OPENFGA__ENDPOINT", "http://localhost");
                jail.set_env(
                    "LAKEKEEPER_TEST__OPENFGA__SERVER_LIST_OBJECTS_DEADLINE",
                    value,
                );
                let config = std::panic::catch_unwind(get_config);
                assert!(config.is_err(), "{value} must be rejected");
                Ok(())
            });
        }
    }

    #[test]
    fn test_openfga_config_server_deadlines_round_trip() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__AUTHZ_BACKEND", "openfga");
            jail.set_env("LAKEKEEPER_TEST__OPENFGA__ENDPOINT", "http://localhost");
            jail.set_env("LAKEKEEPER_TEST__OPENFGA__SERVER_REQUEST_TIMEOUT", "2500ms");
            let config = get_config();
            let round_trip: DynAppConfig =
                serde_json::from_value(serde_json::to_value(&config).unwrap()).unwrap();
            assert_eq!(round_trip.openfga, config.openfga);
            Ok(())
        });
    }

    #[test]
    fn test_openfga_config_no_auth() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__AUTHZ_BACKEND", "openfga");
            jail.set_env("LAKEKEEPER_TEST__OPENFGA__ENDPOINT", "http://localhost");
            jail.set_env("LAKEKEEPER_TEST__OPENFGA__STORE_NAME", "store_name");
            let config = get_config();
            assert!(config.is_openfga_enabled());
            let authz_config = config.openfga.unwrap();
            assert_eq!(authz_config.store_name, "store_name");

            assert_eq!(authz_config.auth, OpenFGAAuth::Anonymous);

            Ok(())
        });
    }

    #[test]
    fn test_openfga_config_api_key() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__AUTHZ_BACKEND", "OpEnFgA");
            jail.set_env("LAKEKEEPER_TEST__OPENFGA__ENDPOINT", "http://localhost");
            jail.set_env("LAKEKEEPER_TEST__OPENFGA__API_KEY", "api_key");
            let config = get_config();
            assert!(config.is_openfga_enabled());
            let authz_config = config.openfga.unwrap();
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
            jail.set_env("LAKEKEEPER_TEST__OPENFGA__ENDPOINT", "http://localhost");
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
            jail.set_env("LAKEKEEPER_TEST__OPENFGA__ENDPOINT", "http://localhost");
            jail.set_env("LAKEKEEPER_TEST__OPENFGA__CLIENT_ID", "client_id");
            jail.set_env("LAKEKEEPER_TEST__OPENFGA__CLIENT_SECRET", "client_secret");
            jail.set_env(
                "LAKEKEEPER_TEST__OPENFGA__TOKEN_ENDPOINT",
                "https://example.com/token",
            );
            let config = get_config();
            assert!(config.is_openfga_enabled());
            let authz_config = config.openfga.unwrap();
            assert_eq!(authz_config.store_name, "lakekeeper");

            assert_eq!(
                authz_config.auth,
                OpenFGAAuth::ClientCredentials {
                    client_id: "client_id".to_string(),
                    client_secret: "client_secret".to_string(),
                    token_endpoint: "https://example.com/token".parse().unwrap(),
                    scope: None
                }
            );
            Ok(())
        });
    }

    #[test]
    fn test_openfga_client_credentials_with_scope() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__AUTHZ_BACKEND", "openfga");
            jail.set_env("LAKEKEEPER_TEST__OPENFGA__ENDPOINT", "http://localhost");
            jail.set_env("LAKEKEEPER_TEST__OPENFGA__CLIENT_ID", "client_id");
            jail.set_env("LAKEKEEPER_TEST__OPENFGA__CLIENT_SECRET", "client_secret");
            jail.set_env("LAKEKEEPER_TEST__OPENFGA__SCOPE", "openfga");
            jail.set_env(
                "LAKEKEEPER_TEST__OPENFGA__TOKEN_ENDPOINT",
                "https://example.com/token",
            );
            let config = get_config();
            assert!(config.is_openfga_enabled());
            let authz_config = config.openfga.unwrap();

            assert_eq!(
                authz_config.auth,
                OpenFGAAuth::ClientCredentials {
                    client_id: "client_id".to_string(),
                    client_secret: "client_secret".to_string(),
                    token_endpoint: "https://example.com/token".parse().unwrap(),
                    scope: Some("openfga".to_string())
                }
            );
            Ok(())
        });
    }
}
