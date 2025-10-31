use std::sync::LazyLock;

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

    let config = match config.extract::<DynAppConfig>() {
        Ok(c) => c,
        Err(e) => {
            panic!("Failed to extract OpenFGA config: {e}");
        }
    };

    config
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
    /// Version of the model to use. If specified, the specified
    /// model version must already exist.
    /// This can be used to roll-back to previously applied model versions
    /// or to connect to externally managed models.
    /// Migration is disabled if the model version is set.
    /// Version should have the format <major>.<minor>.
    pub authorization_model_version: Option<String>,
    /// The maximum number of checks than can be handled by a batch check
    /// request. This is a [configuration option] of the `OpenFGA` server
    /// with default value 50.
    ///
    /// [configuration option]: https://openfga.dev/docs/getting-started/setup-openfga/configuration#OPENFGA_MAX_CHECKS_PER_BATCH_CHECK
    #[serde(default = "default_openfga_max_batch_check_size")]
    pub max_batch_check_size: usize,
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

    Ok(Some(OpenFGAConfig {
        endpoint,
        store_name,
        auth,
        authorization_model_prefix,
        authorization_model_version,
        max_batch_check_size,
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
    }
    .serialize(serializer)
}

#[derive(Serialize, Deserialize, PartialEq, Redact)]
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
}

fn default_openfga_store_name() -> String {
    "lakekeeper".to_string()
}

fn default_openfga_model_prefix() -> String {
    "collaboration".to_string()
}

fn default_openfga_max_batch_check_size() -> usize {
    50
}

#[cfg(test)]
mod test {
    use super::*;

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
