use std::sync::LazyLock;

use lakekeeper::AuthZBackend;
use serde::{Deserialize, Serialize};
use url::Url;
use veil::Redact;

pub static CONFIG: LazyLock<DynAppConfig> = LazyLock::new(get_config);

#[derive(Clone, Deserialize, Serialize, Debug, Default)]
pub struct DynAppConfig {
    #[serde(default)]
    pub authz_backend: AuthZBackend,
    pub opa: Option<OpaConfig>,
}

impl DynAppConfig {
    #[must_use]
    pub fn is_opa_enabled(&self) -> bool {
        self.authz_backend == AuthZBackend::External("opa".to_string())
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
        Err(e) => panic!("Failed to extract OPA config: {e}"),
    }
}

/// Configuration for the OPA (Open Policy Agent) authorizer.
///
/// Decisions are resolved by `POST`ing one `input` document per `(resource, action)`
/// tuple to the OPA data API. The full URL for each decision is
/// `{endpoint}/v1/data/{policy_path}` and OPA must reply with `{"result": <bool>}`.
#[derive(Clone, Serialize, Deserialize, PartialEq, Redact)]
pub struct OpaConfig {
    /// Base URL of the OPA server (e.g. `http://localhost:8181`). The decision
    /// path is appended to this base URL.
    pub endpoint: Url,
    /// Policy path under `/v1/data` whose boolean `result` drives the decision.
    /// Defaults to `lakekeeper/authz/allow`, resolving to
    /// `POST {endpoint}/v1/data/lakekeeper/authz/allow`.
    #[serde(default = "default_opa_policy_path")]
    pub policy_path: String,
    /// Per-request HTTP timeout in milliseconds. Defaults to 5000.
    #[serde(default = "default_opa_request_timeout_ms")]
    pub request_timeout_ms: u64,
    /// Upper bound on parallel in-flight decisions when batching. Defaults to 50.
    #[serde(default = "default_opa_max_concurrency")]
    pub max_concurrency: usize,
    /// Optional bearer token sent as `Authorization: Bearer …` on every decision
    /// request. Intended for sidecar deployments that require simple auth.
    #[serde(default)]
    #[redact]
    pub bearer_token: Option<String>,
}

fn default_opa_policy_path() -> String {
    "lakekeeper/authz/allow".to_string()
}

fn default_opa_request_timeout_ms() -> u64 {
    5_000
}

fn default_opa_max_concurrency() -> usize {
    50
}

#[cfg(test)]
#[allow(clippy::result_large_err)]
mod test {
    use super::*;

    #[test]
    fn test_opa_config_minimal() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__AUTHZ_BACKEND", "opa");
            jail.set_env("LAKEKEEPER_TEST__OPA__ENDPOINT", "http://localhost:8181");
            let config = get_config();
            assert!(config.is_opa_enabled());
            let opa = config.opa.unwrap();
            assert_eq!(opa.policy_path, "lakekeeper/authz/allow");
            assert_eq!(opa.request_timeout_ms, 5_000);
            assert_eq!(opa.max_concurrency, 50);
            assert!(opa.bearer_token.is_none());
            Ok(())
        });
    }

    #[test]
    fn test_opa_config_overrides() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__AUTHZ_BACKEND", "opa");
            jail.set_env("LAKEKEEPER_TEST__OPA__ENDPOINT", "http://opa.local:8181");
            jail.set_env(
                "LAKEKEEPER_TEST__OPA__POLICY_PATH",
                "vauban/lakekeeper/allow",
            );
            jail.set_env("LAKEKEEPER_TEST__OPA__REQUEST_TIMEOUT_MS", "2000");
            jail.set_env("LAKEKEEPER_TEST__OPA__MAX_CONCURRENCY", "16");
            jail.set_env("LAKEKEEPER_TEST__OPA__BEARER_TOKEN", "s3cr3t");
            let opa = get_config().opa.unwrap();
            assert_eq!(opa.policy_path, "vauban/lakekeeper/allow");
            assert_eq!(opa.request_timeout_ms, 2_000);
            assert_eq!(opa.max_concurrency, 16);
            assert_eq!(opa.bearer_token.as_deref(), Some("s3cr3t"));
            Ok(())
        });
    }

    #[test]
    fn test_opa_not_enabled_when_backend_mismatch() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__AUTHZ_BACKEND", "allow_all");
            let config = get_config();
            assert!(!config.is_opa_enabled());
            Ok(())
        });
    }
}
