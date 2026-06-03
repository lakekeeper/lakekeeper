use std::sync::LazyLock;

use serde::{Deserialize, Serialize};
use url::Url;
use veil::Redact;

pub static CONFIG: LazyLock<DynAppConfig> = LazyLock::new(get_config);

#[derive(Clone, Deserialize, Serialize, Debug, Default)]
pub struct DynAppConfig {
    /// Vault connection settings. Required when
    /// `lakekeeper::CONFIG.secret_backend == SecretBackend::KV2`; ignored
    /// otherwise.
    pub kv2: Option<KV2Config>,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Redact)]
pub struct KV2Config {
    pub url: Url,
    pub user: String,
    #[redact]
    pub password: String,
    pub secret_mount: String,
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

    config
        .extract::<DynAppConfig>()
        .expect("Valid KV2 Configuration")
}
