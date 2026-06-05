use std::{path::PathBuf, sync::LazyLock};

use serde::{Deserialize, Serialize};
use url::Url;
use veil::Redact;

pub static CONFIG: LazyLock<DynAppConfig> = LazyLock::new(get_config);

#[derive(Clone, Deserialize, Serialize, Default, Redact)]
pub struct DynAppConfig {
    pub nats_address: Option<Url>,
    pub nats_topic: Option<String>,
    pub nats_creds_file: Option<PathBuf>,
    pub nats_user: Option<String>,
    #[redact]
    pub nats_password: Option<String>,
    #[redact]
    pub nats_token: Option<String>,
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
        .expect("Valid NATS Configuration")
}
