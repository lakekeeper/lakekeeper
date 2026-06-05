use std::{collections::HashMap, sync::LazyLock};

use serde::{Deserialize, Serialize};
use veil::Redact;

pub static CONFIG: LazyLock<DynAppConfig> = LazyLock::new(get_config);

#[derive(Clone, Deserialize, Serialize, Default, Debug)]
pub struct DynAppConfig {
    pub kafka_topic: Option<String>,
    pub kafka_config: Option<KafkaConfig>,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Redact)]
pub struct KafkaConfig {
    #[serde(rename = "sasl.password")]
    #[redact]
    pub sasl_password: Option<String>,
    #[serde(rename = "sasl.oauthbearer.client.secret")]
    #[redact]
    pub sasl_oauthbearer_client_secret: Option<String>,
    #[serde(rename = "ssl.key.password")]
    #[redact]
    pub ssl_key_password: Option<String>,
    #[serde(rename = "ssl.keystore.password")]
    #[redact]
    pub ssl_keystore_password: Option<String>,
    #[serde(flatten)]
    pub conf: HashMap<String, String>,
}

fn get_config() -> DynAppConfig {
    let defaults = figment::providers::Serialized::defaults(DynAppConfig::default());

    #[cfg(not(test))]
    let prefixes = &["ICEBERG_REST__", "LAKEKEEPER__"];
    #[cfg(test)]
    let prefixes = &["LAKEKEEPER_TEST__"];

    // Support `*__KAFKA_CONFIG_FILE=/path/to/json` for the structured kafka_config field.
    let file_keys = &["kafka_config"];

    let mut config = figment::Figment::from(defaults);
    for prefix in prefixes {
        let env = figment::providers::Env::prefixed(prefix).split("__");
        config = config
            .merge(figment_file_provider_adapter::FileAdapter::wrap(env.clone()).only(file_keys))
            .merge(env);
    }

    config
        .extract::<DynAppConfig>()
        .expect("Valid Kafka Configuration")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[allow(clippy::result_large_err)] // figment::Error is wide; not worth boxing in test setup.
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
    #[allow(clippy::result_large_err)]
    fn test_kafka_config_file() {
        let named_tmp_file = tempfile::NamedTempFile::new().unwrap();
        std::io::Write::write_all(
            &mut named_tmp_file.as_file(),
            r#"{"sasl.password"="my_pw","bootstrap.servers"="host1:port,host2:port","security.protocol"="SSL"}"#.as_bytes(),
        )
        .unwrap();
        figment::Jail::expect_with(|jail| {
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
}
