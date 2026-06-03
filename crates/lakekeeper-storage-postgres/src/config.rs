use std::{path::PathBuf, str::FromStr, sync::LazyLock};

use anyhow::anyhow;
use serde::{Deserialize, Deserializer, Serialize};

/// Default encryption key used by the postgres secrets backend when the
/// operator does not configure one. Exposed so the binary can warn
/// loudly if it's still in use with `secret_backend = Postgres`.
pub const DEFAULT_ENCRYPTION_KEY: &str = "<This is unsafe, please set a proper key>";

pub static CONFIG: LazyLock<DynAppConfig> = LazyLock::new(get_config);

#[derive(Clone, Deserialize, Serialize, Debug)]
pub struct DynAppConfig {
    /// Encryption key used to encrypt secrets at rest in postgres.
    pub pg_encryption_key: String,
    pub pg_database_url_read: Option<String>,
    pub pg_database_url_write: Option<String>,
    pub pg_host_r: Option<String>,
    pub pg_host_w: Option<String>,
    pub pg_port: Option<u16>,
    pub pg_user: Option<String>,
    pub pg_password: Option<String>,
    pub pg_database: Option<String>,
    pub pg_ssl_mode: Option<PgSslMode>,
    pub pg_ssl_root_cert: Option<PathBuf>,
    pub pg_enable_statement_logging: bool,
    pub pg_test_before_acquire: bool,
    pub pg_connection_max_lifetime: Option<u64>,
    pub pg_read_pool_connections: u32,
    pub pg_write_pool_connections: u32,
    pub pg_acquire_timeout: u64,
}

impl Default for DynAppConfig {
    fn default() -> Self {
        Self {
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
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
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
        .expect("Valid Postgres Configuration")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[allow(clippy::result_large_err)] // figment::Error is wide; not worth boxing in test setup.
    fn test_pg_ssl_mode_case_insensitive() {
        for s in ["DISABLED", "DisaBled", "disabled", "disable", "Disable"] {
            figment::Jail::expect_with(|jail| {
                jail.set_env("LAKEKEEPER_TEST__PG_SSL_MODE", s);
                let config = get_config();
                assert_eq!(config.pg_ssl_mode, Some(PgSslMode::Disable));
                Ok(())
            });
        }
    }
}
