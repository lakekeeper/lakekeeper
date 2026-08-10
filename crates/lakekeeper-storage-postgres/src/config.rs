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
    /// Schema holding Lakekeeper's tables. `None` leaves `search_path`
    /// untouched, so objects land wherever the server resolves it.
    pub pg_schema: Option<PgSchema>,
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
            pg_schema: None,
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

/// `NAMEDATALEN - 1`. Postgres truncates longer identifiers, which would make
/// the configured schema differ from the one actually used.
const MAX_SCHEMA_NAME_LEN: usize = 63;

/// Postgres schema holding Lakekeeper's own tables.
///
/// Applied per connection via [`crate::with_search_path`], so no server-side
/// `ALTER ROLE ... SET search_path` is required.
///
/// The name is used as a quoted identifier and is therefore case-sensitive:
/// `LAKEKEEPER__PG_SCHEMA=LakeKeeper` selects a different schema than
/// `lakekeeper`. An unquoted name in `ALTER ROLE ... SET search_path` is
/// interpreted as lower case instead, so the two mechanisms can disagree on a
/// mixed-case name.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct PgSchema(String);

impl PgSchema {
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// `search_path` value: this schema first, then `public`.
    ///
    /// `public` is appended so unqualified objects owned by extensions there
    /// still resolve: `uuid_generate_v1mc()` in column defaults,
    /// `pgp_sym_encrypt` / `pgp_sym_decrypt` in the postgres secret backend,
    /// and the `pg_trgm`, `btree_gin` and `btree_gist` operator classes. This
    /// schema comes first, so nothing in `public` can shadow a Lakekeeper
    /// object.
    #[must_use]
    pub fn search_path_value(&self) -> String {
        format!(r#""{}", public"#, self.0)
    }
}

impl FromStr for PgSchema {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();
        if s.is_empty() {
            return Err(anyhow!(
                "LAKEKEEPER__PG_SCHEMA is empty. Set a schema name, or remove the variable to \
                 keep using the schema the server resolves (usually public)."
            ));
        }
        // Restricting to unquoted-identifier characters keeps `,`, `"`, `.` and
        // whitespace out of `CREATE SCHEMA`, the one statement that has to
        // interpolate the name because DDL cannot bind identifiers. Checked
        // before the length, so the length is reported over ASCII only and
        // characters and bytes agree.
        let mut chars = s.chars();
        let first_ok = chars
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_');
        if !first_ok || !chars.all(|c| c.is_ascii_alphanumeric() || c == '_') {
            return Err(anyhow!(
                "LAKEKEEPER__PG_SCHEMA '{s}' is not a valid schema name. Allowed characters are \
                 a-z, A-Z, 0-9 and _, and the first character cannot be a digit. Example: \
                 lakekeeper"
            ));
        }
        if s.len() > MAX_SCHEMA_NAME_LEN {
            return Err(anyhow!(
                "LAKEKEEPER__PG_SCHEMA '{s}' is {len} characters long. Postgres allows at most \
                 {MAX_SCHEMA_NAME_LEN} and would truncate it.",
                len = s.len(),
            ));
        }
        if s.len() >= 3 && s[..3].eq_ignore_ascii_case("pg_") {
            return Err(anyhow!(
                "LAKEKEEPER__PG_SCHEMA '{s}' starts with 'pg_', which Postgres reserves for its \
                 own schemas. Pick a different name."
            ));
        }
        if s.eq_ignore_ascii_case("information_schema") {
            return Err(anyhow!(
                "LAKEKEEPER__PG_SCHEMA cannot be 'information_schema': Postgres owns that schema. \
                 Pick a different name."
            ));
        }
        Ok(Self(s.to_string()))
    }
}

impl<'de> Deserialize<'de> for PgSchema {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        PgSchema::from_str(&s).map_err(serde::de::Error::custom)
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

    #[test]
    fn test_pg_schema_accepts_valid_names() {
        let max_len = "a".repeat(63);
        for s in ["lakekeeper", "LakeKeeper", "_lk", "lk_2", max_len.as_str()] {
            let schema = PgSchema::from_str(s).unwrap_or_else(|e| panic!("{s:?} rejected: {e}"));
            assert_eq!(schema.as_str(), s);
        }
        // Surrounding whitespace is trimmed, not rejected: env vars pick it up easily.
        assert_eq!(
            PgSchema::from_str("  lakekeeper  ").unwrap().as_str(),
            "lakekeeper"
        );
    }

    #[test]
    fn test_pg_schema_rejects_invalid_names() {
        let too_long = "a".repeat(64);
        for s in [
            "",
            "   ",
            "1abc",
            "my-schema",
            "my schema",
            "my.schema",
            "$user",
            "a,b",
            "pg_catalog",
            "PG_temp",
            "information_schema",
            r#"x"; DROP TABLE warehouse; --"#,
            too_long.as_str(),
        ] {
            assert!(
                PgSchema::from_str(s).is_err(),
                "{s:?} should have been rejected"
            );
        }
    }

    #[test]
    fn test_pg_schema_search_path_value() {
        assert_eq!(
            PgSchema::from_str("lakekeeper")
                .unwrap()
                .search_path_value(),
            r#""lakekeeper", public"#
        );
    }

    #[test]
    #[allow(clippy::result_large_err)] // figment::Error is wide; not worth boxing in test setup.
    fn test_pg_schema_from_env() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__PG_SCHEMA", "lakekeeper");
            assert_eq!(
                get_config().pg_schema.as_ref().map(PgSchema::as_str),
                Some("lakekeeper")
            );
            Ok(())
        });
        figment::Jail::expect_with(|_jail| {
            assert_eq!(get_config().pg_schema, None);
            Ok(())
        });
    }
}
