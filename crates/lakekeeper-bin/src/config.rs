use std::sync::LazyLock;

use serde::{Deserialize, Serialize};

pub(crate) static CONFIG_BIN: LazyLock<DynAppConfig> = LazyLock::new(get_config);

#[derive(Clone, Deserialize, Serialize, Debug, Default)]
pub(crate) struct DynAppConfig {
    /// Run migration before serving requests. This can simplify testing.
    /// We do not recommend enabling this in production, especially if
    /// multiple instances of Lakekeeper are running.
    pub(crate) debug: DebugConfig,
}

#[derive(Clone, Deserialize, Serialize, Debug, Default)]
pub(crate) struct DebugConfig {
    pub(crate) migrate_before_serve: bool,
    /// Run the serve command unless another command is specified.
    pub(crate) auto_serve: bool,
}

fn get_config() -> DynAppConfig {
    let defaults = figment::providers::Serialized::defaults(DynAppConfig::default());

    #[cfg(not(test))]
    let prefixes = &["LAKEKEEPER__"];
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
            panic!("Failed to extract Lakekeeper Binary config: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_migrate_before_serve_env_vars() {
        figment::Jail::expect_with(|_jail| {
            let config = get_config();
            assert!(!config.debug.migrate_before_serve);
            Ok(())
        });

        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__DEBUG__MIGRATE_BEFORE_SERVE", "true");
            let config = get_config();
            assert!(config.debug.migrate_before_serve);
            Ok(())
        });

        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__DEBUG__MIGRATE_BEFORE_SERVE", "false");
            let config = get_config();
            assert!(!config.debug.migrate_before_serve);
            Ok(())
        });
    }

    #[test]
    fn test_auto_serve_env_vars() {
        figment::Jail::expect_with(|_jail| {
            let config = get_config();
            assert!(!config.debug.auto_serve);
            Ok(())
        });

        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__DEBUG__AUTO_SERVE", "true");
            let config = get_config();
            assert!(config.debug.auto_serve);
            Ok(())
        });
    }
}
