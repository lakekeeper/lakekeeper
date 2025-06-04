use async_trait::async_trait;
pub use postgres::CatalogState;

use crate::{
    implementations::postgres::{get_reader_pool, get_writer_pool},
    service::{
        endpoint_statistics::EndpointStatisticsSink,
        health::{Health, HealthExt},
        secrets::{Secret, SecretInStorage},
        SecretStore,
    },
    SecretBackend, SecretIdent, CONFIG,
};

#[cfg(feature = "sqlx-postgres")]
pub mod postgres;

pub mod kv2;

/// Get the default Catalog Backend & Secret Store from the configuration.
///
/// # Errors
/// - If the database connection pools cannot be created.
// This is handled in one function as the CatalogState and the SecretStore
// might share the same database connection pool.
pub async fn get_default_catalog_from_config() -> anyhow::Result<(
    CatalogState,
    Secrets,
    std::sync::Arc<dyn EndpointStatisticsSink + 'static>,
)> {
    let read_pool = get_reader_pool(
        CONFIG
            .to_pool_opts()
            .max_connections(CONFIG.pg_read_pool_connections),
    )
    .await?;
    let write_pool = get_writer_pool(
        CONFIG
            .to_pool_opts()
            .max_connections(CONFIG.pg_write_pool_connections),
    )
    .await?;

    let catalog_state = CatalogState::from_pools(read_pool.clone(), write_pool.clone());
    let secrets_state: Secrets = match CONFIG.secret_backend {
        SecretBackend::KV2 => kv2::SecretsState::from_config(
            CONFIG
                .kv2
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("Need vault config to use vault as backend"))?,
        )
        .await?
        .into(),
        SecretBackend::Postgres => {
            postgres::SecretsState::from_pools(read_pool.clone(), write_pool.clone()).into()
        }
    };

    let stats_sink = std::sync::Arc::new(postgres::PostgresStatisticsSink::new(
        catalog_state.write_pool(),
    ));

    Ok((catalog_state, secrets_state, stats_sink))
}

#[derive(Debug, Clone)]
pub enum Secrets {
    Postgres(crate::implementations::postgres::SecretsState),
    KV2(crate::implementations::kv2::SecretsState),
}

#[async_trait]
impl SecretStore for Secrets {
    async fn get_secret_by_id<S: SecretInStorage + serde::de::DeserializeOwned>(
        &self,
        secret_id: SecretIdent,
    ) -> crate::api::Result<Secret<S>> {
        match self {
            Self::Postgres(state) => state.get_secret_by_id(secret_id).await,
            Self::KV2(state) => state.get_secret_by_id(secret_id).await,
        }
    }

    async fn create_secret<
        S: SecretInStorage + Send + Sync + serde::Serialize + std::fmt::Debug,
    >(
        &self,
        secret: S,
    ) -> crate::api::Result<SecretIdent> {
        match self {
            Self::Postgres(state) => state.create_secret(secret).await,
            Self::KV2(state) => state.create_secret(secret).await,
        }
    }

    async fn delete_secret(&self, secret_id: &SecretIdent) -> crate::api::Result<()> {
        match self {
            Self::Postgres(state) => state.delete_secret(secret_id).await,
            Self::KV2(state) => state.delete_secret(secret_id).await,
        }
    }
}

#[async_trait]
impl HealthExt for Secrets {
    async fn health(&self) -> Vec<Health> {
        match self {
            Self::Postgres(state) => state.health().await,
            Self::KV2(state) => state.health().await,
        }
    }

    async fn update_health(&self) {
        match self {
            Self::Postgres(state) => state.update_health().await,
            Self::KV2(state) => state.update_health().await,
        }
    }
}

impl From<crate::implementations::postgres::SecretsState> for Secrets {
    fn from(state: crate::implementations::postgres::SecretsState) -> Self {
        Self::Postgres(state)
    }
}

impl From<crate::implementations::kv2::SecretsState> for Secrets {
    fn from(state: crate::implementations::kv2::SecretsState) -> Self {
        Self::KV2(state)
    }
}
