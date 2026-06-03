//! Binary-local secrets-backend dispatcher.
//!
//! Mirrors [`crate::authorizer::AuthorizerEnum`]: each backend crate
//! (`lakekeeper-storage-postgres` for Postgres-backed secrets,
//! `lakekeeper-secrets-kv2` for Vault) owns its own concrete
//! `SecretsState` type; this enum sits in the binary, statically
//! dispatches between them, and lets the rest of the API context treat
//! the result as a single `S: SecretStore` parameter.

use async_trait::async_trait;
use lakekeeper::{
    SecretId,
    service::{
        SecretStore,
        health::{Health, HealthExt},
        secrets::{Secret, SecretInStorage},
    },
};

#[derive(Debug, Clone)]
pub(crate) enum SecretsEnum {
    Postgres(lakekeeper_storage_postgres::SecretsState),
    KV2(lakekeeper_secrets_kv2::SecretsState),
}

#[async_trait]
impl SecretStore for SecretsEnum {
    async fn get_secret_by_id_impl<S: SecretInStorage + serde::de::DeserializeOwned>(
        &self,
        secret_id: SecretId,
    ) -> lakekeeper::api::Result<Option<Secret<S>>> {
        match self {
            Self::Postgres(state) => state.get_secret_by_id_impl(secret_id).await,
            Self::KV2(state) => state.get_secret_by_id_impl(secret_id).await,
        }
    }

    async fn create_secret_impl<
        S: SecretInStorage + Send + Sync + serde::Serialize + std::fmt::Debug,
    >(
        &self,
        secret: S,
    ) -> lakekeeper::api::Result<SecretId> {
        match self {
            Self::Postgres(state) => state.create_secret_impl(secret).await,
            Self::KV2(state) => state.create_secret_impl(secret).await,
        }
    }

    async fn delete_secret_impl(&self, secret_id: &SecretId) -> lakekeeper::api::Result<()> {
        match self {
            Self::Postgres(state) => state.delete_secret_impl(secret_id).await,
            Self::KV2(state) => state.delete_secret_impl(secret_id).await,
        }
    }
}

#[async_trait]
impl HealthExt for SecretsEnum {
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

impl From<lakekeeper_storage_postgres::SecretsState> for SecretsEnum {
    fn from(state: lakekeeper_storage_postgres::SecretsState) -> Self {
        Self::Postgres(state)
    }
}

impl From<lakekeeper_secrets_kv2::SecretsState> for SecretsEnum {
    fn from(state: lakekeeper_secrets_kv2::SecretsState) -> Self {
        Self::KV2(state)
    }
}
