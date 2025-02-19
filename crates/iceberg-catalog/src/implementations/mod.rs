use async_trait::async_trait;

use crate::{
    service::{
        health::{Health, HealthExt},
        secrets::{Secret, SecretInStorage},
        SecretStore,
    },
    SecretIdent,
};

#[cfg(feature = "sqlx-postgres")]
pub mod postgres;

pub mod kv2;

#[derive(Debug, Clone)]
pub enum Secrets {
    Postgres(crate::implementations::postgres::SecretsState),
    KV2(crate::implementations::kv2::SecretsState),
}

#[async_trait]
impl SecretStore for Secrets {
    async fn get_secret_by_id<S: SecretInStorage + serde::de::DeserializeOwned>(
        &self,
        secret_id: &SecretIdent,
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
