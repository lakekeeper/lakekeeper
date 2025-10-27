use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};

use crate::{api::Result, service::health::HealthExt};

/// Interface for Handling Secrets.
#[async_trait]

pub trait SecretStore
where
    Self: Send + Sync + 'static + HealthExt + Clone + std::fmt::Debug,
{
    /// Get the secret for a given warehouse.
    async fn get_secret_by_id<S: SecretInStorage + DeserializeOwned>(
        &self,
        secret_id: SecretId,
    ) -> Result<Secret<S>>;

    /// Create a new secret
    async fn create_secret<S: SecretInStorage + Send + Sync + Serialize + std::fmt::Debug>(
        &self,
        secret: S,
    ) -> Result<SecretId>;

    /// Delete a secret
    async fn delete_secret(&self, secret_id: &SecretId) -> Result<()>;
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "sqlx", derive(sqlx::Type))]
#[cfg_attr(feature = "sqlx", sqlx(transparent))]
// Is UUID here too strict?
pub struct SecretId(uuid::Uuid);

impl SecretId {
    #[must_use]
    #[inline]
    pub fn into_uuid(&self) -> uuid::Uuid {
        self.0
    }

    #[must_use]
    #[inline]
    pub fn as_uuid(&self) -> &uuid::Uuid {
        &self.0
    }
}

impl From<uuid::Uuid> for SecretId {
    fn from(uuid: uuid::Uuid) -> Self {
        Self(uuid)
    }
}

impl From<SecretId> for uuid::Uuid {
    fn from(ident: SecretId) -> Self {
        ident.0
    }
}

impl std::fmt::Display for SecretId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone)]
pub struct Secret<T> {
    pub secret_id: SecretId,
    pub secret: T,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: Option<chrono::DateTime<chrono::Utc>>,
}

// Prohibits us to store unwanted types in the storage.
pub trait SecretInStorage {}
