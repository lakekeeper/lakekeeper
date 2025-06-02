use async_trait::async_trait;
use http::StatusCode;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

use super::ReadWrite;
use crate::{
    api::{ErrorModel, Result},
    service::{
        health::{Health, HealthExt},
        secrets::{Secret, SecretIdent, SecretStore},
    },
    CONFIG,
};

#[derive(Debug, Clone)]
pub struct SecretsState {
    read_write: ReadWrite,
}

#[async_trait]
impl HealthExt for SecretsState {
    async fn health(&self) -> Vec<Health> {
        self.read_write.health().await
    }

    async fn update_health(&self) {
        self.read_write.update_health().await;
    }
}

impl SecretsState {
    #[must_use]
    pub fn from_pools(read_pool: PgPool, write_pool: PgPool) -> Self {
        Self {
            read_write: ReadWrite::from_pools(read_pool, write_pool),
        }
    }

    #[must_use]
    pub fn read_pool(&self) -> PgPool {
        self.read_write.read_pool.clone()
    }

    #[must_use]
    pub fn write_pool(&self) -> PgPool {
        self.read_write.write_pool.clone()
    }
}

#[async_trait::async_trait]
impl SecretStore for SecretsState {
    /// Get the secret for a given warehouse.
    async fn get_secret_by_id<S: for<'de> Deserialize<'de>>(
        &self,
        secret_id: SecretIdent,
    ) -> Result<Secret<S>> {
        struct SecretRow {
            secret: Option<String>,
            created_at: chrono::DateTime<chrono::Utc>,
            updated_at: Option<chrono::DateTime<chrono::Utc>>,
        }

        let secret: SecretRow = sqlx::query_as!(
            SecretRow,
            r#"
            SELECT 
                pgp_sym_decrypt(secret, $2, 'cipher-algo=aes256') as secret,
                created_at,
                updated_at
            FROM secret
            WHERE secret_id = $1
            "#,
            secret_id.as_uuid(),
            CONFIG.pg_encryption_key
        )
        .fetch_one(&self.read_write.read_pool)
        .await
        .map_err(|e| match e {
            sqlx::Error::RowNotFound => ErrorModel::builder()
                .code(StatusCode::NOT_FOUND.into())
                .message("Secret not found".to_string())
                .r#type("SecretNotFound".to_string())
                .stack(vec![format!("secret_id: {}", secret_id), e.to_string()])
                .build(),
            _ => ErrorModel::builder()
                .code(StatusCode::INTERNAL_SERVER_ERROR.into())
                .message("Error fetching secret".to_string())
                .r#type("SecretFetchError".to_string())
                .stack(vec![format!("secret_id: {}", secret_id), e.to_string()])
                .build(),
        })?;

        let inner =
            serde_json::from_str(&secret.secret.unwrap_or("{}".to_string())).map_err(|_e| {
                ErrorModel::builder()
                    .code(StatusCode::INTERNAL_SERVER_ERROR.into())
                    .message("Error parsing secret".to_string())
                    .r#type("SecretParseError".to_string())
                    // We do not add the error here as it might contain sensitive information
                    .stack(vec![format!("Secret ID: {}", secret_id)])
                    .build()
            })?;

        Ok(Secret {
            secret_id,
            secret: inner,
            created_at: secret.created_at,
            updated_at: secret.updated_at,
        })
    }

    /// Create a new secret
    async fn create_secret<S: Send + Sync + Serialize + std::fmt::Debug>(
        &self,
        secret: S,
    ) -> Result<SecretIdent> {
        let secret_str = serde_json::to_string(&secret).map_err(|_e| {
            ErrorModel::builder()
                .code(StatusCode::INTERNAL_SERVER_ERROR.into())
                .message("Error serializing secret".to_string())
                .r#type("SecretSerializeError".to_string())
                // Redacted by veil
                .stack(vec![format!("secret: {:?}", secret)])
                .build()
        })?;

        let secret_id = sqlx::query_scalar!(
            r#"
            INSERT INTO secret (secret)
            VALUES (pgp_sym_encrypt($1, $2, 'cipher-algo=aes256'))
            RETURNING secret_id
            "#,
            secret_str,
            CONFIG.pg_encryption_key,
        )
        .fetch_one(&self.write_pool())
        .await
        .map_err(|e| {
            ErrorModel::builder()
                .code(StatusCode::INTERNAL_SERVER_ERROR.into())
                .message("Error creating secret".to_string())
                .r#type("SecretCreateError".to_string())
                .source(Some(Box::new(e)))
                .build()
        })?;

        Ok(secret_id.into())
    }

    /// Delete a secret
    async fn delete_secret(&self, secret_id: &SecretIdent) -> Result<()> {
        sqlx::query!(
            r#"
            DELETE FROM secret
            WHERE secret_id = $1
            "#,
            secret_id.as_uuid()
        )
        .execute(&self.read_write.write_pool)
        .await
        .map_err(|e| {
            ErrorModel::builder()
                .code(StatusCode::INTERNAL_SERVER_ERROR.into())
                .message("Error deleting secret".to_string())
                .r#type("SecretDeleteError".to_string())
                .stack(vec![format!("secret_id: {}", secret_id)])
                .source(Some(Box::new(e)))
                .build()
        })?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::storage::{s3::S3AccessKeyCredential, S3Credential, StorageCredential};

    #[sqlx::test]
    async fn test_write_read_secret(pool: sqlx::PgPool) {
        let state = SecretsState::from_pools(pool.clone(), pool);

        let secret: StorageCredential = S3Credential::AccessKey(S3AccessKeyCredential {
            aws_access_key_id: "my access key".to_string(),
            aws_secret_access_key: "my secret key".to_string(),
            external_id: None,
        })
        .into();

        let secret_id = state.create_secret(secret.clone()).await.unwrap();

        let read_secret = state
            .get_secret_by_id::<StorageCredential>(secret_id)
            .await
            .unwrap();

        assert_eq!(read_secret.secret, secret);
    }

    #[sqlx::test]
    async fn test_delete_secret(pool: sqlx::PgPool) {
        let state = SecretsState::from_pools(pool.clone(), pool);

        let secret: StorageCredential = S3Credential::AccessKey(S3AccessKeyCredential {
            aws_access_key_id: "my access key".to_string(),
            aws_secret_access_key: "my secret key".to_string(),
            external_id: None,
        })
        .into();

        let secret_id = state.create_secret(secret.clone()).await.unwrap();

        state.delete_secret(&secret_id).await.unwrap();

        let read_secret = state.get_secret_by_id::<StorageCredential>(secret_id).await;

        assert!(read_secret.is_err());
    }
}
