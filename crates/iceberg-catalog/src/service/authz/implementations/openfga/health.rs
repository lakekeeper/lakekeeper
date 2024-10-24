use async_trait::async_trait;
use openfga_rs::{CheckRequest, CheckRequestTupleKey, ConsistencyPreference};

use super::OpenFGAAuthorizer;
use crate::service::health::{Health, HealthExt, HealthStatus};

#[async_trait]
impl HealthExt for OpenFGAAuthorizer {
    async fn health(&self) -> Vec<Health> {
        self.health.read().await.clone()
    }
    async fn update_health(&self) {
        let mut client = self.client.clone();

        let check_result = client
            .check(CheckRequest {
                store_id: self.store_id.clone(),
                tuple_key: Some(CheckRequestTupleKey {
                    user: "server:*".to_string(),
                    relation: "applied".to_string(),
                    object: "model_version:1".to_string(),
                }),
                contextual_tuples: None,
                authorization_model_id: self.authorization_model_id.clone(),
                trace: false,
                context: None,
                consistency: ConsistencyPreference::MinimizeLatency.into(),
            })
            .await;

        let health = match check_result {
            Ok(_) => Health::now("openfga", HealthStatus::Healthy),
            Err(e) => {
                tracing::error!("OpenFGA health check failed: {:?}", e);
                Health::now("openfga", HealthStatus::Unhealthy)
            }
        };

        let mut lock = self.health.write().await;
        lock.clear();
        lock.extend([health]);
    }
}

#[cfg(test)]
mod tests {
    use needs_env_var::needs_env_var;

    #[needs_env_var(TEST_OPENFGA = 1)]
    mod openfga {
        use super::super::*;
        use crate::service::authz::implementations::openfga::{
            client::new_authorizer, migrate, new_client_from_config,
        };

        #[tokio::test]
        async fn test_health() {
            let mut client = new_client_from_config().await.unwrap();

            let store_name = format!("test_store_{}", uuid::Uuid::now_v7());
            migrate(&mut client, Some(store_name.clone()))
                .await
                .unwrap();

            let authorizer = new_authorizer(client.clone(), Some(store_name))
                .await
                .unwrap();

            authorizer.update_health().await;
            let health = authorizer.health().await;
            assert_eq!(health.len(), 1);
            assert_eq!(health[0].status(), HealthStatus::Healthy);
        }
    }
}
