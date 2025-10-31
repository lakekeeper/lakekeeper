use lakekeeper::{
    async_trait::async_trait,
    service::health::{Health, HealthExt, HealthStatus},
    ProjectId,
};
use openfga_client::client::CheckRequestTupleKey;

use crate::{entities::OpenFgaEntity, relations::ServerRelation, OpenFGAAuthorizer};

#[async_trait]
impl HealthExt for OpenFGAAuthorizer {
    async fn health(&self) -> Vec<Health> {
        self.health.read().await.clone()
    }
    async fn update_health(&self) {
        let check_result = self
            .check(CheckRequestTupleKey {
                user: ProjectId::new_random().to_openfga(),
                relation: ServerRelation::Project.to_string(),
                object: self.openfga_server().clone(),
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
    mod openfga_integration_tests {
        use lakekeeper::{service::ServerId, tokio};
        use openfga_client::client::ConsistencyPreference;

        use super::super::*;
        use crate::{
            client::{new_authorizer, new_client_from_default_config},
            migration::migrate,
        };

        #[tokio::test]
        async fn test_health() {
            let client = new_client_from_default_config().await.unwrap();

            let server_id = ServerId::new_random();
            let store_name = format!("test_store_{}", uuid::Uuid::now_v7());
            migrate(&client, Some(store_name.clone()), server_id)
                .await
                .unwrap();

            let authorizer = new_authorizer(
                client.clone(),
                Some(store_name),
                ConsistencyPreference::HigherConsistency,
                server_id,
            )
            .await
            .unwrap();

            authorizer.update_health().await;
            let health = authorizer.health().await;
            assert_eq!(health.len(), 1);
            assert_eq!(health[0].status(), HealthStatus::Healthy);
        }
    }
}
