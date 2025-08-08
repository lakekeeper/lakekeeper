use async_trait::async_trait;
use openfga_client::client::CheckRequestTupleKey;

use super::{OpenFGAAuthorizer, OpenFgaEntity, ServerRelation, OPENFGA_SERVER};
use crate::{
    service::health::{Health, HealthExt, HealthStatus},
    ProjectId,
};

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
                object: OPENFGA_SERVER.to_string(),
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
        use openfga_client::client::ConsistencyPreference;

        use super::super::*;
        use crate::{
            implementations::postgres::{self, PostgresCatalog},
            service::authz::implementations::openfga::{
                client::new_authorizer, migrate, new_client_from_config,
            },
        };

        #[sqlx::test]
        async fn test_health(pool: sqlx::PgPool) {
            let client = new_client_from_config().await.unwrap();

            let store_name = format!("test_store_{}", uuid::Uuid::now_v7());
            let catalog = PostgresCatalog {};
            let catalog_state = postgres::CatalogState::from_pools(pool.clone(), pool.clone());
            migrate(&client, Some(store_name.clone()), catalog, catalog_state)
                .await
                .unwrap();

            let authorizer = new_authorizer::<PostgresCatalog>(
                client.clone(),
                Some(store_name),
                ConsistencyPreference::HigherConsistency,
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
