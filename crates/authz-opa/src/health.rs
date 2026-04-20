use lakekeeper::{
    async_trait::async_trait,
    service::health::{Health, HealthExt, HealthStatus},
};

use crate::OpaAuthorizer;

#[async_trait]
impl HealthExt for OpaAuthorizer {
    async fn health(&self) -> Vec<Health> {
        self.health.read().await.clone()
    }

    async fn update_health(&self) {
        let health = match self.probe_health().await {
            Ok(()) => Health::now("opa", HealthStatus::Healthy),
            Err(e) => {
                tracing::error!("OPA health check failed: {e}");
                Health::now("opa", HealthStatus::Unhealthy)
            }
        };

        let mut lock = self.health.write().await;
        lock.clear();
        lock.extend([health]);
    }
}
