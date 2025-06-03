#![allow(clippy::module_name_repetitions)]
use std::{collections::HashMap, fmt::Formatter, sync::Arc, time::Duration};

use itertools::{FoldWhile, Itertools};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;

#[async_trait::async_trait]
pub trait HealthExt: Send + Sync + 'static {
    async fn health(&self) -> Vec<Health>;
    async fn update_health(&self);
    async fn update_health_task(
        self: Arc<Self>,
        refresh_interval: Duration,
        jitter_millis: u64,
    ) -> JoinHandle<()> {
        tokio::task::spawn(async move {
            loop {
                self.update_health().await;
                let jitter = { rand::rng().next_u64().min(jitter_millis) };
                tokio::time::sleep(refresh_interval + Duration::from_millis(jitter)).await;
            }
        })
    }
}

#[derive(Clone, Debug, Copy, PartialEq, strum::Display, Deserialize, Serialize)]
pub enum HealthStatus {
    #[serde(rename = "ok")]
    Healthy,
    #[serde(rename = "error")]
    Unhealthy,
    #[serde(rename = "unknown")]
    Unknown,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Health {
    name: String,
    #[serde(with = "chrono::serde::ts_milliseconds", rename = "lastCheck")]
    checked_at: chrono::DateTime<chrono::Utc>,
    status: HealthStatus,
}

impl Health {
    #[must_use]
    pub fn now(name: &'static str, status: HealthStatus) -> Self {
        Self {
            name: name.into(),
            checked_at: chrono::Utc::now(),
            status,
        }
    }

    #[must_use]
    pub fn status(&self) -> HealthStatus {
        self.status
    }
}

#[derive(Clone)]
pub struct ServiceHealthProvider {
    providers: Vec<(&'static str, Arc<dyn HealthExt + Sync + Send>)>,
    check_jitter_millis: u64,
    check_frequency_seconds: u64,
}

impl std::fmt::Debug for ServiceHealthProvider {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServiceHealthProvider")
            .field(
                "providers",
                &self
                    .providers
                    .iter()
                    .map(|(name, _)| *name)
                    .collect::<Vec<_>>(),
            )
            .field("check_jitter_millis", &self.check_jitter_millis)
            .field("check_frequency_seconds", &self.check_frequency_seconds)
            .finish()
    }
}

impl ServiceHealthProvider {
    #[must_use]
    pub fn new(
        providers: Vec<(&'static str, Arc<dyn HealthExt + Sync + Send>)>,
        check_frequency_seconds: u64,
        check_jitter_millis: u64,
    ) -> Self {
        Self {
            providers,
            check_jitter_millis,
            check_frequency_seconds,
        }
    }

    pub async fn spawn_health_checks(&self) {
        for (service_name, provider) in &self.providers {
            let provider = provider.clone();
            provider
                .update_health_task(
                    Duration::from_secs(self.check_frequency_seconds),
                    self.check_jitter_millis,
                )
                .await;
            tracing::info!("Spawned health provider: {service_name}");
        }
    }

    pub async fn collect_health(&self) -> HealthState {
        let mut services = HashMap::new();
        let mut all_healthy = true;
        for (name, provider) in &self.providers {
            let provider_health = provider.health().await;
            all_healthy = all_healthy
                && provider_health
                    .iter()
                    .fold_while(true, |mut all_good, s| {
                        all_good = all_good && matches!(s.status, HealthStatus::Healthy);
                        if all_good {
                            FoldWhile::Continue(true)
                        } else {
                            FoldWhile::Done(false)
                        }
                    })
                    .into_inner();
            services.insert((*name).to_string(), provider_health);
        }

        HealthState {
            health: if all_healthy {
                HealthStatus::Healthy
            } else {
                HealthStatus::Unhealthy
            },
            services,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HealthState {
    pub health: HealthStatus,
    pub services: HashMap<String, Vec<Health>>,
}
