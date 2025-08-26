#![allow(clippy::module_name_repetitions)]
use std::{collections::HashMap, fmt::Formatter, sync::Arc, time::Duration};

use itertools::{FoldWhile, Itertools};
use serde::{Deserialize, Serialize};
use tokio::task::{AbortHandle, JoinSet};

use crate::CancellationToken;

#[async_trait::async_trait]
pub trait HealthExt: Send + Sync + 'static {
    async fn health(&self) -> Vec<Health>;
    async fn update_health(&self);
    async fn update_health_loop(
        self: Arc<Self>,
        refresh_interval: Duration,
        cancellation_token: crate::CancellationToken,
    ) {
        loop {
            // Exit promptly if already cancelled before doing any work
            if cancellation_token.is_cancelled() {
                break;
            }

            self.update_health().await;

            // Jitter is a random value between 0 and 500 milliseconds (inclusive)
            let jitter = fastrand::u64(0..=500);
            tokio::select! {
                () = cancellation_token.cancelled() => break,
                () = tokio::time::sleep(refresh_interval + Duration::from_millis(jitter)) => {}
            }
        }
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
            .field("check_frequency_seconds", &self.check_frequency_seconds)
            .finish()
    }
}

impl ServiceHealthProvider {
    #[must_use]
    pub fn new(
        providers: Vec<(&'static str, Arc<dyn HealthExt + Sync + Send>)>,
        check_frequency_seconds: u64,
    ) -> Self {
        Self {
            providers,
            check_frequency_seconds,
        }
    }

    pub fn spawn_update_health_checks<T: Send + 'static>(
        &self,
        join_set: &mut JoinSet<Result<(), T>>,
        cancellation_token: &CancellationToken,
    ) -> Vec<(String, AbortHandle)> {
        let mut abort_handles = Vec::with_capacity(self.providers.len());
        for (service_name, provider) in &self.providers {
            let provider = provider.clone();
            let service_name_cloned = (*service_name).to_string();
            let check_frequency_seconds = self.check_frequency_seconds;
            let cancellation_token_cloned = cancellation_token.clone();
            let abort_handle = join_set.spawn(async move {
                provider
                    .update_health_loop(
                        Duration::from_secs(check_frequency_seconds),
                        cancellation_token_cloned,
                    )
                    .await;
                Ok(())
            });
            abort_handles.push((
                format!("Health Check for Service '{service_name_cloned}'"),
                abort_handle,
            ));
            tracing::info!("Spawned Health Check for Service '{service_name}'");
        }
        abort_handles
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
