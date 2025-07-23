use crate::api::Result;
use std::time::Duration;

/// Configuration for statistics retention policies
#[derive(Debug, Clone)]
pub struct StatisticsRetentionConfig {
    /// Maximum number of endpoint statistics entries to retain per unique combination
    pub endpoint_max_entries: u32,
    /// Maximum age for endpoint statistics entries
    pub endpoint_max_age: Duration,
    /// Maximum number of warehouse statistics history entries to retain per warehouse
    pub warehouse_max_entries: u32,
    /// Maximum age for warehouse statistics history entries
    pub warehouse_max_age: Duration,
}

/// Trait for implementing statistics retention cleanup
#[async_trait::async_trait]
pub trait StatisticsRetentionService: Send + Sync {
    /// Clean up old endpoint statistics based on retention policy
    async fn cleanup_endpoint_statistics(&self, config: &StatisticsRetentionConfig) -> Result<u64>;

    /// Clean up old warehouse statistics based on retention policy
    async fn cleanup_warehouse_statistics(&self, config: &StatisticsRetentionConfig) -> Result<u64>;

    /// Run full retention cleanup for both endpoint and warehouse statistics
    async fn cleanup_all_statistics(&self, config: &StatisticsRetentionConfig) -> Result<(u64, u64)> {
        let endpoint_cleaned = self.cleanup_endpoint_statistics(config).await?;
        let warehouse_cleaned = self.cleanup_warehouse_statistics(config).await?;
        Ok((endpoint_cleaned, warehouse_cleaned))
    }
}

/// Task for running periodic statistics retention cleanup
pub struct StatisticsRetentionTask<S: StatisticsRetentionService> {
    service: S,
    config: StatisticsRetentionConfig,
    cleanup_interval: Duration,
}

impl<S: StatisticsRetentionService> StatisticsRetentionTask<S> {
    /// Create a new statistics retention task
    pub fn new(service: S, config: StatisticsRetentionConfig, cleanup_interval: Duration) -> Self {
        Self {
            service,
            config,
            cleanup_interval,
        }
    }

    /// Run the retention cleanup task in a loop
    pub async fn run(&self) {
        let mut interval = tokio::time::interval(self.cleanup_interval);
        loop {
            interval.tick().await;
            
            match self.service.cleanup_all_statistics(&self.config).await {
                Ok((endpoint_cleaned, warehouse_cleaned)) => {
                    tracing::info!(
                        endpoint_cleaned, 
                        warehouse_cleaned,
                        "Statistics retention cleanup completed"
                    );
                }
                Err(e) => {
                    tracing::error!(
                        error = ?e,
                        "Statistics retention cleanup failed"
                    );
                }
            }
        }
    }
} 