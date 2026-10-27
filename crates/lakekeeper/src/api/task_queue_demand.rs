//! `GET /task-queue/{queue_name}/demand` on the metrics port: how many workers a queue's tasks
//! need, for autoscalers such as KEDA. Counts only, without authentication.
//!
//! Also samples how long the longest waiting due or heartbeat-expired task of each queue and
//! demand class has waited, into
//! [`METRIC_OLDEST_DUE_AGE`].

use std::{
    sync::{Arc, LazyLock},
    time::Duration,
};

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use axum_prometheus::metrics;
use futures::future::BoxFuture;

use crate::service::{
    CatalogBackendError, CatalogStore, CatalogTaskOps,
    maintenance_mode::{MAINTENANCE, MaintenanceSwitch},
    tasks::{
        DemandLookupError, DemandTarget, RegisteredTaskQueues, TaskQueueDemand,
        TaskQueueDemandRequest, TaskQueueName,
    },
};

/// How long a count is served from memory. Bounds the database load whatever the request rate.
const DEMAND_CACHE_TTL: Duration = Duration::from_secs(5);

/// Seconds the longest waiting due or heartbeat-expired task of a queue has waited, by
/// `queue_name` and `class`; 0 if no task is either. `class` is empty for the whole queue.
pub(crate) const METRIC_OLDEST_DUE_AGE: &str = "lakekeeper_task_queue_oldest_due_age_seconds";

/// How often [`METRIC_OLDEST_DUE_AGE`] is sampled.
const OLDEST_DUE_SAMPLE_INTERVAL: Duration = Duration::from_secs(15);

static OLDEST_DUE_AGE_DESCRIBED: LazyLock<()> = LazyLock::new(|| {
    metrics::describe_gauge!(
        METRIC_OLDEST_DUE_AGE,
        metrics::Unit::Seconds,
        "Seconds the longest waiting due or heartbeat-expired task of a task queue has waited, by queue and demand class; 0 if no task is either"
    );
});

type CountFn = Arc<
    dyn Fn(DemandTarget) -> BoxFuture<'static, Result<TaskQueueDemand, CatalogBackendError>>
        + Send
        + Sync,
>;

/// Counts the demand of task queues, for the demand route and the oldest-due gauge.
#[derive(Clone)]
pub(crate) struct QueueDemand {
    queues: RegisteredTaskQueues,
    count: CountFn,
    maintenance: &'static MaintenanceSwitch,
    cache: moka::future::Cache<(&'static TaskQueueName, Option<String>), TaskQueueDemand>,
    /// The clock the age of the oldest due task is measured with.
    now: fn() -> chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, thiserror::Error)]
enum DemandError {
    #[error(transparent)]
    Lookup(#[from] DemandLookupError),
    #[error("Failed to count the tasks")]
    Count(#[source] Arc<CatalogBackendError>),
}

#[derive(Debug, serde::Deserialize)]
struct DemandQuery {
    class: Option<String>,
}

#[derive(Debug, Default, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
struct DemandResponse {
    /// `due + heartbeat-expired + running`.
    demand: i64,
    due: i64,
    heartbeat_expired: i64,
    running: i64,
    /// Seconds the longest waiting due or heartbeat-expired task has waited. `None` if no task
    /// is either.
    oldest_due_seconds: Option<i64>,
}

impl DemandResponse {
    fn new(d: TaskQueueDemand, now: chrono::DateTime<chrono::Utc>) -> Self {
        Self {
            demand: d.demand(),
            due: d.due,
            heartbeat_expired: d.heartbeat_expired,
            running: d.running,
            oldest_due_seconds: d.oldest_due_seconds(now),
        }
    }
}

impl QueueDemand {
    /// Counts with the catalog `C`.
    pub(crate) fn new<C: CatalogStore>(
        catalog_state: C::State,
        queues: RegisteredTaskQueues,
    ) -> Self {
        let count: CountFn = Arc::new(move |target: DemandTarget| {
            let catalog_state = catalog_state.clone();
            Box::pin(async move {
                let request = TaskQueueDemandRequest::builder()
                    .queue_name(target.queue_name)
                    .legacy_queue_names(&target.legacy_queue_names)
                    .default_max_time_since_last_heartbeat(target.max_time_since_last_heartbeat)
                    .resource_demand(target.resource_demand)
                    .build();
                C::task_queue_demand(request, catalog_state).await
            })
        });
        Self::with(queues, count, &MAINTENANCE, chrono::Utc::now)
    }

    fn with(
        queues: RegisteredTaskQueues,
        count: CountFn,
        maintenance: &'static MaintenanceSwitch,
        now: fn() -> chrono::DateTime<chrono::Utc>,
    ) -> Self {
        Self {
            queues,
            count,
            maintenance,
            cache: moka::future::Cache::builder()
                .max_capacity(1000)
                .time_to_live(DEMAND_CACHE_TTL)
                .build(),
            now,
        }
    }

    /// The demand route.
    pub(crate) fn router(self) -> Router {
        Router::new()
            .route("/task-queue/{queue_name}/demand", get(queue_demand))
            .with_state(self)
    }

    /// The demand of `class` of the queue, or of the whole queue without a class.
    async fn demand(
        &self,
        queue_name: &TaskQueueName,
        class: Option<String>,
    ) -> Result<TaskQueueDemand, DemandError> {
        // No worker starts in read-only maintenance mode, so no task can be picked. Checked
        // first: such a process does not register the built-in queues.
        if self.maintenance.is_read_only() {
            return Ok(TaskQueueDemand::default());
        }
        let target = self
            .queues
            .demand_target(queue_name, class.as_deref())
            .await?;
        let key = (target.queue_name, class);
        self.cache
            .try_get_with(key, (self.count)(target))
            .await
            .map_err(DemandError::Count)
    }

    /// Sets [`METRIC_OLDEST_DUE_AGE`] every [`OLDEST_DUE_SAMPLE_INTERVAL`], for each registered
    /// queue as a whole and for each of its demand classes. Runs until dropped.
    pub(crate) async fn sample_oldest_due_age(self) -> std::convert::Infallible {
        let mut ticker = tokio::time::interval(OLDEST_DUE_SAMPLE_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            match self.oldest_due_ages().await {
                Ok(ages) => record_oldest_due_ages(&ages),
                Err(error) => {
                    tracing::warn!(
                        ?error,
                        "Failed to sample the oldest due task of task queues"
                    );
                }
            }
        }
    }

    /// Seconds the longest waiting due or heartbeat-expired task has waited, by queue and class;
    /// 0 if no task is either.
    async fn oldest_due_ages(
        &self,
    ) -> Result<Vec<(&'static TaskQueueName, String, i64)>, DemandError> {
        let mut ages = Vec::new();
        for (queue_name, class) in self.queues.demand_keys().await {
            let demand = self.demand(queue_name, class.clone()).await?;
            let age = demand.oldest_due_seconds((self.now)()).unwrap_or(0);
            ages.push((queue_name, class.unwrap_or_default(), age));
        }
        Ok(ages)
    }
}

fn record_oldest_due_ages(ages: &[(&'static TaskQueueName, String, i64)]) {
    let () = *OLDEST_DUE_AGE_DESCRIBED;
    for (queue_name, class, age) in ages {
        metrics::gauge!(
            METRIC_OLDEST_DUE_AGE,
            "queue_name" => queue_name.as_str().to_string(),
            "class" => class.clone()
        )
        .set(f64::from(i32::try_from(*age).unwrap_or(i32::MAX)));
    }
}

async fn queue_demand(
    State(state): State<QueueDemand>,
    Path(queue_name): Path<String>,
    Query(DemandQuery { class }): Query<DemandQuery>,
) -> Response {
    match state.demand(&TaskQueueName::from(queue_name), class).await {
        Ok(demand) => Json(DemandResponse::new(demand, (state.now)())).into_response(),
        Err(DemandError::Lookup(error)) => {
            (StatusCode::NOT_FOUND, error.to_string()).into_response()
        }
        Err(error @ DemandError::Count(_)) => {
            tracing::warn!(?error, "Failed to count the demand of a task queue");
            (StatusCode::SERVICE_UNAVAILABLE, error.to_string()).into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        LazyLock,
        atomic::{AtomicUsize, Ordering},
    };

    use http_body_util::BodyExt;
    use serde::{Deserialize, Serialize};
    use tower::ServiceExt;

    use super::*;
    use crate::{
        config::MaintenanceMode,
        service::tasks::{
            QueueRegistration, QueueScope, TaskConfig, TaskData, TaskDemandClass,
            TaskQueueRegistry, TaskResourceDemandRange, UserScheduling,
        },
    };

    static QUEUE_NAME: LazyLock<TaskQueueName> = LazyLock::new(|| "demand-endpoint".into());

    #[derive(Clone, Debug, Serialize, Deserialize)]
    #[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
    struct DemandEndpointConfig {}

    impl TaskConfig for DemandEndpointConfig {
        fn queue_name() -> &'static TaskQueueName {
            &QUEUE_NAME
        }

        fn max_time_since_last_heartbeat() -> chrono::Duration {
            chrono::Duration::seconds(60)
        }
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct Payload {}
    impl TaskData for Payload {}

    async fn queues() -> RegisteredTaskQueues {
        let registry = TaskQueueRegistry::new();
        registry
            .register_queue::<DemandEndpointConfig, Payload>(QueueRegistration {
                queue_name: &QUEUE_NAME,
                worker_fn: Arc::new(|_| Box::pin(async {})),
                num_workers: 0,
                scope: QueueScope::Warehouse,
                user_scheduling: UserScheduling::Disabled,
            })
            .await;
        registry
            .register_demand_classes(
                &QUEUE_NAME,
                vec![
                    TaskDemandClass::builder()
                        .name("small")
                        .resource_demand(TaskResourceDemandRange::builder().at_most(10).build())
                        .build(),
                ],
            )
            .await
            .unwrap();
        registry.registered_task_queues()
    }

    /// Answers the upper bound of the counted range as `due` and as the seconds the oldest due
    /// task has waited, and counts the calls.
    fn counting(calls: &Arc<AtomicUsize>) -> CountFn {
        let calls = Arc::clone(calls);
        Arc::new(move |target: DemandTarget| {
            let calls = Arc::clone(&calls);
            Box::pin(async move {
                calls.fetch_add(1, Ordering::SeqCst);
                let due = target.resource_demand.at_most.unwrap_or(1000);
                Ok(TaskQueueDemand {
                    due,
                    heartbeat_expired: 2,
                    running: 3,
                    oldest_due_since: Some(test_now() - chrono::Duration::seconds(due)),
                })
            })
        })
    }

    fn test_now() -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::from_timestamp(1_000_000_000, 0).unwrap()
    }

    fn demand_with(
        queues: RegisteredTaskQueues,
        count: CountFn,
        maintenance: &'static MaintenanceSwitch,
    ) -> QueueDemand {
        QueueDemand::with(queues, count, maintenance, test_now)
    }

    fn router_with(
        queues: RegisteredTaskQueues,
        count: CountFn,
        maintenance: &'static MaintenanceSwitch,
    ) -> Router {
        demand_with(queues, count, maintenance).router()
    }

    async fn get(router: &Router, uri: &str) -> (StatusCode, String) {
        let response = router
            .clone()
            .oneshot(
                http::Request::get(uri)
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        (status, String::from_utf8(body.to_vec()).unwrap())
    }

    static OFF: LazyLock<MaintenanceSwitch> =
        LazyLock::new(|| MaintenanceSwitch::new(MaintenanceMode::Off));
    static READ_ONLY: LazyLock<MaintenanceSwitch> =
        LazyLock::new(|| MaintenanceSwitch::new(MaintenanceMode::ReadOnly));

    #[tokio::test]
    async fn demand_is_counted_per_class_and_cached() {
        let calls = Arc::new(AtomicUsize::new(0));
        let router = router_with(queues().await, counting(&calls), &OFF);

        let small =
            r#"{"demand":15,"due":10,"heartbeat-expired":2,"running":3,"oldest-due-seconds":10}"#;
        let whole = r#"{"demand":1005,"due":1000,"heartbeat-expired":2,"running":3,"oldest-due-seconds":1000}"#;
        for (uri, expected) in [
            ("/task-queue/demand-endpoint/demand?class=small", small),
            ("/task-queue/demand-endpoint/demand", whole),
            ("/task-queue/demand-endpoint/demand?class=small", small),
            (
                "/task-queue/demand-endpoint/demand?class=small&other=1",
                small,
            ),
            ("/task-queue/demand-endpoint/demand", whole),
        ] {
            assert_eq!(
                get(&router, uri).await,
                (StatusCode::OK, expected.to_string()),
                "{uri}"
            );
        }
        // One count for the class, one for the whole queue.
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn unknown_queues_and_classes_are_not_found_without_a_count() {
        let calls = Arc::new(AtomicUsize::new(0));
        let router = router_with(queues().await, counting(&calls), &OFF);
        for (uri, body) in [
            ("/task-queue/other/demand", "Unknown task queue"),
            ("/task-queue/other/demand?class=small", "Unknown task queue"),
            (
                "/task-queue/demand-endpoint/demand?class=large",
                "Unknown demand class of the task queue",
            ),
            (
                "/task-queue/demand-endpoint/demand?class=",
                "Unknown demand class of the task queue",
            ),
        ] {
            assert_eq!(
                get(&router, uri).await,
                (StatusCode::NOT_FOUND, body.to_string()),
                "{uri}"
            );
        }
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn read_only_maintenance_reports_no_demand() {
        let calls = Arc::new(AtomicUsize::new(0));
        let router = router_with(queues().await, counting(&calls), &READ_ONLY);
        // Also for queues and classes this process did not register.
        for uri in [
            "/task-queue/demand-endpoint/demand?class=small",
            "/task-queue/demand-endpoint/demand?class=large",
            "/task-queue/other/demand",
        ] {
            assert_eq!(
                get(&router, uri).await,
                (
                    StatusCode::OK,
                    r#"{"demand":0,"due":0,"heartbeat-expired":0,"running":0,"oldest-due-seconds":null}"#
                        .to_string()
                ),
                "{uri}"
            );
        }
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn a_failed_count_is_unavailable_and_not_cached() {
        let calls = Arc::new(AtomicUsize::new(0));
        let failing: CountFn = {
            let calls = Arc::clone(&calls);
            Arc::new(move |_| {
                let calls = Arc::clone(&calls);
                Box::pin(async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Err(CatalogBackendError::new_unexpected(std::io::Error::other(
                        "database down",
                    )))
                })
            })
        };
        let router = router_with(queues().await, failing, &OFF);
        for _ in 0..2 {
            assert_eq!(
                get(&router, "/task-queue/demand-endpoint/demand").await,
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "Failed to count the tasks".to_string()
                )
            );
        }
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    /// The gauge lines of `ages`, as the metrics port renders them.
    fn rendered_oldest_due_ages(ages: &[(&'static TaskQueueName, String, i64)]) -> Vec<String> {
        let recorder =
            axum_prometheus::metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        metrics::with_local_recorder(&recorder, || record_oldest_due_ages(ages));
        let mut lines: Vec<_> = recorder
            .handle()
            .render()
            .lines()
            .filter(|line| line.starts_with(METRIC_OLDEST_DUE_AGE))
            .map(str::to_string)
            .collect();
        lines.sort_unstable();
        lines
    }

    #[tokio::test]
    async fn the_oldest_due_age_is_sampled_per_queue_and_class() {
        let calls = Arc::new(AtomicUsize::new(0));
        let demand = demand_with(queues().await, counting(&calls), &OFF);
        let ages = demand.oldest_due_ages().await.unwrap();
        assert_eq!(
            rendered_oldest_due_ages(&ages),
            vec![
                r#"lakekeeper_task_queue_oldest_due_age_seconds{queue_name="demand-endpoint",class=""} 1000"#,
                r#"lakekeeper_task_queue_oldest_due_age_seconds{queue_name="demand-endpoint",class="small"} 10"#,
            ]
        );
        // The demand route answers from the counts of the sample.
        let router = demand.router();
        assert_eq!(
            get(&router, "/task-queue/demand-endpoint/demand?class=small").await,
            (
                StatusCode::OK,
                r#"{"demand":15,"due":10,"heartbeat-expired":2,"running":3,"oldest-due-seconds":10}"#
                    .to_string()
            )
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn without_a_due_task_the_oldest_due_age_is_zero() {
        let none_due: CountFn = Arc::new(|_| Box::pin(async { Ok(TaskQueueDemand::default()) }));
        let calls = Arc::new(AtomicUsize::new(0));
        for demand in [
            demand_with(queues().await, none_due, &OFF),
            demand_with(queues().await, counting(&calls), &READ_ONLY),
        ] {
            let ages = demand.oldest_due_ages().await.unwrap();
            assert_eq!(
                rendered_oldest_due_ages(&ages),
                vec![
                    r#"lakekeeper_task_queue_oldest_due_age_seconds{queue_name="demand-endpoint",class=""} 0"#,
                    r#"lakekeeper_task_queue_oldest_due_age_seconds{queue_name="demand-endpoint",class="small"} 0"#,
                ]
            );
        }
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}
