//! `GET /task-queue/{queue_name}/demand` on the metrics port: how many workers a queue's tasks
//! need, for autoscalers such as KEDA. Counts only, without authentication.
//!
//! [`OldestDueSampler`] reports how long tasks wait, in [`METRIC_OLDEST_DUE_AGE`].

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

use crate::{
    CancellationToken,
    service::{
        CatalogBackendError, CatalogStore, CatalogTaskOps,
        maintenance_mode::{MAINTENANCE, MaintenanceSwitch},
        tasks::{
            DemandLookupError, DemandTarget, RegisteredTaskQueues, TaskQueueDemand,
            TaskQueueDemandRequest, TaskQueueName,
        },
    },
};

/// How long a count is served from memory. Bounds the database load whatever the request rate.
const DEMAND_CACHE_TTL: Duration = Duration::from_secs(5);

/// Seconds the longest waiting due or heartbeat-expired task of a queue has waited, by
/// `queue_name` and `class`; 0 if no task is either, `NaN` if the last sample failed. `class` is
/// empty for the whole queue.
pub(crate) const METRIC_OLDEST_DUE_AGE: &str = "lakekeeper_task_queue_oldest_due_age_seconds";
/// Unix time of the last sample of [`METRIC_OLDEST_DUE_AGE`] in which every queue and class
/// succeeded.
pub(crate) const METRIC_OLDEST_DUE_LAST_SUCCESS: &str =
    "lakekeeper_task_queue_oldest_due_last_success_timestamp_seconds";

/// How often [`METRIC_OLDEST_DUE_AGE`] is sampled, and how long one queue and class may take.
const OLDEST_DUE_SAMPLE_INTERVAL: Duration = Duration::from_secs(15);

/// Registers the descriptions of the oldest-due metrics exactly once.
static METRICS_INITIALIZED: LazyLock<()> = LazyLock::new(|| {
    metrics::describe_gauge!(
        METRIC_OLDEST_DUE_AGE,
        "Seconds the longest waiting due or heartbeat-expired task of a task queue has waited, by queue and class; 0 if no task waits, NaN if the last sample failed"
    );
    metrics::describe_gauge!(
        METRIC_OLDEST_DUE_LAST_SUCCESS,
        "Unix time of the last sample of lakekeeper_task_queue_oldest_due_age_seconds in which every queue and class succeeded"
    );
});

type CountFn = Arc<
    dyn Fn(DemandTarget) -> BoxFuture<'static, Result<TaskQueueDemand, CatalogBackendError>>
        + Send
        + Sync,
>;

type OldestDueFn = Arc<
    dyn Fn(
            DemandTarget,
        )
            -> BoxFuture<'static, Result<Option<chrono::DateTime<chrono::Utc>>, CatalogBackendError>>
        + Send
        + Sync,
>;

#[derive(Clone)]
struct DemandState {
    queues: RegisteredTaskQueues,
    count: CountFn,
    maintenance: &'static MaintenanceSwitch,
    cache: moka::future::Cache<(&'static TaskQueueName, Option<String>), TaskQueueDemand>,
}

#[derive(Debug, serde::Deserialize)]
struct DemandQuery {
    class: Option<String>,
}

#[derive(Debug, serde::Serialize)]
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

/// The request of a demand target.
fn demand_request(target: &DemandTarget) -> TaskQueueDemandRequest<'_> {
    TaskQueueDemandRequest::builder()
        .queue_name(target.queue_name)
        .legacy_queue_names(&target.legacy_queue_names)
        .default_max_time_since_last_heartbeat(target.max_time_since_last_heartbeat)
        .resource_demand(target.resource_demand)
        .build()
}

/// The demand route, counting with the catalog `C`.
pub(crate) fn router<C: CatalogStore>(
    catalog_state: C::State,
    queues: RegisteredTaskQueues,
) -> Router {
    let count: CountFn = Arc::new(move |target: DemandTarget| {
        let catalog_state = catalog_state.clone();
        Box::pin(async move { C::task_queue_demand(demand_request(&target), catalog_state).await })
    });
    router_with(queues, count, &MAINTENANCE)
}

fn router_with(
    queues: RegisteredTaskQueues,
    count: CountFn,
    maintenance: &'static MaintenanceSwitch,
) -> Router {
    let state = DemandState {
        queues,
        count,
        maintenance,
        cache: moka::future::Cache::builder()
            .max_capacity(1000)
            .time_to_live(DEMAND_CACHE_TTL)
            .build(),
    };
    Router::new()
        .route("/task-queue/{queue_name}/demand", get(queue_demand))
        .with_state(state)
}

async fn queue_demand(
    State(state): State<DemandState>,
    Path(queue_name): Path<String>,
    Query(DemandQuery { class }): Query<DemandQuery>,
) -> Response {
    // No worker starts in read-only maintenance mode, so no task can be picked. Checked first:
    // such a process does not register the built-in queues.
    if state.maintenance.is_read_only() {
        let none = DemandResponse::new(TaskQueueDemand::default(), chrono::Utc::now());
        return Json(none).into_response();
    }
    let target = match state
        .queues
        .demand_target(&TaskQueueName::from(queue_name), class.as_deref())
        .await
    {
        Ok(target) => target,
        Err(error) => return (StatusCode::NOT_FOUND, error.to_string()).into_response(),
    };
    let key = (target.queue_name, class);
    match state.cache.try_get_with(key, (state.count)(target)).await {
        Ok(demand) => Json(DemandResponse::new(demand, chrono::Utc::now())).into_response(),
        Err(error) => {
            tracing::warn!(?error, "Failed to count the demand of a task queue");
            (StatusCode::SERVICE_UNAVAILABLE, "Failed to count the tasks").into_response()
        }
    }
}

/// Sets [`METRIC_OLDEST_DUE_AGE`] for each registered queue as a whole and for each of its
/// demand classes, with a statement that does not count the tasks.
pub(crate) struct OldestDueSampler {
    queues: RegisteredTaskQueues,
    oldest_due: OldestDueFn,
    maintenance: &'static MaintenanceSwitch,
}

/// One sample of all queues and classes.
#[derive(Debug, PartialEq)]
struct OldestDueSample {
    /// Seconds, by queue and class. `NaN` where the sample failed.
    ages: Vec<(&'static TaskQueueName, String, f64)>,
    /// When the sample was taken, if every queue and class succeeded.
    succeeded_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, thiserror::Error)]
enum SampleError {
    #[error(transparent)]
    Lookup(#[from] DemandLookupError),
    #[error(transparent)]
    Query(#[from] CatalogBackendError),
    #[error("No answer within {OLDEST_DUE_SAMPLE_INTERVAL:?}")]
    Timeout,
}

impl OldestDueSampler {
    /// Samples with the catalog `C`.
    pub(crate) fn new<C: CatalogStore>(
        catalog_state: C::State,
        queues: RegisteredTaskQueues,
    ) -> Self {
        let oldest_due: OldestDueFn = Arc::new(move |target: DemandTarget| {
            let catalog_state = catalog_state.clone();
            Box::pin(async move {
                C::task_queue_oldest_due(demand_request(&target), catalog_state).await
            })
        });
        Self {
            queues,
            oldest_due,
            maintenance: &MAINTENANCE,
        }
    }

    /// Samples every [`OLDEST_DUE_SAMPLE_INTERVAL`] until cancelled.
    pub(crate) async fn run(self, cancellation_token: CancellationToken) {
        let mut ticker = tokio::time::interval(OLDEST_DUE_SAMPLE_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            let sample = async {
                ticker.tick().await;
                self.sample(chrono::Utc::now()).await
            };
            tokio::select! {
                () = cancellation_token.cancelled() => break,
                sample = sample => record_oldest_due_sample(&sample),
            }
        }
        tracing::info!("Oldest due task sampler cancelled");
    }

    /// How long the longest waiting due or heartbeat-expired task of each queue and class has
    /// waited at `now`.
    async fn sample(&self, now: chrono::DateTime<chrono::Utc>) -> OldestDueSample {
        let mut ages = Vec::new();
        let mut succeeded = true;
        for (queue_name, class) in self.queues.demand_keys().await {
            let oldest_due_since = tokio::time::timeout(
                OLDEST_DUE_SAMPLE_INTERVAL,
                self.oldest_due_since(queue_name, class.as_deref()),
            )
            .await
            .unwrap_or(Err(SampleError::Timeout));
            let age = match oldest_due_since {
                Ok(since) => since.map_or(0.0, |since| (now - since).as_seconds_f64().max(0.0)),
                Err(error) => {
                    tracing::warn!(
                        ?error,
                        %queue_name,
                        ?class,
                        "Failed to sample the oldest due task of a task queue"
                    );
                    succeeded = false;
                    f64::NAN
                }
            };
            ages.push((queue_name, class.unwrap_or_default(), age));
        }
        OldestDueSample {
            ages,
            succeeded_at: succeeded.then_some(now),
        }
    }

    async fn oldest_due_since(
        &self,
        queue_name: &TaskQueueName,
        class: Option<&str>,
    ) -> Result<Option<chrono::DateTime<chrono::Utc>>, SampleError> {
        // No worker starts in read-only maintenance mode, so no task can be picked.
        if self.maintenance.is_read_only() {
            return Ok(None);
        }
        let target = self.queues.demand_target(queue_name, class).await?;
        Ok((self.oldest_due)(target).await?)
    }
}

fn record_oldest_due_sample(sample: &OldestDueSample) {
    let () = &*METRICS_INITIALIZED;
    for (queue_name, class, age) in &sample.ages {
        metrics::gauge!(
            METRIC_OLDEST_DUE_AGE,
            "queue_name" => queue_name.as_str(),
            "class" => class.clone()
        )
        .set(*age);
    }
    if let Some(succeeded_at) = sample.succeeded_at {
        metrics::gauge!(METRIC_OLDEST_DUE_LAST_SUCCESS)
            .set((succeeded_at - chrono::DateTime::UNIX_EPOCH).as_seconds_f64());
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        LazyLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
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
                    // The answer is less than a second later.
                    oldest_due_since: Some(chrono::Utc::now() - chrono::Duration::seconds(due)),
                })
            })
        })
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

    fn sample_time() -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::from_timestamp(1_000_000_000, 0).unwrap()
    }

    /// Answers that the oldest due task waits since the upper bound of the range in seconds
    /// before [`sample_time`]. The whole queue fails while `whole_fails` is set, and the class
    /// never answers while `class_hangs` is set.
    fn oldest_due(whole_fails: &Arc<AtomicBool>, class_hangs: &Arc<AtomicBool>) -> OldestDueFn {
        let whole_fails = Arc::clone(whole_fails);
        let class_hangs = Arc::clone(class_hangs);
        Arc::new(move |target: DemandTarget| {
            let at_most = target.resource_demand.at_most;
            let fails = at_most.is_none() && whole_fails.load(Ordering::SeqCst);
            let hangs = at_most.is_some() && class_hangs.load(Ordering::SeqCst);
            Box::pin(async move {
                if hangs {
                    futures::future::pending::<()>().await;
                }
                if fails {
                    return Err(CatalogBackendError::new_unexpected(std::io::Error::other(
                        "database down",
                    )));
                }
                let seconds = at_most.unwrap_or(1000);
                Ok(Some(sample_time() - chrono::Duration::seconds(seconds)))
            })
        })
    }

    async fn sampler(
        oldest_due: OldestDueFn,
        maintenance: &'static MaintenanceSwitch,
    ) -> OldestDueSampler {
        OldestDueSampler {
            queues: queues().await,
            oldest_due,
            maintenance,
        }
    }

    /// The oldest-due lines of the metrics port after recording `samples` in order.
    fn rendered(samples: &[OldestDueSample]) -> Vec<String> {
        let recorder =
            axum_prometheus::metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        metrics::with_local_recorder(&recorder, || {
            for sample in samples {
                record_oldest_due_sample(sample);
            }
        });
        let mut lines: Vec<_> = recorder
            .handle()
            .render()
            .lines()
            .filter(|line| line.starts_with("lakekeeper_task_queue_oldest_due"))
            .map(str::to_string)
            .collect();
        lines.sort_unstable();
        lines
    }

    const WHOLE: &str =
        r#"lakekeeper_task_queue_oldest_due_age_seconds{queue_name="demand-endpoint",class=""}"#;
    const SMALL: &str = r#"lakekeeper_task_queue_oldest_due_age_seconds{queue_name="demand-endpoint",class="small"}"#;
    const LAST_SUCCESS: &str = "lakekeeper_task_queue_oldest_due_last_success_timestamp_seconds";

    #[tokio::test]
    async fn the_oldest_due_age_is_sampled_per_queue_and_class() {
        let ok = Arc::new(AtomicBool::new(false));
        let sampler = sampler(oldest_due(&ok, &ok), &OFF).await;
        let later = sample_time() + chrono::Duration::milliseconds(500);
        assert_eq!(
            rendered(&[sampler.sample(later).await]),
            vec![
                format!("{WHOLE} 1000.5"),
                format!("{SMALL} 10.5"),
                format!("{LAST_SUCCESS} 1000000000.5"),
            ]
        );
    }

    #[tokio::test]
    async fn without_a_due_task_the_oldest_due_age_is_zero() {
        let ok = Arc::new(AtomicBool::new(false));
        let none_due: OldestDueFn = Arc::new(|_| Box::pin(async { Ok(None) }));
        for sampler in [
            sampler(none_due, &OFF).await,
            sampler(oldest_due(&ok, &ok), &READ_ONLY).await,
        ] {
            assert_eq!(
                rendered(&[sampler.sample(sample_time()).await]),
                vec![
                    format!("{WHOLE} 0"),
                    format!("{SMALL} 0"),
                    format!("{LAST_SUCCESS} 1000000000"),
                ]
            );
        }
    }

    /// A failed or hanging sample replaces the earlier value with `NaN`, and the time of the last
    /// success stays.
    #[tokio::test(start_paused = true)]
    async fn a_failed_sample_is_nan_and_keeps_the_last_success() {
        let whole_fails = Arc::new(AtomicBool::new(false));
        let class_hangs = Arc::new(AtomicBool::new(false));
        let sampler = sampler(oldest_due(&whole_fails, &class_hangs), &OFF).await;
        let first = sampler.sample(sample_time()).await;
        let later = sample_time() + chrono::Duration::seconds(15);

        whole_fails.store(true, Ordering::SeqCst);
        let whole_failed = sampler.sample(later).await;
        assert_eq!(
            rendered(&[first, whole_failed]),
            vec![
                format!("{WHOLE} NaN"),
                format!("{SMALL} 25"),
                format!("{LAST_SUCCESS} 1000000000"),
            ]
        );

        whole_fails.store(false, Ordering::SeqCst);
        let first = sampler.sample(sample_time()).await;
        class_hangs.store(true, Ordering::SeqCst);
        let started = tokio::time::Instant::now();
        let class_hung = sampler.sample(later).await;
        assert_eq!(started.elapsed(), OLDEST_DUE_SAMPLE_INTERVAL);
        assert_eq!(
            rendered(&[first, class_hung]),
            vec![
                format!("{WHOLE} 1015"),
                format!("{SMALL} NaN"),
                format!("{LAST_SUCCESS} 1000000000"),
            ]
        );
    }
}
