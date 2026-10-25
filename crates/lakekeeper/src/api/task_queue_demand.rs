//! `GET /task-queue/{queue_name}/demand` on the metrics port: how many workers a queue's tasks
//! need, for autoscalers such as KEDA. Counts only, without authentication.

use std::{sync::Arc, time::Duration};

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use futures::future::BoxFuture;

use crate::service::{
    CatalogStore, CatalogTaskOps,
    maintenance_mode::{MAINTENANCE, MaintenanceSwitch},
    tasks::{
        DemandTarget, RegisteredTaskQueues, TaskQueueDemand, TaskQueueDemandRequest, TaskQueueName,
    },
};

/// How long a count is served from memory. Bounds the database load whatever the request rate.
const DEMAND_CACHE_TTL: Duration = Duration::from_secs(5);

type CountFn = Arc<
    dyn Fn(DemandTarget) -> BoxFuture<'static, crate::api::Result<TaskQueueDemand>> + Send + Sync,
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

#[derive(Debug, Default, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
struct DemandResponse {
    /// `due + heartbeat-expired + running`.
    demand: i64,
    due: i64,
    heartbeat_expired: i64,
    running: i64,
}

impl From<TaskQueueDemand> for DemandResponse {
    fn from(d: TaskQueueDemand) -> Self {
        Self {
            demand: d.demand(),
            due: d.due,
            heartbeat_expired: d.heartbeat_expired,
            running: d.running,
        }
    }
}

/// The demand route, counting with the catalog `C`.
pub(crate) fn router<C: CatalogStore>(
    catalog_state: C::State,
    queues: RegisteredTaskQueues,
) -> Router {
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
        return Json(DemandResponse::default()).into_response();
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
        Ok(demand) => Json(DemandResponse::from(demand)).into_response(),
        Err(error) => {
            tracing::warn!(?error, "Failed to count the demand of a task queue");
            (StatusCode::SERVICE_UNAVAILABLE, "Failed to count the tasks").into_response()
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

    /// Answers the upper bound of the counted range as `due`, and counts the calls.
    fn counting(calls: &Arc<AtomicUsize>) -> CountFn {
        let calls = Arc::clone(calls);
        Arc::new(move |target: DemandTarget| {
            let calls = Arc::clone(&calls);
            Box::pin(async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(TaskQueueDemand {
                    due: target.resource_demand.at_most.unwrap_or(1000),
                    heartbeat_expired: 2,
                    running: 3,
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

        let small = r#"{"demand":15,"due":10,"heartbeat-expired":2,"running":3}"#;
        let whole = r#"{"demand":1005,"due":1000,"heartbeat-expired":2,"running":3}"#;
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
                    r#"{"demand":0,"due":0,"heartbeat-expired":0,"running":0}"#.to_string()
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
                calls.fetch_add(1, Ordering::SeqCst);
                Box::pin(async {
                    Err(iceberg_ext::catalog::rest::ErrorModel::internal(
                        "database down",
                        "DatabaseError",
                        None,
                    )
                    .into())
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
}
