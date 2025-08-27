use sqlx::PgPool;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;

use crate::{
    api::{management::v1::warehouse::TabularDeleteProfile, ApiContext},
    implementations::postgres::{PostgresCatalog, SecretsState},
    service::{authz::AllowAllAuthorizer, State, UserId},
    tests::TestWarehouseResponse,
};

mod test {
    use std::sync::{Arc, Mutex};

    use serde::{Deserialize, Serialize};
    use sqlx::PgPool;
    use utoipa::ToSchema;
    use uuid::Uuid;

    use crate::{
        api::management::v1::warehouse::{QueueConfig, SetTaskQueueConfigRequest},
        implementations::postgres::PostgresCatalog,
        service::{
            task_queue::{
                EntityId, QueueConfig as QueueConfigTrait, QueueRegistration, TaskData, TaskInput,
                TaskMetadata, TaskQueueRegistry, DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT,
            },
            Catalog, Transaction,
        },
    };

    #[sqlx::test]
    async fn test_task_queue_config_lands_in_task_worker(pool: PgPool) {
        #[derive(Debug, Clone, Deserialize, Serialize, ToSchema)]
        struct Config {
            some_val: String,
        }

        #[derive(Debug, Clone, Deserialize, Serialize, Copy, PartialEq)]
        struct TestTaskData {
            tabular_id: Uuid,
        }
        impl TaskData for TestTaskData {}
        const QUEUE_NAME: &str = "test_queue";
        impl QueueConfigTrait for Config {
            fn queue_name() -> &'static str {
                QUEUE_NAME
            }
        }
        let setup = super::setup_tasks_test(pool).await;
        let ctx = setup.ctx.clone();
        let catalog_state = ctx.v1_state.catalog.clone();
        let task_state = TestTaskData {
            tabular_id: Uuid::now_v7(),
        };

        let (tx, rx) = async_channel::unbounded();
        let ctx_clone = setup.ctx.clone();

        let result = Arc::new(Mutex::new(false));
        let result_clone = result.clone();

        let task_queue_registry = TaskQueueRegistry::new();
        task_queue_registry
            .register_queue::<Config>(QueueRegistration {
                queue_name: QUEUE_NAME,
                worker_fn: Arc::new(move |_| {
                    let ctx = ctx_clone.clone();
                    let rx = rx.clone();
                    let result_clone = result_clone.clone();
                    Box::pin(async move {
                        let task_id = rx.recv().await.unwrap();

                        let task = PostgresCatalog::pick_new_task(
                            QUEUE_NAME,
                            DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT,
                            ctx.v1_state.catalog.clone(),
                        )
                        .await
                        .unwrap()
                        .unwrap();
                        let config = task.queue_config::<Config>().unwrap().unwrap();
                        assert_eq!(config.some_val, "test_value");

                        assert_eq!(task_id, task.task_id);

                        let task = task.task_data::<TestTaskData>().unwrap();
                        assert_eq!(task, task_state);
                        *result_clone.lock().unwrap() = true;
                    })
                }),
                num_workers: 1,
            })
            .await;
        let mut transaction =
            <PostgresCatalog as Catalog>::Transaction::begin_write(catalog_state.clone())
                .await
                .unwrap();
        <PostgresCatalog as Catalog>::set_task_queue_config(
            setup.warehouse.warehouse_id,
            QUEUE_NAME,
            SetTaskQueueConfigRequest {
                queue_config: QueueConfig(
                    serde_json::to_value(Config {
                        some_val: "test_value".to_string(),
                    })
                    .unwrap(),
                ),
                max_seconds_since_last_heartbeat: None,
            },
            transaction.transaction(),
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();

        let mut transaction = <PostgresCatalog as Catalog>::Transaction::begin_write(catalog_state)
            .await
            .unwrap();

        let task_id = PostgresCatalog::enqueue_task(
            QUEUE_NAME,
            TaskInput {
                task_metadata: TaskMetadata {
                    warehouse_id: setup.warehouse.warehouse_id,
                    parent_task_id: None,
                    entity_id: EntityId::Tabular(Uuid::now_v7()),
                    schedule_for: None,
                },
                payload: serde_json::to_value(task_state).unwrap(),
            },
            transaction.transaction(),
        )
        .await
        .unwrap()
        .unwrap();
        transaction.commit().await.unwrap();
        tx.send(task_id).await.unwrap();

        let cancellation_token = crate::CancellationToken::new();
        let task_handle = tokio::task::spawn(
            task_queue_registry
                .task_queues_runner(cancellation_token.clone())
                .await
                .run_queue_workers(false),
        );
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        cancellation_token.cancel();
        task_handle.await.unwrap();
        assert!(
            *result.lock().unwrap(),
            "Task was not processed as expected"
        );
    }
}

struct TasksSetup {
    ctx: ApiContext<State<AllowAllAuthorizer, PostgresCatalog, SecretsState>>,
    warehouse: TestWarehouseResponse,
}

async fn setup_tasks_test(pool: PgPool) -> TasksSetup {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::DEBUG.into())
                .from_env_lossy(),
        )
        .try_init()
        .ok();
    let prof = crate::tests::memory_io_profile();
    let (ctx, warehouse) = crate::tests::setup(
        pool.clone(),
        prof,
        None,
        AllowAllAuthorizer,
        TabularDeleteProfile::Hard {},
        Some(UserId::new_unchecked("oidc", "test-user-id")),
        1,
    )
    .await;

    TasksSetup { ctx, warehouse }
}
