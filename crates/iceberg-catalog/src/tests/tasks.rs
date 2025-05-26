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
    use std::sync::Arc;

    use serde::{Deserialize, Serialize};
    use sqlx::PgPool;
    use utoipa::ToSchema;
    use uuid::Uuid;

    use crate::{
        api::management::v1::warehouse::{QueueConfig, SetTaskQueueConfigRequest},
        implementations::postgres::PostgresCatalog,
        service::{
            task_queue::{
                EntityId, QueueConfig as QueueConfigTrait, TaskInput, TaskMetadata, TaskQueues,
                DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT,
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
        struct TaskState {
            tabular_id: Uuid,
        }
        impl QueueConfigTrait for Config {}
        let setup = super::setup_tasks_test(pool).await;
        let ctx = setup.ctx.clone();
        let catalog_state = ctx.v1_state.catalog.clone();
        let task_state = TaskState {
            tabular_id: Uuid::now_v7(),
        };

        let (tx, rx) = async_channel::unbounded();
        let ctx_clone = setup.ctx.clone();
        let queue_name = "test_queue";

        let mut queues = TaskQueues::new();
        queues.register_queue::<Config>(
            queue_name,
            Arc::new(move || {
                let ctx = ctx_clone.clone();
                let rx = rx.clone();
                Box::pin(async move {
                    let task_id = rx.recv().await.unwrap();

                    let task = PostgresCatalog::pick_new_task(
                        queue_name,
                        DEFAULT_MAX_TIME_SINCE_LAST_HEARTBEAT,
                        ctx.v1_state.catalog.clone(),
                    )
                    .await
                    .unwrap()
                    .unwrap();
                    let config = task.task_config::<Config>().unwrap().unwrap();
                    assert_eq!(config.some_val, "test_value");

                    assert_eq!(task_id, task.task_id);

                    let task = task.task_state::<TaskState>().unwrap();
                    assert_eq!(task, task_state);
                })
            }),
            1,
        );
        let mut transaction =
            <PostgresCatalog as Catalog>::Transaction::begin_write(catalog_state.clone())
                .await
                .unwrap();
        <PostgresCatalog as Catalog>::set_task_queue_config(
            setup.warehouse.warehouse_id,
            queue_name,
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
            queue_name,
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

        let task_handle = tokio::task::spawn(queues.spawn_queues(true));
        task_handle.await.unwrap().unwrap();
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
    let prof = crate::tests::test_io_profile();
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
