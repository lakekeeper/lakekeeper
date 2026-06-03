// Extracted from crates/lakekeeper/src/api/management/v1/tasks.rs (schedule_lifecycle mod).
// VAK-437 split.

use std::sync::{Arc, LazyLock};

use iceberg::spec::{Schema, UnboundPartitionSpec};
use iceberg_ext::catalog::rest::CreateTableRequest;
use lakekeeper::{
    api::{
        RequestMetadata,
        iceberg::{
            types::Prefix,
            v1::{DataAccess, NamespaceParameters, tables::TablesService as _},
        },
        management::v1::{
            ApiServer,
            task_queue::{ScheduleTaskRequest, ScheduleTaskResponse},
            tasks::{ControlTaskAction, ControlTasksRequest, Service as _},
            warehouse::TabularDeleteProfile,
        },
    },
    server::CatalogServer,
    service::{
        TableId,
        authz::AllowAllAuthorizer,
        tasks::{
            QueueRegistration, QueueScope, TaskConfig, TaskData, TaskQueueName, UserScheduling,
            WarehouseTaskEntityId,
        },
    },
};
use lakekeeper_integration_tests::{create_ns, memory_io_profile, setup_with_registry};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

static TEST_QUEUE_NAME: LazyLock<TaskQueueName> =
    LazyLock::new(|| "test_schedulable_lifecycle".into());
static REJECTING_QUEUE_NAME: LazyLock<TaskQueueName> =
    LazyLock::new(|| "test_schedulable_rejecting".into());

/// Marker property the rejecting queue's eligibility check looks at.
/// When set to `"reject"` on a table the queue refuses to schedule.
const REJECTION_MARKER_PROPERTY: &str = "schedule-test.reject-me";

/// Empty payload shared by both test queues.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct TestSchedulablePayload {}
impl TaskData for TestSchedulablePayload {}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(lakekeeper::utoipa::ToSchema))]
struct TestSchedulableConfig {}

impl TaskConfig for TestSchedulableConfig {
    fn queue_name() -> &'static TaskQueueName {
        &TEST_QUEUE_NAME
    }
    fn max_time_since_last_heartbeat() -> chrono::Duration {
        chrono::Duration::seconds(60)
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(lakekeeper::utoipa::ToSchema))]
struct RejectingSchedulableConfig {}

impl TaskConfig for RejectingSchedulableConfig {
    fn queue_name() -> &'static TaskQueueName {
        &REJECTING_QUEUE_NAME
    }
    fn max_time_since_last_heartbeat() -> chrono::Duration {
        chrono::Duration::seconds(60)
    }
    fn check_schedule_eligibility(
        _config: &Self,
        entity_properties: &std::collections::HashMap<String, String>,
        _entity: WarehouseTaskEntityId,
    ) -> Result<(), iceberg_ext::catalog::rest::ErrorModel> {
        if entity_properties
            .get(REJECTION_MARKER_PROPERTY)
            .map(String::as_str)
            == Some("reject")
        {
            return Err(iceberg_ext::catalog::rest::ErrorModel::bad_request(
                format!("rejected by test eligibility fn: {REJECTION_MARKER_PROPERTY}=reject"),
                "RejectedByTestEligibility",
                None,
            ));
        }
        Ok(())
    }
}

fn build_schema() -> Schema {
    use iceberg::spec::{NestedField, PrimitiveType};
    Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", iceberg::spec::Type::Primitive(PrimitiveType::Int))
                .into(),
        ])
        .build()
        .unwrap()
}

#[sqlx::test]
async fn schedule_then_409_then_runnow(pool: PgPool) {
    let (ctx, warehouse, registry) = setup_with_registry(
        pool,
        memory_io_profile(),
        None,
        AllowAllAuthorizer::default(),
        TabularDeleteProfile::Hard {},
        None,
        1,
        None,
    )
    .await;

    registry
        .register_queue::<TestSchedulableConfig, TestSchedulablePayload>(QueueRegistration {
            queue_name: &TEST_QUEUE_NAME,
            worker_fn: Arc::new(|_| Box::pin(async {})),
            num_workers: 0,
            scope: QueueScope::Warehouse,
            #[cfg(feature = "open-api")]
            user_scheduling: UserScheduling::Enabled {
                payload_schema: None,
            },
            #[cfg(not(feature = "open-api"))]
            user_scheduling: UserScheduling::Enabled,
        })
        .await;

    let warehouse_id = warehouse.warehouse_id;
    let ns = create_ns(ctx.clone(), warehouse_id.to_string(), "ns1".to_string()).await;
    let ns_params = NamespaceParameters {
        prefix: Some(Prefix(warehouse_id.to_string())),
        namespace: ns.namespace.clone(),
    };
    let table = CatalogServer::create_table(
        ns_params,
        CreateTableRequest {
            name: "tab-1".to_string(),
            location: None,
            schema: build_schema(),
            partition_spec: Some(UnboundPartitionSpec::builder().build()),
            write_order: None,
            stage_create: Some(false),
            properties: None,
        },
        DataAccess {
            vended_credentials: false,
            remote_signing: false,
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();
    let table_id = table.metadata.uuid();

    let resp: ScheduleTaskResponse = ApiServer::schedule_task(
        warehouse_id,
        &TEST_QUEUE_NAME,
        ScheduleTaskRequest {
            entity: WarehouseTaskEntityId::Table {
                table_id: table_id.into(),
            },
            scheduled_for: None,
            payload: None,
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .expect("first schedule call should succeed");
    let first_task_id = resp.task_id;

    let err = ApiServer::schedule_task(
        warehouse_id,
        &TEST_QUEUE_NAME,
        ScheduleTaskRequest {
            entity: WarehouseTaskEntityId::Table {
                table_id: table_id.into(),
            },
            scheduled_for: None,
            payload: None,
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .expect_err("second schedule call must hit the unique index");
    assert_eq!(err.error.code, 409, "expected 409 Conflict, got {err:?}");
    assert_eq!(err.error.r#type, "TaskAlreadyActive");
    let id_str = first_task_id.to_string();
    assert!(
        err.error.message.contains(&id_str),
        "409 body must include the existing task-id ({id_str}); got: {}",
        err.error.message
    );

    ApiServer::control_tasks(
        warehouse_id,
        ControlTasksRequest {
            action: ControlTaskAction::RunNow,
            task_ids: vec![first_task_id],
        },
        ctx,
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .expect("control_tasks RunNow on the existing task should succeed");
}

#[sqlx::test]
async fn schedule_eligibility_rejection_surfaces_as_400(pool: PgPool) {
    let (ctx, warehouse, registry) = setup_with_registry(
        pool,
        memory_io_profile(),
        None,
        AllowAllAuthorizer::default(),
        TabularDeleteProfile::Hard {},
        None,
        1,
        None,
    )
    .await;

    registry
        .register_queue::<RejectingSchedulableConfig, TestSchedulablePayload>(QueueRegistration {
            queue_name: &REJECTING_QUEUE_NAME,
            worker_fn: Arc::new(|_| Box::pin(async {})),
            num_workers: 0,
            scope: QueueScope::Warehouse,
            #[cfg(feature = "open-api")]
            user_scheduling: UserScheduling::Enabled {
                payload_schema: None,
            },
            #[cfg(not(feature = "open-api"))]
            user_scheduling: UserScheduling::Enabled,
        })
        .await;

    let warehouse_id = warehouse.warehouse_id;
    let ns = create_ns(ctx.clone(), warehouse_id.to_string(), "ns1".to_string()).await;
    let ns_params = NamespaceParameters {
        prefix: Some(Prefix(warehouse_id.to_string())),
        namespace: ns.namespace.clone(),
    };
    let table = CatalogServer::create_table(
        ns_params,
        CreateTableRequest {
            name: "tab-reject".to_string(),
            location: None,
            schema: build_schema(),
            partition_spec: Some(UnboundPartitionSpec::builder().build()),
            write_order: None,
            stage_create: Some(false),
            properties: Some(std::collections::HashMap::from([(
                REJECTION_MARKER_PROPERTY.to_string(),
                "reject".to_string(),
            )])),
        },
        DataAccess {
            vended_credentials: false,
            remote_signing: false,
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .expect("create_table should succeed");
    let table_id = table.metadata.uuid();

    let err = ApiServer::schedule_task(
        warehouse_id,
        &REJECTING_QUEUE_NAME,
        ScheduleTaskRequest {
            entity: WarehouseTaskEntityId::Table {
                table_id: table_id.into(),
            },
            scheduled_for: None,
            payload: None,
        },
        ctx,
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .expect_err("eligibility rejection must surface as an error from the endpoint");

    assert_eq!(err.error.code, 400, "expected 400, got {err:?}");
    assert_eq!(
        err.error.r#type, "RejectedByTestEligibility",
        "endpoint must surface the queue's error code verbatim, got {err:?}"
    );
    assert!(
        err.error.message.contains(REJECTION_MARKER_PROPERTY),
        "endpoint must surface the queue's error message verbatim; got: {}",
        err.error.message
    );
}

#[sqlx::test]
async fn schedule_unknown_queue_returns_404(pool: PgPool) {
    let (ctx, warehouse, _registry) = setup_with_registry(
        pool,
        memory_io_profile(),
        None,
        AllowAllAuthorizer::default(),
        TabularDeleteProfile::Hard {},
        None,
        1,
        None,
    )
    .await;
    let warehouse_id = warehouse.warehouse_id;
    let ns = create_ns(ctx.clone(), warehouse_id.to_string(), "ns1".to_string()).await;
    let table = CatalogServer::create_table(
        NamespaceParameters {
            prefix: Some(Prefix(warehouse_id.to_string())),
            namespace: ns.namespace.clone(),
        },
        CreateTableRequest {
            name: "t-unknown-queue".to_string(),
            location: None,
            schema: build_schema(),
            partition_spec: Some(UnboundPartitionSpec::builder().build()),
            write_order: None,
            stage_create: Some(false),
            properties: None,
        },
        DataAccess {
            vended_credentials: false,
            remote_signing: false,
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .expect("create_table should succeed");

    let unknown = TaskQueueName::from("never-registered-queue");
    let err = ApiServer::schedule_task(
        warehouse_id,
        &unknown,
        ScheduleTaskRequest {
            entity: WarehouseTaskEntityId::Table {
                table_id: table.metadata.uuid().into(),
            },
            scheduled_for: None,
            payload: None,
        },
        ctx,
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .expect_err("unknown queue must not return 2xx");
    assert_eq!(err.error.code, 404, "expected 404, got {err:?}");
    assert_eq!(err.error.r#type, "QueueNotFound");
}

#[sqlx::test]
async fn schedule_non_user_schedulable_queue_returns_400(pool: PgPool) {
    use lakekeeper::service::tasks::tabular_purge_queue::QUEUE_NAME as PURGE_QUEUE_NAME;

    let (ctx, warehouse, _registry) = setup_with_registry(
        pool,
        memory_io_profile(),
        None,
        AllowAllAuthorizer::default(),
        TabularDeleteProfile::Hard {},
        None,
        1,
        None,
    )
    .await;
    let warehouse_id = warehouse.warehouse_id;
    let ns = create_ns(ctx.clone(), warehouse_id.to_string(), "ns1".to_string()).await;
    let table = CatalogServer::create_table(
        NamespaceParameters {
            prefix: Some(Prefix(warehouse_id.to_string())),
            namespace: ns.namespace.clone(),
        },
        CreateTableRequest {
            name: "t-non-schedulable-queue".to_string(),
            location: None,
            schema: build_schema(),
            partition_spec: Some(UnboundPartitionSpec::builder().build()),
            write_order: None,
            stage_create: Some(false),
            properties: None,
        },
        DataAccess {
            vended_credentials: false,
            remote_signing: false,
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .expect("create_table should succeed");

    let err = ApiServer::schedule_task(
        warehouse_id,
        &PURGE_QUEUE_NAME,
        ScheduleTaskRequest {
            entity: WarehouseTaskEntityId::Table {
                table_id: table.metadata.uuid().into(),
            },
            scheduled_for: None,
            payload: None,
        },
        ctx,
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .expect_err("non-schedulable queue must not return 2xx");
    assert_eq!(err.error.code, 400, "expected 400, got {err:?}");
    assert_eq!(err.error.r#type, "QueueNotUserSchedulable");
}

static TYPED_PAYLOAD_QUEUE_NAME: LazyLock<TaskQueueName> =
    LazyLock::new(|| "test_schedulable_typed_payload".into());

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RequiredFieldPayload {
    #[allow(dead_code)]
    must_have: String,
}
impl TaskData for RequiredFieldPayload {}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(lakekeeper::utoipa::ToSchema))]
struct TypedPayloadConfig {}

impl TaskConfig for TypedPayloadConfig {
    fn queue_name() -> &'static TaskQueueName {
        &TYPED_PAYLOAD_QUEUE_NAME
    }
    fn max_time_since_last_heartbeat() -> chrono::Duration {
        chrono::Duration::seconds(60)
    }
}

#[sqlx::test]
async fn schedule_invalid_payload_returns_400(pool: PgPool) {
    let (ctx, warehouse, registry) = setup_with_registry(
        pool,
        memory_io_profile(),
        None,
        AllowAllAuthorizer::default(),
        TabularDeleteProfile::Hard {},
        None,
        1,
        None,
    )
    .await;

    registry
        .register_queue::<TypedPayloadConfig, RequiredFieldPayload>(QueueRegistration {
            queue_name: &TYPED_PAYLOAD_QUEUE_NAME,
            worker_fn: Arc::new(|_| Box::pin(async {})),
            num_workers: 0,
            scope: QueueScope::Warehouse,
            #[cfg(feature = "open-api")]
            user_scheduling: UserScheduling::Enabled {
                payload_schema: None,
            },
            #[cfg(not(feature = "open-api"))]
            user_scheduling: UserScheduling::Enabled,
        })
        .await;

    let warehouse_id = warehouse.warehouse_id;
    let ns = create_ns(ctx.clone(), warehouse_id.to_string(), "ns1".to_string()).await;
    let table = CatalogServer::create_table(
        NamespaceParameters {
            prefix: Some(Prefix(warehouse_id.to_string())),
            namespace: ns.namespace.clone(),
        },
        CreateTableRequest {
            name: "t-bad-payload".to_string(),
            location: None,
            schema: build_schema(),
            partition_spec: Some(UnboundPartitionSpec::builder().build()),
            write_order: None,
            stage_create: Some(false),
            properties: None,
        },
        DataAccess {
            vended_credentials: false,
            remote_signing: false,
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .expect("create_table should succeed");

    let err = ApiServer::schedule_task(
        warehouse_id,
        &TYPED_PAYLOAD_QUEUE_NAME,
        ScheduleTaskRequest {
            entity: WarehouseTaskEntityId::Table {
                table_id: table.metadata.uuid().into(),
            },
            scheduled_for: None,
            payload: Some(serde_json::json!({"unexpected": 42})),
        },
        ctx,
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .expect_err("malformed payload must not return 2xx");
    assert_eq!(err.error.code, 400, "expected 400, got {err:?}");
    assert_eq!(err.error.r#type, "InvalidTaskPayload");
}

#[sqlx::test]
async fn schedule_missing_table_returns_404(pool: PgPool) {
    let (ctx, warehouse, registry) = setup_with_registry(
        pool,
        memory_io_profile(),
        None,
        AllowAllAuthorizer::default(),
        TabularDeleteProfile::Hard {},
        None,
        1,
        None,
    )
    .await;

    registry
        .register_queue::<TestSchedulableConfig, TestSchedulablePayload>(QueueRegistration {
            queue_name: &TEST_QUEUE_NAME,
            worker_fn: Arc::new(|_| Box::pin(async {})),
            num_workers: 0,
            scope: QueueScope::Warehouse,
            #[cfg(feature = "open-api")]
            user_scheduling: UserScheduling::Enabled {
                payload_schema: None,
            },
            #[cfg(not(feature = "open-api"))]
            user_scheduling: UserScheduling::Enabled,
        })
        .await;

    let err = ApiServer::schedule_task(
        warehouse.warehouse_id,
        &TEST_QUEUE_NAME,
        ScheduleTaskRequest {
            entity: WarehouseTaskEntityId::Table {
                table_id: TableId::new_random(),
            },
            scheduled_for: None,
            payload: None,
        },
        ctx,
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .expect_err("missing table must not return 2xx");
    assert_eq!(err.error.code, 404, "expected 404, got {err:?}");
    assert_eq!(
        err.error.r#type, "NoSuchTableException",
        "missing-table 404 must carry the iceberg-standard error type, got {err:?}"
    );
}
