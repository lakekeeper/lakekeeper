use std::collections::HashMap;

use futures::future::join_all;
use iceberg::{NamespaceIdent, TableIdent};
use iceberg_ext::catalog::rest::CreateNamespaceRequest;
use lakekeeper::{
    WarehouseId,
    api::{
        iceberg::{
            types::Prefix,
            v1::{
                DropParams, ListTablesQuery, LoadTableResultOrNotModified, NamespaceParameters,
                TableParameters,
                namespace::NamespaceService,
                tables::{LoadTableRequest, TablesService},
            },
        },
        management::v1::{
            ApiServer,
            tasks::{
                ControlTaskAction, ControlTasksRequest, ListTasksRequest, Service as _, TaskStatus,
            },
            warehouse::{
                ListDeletedTabularsQuery, Service, TabularDeleteProfile, UndropTabularsRequest,
            },
        },
    },
    server::{CatalogServer, NAMESPACE_ID_PROPERTY},
    service::{
        ArcProjectId, CatalogStore, NamespaceId, State, TableId, TabularId, Transaction as _,
        UserId,
        authz::{AllowAllAuthorizer, tests::HidingAuthorizer},
        events::{EventListener, tabular::UndropTabularEvent},
        tasks::{
            ScheduleTaskMetadata, TaskEntity, TaskId, WarehouseTaskEntityId,
            tabular_expiration_queue::QUEUE_NAME as EXPIRATION_QUEUE_NAME,
            tabular_purge_queue::{
                QUEUE_NAME as PURGE_QUEUE_NAME, TabularPurgePayload, TabularPurgeTask,
            },
        },
    },
};
use lakekeeper_integration_tests::{create_ns, create_table, random_request_metadata};
use lakekeeper_storage_postgres::{PostgresBackend, SecretsState};
use sqlx::PgPool;
use uuid::Uuid;

#[sqlx::test]
async fn test_soft_deletion(pool: PgPool) {
    let storage_profile = lakekeeper_integration_tests::memory_io_profile();
    let authorizer = AllowAllAuthorizer::default();

    let (api_context, warehouse) = lakekeeper_integration_tests::setup(
        pool.clone(),
        storage_profile.clone(),
        None,
        authorizer,
        TabularDeleteProfile::Soft {
            expiration_seconds: chrono::Duration::seconds(300),
        },
        None,
        1,
        None,
    )
    .await;

    // Create namespace
    let ns_ident = NamespaceIdent::new(format!("test_namespace_{}", Uuid::now_v7()));
    let prefix = Some(Prefix(warehouse.warehouse_id.to_string()));
    let create_ns_response = CatalogServer::create_namespace(
        prefix.clone(),
        CreateNamespaceRequest {
            namespace: ns_ident.clone(),
            properties: None,
        },
        api_context.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    let ns_id = NamespaceId::from(
        uuid::Uuid::parse_str(
            create_ns_response
                .properties
                .unwrap()
                .get(NAMESPACE_ID_PROPERTY)
                .unwrap(),
        )
        .unwrap(),
    );

    // Create tables in parallel
    let create_futs = (0..20).map(|i| {
        let api_context = api_context.clone();
        let warehouse_id = warehouse.warehouse_id.to_string();
        let ns_name = ns_ident.to_string();
        let table_name = format!("table_{i}");
        tokio::spawn(async move {
            (
                table_name.clone(),
                lakekeeper_integration_tests::create_table(
                    api_context.clone(),
                    &warehouse_id,
                    &ns_name,
                    &table_name,
                    false,
                )
                .await
                .unwrap()
                .metadata
                .uuid(),
            )
        })
    });
    let table_name_to_uuid = join_all(create_futs)
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect::<HashMap<_, _>>();

    // Delete half of the tables in parallel
    let delete_futs = (0..10).map(|i| {
        let api_context = api_context.clone();
        let warehouse_id = warehouse.warehouse_id.to_string();
        let ns_ident_clone = ns_ident.clone();
        let table_name = format!("table_{i}");
        let table_parameters = TableParameters {
            prefix: Some(Prefix(warehouse_id.clone())),
            table: TableIdent::new(ns_ident_clone, table_name),
        };
        tokio::spawn(async move {
            CatalogServer::drop_table(
                table_parameters,
                DropParams {
                    purge_requested: true,
                    force: false,
                },
                api_context,
                random_request_metadata(),
            )
            .await
            .unwrap();
        })
    });
    let drops = join_all(delete_futs).await;
    for j in drops {
        j.expect("drop_table task panicked");
    }

    // Verify that half of the tables are dropped
    let tables = CatalogServer::list_tables(
        NamespaceParameters {
            prefix: Some(Prefix(warehouse.warehouse_id.to_string())),
            namespace: ns_ident.clone(),
        },
        ListTablesQuery {
            return_uuids: true,
            ..Default::default()
        },
        api_context.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    let identifiers = tables.identifiers;
    assert_eq!(identifiers.len(), 10);
    for i in 10..20 {
        let table_name = format!("table_{i}");
        assert!(identifiers.contains(&TableIdent::new(ns_ident.clone(), table_name)));
    }
    for i in 0..10 {
        let table_name = format!("table_{i}");
        assert!(!identifiers.contains(&TableIdent::new(ns_ident.clone(), table_name)));
    }

    // List tasks and check that expiration tasks are enqueued
    let tasks = ApiServer::list_tasks(
        warehouse.warehouse_id,
        ListTasksRequest {
            status: Some(vec![TaskStatus::Scheduled]),
            queue_name: Some(vec![EXPIRATION_QUEUE_NAME.clone()]),
            ..Default::default()
        },
        api_context.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap()
    .tasks;

    assert_eq!(tasks.len(), 10);
    for task in tasks {
        assert_eq!(&task.queue_name, &*EXPIRATION_QUEUE_NAME);
        assert_eq!(task.status, TaskStatus::Scheduled);
    }

    // List deleted tabulars
    let deleted_tabulars = ApiServer::list_soft_deleted_tabulars(
        warehouse.warehouse_id,
        ListDeletedTabularsQuery {
            namespace_id: Some(ns_id),
            ..Default::default()
        },
        api_context.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap()
    .tabulars;

    assert_eq!(deleted_tabulars.len(), 10);
    for i in 0..10 {
        let table_name = format!("table_{i}");
        assert!(deleted_tabulars.iter().any(|t| { t.name == table_name }));
    }

    // Un-delete one of the deleted tables
    let undrop_table_name = "table_4";
    let undrop_table_id =
        TabularId::Table((*table_name_to_uuid.get(undrop_table_name).unwrap()).into());

    ApiServer::undrop_tabulars(
        warehouse.warehouse_id,
        random_request_metadata(),
        UndropTabularsRequest {
            targets: vec![undrop_table_id],
        },
        api_context.clone(),
    )
    .await
    .unwrap();

    // Verify we can load the table
    let table = CatalogServer::load_table(
        TableParameters {
            prefix: Some(Prefix(warehouse.warehouse_id.to_string())),
            table: TableIdent::new(ns_ident.clone(), undrop_table_name.to_string()),
        },
        LoadTableRequest::builder().build(),
        api_context.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    let LoadTableResultOrNotModified::LoadTableResult(table) = table else {
        panic!("Expected LoadTableResult, got NotModified");
    };

    assert_eq!(table.metadata.uuid(), *undrop_table_id);

    // Verify listing tables shows the undropped table
    let tables = CatalogServer::list_tables(
        NamespaceParameters {
            prefix: Some(Prefix(warehouse.warehouse_id.to_string())),
            namespace: ns_ident.clone(),
        },
        ListTablesQuery {
            ..Default::default()
        },
        api_context.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    let identifiers = tables.identifiers;
    assert_eq!(identifiers.len(), 11);
    assert!(identifiers.contains(&TableIdent::new(
        ns_ident.clone(),
        undrop_table_name.to_string()
    )));

    // List deleted tabulars, should now be 1 less
    let deleted_tabulars = ApiServer::list_soft_deleted_tabulars(
        warehouse.warehouse_id,
        ListDeletedTabularsQuery {
            namespace_id: Some(ns_id),
            ..Default::default()
        },
        api_context.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap()
    .tabulars;
    assert_eq!(deleted_tabulars.len(), 9);
}

#[sqlx::test]
async fn test_soft_delete_and_undrop_generic_table(pool: PgPool) {
    use lakekeeper::api::{
        data::v1::generic_tables::{
            GenericTableParameters, GenericTableService as _, ListGenericTablesQuery,
        },
        iceberg::types::DropParams,
    };

    let storage_profile = lakekeeper_integration_tests::memory_io_profile();
    let authorizer = AllowAllAuthorizer::default();

    let (api_context, warehouse) = lakekeeper_integration_tests::setup(
        pool.clone(),
        storage_profile,
        None,
        authorizer,
        TabularDeleteProfile::Soft {
            expiration_seconds: chrono::Duration::seconds(300),
        },
        None,
        1,
        None,
    )
    .await;

    let prefix = warehouse.warehouse_id.to_string();
    let ns_name = format!("test_namespace_{}", Uuid::now_v7());

    lakekeeper_integration_tests::create_ns(api_context.clone(), prefix.clone(), ns_name.clone())
        .await;

    let gt_name = "my_gt";
    lakekeeper_integration_tests::create_generic_table(
        api_context.clone(),
        prefix.clone(),
        ns_name.clone(),
        gt_name,
    )
    .await
    .unwrap();

    let listed = CatalogServer::list_generic_tables(
        NamespaceParameters {
            prefix: Some(Prefix(prefix.clone())),
            namespace: NamespaceIdent::new(ns_name.clone()),
        },
        ListGenericTablesQuery::default(),
        api_context.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    let gt_id = listed
        .identifiers
        .iter()
        .find(|i| i.name == gt_name)
        .and_then(|i| i.id)
        .expect("generic table id should be returned by list");

    // Soft-delete via drop
    CatalogServer::drop_generic_table(
        GenericTableParameters {
            prefix: Some(Prefix(prefix.clone())),
            namespace: NamespaceIdent::new(ns_name.clone()),
            table_name: gt_name.to_string(),
        },
        DropParams {
            purge_requested: false,
            force: false,
        },
        api_context.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    // Active list excludes the dropped generic table.
    let listed = CatalogServer::list_generic_tables(
        NamespaceParameters {
            prefix: Some(Prefix(prefix.clone())),
            namespace: NamespaceIdent::new(ns_name.clone()),
        },
        ListGenericTablesQuery::default(),
        api_context.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    assert!(!listed.identifiers.iter().any(|i| i.name == gt_name));

    // The dropped GT appears in the soft-deleted listing.
    let deleted = ApiServer::list_soft_deleted_tabulars(
        warehouse.warehouse_id,
        ListDeletedTabularsQuery::default(),
        api_context.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap()
    .tabulars;
    assert!(
        deleted.iter().any(|t| t.name == gt_name && t.id == *gt_id),
        "expected dropped generic table in soft-deleted listing: {deleted:?}",
    );

    // Undrop: the GT is listable again, with the same id.
    ApiServer::undrop_tabulars(
        warehouse.warehouse_id,
        random_request_metadata(),
        UndropTabularsRequest {
            targets: vec![TabularId::GenericTable(gt_id)],
        },
        api_context.clone(),
    )
    .await
    .unwrap();

    let listed = CatalogServer::list_generic_tables(
        NamespaceParameters {
            prefix: Some(Prefix(prefix.clone())),
            namespace: NamespaceIdent::new(ns_name.clone()),
        },
        ListGenericTablesQuery::default(),
        api_context.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    assert!(
        listed
            .identifiers
            .iter()
            .any(|i| i.name == gt_name && i.id == Some(gt_id)),
        "undropped generic table should reappear in list with the same id",
    );
}

type HidingCtx =
    lakekeeper::api::ApiContext<State<HidingAuthorizer, PostgresBackend, SecretsState>>;

/// A soft-deleted table `ns.tbl` and its scheduled soft-deletion task.
struct SoftDeletedTable {
    ctx: HidingCtx,
    authz: HidingAuthorizer,
    project_id: ArcProjectId,
    warehouse_id: WarehouseId,
    table_id: TableId,
    task_id: TaskId,
}

async fn soft_deleted_table_with_task(pool: PgPool) -> SoftDeletedTable {
    let authz = HidingAuthorizer::new();
    let (ctx, warehouse) = lakekeeper_integration_tests::setup_simple(
        pool,
        lakekeeper_integration_tests::memory_io_profile(),
        None,
        authz.clone(),
        TabularDeleteProfile::Soft {
            expiration_seconds: chrono::Duration::seconds(300),
        },
        Some(UserId::new_unchecked("oidc", "test-user-id")),
    )
    .await;
    let warehouse_id = warehouse.warehouse_id;
    let prefix = warehouse_id.to_string();
    create_ns(ctx.clone(), prefix.clone(), "ns".to_string()).await;
    let table_id = TableId::from(
        create_table(ctx.clone(), prefix, "ns", "tbl", false)
            .await
            .unwrap()
            .metadata
            .uuid(),
    );
    drop_tbl(&ctx, warehouse_id).await;
    let tasks = scheduled_tasks(&ctx, warehouse_id, &EXPIRATION_QUEUE_NAME).await;
    assert_eq!(tasks.len(), 1);
    SoftDeletedTable {
        ctx,
        authz,
        project_id: warehouse.project_id,
        warehouse_id,
        table_id,
        task_id: tasks[0],
    }
}

/// Soft-delete `ns.tbl`, which schedules its soft-deletion task.
async fn drop_tbl(ctx: &HidingCtx, warehouse_id: WarehouseId) {
    CatalogServer::drop_table(
        TableParameters {
            prefix: Some(Prefix(warehouse_id.to_string())),
            table: TableIdent::new(NamespaceIdent::new("ns".to_string()), "tbl".to_string()),
        },
        DropParams {
            purge_requested: true,
            force: false,
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
}

async fn undrop_tbl(ctx: &HidingCtx, warehouse_id: WarehouseId, table_id: TableId) {
    ApiServer::undrop_tabulars(
        warehouse_id,
        random_request_metadata(),
        UndropTabularsRequest {
            targets: vec![TabularId::Table(table_id)],
        },
        ctx.clone(),
    )
    .await
    .unwrap();
}

async fn scheduled_tasks(
    ctx: &HidingCtx,
    warehouse_id: WarehouseId,
    queue_name: &lakekeeper::service::tasks::TaskQueueName,
) -> Vec<TaskId> {
    ApiServer::list_tasks(
        warehouse_id,
        ListTasksRequest {
            status: Some(vec![TaskStatus::Scheduled]),
            queue_name: Some(vec![queue_name.clone()]),
            ..Default::default()
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap()
    .tasks
    .into_iter()
    .map(|t| t.task_id)
    .collect()
}

async fn soft_deleted_names(ctx: &HidingCtx, warehouse_id: WarehouseId) -> Vec<String> {
    ApiServer::list_soft_deleted_tabulars(
        warehouse_id,
        ListDeletedTabularsQuery::default(),
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap()
    .tabulars
    .iter()
    .map(|t| t.name.clone())
    .collect()
}

async fn cancel_tasks(
    ctx: &HidingCtx,
    warehouse_id: WarehouseId,
    task_ids: Vec<TaskId>,
) -> lakekeeper::api::Result<()> {
    ApiServer::control_tasks(
        warehouse_id,
        ControlTasksRequest {
            action: ControlTaskAction::Cancel,
            task_ids,
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
}

/// Cancelling a soft-deletion task undrops the table, so it needs `undrop` on the table even
/// when the caller may control all tasks of the warehouse.
#[sqlx::test]
async fn test_cancel_soft_deletion_task_requires_undrop(pool: PgPool) {
    let SoftDeletedTable {
        ctx,
        authz,
        warehouse_id,
        task_id,
        ..
    } = soft_deleted_table_with_task(pool).await;
    authz.block_action("table:Undrop");

    let err = cancel_tasks(&ctx, warehouse_id, vec![task_id])
        .await
        .unwrap_err();
    assert_eq!(err.error.code, http::StatusCode::FORBIDDEN, "{err:?}");
    assert_eq!(err.error.r#type, "TableActionForbidden");

    assert_eq!(
        scheduled_tasks(&ctx, warehouse_id, &EXPIRATION_QUEUE_NAME).await,
        vec![task_id]
    );
    assert_eq!(soft_deleted_names(&ctx, warehouse_id).await, vec!["tbl"]);
}

#[sqlx::test]
async fn test_cancel_soft_deletion_task_with_undrop_restores_the_table(pool: PgPool) {
    let SoftDeletedTable {
        ctx,
        warehouse_id,
        task_id,
        ..
    } = soft_deleted_table_with_task(pool).await;

    cancel_tasks(&ctx, warehouse_id, vec![task_id])
        .await
        .unwrap();

    assert_eq!(
        scheduled_tasks(&ctx, warehouse_id, &EXPIRATION_QUEUE_NAME).await,
        Vec::<TaskId>::new()
    );
    assert_eq!(
        soft_deleted_names(&ctx, warehouse_id).await,
        Vec::<String>::new()
    );
}

/// Forwards every undrop event, so a test can assert what a request announced.
#[derive(Debug)]
struct UndropCapture(tokio::sync::mpsc::UnboundedSender<UndropTabularEvent>);

impl std::fmt::Display for UndropCapture {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "UndropCapture")
    }
}

#[async_trait::async_trait]
impl EventListener for UndropCapture {
    async fn tabular_undropped(&self, event: UndropTabularEvent) -> anyhow::Result<()> {
        let _ = self.0.send(event);
        Ok(())
    }
}

async fn capture_undrops(
    ctx: &HidingCtx,
) -> tokio::sync::mpsc::UnboundedReceiver<UndropTabularEvent> {
    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
    ctx.v1_state
        .events
        .append(std::sync::Arc::new(UndropCapture(sender)))
        .await;
    receiver
}

/// The next undrop event, then a check that no second one follows.
async fn the_only_undrop(
    events: &mut tokio::sync::mpsc::UnboundedReceiver<UndropTabularEvent>,
) -> UndropTabularEvent {
    let event = tokio::time::timeout(std::time::Duration::from_secs(5), events.recv())
        .await
        .expect("an undrop event is dispatched")
        .expect("the listener is still registered");
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert!(events.try_recv().is_err(), "exactly one undrop event");
    event
}

/// Waits briefly, then checks that no undrop event was dispatched.
async fn no_undrop(events: &mut tokio::sync::mpsc::UnboundedReceiver<UndropTabularEvent>) {
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    assert!(
        matches!(
            events.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ),
        "no undrop event"
    );
}

/// Cancelling a soft-deletion task announces the undrop as `undrop_tabulars` does.
#[sqlx::test]
async fn test_cancel_soft_deletion_task_emits_the_undrop(pool: PgPool) {
    let SoftDeletedTable {
        ctx,
        warehouse_id,
        table_id,
        task_id,
        ..
    } = soft_deleted_table_with_task(pool).await;
    let mut undrops = capture_undrops(&ctx).await;

    cancel_tasks(&ctx, warehouse_id, vec![task_id])
        .await
        .unwrap();

    let event = the_only_undrop(&mut undrops).await;
    assert_eq!(event.warehouse.warehouse_id, warehouse_id);
    assert_eq!(event.request.targets, vec![TabularId::Table(table_id)]);
    assert_eq!(
        event
            .responses
            .iter()
            .map(|r| (r.tabular_id(), r.tabular_ident().clone()))
            .collect::<Vec<_>>(),
        vec![(
            TabularId::Table(table_id),
            TableIdent::new(NamespaceIdent::new("ns".to_string()), "tbl".to_string())
        )]
    );
}

/// Schedule a purge task for the active table `ns.<name>`: a task outside the soft-deletion
/// queue, which a cancel stops without undropping anything.
async fn scheduled_purge_task(table: &SoftDeletedTable, name: &str) -> TaskId {
    let prefix = table.warehouse_id.to_string();
    let created = create_table(table.ctx.clone(), prefix, "ns", name, false)
        .await
        .unwrap();
    let mut t = <PostgresBackend as CatalogStore>::Transaction::begin_write(
        table.ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();
    let task_id = TabularPurgeTask::schedule_task::<PostgresBackend>(
        ScheduleTaskMetadata {
            project_id: table.project_id.clone(),
            parent_task_id: None,
            scheduled_for: Some(chrono::Utc::now() + chrono::Duration::hours(1)),
            entity: TaskEntity::EntityInWarehouse {
                warehouse_id: table.warehouse_id,
                entity_id: WarehouseTaskEntityId::Table {
                    table_id: created.metadata.uuid().into(),
                },
                entity_name: vec!["ns".to_string(), name.to_string()],
            },
        },
        TabularPurgePayload::new(created.metadata.location()),
        t.transaction(),
    )
    .await
    .unwrap()
    .expect("a purge task is scheduled");
    t.commit().await.unwrap();
    task_id
}

/// One request cancelling a soft-deletion task and a task of another queue still needs
/// `undrop` on the soft-deleted table, and a refusal cancels neither task.
#[sqlx::test]
async fn test_cancel_soft_deletion_task_together_with_another_task_requires_undrop(pool: PgPool) {
    let table = soft_deleted_table_with_task(pool).await;
    let purge_task_id = scheduled_purge_task(&table, "other").await;
    let SoftDeletedTable {
        ctx,
        authz,
        warehouse_id,
        task_id,
        ..
    } = table;
    authz.block_action("table:Undrop");

    let err = cancel_tasks(&ctx, warehouse_id, vec![task_id, purge_task_id])
        .await
        .unwrap_err();
    assert_eq!(err.error.code, http::StatusCode::FORBIDDEN, "{err:?}");
    assert_eq!(err.error.r#type, "TableActionForbidden");
    assert_eq!(
        scheduled_tasks(&ctx, warehouse_id, &EXPIRATION_QUEUE_NAME).await,
        vec![task_id]
    );
    assert_eq!(
        scheduled_tasks(&ctx, warehouse_id, &PURGE_QUEUE_NAME).await,
        vec![purge_task_id]
    );
    assert_eq!(soft_deleted_names(&ctx, warehouse_id).await, vec!["tbl"]);
}

/// One request cancelling a soft-deletion task and a task of another queue cancels both and
/// undrops only the soft-deleted table.
#[sqlx::test]
async fn test_cancel_soft_deletion_task_together_with_another_task(pool: PgPool) {
    let table = soft_deleted_table_with_task(pool).await;
    let purge_task_id = scheduled_purge_task(&table, "other").await;
    let SoftDeletedTable {
        ctx,
        warehouse_id,
        table_id,
        task_id,
        ..
    } = table;
    let mut undrops = capture_undrops(&ctx).await;

    cancel_tasks(&ctx, warehouse_id, vec![task_id, purge_task_id])
        .await
        .unwrap();

    assert_eq!(
        scheduled_tasks(&ctx, warehouse_id, &EXPIRATION_QUEUE_NAME).await,
        Vec::<TaskId>::new()
    );
    assert_eq!(
        scheduled_tasks(&ctx, warehouse_id, &PURGE_QUEUE_NAME).await,
        Vec::<TaskId>::new()
    );
    assert_eq!(
        soft_deleted_names(&ctx, warehouse_id).await,
        Vec::<String>::new()
    );
    let event = the_only_undrop(&mut undrops).await;
    assert_eq!(event.request.targets, vec![TabularId::Table(table_id)]);
}

/// Cancelling a soft-deletion task that an undrop already cancelled changes nothing and
/// announces no undrop.
#[sqlx::test]
async fn test_cancel_stale_soft_deletion_task_of_an_active_table(pool: PgPool) {
    let SoftDeletedTable {
        ctx,
        warehouse_id,
        table_id,
        task_id,
        ..
    } = soft_deleted_table_with_task(pool).await;
    let mut undrops = capture_undrops(&ctx).await;
    undrop_tbl(&ctx, warehouse_id, table_id).await;
    let undrop = the_only_undrop(&mut undrops).await;
    assert_eq!(undrop.request.targets, vec![TabularId::Table(table_id)]);

    cancel_tasks(&ctx, warehouse_id, vec![task_id])
        .await
        .unwrap();

    no_undrop(&mut undrops).await;
    assert_eq!(
        scheduled_tasks(&ctx, warehouse_id, &EXPIRATION_QUEUE_NAME).await,
        Vec::<TaskId>::new()
    );
    assert_eq!(
        soft_deleted_names(&ctx, warehouse_id).await,
        Vec::<String>::new()
    );
}

/// Cancelling the soft-deletion task of an earlier drop leaves a table dropped again
/// soft-deleted, with its newer task still scheduled.
#[sqlx::test]
async fn test_cancel_stale_soft_deletion_task_keeps_a_newer_drop(pool: PgPool) {
    let SoftDeletedTable {
        ctx,
        warehouse_id,
        table_id,
        task_id: stale_task_id,
        ..
    } = soft_deleted_table_with_task(pool).await;
    let mut undrops = capture_undrops(&ctx).await;
    undrop_tbl(&ctx, warehouse_id, table_id).await;
    let undrop = the_only_undrop(&mut undrops).await;
    assert_eq!(undrop.request.targets, vec![TabularId::Table(table_id)]);
    drop_tbl(&ctx, warehouse_id).await;
    let tasks = scheduled_tasks(&ctx, warehouse_id, &EXPIRATION_QUEUE_NAME).await;
    assert_eq!(tasks.len(), 1);
    let newer_task_id = tasks[0];
    assert_ne!(newer_task_id, stale_task_id);

    cancel_tasks(&ctx, warehouse_id, vec![stale_task_id])
        .await
        .unwrap();

    no_undrop(&mut undrops).await;
    assert_eq!(
        scheduled_tasks(&ctx, warehouse_id, &EXPIRATION_QUEUE_NAME).await,
        vec![newer_task_id]
    );
    assert_eq!(soft_deleted_names(&ctx, warehouse_id).await, vec!["tbl"]);
}
