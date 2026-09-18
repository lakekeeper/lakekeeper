use std::{collections::HashMap, str::FromStr, time::Duration};

use iceberg::NamespaceIdent;
use lakekeeper::{
    api::{iceberg::v1::PaginationQuery, management::v1::DeleteKind},
    service::{
        CatalogDatasetOps, CatalogGenericTableOps, CatalogStore, CatalogTabularOps,
        DatasetConstraints, DatasetCreation, DatasetId, DatasetOwnership, GenericTableCreation,
        GenericTableFormat, GenericTableId, Location, NamedEntity, TabularId, TabularListFlags,
        Transaction,
        authz::AllowAllAuthorizer,
        storage::MemoryProfile,
        tasks::{
            ScheduleTaskMetadata, TaskEntity, TaskQueueRegistry, WarehouseTaskEntityId,
            tabular_expiration_queue::{TabularExpirationPayload, TabularExpirationTask},
        },
    },
};
use lakekeeper_storage_postgres::{
    CatalogState, PostgresBackend, PostgresTransaction, SecretsState,
    migrations::migrate_core_only, namespace::tests::initialize_namespace,
    tabular::table::tests::initialize_table, warehouse::test::initialize_warehouse,
};
use sqlx::PgPool;
use uuid::Uuid;

#[sqlx::test]
async fn test_queue_expiration_queue_task(pool: PgPool) {
    migrate_core_only(&pool).await.unwrap();
    let catalog_state = CatalogState::from_pools(pool.clone(), pool.clone());

    let queues = TaskQueueRegistry::new();

    let secrets = SecretsState::from_pools(pool.clone(), pool);
    let cat = catalog_state.clone();
    let sec = secrets.clone();
    let auth = AllowAllAuthorizer::default();
    queues
        .register_built_in_queues::<PostgresBackend, SecretsState, AllowAllAuthorizer>(
            cat,
            sec,
            auth,
            Duration::from_millis(100),
        )
        .await;
    let cancellation_token = lakekeeper::CancellationToken::new();
    let runner = queues.task_queues_runner(cancellation_token.clone()).await;
    let _queue_task = tokio::task::spawn(runner.run_queue_workers(true));

    let (project_id, warehouse_id) = initialize_warehouse(
        catalog_state.clone(),
        Some(MemoryProfile::default().into()),
        None,
        None,
        true,
    )
    .await;

    let table = initialize_table(
        warehouse_id,
        catalog_state.clone(),
        false,
        None,
        None,
        Some("tab".to_string()),
    )
    .await;
    let mut trx = PostgresTransaction::begin_read(catalog_state.clone())
        .await
        .unwrap();
    let _ = PostgresBackend::list_tabulars(
        warehouse_id,
        None,
        TabularListFlags {
            include_active: true,
            include_staged: false,
            include_deleted: true,
        },
        trx.transaction(),
        None,
        PaginationQuery::empty(),
    )
    .await
    .unwrap()
    .remove(&table.table_id.into())
    .unwrap();
    trx.commit().await.unwrap();
    let mut trx =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(catalog_state.clone())
            .await
            .unwrap();
    TabularExpirationTask::schedule_task::<PostgresBackend>(
        ScheduleTaskMetadata {
            project_id,
            parent_task_id: None,
            scheduled_for: Some(chrono::Utc::now() + chrono::Duration::seconds(1)),
            entity: TaskEntity::EntityInWarehouse {
                warehouse_id,
                entity_id: WarehouseTaskEntityId::Table {
                    table_id: table.table_id,
                },
                entity_name: table.table_ident.into_name_parts(),
            },
        },
        TabularExpirationPayload::new(DeleteKind::Purge),
        trx.transaction(),
    )
    .await
    .unwrap();

    PostgresBackend::mark_tabular_as_deleted(
        warehouse_id,
        table.table_id,
        false,
        trx.transaction(),
    )
    .await
    .unwrap();

    trx.commit().await.unwrap();

    let mut trx = PostgresTransaction::begin_read(catalog_state.clone())
        .await
        .unwrap();

    let deletion_info = PostgresBackend::list_tabulars(
        warehouse_id,
        None,
        TabularListFlags {
            include_active: false,
            include_staged: false,
            include_deleted: true,
        },
        trx.transaction(),
        None,
        PaginationQuery::empty(),
    )
    .await
    .unwrap()
    .remove(&table.table_id.into())
    .unwrap();
    assert!(deletion_info.expiration_task().is_some());
    assert!(deletion_info.deleted_at().is_some());
    trx.commit().await.unwrap();

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    loop {
        let mut trx = PostgresTransaction::begin_read(catalog_state.clone())
            .await
            .unwrap();
        let gone = PostgresBackend::list_tabulars(
            warehouse_id,
            None,
            TabularListFlags {
                include_active: false,
                include_staged: false,
                include_deleted: true,
            },
            trx.transaction(),
            None,
            PaginationQuery::empty(),
        )
        .await
        .unwrap()
        .remove(&table.table_id.into())
        .is_none();
        trx.commit().await.unwrap();
        if gone || std::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    let mut trx = PostgresTransaction::begin_read(catalog_state.clone())
        .await
        .unwrap();

    assert!(
        PostgresBackend::list_tabulars(
            warehouse_id,
            None,
            TabularListFlags {
                include_active: false,
                include_staged: false,
                include_deleted: true,
            },
            trx.transaction(),
            None,
            PaginationQuery::empty(),
        )
        .await
        .unwrap()
        .remove(&table.table_id.into())
        .is_none()
    );
    trx.commit().await.unwrap();

    cancellation_token.cancel();
}

#[sqlx::test]
async fn test_expiration_queue_drops_generic_table(pool: PgPool) {
    migrate_core_only(&pool).await.unwrap();
    let catalog_state = CatalogState::from_pools(pool.clone(), pool.clone());

    let queues = TaskQueueRegistry::new();
    let secrets = SecretsState::from_pools(pool.clone(), pool);
    queues
        .register_built_in_queues::<PostgresBackend, SecretsState, AllowAllAuthorizer>(
            catalog_state.clone(),
            secrets,
            AllowAllAuthorizer::default(),
            Duration::from_millis(100),
        )
        .await;
    let cancellation_token = lakekeeper::CancellationToken::new();
    let runner = queues.task_queues_runner(cancellation_token.clone()).await;
    let _queue_task = tokio::task::spawn(runner.run_queue_workers(true));

    let (project_id, warehouse_id) = initialize_warehouse(
        catalog_state.clone(),
        Some(MemoryProfile::default().into()),
        None,
        None,
        true,
    )
    .await;

    let namespace_ident = NamespaceIdent::from_vec(vec![format!("ns_{}", Uuid::now_v7())]).unwrap();
    let namespace =
        initialize_namespace(catalog_state.clone(), warehouse_id, &namespace_ident, None).await;
    let namespace_id = namespace.namespace_id();

    let generic_table_id = GenericTableId::from(Uuid::now_v7());
    let gt_name = format!("gt_{}", Uuid::now_v7());
    let location =
        Location::from_str(&format!("memory://test/{warehouse_id}/{generic_table_id}")).unwrap();
    let mut trx =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(catalog_state.clone())
            .await
            .unwrap();
    let info = PostgresBackend::create_generic_table(
        GenericTableCreation {
            generic_table_id,
            namespace_id,
            warehouse_id,
            name: gt_name.clone(),
            format: GenericTableFormat::Unknown("lance".to_string()),
            location,
            doc: None,
            schema: None,
            statistics: None,
            properties: HashMap::default(),
        },
        trx.transaction(),
    )
    .await
    .unwrap();
    let table_ident = info.tabular_ident.clone();

    TabularExpirationTask::schedule_task::<PostgresBackend>(
        ScheduleTaskMetadata {
            project_id,
            parent_task_id: None,
            scheduled_for: Some(chrono::Utc::now() + chrono::Duration::seconds(1)),
            entity: TaskEntity::EntityInWarehouse {
                warehouse_id,
                entity_id: WarehouseTaskEntityId::GenericTable { generic_table_id },
                entity_name: table_ident.into_name_parts(),
            },
        },
        TabularExpirationPayload::new(DeleteKind::Default),
        trx.transaction(),
    )
    .await
    .unwrap();
    PostgresBackend::mark_tabular_as_deleted(
        warehouse_id,
        TabularId::GenericTable(generic_table_id),
        false,
        trx.transaction(),
    )
    .await
    .unwrap();
    trx.commit().await.unwrap();

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let gone = loop {
        let mut trx = PostgresTransaction::begin_read(catalog_state.clone())
            .await
            .unwrap();
        let still_there = PostgresBackend::list_tabulars(
            warehouse_id,
            None,
            TabularListFlags {
                include_active: false,
                include_staged: false,
                include_deleted: true,
            },
            trx.transaction(),
            None,
            PaginationQuery::empty(),
        )
        .await
        .unwrap()
        .remove(&TabularId::GenericTable(generic_table_id))
        .is_some();
        trx.commit().await.unwrap();
        if !still_there {
            break true;
        }
        if std::time::Instant::now() >= deadline {
            break false;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    };
    assert!(
        gone,
        "expiration task did not hard-delete the generic table within 5s"
    );
    cancellation_token.cancel();
}

/// An imported dataset borrows its prefix, so expiring one must not hand the
/// location to the purge queue -- the objects are not Lakekeeper's to delete.
/// A managed dataset in the same run is the control: it does get a purge task.
#[sqlx::test]
async fn test_expiration_queue_purges_only_a_managed_dataset(pool: PgPool) {
    migrate_core_only(&pool).await.unwrap();
    let catalog_state = CatalogState::from_pools(pool.clone(), pool.clone());

    let queues = TaskQueueRegistry::new();
    let secrets = SecretsState::from_pools(pool.clone(), pool.clone());
    queues
        .register_built_in_queues::<PostgresBackend, SecretsState, AllowAllAuthorizer>(
            catalog_state.clone(),
            secrets,
            AllowAllAuthorizer::default(),
            Duration::from_millis(100),
        )
        .await;
    let cancellation_token = lakekeeper::CancellationToken::new();
    let runner = queues.task_queues_runner(cancellation_token.clone()).await;
    let _queue_task = tokio::task::spawn(runner.run_queue_workers(true));

    let (project_id, warehouse_id) = initialize_warehouse(
        catalog_state.clone(),
        Some(MemoryProfile::default().into()),
        None,
        None,
        true,
    )
    .await;

    let namespace_ident = NamespaceIdent::from_vec(vec![format!("ns_{}", Uuid::now_v7())]).unwrap();
    let namespace =
        initialize_namespace(catalog_state.clone(), warehouse_id, &namespace_ident, None).await;
    let namespace_id = namespace.namespace_id();

    let mut trx =
        <PostgresBackend as CatalogStore>::Transaction::begin_write(catalog_state.clone())
            .await
            .unwrap();

    let mut ids = Vec::new();
    for ownership in [DatasetOwnership::Imported, DatasetOwnership::Managed] {
        let dataset_id = DatasetId::from(Uuid::now_v7());
        let info = PostgresBackend::create_dataset(
            DatasetCreation {
                dataset_id,
                namespace_id,
                warehouse_id,
                name: format!("ds_{dataset_id}"),
                location: Location::from_str(&format!("memory://test/{warehouse_id}/{dataset_id}"))
                    .unwrap(),
                ownership,
                constraints: DatasetConstraints::default(),
            },
            trx.transaction(),
        )
        .await
        .unwrap();

        TabularExpirationTask::schedule_task::<PostgresBackend>(
            ScheduleTaskMetadata {
                project_id: project_id.clone(),
                parent_task_id: None,
                scheduled_for: Some(chrono::Utc::now() + chrono::Duration::seconds(1)),
                entity: TaskEntity::EntityInWarehouse {
                    warehouse_id,
                    entity_id: WarehouseTaskEntityId::Dataset { dataset_id },
                    entity_name: info.tabular_ident.clone().into_name_parts(),
                },
            },
            TabularExpirationPayload::new(DeleteKind::Purge),
            trx.transaction(),
        )
        .await
        .unwrap();
        PostgresBackend::mark_tabular_as_deleted(
            warehouse_id,
            TabularId::Dataset(dataset_id),
            false,
            trx.transaction(),
        )
        .await
        .unwrap();
        ids.push((ownership, dataset_id));
    }
    trx.commit().await.unwrap();

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let remaining: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM task WHERE queue_name = 'soft_deletion' AND warehouse_id = $1",
        )
        .bind(*warehouse_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        if remaining == 0 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "expiration tasks did not finish within 10s"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    for (ownership, dataset_id) in ids {
        // The purge worker may already have finished, so both tables count.
        let purges: i64 = sqlx::query_scalar(
            "SELECT (SELECT count(*) FROM task WHERE queue_name = 'tabular_purge' AND entity_id = $1)
                  + (SELECT count(*) FROM task_log WHERE queue_name = 'tabular_purge' AND entity_id = $1)",
        )
        .bind(*dataset_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        let expected = i64::from(ownership.is_managed());
        assert_eq!(
            purges, expected,
            "{ownership:?} dataset {dataset_id}: expected {expected} purge task(s), found {purges}"
        );
    }

    cancellation_token.cancel();
}
