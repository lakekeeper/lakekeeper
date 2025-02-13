use crate::api::iceberg::types::Prefix;
use crate::api::iceberg::v1::tables::TablesService;
use crate::api::iceberg::v1::{DataAccess, DropParams, TableParameters};
use crate::api::management::v1::warehouse::{CreateWarehouseResponse, TabularDeleteProfile};
use crate::api::ApiContext;
use crate::catalog::CatalogServer;
use crate::implementations::postgres::{PostgresCatalog, SecretsState};
use crate::service::authz::{AllowAllAuthorizer, Authorizer};
use crate::service::task_queue::TaskQueueConfig;
use crate::service::{State, UserId};
use crate::tests::random_request_metadata;
use crate::WarehouseIdent;
use iceberg::spec::TableMetadata;
use iceberg_ext::catalog::rest::LoadTableResult;
use sqlx::PgPool;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;

mod test {
    use crate::api::management::v1::warehouse::{
        ListDeletedTabularsQuery, Service, UndropTabularsRequest,
    };
    use crate::api::management::v1::ApiServer;
    use crate::service::TabularIdentUuid;
    use crate::tests::soft_deletions::{
        load_table, purge_table, setup_drop_test, table_location_exists, DropSetup,
    };
    use crate::tests::{random_request_metadata, spawn_drop_queues};
    use sqlx::PgPool;
    use std::time::Duration;
    use uuid::Uuid;

    #[sqlx::test]
    async fn test_soft_deletions_are_listable(pool: PgPool) {
        let DropSetup {
            ctx,
            warehouse,
            namespace_name,
            table_name,
            table,
        } = setup_drop_test(
            pool,
            chrono::Duration::seconds(125),
            std::time::Duration::from_millis(250),
        )
        .await;

        purge_table(&ctx, warehouse.warehouse_id, &namespace_name, &table_name).await;

        let err = load_table(&ctx, warehouse.warehouse_id, &namespace_name, &table_name)
            .await
            .unwrap_err();
        assert_eq!(err.error.code, http::StatusCode::NOT_FOUND.as_u16());
        assert!(table_location_exists(&table.metadata).await);

        let r = ApiServer::list_soft_deleted_tabulars(
            warehouse.warehouse_id,
            ListDeletedTabularsQuery {
                namespace_id: Some(Uuid::nil().into()),
                page_token: None,
                page_size: 1,
            },
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();
        assert!(r.tabulars.is_empty());

        let r = ApiServer::list_soft_deleted_tabulars(
            warehouse.warehouse_id,
            ListDeletedTabularsQuery {
                namespace_id: None,
                page_token: None,
                page_size: 10,
            },
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();
        assert!(!r.tabulars.is_empty());
        let now = chrono::Utc::now();
        assert_eq!(r.tabulars.first().unwrap().id, table.metadata.uuid());
        assert!(r.tabulars.first().unwrap().deleted_at < now);
        assert!(
            r.tabulars.first().unwrap().expiration_date > now,
            "{} {}",
            r.tabulars.first().unwrap().expiration_date,
            now
        );
    }

    #[sqlx::test]
    async fn test_soft_deleted_tables_are_deleted_after(pool: PgPool) {
        let DropSetup {
            ctx,
            warehouse,
            namespace_name,
            table_name,
            table,
        } = setup_drop_test(
            pool,
            chrono::Duration::milliseconds(125),
            std::time::Duration::from_millis(250),
        )
        .await;

        purge_table(&ctx, warehouse.warehouse_id, &namespace_name, &table_name).await;

        spawn_drop_queues(&ctx);

        let err = load_table(&ctx, warehouse.warehouse_id, &namespace_name, &table_name)
            .await
            .unwrap_err();
        assert_eq!(err.error.code, http::StatusCode::NOT_FOUND.as_u16());
        assert!(table_location_exists(&table.metadata).await);

        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        let mut exists = table_location_exists(&table.metadata).await;
        let mut cnt = 1;
        while exists && cnt < 5 {
            tokio::time::sleep(tokio::time::Duration::from_millis(75 * cnt)).await;
            exists = table_location_exists(&table.metadata).await;
            cnt += 1;
        }
        assert!(!exists);
    }

    #[sqlx::test]
    async fn test_undrop_makes_table_loadable_again(pool: PgPool) {
        let DropSetup {
            ctx,
            warehouse,
            namespace_name,
            table_name,
            table,
        } = setup_drop_test(
            pool,
            chrono::Duration::milliseconds(500),
            std::time::Duration::from_millis(100),
        )
        .await;

        purge_table(&ctx, warehouse.warehouse_id, &namespace_name, &table_name).await;

        spawn_drop_queues(&ctx);
        let err = load_table(&ctx, warehouse.warehouse_id, &namespace_name, &table_name)
            .await
            .unwrap_err();
        assert_eq!(err.error.code, http::StatusCode::NOT_FOUND.as_u16());

        ApiServer::undrop_tabulars(
            random_request_metadata(),
            UndropTabularsRequest {
                targets: vec![TabularIdentUuid::Table(table.metadata.uuid())],
            },
            ctx.clone(),
        )
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(750)).await;
        let t2 = load_table(&ctx, warehouse.warehouse_id, &namespace_name, &table_name)
            .await
            .unwrap();

        assert_eq!(table.metadata.uuid(), t2.metadata.uuid());
    }

    #[sqlx::test]
    async fn test_undropped_tables_can_be_dropped(pool: PgPool) {
        let DropSetup {
            ctx,
            warehouse,
            namespace_name,
            table_name,
            table,
        } = setup_drop_test(
            pool,
            chrono::Duration::milliseconds(500),
            std::time::Duration::from_millis(100),
        )
        .await;

        purge_table(&ctx, warehouse.warehouse_id, &namespace_name, &table_name).await;

        spawn_drop_queues(&ctx);
        let err = load_table(&ctx, warehouse.warehouse_id, &namespace_name, &table_name)
            .await
            .unwrap_err();
        assert_eq!(err.error.code, http::StatusCode::NOT_FOUND.as_u16());

        ApiServer::undrop_tabulars(
            random_request_metadata(),
            UndropTabularsRequest {
                targets: vec![TabularIdentUuid::Table(table.metadata.uuid())],
            },
            ctx.clone(),
        )
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(750)).await;
        let t2 = load_table(&ctx, warehouse.warehouse_id, &namespace_name, &table_name)
            .await
            .unwrap();

        assert_eq!(table.metadata.uuid(), t2.metadata.uuid());

        purge_table(&ctx, warehouse.warehouse_id, &namespace_name, &table_name).await;

        spawn_drop_queues(&ctx);
        let err = load_table(&ctx, warehouse.warehouse_id, &namespace_name, &table_name)
            .await
            .unwrap_err();
        assert_eq!(err.error.code, http::StatusCode::NOT_FOUND.as_u16());

        assert_eq!(err.error.code, http::StatusCode::NOT_FOUND.as_u16());
        assert!(table_location_exists(&table.metadata).await);

        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        let mut exists = table_location_exists(&table.metadata).await;
        let mut cnt = 1;
        while exists && cnt < 5 {
            tokio::time::sleep(tokio::time::Duration::from_millis(75 * cnt)).await;
            exists = table_location_exists(&table.metadata).await;
            cnt += 1;
        }
        assert!(!exists);
    }
}

struct DropSetup {
    ctx: ApiContext<State<AllowAllAuthorizer, PostgresCatalog, SecretsState>>,
    warehouse: CreateWarehouseResponse,
    namespace_name: String,
    table_name: String,
    table: LoadTableResult,
}

async fn setup_drop_test(
    pool: PgPool,
    expiration_seconds: chrono::Duration,
    poll_interval: std::time::Duration,
) -> DropSetup {
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
        TabularDeleteProfile::Soft { expiration_seconds },
        Some(UserId::OIDC("test-user-id".to_string())),
        Some(TaskQueueConfig {
            max_retries: 1,
            max_age: chrono::Duration::seconds(60),
            poll_interval,
        }),
        None,
    )
    .await;

    let ns_name = "ns1";
    let tab_name = "tab1".to_string();
    let _ = crate::tests::create_ns(
        ctx.clone(),
        warehouse.warehouse_id.to_string(),
        ns_name.to_string(),
    )
    .await;

    let tab = super::create_table(
        ctx.clone(),
        &warehouse.warehouse_id.to_string(),
        ns_name,
        &tab_name.clone(),
    )
    .await
    .unwrap();

    DropSetup {
        ctx,
        warehouse,
        namespace_name: ns_name.to_string(),
        table_name: tab_name,
        table: tab,
    }
}

async fn table_location_exists(meta: &TableMetadata) -> bool {
    tokio::fs::try_exists(&meta.location().splitn(2, ":/").collect::<Vec<_>>()[1])
        .await
        .unwrap()
}

async fn load_table<T: Authorizer>(
    ctx: &ApiContext<State<T, PostgresCatalog, SecretsState>>,
    warehouse: WarehouseIdent,
    ns_name: &str,
    tab_name: &str,
) -> crate::api::Result<LoadTableResult> {
    CatalogServer::load_table(
        TableParameters::new(Some(Prefix(warehouse.to_string())), ns_name, tab_name),
        DataAccess::none(),
        ctx.clone(),
        random_request_metadata(),
    )
    .await
}

async fn purge_table<T: Authorizer>(
    ctx: &ApiContext<State<T, PostgresCatalog, SecretsState>>,
    warehouse: WarehouseIdent,
    ns_name: &str,
    tab_name: &str,
) {
    CatalogServer::drop_table(
        TableParameters::new(Some(Prefix(warehouse.to_string())), ns_name, tab_name),
        DropParams {
            purge_requested: Some(true),
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
}
