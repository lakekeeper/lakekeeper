use sqlx::PgPool;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;

use crate::{
    api::{management::v1::warehouse::TabularDeleteProfile, ApiContext},
    implementations::postgres::{PostgresCatalog, SecretsState},
    service::{authz::AllowAllAuthorizer, task_queue::TaskQueueConfig, State, UserId},
    tests::TestWarehouseResponse,
};

mod test {
    use iceberg::NamespaceIdent;
    use sqlx::PgPool;

    use crate::{
        api::iceberg::{
            types::{PageToken, Prefix},
            v1::{ListTablesQuery, NamespaceParameters},
        },
        tests::drop_recursive::setup_drop_test,
    };

    #[sqlx::test]
    async fn test_recursive_drop_drops(pool: PgPool) {
        let setup = setup_drop_test(pool, 1, 1).await;
        let ctx = setup.ctx;
        let warehouse = setup.warehouse;
        let ns_name = setup.namespace_name;
        let tables = super::super::list_tables(
            ctx.clone(),
            NamespaceParameters {
                prefix: Some(Prefix(warehouse.warehouse_id.to_string())),
                namespace: NamespaceIdent::new(ns_name.clone()),
            },
            ListTablesQuery {
                page_token: PageToken::NotSpecified,
                page_size: None,
                return_uuids: false,
            },
        )
        .await;
        assert_eq!(tables.identifiers.len(), 1);
        assert_eq!(tables.identifiers[0].name, "tab0");
    }
}

// TODO: test with multiple warehouses and projects

struct DropSetup {
    ctx: ApiContext<State<AllowAllAuthorizer, PostgresCatalog, SecretsState>>,
    warehouse: TestWarehouseResponse,
    namespace_name: String,
}

async fn setup_drop_test(
    pool: PgPool,
    n_tabs: usize,
    n_views: usize,
    n_namespaces: usize,
) -> DropSetup {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
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
        Some(TaskQueueConfig {
            max_retries: 1,
            max_age: chrono::Duration::seconds(60),
            poll_interval: std::time::Duration::from_secs(10),
        }),
    )
    .await;
    for ns in 0..n_namespaces {
        let ns_name = format!("ns{ns}");

        let _ = crate::tests::create_ns(
            ctx.clone(),
            warehouse.warehouse_id.to_string(),
            ns_name.to_string(),
        )
        .await;
        for i in 0..n_tabs {
            let tab_name = format!("tab{i}");

            let _ = crate::tests::create_table(
                ctx.clone(),
                &warehouse.warehouse_id.to_string(),
                ns_name,
                &tab_name,
            )
            .await
            .unwrap();
        }

        for i in 0..n_views {
            let view_name = format!("view{i}");
            crate::tests::create_view(
                ctx.clone(),
                &warehouse.warehouse_id.to_string(),
                ns_name,
                &view_name,
                None,
            )
            .await
            .unwrap();
        }
    }

    DropSetup {
        ctx,
        warehouse,
        namespace_name: ns_name.to_string(),
    }
}
