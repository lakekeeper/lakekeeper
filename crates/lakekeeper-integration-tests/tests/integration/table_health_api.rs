//! The table health ranking shows only tables whose metadata the caller may read.

use lakekeeper::{
    api::{
        ApiContext, RequestMetadata,
        iceberg::{
            types::Prefix,
            v1::{DataAccess, NamespaceParameters, tables::TablesService as _},
        },
        management::v1::{
            ApiServer,
            health::{ListTableHealthQuery, TableHealthOrderBy, TableHealthService as _},
            warehouse::TabularDeleteProfile,
        },
    },
    server::CatalogServer,
    service::{State, TableId, WarehouseId, authz::tests::HidingAuthorizer},
};
use lakekeeper_integration_tests::{
    create_ns, create_table_request, memory_io_profile, setup_simple,
};
use lakekeeper_storage_postgres::{PostgresBackend, SecretsState};
use serde_json::json;
use sqlx::PgPool;

type Ctx = ApiContext<State<HidingAuthorizer, PostgresBackend, SecretsState>>;
type Api = ApiServer<PostgresBackend, HidingAuthorizer, SecretsState>;

struct Setup {
    ctx: Ctx,
    authz: HidingAuthorizer,
    ns_params: NamespaceParameters,
    warehouse_id: WarehouseId,
}

async fn setup(pool: PgPool) -> Setup {
    let authz = HidingAuthorizer::new();
    let (ctx, warehouse) = setup_simple(
        pool,
        memory_io_profile(),
        None,
        authz.clone(),
        TabularDeleteProfile::Hard {},
        None,
    )
    .await;
    let prefix = warehouse.warehouse_id.to_string();
    let namespace = create_ns(ctx.clone(), prefix.clone(), "ns".to_string())
        .await
        .namespace;
    Setup {
        ctx,
        authz,
        ns_params: NamespaceParameters {
            prefix: Some(Prefix(prefix)),
            namespace,
        },
        warehouse_id: warehouse.warehouse_id,
    }
}

async fn create_table(setup: &Setup, name: &str) -> TableId {
    let table = CatalogServer::create_table(
        setup.ns_params.clone(),
        create_table_request(Some(name.to_string()), Some(false)),
        DataAccess::not_specified(),
        setup.ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();
    TableId::from(table.metadata.uuid())
}

/// Stores a health row as a run of measurement version 1 would. `sizes` are the live data,
/// delete and metadata bytes, `None` for a table that can't be measured.
async fn insert_health(
    pool: &PgPool,
    warehouse_id: WarehouseId,
    table_id: TableId,
    sizes: Option<(i64, i64, i64)>,
) {
    let (status, max_severity, result) = match sizes {
        Some(_) => (
            "measured",
            0_i16,
            json!({"not-measurable": null, "findings": []}),
        ),
        None => (
            "not-measurable",
            2,
            json!({"not-measurable": {"reason": "encrypted"}, "findings": []}),
        ),
    };
    sqlx::query(
        "INSERT INTO health_iceberg_table (warehouse_id, table_id, measurement_version, status, \
         measured_at, next_not_before, max_severity, finding_count, result, live_data_bytes, \
         live_delete_bytes, live_metadata_bytes) \
         VALUES ($1, $2, 1, $3::health_status, now(), now(), $4, 0, $5, $6, $7, $8)",
    )
    .bind(*warehouse_id)
    .bind(*table_id)
    .bind(status)
    .bind(max_severity)
    .bind(result)
    .bind(sizes.map(|sizes| sizes.0))
    .bind(sizes.map(|sizes| sizes.1))
    .bind(sizes.map(|sizes| sizes.2))
    .execute(pool)
    .await
    .unwrap();
}

/// The ranking, read in pages of one.
async fn ranking(setup: &Setup) -> Vec<TableId> {
    let mut ids = vec![];
    let mut page_token = None;
    loop {
        let page = Api::list_table_health(
            setup.warehouse_id,
            ListTableHealthQuery {
                order_by: TableHealthOrderBy::Severity,
                include_soft_deleted: false,
                page_token: page_token.clone(),
                page_size: Some(1),
            },
            setup.ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();
        ids.extend(page.tables.iter().map(|table| table.table_id));
        match page.next_page_token {
            Some(token) => page_token = Some(token),
            None => return ids,
        }
    }
}

/// A hidden table is left out and the page refilled. `ListEverything` on the warehouse doesn't
/// skip the `GetMetadata` check.
#[lakekeeper_storage_postgres::db_test]
async fn test_ranking_lists_only_tables_whose_metadata_the_caller_may_read(pool: PgPool) {
    let setup = setup(pool.clone()).await;
    let mut tables = vec![];
    for (name, bytes) in [("large", 300), ("medium", 200), ("small", 100)] {
        let table_id = create_table(&setup, name).await;
        insert_health(&pool, setup.warehouse_id, table_id, Some((bytes, 0, 1))).await;
        tables.push(table_id);
    }
    assert_eq!(ranking(&setup).await, tables);

    setup
        .authz
        .hide(&format!("table:{}/{}", setup.warehouse_id, tables[1]));
    assert_eq!(ranking(&setup).await, vec![tables[0], tables[2]]);

    setup.authz.block_action("table:GetMetadata");
    assert_eq!(ranking(&setup).await, vec![]);
}
