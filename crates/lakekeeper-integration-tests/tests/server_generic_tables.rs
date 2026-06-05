// Extracted from crates/lakekeeper/src/server/generic_tables.rs.
// Original location was `#[cfg(any())] pub(crate) mod test` (VAK-437 split).

use std::collections::HashMap;

use http::StatusCode;
use iceberg::NamespaceIdent;
use lakekeeper::{
    api::{
        ApiContext,
        data::v1::generic_tables::{
            CreateGenericTableRequest, GenericTableParameters, GenericTableService as _,
            ListGenericTablesQuery, RenameGenericTableRequest, RenameGenericTableTarget,
        },
        iceberg::{
            types::DropParams,
            v1::{DataAccessMode, namespace::NamespaceParameters},
        },
    },
    server::CatalogServer,
    service::{
        CatalogTabularOps as _, GenericTableFormat, State, TabularId, Transaction as _,
        authz::AllowAllAuthorizer,
        idempotency::IdempotencyKey,
        storage::{MemoryProfile, StorageProfile},
    },
};
use lakekeeper_integration_tests::{get_api_context, random_request_metadata};
use lakekeeper_storage_postgres::{
    PostgresBackend, SecretsState, namespace::tests::initialize_namespace,
    warehouse::test::initialize_warehouse,
};
use sqlx::PgPool;

type Ctx = ApiContext<State<AllowAllAuthorizer, PostgresBackend, SecretsState>>;

async fn setup(pool: PgPool) -> (Ctx, NamespaceIdent, lakekeeper::WarehouseId) {
    lakekeeper_storage_postgres::migrations::migrate_core_only(&pool)
        .await
        .unwrap();
    let api_context = get_api_context(&pool, AllowAllAuthorizer::default()).await;
    let state = api_context.v1_state.catalog.clone();
    let (_project_id, warehouse_id) = initialize_warehouse(
        state.clone(),
        Some(StorageProfile::Memory(MemoryProfile::default())),
        None,
        None,
        true,
    )
    .await;

    let namespace = initialize_namespace(
        state,
        warehouse_id,
        &NamespaceIdent::new(uuid::Uuid::now_v7().to_string()),
        None,
    )
    .await
    .namespace_ident()
    .clone();

    (api_context, namespace, warehouse_id)
}

fn rename_target(namespace: &NamespaceIdent, name: &str) -> RenameGenericTableTarget {
    RenameGenericTableTarget {
        namespace: namespace.clone().inner(),
        name: name.to_string(),
    }
}

fn create_request(name: &str) -> CreateGenericTableRequest {
    CreateGenericTableRequest {
        name: name.to_string(),
        format: GenericTableFormat::Unknown("lance".to_string()),
        base_location: None,
        doc: Some("test doc".to_string()),
        properties: HashMap::default(),
        schema: None,
        statistics: None,
    }
}

#[sqlx::test]
async fn test_create_invalid_format_fails(pool: PgPool) {
    let (ctx, namespace, whi) = setup(pool).await;
    let params = NamespaceParameters {
        prefix: Some(whi.to_string().into()),
        namespace,
    };
    for bad in [
        "",
        "LANCE",
        "1lance",
        "lance!",
        "lance space",
        &"a".repeat(65),
    ] {
        let mut req = create_request("gt");
        req.format = GenericTableFormat::Unknown(bad.to_string());
        let err = CatalogServer::create_generic_table(
            params.clone(),
            req,
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .expect_err(&format!("format `{bad}` should be rejected"));
        assert_eq!(err.error.code, StatusCode::BAD_REQUEST, "{err:?}");
    }
}

#[sqlx::test]
async fn test_create_oversized_blob_fails(pool: PgPool) {
    let (ctx, namespace, whi) = setup(pool).await;
    let params = NamespaceParameters {
        prefix: Some(whi.to_string().into()),
        namespace,
    };
    let big = serde_json::Value::String("x".repeat(1024 * 1024 + 1));
    let mut req = create_request("gt-schema");
    req.schema = Some(big.clone());
    let err = CatalogServer::create_generic_table(
        params.clone(),
        req,
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .expect_err("oversized schema must fail");
    assert_eq!(err.error.code, StatusCode::BAD_REQUEST);

    let mut req = create_request("gt-stats");
    req.statistics = Some(big);
    let err = CatalogServer::create_generic_table(params, req, ctx, random_request_metadata())
        .await
        .expect_err("oversized statistics must fail");
    assert_eq!(err.error.code, StatusCode::BAD_REQUEST);
}

#[sqlx::test]
async fn test_create_generic_table(pool: PgPool) {
    let (ctx, namespace, whi) = setup(pool).await;
    let prefix = whi.to_string();

    let result = CatalogServer::create_generic_table(
        NamespaceParameters {
            prefix: Some(prefix.into()),
            namespace: namespace.clone(),
        },
        create_request("my-gt"),
        ctx,
        random_request_metadata(),
    )
    .await
    .unwrap();

    assert_eq!(result.table.name, "my-gt");
    assert_eq!(
        result.table.format,
        GenericTableFormat::Unknown("lance".to_string())
    );
    assert_eq!(result.table.doc, Some("test doc".to_string()));
}

#[sqlx::test]
async fn test_create_duplicate_fails(pool: PgPool) {
    let (ctx, namespace, whi) = setup(pool).await;
    let prefix = whi.to_string();
    let params = NamespaceParameters {
        prefix: Some(prefix.into()),
        namespace: namespace.clone(),
    };

    CatalogServer::create_generic_table(
        params.clone(),
        create_request("dup-gt"),
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    let err = CatalogServer::create_generic_table(
        params,
        create_request("dup-gt"),
        ctx,
        random_request_metadata(),
    )
    .await
    .expect_err("duplicate should fail");

    assert_eq!(err.error.code, StatusCode::CONFLICT);
}

#[sqlx::test]
async fn test_create_empty_name_fails(pool: PgPool) {
    let (ctx, namespace, whi) = setup(pool).await;

    let err = CatalogServer::create_generic_table(
        NamespaceParameters {
            prefix: Some(whi.to_string().into()),
            namespace,
        },
        create_request(""),
        ctx,
        random_request_metadata(),
    )
    .await
    .expect_err("empty name should fail");

    assert_eq!(err.error.code, StatusCode::BAD_REQUEST);
}

#[sqlx::test]
async fn test_load_generic_table(pool: PgPool) {
    let (ctx, namespace, whi) = setup(pool).await;
    let prefix = whi.to_string();

    let created = CatalogServer::create_generic_table(
        NamespaceParameters {
            prefix: Some(prefix.clone().into()),
            namespace: namespace.clone(),
        },
        create_request("load-gt"),
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    let loaded = CatalogServer::load_generic_table(
        GenericTableParameters {
            prefix: Some(prefix.into()),
            namespace,
            table_name: "load-gt".to_string(),
        },
        ctx,
        DataAccessMode::ClientManaged,
        random_request_metadata(),
    )
    .await
    .unwrap();

    assert_eq!(loaded.table.name, "load-gt");
    assert_eq!(loaded.table.base_location, created.table.base_location);
}

#[sqlx::test]
async fn test_load_not_found(pool: PgPool) {
    let (ctx, namespace, whi) = setup(pool).await;

    let err = CatalogServer::load_generic_table(
        GenericTableParameters {
            prefix: Some(whi.to_string().into()),
            namespace,
            table_name: "does-not-exist".to_string(),
        },
        ctx,
        DataAccessMode::ClientManaged,
        random_request_metadata(),
    )
    .await
    .expect_err("should not exist");

    assert_eq!(err.error.code, StatusCode::NOT_FOUND);
}

#[sqlx::test]
async fn test_list_generic_tables(pool: PgPool) {
    let (ctx, namespace, whi) = setup(pool).await;
    let prefix = whi.to_string();
    let params = NamespaceParameters {
        prefix: Some(prefix.clone().into()),
        namespace: namespace.clone(),
    };

    let list = CatalogServer::list_generic_tables(
        params.clone(),
        ListGenericTablesQuery::default(),
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    assert!(list.identifiers.is_empty());

    CatalogServer::create_generic_table(
        params.clone(),
        create_request("gt-a"),
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    CatalogServer::create_generic_table(
        params.clone(),
        create_request("gt-b"),
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    let list = CatalogServer::list_generic_tables(
        params,
        ListGenericTablesQuery::default(),
        ctx,
        random_request_metadata(),
    )
    .await
    .unwrap();

    assert_eq!(list.identifiers.len(), 2);
    let names: Vec<&str> = list.identifiers.iter().map(|i| i.name.as_str()).collect();
    assert!(names.contains(&"gt-a"));
    assert!(names.contains(&"gt-b"));
}

#[sqlx::test]
async fn test_drop_generic_table(pool: PgPool) {
    let (ctx, namespace, whi) = setup(pool).await;
    let prefix = whi.to_string();
    let ns_params = NamespaceParameters {
        prefix: Some(prefix.clone().into()),
        namespace: namespace.clone(),
    };

    CatalogServer::create_generic_table(
        ns_params.clone(),
        create_request("drop-gt"),
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    CatalogServer::drop_generic_table(
        GenericTableParameters {
            prefix: Some(prefix.clone().into()),
            namespace: namespace.clone(),
            table_name: "drop-gt".to_string(),
        },
        DropParams {
            purge_requested: false,
            force: false,
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    let list = CatalogServer::list_generic_tables(
        ns_params,
        ListGenericTablesQuery::default(),
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    assert!(list.identifiers.is_empty());

    let err = CatalogServer::load_generic_table(
        GenericTableParameters {
            prefix: Some(prefix.into()),
            namespace,
            table_name: "drop-gt".to_string(),
        },
        ctx,
        DataAccessMode::ClientManaged,
        random_request_metadata(),
    )
    .await
    .expect_err("should be gone");
    assert_eq!(err.error.code, StatusCode::NOT_FOUND);
}

#[sqlx::test]
async fn test_drop_not_found(pool: PgPool) {
    let (ctx, namespace, whi) = setup(pool).await;

    let err = CatalogServer::drop_generic_table(
        GenericTableParameters {
            prefix: Some(whi.to_string().into()),
            namespace,
            table_name: "ghost".to_string(),
        },
        DropParams {
            purge_requested: false,
            force: false,
        },
        ctx,
        random_request_metadata(),
    )
    .await
    .expect_err("should not exist");

    assert_eq!(err.error.code, StatusCode::NOT_FOUND);
}

#[sqlx::test]
async fn test_create_generic_table_idempotent_replay(pool: PgPool) {
    let (ctx, namespace, whi) = setup(pool).await;
    let prefix = whi.to_string();
    let params = NamespaceParameters {
        prefix: Some(prefix.clone().into()),
        namespace: namespace.clone(),
    };

    let key = IdempotencyKey::parse(&uuid::Uuid::now_v7().to_string()).unwrap();
    let mut metadata = random_request_metadata();
    metadata.with_idempotency_key(key);

    let r1 = CatalogServer::create_generic_table(
        params.clone(),
        create_request("idem-gt"),
        ctx.clone(),
        metadata.clone(),
    )
    .await
    .unwrap();

    let r2 = CatalogServer::create_generic_table(
        params,
        create_request("idem-gt"),
        ctx.clone(),
        metadata,
    )
    .await
    .expect("replay with same idempotency key should succeed");

    assert_eq!(r1.table.name, r2.table.name);
    assert_eq!(r1.table.base_location, r2.table.base_location);

    let list = CatalogServer::list_generic_tables(
        NamespaceParameters {
            prefix: Some(prefix.into()),
            namespace,
        },
        ListGenericTablesQuery::default(),
        ctx,
        random_request_metadata(),
    )
    .await
    .unwrap();
    assert_eq!(
        list.identifiers.len(),
        1,
        "replay must not create a duplicate"
    );
}

#[sqlx::test]
async fn test_cannot_drop_protected_generic_table(pool: PgPool) {
    let (ctx, namespace, whi) = setup(pool).await;
    let prefix = whi.to_string();

    CatalogServer::create_generic_table(
        NamespaceParameters {
            prefix: Some(prefix.clone().into()),
            namespace: namespace.clone(),
        },
        create_request("protected-gt"),
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    let listed = CatalogServer::list_generic_tables(
        NamespaceParameters {
            prefix: Some(prefix.clone().into()),
            namespace: namespace.clone(),
        },
        ListGenericTablesQuery::default(),
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    let gt_id_value = listed
        .identifiers
        .iter()
        .find(|i| i.name == "protected-gt")
        .and_then(|i| i.id)
        .expect("generic table id");

    let mut t = <PostgresBackend as lakekeeper::service::CatalogStore>::Transaction::begin_write(
        ctx.v1_state.catalog.clone(),
    )
    .await
    .unwrap();
    PostgresBackend::set_tabular_protected(
        whi,
        TabularId::GenericTable(gt_id_value),
        true,
        t.transaction(),
    )
    .await
    .unwrap();
    t.commit().await.unwrap();

    let err = CatalogServer::drop_generic_table(
        GenericTableParameters {
            prefix: Some(prefix.clone().into()),
            namespace: namespace.clone(),
            table_name: "protected-gt".to_string(),
        },
        DropParams {
            purge_requested: false,
            force: false,
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .expect_err("protected drop without force must fail");
    assert_eq!(err.error.code, StatusCode::CONFLICT, "{err:?}");

    CatalogServer::drop_generic_table(
        GenericTableParameters {
            prefix: Some(prefix.into()),
            namespace,
            table_name: "protected-gt".to_string(),
        },
        DropParams {
            purge_requested: false,
            force: true,
        },
        ctx,
        random_request_metadata(),
    )
    .await
    .unwrap();
}

#[sqlx::test]
async fn test_rename_generic_table(pool: PgPool) {
    let (ctx, namespace, whi) = setup(pool).await;
    let prefix = whi.to_string();

    CatalogServer::create_generic_table(
        NamespaceParameters {
            prefix: Some(prefix.clone().into()),
            namespace: namespace.clone(),
        },
        create_request("orig"),
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    CatalogServer::rename_generic_table(
        Some(prefix.clone().into()),
        RenameGenericTableRequest {
            source: rename_target(&namespace, "orig"),
            destination: rename_target(&namespace, "renamed"),
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    let err = CatalogServer::load_generic_table(
        GenericTableParameters {
            prefix: Some(prefix.clone().into()),
            namespace: namespace.clone(),
            table_name: "orig".to_string(),
        },
        ctx.clone(),
        DataAccessMode::ClientManaged,
        random_request_metadata(),
    )
    .await
    .expect_err("source should be gone");
    assert_eq!(err.error.code, StatusCode::NOT_FOUND);

    let loaded = CatalogServer::load_generic_table(
        GenericTableParameters {
            prefix: Some(prefix.into()),
            namespace,
            table_name: "renamed".to_string(),
        },
        ctx,
        DataAccessMode::ClientManaged,
        random_request_metadata(),
    )
    .await
    .unwrap();
    assert_eq!(loaded.table.name, "renamed");
}

#[sqlx::test]
async fn test_rename_generic_table_cross_namespace(pool: PgPool) {
    let (ctx, source_ns, whi) = setup(pool).await;
    let prefix = whi.to_string();
    let dest_ns = initialize_namespace(
        ctx.v1_state.catalog.clone(),
        whi,
        &NamespaceIdent::new(uuid::Uuid::now_v7().to_string()),
        None,
    )
    .await
    .namespace_ident()
    .clone();

    CatalogServer::create_generic_table(
        NamespaceParameters {
            prefix: Some(prefix.clone().into()),
            namespace: source_ns.clone(),
        },
        create_request("movable"),
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    CatalogServer::rename_generic_table(
        Some(prefix.clone().into()),
        RenameGenericTableRequest {
            source: rename_target(&source_ns, "movable"),
            destination: rename_target(&dest_ns, "movable"),
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    let loaded = CatalogServer::load_generic_table(
        GenericTableParameters {
            prefix: Some(prefix.into()),
            namespace: dest_ns,
            table_name: "movable".to_string(),
        },
        ctx,
        DataAccessMode::ClientManaged,
        random_request_metadata(),
    )
    .await
    .unwrap();
    assert_eq!(loaded.table.name, "movable");
}

#[sqlx::test]
async fn test_rename_generic_table_source_missing(pool: PgPool) {
    let (ctx, namespace, whi) = setup(pool).await;

    let err = CatalogServer::rename_generic_table(
        Some(whi.to_string().into()),
        RenameGenericTableRequest {
            source: rename_target(&namespace, "ghost"),
            destination: rename_target(&namespace, "renamed"),
        },
        ctx,
        random_request_metadata(),
    )
    .await
    .expect_err("missing source must fail");
    assert_eq!(err.error.code, StatusCode::NOT_FOUND);
}

#[sqlx::test]
async fn test_rename_generic_table_idempotent_replay(pool: PgPool) {
    let (ctx, namespace, whi) = setup(pool).await;
    let prefix = whi.to_string();

    CatalogServer::create_generic_table(
        NamespaceParameters {
            prefix: Some(prefix.clone().into()),
            namespace: namespace.clone(),
        },
        create_request("idem-src"),
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    let key = IdempotencyKey::parse(&uuid::Uuid::now_v7().to_string()).unwrap();
    let mut metadata = random_request_metadata();
    metadata.with_idempotency_key(key);

    let req = RenameGenericTableRequest {
        source: rename_target(&namespace, "idem-src"),
        destination: rename_target(&namespace, "idem-dst"),
    };

    CatalogServer::rename_generic_table(
        Some(prefix.clone().into()),
        req.clone(),
        ctx.clone(),
        metadata.clone(),
    )
    .await
    .unwrap();

    CatalogServer::rename_generic_table(Some(prefix.into()), req, ctx, metadata)
        .await
        .expect("idempotent replay should succeed");
}

#[sqlx::test]
async fn test_list_generic_tables_pagination(pool: PgPool) {
    let (ctx, namespace, whi) = setup(pool).await;
    let prefix = whi.to_string();
    let params = NamespaceParameters {
        prefix: Some(prefix.clone().into()),
        namespace: namespace.clone(),
    };

    let total: usize = 7;
    for i in 0..total {
        CatalogServer::create_generic_table(
            params.clone(),
            create_request(&format!("page-gt-{i}")),
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();
    }

    let mut collected: Vec<String> = Vec::new();
    let mut page_token: Option<String> = None;
    let mut pages: u32 = 0;
    loop {
        let res = CatalogServer::list_generic_tables(
            params.clone(),
            ListGenericTablesQuery {
                page_size: Some(3),
                page_token: page_token.clone(),
            },
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();
        pages += 1;
        assert!(
            pages <= 10,
            "pagination did not terminate (got {pages} pages)"
        );

        for ident in &res.identifiers {
            collected.push(ident.name.clone());
        }
        match res.next_page_token {
            Some(token) if !token.is_empty() && !res.identifiers.is_empty() => {
                page_token = Some(token);
            }
            _ => break,
        }
    }

    collected.sort();
    let mut expected: Vec<String> = (0..total).map(|i| format!("page-gt-{i}")).collect();
    expected.sort();
    assert_eq!(
        collected, expected,
        "all rows must be returned exactly once"
    );
    assert!(
        pages >= 2,
        "page_size=3 over {total} rows should span >=2 pages"
    );
}
