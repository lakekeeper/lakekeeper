use lakekeeper::{
    ProjectId,
    api::{
        RequestMetadata,
        iceberg::v1::{
            ErrorModel,
            config::{GetConfigQueryParams, Service as _},
        },
        management::v1::warehouse::TabularDeleteProfile,
    },
    server::CatalogServer,
    service::{WarehouseNameNotFound, authz::tests::HidingAuthorizer},
};
use lakekeeper_integration_tests::{memory_io_profile, setup};
use sqlx::PgPool;

fn config_query(project: &ProjectId, warehouse_name: &str) -> GetConfigQueryParams {
    GetConfigQueryParams {
        warehouse: Some(format!("{project}/{warehouse_name}")),
    }
}

#[sqlx::test]
async fn test_get_config_visible_warehouse(pool: PgPool) {
    let (ctx, warehouse) = setup(
        pool,
        memory_io_profile(),
        None,
        HidingAuthorizer::new(),
        TabularDeleteProfile::Hard {},
        None,
        1,
        None,
    )
    .await;

    let config = CatalogServer::get_config(
        config_query(&warehouse.project_id, &warehouse.warehouse_name),
        ctx,
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();
    assert_eq!(
        config.overrides.get("uri"),
        Some(&RequestMetadata::new_unauthenticated().base_uri_catalog())
    );
}

/// A warehouse the caller cannot see must produce the *same* error as a
/// warehouse name that does not exist — otherwise the config endpoint leaks
/// warehouse-name existence (and the resolved UUID) to unauthorized callers.
/// See issue #1780: the project-level `list_warehouses` gate that previously
/// blocked this was removed because it forced an OpenFGA fan-out across every
/// warehouse in the project.
#[sqlx::test]
async fn test_get_config_hidden_warehouse_indistinguishable_from_missing(pool: PgPool) {
    let authz = HidingAuthorizer::new();
    let (ctx, warehouse) = setup(
        pool,
        memory_io_profile(),
        None,
        authz.clone(),
        TabularDeleteProfile::Hard {},
        None,
        1,
        None,
    )
    .await;

    // Hide the (existing) warehouse from the authorizer.
    authz.hide(&format!("warehouse:{}", warehouse.warehouse_id));

    // Probe the existing-but-hidden warehouse by its real name.
    let hidden_err = CatalogServer::get_config(
        config_query(&warehouse.project_id, &warehouse.warehouse_name),
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .expect_err("hidden warehouse must not be readable")
    .error;

    // The response for a hidden warehouse must be byte-identical (modulo the
    // random `error_id`) to the canonical "name does not exist" error for the
    // *same* name — i.e. the response is a pure function of the client-supplied
    // input, never of whether the warehouse actually exists or its UUID.
    let canonical_missing =
        ErrorModel::from(WarehouseNameNotFound::new(warehouse.warehouse_name.clone()));
    assert_eq!(hidden_err.code, 404);
    assert_eq!(hidden_err.r#type, canonical_missing.r#type);
    assert_eq!(hidden_err.message, canonical_missing.message);
    // The resolved warehouse UUID must never appear in the masked response.
    assert!(
        !hidden_err
            .message
            .contains(&warehouse.warehouse_id.to_string()),
        "masked error leaked the warehouse UUID: {}",
        hidden_err.message
    );

    // Sanity-check the other branch: a name that genuinely does not exist
    // produces the same error shape (type + code), echoing only its own input.
    let missing_err = CatalogServer::get_config(
        config_query(&warehouse.project_id, "does-not-exist"),
        ctx,
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .expect_err("missing warehouse must error")
    .error;
    assert_eq!(missing_err.code, 404);
    assert_eq!(missing_err.r#type, hidden_err.r#type);
}
