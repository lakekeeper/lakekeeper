//! Regression tests for array-valued query filters on the Management API —
//! see <https://github.com/lakekeeper/lakekeeper/issues/1751>.
//!
//! Unlike the rest of this crate, these tests drive the **real axum router**
//! over HTTP against a real Postgres backend. That matters: the defect these
//! guard against lived in the `Query` extractor, which service-layer tests
//! never reach. Every other test here calls `PostgresBackend::*` or
//! `ApiServer::*` with Rust values and so never deserializes a query string —
//! which is exactly why the bug shipped unnoticed.
//!
//! `GET /management/v1/warehouse` declares `warehouseStatus` as an array
//! parameter and sets no `style`/`explode`, so per the OAS 3.0 defaults every
//! generated client emits the repeated-key form `?warehouseStatus=a&warehouseStatus=b`.
//! The server must accept exactly that. It previously could not: the field is
//! `Option<Vec<WarehouseStatus>>`, and `axum::extract::Query` is backed by
//! `serde_urlencoded`, which cannot deserialize a `Vec<T>` field from *any*
//! query-string form. Every documented encoding returned 400, and bracket
//! notation was silently discarded — returning rows the caller had excluded.

use http_body_util::BodyExt as _;
use lakekeeper::{
    WarehouseId,
    api::{
        ApiContext, RequestMetadata,
        management::v1::{
            ApiServer,
            warehouse::{CreateWarehouseRequest, Service as _, TabularDeleteProfile},
        },
    },
    axum::{Router, body::Body},
    service::{State, authz::AllowAllAuthorizer},
};
use lakekeeper_storage_postgres::{PostgresBackend, SecretsState};
use sqlx::PgPool;
use tower::ServiceExt as _;
use uuid::Uuid;

type Ctx = ApiContext<State<AllowAllAuthorizer, PostgresBackend, SecretsState>>;

/// One active and one deactivated warehouse, so a status filter has something
/// to actually discriminate between.
async fn setup(pool: PgPool) -> (Ctx, WarehouseId, WarehouseId) {
    let (ctx, warehouse) = lakekeeper_integration_tests::setup_simple(
        pool,
        lakekeeper_integration_tests::memory_io_profile(),
        None,
        AllowAllAuthorizer::default(),
        TabularDeleteProfile::Hard {},
        None,
    )
    .await;

    let inactive = ApiServer::create_warehouse(
        CreateWarehouseRequest::builder()
            .warehouse_name(format!("inactive-{}", Uuid::now_v7()))
            .storage_profile(lakekeeper_integration_tests::memory_io_profile())
            .delete_profile(TabularDeleteProfile::Hard {})
            .build(),
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap()
    .warehouse_id();

    ApiServer::deactivate_warehouse(
        inactive,
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    (ctx, warehouse.warehouse_id, inactive)
}

/// Issues a real request against the real management router and returns the
/// status plus the warehouse ids in the response body.
async fn list_warehouses(ctx: &Ctx, query: &str) -> (u16, Vec<String>) {
    let router = Router::new()
        .merge(
            ApiServer::<PostgresBackend, AllowAllAuthorizer, SecretsState>::new_v1_router(
                &AllowAllAuthorizer::default(),
            ),
        )
        .with_state(ctx.clone());

    let mut request = http::Request::builder()
        .uri(format!("/warehouse{query}"))
        .body(Body::empty())
        .unwrap();
    request
        .extensions_mut()
        .insert(RequestMetadata::new_unauthenticated());

    let response = router.oneshot(request).await.unwrap();
    let status = response.status().as_u16();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();

    if status != 200 {
        return (status, Vec::new());
    }
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let mut ids: Vec<String> = body["warehouses"]
        .as_array()
        .unwrap()
        .iter()
        .map(|w| w["id"].as_str().unwrap().to_owned())
        .collect();
    ids.sort();
    (status, ids)
}

fn sorted(ids: &[WarehouseId]) -> Vec<String> {
    let mut ids: Vec<String> = ids.iter().map(ToString::to_string).collect();
    ids.sort();
    ids
}

/// Baseline: with no filter, only active warehouses are listed.
#[sqlx::test]
async fn unfiltered_listing_returns_only_active_warehouses(pool: PgPool) {
    let (ctx, active, _inactive) = setup(pool).await;
    assert_eq!(list_warehouses(&ctx, "").await, (200, sorted(&[active])));
}

/// **The regression.** The OAS 3.0 default encoding (`style: form`,
/// `explode: true`) is what every generated client emits from this spec. It
/// must be accepted *and* applied — a single value, and several repeated keys
/// forming a union. This returned 400 before the `Query` extractor was swapped
/// to `axum_extra`'s.
#[sqlx::test]
async fn the_oas_default_encoding_is_accepted_and_applied(pool: PgPool) {
    let (ctx, active, inactive) = setup(pool).await;

    assert_eq!(
        list_warehouses(&ctx, "?warehouseStatus=active").await,
        (200, sorted(&[active])),
        "single value must filter to the active warehouse"
    );
    assert_eq!(
        list_warehouses(&ctx, "?warehouseStatus=inactive").await,
        (200, sorted(&[inactive])),
        "single value must filter to the inactive warehouse"
    );
    assert_eq!(
        list_warehouses(&ctx, "?warehouseStatus=active&warehouseStatus=inactive").await,
        (200, sorted(&[active, inactive])),
        "repeated keys must be collected into a union, not rejected or truncated"
    );
}

/// A filter that matches nothing must return nothing. This is the assertion
/// that distinguishes "the filter was applied" from "the filter was silently
/// dropped": a dropped filter yields the default listing instead of an empty
/// one.
#[sqlx::test]
async fn a_filter_matching_nothing_returns_an_empty_list(pool: PgPool) {
    let (ctx, _active, inactive) = setup(pool).await;

    ApiServer::activate_warehouse(
        inactive,
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    assert_eq!(
        list_warehouses(&ctx, "?warehouseStatus=inactive").await,
        (200, Vec::new()),
        "no warehouse is inactive, so the response must be empty"
    );
}

/// An unparseable element must fail loudly rather than be dropped.
#[sqlx::test]
async fn an_unknown_status_is_rejected(pool: PgPool) {
    let (ctx, _active, _inactive) = setup(pool).await;
    assert_eq!(
        list_warehouses(&ctx, "?warehouseStatus=not-a-status")
            .await
            .0,
        400
    );
}

/// An explicitly empty value means "not supplied" rather than "an empty list",
/// so it must behave like an omitted parameter instead of erroring.
#[sqlx::test]
async fn an_empty_value_is_treated_as_absent(pool: PgPool) {
    let (ctx, active, _inactive) = setup(pool).await;
    assert_eq!(
        list_warehouses(&ctx, "?warehouseStatus=").await,
        (200, sorted(&[active]))
    );
}

/// Bracket notation (`style: deepObject`, proposed in #1751 and still emitted
/// by the `go-lakekeeper` preprocessor) is *not* a supported encoding, and the
/// fix deliberately does not make it one: the bracketed key matches no field,
/// so the filter is discarded and the caller silently receives rows it asked to
/// exclude. This is pinned to document why the spec must never advertise
/// `deepObject` — and to prove the fix left this path untouched, so existing
/// clients see no behaviour change.
#[sqlx::test]
async fn bracket_notation_is_still_silently_ignored(pool: PgPool) {
    let (ctx, active, _inactive) = setup(pool).await;

    let filtered = list_warehouses(&ctx, "?warehouseStatus%5B0%5D=inactive").await;

    assert_eq!(
        filtered,
        (200, sorted(&[active])),
        "asked for inactive warehouses and got the active one back"
    );
    assert_eq!(
        filtered,
        list_warehouses(&ctx, "").await,
        "bracket notation is indistinguishable from sending no filter at all"
    );
}
