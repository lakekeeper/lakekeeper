//! End-to-end proof that array-valued query filters on the Management API are
//! unusable — see <https://github.com/lakekeeper/lakekeeper/issues/1751>.
//!
//! Unlike the rest of this crate, these tests drive the **real axum router**
//! over HTTP against a real Postgres backend, because the defect lives in the
//! `Query` extractor and is therefore invisible to service-layer tests. Every
//! other test here calls `PostgresBackend::*` directly and so never
//! deserializes a query string at all — which is why this shipped.
//!
//! `GET /management/v1/warehouse` declares `warehouseStatus` as an array
//! parameter with no `style`/`explode`, so per the OAS 3.0 defaults a client
//! sends `?warehouseStatus=a&warehouseStatus=b`. The field is
//! `Option<Vec<WarehouseStatus>>` behind `axum::extract::Query`, which is backed
//! by `serde_urlencoded` — a crate that cannot deserialize a `Vec<T>` field
//! from any query-string form whatsoever.
//!
//! These tests assert the broken status quo and must be inverted when the
//! extractor is fixed.

use http_body_util::BodyExt as _;
use lakekeeper::{
    api::{
        ApiContext, RequestMetadata,
        management::v1::{ApiServer, warehouse::TabularDeleteProfile},
    },
    axum::{Router, body::Body},
    service::{State, authz::AllowAllAuthorizer},
};
use lakekeeper_storage_postgres::{PostgresBackend, SecretsState};
use sqlx::PgPool;
use tower::ServiceExt as _;

type Ctx = ApiContext<State<AllowAllAuthorizer, PostgresBackend, SecretsState>>;

async fn setup(pool: PgPool) -> Ctx {
    let (ctx, _warehouse) = lakekeeper_integration_tests::setup_simple(
        pool,
        lakekeeper_integration_tests::memory_io_profile(),
        None,
        AllowAllAuthorizer::default(),
        TabularDeleteProfile::Hard {},
        None,
    )
    .await;
    ctx
}

/// Issues a real request against the real management router and returns
/// `(status, number of warehouses in the response)`.
async fn list_warehouses(ctx: &Ctx, query: &str) -> (u16, Option<usize>) {
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
        return (status, None);
    }
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let count = body["warehouses"].as_array().map(Vec::len);
    (status, count)
}

/// Baseline: the fixture creates exactly one warehouse, and it is active.
#[sqlx::test]
async fn unfiltered_listing_returns_the_active_warehouse(pool: PgPool) {
    let ctx = setup(pool).await;
    assert_eq!(list_warehouses(&ctx, "").await, (200, Some(1)));
}

/// The OAS 3.0 default encoding — what every generated client emits from this
/// spec — is rejected outright, for one value and for several.
#[sqlx::test]
async fn the_oas_default_encoding_is_rejected(pool: PgPool) {
    let ctx = setup(pool).await;
    assert_eq!(
        list_warehouses(&ctx, "?warehouseStatus=active").await,
        (400, None)
    );
    assert_eq!(
        list_warehouses(&ctx, "?warehouseStatus=active&warehouseStatus=inactive").await,
        (400, None)
    );
}

/// The dangerous case. `deepObject` is the encoding proposed in #1751 and the
/// one the `go-lakekeeper` preprocessor already forces onto every client. It is
/// not rejected: the bracketed key matches no field, the filter is discarded,
/// and the caller receives rows it explicitly excluded.
///
/// Here we ask for `inactive` warehouses only. The fixture has none — yet the
/// response contains the active one.
#[sqlx::test]
async fn deep_object_silently_returns_rows_the_caller_excluded(pool: PgPool) {
    let ctx = setup(pool).await;

    let (status, count) = list_warehouses(&ctx, "?warehouseStatus%5B0%5D=inactive").await;

    assert_eq!(status, 200, "deepObject is accepted rather than rejected");
    assert_eq!(
        count,
        Some(1),
        "asked for inactive warehouses and got the active one back: the filter \
         was silently dropped instead of applied"
    );
}

/// Bracket notation is indistinguishable from sending no filter at all — the
/// clearest statement of why the spec must not advertise `deepObject`.
#[sqlx::test]
async fn deep_object_is_identical_to_sending_no_filter(pool: PgPool) {
    let ctx = setup(pool).await;
    let filtered = list_warehouses(&ctx, "?warehouseStatus%5B0%5D=inactive").await;
    let unfiltered = list_warehouses(&ctx, "").await;

    assert_eq!(filtered, unfiltered);
}
