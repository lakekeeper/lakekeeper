//! A management API extension served through the full router.
//!
//! A test binary of its own: a process installs one set of extension endpoints, and every
//! router that builds here installs the set of [`TestExtension`].

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use http::{Method, Request, StatusCode, header};
use http_body_util::BodyExt as _;
use lakekeeper::{
    DEFAULT_PROJECT_ID, WarehouseId,
    api::{
        ApiContext, RequestMetadata,
        management::v1::{
            ApiServer,
            extension::{
                ArcManagementApiExtension, ExtensionEndpoint, ExtensionRoutes,
                ManagementApiExtension, ManagementApiExtensionError,
                validate_management_api_extensions,
            },
            project::{
                EndpointStatistic, GetEndpointStatisticsRequest, Service as _, TimeWindowSelector,
                WarehouseFilter,
            },
            warehouse::TabularDeleteProfile,
        },
        router::{RouterArgs, new_full_router},
    },
    axum::{Extension, Router, body::Body, response::Response},
    limes,
    service::{
        EndpointStatisticsTrackerTx, State,
        admission::{
            AdmissionContext, AdmissionGate, AdmissionGates, AdmissionRejection, GateDecision,
        },
        authz::{AllowAllAuthorizer, ConfiguredInstanceAdmins},
        endpoint_statistics::{EndpointStatisticsMessage, EndpointStatisticsTracker, FlushMode},
        health::ServiceHealthProvider,
    },
};
use lakekeeper_storage_postgres::{PostgresBackend, PostgresStatisticsSink, SecretsState};
use sqlx::PgPool;
use tower::ServiceExt as _;

type Ctx = ApiContext<State<AllowAllAuthorizer, PostgresBackend, SecretsState>>;
type Routes = ExtensionRoutes<AllowAllAuthorizer, PostgresBackend, SecretsState>;
type ArcExtension = ArcManagementApiExtension<AllowAllAuthorizer, PostgresBackend, SecretsState>;

const TOKEN: &str = "management-extension-test-token";

static TABLE_HEALTH: ExtensionEndpoint = ExtensionEndpoint::new(
    "ext-test-health",
    Method::GET,
    "/management/v1/warehouse/{warehouse_id}/ext-test/health",
);
static REMOVED: ExtensionEndpoint = ExtensionEndpoint::new(
    "ext-test-removed",
    Method::POST,
    "/management/v1/warehouse/{warehouse_id}/ext-test/removed",
);

/// Answers whether the request reached it authenticated.
async fn table_health(Extension(metadata): Extension<RequestMetadata>) -> &'static str {
    if metadata.user_id().is_some() {
        "authenticated"
    } else {
        "anonymous"
    }
}

#[derive(Debug)]
struct TestExtension;

impl ManagementApiExtension<AllowAllAuthorizer, PostgresBackend, SecretsState> for TestExtension {
    fn routes(&self, routes: Routes) -> Routes {
        routes.route(&TABLE_HEALTH, table_health).retired(&REMOVED)
    }

    #[cfg(feature = "open-api")]
    fn api_doc(&self) -> utoipa::openapi::OpenApi {
        use utoipa::openapi::{
            HttpMethod, OpenApiBuilder, PathItem, PathsBuilder, path::OperationBuilder,
        };
        OpenApiBuilder::new()
            .paths(
                PathsBuilder::new().path(
                    TABLE_HEALTH.path(),
                    PathItem::new(
                        HttpMethod::Get,
                        OperationBuilder::new()
                            .operation_id(Some("ext_test_health"))
                            .build(),
                    ),
                ),
            )
            .build()
    }
}

/// Accepts exactly [`TOKEN`].
#[derive(Debug, Clone)]
struct FixedTokenAuthenticator {
    idp_id: String,
}

impl limes::Authenticator for FixedTokenAuthenticator {
    async fn authenticate(
        &self,
        token: &str,
        _introspection: &limes::introspect::IntrospectionResult,
    ) -> limes::error::Result<limes::Authentication> {
        if token != TOKEN {
            return Err(limes::error::Error::Unauthenticated {
                reason: "unknown token".to_string(),
            });
        }
        Ok(limes::Authentication::builder()
            .token_header(None)
            .claims(serde_json::json!({}))
            .subject(limes::Subject::new(
                Some(self.idp_id.clone()),
                "extension-tester".to_string(),
            ))
            .name(None)
            .email(None)
            .principal_type(None)
            .build())
    }

    fn can_handle_token(
        &self,
        _token: &str,
        _introspection: &limes::introspect::IntrospectionResult,
    ) -> bool {
        true
    }

    fn idp_id(&self) -> Option<&String> {
        Some(&self.idp_id)
    }
}

#[derive(Debug)]
struct DenyGate;

#[async_trait]
impl AdmissionGate for DenyGate {
    fn name(&self) -> &'static str {
        "deny"
    }

    async fn admit(&self, _: AdmissionContext<'_>) -> Result<GateDecision, AdmissionRejection> {
        Err(AdmissionRejection::forbidden(
            "not admitted",
            "ExtensionTestDenied",
        ))
    }
}

struct Harness {
    ctx: Ctx,
    pool: PgPool,
    warehouse_id: WarehouseId,
    tx: EndpointStatisticsTrackerTx,
    tracker: tokio::task::JoinHandle<()>,
}

impl Harness {
    async fn new(pool: PgPool) -> Self {
        let (ctx, warehouse) = lakekeeper_integration_tests::setup(
            pool.clone(),
            lakekeeper_integration_tests::memory_io_profile(),
            None,
            AllowAllAuthorizer::default(),
            TabularDeleteProfile::Hard {},
            None,
            1,
            None,
        )
        .await;
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        let tracker = EndpointStatisticsTracker::new(
            rx,
            vec![Arc::new(PostgresStatisticsSink::new(
                pool.clone(),
                pool.clone(),
            ))],
            Duration::from_secs(3600),
            FlushMode::Manual,
        );
        Self {
            ctx,
            pool,
            warehouse_id: warehouse.warehouse_id,
            tx: EndpointStatisticsTrackerTx::new(tx),
            tracker: tokio::task::spawn(tracker.run()),
        }
    }

    async fn router(
        &self,
        admission_gates: AdmissionGates,
        management_api_extensions: Vec<ArcExtension>,
    ) -> anyhow::Result<Router> {
        new_full_router::<_, _, _, _>(RouterArgs {
            authenticator: Some(FixedTokenAuthenticator {
                idp_id: "oidc".to_string(),
            }),
            state: self.ctx.clone(),
            service_health_provider: ServiceHealthProvider::new(vec![], 60),
            cors_origins: None,
            metrics_layer: None,
            endpoint_statistics_tracker_tx: self.tx.clone(),
            instance_admin_membership: Arc::new(ConfiguredInstanceAdmins::from_config()),
            admission_gates,
            management_api_extensions,
        })
        .await
    }

    async fn call(&self, router: &Router, method: Method, token: Option<&str>) -> Response {
        let mut request = Request::builder()
            .method(method)
            .uri(format!(
                "/management/v1/warehouse/{}/ext-test/health",
                self.warehouse_id
            ))
            .header(header::HOST, "localhost");
        if let Some(token) = token {
            request = request.header(header::AUTHORIZATION, format!("Bearer {token}"));
        }
        router
            .clone()
            .oneshot(request.body(Body::empty()).unwrap())
            .await
            .unwrap()
    }

    /// Stops the tracker, which writes what it counted.
    async fn flush(self) -> (Ctx, PgPool, WarehouseId) {
        self.tx
            .send(EndpointStatisticsMessage::Shutdown)
            .await
            .unwrap();
        self.tracker.await.unwrap();
        (self.ctx, self.pool, self.warehouse_id)
    }
}

fn test_extension() -> Vec<ArcExtension> {
    vec![Arc::new(TestExtension)]
}

/// `(name, status code, count)` of every stored statistics row of the default project.
async fn stored_rows(pool: &PgPool) -> Vec<(String, i32, i64)> {
    sqlx::query_as(
        "SELECT matched_path, status_code, count FROM endpoint_statistics
         WHERE project_id = $1 ORDER BY matched_path, status_code",
    )
    .bind(DEFAULT_PROJECT_ID.clone().unwrap().as_str())
    .fetch_all(pool)
    .await
    .unwrap()
}

async fn listed(ctx: Ctx) -> Vec<EndpointStatistic> {
    ApiServer::get_endpoint_statistics(
        ctx,
        GetEndpointStatisticsRequest {
            warehouse: WarehouseFilter::All,
            status_codes: None,
            range_specifier: Some(TimeWindowSelector::Window {
                end: chrono::Utc::now(),
                interval: chrono::Duration::days(1),
            }),
        },
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap()
    .called_endpoints
    .into_iter()
    .flatten()
    .collect()
}

async fn body_text(response: Response) -> String {
    let body = response.into_body().collect().await.unwrap().to_bytes();
    String::from_utf8(body.to_vec()).unwrap()
}

#[sqlx::test]
async fn an_extension_route_is_authenticated_and_counted(pool: PgPool) {
    let harness = Harness::new(pool).await;
    let router = harness
        .router(AdmissionGates::default(), test_extension())
        .await
        .unwrap();

    let response = harness.call(&router, Method::GET, None).await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    let response = harness.call(&router, Method::GET, Some(TOKEN)).await;
    assert_eq!(response.status(), StatusCode::OK);
    let request_id = response.headers()["x-request-id"].to_str().unwrap();
    assert!(uuid::Uuid::parse_str(request_id).is_ok(), "{request_id}");
    assert_eq!(body_text(response).await, "authenticated");

    let (ctx, pool, warehouse_id) = harness.flush().await;
    assert_eq!(
        stored_rows(&pool).await,
        vec![("ext-test-health".to_string(), 200, 1)]
    );
    let listed = listed(ctx).await;
    assert_eq!(listed.len(), 1, "{listed:?}");
    assert_eq!(
        listed[0].http_route,
        "GET /management/v1/warehouse/{warehouse_id}/ext-test/health"
    );
    assert_eq!(listed[0].status_code, 200);
    assert_eq!(listed[0].count, 1);
    assert_eq!(listed[0].warehouse_id, Some(*warehouse_id));
}

#[sqlx::test]
async fn a_head_request_is_counted_under_the_get_endpoint(pool: PgPool) {
    let harness = Harness::new(pool).await;
    let router = harness
        .router(AdmissionGates::default(), test_extension())
        .await
        .unwrap();

    let response = harness.call(&router, Method::HEAD, Some(TOKEN)).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(body_text(response).await, "");

    let (_ctx, pool, _warehouse_id) = harness.flush().await;
    assert_eq!(
        stored_rows(&pool).await,
        vec![("ext-test-health".to_string(), 200, 1)]
    );
}

#[sqlx::test]
async fn a_retired_endpoint_stays_listed(pool: PgPool) {
    let harness = Harness::new(pool).await;
    // Building the router installs the extension endpoints, retired ones included.
    let _router = harness
        .router(AdmissionGates::default(), test_extension())
        .await
        .unwrap();
    let (ctx, pool, _warehouse_id) = harness.flush().await;

    sqlx::query(
        "INSERT INTO endpoint_statistics (project_id, matched_path, status_code, count)
         VALUES ($1, 'ext-test-removed', 200, 3)",
    )
    .bind(DEFAULT_PROJECT_ID.clone().unwrap().as_str())
    .execute(&pool)
    .await
    .unwrap();

    let listed = listed(ctx).await;
    assert_eq!(listed.len(), 1, "{listed:?}");
    assert_eq!(
        listed[0].http_route,
        "POST /management/v1/warehouse/{warehouse_id}/ext-test/removed"
    );
    assert_eq!(listed[0].count, 3);
}

#[sqlx::test]
async fn admission_gates_apply_to_extension_routes(pool: PgPool) {
    let harness = Harness::new(pool).await;
    let router = harness
        .router(
            AdmissionGates::new(vec![Arc::new(DenyGate)]),
            test_extension(),
        )
        .await
        .unwrap();

    let response = harness.call(&router, Method::GET, Some(TOKEN)).await;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let body: serde_json::Value = serde_json::from_str(&body_text(response).await).unwrap();
    assert_eq!(body["error"]["type"], "ExtensionTestDenied");

    let (_ctx, pool, _warehouse_id) = harness.flush().await;
    assert_eq!(stored_rows(&pool).await, vec![]);
}

#[cfg(feature = "open-api")]
#[sqlx::test]
async fn the_management_document_includes_extension_endpoints(pool: PgPool) {
    let harness = Harness::new(pool).await;
    let router = harness
        .router(AdmissionGates::default(), test_extension())
        .await
        .unwrap();

    let response = router
        .oneshot(
            Request::builder()
                .uri("/api-docs/management/v1/openapi.json")
                .header(header::HOST, "localhost")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let doc: serde_json::Value = serde_json::from_str(&body_text(response).await).unwrap();
    assert_eq!(
        doc["paths"][TABLE_HEALTH.path()]["get"]["operationId"],
        "ext_test_health"
    );
    harness.flush().await;
}

#[derive(Debug)]
struct OtherExtension;

static OTHER: ExtensionEndpoint = ExtensionEndpoint::new(
    "ext-test-other",
    Method::GET,
    "/management/v1/warehouse/{warehouse_id}/ext-test/other",
);

impl ManagementApiExtension<AllowAllAuthorizer, PostgresBackend, SecretsState> for OtherExtension {
    fn routes(&self, routes: Routes) -> Routes {
        routes.retired(&OTHER)
    }

    #[cfg(feature = "open-api")]
    fn api_doc(&self) -> utoipa::openapi::OpenApi {
        utoipa::openapi::OpenApi::default()
    }
}

#[derive(Debug)]
struct InvalidExtension;

static UNPREFIXED: ExtensionEndpoint = ExtensionEndpoint::new(
    "test-unprefixed",
    Method::GET,
    "/management/v1/warehouse/{warehouse_id}/ext-test/unprefixed",
);

impl ManagementApiExtension<AllowAllAuthorizer, PostgresBackend, SecretsState>
    for InvalidExtension
{
    fn routes(&self, routes: Routes) -> Routes {
        routes.retired(&UNPREFIXED)
    }

    #[cfg(feature = "open-api")]
    fn api_doc(&self) -> utoipa::openapi::OpenApi {
        utoipa::openapi::OpenApi::default()
    }
}

/// An extension crate can run the server's checks in a unit test.
#[test]
fn extensions_validate_without_a_server() {
    assert_eq!(
        validate_management_api_extensions(&test_extension(), &[], &[]),
        Ok(())
    );
    assert_eq!(
        validate_management_api_extensions::<AllowAllAuthorizer, PostgresBackend, SecretsState>(
            &[Arc::new(InvalidExtension)],
            &[],
            &[]
        ),
        Err(ManagementApiExtensionError::InvalidName {
            name: "test-unprefixed"
        })
    );
}

#[sqlx::test]
async fn the_router_refuses_invalid_or_different_extensions(pool: PgPool) {
    let harness = Harness::new(pool).await;
    let _router = harness
        .router(AdmissionGates::default(), test_extension())
        .await
        .unwrap();

    let error = harness
        .router(AdmissionGates::default(), vec![Arc::new(InvalidExtension)])
        .await
        .unwrap_err();
    assert_eq!(
        error.downcast_ref::<ManagementApiExtensionError>(),
        Some(&ManagementApiExtensionError::InvalidName {
            name: "test-unprefixed"
        })
    );

    let error = harness
        .router(AdmissionGates::default(), vec![Arc::new(OtherExtension)])
        .await
        .unwrap_err();
    assert_eq!(
        error.downcast_ref::<ManagementApiExtensionError>(),
        Some(&ManagementApiExtensionError::DifferentEndpointsInstalled)
    );

    // No extensions install nothing, so the installed set stays.
    let _router = harness
        .router(AdmissionGates::default(), vec![])
        .await
        .unwrap();
    harness.flush().await;
}
