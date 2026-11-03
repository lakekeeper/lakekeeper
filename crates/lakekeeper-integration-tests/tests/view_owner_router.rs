//! The full router decides whether the view owner property rules apply: they apply if it is
//! built with an authenticator, whatever the state it gets says.

use std::{collections::HashSet, sync::Arc};

use http::{Method, Request, StatusCode, header};
use http_body_util::BodyExt as _;
use lakekeeper::{
    api::router::{RouterArgs, new_full_router},
    axum::{Router, body::Body},
    limes,
    service::{
        AdmissionGates, EndpointStatisticsTrackerTx, ServerAuthentication,
        authz::ConfiguredInstanceAdmins, health::ServiceHealthProvider,
    },
};
use lakekeeper_integration_tests::{create_view_request, views_test_setup};
use sqlx::PgPool;
use tower::ServiceExt as _;

const IDP: &str = "oidc";
const USER: &str = "user";

/// Authenticates the bearer token `user` as that subject.
#[derive(Debug, Clone)]
struct SubjectTokenAuthenticator;

impl limes::Authenticator for SubjectTokenAuthenticator {
    async fn authenticate(
        &self,
        token: &str,
        _introspection: &limes::introspect::IntrospectionResult,
    ) -> limes::error::Result<limes::Authentication> {
        if token != USER {
            return Err(limes::error::Error::Unauthenticated {
                reason: "unknown token".to_string(),
            });
        }
        Ok(limes::Authentication::builder()
            .token_header(None)
            .claims(serde_json::json!({}))
            .subject(limes::Subject::new(
                Some(IDP.to_string()),
                token.to_string(),
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
        None
    }
}

type TestContext = lakekeeper::api::ApiContext<
    lakekeeper::service::State<
        lakekeeper::service::authz::AllowAllAuthorizer,
        lakekeeper_storage_postgres::PostgresBackend,
        lakekeeper_storage_postgres::SecretsState,
    >,
>;

async fn router(ctx: TestContext, authenticator: Option<SubjectTokenAuthenticator>) -> Router {
    let (stats_tx, _stats_rx) = tokio::sync::mpsc::channel(100);
    new_full_router::<_, _, _, _>(RouterArgs {
        authenticator,
        state: ctx,
        service_health_provider: ServiceHealthProvider::new(vec![], 60),
        cors_origins: None,
        metrics_layer: None,
        endpoint_statistics_tracker_tx: EndpointStatisticsTrackerTx::new(stats_tx),
        instance_admin_membership: Arc::new(ConfiguredInstanceAdmins::new(HashSet::new())),
        admission_gates: AdmissionGates::default(),
        management_api_extensions: Vec::new(),
    })
    .await
    .unwrap()
}

/// Creates a view whose `trino.run-as-owner` names `admin`, from a client that is not a trusted
/// engine. Returns the status and the JSON body.
async fn create_definer_view(
    router: &Router,
    warehouse_id: &str,
    namespace: &str,
    token: Option<&str>,
) -> (StatusCode, serde_json::Value) {
    let mut view = create_view_request(Some("definer_view"), None);
    view.properties
        .insert("trino.run-as-owner".to_string(), "admin".to_string());
    let mut request = Request::builder()
        .method(Method::POST)
        .uri(format!(
            "/catalog/v1/{warehouse_id}/namespaces/{namespace}/views"
        ))
        .header(header::HOST, "localhost")
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(token) = token {
        request = request.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    let request = request
        .body(Body::from(serde_json::to_string(&view).unwrap()))
        .unwrap();
    let response = router.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap_or_default())
}

#[sqlx::test]
async fn router_with_authenticator_applies_the_owner_property_rules(pool: PgPool) {
    let (mut ctx, namespace, warehouse_id, _) = views_test_setup(pool, None).await;
    // The router decides from its authenticator, not from the state it gets.
    ctx.v1_state.authentication = ServerAuthentication::Disabled;
    let router = router(ctx, Some(SubjectTokenAuthenticator)).await;

    let (status, body) = create_definer_view(
        &router,
        &warehouse_id.to_string(),
        &namespace.to_url_string(),
        Some(USER),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert_eq!(body["error"]["type"], "ProtectedPropertyModification");
}

#[sqlx::test]
async fn router_without_authenticator_does_not_apply_the_owner_property_rules(pool: PgPool) {
    let (mut ctx, namespace, warehouse_id, _) = views_test_setup(pool, None).await;
    ctx.v1_state.authentication = ServerAuthentication::Enabled;
    let router = router(ctx, None).await;

    let (status, body) = create_definer_view(
        &router,
        &warehouse_id.to_string(),
        &namespace.to_url_string(),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["metadata"]["properties"]["trino.run-as-owner"],
        "admin"
    );
}
