//! Runtime maintenance mode through the management handler and the full router.
//!
//! The mode is process-wide, so this binary holds exactly one test, which
//! leaves the mode `off`.

use std::{collections::HashSet, sync::Arc};

use http::{Method, Request, StatusCode, header};
use http_body_util::BodyExt as _;
use lakekeeper::{
    api::{
        RequestMetadata,
        management::v1::{
            ApiServer,
            server::{MaintenanceModeResponse, Service, SetMaintenanceModeRequest},
        },
        router::{RouterArgs, new_full_router},
    },
    axum::{Router, body::Body},
    limes,
    service::{
        AdmissionGates, EndpointStatisticsTrackerTx, UserId,
        authz::{AllowAllAuthorizer, ConfiguredInstanceAdmins},
        health::ServiceHealthProvider,
        maintenance_mode::{MAINTENANCE, MaintenanceMode},
    },
};
use lakekeeper_integration_tests::{SetupTestCatalog, memory_io_profile};
use sqlx::PgPool;
use tower::ServiceExt as _;

const IDP: &str = "oidc";
const ADMIN: &str = "admin";
const USER: &str = "user";

/// Authenticates a bearer token as the subject it names: `admin` or `user`.
#[derive(Debug, Clone)]
struct SubjectTokenAuthenticator {
    idp_id: String,
}

impl limes::Authenticator for SubjectTokenAuthenticator {
    async fn authenticate(
        &self,
        token: &str,
        _introspection: &limes::introspect::IntrospectionResult,
    ) -> limes::error::Result<limes::Authentication> {
        if token != ADMIN && token != USER {
            return Err(limes::error::Error::Unauthenticated {
                reason: "unknown token".to_string(),
            });
        }
        Ok(limes::Authentication::builder()
            .token_header(None)
            .claims(serde_json::json!({}))
            .subject(limes::Subject::new(
                Some(self.idp_id.clone()),
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
        Some(&self.idp_id)
    }
}

/// `POST uri` with `body` as `token`; returns the status and the JSON body.
async fn post(
    router: &Router,
    uri: &str,
    token: &str,
    body: serde_json::Value,
) -> (StatusCode, serde_json::Value) {
    let request = Request::builder()
        .method(Method::POST)
        .uri(uri)
        .header(header::HOST, "localhost")
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let response = router.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap_or_default())
}

#[sqlx::test]
async fn maintenance_mode_toggle_end_to_end(pool: PgPool) {
    let (ctx, _) = SetupTestCatalog::builder()
        .pool(pool.clone())
        .storage_profile(memory_io_profile())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await;
    let admin = RequestMetadata::test_instance_admin(UserId::new_unchecked("oidc", "admin"));
    let non_admin = RequestMetadata::new_unauthenticated();
    let request = |mode| SetMaintenanceModeRequest { mode };

    // A non-admin cannot switch the process into read-only, even with AllowAll.
    let err = ApiServer::set_maintenance_mode(
        request(MaintenanceMode::ReadOnly),
        ctx.clone(),
        non_admin.clone(),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.code, 403);
    assert_eq!(err.error.r#type, "InstanceAdminRequired");
    assert!(!MAINTENANCE.is_read_only());

    // An instance admin can; the response names the serving process.
    let resp = ApiServer::set_maintenance_mode(
        request(MaintenanceMode::ReadOnly),
        ctx.clone(),
        admin.clone(),
    )
    .await
    .unwrap();
    assert_eq!(resp.mode, MaintenanceMode::ReadOnly);
    assert_eq!(resp.startup_mode, MaintenanceMode::Off);
    assert_eq!(
        resp.hostname,
        hostname::get().ok().and_then(|h| h.into_string().ok())
    );
    assert!(MAINTENANCE.is_read_only());

    // Setting the current mode again succeeds.
    let resp = ApiServer::set_maintenance_mode(
        request(MaintenanceMode::ReadOnly),
        ctx.clone(),
        admin.clone(),
    )
    .await
    .unwrap();
    assert_eq!(resp.mode, MaintenanceMode::ReadOnly);

    // A non-admin cannot switch it back either.
    let err =
        ApiServer::set_maintenance_mode(request(MaintenanceMode::Off), ctx.clone(), non_admin)
            .await
            .unwrap_err();
    assert_eq!(err.error.code, 403);
    assert_eq!(err.error.r#type, "InstanceAdminRequired");
    assert!(MAINTENANCE.is_read_only());

    // Through the full router, as an operator calls a pod.
    let (stats_tx, _stats_rx) = tokio::sync::mpsc::channel(100);
    let router = new_full_router::<_, _, _, _>(RouterArgs {
        authenticator: Some(SubjectTokenAuthenticator {
            idp_id: IDP.to_string(),
        }),
        state: ctx,
        service_health_provider: ServiceHealthProvider::new(vec![], 60),
        cors_origins: None,
        metrics_layer: None,
        endpoint_statistics_tracker_tx: EndpointStatisticsTrackerTx::new(stats_tx),
        instance_admin_membership: Arc::new(ConfiguredInstanceAdmins::new(HashSet::from([
            UserId::new_unchecked(IDP, ADMIN),
        ]))),
        admission_gates: AdmissionGates::default(),
        management_api_extensions: Vec::new(),
    })
    .await
    .unwrap();
    let route = "/management/v1/server/maintenance-mode";

    // Other mutating requests are rejected while read-only.
    let (status, body) = post(
        &router,
        "/management/v1/bootstrap",
        ADMIN,
        serde_json::json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["error"]["type"], "MaintenanceModeError");

    // A POST that only reads is still served.
    let (status, _) = post(
        &router,
        "/management/v1/search/user",
        ADMIN,
        serde_json::json!({"search": "admin"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // The maintenance-mode route passes the gate, so a non-admin reaches the
    // instance-admin check.
    let (status, body) = post(&router, route, USER, serde_json::json!({"mode": "off"})).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(body["error"]["type"], "InstanceAdminRequired");
    assert!(MAINTENANCE.is_read_only());

    // The instance admin switches it off again.
    let (status, body) = post(&router, route, ADMIN, serde_json::json!({"mode": "off"})).await;
    assert_eq!(status, StatusCode::OK);
    let resp: MaintenanceModeResponse = serde_json::from_value(body).unwrap();
    assert_eq!(resp.mode, MaintenanceMode::Off);
    assert_eq!(resp.startup_mode, MaintenanceMode::Off);
    assert!(!MAINTENANCE.is_read_only());
}
