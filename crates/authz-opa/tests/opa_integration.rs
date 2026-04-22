//! Integration smoke test for [`OpaAuthorizer`].
//!
//! Spins up a minimal mock OPA sidecar (an axum server on a random port),
//! points an `OpaAuthorizer` at it, and exercises one of the decision methods
//! end-to-end. The purpose is to prove HTTP wiring, input-doc shape, and
//! `{"result": <bool>}` parsing — not to exercise policy semantics.
//!
//! Extend this file with deny-path, label-matching, and batch-ordering tests
//! once the Vauban rules ship.

use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::{Json, Router, extract::State, routing::post};
use lakekeeper::{
    api::RequestMetadata,
    service::{
        ServerId,
        authz::{Authorizer, CatalogServerAction},
    },
};
use lakekeeper_authz_opa::{OpaAuthorizer, OpaConfig};
use serde_json::Value;
use tokio::net::TcpListener;

#[derive(Clone, Default)]
struct MockState {
    received: Arc<Mutex<Vec<Value>>>,
    decision: Arc<Mutex<Value>>,
}

impl MockState {
    fn new(decision: Value) -> Self {
        Self {
            received: Arc::new(Mutex::new(Vec::new())),
            decision: Arc::new(Mutex::new(decision)),
        }
    }
}

async fn mock_decision(State(state): State<MockState>, Json(body): Json<Value>) -> Json<Value> {
    state.received.lock().unwrap().push(body);
    let decision = state.decision.lock().unwrap().clone();
    Json(serde_json::json!({ "result": decision }))
}

async fn start_mock_opa(decision: Value) -> (SocketAddr, MockState) {
    let state = MockState::new(decision);
    let app = Router::new()
        .route("/v1/data/lakekeeper/authz/allow", post(mock_decision))
        .with_state(state.clone());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    // Small grace period so the listener is accepting before the test body
    // issues its first POST. Without this the first request can race the
    // spawn and fail with ConnRefused on fast CI runners.
    tokio::time::sleep(Duration::from_millis(50)).await;
    (addr, state)
}

fn build_authorizer(addr: SocketAddr) -> OpaAuthorizer {
    let config = OpaConfig {
        endpoint: format!("http://{addr}").parse().unwrap(),
        policy_path: "lakekeeper/authz/allow".to_string(),
        request_timeout_ms: 1_000,
        max_concurrency: 4,
        bearer_token: None,
    };
    OpaAuthorizer::new(config, ServerId::new_random()).unwrap()
}

#[tokio::test(flavor = "current_thread")]
async fn permissive_server_action_flows_through() {
    let (addr, state) = start_mock_opa(Value::Bool(true)).await;
    let authz = build_authorizer(addr);
    let metadata = RequestMetadata::new_unauthenticated();

    let actions = [
        CatalogServerAction::ListUsers,
        CatalogServerAction::ProvisionUsers,
    ];
    let result = authz
        .are_allowed_server_actions_impl(&metadata, None, &actions)
        .await
        .expect("decision call");

    assert_eq!(result, vec![true, true]);

    let received = state.received.lock().unwrap();
    assert_eq!(received.len(), 2, "one POST per action");
    for body in received.iter() {
        let input = &body["input"];
        assert_eq!(input["operation"]["kind"], "server");
        assert!(
            input["operation"]["action"].get("action").is_some(),
            "action field should carry the serialized Catalog*Action: {input:?}"
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn denial_returns_false() {
    let (addr, _state) = start_mock_opa(Value::Bool(false)).await;
    let authz = build_authorizer(addr);
    let metadata = RequestMetadata::new_unauthenticated();

    let actions = [CatalogServerAction::ListUsers];
    let result = authz
        .are_allowed_server_actions_impl(&metadata, None, &actions)
        .await
        .expect("decision call");

    assert_eq!(result, vec![false]);
}

#[tokio::test(flavor = "current_thread")]
async fn non_bool_result_surfaces_as_error() {
    let (addr, _state) = start_mock_opa(Value::String("maybe".to_string())).await;
    let authz = build_authorizer(addr);
    let metadata = RequestMetadata::new_unauthenticated();

    let actions = [CatalogServerAction::ListUsers];
    let err = authz
        .are_allowed_server_actions_impl(&metadata, None, &actions)
        .await
        .expect_err("non-bool result must fail");
    // Formatted error must mention the policy path so ops can trace it.
    assert!(
        format!("{err:?}").contains("lakekeeper/authz/allow"),
        "error should reference policy path: {err:?}"
    );
}
