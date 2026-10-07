//! Grant and managed-access helpers shared by the authorizer integration tests.

use std::sync::Arc;

use lakekeeper::{
    ProjectId, WarehouseId,
    api::{
        ApiContext, RequestMetadata, RequestMetadataTestBuilder,
        management::v1::{
            ApiServer,
            check::UserOrRole,
            grant::{ApplyGrantsRequest, GrantEntry, Service as _},
        },
    },
    axum::{
        Router,
        body::Body,
        http::{Request, StatusCode},
    },
    service::{NamespaceId, Role, RoleId, State, UserId, authn::Actor, authz::Authorizer},
};
use lakekeeper_storage_postgres::{PostgresBackend, SecretsState};
use tower::ServiceExt as _;

type Ctx<A> = ApiContext<State<A, PostgresBackend, SecretsState>>;

/// A request by `user_id` acting as itself in `project_id`.
#[must_use]
pub fn principal_metadata(user_id: &UserId, project_id: &ProjectId) -> RequestMetadata {
    RequestMetadataTestBuilder::builder()
        .actor(Actor::Principal(user_id.clone()))
        .project_id(Some(project_id.clone().into()))
        .build()
}

/// A request by `user_id` acting under the assumed role `role_id` in `project_id`.
#[must_use]
pub fn assumed_role_metadata(
    user_id: &UserId,
    role_id: RoleId,
    project_id: &ProjectId,
) -> RequestMetadata {
    RequestMetadataTestBuilder::builder()
        .actor(Actor::Role {
            principal: user_id.clone(),
            assumed_role: Arc::new(Role::new_random_with_id(role_id)),
        })
        .project_id(Some(project_id.clone().into()))
        .build()
}

/// Grant one privilege on a namespace to `principal`, through the same endpoint an operator
/// uses.
///
/// # Panics
/// If the grant is refused.
pub async fn grant_on_namespace_to<A: Authorizer>(
    ctx: &Ctx<A>,
    md: &RequestMetadata,
    warehouse_id: WarehouseId,
    namespace_id: NamespaceId,
    privilege: &str,
    principal: UserOrRole,
) {
    ApiServer::<PostgresBackend, A, SecretsState>::apply_namespace_grants(
        warehouse_id,
        namespace_id,
        ctx.clone(),
        md.clone(),
        ApplyGrantsRequest {
            writes: vec![GrantEntry {
                privilege: privilege.to_string(),
                principal,
            }],
            deletes: vec![],
        },
    )
    .await
    .unwrap();
}

/// Grant one privilege on a namespace to `user`.
///
/// # Panics
/// If the grant is refused.
pub async fn grant_on_namespace<A: Authorizer>(
    ctx: &Ctx<A>,
    md: &RequestMetadata,
    warehouse_id: WarehouseId,
    namespace_id: NamespaceId,
    privilege: &str,
    user: &UserId,
) {
    grant_on_namespace_to(
        ctx,
        md,
        warehouse_id,
        namespace_id,
        privilege,
        UserOrRole::User(user.clone()),
    )
    .await;
}

/// Turn `managed_access` on a namespace on or off through the authorizer's endpoint. Returns
/// the status and the JSON body, `Null` when it is empty.
///
/// # Panics
/// If the router fails or the body is not JSON.
pub async fn post_namespace_managed_access<A: Authorizer>(
    ctx: &Ctx<A>,
    md: &RequestMetadata,
    namespace_id: NamespaceId,
    managed: bool,
) -> (StatusCode, serde_json::Value) {
    let router: Router = Router::new()
        .nest(
            "/management/v1",
            ApiServer::<PostgresBackend, A, SecretsState>::new_v1_router(&ctx.v1_state.authz),
        )
        .with_state(ctx.clone());
    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!(
                    "/management/v1/permissions/namespace/{namespace_id}/managed-access"
                ))
                .header("content-type", "application/json")
                .extension(md.clone())
                .body(Body::from(
                    serde_json::json!({ "managed-access": managed }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = lakekeeper::axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body = if bytes.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    (status, body)
}

/// Turn `managed_access` on a namespace on or off.
///
/// # Panics
/// If the request is refused.
pub async fn set_namespace_managed_access<A: Authorizer>(
    ctx: &Ctx<A>,
    md: &RequestMetadata,
    namespace_id: NamespaceId,
    managed: bool,
) {
    let (status, body) = post_namespace_managed_access(ctx, md, namespace_id, managed).await;
    assert_eq!(status, StatusCode::OK, "{body}");
}
