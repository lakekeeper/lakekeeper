//! Idempotency-Key semantics that are not tied to a single endpoint.
//!
//! Per-endpoint replay coverage lives next to those endpoints; this file pins
//! the cross-cutting rules: keys are scoped to one operation, a staged create
//! replays as staged, and a recursive namespace drop is covered like any other.

use http::StatusCode;
use iceberg::{NamespaceIdent, TableIdent};
use lakekeeper::{
    api::{
        ApiContext, RequestMetadata,
        iceberg::{
            types::{DropParams, Prefix},
            v1::{
                DataAccess, NamespaceParameters, TableParameters,
                namespace::{NamespaceDropFlags, NamespaceService as _},
                tables::TablesService as _,
            },
        },
        management::v1::warehouse::TabularDeleteProfile,
    },
    server::CatalogServer,
    service::{State, authz::AllowAllAuthorizer, idempotency::IdempotencyKey},
};
use lakekeeper_integration_tests::{
    create_ns, create_table as create_table_helper, create_table_request, memory_io_profile,
    setup_simple,
};
use lakekeeper_storage_postgres::{PostgresBackend, SecretsState};
use sqlx::PgPool;

type Ctx = ApiContext<State<AllowAllAuthorizer, PostgresBackend, SecretsState>>;

async fn setup(pool: PgPool) -> (Ctx, String, NamespaceIdent) {
    let (ctx, warehouse) = setup_simple(
        pool,
        memory_io_profile(),
        None,
        AllowAllAuthorizer::default(),
        TabularDeleteProfile::Hard {},
        None,
    )
    .await;
    let prefix = warehouse.warehouse_id.to_string();
    let ns = create_ns(ctx.clone(), prefix.clone(), "idem_ns".to_string()).await;
    (ctx, prefix, ns.namespace)
}

fn metadata_with_key(key: IdempotencyKey) -> RequestMetadata {
    let mut metadata = RequestMetadata::new_unauthenticated();
    metadata.with_idempotency_key(key);
    metadata
}

fn new_key() -> IdempotencyKey {
    IdempotencyKey::parse(&uuid::Uuid::now_v7().to_string()).unwrap()
}

/// The spec makes the key globally unique. Reusing one across operations must be
/// rejected rather than served a replay of the other operation's response.
#[sqlx::test]
async fn test_key_reused_across_operations_is_rejected(pool: PgPool) {
    let (ctx, prefix, ns) = setup(pool).await;
    let key = new_key();

    CatalogServer::create_table(
        NamespaceParameters {
            prefix: Some(Prefix(prefix.clone())),
            namespace: ns.clone(),
        },
        create_table_request(Some("first".to_string()), Some(false)),
        DataAccess::not_specified(),
        ctx.clone(),
        metadata_with_key(key),
    )
    .await
    .unwrap();

    create_table_helper(ctx.clone(), prefix.clone(), "idem_ns", "second", false)
        .await
        .unwrap();

    // Same key, different endpoint. Without the operation check this replays the
    // createTable record and returns 204 without dropping anything.
    let err = CatalogServer::drop_table(
        TableParameters {
            prefix: Some(Prefix(prefix.clone())),
            table: TableIdent {
                namespace: ns.clone(),
                name: "second".to_string(),
            },
        },
        DropParams {
            purge_requested: false,
            force: false,
        },
        ctx.clone(),
        metadata_with_key(key),
    )
    .await
    .unwrap_err();

    assert_eq!(err.error.code, StatusCode::BAD_REQUEST);
    assert_eq!(err.error.r#type, "IdempotencyKeyReused");

    // The drop really did not happen.
    CatalogServer::load_table(
        TableParameters {
            prefix: Some(Prefix(prefix)),
            table: TableIdent {
                namespace: ns,
                name: "second".to_string(),
            },
        },
        lakekeeper::api::iceberg::v1::tables::LoadTableRequest::default(),
        ctx,
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .expect("the rejected replay must not have dropped the table");
}

/// The same key on the same endpoint still replays — the operation check must
/// not break ordinary replay.
#[sqlx::test]
async fn test_same_operation_still_replays(pool: PgPool) {
    let (ctx, prefix, ns) = setup(pool).await;
    let key = new_key();
    let params = NamespaceParameters {
        prefix: Some(Prefix(prefix)),
        namespace: ns,
    };

    let first = CatalogServer::create_table(
        params.clone(),
        create_table_request(Some("replayed".to_string()), Some(false)),
        DataAccess::not_specified(),
        ctx.clone(),
        metadata_with_key(key),
    )
    .await
    .unwrap();

    let replay = CatalogServer::create_table(
        params,
        create_table_request(Some("replayed".to_string()), Some(false)),
        DataAccess::not_specified(),
        ctx,
        metadata_with_key(key),
    )
    .await
    .expect("a replay on the same endpoint must succeed");

    assert_eq!(first.metadata.uuid(), replay.metadata.uuid());
}

/// A `stage_create` table is persisted with no metadata location, and that is
/// what the original response carried. Replaying must reproduce it rather than
/// 404 through the active-only load path.
#[sqlx::test]
async fn test_staged_create_replays_as_staged(pool: PgPool) {
    let (ctx, prefix, ns) = setup(pool).await;
    let key = new_key();
    let params = NamespaceParameters {
        prefix: Some(Prefix(prefix)),
        namespace: ns,
    };

    let first = CatalogServer::create_table(
        params.clone(),
        create_table_request(Some("staged".to_string()), Some(true)),
        DataAccess::not_specified(),
        ctx.clone(),
        metadata_with_key(key),
    )
    .await
    .unwrap();
    assert!(
        first.metadata_location.is_none(),
        "precondition: a staged create returns no metadata location"
    );

    let replay = CatalogServer::create_table(
        params,
        create_table_request(Some("staged".to_string()), Some(true)),
        DataAccess::not_specified(),
        ctx,
        metadata_with_key(key),
    )
    .await
    .expect("replaying a staged create must not 404");

    assert_eq!(first.metadata.uuid(), replay.metadata.uuid());
    assert!(
        replay.metadata_location.is_none(),
        "the replay must still be staged"
    );
}

/// A recursive drop commits a single transaction, so it can carry the key like
/// every other mutation. Previously the key was silently dropped and the retry
/// re-executed against an already-gone namespace.
#[sqlx::test]
async fn test_recursive_namespace_drop_replays(pool: PgPool) {
    let (ctx, prefix, ns) = setup(pool).await;
    let key = new_key();

    create_table_helper(ctx.clone(), prefix.clone(), "idem_ns", "child", false)
        .await
        .unwrap();

    let params = NamespaceParameters {
        prefix: Some(Prefix(prefix)),
        namespace: ns,
    };
    let flags = NamespaceDropFlags {
        force: false,
        purge: false,
        recursive: true,
    };

    CatalogServer::drop_namespace(params.clone(), flags, ctx.clone(), metadata_with_key(key))
        .await
        .unwrap();

    CatalogServer::drop_namespace(params, flags, ctx, metadata_with_key(key))
        .await
        .expect("a retried recursive drop must replay, not 404");
}
