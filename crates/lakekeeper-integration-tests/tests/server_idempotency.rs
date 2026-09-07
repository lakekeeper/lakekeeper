//! Idempotency-Key semantics that are not tied to a single endpoint.
//!
//! Per-endpoint replay coverage lives next to those endpoints; this file pins
//! the cross-cutting rules: keys are scoped to one operation, a staged create
//! replays as staged, and a recursive namespace drop is covered like any other.

use std::{collections::HashMap, sync::Arc};

use http::StatusCode;
use iceberg::{NamespaceIdent, TableIdent};
use iceberg_ext::catalog::rest::RenameTableRequest;
use lakekeeper::{
    api::{
        ApiContext, RequestMetadata, RequestMetadataTestBuilder,
        data::v1::generic_tables::{
            CreateGenericTableRequest, GenericTableParameters, GenericTableService as _,
            RenameGenericTableRequest, RenameGenericTableTarget,
        },
        iceberg::{
            types::{DropParams, Prefix},
            v1::{
                DataAccess, NamespaceParameters, TableParameters, ViewParameters,
                namespace::{NamespaceDropFlags, NamespaceService as _},
                tables::TablesService as _,
                views::ViewService as _,
            },
        },
        management::v1::warehouse::TabularDeleteProfile,
    },
    server::CatalogServer,
    service::{
        GenericTableFormat, State, UserId,
        authn::Actor,
        authz::AllowAllAuthorizer,
        events::{EventListener, IdempotentReplayEvent},
        idempotency::IdempotencyKey,
    },
};
use lakekeeper_integration_tests::{
    create_ns, create_table as create_table_helper, create_table_request, create_view_request,
    memory_io_profile, setup_simple,
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

/// The retry, issued by a different principal than the request that did the
/// work — which is what a retry wrapper on another worker looks like.
fn metadata_with_key_as(key: IdempotencyKey, subject: &str) -> RequestMetadata {
    let mut metadata = RequestMetadataTestBuilder::builder()
        .actor(Actor::Principal(
            UserId::try_from(subject).expect("a valid user id"),
        ))
        .build();
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
        DropParams::builder()
            .purge_requested(false)
            .force(false)
            .build(),
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

/// A retry whose table has since been dropped cannot be replayed — but that is a
/// 404, not a server fault. The replay used to wrap every load error as internal.
#[sqlx::test]
async fn test_replay_of_a_dropped_table_is_not_an_internal_error(pool: PgPool) {
    let (ctx, prefix, ns) = setup(pool).await;
    let key = new_key();
    let params = NamespaceParameters {
        prefix: Some(Prefix(prefix.clone())),
        namespace: ns.clone(),
    };

    CatalogServer::create_table(
        params.clone(),
        create_table_request(Some("gone".to_string()), Some(false)),
        DataAccess::not_specified(),
        ctx.clone(),
        metadata_with_key(key),
    )
    .await
    .unwrap();

    CatalogServer::drop_table(
        TableParameters {
            prefix: Some(Prefix(prefix)),
            table: TableIdent {
                namespace: ns,
                name: "gone".to_string(),
            },
        },
        DropParams::builder()
            .purge_requested(false)
            .force(false)
            .build(),
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    let err = CatalogServer::create_table(
        params,
        create_table_request(Some("gone".to_string()), Some(false)),
        DataAccess::not_specified(),
        ctx,
        metadata_with_key(key),
    )
    .await
    .unwrap_err();

    assert_eq!(err.error.code, StatusCode::NOT_FOUND);
}

/// The staged relaxation belongs to staging retries only. `loadTable` refuses
/// staged tables outright, so a plain create's key must not become a way to read
/// one — which matters because the record binds the key to an endpoint, not to a
/// target.
#[sqlx::test]
async fn test_a_plain_create_key_does_not_expose_a_staged_table(pool: PgPool) {
    let (ctx, prefix, ns) = setup(pool).await;
    let params = NamespaceParameters {
        prefix: Some(Prefix(prefix)),
        namespace: ns,
    };

    // A staged table nobody has committed.
    CatalogServer::create_table(
        params.clone(),
        create_table_request(Some("secret_staged".to_string()), Some(true)),
        DataAccess::not_specified(),
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    // A key spent on an ordinary, non-staging create.
    let key = new_key();
    CatalogServer::create_table(
        params.clone(),
        create_table_request(Some("ordinary".to_string()), Some(false)),
        DataAccess::not_specified(),
        ctx.clone(),
        metadata_with_key(key),
    )
    .await
    .unwrap();

    // Replaying it against the staged table's name must not hand it over.
    let err = CatalogServer::create_table(
        params,
        create_table_request(Some("secret_staged".to_string()), Some(false)),
        DataAccess::not_specified(),
        ctx,
        metadata_with_key(key),
    )
    .await
    .unwrap_err();

    assert_eq!(err.error.code, StatusCode::NOT_FOUND);
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
    let flags = NamespaceDropFlags::builder().recursive().build();

    CatalogServer::drop_namespace(params.clone(), flags, ctx.clone(), metadata_with_key(key))
        .await
        .unwrap();

    CatalogServer::drop_namespace(params, flags, ctx, metadata_with_key(key))
        .await
        .expect("a retried recursive drop must replay, not 404");
}

/// Collects the replay records the seven 204 handlers emit, so a test can assert
/// what a retry announced rather than only that it returned 204.
#[derive(Debug)]
struct ReplayCapture(tokio::sync::mpsc::UnboundedSender<IdempotentReplayEvent>);

impl std::fmt::Display for ReplayCapture {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ReplayCapture")
    }
}

#[async_trait::async_trait]
impl EventListener for ReplayCapture {
    async fn idempotent_replay_served(&self, event: IdempotentReplayEvent) -> anyhow::Result<()> {
        let _ = self.0.send(event);
        Ok(())
    }
}

/// `entity-type:action[flags]:target` for one replay record — the triple an
/// auditor needs, and the triple a wrong event context would get wrong. Flags
/// are included so a site that hardcoded `force`/`purge` instead of passing the
/// request's own is caught, and sorted because their order in the descriptor is
/// insertion order and carries no meaning. Rendered here rather than through
/// `ActionDescriptor::log_string`, which feeds error messages, not audit
/// records.
fn describe(event: &IdempotentReplayEvent) -> String {
    let entity = event
        .entities
        .entities
        .first()
        .expect("a replay names the entity the caller asked about");
    // Ordered by specificity: a tabular entity also carries `namespace`.
    let target = ["table", "view", "generic-table", "namespace"]
        .into_iter()
        .find_map(|key| {
            entity
                .fields
                .iter()
                .find(|field| field.key == key)
                .map(|field| field.value.clone())
        })
        .expect("a replay names its target");
    let descriptor = event
        .actions
        .first()
        .expect("a replay records the action asked for");
    let mut flags = descriptor
        .context
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>();
    flags.sort();
    let action = if flags.is_empty() {
        descriptor.action_name.to_string()
    } else {
        format!("{}[{}]", descriptor.action_name, flags.join(","))
    };
    format!("{}:{action}:{target}", entity.entity_type)
}

fn generic_table_request(name: &str) -> CreateGenericTableRequest {
    CreateGenericTableRequest {
        name: name.to_string(),
        format: GenericTableFormat::Unknown("lance".to_string()),
        base_location: None,
        doc: None,
        properties: HashMap::default(),
        schema: None,
        statistics: None,
    }
}

/// Each drop and rename handler answers the replay above the authorization
/// banner, so it has to record the replay itself — a handler that forgets leaves
/// the retry unattributed for as long as the key lives.
///
/// All seven in one test on purpose: the failure mode is one of seven identical
/// sites silently omitting a line, which only an enumeration catches.
#[sqlx::test]
async fn test_every_replayed_204_is_audited(pool: PgPool) {
    let (ctx, prefix, ns) = setup(pool).await;
    let ns_params = NamespaceParameters {
        prefix: Some(Prefix(prefix.clone())),
        namespace: ns.clone(),
    };
    let table = |name: &str| TableIdent {
        namespace: ns.clone(),
        name: name.to_string(),
    };
    // The drop actions are the only ones carrying context flags, and a flag only
    // appears in the record when true — so every one of them has to be driven
    // true, or a site that hardcoded it to false is invisible.
    let purging_drop = DropParams::builder()
        .purge_requested(true)
        .force(true)
        .build();

    let (sender, mut events) = tokio::sync::mpsc::unbounded_channel();
    ctx.v1_state
        .events
        .append(Arc::new(ReplayCapture(sender)))
        .await;

    // ---- dropTable ----
    create_table_helper(ctx.clone(), prefix.clone(), "idem_ns", "dropped", false)
        .await
        .unwrap();
    let drop_table_key = new_key();
    let drop_table = |metadata| {
        CatalogServer::drop_table(
            TableParameters {
                prefix: Some(Prefix(prefix.clone())),
                table: table("dropped"),
            },
            purging_drop.clone(),
            ctx.clone(),
            metadata,
        )
    };
    drop_table(metadata_with_key(drop_table_key)).await.unwrap();
    drop_table(metadata_with_key_as(drop_table_key, "oidc~retrier"))
        .await
        .expect("the retry is served from the record");

    // ---- renameTable ----
    create_table_helper(ctx.clone(), prefix.clone(), "idem_ns", "ren_src", false)
        .await
        .unwrap();
    let key = new_key();
    for _ in 0..2 {
        CatalogServer::rename_table(
            Some(Prefix(prefix.clone())),
            RenameTableRequest {
                source: table("ren_src"),
                destination: table("ren_dst"),
            },
            ctx.clone(),
            metadata_with_key(key),
        )
        .await
        .expect("the retry is served from the record");
    }

    // ---- dropView ----
    CatalogServer::create_view(
        ns_params.clone(),
        create_view_request(Some("dropped_view"), None),
        ctx.clone(),
        DataAccess::not_specified(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();
    let key = new_key();
    for _ in 0..2 {
        CatalogServer::drop_view(
            ViewParameters {
                prefix: Some(Prefix(prefix.clone())),
                view: table("dropped_view"),
            },
            purging_drop.clone(),
            ctx.clone(),
            metadata_with_key(key),
        )
        .await
        .expect("the retry is served from the record");
    }

    // ---- renameView ----
    CatalogServer::create_view(
        ns_params.clone(),
        create_view_request(Some("vren_src"), None),
        ctx.clone(),
        DataAccess::not_specified(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();
    let key = new_key();
    for _ in 0..2 {
        CatalogServer::rename_view(
            Some(Prefix(prefix.clone())),
            RenameTableRequest {
                source: table("vren_src"),
                destination: table("vren_dst"),
            },
            ctx.clone(),
            metadata_with_key(key),
        )
        .await
        .expect("the retry is served from the record");
    }

    // ---- dropGenericTable ----
    CatalogServer::create_generic_table(
        ns_params.clone(),
        generic_table_request("dropped_gt"),
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();
    let key = new_key();
    for _ in 0..2 {
        CatalogServer::drop_generic_table(
            GenericTableParameters {
                prefix: Some(Prefix(prefix.clone())),
                namespace: ns.clone(),
                table_name: "dropped_gt".to_string(),
            },
            DropParams {
                purge_requested: false,
                force: false,
            },
            ctx.clone(),
            metadata_with_key(key),
        )
        .await
        .expect("the retry is served from the record");
    }

    // ---- renameGenericTable ----
    CatalogServer::create_generic_table(
        ns_params,
        generic_table_request("gtren_src"),
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();
    let key = new_key();
    let rename_target = |name: &str| RenameGenericTableTarget {
        namespace: ns.clone().inner(),
        name: name.to_string(),
    };
    for _ in 0..2 {
        CatalogServer::rename_generic_table(
            Some(Prefix(prefix.clone())),
            RenameGenericTableRequest {
                source: rename_target("gtren_src"),
                destination: rename_target("gtren_dst"),
            },
            ctx.clone(),
            metadata_with_key(key),
        )
        .await
        .expect("the retry is served from the record");
    }

    // ---- dropNamespace ----
    let doomed = create_ns(ctx.clone(), prefix.clone(), "doomed_ns".to_string())
        .await
        .namespace;
    let key = new_key();
    for _ in 0..2 {
        CatalogServer::drop_namespace(
            NamespaceParameters {
                prefix: Some(Prefix(prefix.clone())),
                namespace: doomed.clone(),
            },
            NamespaceDropFlags {
                force: true,
                purge: true,
                recursive: true,
            },
            ctx.clone(),
            metadata_with_key(key),
        )
        .await
        .expect("the retry is served from the record");
    }

    // Emission is fire-and-forget, so wait for the records rather than draining
    // whatever has arrived. Never receiving one is the failure under test, and
    // the timeout reports it.
    let mut records = Vec::new();
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        while records.len() < 7 {
            let event = events.recv().await.expect("the capture outlives dispatch");
            records.push(event);
        }
    })
    .await
    .unwrap_or_else(|_| {
        let seen = records.iter().map(describe).collect::<Vec<_>>();
        panic!("only {} of 7 replays were audited: {seen:?}", records.len())
    });

    let mut seen = records.iter().map(describe).collect::<Vec<_>>();
    seen.sort();
    assert_eq!(
        seen,
        vec![
            "generic-table:drop:dropped_gt",
            "generic-table:rename:gtren_src",
            "namespace:delete[force=true,purge=true,recursive=true]:doomed_ns",
            "table:drop[force=true,purge=true]:dropped",
            "table:rename:ren_src",
            "view:drop[force=true,purge=true]:dropped_view",
            "view:rename:vren_src",
        ]
    );

    // Exactly one replay record per replay. `emit_idempotent_replay` consumes the
    // context, but `APIEventContext` is `Clone`, so a site can still emit twice
    // and compile. Waiting for a receive that must time out catches a duplicate
    // that arrives after the seventh record, which the set above cannot see.
    // This capture only subscribes to replay records, so it says nothing about a
    // site that emitted a replay *and* an authorization event.
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(200), events.recv())
            .await
            .is_err(),
        "a replay must announce itself exactly once"
    );

    // The set above pins identity and flags across all seven. Two things it
    // projects away, on the one record whose request is still in scope: the key
    // that served it, and — the claim the whole event rests on — that the record
    // describes the *retry* and not the request that did the work.
    let dropped = records
        .iter()
        .find(|event| describe(event) == "table:drop[force=true,purge=true]:dropped")
        .expect("the dropTable replay");
    assert_eq!(
        dropped.idempotency_key.as_uuid(),
        drop_table_key.as_uuid(),
        "the record names the key that served it"
    );
    assert_eq!(
        dropped.request_metadata.actor().to_string(),
        "Principal(oidc~retrier)",
        "the retry's actor, not the actor of the request that did the work"
    );
}
