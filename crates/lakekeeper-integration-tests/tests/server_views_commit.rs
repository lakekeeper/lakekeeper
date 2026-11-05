use std::sync::{Arc, Mutex};

use chrono::Utc;
use http::StatusCode;
use iceberg::{
    NamespaceIdent, TableIdent, TableUpdate,
    spec::{TableMetadata, ViewMetadata},
};
use iceberg_ext::catalog::rest::{
    CommitViewRequest, ErrorModel, LoadViewResult, RenameTableRequest, ViewUpdate,
};
use lakekeeper::{
    WarehouseId,
    api::{
        ApiContext, RequestMetadata,
        iceberg::{
            types::{DropParams, Prefix},
            v1::{DataAccess, NamespaceParameters, ViewParameters},
        },
        management::v1::{ApiServer as ManagementApiServer, view::ViewManagementService},
    },
    server::views::{
        commit::commit_view, create::create_view, drop::drop_view, rename::rename_view,
    },
    service::{
        ServerAuthentication, State, TabularId,
        authz::AllowAllAuthorizer,
        contract_verification::{
            ContractVerification, ContractVerificationOutcome, ContractVerifiers,
        },
        events::{CommitViewEvent, EventListener},
    },
};
use lakekeeper_integration_tests::{
    OWNER_PROPERTY, create_view_helper, create_view_request, load_view_helper, memory_files_under,
    put_memory_file, random_request_metadata, trino_engine_request, user_request, views_test_setup,
};
use lakekeeper_storage_postgres::{PostgresBackend, SecretsState};
use maplit::hashmap;
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

#[sqlx::test]
async fn test_commit_view(pool: PgPool) {
    let (api_context, namespace, whi, _) = views_test_setup(pool, None).await;
    let prefix = whi.to_string();
    let view_name = "myview";
    let view = create_view_helper(
        api_context.clone(),
        namespace.clone(),
        create_view_request(Some(view_name), None),
        Some(prefix.clone()),
    )
    .await
    .unwrap();

    let rq: CommitViewRequest = spark_commit_update_request(whi, Some(view.metadata.uuid()));

    let res = Box::pin(commit_view(
        ViewParameters {
            prefix: Some(Prefix(prefix.clone())),
            view: TableIdent::from_strs(namespace.inner().into_iter().chain([view_name.into()]))
                .unwrap(),
        },
        rq,
        api_context,
        DataAccess {
            vended_credentials: true,
            remote_signing: false,
        },
        RequestMetadata::new_unauthenticated(),
    ))
    .await
    .unwrap();

    assert_eq!(res.metadata.current_version_id(), 2);
    assert_eq!(res.metadata.schemas_iter().len(), 3);
    assert_eq!(res.metadata.versions().len(), 2);
    let max_schema = res.metadata.schemas_iter().map(|s| s.schema_id()).max();
    assert_eq!(
        res.metadata.current_version().schema_id(),
        max_schema.unwrap()
    );

    assert_eq!(
        res.metadata.properties(),
        &hashmap! {
            "create_engine_version".to_string() => "Spark 3.5.1".to_string(),
            "spark.query-column-names".to_string() => "id".to_string(),
        }
    );
}

#[sqlx::test]
async fn test_commit_view_preserves_protection(pool: PgPool) {
    let (api_context, namespace, whi, _) = views_test_setup(pool, None).await;
    let prefix = whi.to_string();
    let view_name = "myview";
    let view_ident = TableIdent::from_strs(
        namespace
            .clone()
            .inner()
            .into_iter()
            .chain([view_name.into()]),
    )
    .unwrap();
    let view = create_view_helper(
        api_context.clone(),
        namespace.clone(),
        create_view_request(Some(view_name), None),
        Some(prefix.clone()),
    )
    .await
    .unwrap();

    ManagementApiServer::set_view_protection(
        view.metadata.uuid().into(),
        whi,
        true,
        api_context.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    let rq: CommitViewRequest = spark_commit_update_request(whi, Some(view.metadata.uuid()));
    let res = Box::pin(commit_view(
        ViewParameters {
            prefix: Some(Prefix(prefix.clone())),
            view: view_ident.clone(),
        },
        rq,
        api_context.clone(),
        DataAccess {
            vended_credentials: true,
            remote_signing: false,
        },
        RequestMetadata::new_unauthenticated(),
    ))
    .await
    .unwrap();

    let protection = ManagementApiServer::get_view_protection(
        res.metadata.uuid().into(),
        whi,
        api_context.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    assert!(protection.protected);

    let err = drop_view(
        ViewParameters {
            prefix: Some(Prefix(prefix.clone())),
            view: view_ident,
        },
        DropParams {
            purge_requested: true,
            force: false,
        },
        api_context,
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .expect_err("protected view should remain protected after commit");
    assert_eq!(err.error.code, StatusCode::CONFLICT);
}

#[sqlx::test]
async fn test_commit_view_fails_with_wrong_assertion(pool: PgPool) {
    let (api_context, namespace, whi, _) = views_test_setup(pool, None).await;
    let prefix = whi.to_string();
    let view_name = "myview";
    let _ = create_view_helper(
        api_context.clone(),
        namespace.clone(),
        create_view_request(Some(view_name), None),
        Some(prefix.clone()),
    )
    .await
    .unwrap();

    let rq: CommitViewRequest = spark_commit_update_request(whi, Some(Uuid::now_v7()));

    let err = Box::pin(commit_view(
        ViewParameters {
            prefix: Some(Prefix(prefix.clone())),
            view: TableIdent::from_strs(namespace.inner().into_iter().chain([view_name.into()]))
                .unwrap(),
        },
        rq,
        api_context,
        DataAccess {
            vended_credentials: true,
            remote_signing: false,
        },
        RequestMetadata::new_unauthenticated(),
    ))
    .await
    .expect_err("This unexpectedly didn't fail the uuid assertion.");
    assert_eq!(err.error.code, 400);
    assert_eq!(err.error.r#type, "ViewUuidMismatch");
}

// ---- Engine owner property (`trino.run-as-owner`) ----
//
// The test server configures no trusted engine. The owner property is protected anyway: only a
// request matched as a trusted engine that owns the property may write it, or change the current
// version of a view that holds it. The test server counts as one with authentication.

/// The SQL of the view `create_view_request` creates.
const CREATED_SQL: &str = "select id, xyz from spark_demo.my_table";

type TestContext = ApiContext<State<AllowAllAuthorizer, PostgresBackend, SecretsState>>;

fn view_params(prefix: &str, namespace: &NamespaceIdent, view_name: &str) -> ViewParameters {
    ViewParameters {
        prefix: Some(Prefix(prefix.to_string())),
        view: TableIdent::from_strs(
            namespace
                .clone()
                .inner()
                .into_iter()
                .chain([view_name.into()]),
        )
        .unwrap(),
    }
}

/// Creates a DEFINER view owned by `owner`, as Trino does when it is a trusted engine.
async fn create_definer_view(
    ctx: &TestContext,
    namespace: &NamespaceIdent,
    prefix: &str,
    view_name: &str,
    owner: &str,
) -> LoadViewResult {
    let mut rq = create_view_request(Some(view_name), None);
    rq.properties
        .insert(OWNER_PROPERTY.to_string(), owner.to_string());
    Box::pin(create_view(
        NamespaceParameters {
            prefix: Some(Prefix(prefix.to_string())),
            namespace: namespace.clone(),
        },
        rq,
        ctx.clone(),
        DataAccess {
            vended_credentials: true,
            remote_signing: false,
        },
        trino_engine_request(),
    ))
    .await
    .unwrap()
}

async fn commit_view_as(
    ctx: &TestContext,
    params: ViewParameters,
    updates: serde_json::Value,
    request_metadata: RequestMetadata,
) -> lakekeeper::api::Result<LoadViewResult> {
    let rq: CommitViewRequest = serde_json::from_value(json!({ "updates": updates })).unwrap();
    Box::pin(commit_view(
        params,
        rq,
        ctx.clone(),
        DataAccess {
            vended_credentials: true,
            remote_signing: false,
        },
        request_metadata,
    ))
    .await
}

/// Updates that add a view version running `sql` and make it current, as Iceberg's view
/// `replace` sends them. A new schema makes the version new even if `sql` is unchanged, as
/// Trino's `COMMENT ON COLUMN` does.
fn new_version(sql: &str, extra_updates: &[serde_json::Value]) -> serde_json::Value {
    let mut updates = vec![
        json!({
            "action": "add-schema",
            "schema": {
                "schema-id": 1,
                "type": "struct",
                "fields": [{"id": 0, "name": "id", "required": false, "type": "long", "doc": "new comment"}]
            },
            "last-column-id": 1
        }),
        json!({
            "action": "add-view-version",
            "view-version": {
                "version-id": 2,
                "schema-id": -1,
                "timestamp-ms": Utc::now().timestamp_millis(),
                "summary": {"engine-name": "trino"},
                "representations": [{"type": "sql", "sql": sql, "dialect": "spark"}],
                "default-namespace": []
            }
        }),
        json!({"action": "set-current-view-version", "view-version-id": -1}),
    ];
    updates.extend_from_slice(extra_updates);
    serde_json::Value::Array(updates)
}

fn set_owner(owner: &str) -> serde_json::Value {
    json!({"action": "set-properties", "updates": {OWNER_PROPERTY: owner}})
}

async fn load(ctx: &TestContext, params: ViewParameters) -> LoadViewResult {
    load_view_helper(ctx.clone(), params).await.unwrap()
}

async fn load_owner(ctx: &TestContext, params: ViewParameters) -> Option<String> {
    load(ctx, params)
        .await
        .metadata
        .properties()
        .get(OWNER_PROPERTY)
        .cloned()
}

fn current_sql(view: &LoadViewResult) -> String {
    let representations = view.metadata.current_version().representations();
    assert_eq!(representations.len(), 1);
    let iceberg::spec::ViewRepresentation::Sql(sql) = representations.iter().next().unwrap();
    sql.sql.clone()
}

fn assert_protected_property_error(err: &iceberg_ext::catalog::rest::IcebergErrorResponse) {
    assert_eq!(err.error.code, StatusCode::FORBIDDEN);
    assert_eq!(err.error.r#type, "ProtectedPropertyModification");
}

fn assert_protected_version_error(err: &iceberg_ext::catalog::rest::IcebergErrorResponse) {
    assert_eq!(err.error.code, StatusCode::FORBIDDEN);
    assert_eq!(err.error.r#type, "ProtectedViewVersionChange");
}

fn set_location(location: &str) -> serde_json::Value {
    json!([{"action": "set-location", "location": location}])
}

async fn create_myview(
    ctx: &TestContext,
    namespace: &NamespaceIdent,
    prefix: &str,
) -> LoadViewResult {
    create_view_helper(
        ctx.clone(),
        namespace.clone(),
        create_view_request(Some("myview"), None),
        Some(prefix.to_string()),
    )
    .await
    .unwrap()
}

/// A location in the same directory as `location`.
fn sibling(location: &str, name: &str) -> String {
    let (parent, _) = location.rsplit_once('/').unwrap();
    format!("{parent}/{name}")
}

/// Moving a view into a location that holds files is refused: moving it away again would
/// remove that location with all its files. The failed commit deletes nothing.
#[sqlx::test]
async fn test_commit_view_refuses_move_to_location_with_files(pool: PgPool) {
    let (ctx, namespace, whi, _) = views_test_setup(pool, None).await;
    let prefix = whi.to_string();
    let view = create_myview(&ctx, &namespace, &prefix).await;
    let old_location = view.metadata.location().to_string();
    assert_eq!(
        memory_files_under(&old_location).await,
        vec![view.metadata_location.clone()]
    );
    let occupied = sibling(&old_location, "occupied");
    let foreign_file = format!("{occupied}/data/foreign.parquet");
    put_memory_file(&foreign_file).await;

    let err = commit_view_as(
        &ctx,
        view_params(&prefix, &namespace, "myview"),
        set_location(&occupied),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.code, StatusCode::BAD_REQUEST);
    assert_eq!(err.error.r#type, "InvalidLocation");

    assert_eq!(memory_files_under(&occupied).await, vec![foreign_file]);
    assert_eq!(
        memory_files_under(&old_location).await,
        vec![view.metadata_location.clone()]
    );
    let loaded = load(&ctx, view_params(&prefix, &namespace, "myview")).await;
    assert_eq!(loaded.metadata.location(), old_location);
    assert_eq!(loaded.metadata_location, view.metadata_location);
}

/// Moving a view into its own location is refused: removing the old location would remove
/// the new metadata. The failed commit deletes nothing.
#[sqlx::test]
async fn test_commit_view_refuses_move_into_own_location(pool: PgPool) {
    let (ctx, namespace, whi, _) = views_test_setup(pool, None).await;
    let prefix = whi.to_string();
    let view = create_myview(&ctx, &namespace, &prefix).await;
    let old_location = view.metadata.location().to_string();

    let err = commit_view_as(
        &ctx,
        view_params(&prefix, &namespace, "myview"),
        set_location(&format!("{old_location}/sub")),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.code, StatusCode::BAD_REQUEST);
    assert_eq!(err.error.r#type, "SetLocationNotAllowed");

    assert_eq!(
        memory_files_under(&old_location).await,
        vec![view.metadata_location.clone()]
    );
    let loaded = load(&ctx, view_params(&prefix, &namespace, "myview")).await;
    assert_eq!(loaded.metadata_location, view.metadata_location);
}

/// Trino up to version 481 gives a view a new location on `CREATE OR REPLACE VIEW`. The view
/// moves to the new, empty location, and its old location is removed.
#[sqlx::test]
async fn test_commit_view_moves_to_empty_location(pool: PgPool) {
    let (ctx, namespace, whi, _) = views_test_setup(pool, None).await;
    let prefix = whi.to_string();
    let view = create_myview(&ctx, &namespace, &prefix).await;
    let old_location = view.metadata.location().to_string();
    let new_location = sibling(&old_location, "moved");

    let moved = commit_view_as(
        &ctx,
        view_params(&prefix, &namespace, "myview"),
        set_location(&new_location),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    assert_eq!(moved.metadata.location(), new_location);
    assert_eq!(
        memory_files_under(&new_location).await,
        vec![moved.metadata_location.clone()]
    );
    assert_eq!(
        memory_files_under(&old_location).await,
        Vec::<String>::new()
    );
    let loaded = load(&ctx, view_params(&prefix, &namespace, "myview")).await;
    assert_eq!(loaded.metadata_location, moved.metadata_location);
}

/// Setting the location a view already has, also with a trailing slash, keeps the view in
/// place and deletes none of its files.
#[sqlx::test]
async fn test_commit_view_set_location_to_own_location_deletes_nothing(pool: PgPool) {
    let (ctx, namespace, whi, _) = views_test_setup(pool, None).await;
    let prefix = whi.to_string();
    let view = create_myview(&ctx, &namespace, &prefix).await;
    let location = view.metadata.location().to_string();

    let same = commit_view_as(
        &ctx,
        view_params(&prefix, &namespace, "myview"),
        set_location(&location),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();
    let with_slash = commit_view_as(
        &ctx,
        view_params(&prefix, &namespace, "myview"),
        set_location(&format!("{location}/")),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    assert_eq!(with_slash.metadata.location(), location);
    let mut expected = vec![
        view.metadata_location,
        same.metadata_location,
        with_slash.metadata_location,
    ];
    expected.sort();
    assert_eq!(memory_files_under(&location).await, expected);
}

#[sqlx::test]
async fn test_commit_view_cannot_set_owner_property_from_untrusted_client(pool: PgPool) {
    let (ctx, namespace, whi, _) = views_test_setup(pool, None).await;
    let prefix = whi.to_string();
    create_view_helper(
        ctx.clone(),
        namespace.clone(),
        create_view_request(Some("v"), None),
        Some(prefix.clone()),
    )
    .await
    .unwrap();

    let err = commit_view_as(
        &ctx,
        view_params(&prefix, &namespace, "v"),
        json!([set_owner("admin")]),
        user_request(),
    )
    .await
    .expect_err("an untrusted client must not make itself run the view as another user");
    assert_protected_property_error(&err);
    assert_eq!(
        load_owner(&ctx, view_params(&prefix, &namespace, "v")).await,
        None
    );
}

#[sqlx::test]
async fn test_commit_view_cannot_change_or_remove_owner_property_from_untrusted_client(
    pool: PgPool,
) {
    let (ctx, namespace, whi, _) = views_test_setup(pool, None).await;
    let prefix = whi.to_string();
    create_definer_view(&ctx, &namespace, &prefix, "v", "alice").await;

    let err = commit_view_as(
        &ctx,
        view_params(&prefix, &namespace, "v"),
        json!([set_owner("admin")]),
        user_request(),
    )
    .await
    .expect_err("changing the owner must be refused");
    assert_protected_property_error(&err);

    let err = commit_view_as(
        &ctx,
        view_params(&prefix, &namespace, "v"),
        json!([{"action": "remove-properties", "removals": [OWNER_PROPERTY]}]),
        user_request(),
    )
    .await
    .expect_err("removing the owner must be refused");
    assert_protected_property_error(&err);

    // A commit that names the property is refused even when it sends the stored value.
    let err = commit_view_as(
        &ctx,
        view_params(&prefix, &namespace, "v"),
        json!([set_owner("alice")]),
        user_request(),
    )
    .await
    .expect_err("naming the owner property must be refused");
    assert_protected_property_error(&err);

    assert_eq!(
        load_owner(&ctx, view_params(&prefix, &namespace, "v")).await,
        Some("alice".to_string())
    );
}

#[sqlx::test]
async fn test_commit_view_refuses_new_sql_on_definer_view_from_untrusted_client(pool: PgPool) {
    let (ctx, namespace, whi, _) = views_test_setup(pool, None).await;
    let prefix = whi.to_string();
    let view = create_definer_view(&ctx, &namespace, &prefix, "v", "alice").await;

    // The commit Spark sends to replace a view: new SQL, no owner property.
    let err = Box::pin(commit_view(
        view_params(&prefix, &namespace, "v"),
        spark_commit_update_request(whi, Some(view.metadata.uuid())),
        ctx.clone(),
        DataAccess {
            vended_credentials: true,
            remote_signing: false,
        },
        user_request(),
    ))
    .await
    .expect_err("an untrusted client must not change the query of a DEFINER view");
    assert_protected_version_error(&err);

    let after = load(&ctx, view_params(&prefix, &namespace, "v")).await;
    assert_eq!(after.metadata.current_version_id(), 1);
    assert_eq!(current_sql(&after), CREATED_SQL);
    assert_eq!(
        after.metadata.properties().get(OWNER_PROPERTY),
        Some(&"alice".to_string())
    );
}

#[sqlx::test]
async fn test_commit_view_refuses_rollback_on_definer_view_from_untrusted_client(pool: PgPool) {
    let (ctx, namespace, whi, _) = views_test_setup(pool, None).await;
    let prefix = whi.to_string();
    create_definer_view(&ctx, &namespace, &prefix, "v", "alice").await;

    // Trino replaces the view with new SQL and sends the owner again: version 2 is current.
    let replaced = commit_view_as(
        &ctx,
        view_params(&prefix, &namespace, "v"),
        new_version(
            "select id from spark_demo.other_table",
            &[set_owner("alice")],
        ),
        trino_engine_request(),
    )
    .await
    .unwrap();
    assert_eq!(replaced.metadata.current_version_id(), 2);

    let err = commit_view_as(
        &ctx,
        view_params(&prefix, &namespace, "v"),
        json!([{"action": "set-current-view-version", "view-version-id": 1}]),
        user_request(),
    )
    .await
    .expect_err("an untrusted client must not roll a DEFINER view back to other SQL");
    assert_protected_version_error(&err);

    let after = load(&ctx, view_params(&prefix, &namespace, "v")).await;
    assert_eq!(after.metadata.current_version_id(), 2);
    assert_eq!(current_sql(&after), "select id from spark_demo.other_table");
}

#[sqlx::test]
async fn test_commit_view_from_trusted_engine_without_owner_property_makes_invoker_view(
    pool: PgPool,
) {
    let (ctx, namespace, whi, _) = views_test_setup(pool, None).await;
    let prefix = whi.to_string();
    let created = create_definer_view(&ctx, &namespace, &prefix, "v", "alice").await;

    // Trino's `CREATE OR REPLACE VIEW ... SECURITY INVOKER` sends no owner property. The
    // commit removes the stored owner, so the new SQL runs as the caller.
    let res = commit_view_as(
        &ctx,
        view_params(&prefix, &namespace, "v"),
        new_version("select id from spark_demo.other_table", &[]),
        trino_engine_request(),
    )
    .await
    .unwrap();
    assert_eq!(res.metadata.current_version_id(), 2);
    assert_eq!(current_sql(&res), "select id from spark_demo.other_table");
    assert_eq!(res.metadata.properties().get(OWNER_PROPERTY), None);

    // It is the same view, so grants on it stay.
    let after = load(&ctx, view_params(&prefix, &namespace, "v")).await;
    assert_eq!(after.metadata.uuid(), created.metadata.uuid());
    assert_eq!(after.metadata.properties().get(OWNER_PROPERTY), None);
    assert_eq!(current_sql(&after), "select id from spark_demo.other_table");
}

/// Creates a view that keeps two versions, as the trusted Trino engine does: with `owner` as
/// its owner if given, as an INVOKER view otherwise.
async fn create_view_keeping_two_versions(
    ctx: &TestContext,
    namespace: &NamespaceIdent,
    prefix: &str,
    view_name: &str,
    owner: Option<&str>,
) {
    let mut rq = create_view_request(Some(view_name), None);
    rq.properties
        .insert("version.history.num-entries".to_string(), "2".to_string());
    if let Some(owner) = owner {
        rq.properties
            .insert(OWNER_PROPERTY.to_string(), owner.to_string());
    }
    Box::pin(create_view(
        NamespaceParameters {
            prefix: Some(Prefix(prefix.to_string())),
            namespace: namespace.clone(),
        },
        rq,
        ctx.clone(),
        DataAccess {
            vended_credentials: true,
            remote_signing: false,
        },
        trino_engine_request(),
    ))
    .await
    .unwrap();
}

/// `(version id, SQL)` of every version the view keeps, sorted by id.
fn kept_versions(view: &LoadViewResult) -> Vec<(i32, String)> {
    let mut versions = view
        .metadata
        .versions()
        .map(|version| {
            let iceberg::spec::ViewRepresentation::Sql(sql) =
                version.representations().iter().next().unwrap();
            (version.version_id(), sql.sql.clone())
        })
        .collect::<Vec<_>>();
    versions.sort();
    versions
}

#[sqlx::test]
async fn test_commit_view_owner_removal_keeps_every_added_version(pool: PgPool) {
    let (ctx, namespace, whi, _) = views_test_setup(pool, None).await;
    let prefix = whi.to_string();
    create_view_keeping_two_versions(&ctx, &namespace, &prefix, "definer", Some("alice")).await;
    create_view_keeping_two_versions(&ctx, &namespace, &prefix, "invoker", None).await;

    // Three new versions in one commit, more than the two the views keep. A commit keeps every
    // version it adds.
    let add_version = |version_id: i32, sql: &str| {
        json!({
            "action": "add-view-version",
            "view-version": {
                "version-id": version_id,
                "schema-id": 0,
                "timestamp-ms": Utc::now().timestamp_millis(),
                "summary": {"engine-name": "trino"},
                "representations": [{"type": "sql", "sql": sql, "dialect": "spark"}],
                "default-namespace": []
            }
        })
    };
    let updates = json!([
        add_version(2, "select 2"),
        add_version(3, "select 3"),
        add_version(4, "select 4"),
        {"action": "set-current-view-version", "view-version-id": -1}
    ]);

    // On the DEFINER view, the commit also removes the owner property.
    let definer = commit_view_as(
        &ctx,
        view_params(&prefix, &namespace, "definer"),
        updates.clone(),
        trino_engine_request(),
    )
    .await
    .unwrap();
    assert_eq!(definer.metadata.properties().get(OWNER_PROPERTY), None);
    let invoker = commit_view_as(
        &ctx,
        view_params(&prefix, &namespace, "invoker"),
        updates,
        trino_engine_request(),
    )
    .await
    .unwrap();

    assert_eq!(
        kept_versions(&definer),
        vec![
            (2, "select 2".to_string()),
            (3, "select 3".to_string()),
            (4, "select 4".to_string()),
        ]
    );
    assert_eq!(kept_versions(&definer), kept_versions(&invoker));
    assert_eq!(
        kept_versions(&load(&ctx, view_params(&prefix, &namespace, "definer")).await),
        kept_versions(&definer)
    );
}

#[sqlx::test]
async fn test_commit_view_rollback_from_trusted_engine_without_owner_property_removes_it(
    pool: PgPool,
) {
    let (ctx, namespace, whi, _) = views_test_setup(pool, None).await;
    let prefix = whi.to_string();
    create_definer_view(&ctx, &namespace, &prefix, "v", "alice").await;
    commit_view_as(
        &ctx,
        view_params(&prefix, &namespace, "v"),
        new_version(
            "select id from spark_demo.other_table",
            &[set_owner("alice")],
        ),
        trino_engine_request(),
    )
    .await
    .unwrap();

    // Back to the SQL of version 1, without the owner property.
    let res = commit_view_as(
        &ctx,
        view_params(&prefix, &namespace, "v"),
        json!([{"action": "set-current-view-version", "view-version-id": 1}]),
        trino_engine_request(),
    )
    .await
    .unwrap();
    assert_eq!(res.metadata.current_version_id(), 1);
    assert_eq!(current_sql(&res), CREATED_SQL);
    assert_eq!(res.metadata.properties().get(OWNER_PROPERTY), None);

    let stored = load(&ctx, view_params(&prefix, &namespace, "v")).await;
    assert_eq!(stored.metadata.current_version_id(), 1);
    assert_eq!(stored.metadata.properties().get(OWNER_PROPERTY), None);
}

/// Records the updates every view commit hands to the contract verifiers.
#[derive(Debug, Default)]
struct ViewUpdateRecorder(Mutex<Vec<Vec<ViewUpdate>>>);

#[async_trait::async_trait]
impl ContractVerification for ViewUpdateRecorder {
    fn name(&self) -> &'static str {
        "view-update-recorder"
    }

    async fn check_table_updates(
        &self,
        _table_updates: &[TableUpdate],
        _current_metadata: &TableMetadata,
    ) -> Result<ContractVerificationOutcome, ErrorModel> {
        Ok(ContractVerificationOutcome::Clear {})
    }

    async fn check_view_updates(
        &self,
        view_updates: &[ViewUpdate],
        _current_metadata: &ViewMetadata,
    ) -> Result<ContractVerificationOutcome, ErrorModel> {
        self.0.lock().unwrap().push(view_updates.to_vec());
        Ok(ContractVerificationOutcome::Clear {})
    }

    async fn check_drop(&self, _: TabularId) -> Result<ContractVerificationOutcome, ErrorModel> {
        Ok(ContractVerificationOutcome::Clear {})
    }

    async fn check_rename(
        &self,
        _: TabularId,
        _: &TableIdent,
    ) -> Result<ContractVerificationOutcome, ErrorModel> {
        Ok(ContractVerificationOutcome::Clear {})
    }
}

/// Records the request of every view commit event.
#[derive(Debug, Default)]
struct ViewCommitRecorder(Mutex<Vec<Arc<CommitViewRequest>>>);

impl std::fmt::Display for ViewCommitRecorder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ViewCommitRecorder")
    }
}

#[async_trait::async_trait]
impl EventListener for ViewCommitRecorder {
    async fn view_committed(&self, event: CommitViewEvent) -> anyhow::Result<()> {
        self.0.lock().unwrap().push(event.request);
        Ok(())
    }
}

#[sqlx::test]
async fn test_commit_view_owner_removal_reaches_contract_verifiers_and_events(pool: PgPool) {
    let (mut ctx, namespace, whi, _) = views_test_setup(pool, None).await;
    let verifier = Arc::new(ViewUpdateRecorder::default());
    ctx.v1_state.contract_verifiers = ContractVerifiers::new(vec![verifier.clone()]);
    let events = Arc::new(ViewCommitRecorder::default());
    ctx.v1_state.events.append(events.clone()).await;
    let prefix = whi.to_string();
    create_definer_view(&ctx, &namespace, &prefix, "v", "alice").await;

    // Trino's `CREATE OR REPLACE VIEW ... SECURITY INVOKER`.
    let sent = new_version("select id from spark_demo.other_table", &[]);
    commit_view_as(
        &ctx,
        view_params(&prefix, &namespace, "v"),
        sent.clone(),
        trino_engine_request(),
    )
    .await
    .unwrap();

    let mut expected: Vec<ViewUpdate> = serde_json::from_value(sent).unwrap();
    expected.push(ViewUpdate::RemoveProperties {
        removals: vec![OWNER_PROPERTY.to_string()],
    });
    assert_eq!(
        verifier.0.lock().unwrap().clone(),
        vec![expected.clone()],
        "the contract verifiers see the updates Lakekeeper commits"
    );

    // The event is dispatched in the background.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while events.0.lock().unwrap().is_empty() && std::time::Instant::now() < deadline {
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    let recorded = events.0.lock().unwrap().clone();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].updates, expected);
}

#[sqlx::test]
async fn test_commit_view_untrusted_client_can_rename_and_drop_definer_view(pool: PgPool) {
    let (ctx, namespace, whi, _) = views_test_setup(pool, None).await;
    let prefix = whi.to_string();
    create_definer_view(&ctx, &namespace, &prefix, "v", "alice").await;

    Box::pin(rename_view(
        Some(Prefix(prefix.clone())),
        RenameTableRequest {
            source: view_params(&prefix, &namespace, "v").view,
            destination: view_params(&prefix, &namespace, "renamed").view,
        },
        ctx.clone(),
        user_request(),
    ))
    .await
    .unwrap();
    assert_eq!(
        load_owner(&ctx, view_params(&prefix, &namespace, "renamed")).await,
        Some("alice".to_string())
    );

    drop_view(
        view_params(&prefix, &namespace, "renamed"),
        DropParams {
            purge_requested: true,
            force: false,
        },
        ctx.clone(),
        user_request(),
    )
    .await
    .unwrap();
}

#[sqlx::test]
async fn test_commit_view_allows_new_sql_from_trusted_engine_with_owner_property(pool: PgPool) {
    let (ctx, namespace, whi, _) = views_test_setup(pool, None).await;
    let prefix = whi.to_string();
    create_definer_view(&ctx, &namespace, &prefix, "v", "alice").await;

    // Trino's `CREATE OR REPLACE VIEW` of a DEFINER view, run by bob.
    let res = commit_view_as(
        &ctx,
        view_params(&prefix, &namespace, "v"),
        new_version("select id from spark_demo.other_table", &[set_owner("bob")]),
        trino_engine_request(),
    )
    .await
    .unwrap();
    assert_eq!(res.metadata.current_version_id(), 2);
    assert_eq!(current_sql(&res), "select id from spark_demo.other_table");
    assert_eq!(
        res.metadata.properties().get(OWNER_PROPERTY),
        Some(&"bob".to_string())
    );
}

#[sqlx::test]
async fn test_commit_view_definer_replace_from_trusted_engine_keeps_owner(pool: PgPool) {
    let (ctx, namespace, whi, _) = views_test_setup(pool, None).await;
    let prefix = whi.to_string();
    create_definer_view(&ctx, &namespace, &prefix, "v", "alice").await;

    // Trino's `CREATE OR REPLACE VIEW` of a DEFINER view by its owner sends the owner again.
    let res = commit_view_as(
        &ctx,
        view_params(&prefix, &namespace, "v"),
        new_version(
            "select id from spark_demo.other_table",
            &[set_owner("alice")],
        ),
        trino_engine_request(),
    )
    .await
    .unwrap();
    assert_eq!(current_sql(&res), "select id from spark_demo.other_table");
    assert_eq!(
        res.metadata.properties().get(OWNER_PROPERTY),
        Some(&"alice".to_string())
    );
}

#[sqlx::test]
async fn test_commit_view_allows_same_query_version_change_from_trusted_engine(pool: PgPool) {
    let (ctx, namespace, whi, _) = views_test_setup(pool, None).await;
    let prefix = whi.to_string();
    create_definer_view(&ctx, &namespace, &prefix, "v", "alice").await;

    // Trino's `COMMENT ON COLUMN`: a new version with a new schema and the same SQL, and no
    // owner property.
    let res = commit_view_as(
        &ctx,
        view_params(&prefix, &namespace, "v"),
        new_version(CREATED_SQL, &[]),
        trino_engine_request(),
    )
    .await
    .unwrap();
    assert_eq!(res.metadata.current_version_id(), 2);
    assert_eq!(current_sql(&res), CREATED_SQL);
    assert_eq!(
        res.metadata.properties().get(OWNER_PROPERTY),
        Some(&"alice".to_string())
    );
}

#[sqlx::test]
async fn test_commit_view_allows_untrusted_client_to_set_other_properties_on_definer_view(
    pool: PgPool,
) {
    let (ctx, namespace, whi, _) = views_test_setup(pool, None).await;
    let prefix = whi.to_string();
    create_definer_view(&ctx, &namespace, &prefix, "v", "alice").await;

    let res = commit_view_as(
        &ctx,
        view_params(&prefix, &namespace, "v"),
        json!([
            {"action": "set-properties", "updates": {"comment": "orders by day"}},
            {"action": "remove-properties", "removals": ["engine_version"]}
        ]),
        user_request(),
    )
    .await
    .unwrap();
    assert_eq!(res.metadata.current_version_id(), 1);
    assert_eq!(
        res.metadata.properties(),
        &hashmap! {
            OWNER_PROPERTY.to_string() => "alice".to_string(),
            "comment".to_string() => "orders by day".to_string(),
            "create_engine_version".to_string() => "Spark 3.5.1".to_string(),
            "spark.query-column-names".to_string() => "id".to_string(),
        }
    );
}

#[sqlx::test]
async fn test_commit_view_from_trusted_engine_can_change_and_remove_owner_property(pool: PgPool) {
    let (ctx, namespace, whi, _) = views_test_setup(pool, None).await;
    let prefix = whi.to_string();
    create_definer_view(&ctx, &namespace, &prefix, "v", "alice").await;

    let res = commit_view_as(
        &ctx,
        view_params(&prefix, &namespace, "v"),
        json!([set_owner("bob")]),
        trino_engine_request(),
    )
    .await
    .unwrap();
    assert_eq!(
        res.metadata.properties().get(OWNER_PROPERTY),
        Some(&"bob".to_string())
    );

    let res = commit_view_as(
        &ctx,
        view_params(&prefix, &namespace, "v"),
        json!([{"action": "remove-properties", "removals": [OWNER_PROPERTY]}]),
        trino_engine_request(),
    )
    .await
    .unwrap();
    assert_eq!(res.metadata.properties().get(OWNER_PROPERTY), None);
}

#[sqlx::test]
async fn test_commit_view_without_authentication_does_not_protect_owner_property(pool: PgPool) {
    let (mut ctx, namespace, whi, _) = views_test_setup(pool, None).await;
    // A server without authentication: every request is anonymous and none is a trusted
    // engine, so the owner rules do not apply.
    ctx.v1_state.authentication = ServerAuthentication::Disabled;
    let prefix = whi.to_string();
    create_definer_view(&ctx, &namespace, &prefix, "v", "alice").await;

    let res = commit_view_as(
        &ctx,
        view_params(&prefix, &namespace, "v"),
        new_version("select id from spark_demo.other_table", &[]),
        random_request_metadata(),
    )
    .await
    .unwrap();
    assert_eq!(current_sql(&res), "select id from spark_demo.other_table");

    let res = commit_view_as(
        &ctx,
        view_params(&prefix, &namespace, "v"),
        json!([set_owner("bob")]),
        random_request_metadata(),
    )
    .await
    .unwrap();
    assert_eq!(
        res.metadata.properties().get(OWNER_PROPERTY),
        Some(&"bob".to_string())
    );
}

#[sqlx::test]
async fn test_commit_view_on_authenticated_server_refuses_request_without_authentication(
    pool: PgPool,
) {
    let (ctx, namespace, whi, _) = views_test_setup(pool, None).await;
    let prefix = whi.to_string();
    create_definer_view(&ctx, &namespace, &prefix, "v", "alice").await;

    // An in-process call on a server that authenticates requests is not a trusted engine,
    // even though its request carries no authentication.
    for request_metadata in [
        random_request_metadata(),
        RequestMetadata::new_lakekeeper_internal(Uuid::now_v7()),
    ] {
        let err = commit_view_as(
            &ctx,
            view_params(&prefix, &namespace, "v"),
            json!([set_owner("bob")]),
            request_metadata.clone(),
        )
        .await
        .expect_err("a request without authentication is not a trusted engine");
        assert_protected_property_error(&err);

        let err = commit_view_as(
            &ctx,
            view_params(&prefix, &namespace, "v"),
            new_version("select id from spark_demo.other_table", &[]),
            request_metadata,
        )
        .await
        .expect_err("a request without authentication is not a trusted engine");
        assert_protected_version_error(&err);
    }

    assert_eq!(
        load_owner(&ctx, view_params(&prefix, &namespace, "v")).await,
        Some("alice".to_string())
    );
}

fn spark_commit_update_request(
    warehouse_id: WarehouseId,
    asserted_uuid: Option<Uuid>,
) -> CommitViewRequest {
    let uuid = asserted_uuid.map_or("019059cb-9277-7ff0-b71a-537df05b33f8".into(), |u| {
        u.to_string()
    });
    serde_json::from_value(json!({
  "requirements": [
    {
      "type": "assert-view-uuid",
      "warehouse-uuid": *warehouse_id,
      "uuid": &uuid
    }
  ],
  "updates": [
    {
      "action": "set-properties",
      "updates": {
        "create_engine_version": "Spark 3.5.1",
        "spark.query-column-names": "id",
        "engine_version": "Spark 3.5.1"
      }
    },
    {
      "action": "add-schema",
      "schema": {
        "schema-id": 1,
        "type": "struct",
        "fields": [
          {
            "id": 0,
            "name": "id",
            "required": false,
            "type": "long",
            "doc": "id of thing"
          }
        ]
      },
      "last-column-id": 1
    },
    {
      "action": "add-schema",
      "schema": {
        "schema-id": 2,
        "type": "struct",
        "fields": [
          {
            "id": 0,
            "name": "idx",
            "required": false,
            "type": "long",
            "doc": "idx of thing"
          }
        ]
      },
      "last-column-id": 1
    },
    {
      "action": "add-view-version",
      "view-version": {
        "version-id": 2,
        "schema-id": -1,
        "timestamp-ms": Utc::now().timestamp_millis(),
        "summary": {
          "engine-name": "spark",
          "engine-version": "3.5.1",
          "iceberg-version": "Apache Iceberg 1.5.2 (commit cbb853073e681b4075d7c8707610dceecbee3a82)",
          "app-id": "local-1719494665567"
        },
        "representations": [
          {
            "type": "sql",
            "sql": "select id from spark_demo.my_table",
            "dialect": "spark"
          }
        ],
        "default-namespace": []
      }
    },
    {
        "action": "remove-properties",
        "removals": ["engine_version"]
    },
    {
      "action": "set-current-view-version",
      "view-version-id": -1
    }
  ]
})).unwrap()
}
