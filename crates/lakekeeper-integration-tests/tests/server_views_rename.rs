use http::StatusCode;
use iceberg::{NamespaceIdent, TableIdent};
use iceberg_ext::catalog::rest::{CreateViewRequest, RenameTableRequest};
use lakekeeper::{
    api::{
        RequestMetadata,
        iceberg::{types::Prefix, v1::ViewParameters},
        management::v1::warehouse::TabularDeleteProfile,
    },
    server::views::rename::rename_view,
    service::{UserId, authz::tests::HidingAuthorizer},
};
use lakekeeper_integration_tests::{
    create_ns, create_view, create_view_helper, create_view_request, load_view_helper,
    memory_io_profile, setup_simple, views_test_setup,
};
use lakekeeper_storage_postgres::namespace::tests::initialize_namespace;
use sqlx::PgPool;

#[sqlx::test]
async fn test_rename_view_without_namespace(pool: PgPool) {
    let (api_context, namespace, whi, _) = views_test_setup(pool, None).await;

    let view_name = "my-view";
    let rq: CreateViewRequest = create_view_request(Some(view_name), None);

    let prefix = Prefix(whi.to_string());
    let created_view = create_view_helper(
        api_context.clone(),
        namespace.clone(),
        rq,
        Some(prefix.clone().into_string()),
    )
    .await
    .unwrap();
    let destination = TableIdent {
        namespace: namespace.clone(),
        name: "my-renamed-view".to_string(),
    };
    let source = TableIdent {
        namespace: namespace.clone(),
        name: view_name.to_string(),
    };
    rename_view(
        Some(prefix.clone()),
        RenameTableRequest {
            source: source.clone(),
            destination: destination.clone(),
        },
        api_context.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    let exists = load_view_helper(
        api_context.clone(),
        ViewParameters {
            view: destination,
            prefix: Some(prefix.clone()),
        },
    )
    .await
    .unwrap();

    let not_exists = load_view_helper(
        api_context.clone(),
        ViewParameters {
            view: source,
            prefix: Some(prefix.clone()),
        },
    )
    .await
    .expect_err("View should not exist after renaming.");

    assert_eq!(created_view, exists);
    assert_eq!(StatusCode::NOT_FOUND, not_exists.error.code);
}

#[sqlx::test]
async fn test_rename_view_with_namespace(pool: PgPool) {
    let (api_context, _, whi, _) = views_test_setup(pool, None).await;
    let namespace = NamespaceIdent::from_vec(vec!["Someother-ns".to_string()]).unwrap();
    let new_ns = initialize_namespace(api_context.v1_state.catalog.clone(), whi, &namespace, None)
        .await
        .namespace_ident()
        .clone();

    let view_name = "my-view";
    let rq: CreateViewRequest = create_view_request(Some(view_name), None);

    let prefix = Prefix(whi.to_string());
    let created_view = create_view_helper(
        api_context.clone(),
        namespace.clone(),
        rq,
        Some(prefix.clone().into_string()),
    )
    .await
    .unwrap();
    let destination = TableIdent {
        namespace: new_ns.clone(),
        name: "my-renamed-view".to_string(),
    };
    let source = TableIdent {
        namespace: namespace.clone(),
        name: view_name.to_string(),
    };
    rename_view(
        Some(prefix.clone()),
        RenameTableRequest {
            source: source.clone(),
            destination: destination.clone(),
        },
        api_context.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    let exists = load_view_helper(
        api_context.clone(),
        ViewParameters {
            view: destination,
            prefix: Some(prefix.clone()),
        },
    )
    .await
    .unwrap();

    let not_exists = load_view_helper(
        api_context.clone(),
        ViewParameters {
            view: source,
            prefix: Some(prefix.clone()),
        },
    )
    .await
    .expect_err("View should not exist after renaming.");

    assert_eq!(created_view, exists);
    assert_eq!(StatusCode::NOT_FOUND, not_exists.error.code);
}

/// Two namespaces and a view `from_ns.v`, under a `HidingAuthorizer`.
async fn move_view_setup(
    pool: PgPool,
) -> (
    lakekeeper::api::ApiContext<
        lakekeeper::service::State<
            HidingAuthorizer,
            lakekeeper_storage_postgres::PostgresBackend,
            lakekeeper_storage_postgres::SecretsState,
        >,
    >,
    HidingAuthorizer,
    String,
) {
    let authz = HidingAuthorizer::new();
    let (ctx, warehouse) = setup_simple(
        pool,
        memory_io_profile(),
        None,
        authz.clone(),
        TabularDeleteProfile::Hard {},
        Some(UserId::new_unchecked("oidc", "test-user-id")),
    )
    .await;
    let prefix = warehouse.warehouse_id.to_string();
    create_ns(ctx.clone(), prefix.clone(), "from_ns".to_string()).await;
    create_ns(ctx.clone(), prefix.clone(), "to_ns".to_string()).await;
    create_view(ctx.clone(), &prefix, "from_ns", "v", None)
        .await
        .unwrap();
    (ctx, authz, prefix)
}

fn view_ident(namespace: &str, name: &str) -> TableIdent {
    TableIdent::new(NamespaceIdent::new(namespace.to_string()), name.to_string())
}

/// Moving a view into another namespace needs `move` on the view, not just `rename`.
#[sqlx::test]
async fn test_move_view_without_move_on_source(pool: PgPool) {
    let (ctx, authz, prefix) = move_view_setup(pool).await;
    authz.block_action("view:Move");

    let err = rename_view(
        Some(Prefix(prefix.clone())),
        RenameTableRequest {
            source: view_ident("from_ns", "v"),
            destination: view_ident("to_ns", "v"),
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.code, StatusCode::FORBIDDEN, "{err:?}");
    assert_eq!(err.error.r#type, "ViewActionForbidden");

    // A rename within the namespace does not ask `move`.
    rename_view(
        Some(Prefix(prefix)),
        RenameTableRequest {
            source: view_ident("from_ns", "v"),
            destination: view_ident("from_ns", "renamed"),
        },
        ctx,
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();
}

/// Moving a view into another namespace needs `accept_moved_tabular` on the destination,
/// not just `create_view`; the refusal is recorded as `move`.
#[sqlx::test]
async fn test_move_view_without_accept_moved_tabular_on_destination(pool: PgPool) {
    use lakekeeper::service::events::{EventListener, context::ActionContextKey};
    use lakekeeper_integration_tests::{CapturingAuthzListener, RecordedAction};

    let (ctx, authz, prefix) = move_view_setup(pool).await;
    let listener = std::sync::Arc::new(CapturingAuthzListener::default());
    ctx.v1_state
        .events
        .append(listener.clone() as std::sync::Arc<dyn EventListener>)
        .await;
    authz.block_action("namespace:AcceptMovedTabular");

    let err = rename_view(
        Some(Prefix(prefix)),
        RenameTableRequest {
            source: view_ident("from_ns", "v"),
            destination: view_ident("to_ns", "v"),
        },
        ctx,
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.code, StatusCode::FORBIDDEN, "{err:?}");
    assert_eq!(err.error.r#type, "NamespaceActionForbidden");

    assert_eq!(listener.settled_counts(0, 1).await, (0, 1));
    assert_eq!(
        listener.recorded_actions(),
        (
            vec![],
            vec![vec![RecordedAction {
                action_name: "move".to_string(),
                context: vec![ActionContextKey::Destination(vec!["to_ns".to_string()])],
            }]],
        )
    );
}
