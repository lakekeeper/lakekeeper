use http::StatusCode;
use iceberg::{NamespaceIdent, TableIdent};
use iceberg_ext::catalog::rest::{CreateViewRequest, RenameTableRequest};
use lakekeeper::{
    api::{
        RequestMetadata,
        iceberg::{types::Prefix, v1::ViewParameters},
    },
    server::views::rename::rename_view,
};
use lakekeeper_integration_tests::{
    create_view_helper, create_view_request, load_view_helper, views_test_setup,
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
