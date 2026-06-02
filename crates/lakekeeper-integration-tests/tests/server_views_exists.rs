use iceberg::TableIdent;
use iceberg_ext::catalog::rest::CreateViewRequest;
use lakekeeper::{
    api::{
        RequestMetadata,
        iceberg::{types::Prefix, v1::ViewParameters},
    },
    server::views::exists::view_exists,
    service::authz::AllowAllAuthorizer,
};
use lakekeeper_integration_tests::{create_view_helper, create_view_request, views_test_setup};
use lakekeeper_storage_postgres::{PostgresBackend, SecretsState};
use sqlx::PgPool;

#[sqlx::test]
async fn test_view_exists(pool: PgPool) {
    let (api_context, namespace, whi, _) = views_test_setup(pool, None).await;

    let view_name = "my-view";
    let rq: CreateViewRequest = create_view_request(Some(view_name), None);

    let prefix = Prefix(whi.to_string());
    let _ = create_view_helper(
        api_context.clone(),
        namespace.clone(),
        rq,
        Some(prefix.clone().into_string()),
    )
    .await
    .unwrap();
    view_exists::<lakekeeper_storage_postgres::PostgresBackend, AllowAllAuthorizer, SecretsState>(
        ViewParameters {
            prefix: Some(prefix.clone()),
            view: TableIdent {
                namespace: namespace.clone(),
                name: view_name.to_string(),
            },
        },
        api_context.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    let non_exist = view_exists::<PostgresBackend, AllowAllAuthorizer, SecretsState>(
        ViewParameters {
            prefix: Some(prefix.clone()),
            view: TableIdent {
                namespace: namespace.clone(),
                name: "123".to_string(),
            },
        },
        api_context.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap_err();

    assert_eq!(non_exist.error.code, http::StatusCode::NOT_FOUND);
}
