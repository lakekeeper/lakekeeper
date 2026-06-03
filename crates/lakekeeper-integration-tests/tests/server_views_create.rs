use iceberg::NamespaceIdent;
use lakekeeper_integration_tests::{create_view_helper, create_view_request, views_test_setup};
use lakekeeper_storage_postgres::namespace::tests::initialize_namespace;
use sqlx::PgPool;
use uuid::Uuid;

#[sqlx::test]
async fn test_create_view(pool: PgPool) {
    let (api_context, namespace, whi, _) = views_test_setup(pool, None).await;

    let mut rq = create_view_request(None, None);

    let _view = create_view_helper(
        api_context.clone(),
        namespace.clone(),
        rq.clone(),
        Some(whi.to_string()),
    )
    .await
    .unwrap();
    let view = create_view_helper(
        api_context.clone(),
        namespace.clone(),
        rq.clone(),
        Some(whi.to_string()),
    )
    .await
    .expect_err("Recreate with same ident should fail.");
    assert_eq!(view.error.code, 409);
    let old_name = rq.name.clone();
    rq.name = "some-other-name".to_string();

    let _view = create_view_helper(
        api_context.clone(),
        namespace,
        rq.clone(),
        Some(whi.to_string()),
    )
    .await
    .expect("Recreate with with another name it should work");

    rq.name = old_name;
    let namespace = NamespaceIdent::from_vec(vec![Uuid::now_v7().to_string()]).unwrap();
    let new_ns = initialize_namespace(api_context.v1_state.catalog.clone(), whi, &namespace, None)
        .await
        .namespace_ident()
        .clone();

    let _view = create_view_helper(api_context, new_ns, rq, Some(whi.to_string()))
        .await
        .expect("Recreate with same name but different ns should work.");
}
