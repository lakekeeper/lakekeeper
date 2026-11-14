use lakekeeper::api::iceberg::{types::Prefix, v1::ViewParameters};
use lakekeeper_integration_tests::{
    create_view_helper, create_view_request, load_view_helper, views_test_setup,
};
use sqlx::PgPool;

#[sqlx::test]
async fn test_load_view(pool: PgPool) {
    let (ctx, namespace, whi, _) = views_test_setup(pool, None).await;

    let view_name = "my-view";
    let rq = create_view_request(Some(view_name), None);
    let prefix = whi.to_string();
    create_view_helper(ctx.clone(), namespace.clone(), rq, Some(prefix.clone()))
        .await
        .expect("create_view should succeed");

    let mut view_ns = namespace.inner();
    view_ns.push(view_name.into());
    let view_ident = iceberg::TableIdent::from_strs(view_ns).unwrap();

    let loaded_view = load_view_helper(
        ctx,
        ViewParameters {
            prefix: Some(Prefix(prefix)),
            view: view_ident,
        },
    )
    .await
    .expect("load_view should succeed");

    assert_eq!(loaded_view.metadata.current_version().schema_id(), 0);
}
