use http::StatusCode;
use iceberg::TableIdent;
use iceberg_ext::catalog::rest::CreateViewRequest;
use lakekeeper::{
    WarehouseId,
    api::{
        RequestMetadata,
        iceberg::{
            types::{DropParams, Prefix},
            v1::ViewParameters,
        },
        management::v1::{
            ApiServer as ManagementApiServer,
            tasks::{ListTasksRequest, Service, WarehouseTaskEntityFilter},
            view::ViewManagementService,
        },
    },
    server::views::drop::drop_view,
    service::tasks::{
        WarehouseTaskEntityId, tabular_expiration_queue::QUEUE_NAME as EXPIRATION_QUEUE_NAME,
    },
};
use lakekeeper_integration_tests::{
    create_view_helper, create_view_request, load_view_helper, random_request_metadata,
    views_test_setup,
};
use sqlx::PgPool;

#[sqlx::test]
async fn test_drop_view(pool: PgPool) {
    let (api_context, namespace, whi, _) = views_test_setup(pool, None).await;

    let view_name = "my-view";
    let rq: CreateViewRequest = create_view_request(Some(view_name), None);

    let prefix = &whi.to_string();
    let created_view = create_view_helper(
        api_context.clone(),
        namespace.clone(),
        rq,
        Some(prefix.into()),
    )
    .await
    .unwrap();
    let mut table_ident = namespace.clone().inner();
    table_ident.push(view_name.into());

    let loaded_view = load_view_helper(
        api_context.clone(),
        ViewParameters {
            prefix: Some(Prefix(prefix.clone())),
            view: TableIdent::from_strs(&table_ident).unwrap(),
        },
    )
    .await
    .expect("View should be loadable");
    assert_eq!(loaded_view.metadata, created_view.metadata);
    drop_view(
        ViewParameters {
            prefix: Some(Prefix(prefix.clone())),
            view: TableIdent::from_strs(&table_ident).unwrap(),
        },
        DropParams {
            purge_requested: true,
            force: false,
        },
        api_context.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .expect("View should be droppable");

    let error = load_view_helper(
        api_context.clone(),
        ViewParameters {
            prefix: Some(Prefix(prefix.clone())),
            view: TableIdent::from_strs(table_ident).unwrap(),
        },
    )
    .await
    .expect_err("View should no longer exist");

    assert_eq!(error.error.code, StatusCode::NOT_FOUND);

    // Load expiration task
    let entity = WarehouseTaskEntityId::View {
        view_id: loaded_view.metadata.uuid().into(),
    };
    let expiration_tasks = ManagementApiServer::list_tasks(
        whi,
        ListTasksRequest::builder()
            .entities(Some(vec![WarehouseTaskEntityFilter::View {
                view_id: loaded_view.metadata.uuid().into(),
            }]))
            .build(),
        api_context,
        random_request_metadata(),
    )
    .await
    .unwrap();
    assert_eq!(expiration_tasks.tasks.len(), 1);
    let task = &expiration_tasks.tasks[0];
    assert_eq!(task.entity, Some(entity));
}

#[sqlx::test]
async fn test_cannot_drop_protected_view(pool: PgPool) {
    let (api_context, namespace, whi, _) = views_test_setup(pool, None).await;

    let view_name = "my-view";
    let create_request = create_view_request(Some(view_name), None);

    let prefix = &whi.to_string();
    let created_view = create_view_helper(
        api_context.clone(),
        namespace.clone(),
        create_request,
        Some(prefix.into()),
    )
    .await
    .unwrap();
    let mut table_ident = namespace.clone().inner();
    table_ident.push(view_name.into());

    let view_ident = TableIdent::new(namespace.clone(), view_name.to_string());
    let loaded_view = load_view_helper(
        api_context.clone(),
        ViewParameters {
            prefix: Some(Prefix(prefix.clone())),
            view: view_ident.clone(),
        },
    )
    .await
    .expect("View should be loadable");
    assert_eq!(loaded_view.metadata, created_view.metadata);

    ManagementApiServer::set_view_protection(
        loaded_view.metadata.uuid().into(),
        whi,
        true,
        api_context.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    let e = drop_view(
        ViewParameters {
            prefix: Some(Prefix(prefix.clone())),
            view: view_ident,
        },
        DropParams {
            purge_requested: true,
            force: false,
        },
        api_context.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .expect_err("Protected View should not be droppable");

    assert_eq!(e.error.code, StatusCode::CONFLICT, "{}", e.error);

    ManagementApiServer::set_view_protection(
        loaded_view.metadata.uuid().into(),
        WarehouseId::from_str_or_internal(prefix.as_str()).unwrap(),
        false,
        api_context.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    drop_view(
        ViewParameters {
            prefix: Some(Prefix(prefix.clone())),
            view: TableIdent::from_strs(&table_ident).unwrap(),
        },
        DropParams {
            purge_requested: true,
            force: false,
        },
        api_context.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .expect("Unprotected View should be droppable");

    let error = load_view_helper(
        api_context,
        ViewParameters {
            prefix: Some(Prefix(prefix.clone())),
            view: TableIdent::from_strs(table_ident).unwrap(),
        },
    )
    .await
    .expect_err("View should no longer exist");

    assert_eq!(error.error.code, StatusCode::NOT_FOUND);
}

#[sqlx::test]
async fn test_can_force_drop_protected_view(pool: PgPool) {
    let (api_context, namespace, whi, _) = views_test_setup(pool, None).await;

    let view_name = "my-view";
    let rq: CreateViewRequest = create_view_request(Some(view_name), None);

    let prefix = &whi.to_string();
    let created_view = create_view_helper(
        api_context.clone(),
        namespace.clone(),
        rq,
        Some(prefix.into()),
    )
    .await
    .unwrap();
    let mut table_ident = namespace.clone().inner();
    table_ident.push(view_name.into());

    let loaded_view = load_view_helper(
        api_context.clone(),
        ViewParameters {
            prefix: Some(Prefix(prefix.clone())),
            view: TableIdent::from_strs(&table_ident).unwrap(),
        },
    )
    .await
    .expect("View should be loadable");
    assert_eq!(loaded_view.metadata, created_view.metadata);

    ManagementApiServer::set_view_protection(
        loaded_view.metadata.uuid().into(),
        WarehouseId::from_str_or_internal(prefix.as_str()).unwrap(),
        true,
        api_context.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();

    drop_view(
        ViewParameters {
            prefix: Some(Prefix(prefix.clone())),
            view: TableIdent::from_strs(&table_ident).unwrap(),
        },
        DropParams {
            purge_requested: true,
            force: true,
        },
        api_context.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .expect("Protected View should be droppable via force");

    let error = load_view_helper(
        api_context.clone(),
        ViewParameters {
            prefix: Some(Prefix(prefix.clone())),
            view: TableIdent::from_strs(table_ident).unwrap(),
        },
    )
    .await
    .expect_err("View should no longer exist");

    assert_eq!(error.error.code, StatusCode::NOT_FOUND);

    // force=true must perform an immediate hard delete even in a soft-delete warehouse:
    // no tabular_expiration task should be scheduled for the view.
    let expiration_tasks = ManagementApiServer::list_tasks(
        whi,
        ListTasksRequest::builder()
            .entities(Some(vec![WarehouseTaskEntityFilter::View {
                view_id: loaded_view.metadata.uuid().into(),
            }]))
            .queue_name(Some(vec![EXPIRATION_QUEUE_NAME.clone()]))
            .build(),
        api_context,
        random_request_metadata(),
    )
    .await
    .unwrap();
    assert_eq!(
        expiration_tasks.tasks.len(),
        0,
        "force-drop in soft-delete warehouse must not schedule a tabular_expiration task"
    );
}
