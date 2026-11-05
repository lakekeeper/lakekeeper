use http::StatusCode;
use iceberg::NamespaceIdent;
use lakekeeper::{
    api::iceberg::{
        types::Prefix,
        v1::{DataAccess, NamespaceParameters},
    },
    server::views::create::create_view,
    service::ServerAuthentication,
};
use lakekeeper_integration_tests::{
    OWNER_PROPERTY, REFUSED_NEW_TABULAR_NAMES, create_view_helper, create_view_request,
    memory_files_under, put_memory_file, random_request_metadata, trino_engine_request,
    user_request, views_test_setup,
};
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

/// A view cannot be created in a location that holds files, as a table cannot: dropping the
/// view removes its location with all its files.
#[sqlx::test]
async fn test_create_view_refuses_location_with_files(pool: PgPool) {
    let (api_context, namespace, whi, _) = views_test_setup(pool, None).await;
    let first = create_view_helper(
        api_context.clone(),
        namespace.clone(),
        create_view_request(Some("first"), None),
        Some(whi.to_string()),
    )
    .await
    .unwrap();
    let (parent, _) = first.metadata.location().rsplit_once('/').unwrap();
    let occupied = format!("{parent}/occupied");
    let foreign_file = format!("{occupied}/data/foreign.parquet");
    put_memory_file(&foreign_file).await;

    let err = create_view_helper(
        api_context.clone(),
        namespace.clone(),
        create_view_request(Some("second"), Some(&occupied)),
        Some(whi.to_string()),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.code, StatusCode::BAD_REQUEST);
    assert_eq!(err.error.r#type, "InvalidLocation");
    assert_eq!(memory_files_under(&occupied).await, vec![foreign_file]);
}

/// The test server configures no trusted engine. The owner property is protected anyway: a
/// client that is not a trusted engine must not create a view that runs as another user.
#[sqlx::test]
async fn test_create_view_with_owner_property_refused_from_untrusted_client(pool: PgPool) {
    let (api_context, namespace, whi, _) = views_test_setup(pool, None).await;

    for key in [OWNER_PROPERTY, "Trino.Run-As-Owner"] {
        let mut rq = create_view_request(Some("definer_view"), None);
        rq.properties.insert(key.to_string(), "admin".to_string());

        let err = Box::pin(create_view(
            NamespaceParameters {
                prefix: Some(Prefix(whi.to_string())),
                namespace: namespace.clone(),
            },
            rq,
            api_context.clone(),
            DataAccess {
                vended_credentials: true,
                remote_signing: false,
            },
            user_request(),
        ))
        .await
        .expect_err("an untrusted client must not create a view that runs as another user");
        assert_eq!(err.error.code, StatusCode::FORBIDDEN);
        assert_eq!(err.error.r#type, "ProtectedPropertyModification");
    }

    // Nothing was created: the name is still free.
    create_view_helper(
        api_context,
        namespace,
        create_view_request(Some("definer_view"), None),
        Some(whi.to_string()),
    )
    .await
    .expect("the refused create must not leave a view behind");
}

#[sqlx::test]
async fn test_create_view_with_owner_property_from_trusted_engine(pool: PgPool) {
    let (api_context, namespace, whi, _) = views_test_setup(pool, None).await;

    let mut rq = create_view_request(Some("definer_view"), None);
    rq.properties
        .insert(OWNER_PROPERTY.to_string(), "alice".to_string());

    let view = Box::pin(create_view(
        NamespaceParameters {
            prefix: Some(Prefix(whi.to_string())),
            namespace,
        },
        rq,
        api_context,
        DataAccess {
            vended_credentials: true,
            remote_signing: false,
        },
        trino_engine_request(),
    ))
    .await
    .unwrap();
    assert_eq!(
        view.metadata.properties().get(OWNER_PROPERTY),
        Some(&"alice".to_string())
    );
}

/// A server without authentication cannot trust any client as an engine, so the owner
/// property is not protected there.
#[sqlx::test]
async fn test_create_view_with_owner_property_without_authentication(pool: PgPool) {
    let (mut api_context, namespace, whi, _) = views_test_setup(pool, None).await;
    api_context.v1_state.authentication = ServerAuthentication::Disabled;

    let mut rq = create_view_request(Some("definer_view"), None);
    rq.properties
        .insert(OWNER_PROPERTY.to_string(), "alice".to_string());

    let view = Box::pin(create_view(
        NamespaceParameters {
            prefix: Some(Prefix(whi.to_string())),
            namespace,
        },
        rq,
        api_context,
        DataAccess {
            vended_credentials: true,
            remote_signing: false,
        },
        random_request_metadata(),
    ))
    .await
    .unwrap();
    assert_eq!(
        view.metadata.properties().get(OWNER_PROPERTY),
        Some(&"alice".to_string())
    );
}

/// On a server that authenticates requests, an in-process call without authentication is not a
/// trusted engine.
#[sqlx::test]
async fn test_create_view_with_owner_property_refused_from_request_without_authentication(
    pool: PgPool,
) {
    let (api_context, namespace, whi, _) = views_test_setup(pool, None).await;

    let mut rq = create_view_request(Some("definer_view"), None);
    rq.properties
        .insert(OWNER_PROPERTY.to_string(), "alice".to_string());

    let err = Box::pin(create_view(
        NamespaceParameters {
            prefix: Some(Prefix(whi.to_string())),
            namespace,
        },
        rq,
        api_context,
        DataAccess {
            vended_credentials: true,
            remote_signing: false,
        },
        random_request_metadata(),
    ))
    .await
    .expect_err("a request without authentication is not a trusted engine");
    assert_eq!(err.error.code, StatusCode::FORBIDDEN);
    assert_eq!(err.error.r#type, "ProtectedPropertyModification");
}

#[sqlx::test]
async fn test_create_view_refuses_invalid_names(pool: PgPool) {
    let (api_context, namespace, whi, _) = views_test_setup(pool, None).await;

    for name in REFUSED_NEW_TABULAR_NAMES {
        let err = create_view_helper(
            api_context.clone(),
            namespace.clone(),
            create_view_request(Some(name), None),
            Some(whi.to_string()),
        )
        .await
        .expect_err("The view name must be refused.");
        assert_eq!(err.error.code, 400, "{name:?}");
        assert_eq!(err.error.r#type, "InvalidTableName", "{name:?}");
    }
}
