use http::StatusCode;
use iceberg::{NamespaceIdent, TableIdent, spec::ViewMetadata};
use iceberg_ext::catalog::rest::{LoadViewResult, RegisterViewRequest};
use lakekeeper::{
    WarehouseId,
    api::{
        ApiContext, RequestMetadata,
        iceberg::{
            types::Prefix,
            v1::{
                DataAccess, NamespaceParameters, ViewParameters,
                views::{LoadViewRequest, ViewService},
            },
        },
    },
    server::CatalogServer,
    service::{State, authz::Authorizer},
};
use lakekeeper_integration_tests::{
    OWNER_PROPERTY, create_view_helper, create_view_request, memory_files_under, user_request,
    views_test_setup,
};
use lakekeeper_io::LakekeeperStorage as _;
use lakekeeper_storage_postgres::{PostgresBackend, SecretsState, test_utils::memory_io_profile};
use sqlx::PgPool;
use uuid::Uuid;

type Ctx<A> = ApiContext<State<A, PostgresBackend, SecretsState>>;

async fn register_view<A: Authorizer + Clone>(
    ctx: Ctx<A>,
    warehouse_id: WarehouseId,
    namespace: NamespaceIdent,
    name: &str,
    metadata_location: &str,
    request_metadata: RequestMetadata,
) -> lakekeeper::api::Result<LoadViewResult> {
    <CatalogServer<PostgresBackend, A, SecretsState> as ViewService<
        State<A, PostgresBackend, SecretsState>,
    >>::register_view(
        NamespaceParameters {
            prefix: Some(Prefix(warehouse_id.to_string())),
            namespace,
        },
        RegisterViewRequest::builder()
            .name(name.to_string())
            .metadata_location(metadata_location.to_string())
            .build(),
        ctx,
        DataAccess::not_specified(),
        request_metadata,
    )
    .await
}

async fn load_view<A: Authorizer + Clone>(
    ctx: Ctx<A>,
    warehouse_id: WarehouseId,
    namespace: NamespaceIdent,
    name: &str,
) -> lakekeeper::api::Result<LoadViewResult> {
    <CatalogServer<PostgresBackend, A, SecretsState> as ViewService<
        State<A, PostgresBackend, SecretsState>,
    >>::load_view(
        ViewParameters {
            prefix: Some(Prefix(warehouse_id.to_string())),
            view: TableIdent::new(namespace, name.to_string()),
        },
        LoadViewRequest::default(),
        ctx,
        RequestMetadata::new_unauthenticated(),
    )
    .await
}

/// A view metadata file to write to the in-memory storage of the test's thread.
struct ViewFile {
    metadata: serde_json::Value,
}

impl ViewFile {
    /// A copy of `template` as another view: a new UUID, at `location`.
    fn from_template(template: &ViewMetadata, location: &str) -> Self {
        let mut metadata = serde_json::to_value(template).unwrap();
        metadata["view-uuid"] = serde_json::json!(Uuid::now_v7());
        metadata["location"] = serde_json::json!(location);
        Self { metadata }
    }

    fn uuid(&self) -> Uuid {
        self.metadata["view-uuid"]
            .as_str()
            .unwrap()
            .parse()
            .unwrap()
    }

    fn location(&self) -> &str {
        self.metadata["location"].as_str().unwrap()
    }

    /// Writes the file below the view location and returns its location.
    async fn write(&self) -> String {
        self.write_under(self.location()).await
    }

    /// Writes the file below `location` and returns its location.
    async fn write_under(&self, location: &str) -> String {
        let file = format!("{location}/metadata/00000-{}.metadata.json", Uuid::now_v7());
        let json = serde_json::to_vec(&self.metadata).unwrap();
        let io = memory_io_profile().file_io(None).await.unwrap();
        io.write(&file, json.into()).await.unwrap();
        file
    }
}

/// A warehouse with a namespace and one created view, `template`, whose metadata the
/// registered files copy. Returns a free view location next to the template's.
async fn register_setup(
    pool: PgPool,
) -> (
    Ctx<lakekeeper::service::authz::AllowAllAuthorizer>,
    NamespaceIdent,
    WarehouseId,
    ViewMetadata,
    String,
) {
    let (ctx, namespace, warehouse_id, _) = views_test_setup(pool, None).await;
    let template = create_view_helper(
        ctx.clone(),
        namespace.clone(),
        create_view_request(Some("template"), None),
        Some(warehouse_id.to_string()),
    )
    .await
    .unwrap();
    let (parent, _) = template.metadata.location().rsplit_once('/').unwrap();
    let free_location = format!("{parent}/registered-{}", Uuid::now_v7());
    (
        ctx,
        namespace,
        warehouse_id,
        template.metadata.as_ref().clone(),
        free_location,
    )
}

/// The registered view is tracked at the file the caller named. No metadata file is written,
/// and the response is what `loadView` returns.
#[lakekeeper_storage_postgres::db_test]
async fn test_register_view(pool: PgPool) {
    let (ctx, namespace, warehouse_id, template, location) = register_setup(pool).await;
    let file = ViewFile::from_template(&template, &location);
    let metadata_location = file.write().await;

    let registered = register_view(
        ctx.clone(),
        warehouse_id,
        namespace.clone(),
        "registered",
        &metadata_location,
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    assert_eq!(registered.metadata_location, metadata_location);
    assert_eq!(registered.metadata.uuid(), file.uuid());
    assert_eq!(registered.config, None);
    let loaded = load_view(ctx, warehouse_id, namespace, "registered")
        .await
        .unwrap();
    assert_eq!(loaded, registered);
    assert_eq!(memory_files_under(&location).await, vec![metadata_location]);
}

/// A view dropped without purge leaves its gzip-compressed metadata file in storage. In a
/// warehouse without soft deletion, registering that file restores the view at it.
#[lakekeeper_storage_postgres::db_test]
async fn test_register_view_from_gzip_file(pool: PgPool) {
    use lakekeeper::api::{
        iceberg::types::DropParams, management::v1::warehouse::TabularDeleteProfile,
    };

    let (ctx, warehouse_id, namespace, _, _) =
        setup_with_template(pool, TabularDeleteProfile::Hard {}).await;
    let mut request = create_view_request(Some("dropped"), None);
    request.properties.insert(
        "write.metadata.compression-codec".to_string(),
        "gzip".to_string(),
    );
    let dropped = create_view_helper(
        ctx.clone(),
        namespace.clone(),
        request,
        Some(warehouse_id.to_string()),
    )
    .await
    .unwrap();
    let metadata_location = dropped.metadata_location.clone();
    let io = memory_io_profile().file_io(None).await.unwrap();
    let content = io.read(&metadata_location).await.unwrap().bytes;
    assert!(
        content.starts_with(&[0x1F, 0x8B]),
        "{metadata_location} is not gzip-compressed"
    );
    <CatalogServer<PostgresBackend, _, SecretsState> as ViewService<_>>::drop_view(
        ViewParameters {
            prefix: Some(Prefix(warehouse_id.to_string())),
            view: TableIdent::new(namespace.clone(), "dropped".to_string()),
        },
        DropParams {
            purge_requested: false,
            force: false,
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    let registered = register_view(
        ctx.clone(),
        warehouse_id,
        namespace.clone(),
        "registered",
        &metadata_location,
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    assert_eq!(registered.metadata_location, metadata_location);
    assert_eq!(registered.metadata.as_ref(), dropped.metadata.as_ref());
    let loaded = load_view(ctx, warehouse_id, namespace, "registered")
        .await
        .unwrap();
    assert_eq!(loaded, registered);
}

/// Lakekeeper reads view format version 1 only, so it cannot track a file of another version.
#[lakekeeper_storage_postgres::db_test]
async fn test_register_view_refuses_unknown_format_version(pool: PgPool) {
    let (ctx, namespace, warehouse_id, template, location) = register_setup(pool).await;
    let mut file = ViewFile::from_template(&template, &location);
    file.metadata["format-version"] = serde_json::json!(2);
    let metadata_location = file.write().await;

    let err = register_view(
        ctx.clone(),
        warehouse_id,
        namespace.clone(),
        "registered",
        &metadata_location,
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap_err();

    assert_eq!(err.error.code, StatusCode::BAD_REQUEST, "{err:?}");
    assert_eq!(err.error.r#type, "Deserialization");
    let err = load_view(ctx, warehouse_id, namespace, "registered")
        .await
        .unwrap_err();
    assert_eq!(err.error.code, StatusCode::NOT_FOUND, "{err:?}");
}

#[lakekeeper_storage_postgres::db_test]
async fn test_register_view_in_missing_namespace(pool: PgPool) {
    let (ctx, _, warehouse_id, template, location) = register_setup(pool).await;
    let metadata_location = ViewFile::from_template(&template, &location).write().await;
    let missing = NamespaceIdent::new(format!("missing-{}", Uuid::now_v7()));

    let err = register_view(
        ctx,
        warehouse_id,
        missing,
        "registered",
        &metadata_location,
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap_err();

    assert_eq!(err.error.code, StatusCode::NOT_FOUND, "{err:?}");
    assert_eq!(err.error.r#type, "NoSuchNamespaceException");
}

#[lakekeeper_storage_postgres::db_test]
async fn test_register_view_refuses_metadata_outside_the_warehouse(pool: PgPool) {
    let (ctx, namespace, warehouse_id, template, location) = register_setup(pool).await;
    let outside = format!("memory://outside-{}", Uuid::now_v7());
    let metadata_location = ViewFile::from_template(&template, &location)
        .write_under(&outside)
        .await;

    let err = register_view(
        ctx,
        warehouse_id,
        namespace,
        "registered",
        &metadata_location,
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap_err();

    assert_eq!(err.error.code, StatusCode::BAD_REQUEST, "{err:?}");
    assert_eq!(err.error.r#type, "InvalidLocation");
}

#[lakekeeper_storage_postgres::db_test]
async fn test_register_view_refuses_view_location_outside_the_warehouse(pool: PgPool) {
    let (ctx, namespace, warehouse_id, template, location) = register_setup(pool).await;
    let outside = format!("memory://outside-{}/view", Uuid::now_v7());
    let metadata_location = ViewFile::from_template(&template, &outside)
        .write_under(&location)
        .await;

    let err = register_view(
        ctx.clone(),
        warehouse_id,
        namespace.clone(),
        "registered",
        &metadata_location,
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap_err();

    assert_eq!(err.error.code, StatusCode::BAD_REQUEST, "{err:?}");
    assert_eq!(err.error.r#type, "InvalidLocation");
    let err = load_view(ctx, warehouse_id, namespace, "registered")
        .await
        .unwrap_err();
    assert_eq!(err.error.code, StatusCode::NOT_FOUND, "{err:?}");
}

/// The test server configures no trusted engine. A file that names the user engines run the
/// view as is refused as a create with that property is: registering must not adopt a view
/// that runs as another user.
#[lakekeeper_storage_postgres::db_test]
async fn test_register_view_with_owner_property_refused_from_untrusted_client(pool: PgPool) {
    let (ctx, namespace, warehouse_id, template, location) = register_setup(pool).await;
    let mut file = ViewFile::from_template(&template, &location);
    file.metadata["properties"][OWNER_PROPERTY] = serde_json::json!("admin");
    let metadata_location = file.write().await;

    let err = register_view(
        ctx.clone(),
        warehouse_id,
        namespace.clone(),
        "registered",
        &metadata_location,
        user_request(),
    )
    .await
    .unwrap_err();

    assert_eq!(err.error.code, StatusCode::FORBIDDEN, "{err:?}");
    assert_eq!(err.error.r#type, "ProtectedPropertyModification");
    let err = load_view(ctx, warehouse_id, namespace, "registered")
        .await
        .unwrap_err();
    assert_eq!(err.error.code, StatusCode::NOT_FOUND, "{err:?}");
}

/// A warehouse on a [`HidingAuthorizer`](lakekeeper::service::authz::tests::HidingAuthorizer)
/// with a namespace and one created view, `template`, whose metadata the registered files copy.
/// Returns a free view location next to the template's.
async fn hiding_register_setup(
    pool: PgPool,
) -> (
    Ctx<lakekeeper::service::authz::tests::HidingAuthorizer>,
    lakekeeper::service::authz::tests::HidingAuthorizer,
    NamespaceIdent,
    WarehouseId,
    ViewMetadata,
    String,
) {
    use lakekeeper::{
        api::management::v1::warehouse::TabularDeleteProfile,
        service::{UserId, authz::tests::HidingAuthorizer},
    };
    use lakekeeper_integration_tests::{create_ns, create_view, setup_simple};

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
    let warehouse_id = warehouse.warehouse_id;
    let prefix = warehouse_id.to_string();
    let namespace = create_ns(ctx.clone(), prefix.clone(), "reg_ns".to_string())
        .await
        .namespace;
    let template = create_view(ctx.clone(), &prefix, "reg_ns", "template", None)
        .await
        .unwrap();
    let (parent, _) = template.metadata.location().rsplit_once('/').unwrap();
    let location = format!("{parent}/registered-{}", Uuid::now_v7());
    (
        ctx,
        authz,
        namespace,
        warehouse_id,
        template.metadata.as_ref().clone(),
        location,
    )
}

/// Registering adopts an existing file, so it needs the register right on the warehouse besides
/// `create_view` on the namespace. It is checked before the metadata file is read. A refusal is
/// recorded on the warehouse and, by the requested name, as a refused `create_view` on the
/// namespace.
#[lakekeeper_storage_postgres::db_test]
async fn test_register_view_requires_the_register_right(pool: PgPool) {
    use lakekeeper::service::{
        authz::CatalogWarehouseAction,
        events::{AuthorizationFailureReason, EventListener, context::ActionContextKey},
    };
    use lakekeeper_integration_tests::{CapturingAuthzListener, RecordedAction};

    let (ctx, authz, namespace, warehouse_id, template, location) =
        hiding_register_setup(pool).await;
    let metadata_location = ViewFile::from_template(&template, &location).write().await;
    let missing_location = format!("{location}/metadata/does-not-exist.metadata.json");

    authz.block_action(&format!(
        "warehouse:{:?}",
        CatalogWarehouseAction::RegisterView
    ));
    let listener = std::sync::Arc::new(CapturingAuthzListener::default());
    ctx.v1_state
        .events
        .append(listener.clone() as std::sync::Arc<dyn EventListener>)
        .await;

    // Reading a missing file would fail with a storage error, not with a refusal.
    for location in [&missing_location, &metadata_location] {
        let err = register_view(
            ctx.clone(),
            warehouse_id,
            namespace.clone(),
            "registered",
            location,
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap_err();
        assert_eq!(err.error.code, StatusCode::FORBIDDEN, "{err:?}");
        assert_eq!(err.error.r#type, "WarehouseActionForbidden");
    }

    // Each refusal writes two records, from separate tasks, so they may arrive in any order.
    assert_eq!(listener.settled_counts(0, 4).await, (0, 4));
    assert_eq!(
        listener.failure_reasons(),
        vec![AuthorizationFailureReason::ActionForbidden; 4]
    );
    let (succeeded, mut failed) = listener.recorded_actions();
    failed.sort_by(|a, b| a[0].action_name.cmp(&b[0].action_name));
    assert_eq!(succeeded, Vec::<Vec<RecordedAction>>::new());
    let create_view = vec![RecordedAction {
        action_name: "create_view".to_string(),
        context: vec![
            ActionContextKey::Name("registered".to_string()),
            ActionContextKey::Properties(std::collections::BTreeMap::new()),
        ],
    }];
    let register_view_action = vec![RecordedAction {
        action_name: "register_view".to_string(),
        context: vec![],
    }];
    assert_eq!(
        failed,
        vec![
            create_view.clone(),
            create_view,
            register_view_action.clone(),
            register_view_action,
        ]
    );

    let err = load_view(ctx, warehouse_id, namespace, "registered")
        .await
        .unwrap_err();
    assert_eq!(err.error.code, StatusCode::NOT_FOUND, "{err:?}");
}

/// The register right on the warehouse is not enough: registering also needs `create_view` on
/// the namespace. It is checked with the view ID and the properties from the file, before the
/// view location in the file is validated, so a caller who may not create views here learns
/// nothing about a file the warehouse would refuse.
#[lakekeeper_storage_postgres::db_test]
async fn test_register_view_requires_create_view(pool: PgPool) {
    use lakekeeper::service::events::{
        AuthorizationFailureReason, EventListener, context::ActionContextKey,
    };
    use lakekeeper_integration_tests::{CapturingAuthzListener, RecordedAction};

    let (ctx, authz, namespace, warehouse_id, template, location) =
        hiding_register_setup(pool).await;
    let inside = ViewFile::from_template(&template, &location);
    let inside_location = inside.write().await;
    let outside = ViewFile::from_template(
        &template,
        &format!("memory://outside-{}/view", Uuid::now_v7()),
    );
    let outside_location = outside.write_under(&location).await;

    authz.block_action("namespace:CreateView");
    let listener = std::sync::Arc::new(CapturingAuthzListener::default());
    ctx.v1_state
        .events
        .append(listener.clone() as std::sync::Arc<dyn EventListener>)
        .await;

    // Each request has its own name, so each record can be matched to its request. Without the
    // check, the file whose view location is outside the warehouse would fail with 400
    // `InvalidLocation`, not with a refusal.
    let requests = [
        ("registered_inside", &inside, &inside_location),
        ("registered_outside", &outside, &outside_location),
    ];
    for (name, _, metadata_location) in requests {
        let err = register_view(
            ctx.clone(),
            warehouse_id,
            namespace.clone(),
            name,
            metadata_location,
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap_err();
        assert_eq!(err.error.code, StatusCode::FORBIDDEN, "{err:?}");
        assert_eq!(err.error.r#type, "NamespaceActionForbidden");
    }

    assert_eq!(listener.settled_counts(2, 2).await, (2, 2));
    assert_eq!(
        listener.failure_reasons(),
        vec![AuthorizationFailureReason::ActionForbidden; 2]
    );
    let (succeeded, mut failed) = listener.recorded_actions();
    let register_view_action = vec![RecordedAction {
        action_name: "register_view".to_string(),
        context: vec![],
    }];
    assert_eq!(
        succeeded,
        vec![register_view_action.clone(), register_view_action]
    );
    let properties: std::collections::BTreeMap<String, String> = template
        .properties()
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let expected: Vec<_> = requests
        .iter()
        .map(|(name, file, _)| {
            vec![RecordedAction {
                action_name: "create_view".to_string(),
                context: vec![
                    ActionContextKey::Name((*name).to_string()),
                    ActionContextKey::ViewId(file.uuid().to_string()),
                    ActionContextKey::Properties(properties.clone()),
                ],
            }]
        })
        .collect();
    // Records are written from separate tasks, so they may arrive in any order. `expected` is
    // in name order.
    failed.sort_by_key(|a| {
        a[0].context.iter().find_map(|key| match key {
            ActionContextKey::Name(name) => Some(name.clone()),
            _ => None,
        })
    });
    assert_eq!(failed, expected);

    for (name, _, _) in requests {
        let err = load_view(ctx.clone(), warehouse_id, namespace.clone(), name)
            .await
            .unwrap_err();
        assert_eq!(err.error.code, StatusCode::NOT_FOUND, "{name}: {err:?}");
    }
}

/// Soft deletion with an expiration far beyond any test.
fn soft_deletion() -> lakekeeper::api::management::v1::warehouse::TabularDeleteProfile {
    lakekeeper::api::management::v1::warehouse::TabularDeleteProfile::Soft {
        expiration_seconds: chrono::Duration::seconds(3600),
    }
}

/// A warehouse with `delete_profile`, namespace `reg_ns` holding view `template`, and a free
/// view location next to the template's.
async fn setup_with_template(
    pool: PgPool,
    delete_profile: lakekeeper::api::management::v1::warehouse::TabularDeleteProfile,
) -> (
    Ctx<lakekeeper::service::authz::AllowAllAuthorizer>,
    WarehouseId,
    NamespaceIdent,
    ViewMetadata,
    String,
) {
    use lakekeeper::service::authz::AllowAllAuthorizer;
    use lakekeeper_integration_tests::{create_ns, create_view, setup_simple};

    let (ctx, warehouse) = setup_simple(
        pool,
        memory_io_profile(),
        None,
        AllowAllAuthorizer::default(),
        delete_profile,
        None,
    )
    .await;
    let warehouse_id = warehouse.warehouse_id;
    let namespace = create_ns(ctx.clone(), warehouse_id.to_string(), "reg_ns".to_string())
        .await
        .namespace;
    let template = create_view(
        ctx.clone(),
        &warehouse_id.to_string(),
        "reg_ns",
        "template",
        None,
    )
    .await
    .unwrap();
    let (parent, _) = template.metadata.location().rsplit_once('/').unwrap();
    let free_location = format!("{parent}/registered-{}", Uuid::now_v7());
    (
        ctx,
        warehouse_id,
        namespace,
        template.metadata.as_ref().clone(),
        free_location,
    )
}

/// Registers `metadata_location` as `name` and expects a 409 of `error_type`.
async fn assert_register_conflicts(
    ctx: &Ctx<lakekeeper::service::authz::AllowAllAuthorizer>,
    warehouse_id: WarehouseId,
    namespace: &NamespaceIdent,
    (name, metadata_location): (&str, &str),
    error_type: &str,
) {
    let err = register_view(
        ctx.clone(),
        warehouse_id,
        namespace.clone(),
        name,
        metadata_location,
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.code, StatusCode::CONFLICT, "{name}: {err:?}");
    assert_eq!(err.error.r#type, error_type, "{name}: {err:?}");
}

/// Registering the same file again fails, under the same name and under another one: the
/// name or the view UUID is taken.
#[lakekeeper_storage_postgres::db_test]
async fn test_register_view_twice_conflicts(pool: PgPool) {
    let (ctx, warehouse_id, namespace, template, location) =
        setup_with_template(pool, soft_deletion()).await;
    let metadata_location = ViewFile::from_template(&template, &location).write().await;
    let registered = register_view(
        ctx.clone(),
        warehouse_id,
        namespace.clone(),
        "registered",
        &metadata_location,
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();

    for name in ["registered", "registered_again"] {
        assert_register_conflicts(
            &ctx,
            warehouse_id,
            &namespace,
            (name, &metadata_location),
            "AlreadyExistsException",
        )
        .await;
    }
    let err = load_view(
        ctx.clone(),
        warehouse_id,
        namespace.clone(),
        "registered_again",
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.code, StatusCode::NOT_FOUND, "{err:?}");
    let loaded = load_view(ctx, warehouse_id, namespace, "registered")
        .await
        .unwrap();
    assert_eq!(loaded, registered);
}

/// A dropped view keeps its UUID while it is soft-deleted: registering a file with that UUID
/// fails, so the dropped view can still be restored.
#[lakekeeper_storage_postgres::db_test]
async fn test_register_view_with_uuid_of_dropped_view_conflicts(pool: PgPool) {
    use lakekeeper::api::iceberg::types::DropParams;

    let (ctx, warehouse_id, namespace, template, location) =
        setup_with_template(pool, soft_deletion()).await;
    <CatalogServer<PostgresBackend, _, SecretsState> as ViewService<_>>::drop_view(
        ViewParameters {
            prefix: Some(Prefix(warehouse_id.to_string())),
            view: TableIdent::new(namespace.clone(), "template".to_string()),
        },
        DropParams {
            purge_requested: false,
            force: false,
        },
        ctx.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();
    let mut file = ViewFile::from_template(&template, &location);
    file.metadata["view-uuid"] = serde_json::json!(template.uuid());
    let metadata_location = file.write().await;

    assert_register_conflicts(
        &ctx,
        warehouse_id,
        &namespace,
        ("registered", &metadata_location),
        "AlreadyExistsException",
    )
    .await;
}

/// Tables, views and generic tables share one ID space.
#[lakekeeper_storage_postgres::db_test]
async fn test_register_view_with_uuid_of_table_conflicts(pool: PgPool) {
    let (ctx, warehouse_id, namespace, template, location) =
        setup_with_template(pool, soft_deletion()).await;
    let table = lakekeeper_integration_tests::create_table(
        ctx.clone(),
        warehouse_id.to_string(),
        "reg_ns",
        "table",
        false,
    )
    .await
    .unwrap();
    let mut file = ViewFile::from_template(&template, &location);
    file.metadata["view-uuid"] = serde_json::json!(table.metadata.uuid());
    let metadata_location = file.write().await;

    assert_register_conflicts(
        &ctx,
        warehouse_id,
        &namespace,
        ("registered", &metadata_location),
        "AlreadyExistsException",
    )
    .await;
}

/// Tables, views and generic tables share one name space in a namespace.
#[lakekeeper_storage_postgres::db_test]
async fn test_register_view_with_name_of_table_or_generic_table_conflicts(pool: PgPool) {
    let (ctx, warehouse_id, namespace, template, location) =
        setup_with_template(pool, soft_deletion()).await;
    lakekeeper_integration_tests::create_table(
        ctx.clone(),
        warehouse_id.to_string(),
        "reg_ns",
        "taken_by_table",
        false,
    )
    .await
    .unwrap();
    lakekeeper_integration_tests::create_generic_table(
        ctx.clone(),
        warehouse_id.to_string(),
        "reg_ns",
        "taken_by_generic_table",
    )
    .await
    .unwrap();
    let metadata_location = ViewFile::from_template(&template, &location).write().await;

    for name in ["taken_by_table", "taken_by_generic_table"] {
        assert_register_conflicts(
            &ctx,
            warehouse_id,
            &namespace,
            (name, &metadata_location),
            "AlreadyExistsException",
        )
        .await;
    }
}

/// A view location inside a table's location is refused, as for a created view: dropping
/// either would remove the other's files.
#[lakekeeper_storage_postgres::db_test]
async fn test_register_view_inside_table_location_conflicts(pool: PgPool) {
    let (ctx, warehouse_id, namespace, template, _) =
        setup_with_template(pool, soft_deletion()).await;
    let table = lakekeeper_integration_tests::create_table(
        ctx.clone(),
        warehouse_id.to_string(),
        "reg_ns",
        "table",
        false,
    )
    .await
    .unwrap();
    let inside_table = format!("{}/view", table.metadata.location());
    let metadata_location = ViewFile::from_template(&template, &inside_table)
        .write()
        .await;

    assert_register_conflicts(
        &ctx,
        warehouse_id,
        &namespace,
        ("registered", &metadata_location),
        "LocationAlreadyTaken",
    )
    .await;
}

/// A retry with the same `Idempotency-Key` returns the registered view, as `loadView` does,
/// and registers nothing new. The retry names a different file, so a second
/// registration could not hide behind the first one's name or UUID conflict.
#[lakekeeper_storage_postgres::db_test]
async fn test_register_view_replays_with_the_same_idempotency_key(pool: PgPool) {
    use lakekeeper::service::idempotency::IdempotencyKey;

    let (ctx, warehouse_id, namespace, template, location) =
        setup_with_template(pool, soft_deletion()).await;
    let key = IdempotencyKey::parse(&Uuid::now_v7().to_string()).unwrap();
    let with_key = || {
        let mut request_metadata = RequestMetadata::new_unauthenticated();
        request_metadata.with_idempotency_key(key);
        request_metadata
    };
    let metadata_location = ViewFile::from_template(&template, &location).write().await;
    let registered = register_view(
        ctx.clone(),
        warehouse_id,
        namespace.clone(),
        "registered",
        &metadata_location,
        with_key(),
    )
    .await
    .unwrap();

    let other_location = format!("{location}-retry");
    let other_metadata_location = ViewFile::from_template(&template, &other_location)
        .write()
        .await;
    let replayed = register_view(
        ctx.clone(),
        warehouse_id,
        namespace.clone(),
        "registered",
        &other_metadata_location,
        with_key(),
    )
    .await
    .unwrap();

    assert_eq!(replayed, registered);
    let loaded = load_view(ctx.clone(), warehouse_id, namespace.clone(), "registered")
        .await
        .unwrap();
    assert_eq!(loaded, registered);
    // The retry's file is not registered: its UUID is still free.
    register_view(
        ctx,
        warehouse_id,
        namespace,
        "registered_retry_file",
        &other_metadata_location,
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .expect("the replay must not register the retry's file");
}

/// Captures the view events of the calls under test.
#[derive(Debug, Default)]
struct ViewEventListener {
    registered: std::sync::Mutex<Vec<lakekeeper::service::events::RegisterViewEvent>>,
    /// Names of the created views.
    created: std::sync::Mutex<Vec<String>>,
}

impl std::fmt::Display for ViewEventListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ViewEventListener")
    }
}

#[lakekeeper::async_trait::async_trait]
impl lakekeeper::service::events::EventListener for ViewEventListener {
    async fn view_registered(
        &self,
        event: lakekeeper::service::events::RegisterViewEvent,
    ) -> anyhow::Result<()> {
        self.registered.lock().unwrap().push(event);
        Ok(())
    }

    async fn view_created(
        &self,
        event: lakekeeper::service::events::CreateViewEvent,
    ) -> anyhow::Result<()> {
        self.created.lock().unwrap().push(event.view_name);
        Ok(())
    }
}

/// A successful register emits one `view_registered` event, and no `view_created`, so a
/// consumer can tell a view adopted from storage from a created one. A refused or conflicting
/// register emits none.
#[lakekeeper_storage_postgres::db_test]
async fn test_register_view_emits_view_registered_only_on_success(pool: PgPool) {
    use lakekeeper::service::events::EventListener;

    let (ctx, namespace, warehouse_id, _) = views_test_setup(pool, None).await;
    let listener = std::sync::Arc::new(ViewEventListener::default());
    ctx.v1_state
        .events
        .append(listener.clone() as std::sync::Arc<dyn EventListener>)
        .await;
    let template = create_view_helper(
        ctx.clone(),
        namespace.clone(),
        create_view_request(Some("template"), None),
        Some(warehouse_id.to_string()),
    )
    .await
    .unwrap();
    let (parent, _) = template.metadata.location().rsplit_once('/').unwrap();
    let location = format!("{parent}/registered-{}", Uuid::now_v7());
    let register = |name: &'static str, metadata_location: String, request_metadata| {
        let (ctx, namespace) = (ctx.clone(), namespace.clone());
        async move {
            register_view(
                ctx,
                warehouse_id,
                namespace,
                name,
                &metadata_location,
                request_metadata,
            )
            .await
        }
    };
    let registered_events = |count: usize| {
        let listener = listener.clone();
        lakekeeper_integration_tests::eventually("the view_registered events", move || {
            let listener = listener.clone();
            async move { listener.registered.lock().unwrap().len() == count }
        })
    };

    let metadata_location = ViewFile::from_template(&template.metadata, &location)
        .write()
        .await;
    let registered = register(
        "registered",
        metadata_location.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();
    registered_events(1).await;

    // A conflict: the same file again.
    register(
        "registered_again",
        metadata_location.clone(),
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap_err();
    // A refusal: a file with the owner property, from a client that is not a trusted engine.
    let mut definer = ViewFile::from_template(&template.metadata, &format!("{location}-definer"));
    definer.metadata["properties"][OWNER_PROPERTY] = serde_json::json!("admin");
    register("definer", definer.write().await, user_request())
        .await
        .unwrap_err();
    // A last successful register. The test runtime runs the dispatch tasks in the order they
    // are spawned, so once its event is here, an event of the calls above would be too.
    let marker = ViewFile::from_template(&template.metadata, &format!("{location}-marker"));
    register(
        "marker",
        marker.write().await,
        RequestMetadata::new_unauthenticated(),
    )
    .await
    .unwrap();
    registered_events(2).await;

    let events = listener.registered.lock().unwrap().clone();
    let names = events
        .iter()
        .map(|event| event.request.name.as_str())
        .collect::<Vec<_>>();
    assert_eq!(names, ["registered", "marker"]);
    let event = &events[0];
    assert_eq!(event.request.metadata_location, metadata_location);
    assert_eq!(event.metadata_location.to_string(), metadata_location);
    assert_eq!(event.metadata.as_ref(), registered.metadata.as_ref());
    assert_eq!(event.namespace.namespace.namespace_ident(), &namespace);
    assert_eq!(event.namespace.warehouse.warehouse_id, warehouse_id);
    assert_eq!(
        *listener.created.lock().unwrap(),
        ["template"],
        "only the created view emits view_created"
    );
}
