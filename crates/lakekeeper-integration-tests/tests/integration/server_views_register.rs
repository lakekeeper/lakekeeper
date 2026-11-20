use std::io::Write as _;

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
    gzip: bool,
}

impl ViewFile {
    /// A copy of `template` as another view: a new UUID, at `location`.
    fn from_template(template: &ViewMetadata, location: &str) -> Self {
        let mut metadata = serde_json::to_value(template).unwrap();
        metadata["view-uuid"] = serde_json::json!(Uuid::now_v7());
        metadata["location"] = serde_json::json!(location);
        Self {
            metadata,
            gzip: false,
        }
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
        let suffix = if self.gzip {
            "gz.metadata.json"
        } else {
            "metadata.json"
        };
        let file = format!("{location}/metadata/00000-{}.{suffix}", Uuid::now_v7());
        let json = serde_json::to_vec(&self.metadata).unwrap();
        let bytes = if self.gzip {
            let mut encoder =
                flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
            encoder.write_all(&json).unwrap();
            encoder.finish().unwrap()
        } else {
            json
        };
        let io = memory_io_profile().file_io(None).await.unwrap();
        io.write(&file, bytes.into()).await.unwrap();
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

#[lakekeeper_storage_postgres::db_test]
async fn test_register_view_from_gzip_file(pool: PgPool) {
    let (ctx, namespace, warehouse_id, template, location) = register_setup(pool).await;
    let mut file = ViewFile::from_template(&template, &location);
    file.gzip = true;
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

/// Registering adopts an existing file, so it needs the register right on the warehouse besides
/// `create_view` on the namespace. It is checked before the metadata file is read. A refusal is
/// recorded on the warehouse and, by the requested name, as a refused `create_view` on the
/// namespace.
#[lakekeeper_storage_postgres::db_test]
async fn test_register_view_requires_the_register_right(pool: PgPool) {
    use lakekeeper::{
        api::management::v1::warehouse::TabularDeleteProfile,
        service::{
            UserId,
            authz::{CatalogWarehouseAction, tests::HidingAuthorizer},
            events::{AuthorizationFailureReason, EventListener, context::ActionContextKey},
        },
    };
    use lakekeeper_integration_tests::{
        CapturingAuthzListener, RecordedAction, create_ns, create_view, setup_simple,
    };

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
    let metadata_location = ViewFile::from_template(&template.metadata, &location)
        .write()
        .await;
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

/// A warehouse with soft deletion, namespace `reg_ns` holding view `template`, and a free view
/// location next to the template's.
async fn conflict_setup(
    pool: PgPool,
) -> (
    Ctx<lakekeeper::service::authz::AllowAllAuthorizer>,
    WarehouseId,
    NamespaceIdent,
    ViewMetadata,
    String,
) {
    use lakekeeper::{
        api::management::v1::warehouse::TabularDeleteProfile, service::authz::AllowAllAuthorizer,
    };
    use lakekeeper_integration_tests::{create_ns, create_view, setup_simple};

    let (ctx, warehouse) = setup_simple(
        pool,
        memory_io_profile(),
        None,
        AllowAllAuthorizer::default(),
        TabularDeleteProfile::Soft {
            expiration_seconds: chrono::Duration::seconds(3600),
        },
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
    let (ctx, warehouse_id, namespace, template, location) = conflict_setup(pool).await;
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

    let (ctx, warehouse_id, namespace, template, location) = conflict_setup(pool).await;
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
    let (ctx, warehouse_id, namespace, template, location) = conflict_setup(pool).await;
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
    let (ctx, warehouse_id, namespace, template, location) = conflict_setup(pool).await;
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
    let (ctx, warehouse_id, namespace, template, _) = conflict_setup(pool).await;
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
