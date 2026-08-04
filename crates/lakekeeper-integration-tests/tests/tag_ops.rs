//! Integration tests for the governance-tag Management API (`tag.rs`).
//!
//! Exercises the `Service` handlers end-to-end against Postgres: definition
//! lifecycle (create/get/list/update/delete + the validation error paths),
//! attachment on warehouse, namespace, table, view, generic-table, and column
//! targets, reverse lookup (`/attachments`, with value filter and pagination), and
//! effective/inherited tags (`?effective=true`). Mirrors the harness used by
//! `role_ops.rs`.

use http::StatusCode;
use iceberg::{
    NamespaceIdent,
    spec::{NestedField, PrimitiveType, Schema, Type, UnboundPartitionSpec},
};
use iceberg_ext::catalog::rest::CreateTableRequest;
use lakekeeper::{
    ProjectId, WarehouseId,
    api::{
        ApiContext, RequestMetadata, RequestMetadataTestBuilder,
        data::v1::generic_tables::{GenericTableService as _, ListGenericTablesQuery},
        iceberg::{
            types::Prefix,
            v1::{
                CreateNamespaceRequest, NamespaceParameters,
                namespace::NamespaceService as _,
                tables::{DataAccess, TablesService as _},
            },
        },
        management::v1::{
            ApiServer,
            project::{CreateProjectRequest, Service as _},
            tag::{
                CreateTagDefinitionRequest, ListTagAttachmentsQuery, ListTagDefinitionsQuery,
                ListTagsQuery, Service as _, SetTagRequest, TagAttachmentTarget, TagDefinition,
                TagInheritanceSource, UpdateTagDefinitionRequest,
            },
            warehouse::{CreateWarehouseRequest, Service as _, TabularDeleteProfile},
        },
    },
    server::CatalogServer,
    service::{
        CatalogNamespaceOps as _, GenericTableId, NamespaceId, State, TableId, TagScope, TagSource,
        TagValueKind, ViewId, authz::AllowAllAuthorizer,
    },
};
use lakekeeper_integration_tests::{
    SetupTestCatalog, TestWarehouseResponse, create_generic_table, create_view, memory_io_profile,
    random_request_metadata,
};
use lakekeeper_storage_postgres::{PostgresBackend, SecretsState};
use sqlx::PgPool;

type Ctx = ApiContext<State<AllowAllAuthorizer, PostgresBackend, SecretsState>>;
type Server = ApiServer<PostgresBackend, AllowAllAuthorizer, SecretsState>;

async fn setup_catalog(pool: PgPool) -> (Ctx, TestWarehouseResponse) {
    SetupTestCatalog::builder()
        .pool(pool)
        .storage_profile(memory_io_profile())
        .authorizer(AllowAllAuthorizer::default())
        .number_of_warehouses(1)
        .build()
        .setup()
        .await
}

fn request_metadata_with_project(project_id: &ProjectId) -> RequestMetadata {
    RequestMetadataTestBuilder::builder()
        .project_id(Some(project_id.clone().into()))
        .build()
}

/// Create a tag definition through the Management API.
async fn create_def(
    ctx: &Ctx,
    project_id: &ProjectId,
    name: &str,
    scope: Vec<TagScope>,
    value_kind: TagValueKind,
    allowed_values: Option<Vec<String>>,
) -> lakekeeper::api::Result<TagDefinition> {
    Server::create_tag_definition(
        CreateTagDefinitionRequest::builder()
            .name(name.to_string())
            .scope(scope)
            .value_kind(value_kind)
            .allowed_values(allowed_values)
            .build(),
        ctx.clone(),
        request_metadata_with_project(project_id),
    )
    .await
}

/// Create a namespace and a table with a two-field schema (`id`, `email`) and
/// return its `TableId`. The named fields give the column-tag resolution path a
/// real schema to resolve against.
async fn create_table_with_columns(ctx: &Ctx, warehouse_id: WarehouseId) -> TableId {
    create_nested_namespace(ctx, warehouse_id, &["tag_ns"]).await;
    create_table_in(ctx, warehouse_id, &["tag_ns"], "tagged_table").await
}

/// Create a namespace through the Management API and resolve its `NamespaceId`.
async fn create_namespace(ctx: &Ctx, warehouse_id: WarehouseId, ns_name: &str) -> NamespaceId {
    create_nested_namespace(ctx, warehouse_id, &[ns_name]).await
}

/// Create a namespace and a view in it; return the view's `ViewId`.
async fn create_view_returning_id(
    ctx: &Ctx,
    warehouse_id: WarehouseId,
    ns_name: &str,
    view_name: &str,
) -> ViewId {
    create_namespace(ctx, warehouse_id, ns_name).await;
    let loaded = create_view(
        ctx.clone(),
        &warehouse_id.to_string(),
        ns_name,
        view_name,
        None,
    )
    .await
    .unwrap();
    loaded.metadata.uuid().into()
}

/// Create a namespace and a generic table in it; return the generic table's
/// `GenericTableId` (resolved via list, as `create_generic_table` does not echo
/// the id).
async fn create_generic_table_returning_id(
    ctx: &Ctx,
    warehouse_id: WarehouseId,
    ns_name: &str,
    name: &str,
) -> GenericTableId {
    create_namespace(ctx, warehouse_id, ns_name).await;
    create_generic_table(ctx.clone(), warehouse_id.to_string(), ns_name, name)
        .await
        .unwrap();
    let prefix: Prefix = warehouse_id.to_string().into();
    let listed = CatalogServer::list_generic_tables(
        NamespaceParameters {
            prefix: Some(prefix),
            namespace: NamespaceIdent::new(ns_name.to_string()),
        },
        ListGenericTablesQuery::default(),
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    listed
        .identifiers
        .iter()
        .find(|i| i.name == name)
        .and_then(|i| i.id)
        .unwrap()
}

// ==================== Definition lifecycle ====================

/// Create + get a `marker` definition: create echoes scope/kind and reports no
/// allowed values; get likewise reports `None` allowed values.
#[sqlx::test]
async fn test_create_get_marker_definition(pool: PgPool) {
    let (ctx, wh) = setup_catalog(pool).await;
    let pid = &wh.project_id;

    let created = create_def(
        &ctx,
        pid,
        "pii",
        vec![TagScope::Column],
        TagValueKind::Marker,
        None,
    )
    .await
    .unwrap();
    assert_eq!(created.name, "pii");
    assert_eq!(created.value_kind, TagValueKind::Marker);
    assert_eq!(created.scope, vec![TagScope::Column]);
    assert_eq!(created.allowed_values, None);
    assert_eq!(created.updated_at, None);

    let fetched =
        Server::get_tag_definition(ctx.clone(), request_metadata_with_project(pid), created.id)
            .await
            .unwrap();
    assert_eq!(fetched.id, created.id);
    assert_eq!(fetched.name, "pii");
    assert_eq!(fetched.value_kind, TagValueKind::Marker);
    assert_eq!(fetched.scope, vec![TagScope::Column]);
    assert_eq!(fetched.allowed_values, None);
}

/// Create + get a `free_text` definition: no allowed values on either path.
#[sqlx::test]
async fn test_create_get_free_text_definition(pool: PgPool) {
    let (ctx, wh) = setup_catalog(pool).await;
    let pid = &wh.project_id;

    let created = create_def(
        &ctx,
        pid,
        "tier",
        vec![TagScope::Warehouse],
        TagValueKind::FreeText,
        None,
    )
    .await
    .unwrap();
    assert_eq!(created.value_kind, TagValueKind::FreeText);
    assert_eq!(created.scope, vec![TagScope::Warehouse]);
    assert_eq!(created.allowed_values, None);

    let fetched =
        Server::get_tag_definition(ctx.clone(), request_metadata_with_project(pid), created.id)
            .await
            .unwrap();
    assert_eq!(fetched.value_kind, TagValueKind::FreeText);
    assert_eq!(fetched.allowed_values, None);
}

/// Create + get an `enumerated` definition: create echoes the request's allowed
/// values; get returns them (sorted) from the store.
#[sqlx::test]
async fn test_create_get_enumerated_definition(pool: PgPool) {
    let (ctx, wh) = setup_catalog(pool).await;
    let pid = &wh.project_id;

    // Values supplied already sorted so the create-echo and the store's sorted
    // get-response are the identical vec.
    let created = create_def(
        &ctx,
        pid,
        "sensitivity",
        vec![TagScope::Table, TagScope::Column],
        TagValueKind::Enumerated,
        Some(vec!["internal".to_string(), "public".to_string()]),
    )
    .await
    .unwrap();
    assert_eq!(created.value_kind, TagValueKind::Enumerated);
    assert_eq!(created.scope, vec![TagScope::Table, TagScope::Column]);
    assert_eq!(
        created.allowed_values,
        Some(vec!["internal".to_string(), "public".to_string()])
    );

    let fetched =
        Server::get_tag_definition(ctx.clone(), request_metadata_with_project(pid), created.id)
            .await
            .unwrap();
    assert_eq!(fetched.value_kind, TagValueKind::Enumerated);
    assert_eq!(
        fetched.allowed_values,
        Some(vec!["internal".to_string(), "public".to_string()])
    );
}

/// List returns the created definitions; the `name` filter returns exactly the
/// single match, and an unknown name yields zero entries and no page token.
#[sqlx::test]
async fn test_list_definitions_and_name_filter(pool: PgPool) {
    let (ctx, wh) = setup_catalog(pool).await;
    let pid = &wh.project_id;

    create_def(
        &ctx,
        pid,
        "alpha",
        vec![TagScope::Warehouse],
        TagValueKind::Marker,
        None,
    )
    .await
    .unwrap();
    create_def(
        &ctx,
        pid,
        "beta",
        vec![TagScope::Warehouse],
        TagValueKind::Marker,
        None,
    )
    .await
    .unwrap();

    // Unfiltered list contains exactly the two created definitions.
    let all = Server::list_tag_definitions(
        ctx.clone(),
        ListTagDefinitionsQuery {
            page_token: None,
            page_size: None,
            name: None,
        },
        request_metadata_with_project(pid),
    )
    .await
    .unwrap();
    let mut names: Vec<&str> = all
        .tag_definitions
        .iter()
        .map(|d| d.name.as_str())
        .collect();
    names.sort_unstable();
    assert_eq!(names, vec!["alpha", "beta"]);

    // Name filter: exactly the one match.
    let filtered = Server::list_tag_definitions(
        ctx.clone(),
        ListTagDefinitionsQuery {
            page_token: None,
            page_size: None,
            name: Some("alpha".to_string()),
        },
        request_metadata_with_project(pid),
    )
    .await
    .unwrap();
    assert_eq!(filtered.tag_definitions.len(), 1);
    assert_eq!(filtered.tag_definitions[0].name, "alpha");
    assert_eq!(filtered.next_page_token, None);

    // Unknown name: zero entries, no page token.
    let none = Server::list_tag_definitions(
        ctx.clone(),
        ListTagDefinitionsQuery {
            page_token: None,
            page_size: None,
            name: Some("does-not-exist".to_string()),
        },
        request_metadata_with_project(pid),
    )
    .await
    .unwrap();
    assert_eq!(none.tag_definitions.len(), 0);
    assert_eq!(none.next_page_token, None);
}

/// Widening scope (subset -> superset) is accepted; the returned scope is the
/// requested superset.
#[sqlx::test]
async fn test_update_widen_scope(pool: PgPool) {
    let (ctx, wh) = setup_catalog(pool).await;
    let pid = &wh.project_id;

    let created = create_def(
        &ctx,
        pid,
        "widen",
        vec![TagScope::Warehouse],
        TagValueKind::FreeText,
        None,
    )
    .await
    .unwrap();

    let updated = Server::update_tag_definition(
        ctx.clone(),
        request_metadata_with_project(pid),
        created.id,
        UpdateTagDefinitionRequest {
            name: "widen".to_string(),
            description: None,
            scope: vec![TagScope::Warehouse, TagScope::Table],
            add_allowed_values: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(updated.scope, vec![TagScope::Warehouse, TagScope::Table]);
}

/// Narrowing scope (removing a configured scope) is rejected with `TagScopeNarrowed`.
#[sqlx::test]
async fn test_update_narrow_scope_rejected(pool: PgPool) {
    let (ctx, wh) = setup_catalog(pool).await;
    let pid = &wh.project_id;

    let created = create_def(
        &ctx,
        pid,
        "narrow",
        vec![TagScope::Warehouse, TagScope::Table],
        TagValueKind::Marker,
        None,
    )
    .await
    .unwrap();

    let err = Server::update_tag_definition(
        ctx.clone(),
        request_metadata_with_project(pid),
        created.id,
        UpdateTagDefinitionRequest {
            name: "narrow".to_string(),
            description: None,
            scope: vec![TagScope::Warehouse],
            add_allowed_values: None,
        },
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.r#type, "TagScopeNarrowed");
    assert_eq!(err.error.code, StatusCode::BAD_REQUEST.as_u16());
}

/// Renaming a definition (keeping scope) is accepted.
#[sqlx::test]
async fn test_update_rename(pool: PgPool) {
    let (ctx, wh) = setup_catalog(pool).await;
    let pid = &wh.project_id;

    let created = create_def(
        &ctx,
        pid,
        "old-name",
        vec![TagScope::Warehouse],
        TagValueKind::Marker,
        None,
    )
    .await
    .unwrap();

    let updated = Server::update_tag_definition(
        ctx.clone(),
        request_metadata_with_project(pid),
        created.id,
        UpdateTagDefinitionRequest {
            name: "new-name".to_string(),
            description: None,
            scope: vec![TagScope::Warehouse],
            add_allowed_values: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(updated.name, "new-name");
}

/// Renaming a definition into a reserved namespace is rejected with `ReservedTagNamespace`.
#[sqlx::test]
async fn test_update_reserved_name_rejected(pool: PgPool) {
    let (ctx, wh) = setup_catalog(pool).await;
    let pid = &wh.project_id;

    let created = create_def(
        &ctx,
        pid,
        "renamable",
        vec![TagScope::Warehouse],
        TagValueKind::Marker,
        None,
    )
    .await
    .unwrap();

    let err = Server::update_tag_definition(
        ctx.clone(),
        request_metadata_with_project(pid),
        created.id,
        UpdateTagDefinitionRequest {
            name: "system.foo".to_string(),
            description: None,
            scope: vec![TagScope::Warehouse],
            add_allowed_values: None,
        },
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.r#type, "ReservedTagNamespace");
    assert_eq!(err.error.code, StatusCode::BAD_REQUEST.as_u16());
}

/// Adding allowed values to a non-enumerated definition is rejected with `InvalidTagDefinition`.
#[sqlx::test]
async fn test_update_add_allowed_values_to_non_enumerated_rejected(pool: PgPool) {
    let (ctx, wh) = setup_catalog(pool).await;
    let pid = &wh.project_id;

    let created = create_def(
        &ctx,
        pid,
        "plain",
        vec![TagScope::Warehouse],
        TagValueKind::FreeText,
        None,
    )
    .await
    .unwrap();

    let err = Server::update_tag_definition(
        ctx.clone(),
        request_metadata_with_project(pid),
        created.id,
        UpdateTagDefinitionRequest {
            name: "plain".to_string(),
            description: None,
            scope: vec![TagScope::Warehouse],
            add_allowed_values: Some(vec!["x".to_string()]),
        },
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.r#type, "InvalidTagDefinition");
    assert_eq!(err.error.code, StatusCode::BAD_REQUEST.as_u16());
}

/// Creating a definition in a reserved namespace is rejected with `ReservedTagNamespace`.
#[sqlx::test]
async fn test_create_reserved_prefix_rejected(pool: PgPool) {
    let (ctx, wh) = setup_catalog(pool).await;
    let pid = &wh.project_id;

    let err = create_def(
        &ctx,
        pid,
        "system.foo",
        vec![TagScope::Warehouse],
        TagValueKind::Marker,
        None,
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.r#type, "ReservedTagNamespace");
    assert_eq!(err.error.code, StatusCode::BAD_REQUEST.as_u16());
}

/// Creating an enumerated definition without allowed values is rejected with `InvalidTagDefinition`.
#[sqlx::test]
async fn test_create_enumerated_without_allowed_values_rejected(pool: PgPool) {
    let (ctx, wh) = setup_catalog(pool).await;
    let pid = &wh.project_id;

    let err = create_def(
        &ctx,
        pid,
        "enum-empty",
        vec![TagScope::Warehouse],
        TagValueKind::Enumerated,
        None,
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.r#type, "InvalidTagDefinition");
    assert_eq!(err.error.code, StatusCode::BAD_REQUEST.as_u16());
}

/// Creating a non-enumerated definition with allowed values is rejected with `InvalidTagDefinition`.
#[sqlx::test]
async fn test_create_marker_with_allowed_values_rejected(pool: PgPool) {
    let (ctx, wh) = setup_catalog(pool).await;
    let pid = &wh.project_id;

    let err = create_def(
        &ctx,
        pid,
        "marker-vals",
        vec![TagScope::Warehouse],
        TagValueKind::Marker,
        Some(vec!["nope".to_string()]),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.r#type, "InvalidTagDefinition");
    assert_eq!(err.error.code, StatusCode::BAD_REQUEST.as_u16());
}

/// A duplicate name (case-insensitive) is rejected with `TagNameAlreadyExists` (409).
#[sqlx::test]
async fn test_create_duplicate_name_case_insensitive_rejected(pool: PgPool) {
    let (ctx, wh) = setup_catalog(pool).await;
    let pid = &wh.project_id;

    create_def(
        &ctx,
        pid,
        "PII",
        vec![TagScope::Column],
        TagValueKind::Marker,
        None,
    )
    .await
    .unwrap();

    let err = create_def(
        &ctx,
        pid,
        "pii",
        vec![TagScope::Column],
        TagValueKind::Marker,
        None,
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.r#type, "TagNameAlreadyExists");
    assert_eq!(err.error.code, StatusCode::CONFLICT.as_u16());
}

/// An unused definition can be deleted; a subsequent get reports not-found.
#[sqlx::test]
async fn test_delete_unused_definition(pool: PgPool) {
    let (ctx, wh) = setup_catalog(pool).await;
    let pid = &wh.project_id;

    let created = create_def(
        &ctx,
        pid,
        "temp",
        vec![TagScope::Warehouse],
        TagValueKind::Marker,
        None,
    )
    .await
    .unwrap();

    Server::delete_tag_definition(ctx.clone(), request_metadata_with_project(pid), created.id)
        .await
        .unwrap();

    let err =
        Server::get_tag_definition(ctx.clone(), request_metadata_with_project(pid), created.id)
            .await
            .unwrap_err();
    assert_eq!(err.error.r#type, "TagDefinitionIdNotFound");
    assert_eq!(err.error.code, StatusCode::NOT_FOUND.as_u16());
}

// ==================== Attachment: warehouse target ====================

/// Apply a `free_text` tag to the warehouse, list it, remove it, and confirm
/// removal is idempotent.
#[sqlx::test]
async fn test_warehouse_tag_apply_list_remove_idempotent(pool: PgPool) {
    let (ctx, wh) = setup_catalog(pool).await;
    let pid = &wh.project_id;
    let warehouse_id = wh.warehouse_id;

    let def = create_def(
        &ctx,
        pid,
        "tier",
        vec![TagScope::Warehouse],
        TagValueKind::FreeText,
        None,
    )
    .await
    .unwrap();

    // Apply.
    let applied = Server::set_warehouse_tag(
        warehouse_id,
        "tier".to_string(),
        SetTagRequest {
            value: Some("gold".to_string()),
        },
        ctx.clone(),
        request_metadata_with_project(pid),
    )
    .await
    .unwrap();
    assert_eq!(applied.tag_definition_id, def.id);
    assert_eq!(applied.name, "tier");
    assert_eq!(applied.value, Some("gold".to_string()));

    // List: exactly the one tag.
    let listed = Server::list_warehouse_tags(
        warehouse_id,
        ctx.clone(),
        request_metadata_with_project(pid),
        ListTagsQuery::default(),
    )
    .await
    .unwrap();
    assert_eq!(listed.tags.len(), 1);
    assert_eq!(listed.tags[0].tag_definition_id, def.id);
    assert_eq!(listed.tags[0].name, "tier");
    assert_eq!(listed.tags[0].value, Some("gold".to_string()));
    assert_eq!(listed.tags[0].source, TagSource::Manual);

    // Remove.
    Server::delete_warehouse_tag(
        warehouse_id,
        "tier".to_string(),
        ctx.clone(),
        request_metadata_with_project(pid),
    )
    .await
    .unwrap();
    let after = Server::list_warehouse_tags(
        warehouse_id,
        ctx.clone(),
        request_metadata_with_project(pid),
        ListTagsQuery::default(),
    )
    .await
    .unwrap();
    assert_eq!(after.tags.len(), 0);

    // Remove again: idempotent, no error.
    Server::delete_warehouse_tag(
        warehouse_id,
        "tier".to_string(),
        ctx.clone(),
        request_metadata_with_project(pid),
    )
    .await
    .unwrap();
}

/// Applying a value to a `marker` tag is rejected with `InvalidTagValue`.
#[sqlx::test]
async fn test_warehouse_tag_marker_with_value_rejected(pool: PgPool) {
    let (ctx, wh) = setup_catalog(pool).await;
    let pid = &wh.project_id;
    let warehouse_id = wh.warehouse_id;

    create_def(
        &ctx,
        pid,
        "pii",
        vec![TagScope::Warehouse],
        TagValueKind::Marker,
        None,
    )
    .await
    .unwrap();

    let err = Server::set_warehouse_tag(
        warehouse_id,
        "pii".to_string(),
        SetTagRequest {
            value: Some("oops".to_string()),
        },
        ctx.clone(),
        request_metadata_with_project(pid),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.r#type, "InvalidTagValue");
    assert_eq!(err.error.code, StatusCode::BAD_REQUEST.as_u16());
}

/// Enumerated value legality: a value outside the allowed set is rejected with
/// `InvalidTagValue`; an allowed value is accepted.
#[sqlx::test]
async fn test_warehouse_tag_enumerated_value_legality(pool: PgPool) {
    let (ctx, wh) = setup_catalog(pool).await;
    let pid = &wh.project_id;
    let warehouse_id = wh.warehouse_id;

    let def = create_def(
        &ctx,
        pid,
        "sensitivity",
        vec![TagScope::Warehouse],
        TagValueKind::Enumerated,
        Some(vec!["internal".to_string(), "public".to_string()]),
    )
    .await
    .unwrap();

    // Value not in the allowed set.
    let err = Server::set_warehouse_tag(
        warehouse_id,
        "sensitivity".to_string(),
        SetTagRequest {
            value: Some("secret".to_string()),
        },
        ctx.clone(),
        request_metadata_with_project(pid),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.r#type, "InvalidTagValue");
    assert_eq!(err.error.code, StatusCode::BAD_REQUEST.as_u16());

    // Allowed value.
    let applied = Server::set_warehouse_tag(
        warehouse_id,
        "sensitivity".to_string(),
        SetTagRequest {
            value: Some("public".to_string()),
        },
        ctx.clone(),
        request_metadata_with_project(pid),
    )
    .await
    .unwrap();
    assert_eq!(applied.tag_definition_id, def.id);
    assert_eq!(applied.value, Some("public".to_string()));
}

/// A tag scoped only to `table` cannot be applied to a warehouse: `TagScopeNotAllowed`.
#[sqlx::test]
async fn test_warehouse_tag_scope_not_allowed(pool: PgPool) {
    let (ctx, wh) = setup_catalog(pool).await;
    let pid = &wh.project_id;
    let warehouse_id = wh.warehouse_id;

    create_def(
        &ctx,
        pid,
        "table-only",
        vec![TagScope::Table],
        TagValueKind::Marker,
        None,
    )
    .await
    .unwrap();

    let err = Server::set_warehouse_tag(
        warehouse_id,
        "table-only".to_string(),
        SetTagRequest { value: None },
        ctx.clone(),
        request_metadata_with_project(pid),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.r#type, "TagScopeNotAllowed");
    assert_eq!(err.error.code, StatusCode::BAD_REQUEST.as_u16());
}

/// A definition attached to a target cannot be deleted (`TagDefinitionInUse`);
/// after removing the attachment it deletes.
#[sqlx::test]
async fn test_delete_definition_in_use(pool: PgPool) {
    let (ctx, wh) = setup_catalog(pool).await;
    let pid = &wh.project_id;
    let warehouse_id = wh.warehouse_id;

    let def = create_def(
        &ctx,
        pid,
        "in-use",
        vec![TagScope::Warehouse],
        TagValueKind::FreeText,
        None,
    )
    .await
    .unwrap();

    Server::set_warehouse_tag(
        warehouse_id,
        "in-use".to_string(),
        SetTagRequest {
            value: Some("v".to_string()),
        },
        ctx.clone(),
        request_metadata_with_project(pid),
    )
    .await
    .unwrap();

    // Delete while in use -> rejected.
    let err =
        Server::delete_tag_definition(ctx.clone(), request_metadata_with_project(pid), def.id)
            .await
            .unwrap_err();
    assert_eq!(err.error.r#type, "TagDefinitionInUse");
    assert_eq!(err.error.code, StatusCode::CONFLICT.as_u16());

    // Remove the attachment, then delete succeeds.
    Server::delete_warehouse_tag(
        warehouse_id,
        "in-use".to_string(),
        ctx.clone(),
        request_metadata_with_project(pid),
    )
    .await
    .unwrap();
    Server::delete_tag_definition(ctx.clone(), request_metadata_with_project(pid), def.id)
        .await
        .unwrap();
}

// ==================== Attachment: table + column ====================

/// Apply a `table`-scoped tag to a table and confirm it is listed.
#[sqlx::test]
async fn test_table_tag_apply_and_list(pool: PgPool) {
    let (ctx, wh) = setup_catalog(pool).await;
    let pid = &wh.project_id;
    let warehouse_id = wh.warehouse_id;
    let table_id = create_table_with_columns(&ctx, warehouse_id).await;

    let def = create_def(
        &ctx,
        pid,
        "table-tag",
        vec![TagScope::Table],
        TagValueKind::FreeText,
        None,
    )
    .await
    .unwrap();

    let applied = Server::set_table_tag(
        warehouse_id,
        table_id,
        "table-tag".to_string(),
        SetTagRequest {
            value: Some("owned-by-analytics".to_string()),
        },
        ctx.clone(),
        request_metadata_with_project(pid),
    )
    .await
    .unwrap();
    assert_eq!(applied.tag_definition_id, def.id);
    assert_eq!(applied.value, Some("owned-by-analytics".to_string()));

    let listed = Server::list_table_tags(
        warehouse_id,
        table_id,
        ctx.clone(),
        request_metadata_with_project(pid),
        ListTagsQuery::default(),
    )
    .await
    .unwrap();
    assert_eq!(listed.tags.len(), 1);
    assert_eq!(listed.tags[0].tag_definition_id, def.id);
    assert_eq!(listed.tags[0].name, "table-tag");
    assert_eq!(listed.tags[0].value, Some("owned-by-analytics".to_string()));
}

/// Apply a `column`-scoped tag to column `email` by name.
#[sqlx::test]
async fn test_table_column_tag_by_name(pool: PgPool) {
    let (ctx, wh) = setup_catalog(pool).await;
    let pid = &wh.project_id;
    let warehouse_id = wh.warehouse_id;
    let table_id = create_table_with_columns(&ctx, warehouse_id).await;

    let def = create_def(
        &ctx,
        pid,
        "column-pii",
        vec![TagScope::Column],
        TagValueKind::Marker,
        None,
    )
    .await
    .unwrap();

    let applied = Server::set_table_column_tag(
        warehouse_id,
        table_id,
        "email".to_string(),
        "column-pii".to_string(),
        SetTagRequest { value: None },
        ctx.clone(),
        request_metadata_with_project(pid),
    )
    .await
    .unwrap();
    assert_eq!(applied.tag_definition_id, def.id);
    assert_eq!(applied.name, "column-pii");
    assert_eq!(applied.value, None);

    let listed = Server::list_table_column_tags(
        warehouse_id,
        table_id,
        "email".to_string(),
        ctx.clone(),
        request_metadata_with_project(pid),
        ListTagsQuery::default(),
    )
    .await
    .unwrap();
    assert_eq!(listed.tags.len(), 1);
    assert_eq!(listed.tags[0].tag_definition_id, def.id);
}

/// Applying a column tag to a non-existent column is rejected with `ColumnNotFound` (404).
#[sqlx::test]
async fn test_table_column_tag_unknown_column_not_found(pool: PgPool) {
    let (ctx, wh) = setup_catalog(pool).await;
    let pid = &wh.project_id;
    let warehouse_id = wh.warehouse_id;
    let table_id = create_table_with_columns(&ctx, warehouse_id).await;

    create_def(
        &ctx,
        pid,
        "column-pii",
        vec![TagScope::Column],
        TagValueKind::Marker,
        None,
    )
    .await
    .unwrap();

    let err = Server::set_table_column_tag(
        warehouse_id,
        table_id,
        "nope".to_string(),
        "column-pii".to_string(),
        SetTagRequest { value: None },
        ctx.clone(),
        request_metadata_with_project(pid),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.r#type, "ColumnNotFound");
    assert_eq!(err.error.code, StatusCode::NOT_FOUND.as_u16());
}

// ==================== Enumerated update: allowed-values echo ====================

/// Updating an enumerated definition (adding a value) returns the merged,
/// sorted allowed values on the update response, and `get` agrees. Marker and
/// free-text updates report `None` allowed values.
#[sqlx::test]
async fn test_update_enumerated_returns_allowed_values(pool: PgPool) {
    let (ctx, wh) = setup_catalog(pool).await;
    let pid = &wh.project_id;

    let created = create_def(
        &ctx,
        pid,
        "grades",
        vec![TagScope::Warehouse],
        TagValueKind::Enumerated,
        Some(vec!["a".to_string(), "b".to_string()]),
    )
    .await
    .unwrap();

    // Add "c": the response carries the merged set, sorted.
    let updated = Server::update_tag_definition(
        ctx.clone(),
        request_metadata_with_project(pid),
        created.id,
        UpdateTagDefinitionRequest {
            name: "grades".to_string(),
            description: None,
            scope: vec![TagScope::Warehouse],
            add_allowed_values: Some(vec!["c".to_string()]),
        },
    )
    .await
    .unwrap();
    assert_eq!(
        updated.allowed_values,
        Some(vec!["a".to_string(), "b".to_string(), "c".to_string()])
    );

    // get returns the same sorted set from the store.
    let fetched =
        Server::get_tag_definition(ctx.clone(), request_metadata_with_project(pid), created.id)
            .await
            .unwrap();
    assert_eq!(
        fetched.allowed_values,
        Some(vec!["a".to_string(), "b".to_string(), "c".to_string()])
    );

    // Marker update: no allowed values.
    let marker = create_def(
        &ctx,
        pid,
        "marker-def",
        vec![TagScope::Warehouse],
        TagValueKind::Marker,
        None,
    )
    .await
    .unwrap();
    let updated_marker = Server::update_tag_definition(
        ctx.clone(),
        request_metadata_with_project(pid),
        marker.id,
        UpdateTagDefinitionRequest {
            name: "marker-def".to_string(),
            description: None,
            scope: vec![TagScope::Warehouse],
            add_allowed_values: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(updated_marker.allowed_values, None);

    // Free-text update: no allowed values.
    let free = create_def(
        &ctx,
        pid,
        "free-def",
        vec![TagScope::Warehouse],
        TagValueKind::FreeText,
        None,
    )
    .await
    .unwrap();
    let updated_free = Server::update_tag_definition(
        ctx.clone(),
        request_metadata_with_project(pid),
        free.id,
        UpdateTagDefinitionRequest {
            name: "free-def".to_string(),
            description: None,
            scope: vec![TagScope::Warehouse],
            add_allowed_values: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(updated_free.allowed_values, None);
}

// ==================== Attachment: namespace target ====================

/// Apply a `free_text` tag to a namespace, list it, remove it, and confirm
/// removal is idempotent.
#[sqlx::test]
async fn test_namespace_tag_apply_list_remove_idempotent(pool: PgPool) {
    let (ctx, wh) = setup_catalog(pool).await;
    let pid = &wh.project_id;
    let warehouse_id = wh.warehouse_id;
    let namespace_id = create_namespace(&ctx, warehouse_id, "ns_tag_target").await;

    let def = create_def(
        &ctx,
        pid,
        "domain",
        vec![TagScope::Namespace],
        TagValueKind::FreeText,
        None,
    )
    .await
    .unwrap();

    let applied = Server::set_namespace_tag(
        warehouse_id,
        namespace_id,
        "domain".to_string(),
        SetTagRequest {
            value: Some("sales".to_string()),
        },
        ctx.clone(),
        request_metadata_with_project(pid),
    )
    .await
    .unwrap();
    assert_eq!(applied.tag_definition_id, def.id);
    assert_eq!(applied.name, "domain");
    assert_eq!(applied.value, Some("sales".to_string()));

    let listed = Server::list_namespace_tags(
        warehouse_id,
        namespace_id,
        ctx.clone(),
        request_metadata_with_project(pid),
        ListTagsQuery::default(),
    )
    .await
    .unwrap();
    assert_eq!(listed.tags.len(), 1);
    assert_eq!(listed.tags[0].tag_definition_id, def.id);
    assert_eq!(listed.tags[0].name, "domain");
    assert_eq!(listed.tags[0].value, Some("sales".to_string()));
    assert_eq!(listed.tags[0].source, TagSource::Manual);

    Server::delete_namespace_tag(
        warehouse_id,
        namespace_id,
        "domain".to_string(),
        ctx.clone(),
        request_metadata_with_project(pid),
    )
    .await
    .unwrap();
    let after = Server::list_namespace_tags(
        warehouse_id,
        namespace_id,
        ctx.clone(),
        request_metadata_with_project(pid),
        ListTagsQuery::default(),
    )
    .await
    .unwrap();
    assert_eq!(after.tags.len(), 0);

    // Remove again: idempotent, no error.
    Server::delete_namespace_tag(
        warehouse_id,
        namespace_id,
        "domain".to_string(),
        ctx.clone(),
        request_metadata_with_project(pid),
    )
    .await
    .unwrap();
}

// ==================== Attachment: view target ====================

/// Apply a `free_text` tag to a view, list it, remove it, and confirm removal
/// is idempotent.
#[sqlx::test]
async fn test_view_tag_apply_list_remove_idempotent(pool: PgPool) {
    let (ctx, wh) = setup_catalog(pool).await;
    let pid = &wh.project_id;
    let warehouse_id = wh.warehouse_id;
    let view_id = create_view_returning_id(&ctx, warehouse_id, "view_tag_ns", "tagged_view").await;

    let def = create_def(
        &ctx,
        pid,
        "view-tag",
        vec![TagScope::View],
        TagValueKind::FreeText,
        None,
    )
    .await
    .unwrap();

    let applied = Server::set_view_tag(
        warehouse_id,
        view_id,
        "view-tag".to_string(),
        SetTagRequest {
            value: Some("curated".to_string()),
        },
        ctx.clone(),
        request_metadata_with_project(pid),
    )
    .await
    .unwrap();
    assert_eq!(applied.tag_definition_id, def.id);
    assert_eq!(applied.name, "view-tag");
    assert_eq!(applied.value, Some("curated".to_string()));

    let listed = Server::list_view_tags(
        warehouse_id,
        view_id,
        ctx.clone(),
        request_metadata_with_project(pid),
        ListTagsQuery::default(),
    )
    .await
    .unwrap();
    assert_eq!(listed.tags.len(), 1);
    assert_eq!(listed.tags[0].tag_definition_id, def.id);
    assert_eq!(listed.tags[0].name, "view-tag");
    assert_eq!(listed.tags[0].value, Some("curated".to_string()));
    assert_eq!(listed.tags[0].source, TagSource::Manual);

    Server::delete_view_tag(
        warehouse_id,
        view_id,
        "view-tag".to_string(),
        ctx.clone(),
        request_metadata_with_project(pid),
    )
    .await
    .unwrap();
    let after = Server::list_view_tags(
        warehouse_id,
        view_id,
        ctx.clone(),
        request_metadata_with_project(pid),
        ListTagsQuery::default(),
    )
    .await
    .unwrap();
    assert_eq!(after.tags.len(), 0);

    // Remove again: idempotent, no error.
    Server::delete_view_tag(
        warehouse_id,
        view_id,
        "view-tag".to_string(),
        ctx.clone(),
        request_metadata_with_project(pid),
    )
    .await
    .unwrap();
}

// ==================== Attachment: generic-table target ====================

/// Apply a `free_text` tag to a generic table, list it, remove it, and confirm
/// removal is idempotent.
#[sqlx::test]
async fn test_generic_table_tag_apply_list_remove_idempotent(pool: PgPool) {
    let (ctx, wh) = setup_catalog(pool).await;
    let pid = &wh.project_id;
    let warehouse_id = wh.warehouse_id;
    let generic_table_id =
        create_generic_table_returning_id(&ctx, warehouse_id, "gt_tag_ns", "tagged_gt").await;

    let def = create_def(
        &ctx,
        pid,
        "gt-tag",
        vec![TagScope::GenericTable],
        TagValueKind::FreeText,
        None,
    )
    .await
    .unwrap();

    let applied = Server::set_generic_table_tag(
        warehouse_id,
        generic_table_id,
        "gt-tag".to_string(),
        SetTagRequest {
            value: Some("lance-dataset".to_string()),
        },
        ctx.clone(),
        request_metadata_with_project(pid),
    )
    .await
    .unwrap();
    assert_eq!(applied.tag_definition_id, def.id);
    assert_eq!(applied.name, "gt-tag");
    assert_eq!(applied.value, Some("lance-dataset".to_string()));

    let listed = Server::list_generic_table_tags(
        warehouse_id,
        generic_table_id,
        ctx.clone(),
        request_metadata_with_project(pid),
        ListTagsQuery::default(),
    )
    .await
    .unwrap();
    assert_eq!(listed.tags.len(), 1);
    assert_eq!(listed.tags[0].tag_definition_id, def.id);
    assert_eq!(listed.tags[0].name, "gt-tag");
    assert_eq!(listed.tags[0].value, Some("lance-dataset".to_string()));
    assert_eq!(listed.tags[0].source, TagSource::Manual);

    Server::delete_generic_table_tag(
        warehouse_id,
        generic_table_id,
        "gt-tag".to_string(),
        ctx.clone(),
        request_metadata_with_project(pid),
    )
    .await
    .unwrap();
    let after = Server::list_generic_table_tags(
        warehouse_id,
        generic_table_id,
        ctx.clone(),
        request_metadata_with_project(pid),
        ListTagsQuery::default(),
    )
    .await
    .unwrap();
    assert_eq!(after.tags.len(), 0);

    // Remove again: idempotent, no error.
    Server::delete_generic_table_tag(
        warehouse_id,
        generic_table_id,
        "gt-tag".to_string(),
        ctx.clone(),
        request_metadata_with_project(pid),
    )
    .await
    .unwrap();
}

// ==================== Attachment: table + column removal ====================

/// Apply a table tag and a column tag, then remove each independently and
/// confirm the listings reflect the removals without disturbing the other.
#[sqlx::test]
async fn test_table_and_column_tag_remove(pool: PgPool) {
    let (ctx, wh) = setup_catalog(pool).await;
    let pid = &wh.project_id;
    let warehouse_id = wh.warehouse_id;
    let table_id = create_table_with_columns(&ctx, warehouse_id).await;

    let table_def = create_def(
        &ctx,
        pid,
        "table-tag",
        vec![TagScope::Table],
        TagValueKind::FreeText,
        None,
    )
    .await
    .unwrap();
    let column_def = create_def(
        &ctx,
        pid,
        "column-pii",
        vec![TagScope::Column],
        TagValueKind::Marker,
        None,
    )
    .await
    .unwrap();

    Server::set_table_tag(
        warehouse_id,
        table_id,
        "table-tag".to_string(),
        SetTagRequest {
            value: Some("owned-by-analytics".to_string()),
        },
        ctx.clone(),
        request_metadata_with_project(pid),
    )
    .await
    .unwrap();
    Server::set_table_column_tag(
        warehouse_id,
        table_id,
        "email".to_string(),
        "column-pii".to_string(),
        SetTagRequest { value: None },
        ctx.clone(),
        request_metadata_with_project(pid),
    )
    .await
    .unwrap();

    // Both present.
    let table_tags = Server::list_table_tags(
        warehouse_id,
        table_id,
        ctx.clone(),
        request_metadata_with_project(pid),
        ListTagsQuery::default(),
    )
    .await
    .unwrap();
    assert_eq!(table_tags.tags.len(), 1);
    assert_eq!(table_tags.tags[0].tag_definition_id, table_def.id);
    let column_tags = Server::list_table_column_tags(
        warehouse_id,
        table_id,
        "email".to_string(),
        ctx.clone(),
        request_metadata_with_project(pid),
        ListTagsQuery::default(),
    )
    .await
    .unwrap();
    assert_eq!(column_tags.tags.len(), 1);
    assert_eq!(column_tags.tags[0].tag_definition_id, column_def.id);

    // Remove the table tag; the column tag is untouched.
    Server::delete_table_tag(
        warehouse_id,
        table_id,
        "table-tag".to_string(),
        ctx.clone(),
        request_metadata_with_project(pid),
    )
    .await
    .unwrap();
    let table_tags = Server::list_table_tags(
        warehouse_id,
        table_id,
        ctx.clone(),
        request_metadata_with_project(pid),
        ListTagsQuery::default(),
    )
    .await
    .unwrap();
    assert_eq!(table_tags.tags.len(), 0);
    let column_tags = Server::list_table_column_tags(
        warehouse_id,
        table_id,
        "email".to_string(),
        ctx.clone(),
        request_metadata_with_project(pid),
        ListTagsQuery::default(),
    )
    .await
    .unwrap();
    assert_eq!(column_tags.tags.len(), 1);

    // Remove the column tag.
    Server::delete_table_column_tag(
        warehouse_id,
        table_id,
        "email".to_string(),
        "column-pii".to_string(),
        ctx.clone(),
        request_metadata_with_project(pid),
    )
    .await
    .unwrap();
    let column_tags = Server::list_table_column_tags(
        warehouse_id,
        table_id,
        "email".to_string(),
        ctx.clone(),
        request_metadata_with_project(pid),
        ListTagsQuery::default(),
    )
    .await
    .unwrap();
    assert_eq!(column_tags.tags.len(), 0);
}

/// A definition attached to a non-warehouse target (a table) cannot be deleted
/// (`TagDefinitionInUse`); after removing the attachment it deletes.
#[sqlx::test]
async fn test_delete_definition_in_use_by_table(pool: PgPool) {
    let (ctx, wh) = setup_catalog(pool).await;
    let pid = &wh.project_id;
    let warehouse_id = wh.warehouse_id;
    let table_id = create_table_with_columns(&ctx, warehouse_id).await;

    let def = create_def(
        &ctx,
        pid,
        "table-in-use",
        vec![TagScope::Table],
        TagValueKind::FreeText,
        None,
    )
    .await
    .unwrap();

    Server::set_table_tag(
        warehouse_id,
        table_id,
        "table-in-use".to_string(),
        SetTagRequest {
            value: Some("v".to_string()),
        },
        ctx.clone(),
        request_metadata_with_project(pid),
    )
    .await
    .unwrap();

    // Delete while in use -> rejected.
    let err =
        Server::delete_tag_definition(ctx.clone(), request_metadata_with_project(pid), def.id)
            .await
            .unwrap_err();
    assert_eq!(err.error.r#type, "TagDefinitionInUse");
    assert_eq!(err.error.code, StatusCode::CONFLICT.as_u16());

    // Remove the attachment, then delete succeeds.
    Server::delete_table_tag(
        warehouse_id,
        table_id,
        "table-in-use".to_string(),
        ctx.clone(),
        request_metadata_with_project(pid),
    )
    .await
    .unwrap();
    Server::delete_tag_definition(ctx.clone(), request_metadata_with_project(pid), def.id)
        .await
        .unwrap();
}

// ==================== Cross-project rejection ====================

/// A tag definition in project A cannot be applied to a target (warehouse) in
/// project B: with the request scoped to project A, the target resolves in a
/// different project and the attachment is rejected as `TagTargetNotFound`
/// (404) before the definition is even resolved.
#[sqlx::test]
async fn test_cross_project_target_rejected(pool: PgPool) {
    let (ctx, wh) = setup_catalog(pool).await;
    let pid = &wh.project_id;

    // Second project + a warehouse inside it.
    let project_b = Server::create_project(
        CreateProjectRequest {
            project_name: "project-b".to_string(),
            project_id: None,
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap()
    .project_id;
    let warehouse_b = Server::create_warehouse(
        CreateWarehouseRequest {
            warehouse_name: "warehouse-b".to_string(),
            project_id: Some((*project_b).clone()),
            storage_profile: memory_io_profile(),
            storage_credential: None,
            delete_profile: TabularDeleteProfile::Hard {},
            allowed_format_versions: None,
            default_format_version: None,
            managed_by: Default::default(),
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    let warehouse_b_id = warehouse_b.warehouse_id();

    // Definition lives in project A.
    create_def(
        &ctx,
        pid,
        "cross-tag",
        vec![TagScope::Warehouse],
        TagValueKind::FreeText,
        None,
    )
    .await
    .unwrap();

    // Request scoped to project A, target warehouse in project B -> 404.
    let err = Server::set_warehouse_tag(
        warehouse_b_id,
        "cross-tag".to_string(),
        SetTagRequest {
            value: Some("x".to_string()),
        },
        ctx.clone(),
        request_metadata_with_project(pid),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.r#type, "TagTargetNotFound");
    assert_eq!(err.error.code, StatusCode::NOT_FOUND.as_u16());
}

// ==================== Reverse lookup (attachments) ====================

/// Apply one definition to every target type, then reverse-list its attachments.
/// Exercises target-subtype reconstruction end-to-end: warehouse, namespace, table,
/// column (by field-id), view, and generic-table each come back as the right variant.
#[sqlx::test]
async fn test_list_tag_attachments_across_targets(pool: PgPool) {
    let (ctx, wh) = setup_catalog(pool).await;
    let pid = &wh.project_id;
    let warehouse_id = wh.warehouse_id;

    // One target of each type (create_table_with_columns makes "email" = field-id 2).
    let table_id = create_table_with_columns(&ctx, warehouse_id).await;
    let namespace_id = create_namespace(&ctx, warehouse_id, "reverse_ns").await;
    let view_id = create_view_returning_id(&ctx, warehouse_id, "view_ns", "v1").await;
    let generic_table_id =
        create_generic_table_returning_id(&ctx, warehouse_id, "gt_ns", "gt1").await;

    let def = create_def(
        &ctx,
        pid,
        "everywhere",
        vec![
            TagScope::Warehouse,
            TagScope::Namespace,
            TagScope::Table,
            TagScope::Column,
            TagScope::View,
            TagScope::GenericTable,
        ],
        TagValueKind::Marker,
        None,
    )
    .await
    .unwrap();

    // Apply in a fixed order; separate calls give strictly increasing created_at, so
    // the listing order equals the application order.
    Server::set_warehouse_tag(
        warehouse_id,
        "everywhere".to_string(),
        SetTagRequest { value: None },
        ctx.clone(),
        request_metadata_with_project(pid),
    )
    .await
    .unwrap();
    Server::set_namespace_tag(
        warehouse_id,
        namespace_id,
        "everywhere".to_string(),
        SetTagRequest { value: None },
        ctx.clone(),
        request_metadata_with_project(pid),
    )
    .await
    .unwrap();
    Server::set_table_tag(
        warehouse_id,
        table_id,
        "everywhere".to_string(),
        SetTagRequest { value: None },
        ctx.clone(),
        request_metadata_with_project(pid),
    )
    .await
    .unwrap();
    Server::set_table_column_tag(
        warehouse_id,
        table_id,
        "email".to_string(),
        "everywhere".to_string(),
        SetTagRequest { value: None },
        ctx.clone(),
        request_metadata_with_project(pid),
    )
    .await
    .unwrap();
    Server::set_view_tag(
        warehouse_id,
        view_id,
        "everywhere".to_string(),
        SetTagRequest { value: None },
        ctx.clone(),
        request_metadata_with_project(pid),
    )
    .await
    .unwrap();
    Server::set_generic_table_tag(
        warehouse_id,
        generic_table_id,
        "everywhere".to_string(),
        SetTagRequest { value: None },
        ctx.clone(),
        request_metadata_with_project(pid),
    )
    .await
    .unwrap();

    let listed = Server::list_tag_attachments(
        ctx.clone(),
        request_metadata_with_project(pid),
        def.id,
        ListTagAttachmentsQuery::builder()
            .page_size(Some(50))
            .build(),
    )
    .await
    .unwrap();

    let targets: Vec<&TagAttachmentTarget> = listed.attachments.iter().map(|a| &a.target).collect();
    let expected = [
        TagAttachmentTarget::Warehouse { warehouse_id },
        TagAttachmentTarget::Namespace {
            warehouse_id,
            namespace_id,
        },
        TagAttachmentTarget::Table {
            warehouse_id,
            table_id,
        },
        TagAttachmentTarget::Column {
            warehouse_id,
            table_id,
            field_id: 2,
        },
        TagAttachmentTarget::View {
            warehouse_id,
            view_id,
        },
        TagAttachmentTarget::GenericTable {
            warehouse_id,
            generic_table_id,
        },
    ];
    assert_eq!(targets, expected.iter().collect::<Vec<_>>());
    // Markers carry no value, and every attachment was applied manually.
    assert!(listed.attachments.iter().all(|a| a.value.is_none()));
    assert!(
        listed
            .attachments
            .iter()
            .all(|a| a.source == TagSource::Manual)
    );

    // Keyset convention (matches list-tag-definitions): a trailing token is returned
    // even when the page wasn't full; following it yields an empty final page.
    let tail = Server::list_tag_attachments(
        ctx.clone(),
        request_metadata_with_project(pid),
        def.id,
        ListTagAttachmentsQuery::builder()
            .page_token(listed.next_page_token)
            .page_size(Some(50))
            .build(),
    )
    .await
    .unwrap();
    assert!(tail.attachments.is_empty());
    assert_eq!(tail.next_page_token, None);
}

/// Reverse lookup with a value filter and keyset pagination.
#[sqlx::test]
async fn test_list_tag_attachments_value_filter_and_pagination(pool: PgPool) {
    let (ctx, wh) = setup_catalog(pool).await;
    let pid = &wh.project_id;
    let warehouse_id = wh.warehouse_id;
    let table_id = create_table_with_columns(&ctx, warehouse_id).await;

    let def = create_def(
        &ctx,
        pid,
        "region",
        vec![TagScope::Warehouse, TagScope::Table, TagScope::Column],
        TagValueKind::FreeText,
        None,
    )
    .await
    .unwrap();

    // warehouse="eu", table="us", column(email)="eu".
    Server::set_warehouse_tag(
        warehouse_id,
        "region".to_string(),
        SetTagRequest {
            value: Some("eu".to_string()),
        },
        ctx.clone(),
        request_metadata_with_project(pid),
    )
    .await
    .unwrap();
    Server::set_table_tag(
        warehouse_id,
        table_id,
        "region".to_string(),
        SetTagRequest {
            value: Some("us".to_string()),
        },
        ctx.clone(),
        request_metadata_with_project(pid),
    )
    .await
    .unwrap();
    Server::set_table_column_tag(
        warehouse_id,
        table_id,
        "email".to_string(),
        "region".to_string(),
        SetTagRequest {
            value: Some("eu".to_string()),
        },
        ctx.clone(),
        request_metadata_with_project(pid),
    )
    .await
    .unwrap();

    // No filter -> all three.
    let all = Server::list_tag_attachments(
        ctx.clone(),
        request_metadata_with_project(pid),
        def.id,
        ListTagAttachmentsQuery::builder()
            .page_size(Some(50))
            .build(),
    )
    .await
    .unwrap();
    assert_eq!(all.attachments.len(), 3);

    // Filter "eu" -> warehouse + column (the two "eu" attachments), in apply order.
    let eu = Server::list_tag_attachments(
        ctx.clone(),
        request_metadata_with_project(pid),
        def.id,
        ListTagAttachmentsQuery::builder()
            .page_size(Some(50))
            .value(Some("eu".to_string()))
            .build(),
    )
    .await
    .unwrap();
    let eu_targets: Vec<&TagAttachmentTarget> = eu.attachments.iter().map(|a| &a.target).collect();
    assert_eq!(
        eu_targets,
        vec![
            &TagAttachmentTarget::Warehouse { warehouse_id },
            &TagAttachmentTarget::Column {
                warehouse_id,
                table_id,
                field_id: 2,
            },
        ]
    );
    assert!(
        eu.attachments
            .iter()
            .all(|a| a.value.as_deref() == Some("eu"))
    );

    // A value nothing carries -> empty.
    let apac = Server::list_tag_attachments(
        ctx.clone(),
        request_metadata_with_project(pid),
        def.id,
        ListTagAttachmentsQuery::builder()
            .page_size(Some(50))
            .value(Some("apac".to_string()))
            .build(),
    )
    .await
    .unwrap();
    assert!(apac.attachments.is_empty());
    assert_eq!(apac.next_page_token, None);

    // Keyset pagination: page of 2, then the remaining 1.
    let page1 = Server::list_tag_attachments(
        ctx.clone(),
        request_metadata_with_project(pid),
        def.id,
        ListTagAttachmentsQuery::builder()
            .page_size(Some(2))
            .build(),
    )
    .await
    .unwrap();
    assert_eq!(page1.attachments.len(), 2);
    let token = page1.next_page_token.clone();
    assert!(token.is_some());

    let page2 = Server::list_tag_attachments(
        ctx.clone(),
        request_metadata_with_project(pid),
        def.id,
        ListTagAttachmentsQuery::builder()
            .page_token(token)
            .page_size(Some(2))
            .build(),
    )
    .await
    .unwrap();
    assert_eq!(page2.attachments.len(), 1);
    assert_eq!(
        page2.attachments[0].target,
        TagAttachmentTarget::Column {
            warehouse_id,
            table_id,
            field_id: 2,
        }
    );
}

/// An unused definition reverse-lists as empty; an unknown definition is hidden as
/// not-found (the `ReadAttachments` authz resolves the definition first).
#[sqlx::test]
async fn test_list_tag_attachments_empty_and_unknown(pool: PgPool) {
    let (ctx, wh) = setup_catalog(pool).await;
    let pid = &wh.project_id;

    let def = create_def(
        &ctx,
        pid,
        "unused",
        vec![TagScope::Warehouse],
        TagValueKind::Marker,
        None,
    )
    .await
    .unwrap();

    // Existing but unapplied -> empty, no token.
    let empty = Server::list_tag_attachments(
        ctx.clone(),
        request_metadata_with_project(pid),
        def.id,
        ListTagAttachmentsQuery::builder()
            .page_size(Some(50))
            .build(),
    )
    .await
    .unwrap();
    assert!(empty.attachments.is_empty());
    assert_eq!(empty.next_page_token, None);

    // Unknown definition -> 404 (existence hidden by the authz layer).
    let err = Server::list_tag_attachments(
        ctx.clone(),
        request_metadata_with_project(pid),
        lakekeeper::service::TagDefinitionId::new_random(),
        ListTagAttachmentsQuery::builder()
            .page_size(Some(50))
            .build(),
    )
    .await
    .unwrap_err();
    assert_eq!(err.error.r#type, "TagDefinitionIdNotFound");
    assert_eq!(err.error.code, StatusCode::NOT_FOUND.as_u16());
}

// ==================== Inheritance (effective tags) ====================

/// Create a (possibly nested) namespace and resolve its `NamespaceId`. Parents must
/// already exist.
async fn create_nested_namespace(
    ctx: &Ctx,
    warehouse_id: WarehouseId,
    parts: &[&str],
) -> NamespaceId {
    let prefix: Prefix = warehouse_id.to_string().into();
    let namespace =
        NamespaceIdent::from_vec(parts.iter().map(ToString::to_string).collect()).unwrap();
    CatalogServer::create_namespace(
        Some(prefix),
        CreateNamespaceRequest {
            namespace: namespace.clone(),
            properties: None,
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    PostgresBackend::get_namespace(warehouse_id, namespace, ctx.v1_state.catalog.clone())
        .await
        .unwrap()
        .unwrap()
        .namespace_id()
}

/// Create a table (`id`, `email` columns) in an existing (possibly nested) namespace.
async fn create_table_in(
    ctx: &Ctx,
    warehouse_id: WarehouseId,
    ns_parts: &[&str],
    table_name: &str,
) -> TableId {
    let prefix: Prefix = warehouse_id.to_string().into();
    let namespace =
        NamespaceIdent::from_vec(ns_parts.iter().map(ToString::to_string).collect()).unwrap();
    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "email", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .unwrap();
    let table = CatalogServer::create_table(
        NamespaceParameters {
            namespace,
            prefix: Some(prefix),
        },
        CreateTableRequest {
            name: table_name.to_string(),
            location: None,
            schema,
            partition_spec: Some(UnboundPartitionSpec::builder().build()),
            write_order: None,
            stage_create: Some(false),
            properties: None,
        },
        DataAccess::not_specified(),
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    table.metadata.uuid().into()
}

/// Effective tags on a namespace: inherits from parent namespaces + warehouse,
/// most-specific-wins, with provenance. Direct list stays unchanged.
#[sqlx::test]
async fn test_effective_tags_namespace_inheritance(pool: PgPool) {
    let (ctx, wh) = setup_catalog(pool).await;
    let pid = &wh.project_id;
    let warehouse_id = wh.warehouse_id;

    let ns_a = create_nested_namespace(&ctx, warehouse_id, &["a"]).await;
    let ns_ab = create_nested_namespace(&ctx, warehouse_id, &["a", "b"]).await;

    create_def(
        &ctx,
        pid,
        "region",
        vec![TagScope::Namespace],
        TagValueKind::FreeText,
        None,
    )
    .await
    .unwrap();
    create_def(
        &ctx,
        pid,
        "pii",
        vec![TagScope::Namespace],
        TagValueKind::Marker,
        None,
    )
    .await
    .unwrap();
    create_def(
        &ctx,
        pid,
        "tier",
        vec![TagScope::Warehouse, TagScope::Namespace],
        TagValueKind::FreeText,
        None,
    )
    .await
    .unwrap();

    // ns a: region=eu + pii; ns a.b: region=emea; warehouse: tier=gold.
    let apply_ns = |ns, name: &'static str, value: Option<&'static str>| {
        let ctx = ctx.clone();
        async move {
            Server::set_namespace_tag(
                warehouse_id,
                ns,
                name.to_string(),
                SetTagRequest {
                    value: value.map(ToString::to_string),
                },
                ctx,
                request_metadata_with_project(pid),
            )
            .await
            .unwrap();
        }
    };
    apply_ns(ns_a, "region", Some("eu")).await;
    apply_ns(ns_a, "pii", None).await;
    apply_ns(ns_ab, "region", Some("emea")).await;
    Server::set_warehouse_tag(
        warehouse_id,
        "tier".to_string(),
        SetTagRequest {
            value: Some("gold".to_string()),
        },
        ctx.clone(),
        request_metadata_with_project(pid),
    )
    .await
    .unwrap();

    // Effective on a.b: region resolves to a.b's own "emea" (shadows a's "eu"); pii
    // inherited from a; tier inherited from the warehouse.
    let eff = Server::list_namespace_tags(
        warehouse_id,
        ns_ab,
        ctx.clone(),
        request_metadata_with_project(pid),
        ListTagsQuery {
            effective: Some(true),
        },
    )
    .await
    .unwrap();
    assert_eq!(eff.tags.len(), 3);
    let by_name = |name: &str| eff.tags.iter().find(|t| t.name == name).unwrap();
    assert_eq!(by_name("region").value.as_deref(), Some("emea"));
    assert_eq!(by_name("region").inherited_from, None);
    assert_eq!(
        by_name("pii").inherited_from,
        Some(TagInheritanceSource::Namespace {
            warehouse_id,
            namespace_id: ns_a
        })
    );
    // Inherited marker carries no value.
    assert_eq!(by_name("pii").value, None);
    assert_eq!(by_name("tier").value.as_deref(), Some("gold"));
    assert_eq!(
        by_name("tier").inherited_from,
        Some(TagInheritanceSource::Warehouse { warehouse_id })
    );

    // Direct list is unchanged: only a.b's own region.
    let direct = Server::list_namespace_tags(
        warehouse_id,
        ns_ab,
        ctx.clone(),
        request_metadata_with_project(pid),
        ListTagsQuery::default(),
    )
    .await
    .unwrap();
    assert_eq!(direct.tags.len(), 1);
    assert_eq!(direct.tags[0].name, "region");
    assert_eq!(direct.tags[0].value.as_deref(), Some("emea"));
    assert_eq!(direct.tags[0].inherited_from, None);
}

/// Effective tags on a table: inherits from its namespace chain; a direct table tag
/// shadows an inherited one; a tag on the table's *column* is excluded from the
/// table's effective set (columns do not propagate up).
#[sqlx::test]
async fn test_effective_tags_table_inheritance_and_column_exclusion(pool: PgPool) {
    let (ctx, wh) = setup_catalog(pool).await;
    let pid = &wh.project_id;
    let warehouse_id = wh.warehouse_id;

    let ns_a = create_nested_namespace(&ctx, warehouse_id, &["a"]).await;
    let ns_ab = create_nested_namespace(&ctx, warehouse_id, &["a", "b"]).await;
    let table_id = create_table_in(&ctx, warehouse_id, &["a", "b"], "t").await;

    create_def(
        &ctx,
        pid,
        "region",
        vec![TagScope::Namespace, TagScope::Table],
        TagValueKind::FreeText,
        None,
    )
    .await
    .unwrap();
    create_def(
        &ctx,
        pid,
        "pii",
        vec![TagScope::Namespace, TagScope::Table, TagScope::Column],
        TagValueKind::Marker,
        None,
    )
    .await
    .unwrap();

    // ns a: pii; ns a.b: region=emea; table: region=de (direct); table.email: pii.
    Server::set_namespace_tag(
        warehouse_id,
        ns_a,
        "pii".to_string(),
        SetTagRequest { value: None },
        ctx.clone(),
        request_metadata_with_project(pid),
    )
    .await
    .unwrap();
    Server::set_namespace_tag(
        warehouse_id,
        ns_ab,
        "region".to_string(),
        SetTagRequest {
            value: Some("emea".to_string()),
        },
        ctx.clone(),
        request_metadata_with_project(pid),
    )
    .await
    .unwrap();
    Server::set_table_tag(
        warehouse_id,
        table_id,
        "region".to_string(),
        SetTagRequest {
            value: Some("de".to_string()),
        },
        ctx.clone(),
        request_metadata_with_project(pid),
    )
    .await
    .unwrap();
    Server::set_table_column_tag(
        warehouse_id,
        table_id,
        "email".to_string(),
        "pii".to_string(),
        SetTagRequest { value: None },
        ctx.clone(),
        request_metadata_with_project(pid),
    )
    .await
    .unwrap();

    // Effective on the table: region=de (direct, shadows a.b's "emea"); pii inherited
    // from namespace a. The column's pii is NOT included (field-level, excluded).
    let eff = Server::list_table_tags(
        warehouse_id,
        table_id,
        ctx.clone(),
        request_metadata_with_project(pid),
        ListTagsQuery {
            effective: Some(true),
        },
    )
    .await
    .unwrap();
    assert_eq!(eff.tags.len(), 2);
    let by_name = |name: &str| eff.tags.iter().find(|t| t.name == name).unwrap();
    assert_eq!(by_name("region").value.as_deref(), Some("de"));
    assert_eq!(by_name("region").inherited_from, None);
    assert_eq!(
        by_name("pii").inherited_from,
        Some(TagInheritanceSource::Namespace {
            warehouse_id,
            namespace_id: ns_a
        })
    );

    // Direct list on the table: only its own region=de.
    let direct = Server::list_table_tags(
        warehouse_id,
        table_id,
        ctx.clone(),
        request_metadata_with_project(pid),
        ListTagsQuery::default(),
    )
    .await
    .unwrap();
    assert_eq!(direct.tags.len(), 1);
    assert_eq!(direct.tags[0].name, "region");
    assert_eq!(direct.tags[0].inherited_from, None);
}

/// Effective tags on a view (shares the tabular SQL arm) — including a tag whose
/// scope does NOT list `view`, proving inheritance is scope-independent — plus the
/// no-op targets: `?effective=true` on a warehouse (no ancestors) and on a column
/// (columns never inherit) both equal the direct list.
#[sqlx::test]
async fn test_effective_tags_view_and_noop_targets(pool: PgPool) {
    let (ctx, wh) = setup_catalog(pool).await;
    let pid = &wh.project_id;
    let warehouse_id = wh.warehouse_id;

    let ns_a = create_nested_namespace(&ctx, warehouse_id, &["a"]).await;
    let loaded = create_view(ctx.clone(), &warehouse_id.to_string(), "a", "v1", None)
        .await
        .unwrap();
    let view_id: ViewId = loaded.metadata.uuid().into();
    let table_id = create_table_in(&ctx, warehouse_id, &["a"], "t").await;

    create_def(
        &ctx,
        pid,
        "curated",
        vec![TagScope::Namespace, TagScope::View],
        TagValueKind::FreeText,
        None,
    )
    .await
    .unwrap();
    // pii's scope is namespace/warehouse/column — deliberately NOT `view`.
    create_def(
        &ctx,
        pid,
        "pii",
        vec![TagScope::Namespace, TagScope::Warehouse, TagScope::Column],
        TagValueKind::Marker,
        None,
    )
    .await
    .unwrap();

    Server::set_namespace_tag(
        warehouse_id,
        ns_a,
        "curated".to_string(),
        SetTagRequest {
            value: Some("yes".to_string()),
        },
        ctx.clone(),
        request_metadata_with_project(pid),
    )
    .await
    .unwrap();
    Server::set_warehouse_tag(
        warehouse_id,
        "pii".to_string(),
        SetTagRequest { value: None },
        ctx.clone(),
        request_metadata_with_project(pid),
    )
    .await
    .unwrap();

    // View effective: curated inherited from namespace a; pii inherited from the
    // warehouse EVEN THOUGH pii is not `view`-scoped (inheritance ignores scope).
    let eff = Server::list_view_tags(
        warehouse_id,
        view_id,
        ctx.clone(),
        request_metadata_with_project(pid),
        ListTagsQuery {
            effective: Some(true),
        },
    )
    .await
    .unwrap();
    assert_eq!(eff.tags.len(), 2);
    let by_name = |name: &str| eff.tags.iter().find(|t| t.name == name).unwrap();
    assert_eq!(by_name("curated").value.as_deref(), Some("yes"));
    assert_eq!(
        by_name("curated").inherited_from,
        Some(TagInheritanceSource::Namespace {
            warehouse_id,
            namespace_id: ns_a
        })
    );
    assert_eq!(
        by_name("pii").inherited_from,
        Some(TagInheritanceSource::Warehouse { warehouse_id })
    );

    // Warehouse no-op: effective == direct (its own pii, no ancestors).
    let wh_eff = Server::list_warehouse_tags(
        warehouse_id,
        ctx.clone(),
        request_metadata_with_project(pid),
        ListTagsQuery {
            effective: Some(true),
        },
    )
    .await
    .unwrap();
    assert_eq!(wh_eff.tags.len(), 1);
    assert_eq!(wh_eff.tags[0].name, "pii");
    assert_eq!(wh_eff.tags[0].inherited_from, None);

    // Column no-op: apply pii to a column; effective == direct — the column does NOT
    // inherit the namespace's `curated` or the warehouse's `pii`.
    Server::set_table_column_tag(
        warehouse_id,
        table_id,
        "email".to_string(),
        "pii".to_string(),
        SetTagRequest { value: None },
        ctx.clone(),
        request_metadata_with_project(pid),
    )
    .await
    .unwrap();
    let col_eff = Server::list_table_column_tags(
        warehouse_id,
        table_id,
        "email".to_string(),
        ctx.clone(),
        request_metadata_with_project(pid),
        ListTagsQuery {
            effective: Some(true),
        },
    )
    .await
    .unwrap();
    assert_eq!(col_eff.tags.len(), 1);
    assert_eq!(col_eff.tags[0].name, "pii");
    assert_eq!(col_eff.tags[0].inherited_from, None);
}

/// Re-applying a tag with an identical value is a no-op: `applied_at` does not move.
/// Applying a different value does move it.
#[sqlx::test]
async fn test_reapply_same_value_is_noop(pool: PgPool) {
    let (ctx, wh) = setup_catalog(pool).await;
    let pid = &wh.project_id;
    let warehouse_id = wh.warehouse_id;
    create_def(
        &ctx,
        pid,
        "tier",
        vec![TagScope::Warehouse],
        TagValueKind::FreeText,
        None,
    )
    .await
    .unwrap();

    let set = |value: &'static str| {
        let ctx = ctx.clone();
        async move {
            Server::set_warehouse_tag(
                warehouse_id,
                "tier".to_string(),
                SetTagRequest {
                    value: Some(value.to_string()),
                },
                ctx,
                request_metadata_with_project(pid),
            )
            .await
            .unwrap()
        }
    };

    let first = set("gold").await;
    let reapplied = set("gold").await; // identical -> no-op
    assert_eq!(
        reapplied.applied_at, first.applied_at,
        "re-applying the same value must not move applied_at"
    );

    let changed = set("silver").await; // different value -> real update
    assert_ne!(
        changed.applied_at, first.applied_at,
        "changing the value must move applied_at"
    );
    assert_eq!(
        changed.value.as_deref(),
        Some("silver"),
        "the response must carry the newly-applied value, not the previous one"
    );
}
