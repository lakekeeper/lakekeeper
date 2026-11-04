//! Governance Tags as Iceberg REST Catalog labels on `loadTable` and `loadView`.
//!
//! Tags are written through the Management API and read back through the catalog
//! load handlers against Postgres: effective object tags with inheritance and
//! override, column tags as field labels across schema changes, the value-kind
//! mapping, views, idempotent replays, and the conditional-load `ETag` following
//! tag changes.

use std::collections::BTreeMap;

use iceberg::{
    NamespaceIdent, TableIdent, TableUpdate,
    spec::{NestedField, PrimitiveType, Schema, Type, UnboundPartitionSpec},
};
use iceberg_ext::catalog::rest::{
    CommitTableRequest, CreateTableRequest, ETag, Labels, LoadTableResult,
};
use lakekeeper::{
    ProjectId, WarehouseId,
    api::{
        ApiContext, RequestMetadata, RequestMetadataTestBuilder,
        iceberg::{
            types::Prefix,
            v1::{
                CreateNamespaceRequest, NamespaceParameters, TableParameters, ViewParameters,
                namespace::NamespaceService as _,
                tables::{
                    DataAccess, DataAccessMode, LoadTableRequest, LoadTableResultOrNotModified,
                    TablesService as _,
                },
                views::{LoadViewRequest, ViewService as _},
            },
        },
        management::v1::{
            ApiServer,
            tag::{CreateTagDefinitionRequest, Service as _, SetTagRequest},
        },
    },
    server::{CatalogServer, tables::load_table::load_table},
    service::{
        CatalogNamespaceOps as _, NamespaceId, State, TableId, TagScope, TagValueKind, ViewId,
        authz::AllowAllAuthorizer, idempotency::IdempotencyKey,
    },
};
use lakekeeper_integration_tests::{
    SetupTestCatalog, TestWarehouseResponse, create_view, memory_io_profile,
    random_request_metadata,
};
use lakekeeper_storage_postgres::{PostgresBackend, SecretsState};
use sqlx::PgPool;

type Ctx = ApiContext<State<AllowAllAuthorizer, PostgresBackend, SecretsState>>;
type Server = ApiServer<PostgresBackend, AllowAllAuthorizer, SecretsState>;

const NS: &str = "labels_ns";
const TABLE: &str = "labelled_table";
const VIEW: &str = "labelled_view";

/// One warehouse, plus the request metadata that writes its tags.
struct Fixture {
    ctx: Ctx,
    warehouse_id: WarehouseId,
    project_id: ProjectId,
}

impl Fixture {
    async fn new(pool: PgPool) -> Self {
        let (ctx, wh): (Ctx, TestWarehouseResponse) = SetupTestCatalog::builder()
            .pool(pool)
            .storage_profile(memory_io_profile())
            .authorizer(AllowAllAuthorizer::default())
            .number_of_warehouses(1)
            .build()
            .setup()
            .await;
        Self {
            ctx,
            warehouse_id: wh.warehouse_id,
            project_id: (*wh.project_id).clone(),
        }
    }

    fn request_metadata_with_project(&self) -> RequestMetadata {
        RequestMetadataTestBuilder::builder()
            .project_id(Some(self.project_id.clone().into()))
            .build()
    }

    fn prefix(&self) -> Prefix {
        self.warehouse_id.to_string().into()
    }

    /// Create a tag definition usable on every target these tests tag.
    async fn create_def(
        &self,
        name: &str,
        value_kind: TagValueKind,
        allowed_values: Option<&[&str]>,
    ) {
        Server::create_tag_definition(
            CreateTagDefinitionRequest::builder()
                .name(name.to_string())
                .scope(vec![
                    TagScope::Warehouse,
                    TagScope::Namespace,
                    TagScope::Table,
                    TagScope::View,
                    TagScope::Column,
                ])
                .value_kind(value_kind)
                .allowed_values(allowed_values.map(|v| v.iter().map(ToString::to_string).collect()))
                .build(),
            self.ctx.clone(),
            self.request_metadata_with_project(),
        )
        .await
        .unwrap();
    }

    async fn tag_warehouse(&self, name: &str, value: Option<&str>) {
        Server::set_warehouse_tag(
            self.warehouse_id,
            name.to_string(),
            set_tag_request(value),
            self.ctx.clone(),
            self.request_metadata_with_project(),
        )
        .await
        .unwrap();
    }

    async fn tag_namespace(&self, namespace_id: NamespaceId, name: &str, value: Option<&str>) {
        Server::set_namespace_tag(
            self.warehouse_id,
            namespace_id,
            name.to_string(),
            set_tag_request(value),
            self.ctx.clone(),
            self.request_metadata_with_project(),
        )
        .await
        .unwrap();
    }

    async fn untag_namespace(&self, namespace_id: NamespaceId, name: &str) {
        Server::delete_namespace_tag(
            self.warehouse_id,
            namespace_id,
            name.to_string(),
            self.ctx.clone(),
            self.request_metadata_with_project(),
        )
        .await
        .unwrap();
    }

    async fn tag_table(&self, table_id: TableId, name: &str, value: Option<&str>) {
        Server::set_table_tag(
            self.warehouse_id,
            table_id,
            name.to_string(),
            set_tag_request(value),
            self.ctx.clone(),
            self.request_metadata_with_project(),
        )
        .await
        .unwrap();
    }

    async fn untag_table(&self, table_id: TableId, name: &str) {
        Server::delete_table_tag(
            self.warehouse_id,
            table_id,
            name.to_string(),
            self.ctx.clone(),
            self.request_metadata_with_project(),
        )
        .await
        .unwrap();
    }

    async fn tag_column(&self, table_id: TableId, column: &str, name: &str, value: Option<&str>) {
        Server::set_table_column_tag(
            self.warehouse_id,
            table_id,
            column.to_string(),
            name.to_string(),
            set_tag_request(value),
            self.ctx.clone(),
            self.request_metadata_with_project(),
        )
        .await
        .unwrap();
    }

    async fn untag_column(&self, table_id: TableId, column: &str, name: &str) {
        Server::delete_table_column_tag(
            self.warehouse_id,
            table_id,
            column.to_string(),
            name.to_string(),
            self.ctx.clone(),
            self.request_metadata_with_project(),
        )
        .await
        .unwrap();
    }

    async fn tag_view(&self, view_id: ViewId, name: &str, value: Option<&str>) {
        Server::set_view_tag(
            self.warehouse_id,
            view_id,
            name.to_string(),
            set_tag_request(value),
            self.ctx.clone(),
            self.request_metadata_with_project(),
        )
        .await
        .unwrap();
    }

    async fn create_namespace(&self) -> NamespaceId {
        let namespace = NamespaceIdent::new(NS.to_string());
        CatalogServer::create_namespace(
            Some(self.prefix()),
            CreateNamespaceRequest {
                namespace: namespace.clone(),
                properties: None,
            },
            self.ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();
        PostgresBackend::get_namespace(
            self.warehouse_id,
            namespace,
            self.ctx.v1_state.catalog.clone(),
        )
        .await
        .unwrap()
        .unwrap()
        .namespace_id()
    }

    /// Create the table with fields `id` (1) and `email` (2), under `request_metadata`.
    async fn create_table_as(&self, request_metadata: RequestMetadata) -> LoadTableResult {
        CatalogServer::create_table(
            NamespaceParameters {
                namespace: NamespaceIdent::new(NS.to_string()),
                prefix: Some(self.prefix()),
            },
            CreateTableRequest {
                name: TABLE.to_string(),
                location: None,
                schema: schema(&[(1, "id"), (2, "email")]),
                partition_spec: Some(UnboundPartitionSpec::builder().build()),
                write_order: None,
                stage_create: Some(false),
                properties: None,
            },
            DataAccess::not_specified(),
            self.ctx.clone(),
            request_metadata,
        )
        .await
        .unwrap()
    }

    async fn create_table(&self) -> TableId {
        let table = self.create_table_as(random_request_metadata()).await;
        assert_eq!(table.labels, None, "createTable carries no labels");
        table.metadata.uuid().into()
    }

    fn table_parameters(&self) -> TableParameters {
        TableParameters {
            prefix: Some(self.prefix()),
            table: TableIdent::new(NamespaceIdent::new(NS.to_string()), TABLE.to_string()),
        }
    }

    /// Make `schema` the table's current schema.
    async fn set_current_schema(&self, schema: Schema) {
        CatalogServer::commit_table(
            self.table_parameters(),
            CommitTableRequest {
                identifier: None,
                requirements: vec![],
                updates: vec![
                    TableUpdate::AddSchema { schema },
                    TableUpdate::SetCurrentSchema { schema_id: -1 },
                ],
            },
            self.ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();
    }

    async fn load_table_with_etags(&self, etags: Vec<ETag>) -> LoadTableResultOrNotModified {
        Box::pin(load_table(
            self.table_parameters(),
            LoadTableRequest::builder()
                .data_access(DataAccessMode::ClientManaged)
                .etags(etags)
                .build(),
            self.ctx.clone(),
            random_request_metadata(),
        ))
        .await
        .unwrap()
    }

    async fn load_table(&self) -> LoadTableResult {
        let LoadTableResultOrNotModified::LoadTableResult(result) =
            self.load_table_with_etags(vec![]).await
        else {
            panic!("an unconditional load returns a body");
        };
        result
    }

    /// A conditional load answered with a body; panics on a `304`.
    async fn reload(&self, cached: &LoadTableResult, why: &str) -> LoadTableResult {
        match self.load_table_with_etags(echoed(cached)).await {
            LoadTableResultOrNotModified::LoadTableResult(result) => result,
            LoadTableResultOrNotModified::NotModifiedResponse(_) => panic!("{why} must not 304"),
        }
    }

    /// Asserts a conditional load for `cached` is a `304` echoing its tag.
    async fn assert_not_modified(&self, cached: &LoadTableResult) {
        assert_eq!(
            self.load_table_with_etags(echoed(cached)).await,
            LoadTableResultOrNotModified::NotModifiedResponse(
                cached.etag().expect("a load carries an ETag")
            ),
            "unchanged labels 304 with the cached tag"
        );
    }
}

fn set_tag_request(value: Option<&str>) -> SetTagRequest {
    SetTagRequest {
        value: value.map(str::to_string),
    }
}

fn schema(fields: &[(i32, &str)]) -> Schema {
    Schema::builder()
        .with_fields(
            fields
                .iter()
                .map(|(id, name)| {
                    NestedField::required(*id, *name, Type::Primitive(PrimitiveType::String)).into()
                })
                .collect::<Vec<_>>(),
        )
        .build()
        .unwrap()
}

/// The tag a client echoes in `If-None-Match`, as the HTTP layer parses it.
fn echoed(result: &LoadTableResult) -> Vec<ETag> {
    vec![ETag::from(
        result.etag().expect("a load carries an ETag").validator(),
    )]
}

fn label_map(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect()
}

#[sqlx::test]
async fn test_load_table_without_tags_has_no_labels(pool: PgPool) {
    let f = Fixture::new(pool).await;
    f.create_namespace().await;
    f.create_table().await;

    let loaded = f.load_table().await;
    assert_eq!(loaded.labels, None);
    let json = serde_json::to_value(&loaded).unwrap();
    assert!(json.get("labels").is_none(), "no `labels` key: {json}");
}

/// Warehouse and namespace tags are inherited, the table's own tag overrides the
/// namespace's, each value kind maps to its label value, and column tags group per
/// field id.
#[sqlx::test]
async fn test_load_table_labels_from_governance_tags(pool: PgPool) {
    let f = Fixture::new(pool).await;
    let namespace_id = f.create_namespace().await;
    let table_id = f.create_table().await;

    f.create_def("tier", TagValueKind::FreeText, None).await;
    f.create_def("domain", TagValueKind::FreeText, None).await;
    f.create_def(
        "classification",
        TagValueKind::Enumerated,
        Some(&["internal", "restricted"]),
    )
    .await;
    f.create_def("certified", TagValueKind::Marker, None).await;
    f.create_def("pii", TagValueKind::Marker, None).await;
    f.create_def("pii-type", TagValueKind::FreeText, None).await;

    f.tag_warehouse("tier", Some("gold")).await;
    f.tag_namespace(namespace_id, "domain", Some("payments"))
        .await;
    f.tag_namespace(namespace_id, "classification", Some("internal"))
        .await;
    f.tag_table(table_id, "classification", Some("restricted"))
        .await;
    f.tag_table(table_id, "certified", None).await;
    f.tag_column(table_id, "id", "pii", None).await;
    f.tag_column(table_id, "email", "pii", None).await;
    f.tag_column(table_id, "email", "pii-type", Some("email"))
        .await;

    let loaded = f.load_table().await;
    let expected = Labels::from_maps(
        label_map(&[
            ("certified", "true"),
            ("classification", "restricted"),
            ("domain", "payments"),
            ("tier", "gold"),
        ]),
        BTreeMap::from([
            (1, label_map(&[("pii", "true")])),
            (2, label_map(&[("pii", "true"), ("pii-type", "email")])),
        ]),
    );
    assert_eq!(loaded.labels, expected);
    assert_eq!(
        serde_json::to_value(&loaded).unwrap()["labels"],
        serde_json::json!({
            "object-labels": {
                "certified": "true",
                "classification": "restricted",
                "domain": "payments",
                "tier": "gold"
            },
            "fields": [
                {"field-id": 1, "labels": {"pii": "true"}},
                {"field-id": 2, "labels": {"pii": "true", "pii-type": "email"}}
            ]
        })
    );
}

/// A column label is keyed by field id, so it follows a rename. Once the column is
/// dropped from the current schema the label stays, since an older schema of the
/// table still has the field.
#[sqlx::test]
async fn test_column_labels_follow_field_ids_across_schema_changes(pool: PgPool) {
    let f = Fixture::new(pool).await;
    f.create_namespace().await;
    let table_id = f.create_table().await;
    f.create_def("pii", TagValueKind::Marker, None).await;
    f.tag_column(table_id, "email", "pii", None).await;
    let expected = Labels::from_maps(
        BTreeMap::new(),
        BTreeMap::from([(2, label_map(&[("pii", "true")]))]),
    );
    let tagged = f.load_table().await;
    assert_eq!(tagged.labels, expected);

    f.set_current_schema(schema(&[(1, "id"), (2, "contact")]))
        .await;
    let renamed = f.reload(&tagged, "a schema change").await;
    assert_eq!(renamed.labels, expected);

    f.set_current_schema(schema(&[(1, "id")])).await;
    let dropped = f.reload(&renamed, "a schema change").await;
    assert_eq!(dropped.labels, expected);
}

/// Views carry their effective object tags; they have no field labels.
#[sqlx::test]
async fn test_load_view_labels_from_governance_tags(pool: PgPool) {
    let f = Fixture::new(pool).await;
    let namespace_id = f.create_namespace().await;
    let view_id: ViewId = create_view(f.ctx.clone(), &f.warehouse_id.to_string(), NS, VIEW, None)
        .await
        .unwrap()
        .metadata
        .uuid()
        .into();

    f.create_def("domain", TagValueKind::FreeText, None).await;
    f.create_def("curated", TagValueKind::Marker, None).await;
    f.tag_namespace(namespace_id, "domain", Some("payments"))
        .await;
    f.tag_view(view_id, "curated", None).await;

    let loaded = CatalogServer::load_view(
        ViewParameters {
            prefix: Some(f.prefix()),
            view: TableIdent::new(NamespaceIdent::new(NS.to_string()), VIEW.to_string()),
        },
        LoadViewRequest {
            data_access: DataAccessMode::ClientManaged,
            referenced_by: None,
        },
        f.ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
    assert_eq!(
        loaded.labels,
        Labels::from_maps(
            label_map(&[("curated", "true"), ("domain", "payments")]),
            BTreeMap::new()
        )
    );
}

/// An idempotent replay reproduces the `createTable` response — no labels, and
/// the same tag — even when the table now has inherited labels.
#[sqlx::test]
async fn test_create_table_replay_carries_no_labels(pool: PgPool) {
    let f = Fixture::new(pool).await;
    let namespace_id = f.create_namespace().await;
    f.create_def("domain", TagValueKind::FreeText, None).await;
    f.tag_namespace(namespace_id, "domain", Some("payments"))
        .await;

    let key = IdempotencyKey::parse(&uuid::Uuid::now_v7().to_string()).unwrap();
    let with_key = || {
        let mut metadata = RequestMetadata::new_unauthenticated();
        metadata.with_idempotency_key(key);
        metadata
    };
    let first = f.create_table_as(with_key()).await;
    let replay = f.create_table_as(with_key()).await;
    assert_eq!(first.metadata.uuid(), replay.metadata.uuid());
    assert_eq!(replay.labels, None);
    assert_eq!(replay.etag(), first.etag());

    // A load of the same table returns its labels.
    assert_eq!(
        f.load_table().await.labels,
        Labels::from_maps(label_map(&[("domain", "payments")]), BTreeMap::new())
    );
}

/// A conditional load returns `304` while the labels hold and a body once a tag on
/// the table, an ancestor or a column changes, though the metadata never does.
#[sqlx::test]
async fn test_load_table_etag_follows_tag_changes(pool: PgPool) {
    let f = Fixture::new(pool).await;
    let namespace_id = f.create_namespace().await;
    let table_id = f.create_table().await;
    f.create_def("domain", TagValueKind::FreeText, None).await;
    f.create_def("pii", TagValueKind::Marker, None).await;

    let untagged = f.load_table().await;
    f.assert_not_modified(&untagged).await;

    // An inherited tag appears.
    f.tag_namespace(namespace_id, "domain", Some("payments"))
        .await;
    let inherited = f.reload(&untagged, "a namespace tag change").await;
    assert_eq!(inherited.metadata_location, untagged.metadata_location);
    assert_eq!(
        inherited.labels,
        Labels::from_maps(label_map(&[("domain", "payments")]), BTreeMap::new())
    );
    f.assert_not_modified(&inherited).await;

    // The table's own tag overrides it.
    f.tag_table(table_id, "domain", Some("risk")).await;
    let overridden = f.reload(&inherited, "a table tag override").await;
    assert_eq!(
        overridden.labels,
        Labels::from_maps(label_map(&[("domain", "risk")]), BTreeMap::new())
    );

    // A column tag appears.
    f.tag_column(table_id, "email", "pii", None).await;
    let column = f.reload(&overridden, "a column tag change").await;
    assert_eq!(
        column.labels,
        Labels::from_maps(
            label_map(&[("domain", "risk")]),
            BTreeMap::from([(2, label_map(&[("pii", "true")]))])
        )
    );

    // Removing every tag brings back the body, and the tag, of the untagged table.
    f.untag_column(table_id, "email", "pii").await;
    f.untag_table(table_id, "domain").await;
    f.untag_namespace(namespace_id, "domain").await;
    let cleared = f.reload(&column, "removing tags").await;
    assert_eq!(cleared.labels, None);
    assert_eq!(cleared.etag(), untagged.etag());
}
