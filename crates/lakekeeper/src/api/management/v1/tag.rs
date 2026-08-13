use std::sync::Arc;

use axum::{Json, response::IntoResponse};
use iceberg_ext::catalog::rest::ErrorModel;
use serde::{Deserialize, Serialize};

use crate::{
    ProjectId, WarehouseId,
    api::{
        ApiContext,
        iceberg::{
            types::PageToken,
            v1::{PaginationQuery, tables::LoadTableFilters},
        },
        management::v1::ApiServer,
    },
    request_metadata::RequestMetadata,
    service::{
        ApplyTagError, ArcProjectId, CachePolicy, CatalogBackendError,
        CatalogCreateTagDefinitionRequest, CatalogStore, CatalogTableOps, CatalogTagOps,
        CatalogWarehouseOps, ColumnNotFound, CreateTagDefinitionError, DeleteTagDefinitionError,
        GenericTableId, InvalidTagDefinition, LoadTableError, NamespaceId, RemoveTagError, Result,
        SecretStore, State, TableId, TabularId, TabularListFlags, TagAttachmentFilter,
        TagDefinitionId, TagDefinitionReserved, TagId, TagNameNotFound, TagScope, TagSource,
        TagTarget, TagTargetNotFound, TagValueKind, TagValueSpec, Transaction,
        UpdateTagDefinitionError, UpdateTagDefinitionRequest as CatalogUpdateTagDefinitionRequest,
        ViewId, WarehouseStatus,
        authz::{
            AuthZError, AuthZGenericTableOps, AuthZProjectOps, AuthZTableOps, AuthZTagOps,
            AuthZViewOps, Authorizer, AuthzNamespaceOps, AuthzWarehouseOps,
            CatalogGenericTableAction, CatalogNamespaceAction, CatalogProjectAction,
            CatalogTableAction, CatalogTagAction, CatalogViewAction, CatalogWarehouseAction,
            GrantResource, GrantSpec, RequireTagActionError,
        },
        emit_bootstrap_grants,
        events::{
            APIEventContext,
            context::{
                Unresolved, UserProvidedGenericTable, UserProvidedNamespace, UserProvidedTable,
                UserProvidedView,
            },
        },
        is_reserved_tag_name, validate_scope_widening, validate_tag_name, validate_tag_scope,
        validate_tag_value, validate_tag_value_chars, write_bootstrap_grants,
    },
};

/// Rejects allowed values that would violate the storage contract: every value
/// must be a non-empty, unique string.
fn validate_allowed_values(values: &[String]) -> Result<(), InvalidTagDefinition> {
    for value in values {
        if value.is_empty() {
            return Err(InvalidTagDefinition::new(
                "Allowed values must be non-empty strings",
            ));
        }
        validate_tag_value_chars(value)
            .map_err(|why| InvalidTagDefinition::new(format!("Allowed values {why}")))?;
    }
    let mut deduplicated: Vec<&String> = values.iter().collect();
    deduplicated.sort_unstable();
    deduplicated.dedup();
    if deduplicated.len() != values.len() {
        return Err(InvalidTagDefinition::new("Allowed values must be unique"));
    }
    Ok(())
}

#[derive(Debug, Deserialize, typed_builder::TypedBuilder)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct CreateTagDefinitionRequest {
    /// Name of the tag definition. `.` is the hierarchy delimiter
    /// (e.g. `pii.classification`). Unique within the project,
    /// case-insensitively.
    pub name: String,
    /// Description of the tag definition
    #[serde(default)]
    #[builder(default)]
    pub description: Option<String>,
    /// Target types this definition may be applied to
    pub scope: Vec<TagScope>,
    /// How values of this definition are constrained
    pub value_kind: TagValueKind,
    /// Permitted values. Required (non-empty) for `enumerated` definitions,
    /// must be omitted otherwise.
    #[serde(default)]
    #[builder(default)]
    pub allowed_values: Option<Vec<String>>,
}

#[derive(Debug, Serialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct TagDefinition {
    /// Globally unique UUID identifier
    #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
    pub id: TagDefinitionId,
    /// Name of the tag definition
    pub name: String,
    /// Description of the tag definition
    pub description: Option<String>,
    /// Target types this definition may be applied to
    pub scope: Vec<TagScope>,
    /// How values of this definition are constrained
    pub value_kind: TagValueKind,
    /// Permitted values of an enumerated definition. Present when a single
    /// definition is fetched or created; omitted on list entries.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allowed_values: Option<Vec<String>>,
    /// Timestamp when the tag definition was created
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// Timestamp when the tag definition was last updated
    pub updated_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl From<crate::service::TagDefinition> for TagDefinition {
    fn from(value: crate::service::TagDefinition) -> Self {
        Self {
            id: value.tag_definition_id,
            name: value.name,
            description: value.description,
            scope: value.scope,
            value_kind: value.value_kind,
            allowed_values: None,
            created_at: value.created_at,
            updated_at: value.updated_at,
        }
    }
}

#[derive(Debug, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct UpdateTagDefinitionRequest {
    /// New name of the tag definition (may equal the current name)
    pub name: String,
    /// Description of the tag definition. If not set, the description will be removed.
    #[serde(default)]
    pub description: Option<String>,
    /// Full replacement scope. Must contain every currently configured scope
    /// (scope can only be widened).
    pub scope: Vec<TagScope>,
    /// Values to add to an enumerated definition's allowed set. Existing values
    /// are never removed. Only valid for `enumerated` definitions.
    #[serde(default)]
    pub add_allowed_values: Option<Vec<String>>,
}

#[derive(Debug, Serialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct ListTagDefinitionsResponse {
    pub tag_definitions: Vec<TagDefinition>,
    pub next_page_token: Option<String>,
}

impl From<crate::service::ListTagDefinitionsResponse> for ListTagDefinitionsResponse {
    fn from(value: crate::service::ListTagDefinitionsResponse) -> Self {
        Self {
            tag_definitions: value.tag_definitions.into_iter().map(Into::into).collect(),
            next_page_token: value.next_page_token,
        }
    }
}

impl IntoResponse for ListTagDefinitionsResponse {
    fn into_response(self) -> axum::response::Response {
        (http::StatusCode::OK, Json(self)).into_response()
    }
}

#[derive(Debug, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::IntoParams))]
#[serde(rename_all = "camelCase")]
pub struct ListTagDefinitionsQuery {
    /// Next page token
    #[serde(default)]
    pub page_token: Option<String>,
    /// Signals an upper bound of the number of results that a client will receive.
    /// Default: 100
    #[serde(default)]
    pub page_size: Option<i64>,
    /// Filter by exact name (case-insensitive). If set, the response contains
    /// zero or one entries and no pagination token.
    #[serde(default)]
    pub name: Option<String>,
}

impl ListTagDefinitionsQuery {
    #[must_use]
    pub fn pagination_query(&self) -> PaginationQuery {
        PaginationQuery {
            page_token: self
                .page_token
                .clone()
                .map_or(PageToken::Empty, PageToken::Present),
            page_size: self.page_size,
        }
    }
}

#[derive(Debug, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct SetTagRequest {
    /// Value for the tag. Required for `free-text` and `enumerated`
    /// definitions; must be omitted for `marker` definitions.
    #[serde(default)]
    pub value: Option<String>,
}

#[derive(Debug, Serialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct AppliedTag {
    /// ID of the applied tag definition
    #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
    pub tag_definition_id: TagDefinitionId,
    /// Name of the applied tag definition
    pub name: String,
    /// Value the tag carries on this target
    pub value: Option<String>,
    /// Timestamp when the tag was applied or last updated on this target
    pub applied_at: chrono::DateTime<chrono::Utc>,
}

impl AppliedTag {
    fn new(tag: &crate::service::Tag, definition: &crate::service::TagDefinition) -> Self {
        Self {
            tag_definition_id: definition.tag_definition_id,
            name: definition.name.clone(),
            value: tag.value.clone(),
            applied_at: tag.updated_at.unwrap_or(tag.created_at),
        }
    }
}

#[derive(Debug, Serialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct ListTagsResponse {
    pub tags: Vec<TargetTag>,
}

impl IntoResponse for ListTagsResponse {
    fn into_response(self) -> axum::response::Response {
        (http::StatusCode::OK, Json(self)).into_response()
    }
}

/// A column and the governance tags attached directly to it. The column is identified
/// by Iceberg field-id only; resolve it to a name against the table metadata client-side
/// (field-ids are stable across schema evolution, names are not).
#[derive(Debug, Serialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct ColumnTags {
    /// Iceberg field-id of the column.
    pub field_id: i32,
    /// Tags attached directly to this column. Always non-empty — a column with no tags
    /// is omitted from the response entirely.
    #[cfg_attr(feature = "open-api", schema(min_items = 1))]
    pub tags: Vec<TargetTag>,
}

/// Every column of a table that carries at least one tag, each with its direct tags;
/// columns without tags are omitted. Column tags are never inherited, so this is a
/// direct-only listing (no effective view).
#[derive(Debug, Serialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct ListColumnTagsResponse {
    pub columns: Vec<ColumnTags>,
}

impl IntoResponse for ListColumnTagsResponse {
    fn into_response(self) -> axum::response::Response {
        (http::StatusCode::OK, Json(self)).into_response()
    }
}

/// A tag on a target. With `effective=true` an entry may be inherited from an
/// ancestor — see `inherited-from`. The `value`/`source`/`created-at`/`updated-at`
/// fields describe the attachment that supplies the tag: the queried object itself
/// when `inherited-from` is absent, otherwise the ancestor named there.
#[derive(Debug, Serialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct TargetTag {
    /// ID of the tag definition
    #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
    pub tag_definition_id: TagDefinitionId,
    /// Name of the tag definition
    pub name: String,
    /// Value the tag carries on the supplying attachment
    pub value: Option<String>,
    /// How the supplying attachment was produced (producer axis, e.g. `manual`;
    /// independent of direct-vs-inherited, which is `inherited-from`)
    pub source: TagSource,
    /// Timestamp when the supplying attachment was created
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// Timestamp when the supplying attachment's value was last updated
    pub updated_at: Option<chrono::DateTime<chrono::Utc>>,
    /// For an effective tag inherited from an ancestor, the object it is attached
    /// to. Absent when the tag is attached directly to the queried object. Only
    /// ever populated with `effective=true`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inherited_from: Option<TagInheritanceSource>,
}

/// The ancestor an inherited effective tag is attached to. Restricted to the levels
/// that can actually be ancestors (warehouse / namespace).
// Body fields use per-field `#[serde(rename)]` (not the enum-level
// `rename_all_fields`) because utoipa's ToSchema honors per-field serde renames but
// NOT `rename_all_fields` — the explicit renames keep the generated OpenAPI schema
// and the runtime JSON in kebab-case agreement with the rest of the tag payloads.
#[derive(Debug, Serialize, PartialEq, Eq)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum TagInheritanceSource {
    Warehouse {
        #[serde(rename = "warehouse-id")]
        #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
        warehouse_id: WarehouseId,
    },
    Namespace {
        #[serde(rename = "warehouse-id")]
        #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
        warehouse_id: WarehouseId,
        #[serde(rename = "namespace-id")]
        #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
        namespace_id: NamespaceId,
    },
}

impl TagInheritanceSource {
    /// `None` for a directly-attached tag; the ancestor otherwise.
    fn from_origin(origin: crate::service::EffectiveTagSource) -> Option<Self> {
        use crate::service::EffectiveTagSource;
        match origin {
            EffectiveTagSource::Direct => None,
            EffectiveTagSource::Warehouse { warehouse_id } => {
                Some(Self::Warehouse { warehouse_id })
            }
            EffectiveTagSource::Namespace {
                warehouse_id,
                namespace_id,
            } => Some(Self::Namespace {
                warehouse_id,
                namespace_id,
            }),
        }
    }
}

/// The object a tag is attached to in a reverse-lookup listing. `type`
/// discriminates the target kind; columns are addressed by Iceberg field-id.
#[derive(Debug, Serialize, PartialEq, Eq)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum TagAttachmentTarget {
    Warehouse {
        #[serde(rename = "warehouse-id")]
        #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
        warehouse_id: WarehouseId,
    },
    Namespace {
        #[serde(rename = "warehouse-id")]
        #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
        warehouse_id: WarehouseId,
        #[serde(rename = "namespace-id")]
        #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
        namespace_id: NamespaceId,
    },
    Table {
        #[serde(rename = "warehouse-id")]
        #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
        warehouse_id: WarehouseId,
        #[serde(rename = "table-id")]
        #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
        table_id: TableId,
    },
    View {
        #[serde(rename = "warehouse-id")]
        #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
        warehouse_id: WarehouseId,
        #[serde(rename = "view-id")]
        #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
        view_id: ViewId,
    },
    GenericTable {
        #[serde(rename = "warehouse-id")]
        #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
        warehouse_id: WarehouseId,
        #[serde(rename = "generic-table-id")]
        #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
        generic_table_id: GenericTableId,
    },
    Column {
        #[serde(rename = "warehouse-id")]
        #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
        warehouse_id: WarehouseId,
        #[serde(rename = "table-id")]
        #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
        table_id: TableId,
        /// Iceberg field-id of the column within the table's schema.
        #[serde(rename = "field-id")]
        field_id: i32,
    },
}

impl From<TagTarget> for TagAttachmentTarget {
    fn from(target: TagTarget) -> Self {
        match target {
            TagTarget::Warehouse(warehouse_id) => Self::Warehouse { warehouse_id },
            TagTarget::Namespace {
                warehouse_id,
                namespace_id,
            } => Self::Namespace {
                warehouse_id,
                namespace_id,
            },
            TagTarget::Tabular {
                warehouse_id,
                tabular_id,
            } => match tabular_id {
                TabularId::Table(table_id) => Self::Table {
                    warehouse_id,
                    table_id,
                },
                TabularId::View(view_id) => Self::View {
                    warehouse_id,
                    view_id,
                },
                TabularId::GenericTable(generic_table_id) => Self::GenericTable {
                    warehouse_id,
                    generic_table_id,
                },
            },
            // Column tags are only creatable on tables, so the parent is a table.
            TagTarget::Column {
                warehouse_id,
                tabular_id,
                field_id,
            } => Self::Column {
                warehouse_id,
                table_id: TableId::new(*tabular_id.as_ref()),
                field_id,
            },
        }
    }
}

/// One target a tag definition is attached to (reverse lookup).
#[derive(Debug, Serialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct TagAttachment {
    /// The object the tag is attached to
    pub target: TagAttachmentTarget,
    /// Value the tag carries on this target
    pub value: Option<String>,
    /// How the tag came to exist on this target
    pub source: TagSource,
    /// Timestamp when the tag was applied to this target
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// Timestamp when the tag's value was last updated on this target
    pub updated_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl From<crate::service::Tag> for TagAttachment {
    fn from(tag: crate::service::Tag) -> Self {
        Self {
            target: tag.target.into(),
            value: tag.value,
            source: tag.source,
            created_at: tag.created_at,
            updated_at: tag.updated_at,
        }
    }
}

#[derive(Debug, Serialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct ListTagAttachmentsResponse {
    /// The targets carrying this tag (direct attachments only — no hierarchy expansion)
    pub attachments: Vec<TagAttachment>,
    pub next_page_token: Option<String>,
}

impl From<crate::service::ListTagAttachmentsResponse> for ListTagAttachmentsResponse {
    fn from(value: crate::service::ListTagAttachmentsResponse) -> Self {
        Self {
            attachments: value.tags.into_iter().map(Into::into).collect(),
            next_page_token: value.next_page_token,
        }
    }
}

impl IntoResponse for ListTagAttachmentsResponse {
    fn into_response(self) -> axum::response::Response {
        (http::StatusCode::OK, Json(self)).into_response()
    }
}

#[derive(Debug, Deserialize, typed_builder::TypedBuilder)]
#[cfg_attr(feature = "open-api", derive(utoipa::IntoParams))]
#[serde(rename_all = "camelCase")]
pub struct ListTagAttachmentsQuery {
    /// Next page token
    #[serde(default)]
    #[builder(default)]
    pub page_token: Option<String>,
    /// Signals an upper bound of the number of results that a client will receive.
    /// Default: 100
    #[serde(default)]
    #[builder(default)]
    pub page_size: Option<i64>,
    /// Return only attachments carrying exactly this value (case-sensitive). Omit to
    /// return all. Marker tags carry no value, so a value filter excludes them.
    #[serde(default)]
    #[builder(default)]
    pub value: Option<String>,
    /// Restrict to a single target object type.
    #[serde(default)]
    #[builder(default)]
    pub target_type: Option<TagScope>,
    /// Only attachments created at or after this instant (RFC 3339, inclusive).
    #[serde(default)]
    #[builder(default)]
    pub created_after: Option<chrono::DateTime<chrono::Utc>>,
    /// Only attachments created at or before this instant (RFC 3339, inclusive).
    #[serde(default)]
    #[builder(default)]
    pub created_before: Option<chrono::DateTime<chrono::Utc>>,
    /// Restrict to attachments within a single warehouse.
    #[serde(default)]
    #[builder(default)]
    #[cfg_attr(feature = "open-api", param(value_type = Option<uuid::Uuid>))]
    pub warehouse_id: Option<WarehouseId>,
}

impl ListTagAttachmentsQuery {
    #[must_use]
    pub fn pagination_query(&self) -> PaginationQuery {
        PaginationQuery {
            page_token: self
                .page_token
                .clone()
                .map_or(PageToken::Empty, PageToken::Present),
            page_size: self.page_size,
        }
    }
}

impl<C: CatalogStore, A: Authorizer + Clone, S: SecretStore> Service<C, A, S>
    for ApiServer<C, A, S>
{
}

#[async_trait::async_trait]
pub trait Service<C: CatalogStore, A: Authorizer, S: SecretStore> {
    async fn create_tag_definition(
        request: CreateTagDefinitionRequest,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<TagDefinition> {
        // -------------------- VALIDATIONS --------------------
        validate_tag_name(&request.name)?;
        if is_reserved_tag_name(&request.name) {
            return Err(ErrorModel::from(TagDefinitionReserved::new()).into());
        }
        let allowed_values = request.allowed_values.as_deref().unwrap_or_default();
        match request.value_kind {
            TagValueKind::Enumerated => {
                if allowed_values.is_empty() {
                    return Err(ErrorModel::from(InvalidTagDefinition::new(
                        "Enumerated tag definitions require at least one allowed value",
                    ))
                    .into());
                }
                validate_allowed_values(allowed_values).map_err(ErrorModel::from)?;
            }
            TagValueKind::Marker | TagValueKind::FreeText => {
                if !allowed_values.is_empty() {
                    return Err(ErrorModel::from(InvalidTagDefinition::new(
                        "Allowed values may only be provided for enumerated tag definitions",
                    ))
                    .into());
                }
            }
        }

        let authorizer = context.v1_state.authz;
        let project_id = request_metadata.require_project_id(None)?;

        // -------------------- AUTHZ --------------------
        let event_ctx = APIEventContext::for_project_arc(
            request_metadata.into(),
            context.v1_state.events.clone(),
            project_id.clone(),
            Arc::new(CatalogProjectAction::CreateTag {
                name: Some(request.name.clone()),
            }),
        );
        let catalog_state = context.v1_state.catalog;
        let authz_result = authorize_create_tag_definition::<A, C>(
            authorizer,
            catalog_state,
            &event_ctx,
            &request,
        )
        .await;
        let (event_ctx, (tag_definition, bootstrap_grants)) = event_ctx.emit_authz(authz_result)?;
        emit_bootstrap_grants(
            event_ctx.dispatcher(),
            event_ctx.request_metadata_arc(),
            bootstrap_grants,
        )
        .await;
        let event_ctx = event_ctx.resolve(tag_definition);
        let mut result: TagDefinition = (**event_ctx.resolved()).clone().into();
        // Echo the validated allowed values, sorted, so create and get agree on order.
        if result.value_kind == TagValueKind::Enumerated {
            result.allowed_values = request.allowed_values.map(|mut v| {
                v.sort();
                v
            });
        }
        event_ctx.emit_tag_definition_created();
        Ok(result)
    }

    async fn list_tag_definitions(
        context: ApiContext<State<A, C, S>>,
        query: ListTagDefinitionsQuery,
        request_metadata: RequestMetadata,
    ) -> Result<ListTagDefinitionsResponse> {
        // -------------------- VALIDATIONS --------------------
        let project_id = request_metadata.require_project_id(None)?;

        // -------------------- AUTHZ --------------------
        let event_ctx = APIEventContext::for_project_arc(
            request_metadata.into(),
            context.v1_state.events.clone(),
            project_id.clone(),
            Arc::new(CatalogProjectAction::ListTags),
        );
        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;
        let authz_result =
            authorize_list_tag_definitions::<A, C>(authorizer, catalog_state, &event_ctx, query)
                .await;
        let (_event_ctx, tag_definitions) = event_ctx.emit_authz(authz_result)?;
        Ok(tag_definitions.into())
    }

    async fn get_tag_definition(
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        tag_definition_id: TagDefinitionId,
    ) -> Result<TagDefinition> {
        let project_id = request_metadata.require_project_id(None)?;

        // -------------------- AUTHZ --------------------
        let event_ctx = APIEventContext::for_tag(
            request_metadata.into(),
            context.v1_state.events.clone(),
            tag_definition_id,
            CatalogTagAction::Read,
        );
        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;
        let authz_result =
            authorize_get_tag_definition::<A, C>(authorizer, catalog_state, &event_ctx, project_id)
                .await;
        let (_event_ctx, tag_definition) = event_ctx.emit_authz(authz_result)?;
        Ok(tag_definition)
    }

    async fn list_tag_attachments(
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        tag_definition_id: TagDefinitionId,
        query: ListTagAttachmentsQuery,
    ) -> Result<ListTagAttachmentsResponse> {
        let project_id = request_metadata.require_project_id(None)?;

        // -------------------- AUTHZ --------------------
        let event_ctx = APIEventContext::for_tag(
            request_metadata.into(),
            context.v1_state.events.clone(),
            tag_definition_id,
            CatalogTagAction::ReadAttachments,
        );
        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;
        let authz_result = authorize_list_tag_attachments::<A, C>(
            authorizer,
            catalog_state,
            &event_ctx,
            project_id,
            query,
        )
        .await;
        let (_event_ctx, attachments) = event_ctx.emit_authz(authz_result)?;
        Ok(attachments.into())
    }

    async fn update_tag_definition(
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        tag_definition_id: TagDefinitionId,
        request: UpdateTagDefinitionRequest,
    ) -> Result<TagDefinition> {
        // -------------------- VALIDATIONS --------------------
        validate_tag_name(&request.name)?;
        if is_reserved_tag_name(&request.name) {
            return Err(ErrorModel::from(TagDefinitionReserved::new()).into());
        }
        if let Some(add_allowed_values) = &request.add_allowed_values {
            validate_allowed_values(add_allowed_values).map_err(ErrorModel::from)?;
        }

        let project_id = request_metadata.require_project_id(None)?;

        // -------------------- AUTHZ --------------------
        let event_ctx = APIEventContext::for_tag(
            request_metadata.into(),
            context.v1_state.events.clone(),
            tag_definition_id,
            CatalogTagAction::Update,
        );
        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;
        let authz_result = authorize_update_tag_definition::<A, C>(
            authorizer,
            catalog_state,
            &event_ctx,
            project_id,
            request,
        )
        .await;
        let (event_ctx, (tag_definition, allowed_values)) = event_ctx.emit_authz(authz_result)?;
        let event_ctx = event_ctx.resolve(tag_definition);
        let mut result: TagDefinition = (**event_ctx.resolved()).clone().into();
        result.allowed_values = allowed_values;
        event_ctx.emit_tag_definition_updated();
        Ok(result)
    }

    async fn delete_tag_definition(
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        tag_definition_id: TagDefinitionId,
    ) -> Result<()> {
        let project_id = request_metadata.require_project_id(None)?;

        // -------------------- AUTHZ --------------------
        let event_ctx = APIEventContext::for_tag(
            request_metadata.into(),
            context.v1_state.events.clone(),
            tag_definition_id,
            CatalogTagAction::Delete,
        );
        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;
        let authz_result = authorize_delete_tag_definition::<A, C>(
            authorizer,
            catalog_state,
            &event_ctx,
            project_id,
        )
        .await;
        let (event_ctx, tag_definition) = event_ctx.emit_authz(authz_result)?;
        let event_ctx = event_ctx.resolve(tag_definition);
        event_ctx.emit_tag_definition_deleted();
        Ok(())
    }

    // -------------------- Warehouse tags --------------------

    async fn set_warehouse_tag(
        warehouse_id: WarehouseId,
        tag_name: String,
        request: SetTagRequest,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<AppliedTag> {
        let project_id = request_metadata.require_project_id(None)?;

        // -------------------- AUTHZ --------------------
        let event_ctx = APIEventContext::for_warehouse(
            request_metadata.into(),
            context.v1_state.events.clone(),
            warehouse_id,
            CatalogWarehouseAction::ManageTags,
        );
        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;
        let authz_result = authorize_set_warehouse_tag::<A, C>(
            &authorizer,
            catalog_state,
            &event_ctx,
            &project_id,
            &tag_name,
            request.value.as_deref(),
        )
        .await;
        let (
            event_ctx,
            AppliedTagResult {
                tag,
                tag_definition,
                changed,
            },
        ) = event_ctx.emit_authz(authz_result)?;
        let result = AppliedTag::new(&tag, &tag_definition);
        let event_ctx = event_ctx.resolve((tag, tag_definition));
        // An idempotent re-apply of an identical value changed nothing — no event.
        if changed {
            event_ctx.emit_tag_applied();
        }
        Ok(result)
    }

    async fn delete_warehouse_tag(
        warehouse_id: WarehouseId,
        tag_name: String,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        let project_id = request_metadata.require_project_id(None)?;

        // -------------------- AUTHZ --------------------
        let event_ctx = APIEventContext::for_warehouse(
            request_metadata.into(),
            context.v1_state.events.clone(),
            warehouse_id,
            CatalogWarehouseAction::ManageTags,
        );
        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;
        let authz_result = authorize_delete_warehouse_tag::<A, C>(
            &authorizer,
            catalog_state,
            &event_ctx,
            &project_id,
            &tag_name,
        )
        .await;
        let (event_ctx, removed) = event_ctx.emit_authz(authz_result)?;
        if let Some(removed) = removed {
            let event_ctx = event_ctx.resolve(removed);
            event_ctx.emit_tag_removed();
        }
        Ok(())
    }

    async fn list_warehouse_tags(
        warehouse_id: WarehouseId,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        query: ListTagsQuery,
    ) -> Result<ListTagsResponse> {
        let project_id = request_metadata.require_project_id(None)?;

        // -------------------- AUTHZ --------------------
        let event_ctx = APIEventContext::for_warehouse(
            request_metadata.into(),
            context.v1_state.events.clone(),
            warehouse_id,
            CatalogWarehouseAction::GetMetadata,
        );
        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;
        let authz_result = authorize_list_warehouse_tags::<A, C>(
            &authorizer,
            catalog_state,
            &event_ctx,
            &project_id,
            query.effective(),
        )
        .await;
        let (_event_ctx, tags) = event_ctx.emit_authz(authz_result)?;
        Ok(tags)
    }

    // -------------------- Namespace tags --------------------

    async fn set_namespace_tag(
        warehouse_id: WarehouseId,
        namespace_id: NamespaceId,
        tag_name: String,
        request: SetTagRequest,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<AppliedTag> {
        let project_id = request_metadata.require_project_id(None)?;

        // -------------------- AUTHZ --------------------
        let event_ctx = APIEventContext::for_namespace(
            request_metadata.into(),
            context.v1_state.events.clone(),
            warehouse_id,
            namespace_id,
            CatalogNamespaceAction::ManageTags,
        );
        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;
        let authz_result = authorize_set_namespace_tag::<A, C>(
            &authorizer,
            catalog_state,
            &event_ctx,
            namespace_id,
            &project_id,
            &tag_name,
            request.value.as_deref(),
        )
        .await;
        let (
            event_ctx,
            AppliedTagResult {
                tag,
                tag_definition,
                changed,
            },
        ) = event_ctx.emit_authz(authz_result)?;
        let result = AppliedTag::new(&tag, &tag_definition);
        let event_ctx = event_ctx.resolve((tag, tag_definition));
        // An idempotent re-apply of an identical value changed nothing — no event.
        if changed {
            event_ctx.emit_tag_applied();
        }
        Ok(result)
    }

    async fn delete_namespace_tag(
        warehouse_id: WarehouseId,
        namespace_id: NamespaceId,
        tag_name: String,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        let project_id = request_metadata.require_project_id(None)?;

        // -------------------- AUTHZ --------------------
        let event_ctx = APIEventContext::for_namespace(
            request_metadata.into(),
            context.v1_state.events.clone(),
            warehouse_id,
            namespace_id,
            CatalogNamespaceAction::ManageTags,
        );
        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;
        let authz_result = authorize_delete_namespace_tag::<A, C>(
            &authorizer,
            catalog_state,
            &event_ctx,
            namespace_id,
            &project_id,
            &tag_name,
        )
        .await;
        let (event_ctx, removed) = event_ctx.emit_authz(authz_result)?;
        if let Some(removed) = removed {
            let event_ctx = event_ctx.resolve(removed);
            event_ctx.emit_tag_removed();
        }
        Ok(())
    }

    async fn list_namespace_tags(
        warehouse_id: WarehouseId,
        namespace_id: NamespaceId,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        query: ListTagsQuery,
    ) -> Result<ListTagsResponse> {
        let project_id = request_metadata.require_project_id(None)?;

        // -------------------- AUTHZ --------------------
        let event_ctx = APIEventContext::for_namespace(
            request_metadata.into(),
            context.v1_state.events.clone(),
            warehouse_id,
            namespace_id,
            CatalogNamespaceAction::GetMetadata,
        );
        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;
        let authz_result = authorize_list_namespace_tags::<A, C>(
            &authorizer,
            catalog_state,
            &event_ctx,
            namespace_id,
            &project_id,
            query.effective(),
        )
        .await;
        let (_event_ctx, tags) = event_ctx.emit_authz(authz_result)?;
        Ok(tags)
    }

    // -------------------- Table tags --------------------

    async fn set_table_tag(
        warehouse_id: WarehouseId,
        table_id: TableId,
        tag_name: String,
        request: SetTagRequest,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<AppliedTag> {
        let project_id = request_metadata.require_project_id(None)?;

        // -------------------- AUTHZ --------------------
        let event_ctx = APIEventContext::for_table(
            request_metadata.into(),
            context.v1_state.events.clone(),
            warehouse_id,
            table_id,
            CatalogTableAction::ManageTags,
        );
        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;
        let authz_result = authorize_set_table_tag::<A, C>(
            &authorizer,
            catalog_state,
            &event_ctx,
            table_id,
            &project_id,
            &tag_name,
            request.value.as_deref(),
        )
        .await;
        let (
            event_ctx,
            AppliedTagResult {
                tag,
                tag_definition,
                changed,
            },
        ) = event_ctx.emit_authz(authz_result)?;
        let result = AppliedTag::new(&tag, &tag_definition);
        let event_ctx = event_ctx.resolve((tag, tag_definition));
        // An idempotent re-apply of an identical value changed nothing — no event.
        if changed {
            event_ctx.emit_tag_applied();
        }
        Ok(result)
    }

    async fn delete_table_tag(
        warehouse_id: WarehouseId,
        table_id: TableId,
        tag_name: String,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        let project_id = request_metadata.require_project_id(None)?;

        // -------------------- AUTHZ --------------------
        let event_ctx = APIEventContext::for_table(
            request_metadata.into(),
            context.v1_state.events.clone(),
            warehouse_id,
            table_id,
            CatalogTableAction::ManageTags,
        );
        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;
        let authz_result = authorize_delete_table_tag::<A, C>(
            &authorizer,
            catalog_state,
            &event_ctx,
            table_id,
            &project_id,
            &tag_name,
        )
        .await;
        let (event_ctx, removed) = event_ctx.emit_authz(authz_result)?;
        if let Some(removed) = removed {
            let event_ctx = event_ctx.resolve(removed);
            event_ctx.emit_tag_removed();
        }
        Ok(())
    }

    async fn list_table_tags(
        warehouse_id: WarehouseId,
        table_id: TableId,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        query: ListTagsQuery,
    ) -> Result<ListTagsResponse> {
        let project_id = request_metadata.require_project_id(None)?;

        // -------------------- AUTHZ --------------------
        let event_ctx = APIEventContext::for_table(
            request_metadata.into(),
            context.v1_state.events.clone(),
            warehouse_id,
            table_id,
            CatalogTableAction::GetMetadata,
        );
        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;
        let authz_result = authorize_list_table_tags::<A, C>(
            &authorizer,
            catalog_state,
            &event_ctx,
            table_id,
            &project_id,
            query.effective(),
        )
        .await;
        let (_event_ctx, tags) = event_ctx.emit_authz(authz_result)?;
        Ok(tags)
    }

    // -------------------- Table column tags --------------------

    async fn set_table_column_tag(
        warehouse_id: WarehouseId,
        table_id: TableId,
        column_name: String,
        tag_name: String,
        request: SetTagRequest,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<AppliedTag> {
        let project_id = request_metadata.require_project_id(None)?;

        // -------------------- AUTHZ --------------------
        // Column tags are governed by the owning table's `manage_tags`.
        let event_ctx = APIEventContext::for_table(
            request_metadata.into(),
            context.v1_state.events.clone(),
            warehouse_id,
            table_id,
            CatalogTableAction::ManageTags,
        );
        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;
        let authz_result = authorize_set_table_column_tag::<A, C>(
            &authorizer,
            catalog_state,
            &event_ctx,
            table_id,
            &column_name,
            &project_id,
            &tag_name,
            request.value.as_deref(),
        )
        .await;
        let (
            event_ctx,
            AppliedTagResult {
                tag,
                tag_definition,
                changed,
            },
        ) = event_ctx.emit_authz(authz_result)?;
        let result = AppliedTag::new(&tag, &tag_definition);
        let event_ctx = event_ctx.resolve((tag, tag_definition));
        // An idempotent re-apply of an identical value changed nothing — no event.
        if changed {
            event_ctx.emit_tag_applied();
        }
        Ok(result)
    }

    async fn delete_table_column_tag(
        warehouse_id: WarehouseId,
        table_id: TableId,
        column_name: String,
        tag_name: String,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        let project_id = request_metadata.require_project_id(None)?;

        // -------------------- AUTHZ --------------------
        let event_ctx = APIEventContext::for_table(
            request_metadata.into(),
            context.v1_state.events.clone(),
            warehouse_id,
            table_id,
            CatalogTableAction::ManageTags,
        );
        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;
        let authz_result = authorize_delete_table_column_tag::<A, C>(
            &authorizer,
            catalog_state,
            &event_ctx,
            table_id,
            &column_name,
            &project_id,
            &tag_name,
        )
        .await;
        let (event_ctx, removed) = event_ctx.emit_authz(authz_result)?;
        if let Some(removed) = removed {
            let event_ctx = event_ctx.resolve(removed);
            event_ctx.emit_tag_removed();
        }
        Ok(())
    }

    async fn list_table_column_tags(
        warehouse_id: WarehouseId,
        table_id: TableId,
        column_name: String,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        query: ListTagsQuery,
    ) -> Result<ListTagsResponse> {
        let project_id = request_metadata.require_project_id(None)?;

        // -------------------- AUTHZ --------------------
        let event_ctx = APIEventContext::for_table(
            request_metadata.into(),
            context.v1_state.events.clone(),
            warehouse_id,
            table_id,
            CatalogTableAction::GetMetadata,
        );
        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;
        let authz_result = authorize_list_table_column_tags::<A, C>(
            &authorizer,
            catalog_state,
            &event_ctx,
            table_id,
            &column_name,
            &project_id,
            query.effective(),
        )
        .await;
        let (_event_ctx, tags) = event_ctx.emit_authz(authz_result)?;
        Ok(tags)
    }

    /// A table's column tags in one call, grouped by column — the table-wide counterpart
    /// of `list_table_column_tags` (which lists a single column). Gated once on
    /// `GetMetadata` for the table. Direct tags only (columns do not inherit tags).
    async fn list_column_tags(
        warehouse_id: WarehouseId,
        table_id: TableId,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<ListColumnTagsResponse> {
        let project_id = request_metadata.require_project_id(None)?;

        // -------------------- AUTHZ --------------------
        let event_ctx = APIEventContext::for_table(
            request_metadata.into(),
            context.v1_state.events.clone(),
            warehouse_id,
            table_id,
            CatalogTableAction::GetMetadata,
        );
        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;
        let authz_result = authorize_list_column_tags::<A, C>(
            &authorizer,
            catalog_state,
            &event_ctx,
            table_id,
            &project_id,
        )
        .await;
        let (_event_ctx, columns) = event_ctx.emit_authz(authz_result)?;
        Ok(columns)
    }

    // -------------------- View tags --------------------

    async fn set_view_tag(
        warehouse_id: WarehouseId,
        view_id: ViewId,
        tag_name: String,
        request: SetTagRequest,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<AppliedTag> {
        let project_id = request_metadata.require_project_id(None)?;

        // -------------------- AUTHZ --------------------
        let event_ctx = APIEventContext::for_view(
            request_metadata.into(),
            context.v1_state.events.clone(),
            warehouse_id,
            view_id,
            CatalogViewAction::ManageTags,
        );
        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;
        let authz_result = authorize_set_view_tag::<A, C>(
            &authorizer,
            catalog_state,
            &event_ctx,
            view_id,
            &project_id,
            &tag_name,
            request.value.as_deref(),
        )
        .await;
        let (
            event_ctx,
            AppliedTagResult {
                tag,
                tag_definition,
                changed,
            },
        ) = event_ctx.emit_authz(authz_result)?;
        let result = AppliedTag::new(&tag, &tag_definition);
        let event_ctx = event_ctx.resolve((tag, tag_definition));
        // An idempotent re-apply of an identical value changed nothing — no event.
        if changed {
            event_ctx.emit_tag_applied();
        }
        Ok(result)
    }

    async fn delete_view_tag(
        warehouse_id: WarehouseId,
        view_id: ViewId,
        tag_name: String,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        let project_id = request_metadata.require_project_id(None)?;

        // -------------------- AUTHZ --------------------
        let event_ctx = APIEventContext::for_view(
            request_metadata.into(),
            context.v1_state.events.clone(),
            warehouse_id,
            view_id,
            CatalogViewAction::ManageTags,
        );
        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;
        let authz_result = authorize_delete_view_tag::<A, C>(
            &authorizer,
            catalog_state,
            &event_ctx,
            view_id,
            &project_id,
            &tag_name,
        )
        .await;
        let (event_ctx, removed) = event_ctx.emit_authz(authz_result)?;
        if let Some(removed) = removed {
            let event_ctx = event_ctx.resolve(removed);
            event_ctx.emit_tag_removed();
        }
        Ok(())
    }

    async fn list_view_tags(
        warehouse_id: WarehouseId,
        view_id: ViewId,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        query: ListTagsQuery,
    ) -> Result<ListTagsResponse> {
        let project_id = request_metadata.require_project_id(None)?;

        // -------------------- AUTHZ --------------------
        let event_ctx = APIEventContext::for_view(
            request_metadata.into(),
            context.v1_state.events.clone(),
            warehouse_id,
            view_id,
            CatalogViewAction::GetMetadata,
        );
        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;
        let authz_result = authorize_list_view_tags::<A, C>(
            &authorizer,
            catalog_state,
            &event_ctx,
            view_id,
            &project_id,
            query.effective(),
        )
        .await;
        let (_event_ctx, tags) = event_ctx.emit_authz(authz_result)?;
        Ok(tags)
    }

    // -------------------- Generic table tags --------------------

    async fn set_generic_table_tag(
        warehouse_id: WarehouseId,
        generic_table_id: GenericTableId,
        tag_name: String,
        request: SetTagRequest,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<AppliedTag> {
        let project_id = request_metadata.require_project_id(None)?;

        // -------------------- AUTHZ --------------------
        let event_ctx = APIEventContext::for_generic_table(
            request_metadata.into(),
            context.v1_state.events.clone(),
            warehouse_id,
            generic_table_id,
            CatalogGenericTableAction::ManageTags,
        );
        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;
        let authz_result = authorize_set_generic_table_tag::<A, C>(
            &authorizer,
            catalog_state,
            &event_ctx,
            generic_table_id,
            &project_id,
            &tag_name,
            request.value.as_deref(),
        )
        .await;
        let (
            event_ctx,
            AppliedTagResult {
                tag,
                tag_definition,
                changed,
            },
        ) = event_ctx.emit_authz(authz_result)?;
        let result = AppliedTag::new(&tag, &tag_definition);
        let event_ctx = event_ctx.resolve((tag, tag_definition));
        // An idempotent re-apply of an identical value changed nothing — no event.
        if changed {
            event_ctx.emit_tag_applied();
        }
        Ok(result)
    }

    async fn delete_generic_table_tag(
        warehouse_id: WarehouseId,
        generic_table_id: GenericTableId,
        tag_name: String,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        let project_id = request_metadata.require_project_id(None)?;

        // -------------------- AUTHZ --------------------
        let event_ctx = APIEventContext::for_generic_table(
            request_metadata.into(),
            context.v1_state.events.clone(),
            warehouse_id,
            generic_table_id,
            CatalogGenericTableAction::ManageTags,
        );
        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;
        let authz_result = authorize_delete_generic_table_tag::<A, C>(
            &authorizer,
            catalog_state,
            &event_ctx,
            generic_table_id,
            &project_id,
            &tag_name,
        )
        .await;
        let (event_ctx, removed) = event_ctx.emit_authz(authz_result)?;
        if let Some(removed) = removed {
            let event_ctx = event_ctx.resolve(removed);
            event_ctx.emit_tag_removed();
        }
        Ok(())
    }

    async fn list_generic_table_tags(
        warehouse_id: WarehouseId,
        generic_table_id: GenericTableId,
        context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        query: ListTagsQuery,
    ) -> Result<ListTagsResponse> {
        let project_id = request_metadata.require_project_id(None)?;

        // -------------------- AUTHZ --------------------
        let event_ctx = APIEventContext::for_generic_table(
            request_metadata.into(),
            context.v1_state.events.clone(),
            warehouse_id,
            generic_table_id,
            CatalogGenericTableAction::GetMetadata,
        );
        let authorizer = context.v1_state.authz;
        let catalog_state = context.v1_state.catalog;
        let authz_result = authorize_list_generic_table_tags::<A, C>(
            &authorizer,
            catalog_state,
            &event_ctx,
            generic_table_id,
            &project_id,
            query.effective(),
        )
        .await;
        let (_event_ctx, tags) = event_ctx.emit_authz(authz_result)?;
        Ok(tags)
    }
}

/// Also returns the grants the definition was born with, for the caller to announce once
/// the transaction has committed.
async fn authorize_create_tag_definition<A: Authorizer, C: CatalogStore>(
    authorizer: A,
    catalog_state: C::State,
    event_ctx: &APIEventContext<ProjectId, Unresolved, CatalogProjectAction>,
    request: &CreateTagDefinitionRequest,
) -> Result<(Arc<crate::service::TagDefinition>, Vec<GrantSpec>), AuthZError> {
    let project_id = event_ctx.user_provided_entity_arc_ref();
    let request_metadata = event_ctx.request_metadata();
    let action = event_ctx.action();
    authorizer
        .require_project_action(request_metadata, project_id, action.clone())
        .await?;

    // -------------------- Business Logic --------------------
    let description = request.description.as_deref().filter(|d| !d.is_empty());
    let tag_definition_id = TagDefinitionId::new_random();
    let allowed_values: Vec<&str> = request
        .allowed_values
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(String::as_str)
        .collect();
    let value_spec = match request.value_kind {
        TagValueKind::Marker => TagValueSpec::Marker,
        TagValueKind::FreeText => TagValueSpec::FreeText,
        TagValueKind::Enumerated => TagValueSpec::Enumerated {
            allowed_values: &allowed_values,
        },
    };
    let catalog_request = CatalogCreateTagDefinitionRequest::builder()
        .tag_definition_id(tag_definition_id)
        .name(&request.name)
        .description(description)
        .scope(&request.scope)
        .value_spec(value_spec)
        .build();

    let mut t: <C as CatalogStore>::Transaction =
        C::Transaction::begin_write(catalog_state.clone())
            .await
            .map_err(|e| CatalogBackendError::new_unexpected(e.error))
            .map_err(CreateTagDefinitionError::from)?;
    let tag_definition =
        C::create_tag_definition(project_id, catalog_request, t.transaction()).await?;
    authorizer
        .create_tag(request_metadata, tag_definition_id, project_id.clone())
        .await
        .map_err::<CreateTagDefinitionError, _>(|e| {
            CatalogBackendError::new_unexpected(e.error).into()
        })?;
    let bootstrap_grants = write_bootstrap_grants::<A, C>(
        &authorizer,
        request_metadata,
        &GrantResource::Tag(tag_definition_id),
        t.transaction(),
    )
    .await
    .map_err::<CreateTagDefinitionError, _>(|e| {
        CatalogBackendError::new_unexpected(ErrorModel::from(e)).into()
    })?;
    t.commit()
        .await
        .map_err::<CreateTagDefinitionError, _>(|e| {
            CatalogBackendError::new_unexpected(e.error).into()
        })?;
    Ok((Arc::new(tag_definition), bootstrap_grants))
}

async fn authorize_list_tag_definitions<A: Authorizer, C: CatalogStore>(
    authorizer: A,
    catalog_state: C::State,
    event_ctx: &APIEventContext<ProjectId, Unresolved, CatalogProjectAction>,
    query: ListTagDefinitionsQuery,
) -> Result<crate::service::ListTagDefinitionsResponse, AuthZError> {
    let project_id = event_ctx.user_provided_entity_arc_ref();
    let request_metadata = event_ctx.request_metadata();
    let action = event_ctx.action();
    authorizer
        .require_project_action(request_metadata, project_id, action.clone())
        .await?;

    // -------------------- Business Logic --------------------
    if let Some(name) = &query.name {
        let tag_definitions = C::get_tag_definition_by_name(project_id, name, catalog_state)
            .await
            .map_err(crate::service::ListTagDefinitionsError::from)?
            .into_iter()
            .collect();
        return Ok(crate::service::ListTagDefinitionsResponse {
            tag_definitions,
            next_page_token: None,
        });
    }
    let tag_definitions =
        C::list_tag_definitions(project_id, query.pagination_query(), catalog_state).await?;
    Ok(tag_definitions)
}

async fn authorize_get_tag_definition<A: Authorizer, C: CatalogStore>(
    authorizer: A,
    catalog_state: C::State,
    event_ctx: &APIEventContext<TagDefinitionId, Unresolved, CatalogTagAction>,
    project_id: ArcProjectId,
) -> Result<TagDefinition, AuthZError> {
    let tag_definition_id = *event_ctx.user_provided_entity();
    let request_metadata = event_ctx.request_metadata();
    let action = event_ctx.action();

    let tag_definition =
        C::get_tag_definition(&project_id, tag_definition_id, catalog_state.clone()).await;
    let tag_definition = authorizer
        .require_tag_action(
            request_metadata,
            tag_definition_id,
            tag_definition,
            action.clone(),
        )
        .await?;

    // -------------------- Business Logic --------------------
    let allowed_values = if tag_definition.value_kind == TagValueKind::Enumerated {
        Some(
            C::get_tag_allowed_values(tag_definition_id, catalog_state)
                .await
                .map_err(RequireTagActionError::from)?,
        )
    } else {
        None
    };
    let mut result = TagDefinition::from(tag_definition);
    result.allowed_values = allowed_values;
    Ok(result)
}

async fn authorize_list_tag_attachments<A: Authorizer, C: CatalogStore>(
    authorizer: A,
    catalog_state: C::State,
    event_ctx: &APIEventContext<TagDefinitionId, Unresolved, CatalogTagAction>,
    project_id: ArcProjectId,
    query: ListTagAttachmentsQuery,
) -> Result<crate::service::ListTagAttachmentsResponse, AuthZError> {
    let tag_definition_id = *event_ctx.user_provided_entity();
    let request_metadata = event_ctx.request_metadata();
    let action = event_ctx.action();

    // Resolve + authorize the definition in the request's project. `ReadAttachments`
    // is restricted to tag owners / project security admins (broader disclosure than
    // `Read`); non-existence is hidden as not-found.
    let tag_definition =
        C::get_tag_definition(&project_id, tag_definition_id, catalog_state.clone()).await;
    let _tag_definition = authorizer
        .require_tag_action(
            request_metadata,
            tag_definition_id,
            tag_definition,
            action.clone(),
        )
        .await?;

    // -------------------- Business Logic --------------------
    // All attachments of a project-scoped definition are in-project by construction
    // (apply reconciles the target's project), so no per-row project check is needed.
    let pagination = query.pagination_query();
    let filter = TagAttachmentFilter::builder()
        .value(query.value)
        .target_type(query.target_type)
        .created_after(query.created_after)
        .created_before(query.created_before)
        .warehouse_id(query.warehouse_id)
        .build();
    let attachments =
        C::list_tag_attachments(tag_definition_id, &filter, pagination, catalog_state).await?;
    Ok(attachments)
}

async fn authorize_update_tag_definition<A: Authorizer, C: CatalogStore>(
    authorizer: A,
    catalog_state: C::State,
    event_ctx: &APIEventContext<TagDefinitionId, Unresolved, CatalogTagAction>,
    project_id: ArcProjectId,
    request: UpdateTagDefinitionRequest,
) -> Result<(Arc<crate::service::TagDefinition>, Option<Vec<String>>), AuthZError> {
    let tag_definition_id = *event_ctx.user_provided_entity();
    let request_metadata = event_ctx.request_metadata();
    let action = event_ctx.action();

    let current =
        C::get_tag_definition(&project_id, tag_definition_id, catalog_state.clone()).await;
    let current = authorizer
        .require_tag_action(request_metadata, tag_definition_id, current, action.clone())
        .await?;

    // Reserved definitions are catalog-managed and immutable through this API.
    if current.is_protected() {
        return Err(UpdateTagDefinitionError::from(TagDefinitionReserved::new()).into());
    }
    validate_scope_widening(&current.scope, &request.scope)
        .map_err(UpdateTagDefinitionError::from)?;
    let add_allowed_values: Vec<&str> = request
        .add_allowed_values
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(String::as_str)
        .collect();
    if !add_allowed_values.is_empty() && current.value_kind != TagValueKind::Enumerated {
        return Err(UpdateTagDefinitionError::from(InvalidTagDefinition::new(
            "Allowed values can only be added to enumerated tag definitions",
        ))
        .into());
    }

    // -------------------- Business Logic --------------------
    let description = request.description.as_deref().filter(|d| !d.is_empty());
    let catalog_request = CatalogUpdateTagDefinitionRequest::builder()
        .name(&request.name)
        .description(description)
        .scope(&request.scope)
        .add_allowed_values(&add_allowed_values)
        .build();

    let mut t = C::Transaction::begin_write(catalog_state)
        .await
        .map_err::<UpdateTagDefinitionError, _>(|e| {
            CatalogBackendError::new_unexpected(e.error).into()
        })?;
    // The store reads the merged allowed values back inside this transaction, so the
    // response reflects the write without a post-commit (replica) read.
    let (tag_definition, allowed_values) = C::update_tag_definition(
        &project_id,
        tag_definition_id,
        catalog_request,
        t.transaction(),
    )
    .await?;
    t.commit()
        .await
        .map_err::<UpdateTagDefinitionError, _>(|e| {
            CatalogBackendError::new_unexpected(e.error).into()
        })?;
    // Echo allowed values only for enumerated definitions (empty/irrelevant otherwise).
    let allowed_values =
        (tag_definition.value_kind == TagValueKind::Enumerated).then_some(allowed_values);
    Ok((Arc::new(tag_definition), allowed_values))
}

async fn authorize_delete_tag_definition<A: Authorizer, C: CatalogStore>(
    authorizer: A,
    catalog_state: C::State,
    event_ctx: &APIEventContext<TagDefinitionId, Unresolved, CatalogTagAction>,
    project_id: ArcProjectId,
) -> Result<Arc<crate::service::TagDefinition>, AuthZError> {
    let tag_definition_id = *event_ctx.user_provided_entity();
    let request_metadata = event_ctx.request_metadata();
    let action = event_ctx.action();

    let current =
        C::get_tag_definition(&project_id, tag_definition_id, catalog_state.clone()).await;
    let current = authorizer
        .require_tag_action(request_metadata, tag_definition_id, current, action.clone())
        .await?;

    // Reserved definitions are catalog-managed and immutable through this API.
    if current.is_protected() {
        return Err(DeleteTagDefinitionError::from(TagDefinitionReserved::new()).into());
    }

    // -------------------- Business Logic --------------------
    let mut t = C::Transaction::begin_write(catalog_state)
        .await
        .map_err::<DeleteTagDefinitionError, _>(|e| {
            CatalogBackendError::new_unexpected(e.error).into()
        })?;
    C::delete_tag_definition(&project_id, tag_definition_id, t.transaction()).await?;
    t.commit()
        .await
        .map_err::<DeleteTagDefinitionError, _>(|e| {
            CatalogBackendError::new_unexpected(e.error).into()
        })?;

    // Post-commit: best-effort authz cleanup, matching the tabular drop path.
    // Orphaned edges left by a failed cleanup are harmless — `create_tag`'s
    // `require_no_relations` guard rejects reuse of an id that still has relations.
    authorizer
        .delete_tag(request_metadata, tag_definition_id)
        .await
        .inspect_err(|e| {
            tracing::error!(
                ?e,
                "Failed to delete tag definition from authorizer: {}",
                e.error
            );
        })
        .ok();
    Ok(Arc::new(current))
}

type TagWithDefinition = (Arc<crate::service::Tag>, Arc<crate::service::TagDefinition>);

/// Outcome of applying a tag to a target: the tag, its definition, and whether
/// the apply changed anything. `changed == false` means an idempotent re-apply
/// of the same value — nothing was written (`updated_at` unchanged) and the
/// handler skips the `tag_applied` event. The apply itself is still recorded on
/// the authorization-succeeded audit stream regardless.
struct AppliedTagResult {
    tag: Arc<crate::service::Tag>,
    tag_definition: Arc<crate::service::TagDefinition>,
    changed: bool,
}

// -------------------- Target-side authorization --------------------
// Each helper mirrors the target type's protection handler: it loads the
// target (proving existence) and authorizes the context's action on it,
// then yields the `TagTarget` for the tag-side flow.

/// Reject when the addressed target lives in a different project than the
/// request's project context. Tags are project-scoped vocabulary, so a target
/// outside the caller's project must not go on to resolve a definition — hidden
/// as not-found to avoid cross-project probing.
fn ensure_target_in_project(
    target_project: &ProjectId,
    request_project: &ProjectId,
) -> Result<(), AuthZError> {
    if target_project != request_project {
        return Err(AuthZError::TagTargetNotFound(TagTargetNotFound::new()));
    }
    Ok(())
}

async fn authorize_warehouse_tag_target<A: Authorizer, C: CatalogStore>(
    authorizer: &A,
    catalog_state: C::State,
    event_ctx: &APIEventContext<WarehouseId, Unresolved, CatalogWarehouseAction>,
    request_project_id: &ProjectId,
) -> Result<TagTarget, AuthZError> {
    let warehouse_id = *event_ctx.user_provided_entity();
    let warehouse = C::get_warehouse_by_id_cache_aware(
        warehouse_id,
        WarehouseStatus::active_and_inactive(),
        CachePolicy::Use,
        catalog_state,
    )
    .await;
    let resolved = authorizer
        .require_warehouse_action(
            event_ctx.request_metadata(),
            warehouse_id,
            warehouse,
            event_ctx.action().clone(),
        )
        .await?;
    ensure_target_in_project(resolved.project_id.as_ref(), request_project_id)?;
    Ok(TagTarget::Warehouse(warehouse_id))
}

async fn authorize_namespace_tag_target<A: Authorizer, C: CatalogStore>(
    authorizer: &A,
    catalog_state: C::State,
    event_ctx: &APIEventContext<UserProvidedNamespace, Unresolved, CatalogNamespaceAction>,
    namespace_id: NamespaceId,
    request_project_id: &ProjectId,
) -> Result<TagTarget, AuthZError> {
    let warehouse_id = event_ctx.user_provided_entity().warehouse_id;
    let (warehouse, _) = authorizer
        .load_and_authorize_namespace_action::<C>(
            event_ctx.request_metadata(),
            event_ctx.user_provided_entity().clone(),
            event_ctx.action().clone(),
            CachePolicy::Skip,
            catalog_state,
        )
        .await?;
    ensure_target_in_project(warehouse.project_id.as_ref(), request_project_id)?;
    Ok(TagTarget::Namespace {
        warehouse_id,
        namespace_id,
    })
}

async fn authorize_table_tag_target<A: Authorizer, C: CatalogStore>(
    authorizer: &A,
    catalog_state: C::State,
    event_ctx: &APIEventContext<UserProvidedTable, Unresolved, CatalogTableAction>,
    table_id: TableId,
    request_project_id: &ProjectId,
) -> Result<TagTarget, AuthZError> {
    let warehouse_id = event_ctx.user_provided_entity().warehouse_id;
    let (warehouse, _, _) = authorizer
        .load_and_authorize_table_operation::<C>(
            event_ctx.request_metadata(),
            event_ctx.user_provided_entity(),
            TabularListFlags::all(),
            event_ctx.action().clone(),
            catalog_state,
        )
        .await?;
    ensure_target_in_project(warehouse.project_id.as_ref(), request_project_id)?;
    Ok(TagTarget::Tabular {
        warehouse_id,
        tabular_id: TabularId::Table(table_id),
    })
}

async fn authorize_table_column_tag_target<A: Authorizer, C: CatalogStore>(
    authorizer: &A,
    catalog_state: C::State,
    event_ctx: &APIEventContext<UserProvidedTable, Unresolved, CatalogTableAction>,
    table_id: TableId,
    column_name: &str,
    request_project_id: &ProjectId,
) -> Result<TagTarget, AuthZError> {
    let warehouse_id = event_ctx.user_provided_entity().warehouse_id;
    let (warehouse, _, _) = authorizer
        .load_and_authorize_table_operation::<C>(
            event_ctx.request_metadata(),
            event_ctx.user_provided_entity(),
            TabularListFlags::all(),
            event_ctx.action().clone(),
            catalog_state.clone(),
        )
        .await?;
    ensure_target_in_project(warehouse.project_id.as_ref(), request_project_id)?;
    let field_id =
        resolve_column_field_id::<C>(warehouse_id, table_id, column_name, catalog_state).await?;
    Ok(TagTarget::Column {
        warehouse_id,
        tabular_id: TabularId::Table(table_id),
        field_id,
    })
}

async fn authorize_view_tag_target<A: Authorizer, C: CatalogStore>(
    authorizer: &A,
    catalog_state: C::State,
    event_ctx: &APIEventContext<UserProvidedView, Unresolved, CatalogViewAction>,
    view_id: ViewId,
    request_project_id: &ProjectId,
) -> Result<TagTarget, AuthZError> {
    let warehouse_id = event_ctx.user_provided_entity().warehouse_id;
    let (warehouse, _, _) = authorizer
        .load_and_authorize_view_operation::<C>(
            event_ctx.request_metadata(),
            event_ctx.user_provided_entity(),
            TabularListFlags::all(),
            event_ctx.action().clone(),
            catalog_state,
        )
        .await?;
    ensure_target_in_project(warehouse.project_id.as_ref(), request_project_id)?;
    Ok(TagTarget::Tabular {
        warehouse_id,
        tabular_id: TabularId::View(view_id),
    })
}

async fn authorize_generic_table_tag_target<A: Authorizer, C: CatalogStore>(
    authorizer: &A,
    catalog_state: C::State,
    event_ctx: &APIEventContext<UserProvidedGenericTable, Unresolved, CatalogGenericTableAction>,
    generic_table_id: GenericTableId,
    request_project_id: &ProjectId,
) -> Result<TagTarget, AuthZError> {
    let warehouse_id = event_ctx.user_provided_entity().warehouse_id;
    let (warehouse, _, _) = authorizer
        .load_and_authorize_generic_table_operation::<C>(
            event_ctx.request_metadata(),
            event_ctx.user_provided_entity(),
            TabularListFlags::all(),
            event_ctx.action().clone(),
            catalog_state,
        )
        .await?;
    ensure_target_in_project(warehouse.project_id.as_ref(), request_project_id)?;
    Ok(TagTarget::Tabular {
        warehouse_id,
        tabular_id: TabularId::GenericTable(generic_table_id),
    })
}

/// Resolve a column name (nested paths use `.` as separator) against the
/// table's current schema.
async fn resolve_column_field_id<C: CatalogStore>(
    warehouse_id: WarehouseId,
    table_id: TableId,
    column_name: &str,
    catalog_state: C::State,
) -> Result<i32, AuthZError> {
    let mut t = C::Transaction::begin_read(catalog_state)
        .await
        .map_err(|e| CatalogBackendError::new_unexpected(e.error))
        .map_err(RequireTagActionError::from)?;
    let tables = C::load_tables(
        warehouse_id,
        [table_id],
        true,
        &LoadTableFilters::default(),
        t.transaction(),
    )
    .await
    .map_err(|e| match e {
        LoadTableError::CatalogBackendError(e) => e,
        e => CatalogBackendError::new_unexpected(e),
    })
    .map_err(RequireTagActionError::from)?;
    t.commit()
        .await
        .map_err(|e| CatalogBackendError::new_unexpected(e.error))
        .map_err(RequireTagActionError::from)?;
    let table = tables
        .into_iter()
        .find(|r| r.table_id == table_id)
        .ok_or_else(TagTargetNotFound::new)?;
    table
        .table_metadata
        .current_schema()
        .field_id_by_name(column_name)
        .ok_or_else(|| ColumnNotFound::new(column_name).into())
}

// -------------------- Tag-side flows --------------------

/// Resolve `tag_name` to its definition and authorize `action` on it.
async fn require_tag_definition_by_name<A: Authorizer, C: CatalogStore>(
    authorizer: &A,
    request_metadata: &RequestMetadata,
    project_id: &ProjectId,
    tag_name: &str,
    action: CatalogTagAction,
    catalog_state: C::State,
) -> Result<crate::service::TagDefinition, AuthZError> {
    let definition = C::get_tag_definition_by_name(project_id, tag_name, catalog_state)
        .await
        .map_err(RequireTagActionError::from)?
        .ok_or_else(|| TagNameNotFound::new(tag_name))?;
    Ok(authorizer
        .require_tag_action(
            request_metadata,
            definition.tag_definition_id,
            Ok(Some(definition)),
            action,
        )
        .await?)
}

async fn set_tag_on_target<A: Authorizer, C: CatalogStore>(
    authorizer: &A,
    catalog_state: C::State,
    request_metadata: &RequestMetadata,
    project_id: &ProjectId,
    tag_name: &str,
    value: Option<&str>,
    target: TagTarget,
) -> Result<AppliedTagResult, AuthZError> {
    let definition = require_tag_definition_by_name::<A, C>(
        authorizer,
        request_metadata,
        project_id,
        tag_name,
        CatalogTagAction::Apply,
        catalog_state.clone(),
    )
    .await?;
    validate_tag_scope(&definition.scope, target.scope()).map_err(ApplyTagError::from)?;
    let allowed_values = if definition.value_kind == TagValueKind::Enumerated {
        C::get_tag_allowed_values(definition.tag_definition_id, catalog_state.clone())
            .await
            .map_err(RequireTagActionError::from)?
    } else {
        Vec::new()
    };
    validate_tag_value(definition.value_kind, &allowed_values, value)
        .map_err(ApplyTagError::from)?;

    let mut t = C::Transaction::begin_write(catalog_state)
        .await
        .map_err(|e| CatalogBackendError::new_unexpected(e.error))
        .map_err(ApplyTagError::from)?;
    let (tag, changed) = C::apply_tag(
        TagId::new_random(),
        definition.tag_definition_id,
        target,
        value,
        TagSource::Manual,
        t.transaction(),
    )
    .await?;
    t.commit()
        .await
        .map_err::<ApplyTagError, _>(|e| CatalogBackendError::new_unexpected(e.error).into())?;
    Ok(AppliedTagResult {
        tag: Arc::new(tag),
        tag_definition: Arc::new(definition),
        changed,
    })
}

/// Remove the manual tag of the given definition from the target. Returns
/// `None` (without touching the catalog) when the definition exists but is
/// not attached to this target — removal is idempotent.
async fn delete_tag_from_target<A: Authorizer, C: CatalogStore>(
    authorizer: &A,
    catalog_state: C::State,
    request_metadata: &RequestMetadata,
    project_id: &ProjectId,
    tag_name: &str,
    target: TagTarget,
) -> Result<Option<TagWithDefinition>, AuthZError> {
    let definition = require_tag_definition_by_name::<A, C>(
        authorizer,
        request_metadata,
        project_id,
        tag_name,
        CatalogTagAction::Remove,
        catalog_state.clone(),
    )
    .await?;

    // Atomic delete-and-return in one write transaction: no replica read between
    // "find" and "delete" (so a just-applied tag is seen), and concurrent deletes
    // resolve to an idempotent `None` rather than an error.
    let mut t = C::Transaction::begin_write(catalog_state)
        .await
        .map_err(|e| CatalogBackendError::new_unexpected(e.error))
        .map_err(RemoveTagError::from)?;
    let removed = C::remove_tag_for_target(
        target,
        definition.tag_definition_id,
        TagSource::Manual,
        t.transaction(),
    )
    .await?;
    t.commit()
        .await
        .map_err::<RemoveTagError, _>(|e| CatalogBackendError::new_unexpected(e.error).into())?;
    Ok(removed.map(|tag| (Arc::new(tag), Arc::new(definition))))
}

async fn list_tags_on_target<C: CatalogStore>(
    catalog_state: C::State,
    target: TagTarget,
) -> Result<ListTagsResponse, AuthZError> {
    // The store joins the definition name (the FK is RESTRICT, so every tag has one),
    // so there is no per-row definition lookup here.
    let tags = C::list_tags_for_target(target, catalog_state)
        .await
        .map_err(RequireTagActionError::from)?;
    let result = tags
        .into_iter()
        .map(|t| TargetTag {
            tag_definition_id: t.tag.tag_definition_id,
            name: t.definition_name,
            value: t.tag.value,
            source: t.tag.source,
            created_at: t.tag.created_at,
            updated_at: t.tag.updated_at,
            inherited_from: None,
        })
        .collect();
    Ok(ListTagsResponse { tags: result })
}

/// Effective (inheritance) view of a target's tags: the target's own direct tags
/// plus tags inherited from ancestors (namespace chain + warehouse), resolved
/// most-specific-wins. Each entry carries `inherited_from` — absent for the target's
/// own tag, otherwise the ancestor it is inherited from.
///
/// Gated solely on the caller's access to the target itself (already authorized by
/// the caller before dispatch): an inherited tag is part of the target's effective
/// governance, so anyone who can read the target's metadata sees it — including the
/// value and the ancestor it comes from. Granting metadata access on a sub-object
/// therefore also discloses the tags it inherits.
async fn list_effective_tags_on_target<C: CatalogStore>(
    catalog_state: C::State,
    target: TagTarget,
) -> Result<ListTagsResponse, AuthZError> {
    let candidates = C::list_effective_tag_candidates(target, catalog_state)
        .await
        .map_err(RequireTagActionError::from)?;
    let tags = crate::service::resolve_effective_tags(candidates)
        .into_iter()
        .map(|c| TargetTag {
            tag_definition_id: c.tag_definition_id,
            name: c.name,
            value: c.value,
            source: c.source,
            created_at: c.created_at,
            updated_at: c.updated_at,
            inherited_from: TagInheritanceSource::from_origin(c.origin),
        })
        .collect();
    Ok(ListTagsResponse { tags })
}

/// Route a resolved+authorized target to the direct or effective (inheritance) list.
async fn list_tags_dispatch<C: CatalogStore>(
    effective: bool,
    catalog_state: C::State,
    target: TagTarget,
) -> Result<ListTagsResponse, AuthZError> {
    if effective {
        list_effective_tags_on_target::<C>(catalog_state, target).await
    } else {
        list_tags_on_target::<C>(catalog_state, target).await
    }
}

/// Query parameters for the tag list endpoints.
#[derive(Debug, Default, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::IntoParams))]
#[serde(rename_all = "camelCase")]
pub struct ListTagsQuery {
    /// When `true`, return the resolved *effective* tags — the target's own tags
    /// plus tags inherited from its ancestor namespaces and warehouse
    /// (most-specific-wins) — instead of only directly-attached tags. No-op for a
    /// warehouse (no ancestors) and for a column (columns do not inherit).
    #[serde(default)]
    pub effective: Option<bool>,
}

impl ListTagsQuery {
    #[must_use]
    pub fn effective(&self) -> bool {
        self.effective.unwrap_or(false)
    }
}

// -------------------- Per-endpoint authorization --------------------

async fn authorize_set_warehouse_tag<A: Authorizer, C: CatalogStore>(
    authorizer: &A,
    catalog_state: C::State,
    event_ctx: &APIEventContext<WarehouseId, Unresolved, CatalogWarehouseAction>,
    project_id: &ProjectId,
    tag_name: &str,
    value: Option<&str>,
) -> Result<AppliedTagResult, AuthZError> {
    let target = authorize_warehouse_tag_target::<A, C>(
        authorizer,
        catalog_state.clone(),
        event_ctx,
        project_id,
    )
    .await?;
    set_tag_on_target::<A, C>(
        authorizer,
        catalog_state,
        event_ctx.request_metadata(),
        project_id,
        tag_name,
        value,
        target,
    )
    .await
}

async fn authorize_delete_warehouse_tag<A: Authorizer, C: CatalogStore>(
    authorizer: &A,
    catalog_state: C::State,
    event_ctx: &APIEventContext<WarehouseId, Unresolved, CatalogWarehouseAction>,
    project_id: &ProjectId,
    tag_name: &str,
) -> Result<Option<TagWithDefinition>, AuthZError> {
    let target = authorize_warehouse_tag_target::<A, C>(
        authorizer,
        catalog_state.clone(),
        event_ctx,
        project_id,
    )
    .await?;
    delete_tag_from_target::<A, C>(
        authorizer,
        catalog_state,
        event_ctx.request_metadata(),
        project_id,
        tag_name,
        target,
    )
    .await
}

async fn authorize_list_warehouse_tags<A: Authorizer, C: CatalogStore>(
    authorizer: &A,
    catalog_state: C::State,
    event_ctx: &APIEventContext<WarehouseId, Unresolved, CatalogWarehouseAction>,
    project_id: &ProjectId,
    effective: bool,
) -> Result<ListTagsResponse, AuthZError> {
    let target = authorize_warehouse_tag_target::<A, C>(
        authorizer,
        catalog_state.clone(),
        event_ctx,
        project_id,
    )
    .await?;
    list_tags_dispatch::<C>(effective, catalog_state, target).await
}

async fn authorize_set_namespace_tag<A: Authorizer, C: CatalogStore>(
    authorizer: &A,
    catalog_state: C::State,
    event_ctx: &APIEventContext<UserProvidedNamespace, Unresolved, CatalogNamespaceAction>,
    namespace_id: NamespaceId,
    project_id: &ProjectId,
    tag_name: &str,
    value: Option<&str>,
) -> Result<AppliedTagResult, AuthZError> {
    let target = authorize_namespace_tag_target::<A, C>(
        authorizer,
        catalog_state.clone(),
        event_ctx,
        namespace_id,
        project_id,
    )
    .await?;
    set_tag_on_target::<A, C>(
        authorizer,
        catalog_state,
        event_ctx.request_metadata(),
        project_id,
        tag_name,
        value,
        target,
    )
    .await
}

async fn authorize_delete_namespace_tag<A: Authorizer, C: CatalogStore>(
    authorizer: &A,
    catalog_state: C::State,
    event_ctx: &APIEventContext<UserProvidedNamespace, Unresolved, CatalogNamespaceAction>,
    namespace_id: NamespaceId,
    project_id: &ProjectId,
    tag_name: &str,
) -> Result<Option<TagWithDefinition>, AuthZError> {
    let target = authorize_namespace_tag_target::<A, C>(
        authorizer,
        catalog_state.clone(),
        event_ctx,
        namespace_id,
        project_id,
    )
    .await?;
    delete_tag_from_target::<A, C>(
        authorizer,
        catalog_state,
        event_ctx.request_metadata(),
        project_id,
        tag_name,
        target,
    )
    .await
}

async fn authorize_list_namespace_tags<A: Authorizer, C: CatalogStore>(
    authorizer: &A,
    catalog_state: C::State,
    event_ctx: &APIEventContext<UserProvidedNamespace, Unresolved, CatalogNamespaceAction>,
    namespace_id: NamespaceId,
    project_id: &ProjectId,
    effective: bool,
) -> Result<ListTagsResponse, AuthZError> {
    let target = authorize_namespace_tag_target::<A, C>(
        authorizer,
        catalog_state.clone(),
        event_ctx,
        namespace_id,
        project_id,
    )
    .await?;
    list_tags_dispatch::<C>(effective, catalog_state, target).await
}

async fn authorize_set_table_tag<A: Authorizer, C: CatalogStore>(
    authorizer: &A,
    catalog_state: C::State,
    event_ctx: &APIEventContext<UserProvidedTable, Unresolved, CatalogTableAction>,
    table_id: TableId,
    project_id: &ProjectId,
    tag_name: &str,
    value: Option<&str>,
) -> Result<AppliedTagResult, AuthZError> {
    let target = authorize_table_tag_target::<A, C>(
        authorizer,
        catalog_state.clone(),
        event_ctx,
        table_id,
        project_id,
    )
    .await?;
    set_tag_on_target::<A, C>(
        authorizer,
        catalog_state,
        event_ctx.request_metadata(),
        project_id,
        tag_name,
        value,
        target,
    )
    .await
}

async fn authorize_delete_table_tag<A: Authorizer, C: CatalogStore>(
    authorizer: &A,
    catalog_state: C::State,
    event_ctx: &APIEventContext<UserProvidedTable, Unresolved, CatalogTableAction>,
    table_id: TableId,
    project_id: &ProjectId,
    tag_name: &str,
) -> Result<Option<TagWithDefinition>, AuthZError> {
    let target = authorize_table_tag_target::<A, C>(
        authorizer,
        catalog_state.clone(),
        event_ctx,
        table_id,
        project_id,
    )
    .await?;
    delete_tag_from_target::<A, C>(
        authorizer,
        catalog_state,
        event_ctx.request_metadata(),
        project_id,
        tag_name,
        target,
    )
    .await
}

async fn authorize_list_table_tags<A: Authorizer, C: CatalogStore>(
    authorizer: &A,
    catalog_state: C::State,
    event_ctx: &APIEventContext<UserProvidedTable, Unresolved, CatalogTableAction>,
    table_id: TableId,
    project_id: &ProjectId,
    effective: bool,
) -> Result<ListTagsResponse, AuthZError> {
    let target = authorize_table_tag_target::<A, C>(
        authorizer,
        catalog_state.clone(),
        event_ctx,
        table_id,
        project_id,
    )
    .await?;
    list_tags_dispatch::<C>(effective, catalog_state, target).await
}

#[allow(clippy::too_many_arguments)]
async fn authorize_set_table_column_tag<A: Authorizer, C: CatalogStore>(
    authorizer: &A,
    catalog_state: C::State,
    event_ctx: &APIEventContext<UserProvidedTable, Unresolved, CatalogTableAction>,
    table_id: TableId,
    column_name: &str,
    project_id: &ProjectId,
    tag_name: &str,
    value: Option<&str>,
) -> Result<AppliedTagResult, AuthZError> {
    let target = authorize_table_column_tag_target::<A, C>(
        authorizer,
        catalog_state.clone(),
        event_ctx,
        table_id,
        column_name,
        project_id,
    )
    .await?;
    set_tag_on_target::<A, C>(
        authorizer,
        catalog_state,
        event_ctx.request_metadata(),
        project_id,
        tag_name,
        value,
        target,
    )
    .await
}

async fn authorize_delete_table_column_tag<A: Authorizer, C: CatalogStore>(
    authorizer: &A,
    catalog_state: C::State,
    event_ctx: &APIEventContext<UserProvidedTable, Unresolved, CatalogTableAction>,
    table_id: TableId,
    column_name: &str,
    project_id: &ProjectId,
    tag_name: &str,
) -> Result<Option<TagWithDefinition>, AuthZError> {
    let target = authorize_table_column_tag_target::<A, C>(
        authorizer,
        catalog_state.clone(),
        event_ctx,
        table_id,
        column_name,
        project_id,
    )
    .await?;
    delete_tag_from_target::<A, C>(
        authorizer,
        catalog_state,
        event_ctx.request_metadata(),
        project_id,
        tag_name,
        target,
    )
    .await
}

async fn authorize_list_table_column_tags<A: Authorizer, C: CatalogStore>(
    authorizer: &A,
    catalog_state: C::State,
    event_ctx: &APIEventContext<UserProvidedTable, Unresolved, CatalogTableAction>,
    table_id: TableId,
    column_name: &str,
    project_id: &ProjectId,
    effective: bool,
) -> Result<ListTagsResponse, AuthZError> {
    let target = authorize_table_column_tag_target::<A, C>(
        authorizer,
        catalog_state.clone(),
        event_ctx,
        table_id,
        column_name,
        project_id,
    )
    .await?;
    list_tags_dispatch::<C>(effective, catalog_state, target).await
}

/// Authorize the table (`GetMetadata`), then list every column's direct tags in one
/// query and group them per column. Direct-only: columns do not inherit tags.
async fn authorize_list_column_tags<A: Authorizer, C: CatalogStore>(
    authorizer: &A,
    catalog_state: C::State,
    event_ctx: &APIEventContext<UserProvidedTable, Unresolved, CatalogTableAction>,
    table_id: TableId,
    project_id: &ProjectId,
) -> Result<ListColumnTagsResponse, AuthZError> {
    // Authorizes the table for the event's action (`GetMetadata`) and checks project.
    authorize_table_tag_target::<A, C>(
        authorizer,
        catalog_state.clone(),
        event_ctx,
        table_id,
        project_id,
    )
    .await?;
    let warehouse_id = event_ctx.user_provided_entity().warehouse_id;
    let tabular_id = TabularId::Table(table_id);

    let tags = C::list_column_tags_for_tabular(warehouse_id, tabular_id, catalog_state)
        .await
        .map_err(RequireTagActionError::from)?;
    Ok(group_column_tags(tags))
}

/// Group a field-id-ordered list of column tags into per-column entries. Columns are
/// identified by field-id only; callers resolve names against the table metadata.
fn group_column_tags(tags: Vec<crate::service::TagWithName>) -> ListColumnTagsResponse {
    let mut columns: Vec<ColumnTags> = Vec::new();
    let mut prev_field_id: Option<i32> = None;
    for t in tags {
        // The store returns only column tags; skip anything else defensively.
        let TagTarget::Column { field_id, .. } = t.tag.target else {
            continue;
        };
        // Grouping relies on the store's field-id ordering so equal field-ids are
        // contiguous (only the last column is checked when merging). Guard it in debug
        // builds; unordered input would silently split a column into several entries.
        if let Some(prev) = prev_field_id {
            debug_assert!(
                field_id >= prev,
                "column tags must arrive field-id-ordered for grouping"
            );
        }
        prev_field_id = Some(field_id);
        let tag = TargetTag {
            tag_definition_id: t.tag.tag_definition_id,
            name: t.definition_name,
            value: t.tag.value,
            source: t.tag.source,
            created_at: t.tag.created_at,
            updated_at: t.tag.updated_at,
            inherited_from: None,
        };
        match columns.last_mut() {
            Some(c) if c.field_id == field_id => c.tags.push(tag),
            _ => columns.push(ColumnTags {
                field_id,
                tags: vec![tag],
            }),
        }
    }
    ListColumnTagsResponse { columns }
}

async fn authorize_set_view_tag<A: Authorizer, C: CatalogStore>(
    authorizer: &A,
    catalog_state: C::State,
    event_ctx: &APIEventContext<UserProvidedView, Unresolved, CatalogViewAction>,
    view_id: ViewId,
    project_id: &ProjectId,
    tag_name: &str,
    value: Option<&str>,
) -> Result<AppliedTagResult, AuthZError> {
    let target = authorize_view_tag_target::<A, C>(
        authorizer,
        catalog_state.clone(),
        event_ctx,
        view_id,
        project_id,
    )
    .await?;
    set_tag_on_target::<A, C>(
        authorizer,
        catalog_state,
        event_ctx.request_metadata(),
        project_id,
        tag_name,
        value,
        target,
    )
    .await
}

async fn authorize_delete_view_tag<A: Authorizer, C: CatalogStore>(
    authorizer: &A,
    catalog_state: C::State,
    event_ctx: &APIEventContext<UserProvidedView, Unresolved, CatalogViewAction>,
    view_id: ViewId,
    project_id: &ProjectId,
    tag_name: &str,
) -> Result<Option<TagWithDefinition>, AuthZError> {
    let target = authorize_view_tag_target::<A, C>(
        authorizer,
        catalog_state.clone(),
        event_ctx,
        view_id,
        project_id,
    )
    .await?;
    delete_tag_from_target::<A, C>(
        authorizer,
        catalog_state,
        event_ctx.request_metadata(),
        project_id,
        tag_name,
        target,
    )
    .await
}

async fn authorize_list_view_tags<A: Authorizer, C: CatalogStore>(
    authorizer: &A,
    catalog_state: C::State,
    event_ctx: &APIEventContext<UserProvidedView, Unresolved, CatalogViewAction>,
    view_id: ViewId,
    project_id: &ProjectId,
    effective: bool,
) -> Result<ListTagsResponse, AuthZError> {
    let target = authorize_view_tag_target::<A, C>(
        authorizer,
        catalog_state.clone(),
        event_ctx,
        view_id,
        project_id,
    )
    .await?;
    list_tags_dispatch::<C>(effective, catalog_state, target).await
}

async fn authorize_set_generic_table_tag<A: Authorizer, C: CatalogStore>(
    authorizer: &A,
    catalog_state: C::State,
    event_ctx: &APIEventContext<UserProvidedGenericTable, Unresolved, CatalogGenericTableAction>,
    generic_table_id: GenericTableId,
    project_id: &ProjectId,
    tag_name: &str,
    value: Option<&str>,
) -> Result<AppliedTagResult, AuthZError> {
    let target = authorize_generic_table_tag_target::<A, C>(
        authorizer,
        catalog_state.clone(),
        event_ctx,
        generic_table_id,
        project_id,
    )
    .await?;
    set_tag_on_target::<A, C>(
        authorizer,
        catalog_state,
        event_ctx.request_metadata(),
        project_id,
        tag_name,
        value,
        target,
    )
    .await
}

async fn authorize_delete_generic_table_tag<A: Authorizer, C: CatalogStore>(
    authorizer: &A,
    catalog_state: C::State,
    event_ctx: &APIEventContext<UserProvidedGenericTable, Unresolved, CatalogGenericTableAction>,
    generic_table_id: GenericTableId,
    project_id: &ProjectId,
    tag_name: &str,
) -> Result<Option<TagWithDefinition>, AuthZError> {
    let target = authorize_generic_table_tag_target::<A, C>(
        authorizer,
        catalog_state.clone(),
        event_ctx,
        generic_table_id,
        project_id,
    )
    .await?;
    delete_tag_from_target::<A, C>(
        authorizer,
        catalog_state,
        event_ctx.request_metadata(),
        project_id,
        tag_name,
        target,
    )
    .await
}

async fn authorize_list_generic_table_tags<A: Authorizer, C: CatalogStore>(
    authorizer: &A,
    catalog_state: C::State,
    event_ctx: &APIEventContext<UserProvidedGenericTable, Unresolved, CatalogGenericTableAction>,
    generic_table_id: GenericTableId,
    project_id: &ProjectId,
    effective: bool,
) -> Result<ListTagsResponse, AuthZError> {
    let target = authorize_generic_table_tag_target::<A, C>(
        authorizer,
        catalog_state.clone(),
        event_ctx,
        generic_table_id,
        project_id,
    )
    .await?;
    list_tags_dispatch::<C>(effective, catalog_state, target).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::MAX_TAG_VALUE_LEN;

    // The discriminated-union DTOs must emit kebab-case BODY fields (not just kebab
    // variant tags) so they match every sibling field in the same payloads
    // (`inherited-from`, `tag-definition-id`, …). Guards the enum `rename_all_fields`.
    #[test]
    fn tag_attachment_target_serializes_kebab_case_fields() {
        let v = serde_json::to_value(TagAttachmentTarget::Column {
            warehouse_id: WarehouseId::new(uuid::Uuid::nil()),
            table_id: TableId::new(uuid::Uuid::nil()),
            field_id: 3,
        })
        .unwrap();
        assert_eq!(v["type"], "column");
        assert!(
            v.get("warehouse-id").is_some(),
            "expected kebab warehouse-id"
        );
        assert!(v.get("table-id").is_some(), "expected kebab table-id");
        assert!(v.get("field-id").is_some(), "expected kebab field-id");
        assert!(v.get("warehouse_id").is_none(), "must not emit snake_case");

        // Multi-word variant: the discriminant itself must be kebab-cased.
        let g = serde_json::to_value(TagAttachmentTarget::GenericTable {
            warehouse_id: WarehouseId::new(uuid::Uuid::nil()),
            generic_table_id: GenericTableId::new(uuid::Uuid::nil()),
        })
        .unwrap();
        assert_eq!(g["type"], "generic-table");
        assert!(
            g.get("generic-table-id").is_some(),
            "expected kebab generic-table-id"
        );
    }

    #[test]
    fn tag_inheritance_source_serializes_kebab_case_fields() {
        let v = serde_json::to_value(TagInheritanceSource::Namespace {
            warehouse_id: WarehouseId::new(uuid::Uuid::nil()),
            namespace_id: NamespaceId::new(uuid::Uuid::nil()),
        })
        .unwrap();
        assert_eq!(v["type"], "namespace");
        assert!(
            v.get("warehouse-id").is_some(),
            "expected kebab warehouse-id"
        );
        assert!(
            v.get("namespace-id").is_some(),
            "expected kebab namespace-id"
        );
        assert!(v.get("namespace_id").is_none(), "must not emit snake_case");

        // Warehouse variant (the top of the inheritance chain).
        let w = serde_json::to_value(TagInheritanceSource::Warehouse {
            warehouse_id: WarehouseId::new(uuid::Uuid::nil()),
        })
        .unwrap();
        assert_eq!(w["type"], "warehouse");
        assert!(
            w.get("warehouse-id").is_some(),
            "expected kebab warehouse-id"
        );
    }

    #[test]
    fn validate_allowed_values_accepts_unique_non_empty() {
        assert_eq!(
            validate_allowed_values(&["public".to_string(), "internal".to_string()]),
            Ok(())
        );
        assert_eq!(validate_allowed_values(&[]), Ok(()));
    }

    #[test]
    fn validate_allowed_values_rejects_empty_and_duplicates() {
        assert_eq!(
            validate_allowed_values(&["public".to_string(), String::new()]),
            Err(InvalidTagDefinition::new(
                "Allowed values must be non-empty strings"
            ))
        );
        assert_eq!(
            validate_allowed_values(&["public".to_string(), "public".to_string()]),
            Err(InvalidTagDefinition::new("Allowed values must be unique"))
        );
        // Uniqueness is case-sensitive, matching value validation.
        assert_eq!(
            validate_allowed_values(&["public".to_string(), "Public".to_string()]),
            Ok(())
        );
    }

    #[test]
    fn validate_allowed_values_caps_length_and_rejects_control_chars() {
        assert_eq!(
            validate_allowed_values(&["x".repeat(MAX_TAG_VALUE_LEN)]),
            Ok(())
        );
        assert_eq!(
            validate_allowed_values(&["x".repeat(MAX_TAG_VALUE_LEN + 1)]),
            Err(InvalidTagDefinition::new(format!(
                "Allowed values must be at most {MAX_TAG_VALUE_LEN} characters"
            )))
        );
        assert_eq!(
            validate_allowed_values(&["a\0b".to_string()]),
            Err(InvalidTagDefinition::new(
                "Allowed values must not contain control characters"
            ))
        );
    }
}
