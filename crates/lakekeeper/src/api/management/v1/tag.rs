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
        SecretStore, State, TableId, TabularId, TabularListFlags, TagDefinitionId,
        TagDefinitionReserved, TagId, TagNameNotFound, TagScope, TagSource, TagTarget,
        TagTargetNotFound, TagValueKind, TagValueSpec, Transaction, UpdateTagDefinitionError,
        UpdateTagDefinitionRequest as CatalogUpdateTagDefinitionRequest, ViewId, WarehouseStatus,
        authz::{
            AuthZError, AuthZGenericTableOps, AuthZProjectOps, AuthZTableOps, AuthZTagOps,
            AuthZViewOps, Authorizer, AuthzNamespaceOps, AuthzWarehouseOps,
            CatalogGenericTableAction, CatalogNamespaceAction, CatalogProjectAction,
            CatalogTableAction, CatalogTagAction, CatalogViewAction, CatalogWarehouseAction,
            RequireTagActionError,
        },
        events::{
            APIEventContext,
            context::{
                Unresolved, UserProvidedGenericTable, UserProvidedNamespace, UserProvidedTable,
                UserProvidedView,
            },
        },
        is_reserved_tag_name, validate_scope_widening, validate_tag_name, validate_tag_scope,
        validate_tag_value,
    },
};

/// Rejects allowed values that would violate the storage contract: every value
/// must be a non-empty, unique string.
fn validate_allowed_values(values: &[String]) -> Result<(), InvalidTagDefinition> {
    if values.iter().any(String::is_empty) {
        return Err(InvalidTagDefinition::new(
            "Allowed values must be non-empty strings",
        ));
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

/// A tag attached to a target.
#[derive(Debug, Serialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct TargetTag {
    /// ID of the applied tag definition
    #[cfg_attr(feature = "open-api", schema(value_type = uuid::Uuid))]
    pub tag_definition_id: TagDefinitionId,
    /// Name of the applied tag definition
    pub name: String,
    /// Value the tag carries on this target
    pub value: Option<String>,
    /// How the tag came to exist on this target
    pub source: TagSource,
    /// Timestamp when the tag was applied to this target
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// Timestamp when the tag's value was last updated on this target
    pub updated_at: Option<chrono::DateTime<chrono::Utc>>,
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
        let (event_ctx, tag_definition) = event_ctx.emit_authz(authz_result)?;
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
        let (event_ctx, (tag, tag_definition)) = event_ctx.emit_authz(authz_result)?;
        let result = AppliedTag::new(&tag, &tag_definition);
        let event_ctx = event_ctx.resolve((tag, tag_definition));
        event_ctx.emit_tag_applied();
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
        let (event_ctx, (tag, tag_definition)) = event_ctx.emit_authz(authz_result)?;
        let result = AppliedTag::new(&tag, &tag_definition);
        let event_ctx = event_ctx.resolve((tag, tag_definition));
        event_ctx.emit_tag_applied();
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
        let (event_ctx, (tag, tag_definition)) = event_ctx.emit_authz(authz_result)?;
        let result = AppliedTag::new(&tag, &tag_definition);
        let event_ctx = event_ctx.resolve((tag, tag_definition));
        event_ctx.emit_tag_applied();
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
        let (event_ctx, (tag, tag_definition)) = event_ctx.emit_authz(authz_result)?;
        let result = AppliedTag::new(&tag, &tag_definition);
        let event_ctx = event_ctx.resolve((tag, tag_definition));
        event_ctx.emit_tag_applied();
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
        )
        .await;
        let (_event_ctx, tags) = event_ctx.emit_authz(authz_result)?;
        Ok(tags)
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
        let (event_ctx, (tag, tag_definition)) = event_ctx.emit_authz(authz_result)?;
        let result = AppliedTag::new(&tag, &tag_definition);
        let event_ctx = event_ctx.resolve((tag, tag_definition));
        event_ctx.emit_tag_applied();
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
        let (event_ctx, (tag, tag_definition)) = event_ctx.emit_authz(authz_result)?;
        let result = AppliedTag::new(&tag, &tag_definition);
        let event_ctx = event_ctx.resolve((tag, tag_definition));
        event_ctx.emit_tag_applied();
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
        )
        .await;
        let (_event_ctx, tags) = event_ctx.emit_authz(authz_result)?;
        Ok(tags)
    }
}

async fn authorize_create_tag_definition<A: Authorizer, C: CatalogStore>(
    authorizer: A,
    catalog_state: C::State,
    event_ctx: &APIEventContext<ProjectId, Unresolved, CatalogProjectAction>,
    request: &CreateTagDefinitionRequest,
) -> Result<Arc<crate::service::TagDefinition>, AuthZError> {
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
    t.commit()
        .await
        .map_err::<CreateTagDefinitionError, _>(|e| {
            CatalogBackendError::new_unexpected(e.error).into()
        })?;
    Ok(Arc::new(tag_definition))
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

    let read_state = catalog_state.clone();
    let mut t = C::Transaction::begin_write(catalog_state)
        .await
        .map_err::<UpdateTagDefinitionError, _>(|e| {
            CatalogBackendError::new_unexpected(e.error).into()
        })?;
    let tag_definition = C::update_tag_definition(
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
    // For enumerated definitions, return the merged allowed values (sorted by the
    // store) so update echoes the same shape as get; None for other kinds.
    let allowed_values = if tag_definition.value_kind == TagValueKind::Enumerated {
        Some(
            C::get_tag_allowed_values(tag_definition_id, read_state)
                .await
                .map_err(RequireTagActionError::from)?,
        )
    } else {
        None
    };
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
    authorizer
        .delete_tag(request_metadata, tag_definition_id)
        .await
        .map_err::<DeleteTagDefinitionError, _>(|e| {
            CatalogBackendError::new_unexpected(e.error).into()
        })?;
    t.commit()
        .await
        .map_err::<DeleteTagDefinitionError, _>(|e| {
            CatalogBackendError::new_unexpected(e.error).into()
        })?;
    Ok(Arc::new(current))
}

type TagWithDefinition = (Arc<crate::service::Tag>, Arc<crate::service::TagDefinition>);

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
) -> Result<TagWithDefinition, AuthZError> {
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
    let tag = C::apply_tag(
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
    Ok((Arc::new(tag), Arc::new(definition)))
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
    let tags = C::list_tags_for_target(target, catalog_state.clone())
        .await
        .map_err(RequireTagActionError::from)?;
    let Some(tag) = tags.into_iter().find(|t| {
        t.tag_definition_id == definition.tag_definition_id && t.source == TagSource::Manual
    }) else {
        return Ok(None);
    };

    let mut t = C::Transaction::begin_write(catalog_state)
        .await
        .map_err(|e| CatalogBackendError::new_unexpected(e.error))
        .map_err(RemoveTagError::from)?;
    C::remove_tag(tag.tag_id, t.transaction()).await?;
    t.commit()
        .await
        .map_err::<RemoveTagError, _>(|e| CatalogBackendError::new_unexpected(e.error).into())?;
    Ok(Some((Arc::new(tag), Arc::new(definition))))
}

async fn list_tags_on_target<C: CatalogStore>(
    catalog_state: C::State,
    project_id: &ProjectId,
    target: TagTarget,
) -> Result<ListTagsResponse, AuthZError> {
    let tags = C::list_tags_for_target(target, catalog_state.clone())
        .await
        .map_err(RequireTagActionError::from)?;
    let mut result = Vec::with_capacity(tags.len());
    for tag in tags {
        let definition =
            C::get_tag_definition(project_id, tag.tag_definition_id, catalog_state.clone())
                .await
                .map_err(RequireTagActionError::from)?;
        let Some(definition) = definition else {
            // The FK from tags to definitions is RESTRICT, so this cannot
            // happen; skip defensively rather than failing the whole list.
            tracing::warn!(
                "Tag definition `{}` referenced by tag `{}` not found; omitting entry",
                tag.tag_definition_id,
                tag.tag_id
            );
            continue;
        };
        result.push(TargetTag {
            tag_definition_id: tag.tag_definition_id,
            name: definition.name,
            value: tag.value,
            source: tag.source,
            created_at: tag.created_at,
            updated_at: tag.updated_at,
        });
    }
    Ok(ListTagsResponse { tags: result })
}

// -------------------- Per-endpoint authorization --------------------

async fn authorize_set_warehouse_tag<A: Authorizer, C: CatalogStore>(
    authorizer: &A,
    catalog_state: C::State,
    event_ctx: &APIEventContext<WarehouseId, Unresolved, CatalogWarehouseAction>,
    project_id: &ProjectId,
    tag_name: &str,
    value: Option<&str>,
) -> Result<TagWithDefinition, AuthZError> {
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
) -> Result<ListTagsResponse, AuthZError> {
    let target = authorize_warehouse_tag_target::<A, C>(
        authorizer,
        catalog_state.clone(),
        event_ctx,
        project_id,
    )
    .await?;
    list_tags_on_target::<C>(catalog_state, project_id, target).await
}

async fn authorize_set_namespace_tag<A: Authorizer, C: CatalogStore>(
    authorizer: &A,
    catalog_state: C::State,
    event_ctx: &APIEventContext<UserProvidedNamespace, Unresolved, CatalogNamespaceAction>,
    namespace_id: NamespaceId,
    project_id: &ProjectId,
    tag_name: &str,
    value: Option<&str>,
) -> Result<TagWithDefinition, AuthZError> {
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
) -> Result<ListTagsResponse, AuthZError> {
    let target = authorize_namespace_tag_target::<A, C>(
        authorizer,
        catalog_state.clone(),
        event_ctx,
        namespace_id,
        project_id,
    )
    .await?;
    list_tags_on_target::<C>(catalog_state, project_id, target).await
}

async fn authorize_set_table_tag<A: Authorizer, C: CatalogStore>(
    authorizer: &A,
    catalog_state: C::State,
    event_ctx: &APIEventContext<UserProvidedTable, Unresolved, CatalogTableAction>,
    table_id: TableId,
    project_id: &ProjectId,
    tag_name: &str,
    value: Option<&str>,
) -> Result<TagWithDefinition, AuthZError> {
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
) -> Result<ListTagsResponse, AuthZError> {
    let target = authorize_table_tag_target::<A, C>(
        authorizer,
        catalog_state.clone(),
        event_ctx,
        table_id,
        project_id,
    )
    .await?;
    list_tags_on_target::<C>(catalog_state, project_id, target).await
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
) -> Result<TagWithDefinition, AuthZError> {
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
    list_tags_on_target::<C>(catalog_state, project_id, target).await
}

async fn authorize_set_view_tag<A: Authorizer, C: CatalogStore>(
    authorizer: &A,
    catalog_state: C::State,
    event_ctx: &APIEventContext<UserProvidedView, Unresolved, CatalogViewAction>,
    view_id: ViewId,
    project_id: &ProjectId,
    tag_name: &str,
    value: Option<&str>,
) -> Result<TagWithDefinition, AuthZError> {
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
) -> Result<ListTagsResponse, AuthZError> {
    let target = authorize_view_tag_target::<A, C>(
        authorizer,
        catalog_state.clone(),
        event_ctx,
        view_id,
        project_id,
    )
    .await?;
    list_tags_on_target::<C>(catalog_state, project_id, target).await
}

async fn authorize_set_generic_table_tag<A: Authorizer, C: CatalogStore>(
    authorizer: &A,
    catalog_state: C::State,
    event_ctx: &APIEventContext<UserProvidedGenericTable, Unresolved, CatalogGenericTableAction>,
    generic_table_id: GenericTableId,
    project_id: &ProjectId,
    tag_name: &str,
    value: Option<&str>,
) -> Result<TagWithDefinition, AuthZError> {
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
) -> Result<ListTagsResponse, AuthZError> {
    let target = authorize_generic_table_tag_target::<A, C>(
        authorizer,
        catalog_state.clone(),
        event_ctx,
        generic_table_id,
        project_id,
    )
    .await?;
    list_tags_on_target::<C>(catalog_state, project_id, target).await
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
