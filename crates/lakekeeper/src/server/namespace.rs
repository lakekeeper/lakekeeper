use std::{collections::HashMap, ops::Deref, sync::Arc};

use futures::FutureExt;
use http::StatusCode;
use iceberg::NamespaceIdent;
use iceberg_ext::configs::{ConfigProperty as _, namespace::NamespaceProperties};
use itertools::Itertools;
use lakekeeper_io::Location;

mod create;
mod list;

use super::{CatalogServer, UnfilteredPage, require_warehouse_id};
use crate::{
    CONFIG,
    api::{
        iceberg::v1::{
            ApiContext, CreateNamespaceRequest, CreateNamespaceResponse, ErrorModel,
            GetNamespaceResponse, ListNamespacesQuery, ListNamespacesResponse, NamespaceParameters,
            Prefix, Result, UpdateNamespacePropertiesRequest, UpdateNamespacePropertiesResponse,
            namespace::{GetNamespacePropertiesQuery, NamespaceDropFlags},
        },
        management::v1::warehouse::TabularDeleteProfile,
    },
    request_metadata::RequestMetadata,
    server,
    service::{
        CachePolicy, CatalogNamespaceOps, CatalogStore, CatalogTaskOps, NamedEntity, NamespaceId,
        ResolvedWarehouse, State, TabularId, Transaction,
        authz::{
            Authorizer, AuthzNamespaceOps, CatalogNamespaceAction, CatalogWarehouseAction,
            NamespaceParent,
        },
        events::{
            APIEventContext, EventDispatcher, NamespaceOrWarehouseAPIContext,
            context::{
                ResolvedNamespace, Unresolved, UserProvidedNamespace, authz_to_error_no_audit,
            },
        },
        secrets::SecretStore,
        tasks::{
            CancelTasksFilter, ScheduleTaskMetadata, TaskEntity, WarehouseTaskEntityId,
            tabular_purge_queue::{TabularPurgePayload, TabularPurgeTask},
        },
    },
};

pub const UNSUPPORTED_NAMESPACE_PROPERTIES: &[&str] = &[];
// If this is increased, we need to modify namespace creation and deletion
// to take care of the hierarchical structure.
pub const MAX_NAMESPACE_DEPTH: i32 = 5;
pub const NAMESPACE_ID_PROPERTY: &str = "namespace_id";
pub(crate) const MANAGED_ACCESS_PROPERTY: &str = "managed_access";

#[async_trait::async_trait]
impl<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>
    crate::api::iceberg::v1::namespace::NamespaceService<State<A, C, S>>
    for CatalogServer<C, A, S>
{
    #[allow(clippy::too_many_lines)]
    async fn list_namespaces(
        prefix: Option<Prefix>,
        query: ListNamespacesQuery,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<ListNamespacesResponse> {
        // ------------------- VALIDATIONS -------------------
        let warehouse_id = require_warehouse_id(prefix.as_ref())?;
        let ListNamespacesQuery {
            page_token: _,
            page_size: _,
            parent,
            return_uuids,
            return_protection_status,
        } = &query;
        parent.as_ref().map(validate_namespace_ident).transpose()?;
        let return_uuids = *return_uuids;

        // ------------------- AUTHZ -------------------
        let authorizer = state.v1_state.authz;

        let event_ctx = create_namespace_or_warehouse_event_context(
            parent.clone(),
            request_metadata,
            state.v1_state.events,
            warehouse_id,
            CatalogNamespaceAction::ListNamespaces,
            CatalogWarehouseAction::ListNamespaces,
        );

        let authz_result = list::authorize_namespace_list::<C, A>(
            authorizer.clone(),
            event_ctx.request_metadata(),
            warehouse_id,
            parent.as_ref(),
            state.v1_state.catalog.clone(),
        )
        .await;

        let (event_ctx, (can_list_everything, warehouse, _parent_namespace)) =
            event_ctx.emit_authz(authz_result)?;

        // ------------------- BUSINESS LOGIC -------------------
        let mut t = C::Transaction::begin_read(state.v1_state.catalog).await?;
        let (idents, ids, next_page_token) = server::fetch_until_full_page::<_, _, _, C>(
            query.page_size,
            query.page_token.clone(),
            |ps, page_token, trx| {
                let parent = parent.clone();
                let authorizer = authorizer.clone();
                let warehouse = warehouse.clone();
                let request_metadata = event_ctx.request_metadata().clone();
                async move {
                    let request_metadata = &request_metadata;
                    let query = ListNamespacesQuery {
                        page_size: Some(ps),
                        page_token: page_token.into(),
                        parent,
                        return_uuids: true,
                        return_protection_status: true,
                    };

                    // list_namespaces gives us a HashMap<Id, Ident> and a Vec<(Id, Token)>, in order
                    // to do sane pagination, we need to rely on the order of the Vec<(Id, Token)> to
                    // return the correct next page token which is why we do these unholy things here.
                    let list_namespaces =
                        C::list_namespaces(warehouse_id, &query, trx.transaction()).await?;
                    let parent_namespaces = list_namespaces.parent_namespaces;
                    let (ids, responses, tokens): (Vec<_>, Vec<_>, Vec<_>) = list_namespaces
                        .namespaces
                        .into_iter_with_page_tokens()
                        .multiunzip();

                    let masks = if can_list_everything {
                        // No need to check individual permissions if everything in namespace can
                        // be listed.
                        vec![true; ids.len()]
                    } else {
                        authorizer
                            .are_allowed_namespace_actions_vec(
                                request_metadata,
                                None,
                                &warehouse,
                                &parent_namespaces,
                                &responses
                                    .iter()
                                    .map(|id| (id, CatalogNamespaceAction::IncludeInList))
                                    .collect::<Vec<_>>(),
                            )
                            .await
                            .map_err(authz_to_error_no_audit)?
                            .into_inner()
                    };

                    let (next_namespaces, next_ids, next_page_tokens, mask): (
                        Vec<_>,
                        Vec<_>,
                        Vec<_>,
                        Vec<bool>,
                    ) = masks
                        .into_iter()
                        .zip(responses.into_iter())
                        .zip(tokens.into_iter())
                        .map(|((allowed, namespace), token)| {
                            let namespace_id = namespace.namespace_id();
                            (namespace, namespace_id, token, allowed)
                        })
                        .multiunzip();

                    Ok(UnfilteredPage::new(
                        next_namespaces,
                        next_ids,
                        next_page_tokens,
                        mask,
                        ps.clamp(0, i64::MAX).try_into().expect("We clamped it"),
                    ))
                }
                .boxed()
            },
            &mut t,
        )
        .await?;
        t.commit().await?;
        let (namespaces, protection): (Vec<_>, Vec<_>) = idents
            .into_iter()
            .map(|n| (n.namespace_ident().clone(), n.is_protected()))
            .unzip();
        let namespaces = Arc::new(namespaces);

        Ok(ListNamespacesResponse {
            next_page_token,
            namespaces,
            protection_status: return_protection_status.then_some(protection),
            namespace_uuids: return_uuids.then_some(ids.into_iter().map(|s| *s).collect()),
        })
    }

    async fn create_namespace(
        prefix: Option<Prefix>,
        request: CreateNamespaceRequest,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<CreateNamespaceResponse> {
        // ------------------- VALIDATIONS -------------------
        let warehouse_id = require_warehouse_id(prefix.as_ref())?;
        let CreateNamespaceRequest {
            namespace,
            properties,
        } = &request;
        validate_namespace_ident_creation(namespace)?;
        properties
            .as_ref()
            .map(|p| validate_namespace_properties_keys(p.keys()))
            .transpose()?;

        if CONFIG
            .reserved_namespaces
            .contains(&namespace.as_ref()[0].to_lowercase())
        {
            tracing::debug!("Denying reserved namespace: '{}'", &namespace.as_ref()[0]);
            return Err(ErrorModel::bad_request(
                "Namespace is reserved for internal use.",
                "ReservedNamespace",
                None,
            )
            .into());
        }

        // ------------------- AUTHZ -------------------
        let authorizer = state.v1_state.authz;

        let properties_btree: Arc<std::collections::BTreeMap<String, String>> =
            Arc::new(properties.clone().unwrap_or_default().into_iter().collect());

        let event_ctx = create_namespace_or_warehouse_event_context(
            namespace.parent().clone(),
            request_metadata,
            state.v1_state.events,
            warehouse_id,
            CatalogNamespaceAction::CreateNamespace {
                properties: properties_btree.clone(),
            },
            CatalogWarehouseAction::CreateNamespace {
                properties: properties_btree.clone(),
            },
        );

        let authz_result = create::authorize_namespace_create::<C, _>(
            &authorizer,
            event_ctx.request_metadata(),
            warehouse_id,
            namespace.parent().as_ref(),
            state.v1_state.catalog.clone(),
            properties_btree,
        )
        .await;

        let (event_ctx, (warehouse, parent_namespace)) = event_ctx.emit_authz(authz_result)?;

        let event_ctx = match (&parent_namespace, event_ctx) {
            (Some(parent_namespace), NamespaceOrWarehouseAPIContext::Namespace(ctx)) => NamespaceOrWarehouseAPIContext::Namespace(ctx
                .resolve(ResolvedNamespace {
                    warehouse: warehouse.clone(),
                    namespace: parent_namespace.namespace.clone(),
                })),
            (None, NamespaceOrWarehouseAPIContext::Warehouse(ctx)) => {
                 NamespaceOrWarehouseAPIContext::Warehouse(ctx.resolve(warehouse.clone()))
            }
            _ => return Err(ErrorModel::internal("Inconsistent authorization context after namespace creation authorization. Please report this to the developers.".to_string(), "InconsistentAuthZContext", None).into()),
        };

        // ------------------- BUSINESS LOGIC -------------------
        let namespace_id = NamespaceId::new_random();

        let mut namespace_props = NamespaceProperties::try_from_maybe_props(properties.clone())
            .map_err(|e| ErrorModel::bad_request(e.to_string(), e.err_type(), None))?;
        // Set location if not specified - validate location if specified
        set_namespace_location_property(&mut namespace_props, &warehouse, namespace_id)?;
        remove_managed_namespace_properties(&mut namespace_props);

        let mut request = request;
        request.properties = Some(namespace_props.into());

        let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;
        let r = C::create_namespace(warehouse_id, namespace_id, request, t.transaction()).await?;
        let authz_parent = if let Some(parent_namespace) = &parent_namespace {
            NamespaceParent::Namespace(parent_namespace.namespace_id())
        } else {
            NamespaceParent::Warehouse(warehouse_id)
        };
        authorizer
            .create_namespace(event_ctx.request_metadata(), namespace_id, authz_parent)
            .await?;
        t.commit().await?;

        event_ctx.emit_namespace_created_async(r.clone());

        let r_namespace = r.namespace.clone();
        let mut properties = r_namespace.properties.clone().unwrap_or_default();
        properties.insert(NAMESPACE_ID_PROPERTY.to_string(), namespace_id.to_string());
        Ok(CreateNamespaceResponse {
            namespace: r_namespace.namespace_ident.clone(),
            properties: Some(properties),
        })
    }

    /// Return all stored metadata properties for a given namespace
    async fn load_namespace_metadata(
        parameters: NamespaceParameters,
        query: GetNamespacePropertiesQuery,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<GetNamespaceResponse> {
        let GetNamespacePropertiesQuery { return_uuid } = query;
        // ------------------- VALIDATIONS -------------------
        let warehouse_id = require_warehouse_id(parameters.prefix.as_ref())?;
        validate_namespace_ident(&parameters.namespace)?;

        // ------------------- AUTHZ -------------------
        let authorizer = state.v1_state.authz;

        let event_ctx = APIEventContext::for_namespace(
            Arc::new(request_metadata),
            state.v1_state.events,
            warehouse_id,
            parameters.namespace.clone(),
            CatalogNamespaceAction::GetMetadata,
        );

        let authz_result = authorizer
            .load_and_authorize_namespace_action::<C>(
                event_ctx.request_metadata(),
                UserProvidedNamespace {
                    warehouse_id,
                    namespace: parameters.namespace.into(),
                },
                event_ctx.action().clone(),
                CachePolicy::Skip,
                state.v1_state.catalog,
            )
            .await;

        let (event_ctx, (warehouse, namespace)) = event_ctx.emit_authz(authz_result)?;

        let event_ctx = event_ctx.resolve(ResolvedNamespace {
            warehouse,
            namespace: namespace.namespace,
        });
        let namespace = &event_ctx.resolved().namespace;
        let namespace_ident = namespace.namespace_ident().clone();

        // ------------------- BUSINESS LOGIC -------------------
        let namespace_id = namespace.namespace_id();
        let mut properties = namespace.properties().cloned().unwrap_or_default();
        properties.insert(NAMESPACE_ID_PROPERTY.to_string(), namespace_id.to_string());
        let properties = Arc::new(properties);

        event_ctx.emit_namespace_metadata_loaded_async(properties.clone());

        Ok(GetNamespaceResponse {
            properties: Some(properties),
            namespace: namespace_ident,
            namespace_uuid: return_uuid.then_some(*namespace_id),
        })
    }

    /// Check if a namespace exists
    async fn namespace_exists(
        parameters: NamespaceParameters,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        let warehouse_id = require_warehouse_id(parameters.prefix.as_ref())?;

        let event_ctx = APIEventContext::for_namespace(
            Arc::new(request_metadata),
            state.v1_state.events,
            warehouse_id,
            parameters.namespace.clone(),
            CatalogNamespaceAction::GetMetadata,
        );

        let authorizer = state.v1_state.authz;
        let authz_result = authorizer
            .load_and_authorize_namespace_action::<C>(
                event_ctx.request_metadata(),
                UserProvidedNamespace {
                    warehouse_id,
                    namespace: parameters.namespace.into(),
                },
                event_ctx.action().clone(),
                CachePolicy::Skip,
                state.v1_state.catalog,
            )
            .await;

        event_ctx.emit_authz(authz_result)?;

        Ok(())
    }

    /// Drop a namespace from the catalog. Namespace must be empty.
    async fn drop_namespace(
        parameters: NamespaceParameters,
        flags: NamespaceDropFlags,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        //  ------------------- VALIDATIONS -------------------
        let warehouse_id = require_warehouse_id(parameters.prefix.as_ref())?;
        validate_namespace_ident(&parameters.namespace)?;

        if CONFIG
            .reserved_namespaces
            .contains(&parameters.namespace.as_ref()[0].to_lowercase())
        {
            return Err(ErrorModel::bad_request(
                "Cannot drop namespace which is reserved for internal use.",
                "ReservedNamespace",
                None,
            )
            .into());
        }

        //  ------------------- AUTHZ -------------------
        let authorizer = state.v1_state.authz;

        let event_ctx = APIEventContext::for_namespace(
            Arc::new(request_metadata.clone()),
            state.v1_state.events,
            warehouse_id,
            parameters.namespace.clone(),
            CatalogNamespaceAction::Delete,
        );

        let authz_result = authorizer
            .load_and_authorize_namespace_action::<C>(
                event_ctx.request_metadata(),
                UserProvidedNamespace {
                    warehouse_id,
                    namespace: parameters.namespace.into(),
                },
                event_ctx.action().clone(),
                CachePolicy::Skip,
                state.v1_state.catalog.clone(),
            )
            .await;
        let (event_ctx, (warehouse, namespace)) = event_ctx.emit_authz(authz_result)?;

        let event_ctx = event_ctx.resolve(ResolvedNamespace {
            warehouse: warehouse.clone(),
            namespace: namespace.namespace.clone(),
        });

        //  ------------------- BUSINESS LOGIC -------------------
        let namespace_id = namespace.namespace_id();
        let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;
        let r = if flags.recursive {
            try_recursive_drop::<_, C>(
                flags,
                authorizer,
                &warehouse,
                t,
                namespace_id,
                &request_metadata,
            )
            .await
        } else {
            C::drop_namespace(warehouse_id, namespace_id, flags, t.transaction()).await?;
            authorizer
                .delete_namespace(&request_metadata, namespace_id)
                .await?;
            t.commit().await?;
            Ok(())
        };

        match r {
            Ok(()) => {
                event_ctx.emit_namespace_dropped_async();
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    /// Set or remove properties on a namespace
    async fn update_namespace_properties(
        parameters: NamespaceParameters,
        request: UpdateNamespacePropertiesRequest,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<UpdateNamespacePropertiesResponse> {
        //  ------------------- VALIDATIONS -------------------
        let warehouse_id = require_warehouse_id(parameters.prefix.as_ref())?;
        validate_namespace_ident(&parameters.namespace)?;
        let UpdateNamespacePropertiesRequest { removals, updates } = request;
        updates
            .as_ref()
            .map(|p| validate_namespace_properties_keys(p.keys()))
            .transpose()?;
        removals
            .as_ref()
            .map(validate_namespace_properties_keys)
            .transpose()?;

        namespace_location_may_not_change(updates.as_ref(), removals.as_ref())?;
        let mut updates = NamespaceProperties::try_from_maybe_props(updates.clone())
            .map_err(|e| ErrorModel::bad_request(e.to_string(), e.err_type(), None))?;
        remove_managed_namespace_properties(&mut updates);
        //  ------------------- AUTHZ -------------------
        let authorizer = state.v1_state.authz;

        let action = CatalogNamespaceAction::UpdateProperties {
            updated_properties: Arc::new(updates.clone().into_iter().collect()),
            removed_properties: Arc::new(removals.clone().unwrap_or_default()),
        };
        let event_ctx = APIEventContext::for_namespace(
            Arc::new(request_metadata),
            state.v1_state.events,
            warehouse_id,
            parameters.namespace.clone(),
            action,
        );

        let authz_result = authorizer
            .load_and_authorize_namespace_action::<C>(
                event_ctx.request_metadata(),
                UserProvidedNamespace {
                    warehouse_id,
                    namespace: parameters.namespace.into(),
                },
                event_ctx.action().clone(),
                CachePolicy::Skip,
                state.v1_state.catalog.clone(),
            )
            .await;

        let (event_ctx, (warehouse, namespace)) = event_ctx.emit_authz(authz_result)?;
        let event_ctx = event_ctx.resolve(ResolvedNamespace {
            warehouse,
            namespace: namespace.namespace,
        });

        //  ------------------- BUSINESS LOGIC -------------------
        let namespace = &event_ctx.resolved().namespace;
        let namespace_id = namespace.namespace_id();

        let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;
        let (updated_properties, r) =
            update_namespace_properties(namespace.properties().cloned(), updates, removals);
        let updated_namespace = C::update_namespace_properties(
            warehouse_id,
            namespace_id,
            updated_properties,
            t.transaction(),
        )
        .await?;
        t.commit().await?;

        event_ctx.emit_namespace_properties_updated_async(updated_namespace, Arc::new(r.clone()));

        Ok(r)
    }
}

#[allow(clippy::too_many_lines)]
async fn try_recursive_drop<A: Authorizer, C: CatalogStore>(
    flags: NamespaceDropFlags,
    authorizer: A,
    warehouse: &ResolvedWarehouse,
    mut t: <C as CatalogStore>::Transaction,
    namespace_id: NamespaceId,
    request_metadata: &RequestMetadata,
) -> Result<()> {
    if matches!(
        warehouse.tabular_delete_profile,
        TabularDeleteProfile::Hard {}
    ) || (flags.force
        && matches!(
            warehouse.tabular_delete_profile,
            TabularDeleteProfile::Soft { .. }
        ))
    {
        let drop_info =
            C::drop_namespace(warehouse.warehouse_id, namespace_id, flags, t.transaction()).await?;

        C::cancel_scheduled_tasks(
            None,
            CancelTasksFilter::TaskIds(drop_info.open_tasks),
            false,
            t.transaction(),
        )
        .await?;
        let project_id = &warehouse.project_id;

        if flags.purge {
            for (tabular_id, tabular_location, tabular_ident) in &drop_info.child_tables {
                TabularPurgeTask::schedule_task::<C>(
                    ScheduleTaskMetadata {
                        project_id: project_id.clone(),
                        parent_task_id: None,
                        scheduled_for: None,
                        entity: TaskEntity::EntityInWarehouse {
                            entity_name: tabular_ident.clone().into_name_parts(),
                            warehouse_id: warehouse.warehouse_id,
                            entity_id: WarehouseTaskEntityId::from(*tabular_id),
                        },
                    },
                    TabularPurgePayload {
                        tabular_location: tabular_location.to_string(),
                    },
                    t.transaction(),
                )
                .await?;
            }
        }
        // commit before starting the purge tasks so that we cannot end in the situation where
        // data is deleted but the transaction is not committed, meaning dangling pointers.
        t.commit().await?;

        // namespace is gone from catalog, we should not return an error to the client if we fail to
        // delete it from the authorizer.
        authorizer
            .delete_namespace(request_metadata, namespace_id)
            .await
            .inspect_err(|err| {
                tracing::error!("Failed to delete namespace from authorizer: {}", err.error);
            })
            .ok();

        // Delete child tables from authorizer as well.
        // We do not fail the entire operation if this fails, as the namespace and tables are
        // already gone from the catalog.
        for (tabular_id, _tabular_location, tabular_ident) in drop_info.child_tables {
            match tabular_id {
                TabularId::Table(table_id) => {
                    authorizer
                        .delete_table(warehouse.warehouse_id, table_id)
                        .await
                        .inspect_err(|err| {
                            tracing::error!(
                                "Failed to delete table '{tabular_ident}' with id '{table_id}' from authorizer after recursive namespace drop: {}",
                                err.error
                            );
                        })
                        .ok();
                }
                TabularId::View(view_id) => {
                    authorizer
                        .delete_view(warehouse.warehouse_id, view_id)
                        .await
                        .inspect_err(|err| {
                            tracing::error!(
                                "Failed to delete view '{tabular_ident}' with id '{view_id}' from authorizer after recursive namespace drop: {}",
                                err.error
                            );
                        })
                        .ok();
                }
            }
        }

        // Drop child namespaces from authorizer as well.
        // We do not fail the entire operation if this fails, as the namespace and tables are
        // already gone from the catalog.
        for child_namespace_id in drop_info.child_namespaces {
            authorizer
                .delete_namespace(request_metadata, child_namespace_id)
                .await
                .inspect_err(|err| {
                    tracing::error!(
                        "Failed to delete child namespace with id '{child_namespace_id}' from authorizer after recursive namespace drop: {}",
                        err.error
                    );
                })
                .ok();
        }

        Ok(())
    } else {
        Err(ErrorModel::bad_request(
            "Cannot recursively delete namespace with soft-deletion without force flag",
            "NamespaceDeleteNotAllowed",
            None,
        )
        .into())
    }
}

pub(crate) fn uppercase_first_letter(s: &str) -> String {
    let mut c = s.chars();
    match c.next() {
        None => String::new(),
        Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
    }
}

pub(crate) fn validate_namespace_properties_keys<'a, I>(properties: I) -> Result<()>
where
    I: IntoIterator<Item = &'a String>,
{
    for prop in properties {
        if UNSUPPORTED_NAMESPACE_PROPERTIES.contains(&prop.as_str()) {
            return Err(ErrorModel::builder()
                .code(StatusCode::BAD_REQUEST.into())
                .message(format!(
                    "Specifying the '{prop}' property for Namespaces is not supported. '{prop}' is managed by the catalog.",
                ))
                .r#type(format!("{}PropertyNotSupported", uppercase_first_letter(prop)))
                .build()
                .into());
        } else if prop != &prop.to_lowercase() {
            return Err(ErrorModel::builder()
                .code(StatusCode::BAD_REQUEST.into())
                .message(format!("The property '{prop}' is not all lowercase."))
                .r#type(format!("{}NotLowercase", uppercase_first_letter(prop)))
                .build()
                .into());
        }
    }
    Ok(())
}

pub(crate) fn validate_namespace_ident(namespace: &NamespaceIdent) -> Result<()> {
    if namespace.len() > MAX_NAMESPACE_DEPTH as usize {
        return Err(ErrorModel::bad_request(
            format!("Namespace exceeds maximum depth of {MAX_NAMESPACE_DEPTH}",),
            "NamespaceDepthExceeded".to_string(),
            None,
        )
        .into());
    }

    if namespace.deref().iter().any(|s| s.contains('.')) {
        return Err(ErrorModel::bad_request(
            "Namespace parts cannot contain '.'".to_string(),
            "NamespacePartContainsDot".to_string(),
            None,
        )
        .append_detail(format!("Namespace: {namespace:?}"))
        .into());
    }

    if namespace.iter().any(String::is_empty) {
        return Err(ErrorModel::bad_request(
            "Namespace parts cannot be empty".to_string(),
            "NamespacePartEmpty".to_string(),
            None,
        )
        .append_detail(format!("Namespace: {namespace:?}"))
        .into());
    }

    Ok(())
}

pub(crate) fn validate_namespace_ident_creation(namespace: &NamespaceIdent) -> Result<()> {
    validate_namespace_ident(namespace)?;

    // Deny a "+" in in namespace, since some clients (spark, trino) encode space as "+" in URLs and supporting
    // space is more important. Other clients properly encode space as "%20".
    if namespace.as_ref().iter().any(|part| part.contains('+')) {
        return Err(ErrorModel::bad_request(
            "Namespace cannot contain '+' character.",
            "InvalidNamespace",
            None,
        )
        .into());
    }

    Ok(())
}

fn remove_managed_namespace_properties(namespace_props: &mut NamespaceProperties) {
    namespace_props.remove_untyped(NAMESPACE_ID_PROPERTY);
    namespace_props.remove_untyped(MANAGED_ACCESS_PROPERTY);
}

fn set_namespace_location_property(
    namespace_props: &mut NamespaceProperties,
    warehouse: &ResolvedWarehouse,
    namespace_id: NamespaceId,
) -> Result<()> {
    let mut location = namespace_props.get_location();

    // NS locations should always have a trailing slash
    location.as_mut().map(Location::with_trailing_slash);

    // For customer specified location, we need to check if we can write to the location.
    // If no location is specified, we use our default location.
    let location = if let Some(location) = location {
        warehouse
            .storage_profile
            .require_allowed_location(&location)?;
        location
    } else {
        warehouse
            .storage_profile
            .default_namespace_location(namespace_id)?
    };

    namespace_props.insert(&location);
    Ok(())
}

fn update_namespace_properties(
    previous_properties: Option<HashMap<String, String>>,
    updates: NamespaceProperties,
    removals: Option<Vec<String>>,
) -> (HashMap<String, String>, UpdateNamespacePropertiesResponse) {
    let mut properties = previous_properties.unwrap_or_default();

    let mut changes_updated = vec![];
    let mut changes_removed = vec![];
    let mut changes_missing = vec![];

    for key in removals.unwrap_or_default() {
        if properties.remove(&key).is_some() {
            changes_removed.push(key.clone());
        } else {
            changes_missing.push(key.clone());
        }
    }

    for (key, value) in updates {
        // Push to updated if the value for the key is different.
        // Also push on insert

        if properties.insert(key.clone(), value.clone()) != Some(value) {
            changes_updated.push(key);
        }
    }

    // Remove managed property namespace_id
    properties.remove(NAMESPACE_ID_PROPERTY);

    (
        properties,
        UpdateNamespacePropertiesResponse {
            updated: changes_updated,
            removed: changes_removed,
            missing: if changes_missing.is_empty() {
                None
            } else {
                Some(changes_missing)
            },
        },
    )
}

fn namespace_location_may_not_change(
    updates: Option<&HashMap<String, String>>,
    removals: Option<&Vec<String>>,
) -> Result<()> {
    if removals
        .as_ref()
        .is_some_and(|r| r.contains(&Location::KEY.to_string()))
    {
        return Err(ErrorModel::bad_request(
            "Namespace property `location` cannot be removed.",
            "LocationCannotBeRemoved",
            None,
        )
        .into());
    }

    if let Some(location) = updates.as_ref().and_then(|u| u.get(Location::KEY)) {
        return Err(ErrorModel::bad_request(
            "Namespace property `location` cannot be updated.",
            "LocationCannotBeUpdated",
            None,
        )
        .append_detail(format!("Location: {location:?}"))
        .into());
    }

    Ok(())
}

/// Helper function to create event context for either namespace or warehouse actions
fn create_namespace_or_warehouse_event_context(
    namespace: Option<NamespaceIdent>,
    request_metadata: RequestMetadata,
    events: EventDispatcher,
    warehouse_id: crate::service::WarehouseId,
    namespace_action: CatalogNamespaceAction,
    warehouse_action: CatalogWarehouseAction,
) -> NamespaceOrWarehouseAPIContext<Unresolved, Unresolved> {
    match namespace {
        Some(parent_ident) => APIEventContext::for_namespace(
            Arc::new(request_metadata),
            events,
            warehouse_id,
            parent_ident,
            namespace_action,
        )
        .into(),
        None => APIEventContext::for_warehouse(
            Arc::new(request_metadata),
            events,
            warehouse_id,
            warehouse_action,
        )
        .into(),
    }
}

#[cfg(test)]
mod tests {

    use std::{collections::HashSet, hash::RandomState};

    use iceberg::NamespaceIdent;
    use iceberg_ext::catalog::rest::CreateNamespaceRequest;
    use sqlx::PgPool;

    use crate::{
        api::{
            ApiContext,
            iceberg::{
                types::{PageToken, Prefix},
                v1::{
                    NamespaceParameters,
                    namespace::{NamespaceDropFlags, NamespaceService},
                },
            },
            management::v1::{
                ApiServer as ManagementApiServer, namespace::NamespaceManagementService,
                warehouse::TabularDeleteProfile,
            },
        },
        implementations::postgres::{PostgresBackend, SecretsState},
        request_metadata::RequestMetadata,
        server::{CatalogServer, NAMESPACE_ID_PROPERTY, test::impl_pagination_tests},
        service::{
            ListNamespacesQuery, NamespaceId, State, UserId,
            authz::{AllowAllAuthorizer, tests::HidingAuthorizer},
        },
    };

    async fn ns_paginate_test_setup(
        pool: PgPool,
        number_of_namespaces: usize,
        hide_ranges: &[(usize, usize)],
    ) -> (
        ApiContext<State<HidingAuthorizer, PostgresBackend, SecretsState>>,
        Option<Prefix>,
    ) {
        let prof = crate::server::test::memory_io_profile();

        let authz = HidingAuthorizer::new();
        // Prevent hidden namespaces from becoming visible through `can_list_everything`.
        authz.block_can_list_everything();

        let (ctx, warehouse) = crate::server::test::setup(
            pool.clone(),
            prof,
            None,
            authz.clone(),
            TabularDeleteProfile::Hard {},
            Some(UserId::new_unchecked("oidc", "test-user-id")),
        )
        .await;

        for n in 0..number_of_namespaces {
            let ns = format!("{n}");
            let ns = CatalogServer::create_namespace(
                Some(Prefix(warehouse.warehouse_id.to_string())),
                CreateNamespaceRequest {
                    namespace: NamespaceIdent::new(ns),
                    properties: None,
                },
                ctx.clone(),
                RequestMetadata::new_unauthenticated(),
            )
            .await
            .unwrap();
            for (range_start, range_end) in hide_ranges {
                if n >= *range_start && n < *range_end {
                    authz.hide(&format!(
                        "namespace:{}",
                        ns.properties
                            .as_ref()
                            .unwrap()
                            .get(NAMESPACE_ID_PROPERTY)
                            .unwrap()
                    ));
                }
            }
        }
        (ctx, Some(Prefix(warehouse.warehouse_id.to_string())))
    }

    impl_pagination_tests!(
        namespace,
        ns_paginate_test_setup,
        CatalogServer,
        ListNamespacesQuery,
        namespaces,
        |ns| ns.inner()[0].clone()
    );

    #[sqlx::test]
    async fn cannot_drop_protected_namespace(pool: sqlx::PgPool) {
        let prof = crate::server::test::memory_io_profile();
        let (ctx, warehouse) = crate::server::test::setup(
            pool.clone(),
            prof,
            None,
            AllowAllAuthorizer::default(),
            TabularDeleteProfile::Hard {},
            Some(UserId::new_unchecked("oidc", "test-user-id")),
        )
        .await;
        let ns = CatalogServer::create_namespace(
            Some(Prefix(warehouse.warehouse_id.to_string())),
            CreateNamespaceRequest {
                namespace: NamespaceIdent::new("ns".to_string()),
                properties: None,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();
        let ns_id = NamespaceId::from(
            *CatalogServer::list_namespaces(
                Some(Prefix(warehouse.warehouse_id.to_string())),
                ListNamespacesQuery {
                    page_token: PageToken::NotSpecified,
                    page_size: Some(1),
                    parent: None,
                    return_uuids: true,
                    return_protection_status: true,
                },
                ctx.clone(),
                RequestMetadata::new_unauthenticated(),
            )
            .await
            .unwrap()
            .namespace_uuids
            .unwrap()
            .first()
            .unwrap(),
        );
        ManagementApiServer::set_namespace_protection(
            ns_id,
            warehouse.warehouse_id,
            true,
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        let e = CatalogServer::drop_namespace(
            NamespaceParameters {
                prefix: Some(Prefix(warehouse.warehouse_id.to_string())),
                namespace: ns.namespace.clone(),
            },
            NamespaceDropFlags {
                recursive: false,
                force: false,
                purge: false,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap_err();

        assert_eq!(e.error.code, http::StatusCode::CONFLICT);

        ManagementApiServer::set_namespace_protection(
            ns_id,
            warehouse.warehouse_id,
            false,
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        CatalogServer::drop_namespace(
            NamespaceParameters {
                prefix: Some(Prefix(warehouse.warehouse_id.to_string())),
                namespace: ns.namespace.clone(),
            },
            NamespaceDropFlags {
                recursive: false,
                force: false,
                purge: false,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();
    }

    #[sqlx::test]
    async fn test_list_namespaces(pool: PgPool) {
        let prof = crate::server::test::memory_io_profile();

        let authz = HidingAuthorizer::new();

        let (ctx, warehouse) = crate::server::test::setup(
            pool.clone(),
            prof,
            None,
            authz.clone(),
            TabularDeleteProfile::Hard {},
            Some(UserId::new_unchecked("oidc", "test-user-id")),
        )
        .await;

        // Create parent namespace.
        let parent_ns_name = "parent-ns".to_string();
        let _ = CatalogServer::create_namespace(
            Some(Prefix(warehouse.warehouse_id.to_string())),
            CreateNamespaceRequest {
                namespace: NamespaceIdent::new(parent_ns_name.clone()),
                properties: None,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        // Create child namespaces.
        for n in 0..10 {
            let namespace =
                NamespaceIdent::from_vec(vec![parent_ns_name.clone(), format!("ns-{n}")]).unwrap();
            let _ = CatalogServer::create_namespace(
                Some(Prefix(warehouse.warehouse_id.to_string())),
                CreateNamespaceRequest {
                    namespace,
                    properties: None,
                },
                ctx.clone(),
                RequestMetadata::new_unauthenticated(),
            )
            .await
            .unwrap();
        }

        // By default `HidingAuthorizer` allows everything, meaning the quick check path in
        // `list_namespaces` will be hit since `can_list_everything: true`.
        let all = CatalogServer::list_namespaces(
            Some(Prefix(warehouse.warehouse_id.to_string())),
            ListNamespacesQuery {
                page_token: PageToken::NotSpecified,
                page_size: Some(11),
                parent: Some(NamespaceIdent::new(parent_ns_name.clone())),
                return_uuids: true,
                return_protection_status: true,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();
        assert_eq!(all.namespaces.len(), 10);

        // Block `can_list_everything` to hit alternative code path.
        ctx.v1_state.authz.block_can_list_everything();
        let all = CatalogServer::list_namespaces(
            Some(Prefix(warehouse.warehouse_id.to_string())),
            ListNamespacesQuery {
                page_token: PageToken::NotSpecified,
                page_size: Some(11),
                parent: Some(NamespaceIdent::new(parent_ns_name)),
                return_uuids: true,
                return_protection_status: true,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();
        assert_eq!(all.namespaces.len(), 10);
    }

    #[sqlx::test]
    async fn test_ns_pagination(pool: sqlx::PgPool) {
        let prof = crate::server::test::memory_io_profile();

        let authz = HidingAuthorizer::new();
        // Prevent hidden namespaces from becoming visible through `can_list_everything`.
        authz.block_can_list_everything();

        let (ctx, warehouse) = crate::server::test::setup(
            pool.clone(),
            prof,
            None,
            authz.clone(),
            TabularDeleteProfile::Hard {},
            Some(UserId::new_unchecked("oidc", "test-user-id")),
        )
        .await;
        for n in 0..10 {
            let ns = format!("ns-{n}");
            let _ = CatalogServer::create_namespace(
                Some(Prefix(warehouse.warehouse_id.to_string())),
                CreateNamespaceRequest {
                    namespace: NamespaceIdent::new(ns),
                    properties: None,
                },
                ctx.clone(),
                RequestMetadata::new_unauthenticated(),
            )
            .await
            .unwrap();
        }

        let all = CatalogServer::list_namespaces(
            Some(Prefix(warehouse.warehouse_id.to_string())),
            ListNamespacesQuery {
                page_token: PageToken::NotSpecified,
                page_size: Some(11),
                parent: None,
                return_uuids: true,
                return_protection_status: true,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();
        assert_eq!(all.namespaces.len(), 10);

        let _ = CatalogServer::list_namespaces(
            Some(Prefix(warehouse.warehouse_id.to_string())),
            ListNamespacesQuery {
                page_token: PageToken::NotSpecified,
                page_size: Some(10),
                parent: None,
                return_uuids: true,
                return_protection_status: true,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();
        assert_eq!(all.namespaces.len(), 10);

        let first_six = CatalogServer::list_namespaces(
            Some(Prefix(warehouse.warehouse_id.to_string())),
            ListNamespacesQuery {
                page_token: PageToken::NotSpecified,
                page_size: Some(6),
                parent: None,
                return_uuids: true,
                return_protection_status: true,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        assert_eq!(first_six.namespaces.len(), 6);
        let first_six_items: HashSet<String, RandomState> = first_six
            .namespaces
            .iter()
            .map(iceberg::NamespaceIdent::to_url_string)
            .collect();
        for i in 0..6 {
            assert!(first_six_items.contains(&format!("ns-{i}")));
        }

        let next_four = CatalogServer::list_namespaces(
            Some(Prefix(warehouse.warehouse_id.to_string())),
            ListNamespacesQuery {
                page_token: PageToken::Present(first_six.next_page_token.unwrap()),
                page_size: Some(6),
                parent: None,
                return_uuids: true,
                return_protection_status: true,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();
        let next_four_items: HashSet<String, RandomState> = next_four
            .namespaces
            .iter()
            .map(iceberg::NamespaceIdent::to_url_string)
            .collect();
        for i in 6..10 {
            assert!(next_four_items.contains(&format!("ns-{i}")));
        }

        let mut ids = all.namespace_uuids.unwrap();
        ids.sort();
        for i in ids.iter().take(6).skip(4) {
            authz.hide(&format!("namespace:{i}"));
        }

        let page = CatalogServer::list_namespaces(
            Some(Prefix(warehouse.warehouse_id.to_string())),
            ListNamespacesQuery {
                page_token: PageToken::NotSpecified,
                page_size: Some(5),
                parent: None,
                return_uuids: true,
                return_protection_status: true,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        assert_eq!(page.namespaces.len(), 5);
        assert!(page.next_page_token.is_some());

        let page_items: HashSet<String, RandomState> = page
            .namespaces
            .iter()
            .map(iceberg::NamespaceIdent::to_url_string)
            .collect();

        for i in 0..5 {
            let ns_id = if i > 3 { i + 2 } else { i };
            assert!(page_items.contains(&format!("ns-{ns_id}")));
        }
        let next_page = CatalogServer::list_namespaces(
            Some(Prefix(warehouse.warehouse_id.to_string())),
            ListNamespacesQuery {
                page_token: PageToken::Present(page.next_page_token.unwrap()),
                page_size: Some(5),
                parent: None,
                return_uuids: true,
                return_protection_status: true,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();
        assert_eq!(next_page.namespaces.len(), 3);

        let next_page_items: HashSet<String, RandomState> = next_page
            .namespaces
            .iter()
            .map(iceberg::NamespaceIdent::to_url_string)
            .collect();

        for i in 7..10 {
            assert!(next_page_items.contains(&format!("ns-{i}")));
        }
    }

    #[test]
    fn test_update_ns_properties() {
        use super::*;
        let previous_properties = HashMap::from_iter(vec![
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
            ("key3".to_string(), "value3".to_string()),
            ("key5".to_string(), "value5".to_string()),
        ]);

        let updates = NamespaceProperties::from_props_unchecked(vec![
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value12".to_string()),
        ]);

        let removals = Some(vec!["key3".to_string(), "key4".to_string()]);

        let (new_props, result) =
            update_namespace_properties(Some(previous_properties), updates, removals);
        assert_eq!(result.updated, vec!["key2".to_string()]);
        assert_eq!(result.removed, vec!["key3".to_string()]);
        assert_eq!(result.missing, Some(vec!["key4".to_string()]));
        assert_eq!(
            new_props,
            HashMap::from_iter(vec![
                ("key1".to_string(), "value1".to_string()),
                ("key2".to_string(), "value12".to_string()),
                ("key5".to_string(), "value5".to_string()),
            ])
        );
    }

    #[test]
    fn test_update_ns_properties_empty_removal() {
        use super::*;
        let previous_properties = HashMap::from_iter(vec![]);
        let updates = NamespaceProperties::from_props_unchecked(vec![]);
        let removals = Some(vec![]);

        let (new_props, result) =
            update_namespace_properties(Some(previous_properties), updates, removals);
        assert!(result.updated.is_empty());
        assert!(result.removed.is_empty());
        assert!(result.missing.is_none());
        assert!(new_props.is_empty());
    }
}
