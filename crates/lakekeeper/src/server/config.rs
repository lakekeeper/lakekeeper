use std::{str::FromStr, sync::Arc};

use super::CatalogServer;
use crate::{
    CONFIG,
    api::{
        iceberg::v1::{
            ApiContext, CatalogConfig, ErrorModel, PageToken, PaginationQuery, Result,
            config::GetConfigQueryParams,
        },
        management::v1::user::{UserLastUpdatedWith, parse_create_user_request},
    },
    config::MaintenanceMode,
    request_metadata::RequestMetadata,
    service::{
        CatalogStore, CatalogWarehouseOps, ProjectId, SecretStore, State, Transaction,
        WarehouseNameNotFound, WarehouseStatus,
        authz::{
            Authorizer, AuthzWarehouseOps, CatalogWarehouseAction, RequireWarehouseActionError,
        },
        events::APIEventContext,
    },
};

#[async_trait::async_trait]
impl<A: Authorizer + Clone, C: CatalogStore, S: SecretStore>
    crate::api::iceberg::v1::config::Service<State<A, C, S>> for CatalogServer<C, A, S>
{
    async fn get_config(
        query: GetConfigQueryParams,
        api_context: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<CatalogConfig> {
        let authorizer = api_context.v1_state.authz;

        // Maintenance mode: the catalog is in a read-only window (typically a
        // Kubernetes operator running a schema migration). Skip the
        // first-touch user-register side-effect so `GET /v1/config` stays a
        // pure read. Returning the catalog config itself is still valuable —
        // existing clients need it to keep reading.
        if matches!(CONFIG.maintenance_mode, MaintenanceMode::Off) {
            maybe_register_user::<C>(&request_metadata, api_context.v1_state.catalog.clone())
                .await?;
        }

        let request_metadata_arc = Arc::new(request_metadata);

        // Arg takes precedence over auth
        let Some(query_warehouse) = query.warehouse else {
            return Err(ErrorModel::bad_request("No warehouse specified. Please specify the 'warehouse' parameter in the GET /config request.".to_string(), "GetConfigNoWarehouseProvided", None).into());
        };
        let (project_from_arg, warehouse_from_arg) = parse_warehouse_arg(&query_warehouse);
        let project_id = request_metadata_arc.require_project_id(project_from_arg)?;

        // Authorize the single warehouse (get-config), not the project: a project-level
        // "list warehouses" check considers every warehouse and times out on large
        // projects (issue #1780). With no project gate, the caller-supplied project_id
        // is unchecked, so a missing name and a warehouse the caller can't see both
        // return the same name-keyed `NoSuchWarehouseException` — otherwise name->id
        // resolution leaks existence/UUID. (Audit log keeps the truth; response timing
        // still differs.)
        let not_found = || ErrorModel::from(WarehouseNameNotFound::new(warehouse_from_arg.clone()));

        let Some(warehouse) = C::get_warehouse_by_name(
            &warehouse_from_arg,
            &project_id,
            WarehouseStatus::active(),
            api_context.v1_state.catalog.clone(),
        )
        .await?
        else {
            return Err(not_found().into());
        };

        let action = CatalogWarehouseAction::GetConfig;
        let event_ctx = APIEventContext::for_warehouse(
            request_metadata_arc.clone(),
            api_context.v1_state.events,
            warehouse.warehouse_id,
            action.clone(),
        );

        let authz_result = authorizer
            .require_warehouse_action(
                &request_metadata_arc,
                warehouse.warehouse_id,
                Ok(Some(warehouse)),
                action,
            )
            .await;
        // Mask "can't see it" as not-found (see above); a forbidden on a warehouse the
        // caller *can* see keeps its native 403, which leaks nothing new.
        let warehouse_hidden = authz_result
            .as_ref()
            .err()
            .is_some_and(RequireWarehouseActionError::is_warehouse_hidden);
        let (_event_ctx, warehouse) = match event_ctx.emit_authz(authz_result) {
            Ok(checked) => checked,
            // emit_authz has already written the full-detail audit event.
            Err(masked) => {
                return Err(if warehouse_hidden {
                    not_found()
                } else {
                    masked
                }
                .into());
            }
        };

        let mut config = warehouse.storage_profile.generate_catalog_config(
            warehouse.warehouse_id,
            &request_metadata_arc,
            warehouse.tabular_delete_profile,
        );

        config.defaults.insert(
            "prefix".to_string(),
            CONFIG.warehouse_prefix(warehouse.warehouse_id),
        );
        config.defaults.insert(
            "rest-page-size".to_string(),
            CONFIG.pagination_size_default.to_string(),
        );

        config
            .overrides
            .insert("uri".to_string(), request_metadata_arc.base_uri_catalog());

        if CONFIG.idempotency.enabled {
            config.overrides.insert(
                "idempotency-key-lifetime".to_string(),
                CONFIG.idempotency.lifetime_iso8601(),
            );
        }

        Ok(config)
    }
}

fn parse_warehouse_arg(arg: &str) -> (Option<ProjectId>, String) {
    // structure of the argument is <(optional uuid project_id)>/<warehouse_name>
    // Warehouse names cannot include /

    // Split arg at first /
    let parts: Vec<&str> = arg.splitn(2, '/').collect();
    match parts.len() {
        1 => {
            // No project_id provided
            let warehouse_name = parts[0].to_string();
            (None, warehouse_name)
        }
        2 => {
            // Maybe project_id and warehouse_id provided
            // If parts[0] is a valid UUID, it is a project_id, otherwise the whole thing is a warehouse_id
            match ProjectId::from_str(parts[0]) {
                Ok(project_id) => {
                    let warehouse_name = parts[1].to_string();
                    (Some(project_id), warehouse_name)
                }
                Err(_) => (None, arg.to_string()),
            }
        }
        // Because of the splitn(2, ..) there can't be more than 2 parts
        _ => unreachable!(),
    }
}

async fn maybe_register_user<D: CatalogStore>(
    request_metadata: &RequestMetadata,
    state: <D as CatalogStore>::State,
) -> Result<()> {
    let Some(user_id) = request_metadata.user_id() else {
        return Ok(());
    };

    // `parse_create_user_request` can fail - we can't run it for already registered users
    let user = D::list_user(
        Some(vec![user_id.clone()]),
        None,
        PaginationQuery {
            page_token: PageToken::Empty,
            page_size: Some(1),
        },
        state.clone(),
    )
    .await?;

    if user.users.is_empty() {
        let (creation_user_id, name, user_type, email) =
            parse_create_user_request(request_metadata, None)?;

        // If the user is authenticated, create a user in the catalog
        let mut t = D::Transaction::begin_write(state).await?;
        D::create_or_update_user(
            &creation_user_id,
            &name,
            email.as_deref(),
            UserLastUpdatedWith::ConfigCallCreation,
            user_type,
            t.transaction(),
        )
        .await?;
        t.commit().await?;
    }

    Ok(())
}
