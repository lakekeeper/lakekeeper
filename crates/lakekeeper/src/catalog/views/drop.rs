use std::sync::Arc;

use crate::{
    api::{
        iceberg::{types::DropParams, v1::ViewParameters},
        management::v1::{warehouse::TabularDeleteProfile, DeleteKind, TabularType},
        set_not_found_status_code, ApiContext,
    },
    catalog::{require_warehouse_id, tables::validate_table_or_view_ident},
    request_metadata::RequestMetadata,
    service::{
        authz::{Authorizer, CatalogViewAction, CatalogWarehouseAction},
        contract_verification::ContractVerification,
        task_queue::{
            tabular_expiration_queue::TabularExpirationPayload,
            tabular_purge_queue::TabularPurgePayload, EntityId, TaskMetadata,
        },
        Catalog, Result, SecretStore, State, TabularId, Transaction, ViewId,
    },
};

pub(crate) async fn drop_view<C: Catalog, A: Authorizer + Clone, S: SecretStore>(
    parameters: ViewParameters,
    DropParams {
        purge_requested,
        force,
    }: DropParams,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
) -> Result<()> {
    // ------------------- VALIDATIONS -------------------
    let ViewParameters { prefix, view } = &parameters;
    let warehouse_id = require_warehouse_id(prefix.clone())?;
    validate_table_or_view_ident(view)?;

    // ------------------- AUTHZ -------------------
    let authorizer = state.v1_state.authz;
    authorizer
        .require_warehouse_action(
            &request_metadata,
            warehouse_id,
            CatalogWarehouseAction::CanUse,
        )
        .await?;
    let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;
    let view_id = C::view_to_id(warehouse_id, view, t.transaction()).await; // Can't fail before authz

    let view_id: ViewId = authorizer
        .require_view_action(&request_metadata, view_id, CatalogViewAction::CanDrop)
        .await
        .map_err(set_not_found_status_code)?;

    // ------------------- BUSINESS LOGIC -------------------

    let warehouse = C::require_warehouse(warehouse_id, t.transaction()).await?;

    state
        .v1_state
        .contract_verifiers
        .check_drop(TabularId::View(*view_id))
        .await?
        .into_result()?;

    tracing::debug!("Proceeding to delete view");

    match warehouse.tabular_delete_profile {
        TabularDeleteProfile::Hard {} => {
            let location = C::drop_view(view_id, force, t.transaction()).await?;

            if purge_requested {
                C::queue_tabular_purge(
                    TaskMetadata {
                        warehouse_id,
                        entity_id: EntityId::Tabular(*view_id),
                        parent_task_id: None,
                        schedule_for: None,
                    },
                    TabularPurgePayload {
                        tabular_location: location,
                        tabular_type: TabularType::View,
                    },
                    t.transaction(),
                )
                .await?;
                tracing::debug!("Queued purge task for dropped view '{view_id}'.");
            }
            t.commit().await?;

            authorizer
                .delete_view(view_id)
                .await
                .inspect_err(|e| {
                    tracing::error!(?e, "Failed to delete view from authorizer: {}", e.error);
                })
                .ok();
        }
        TabularDeleteProfile::Soft { expiration_seconds } => {
            let _ = C::queue_tabular_expiration(
                TaskMetadata {
                    entity_id: EntityId::Tabular(*view_id),
                    warehouse_id,
                    parent_task_id: None,
                    schedule_for: Some(chrono::Utc::now() + expiration_seconds),
                },
                TabularExpirationPayload {
                    tabular_type: TabularType::View,
                    deletion_kind: if purge_requested {
                        DeleteKind::Purge
                    } else {
                        DeleteKind::Default
                    },
                },
                t.transaction(),
            )
            .await?;
            C::mark_tabular_as_deleted(TabularId::View(*view_id), force, t.transaction()).await?;

            tracing::debug!("Queued expiration task for dropped view '{view_id}'.");
            t.commit().await?;
        }
    }

    state
        .v1_state
        .hooks
        .drop_view(
            warehouse_id,
            parameters,
            DropParams {
                purge_requested,
                force,
            },
            view_id,
            Arc::new(request_metadata),
        )
        .await;

    Ok(())
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use http::StatusCode;
    use iceberg::TableIdent;
    use iceberg_ext::catalog::rest::CreateViewRequest;
    use sqlx::PgPool;

    use crate::{
        api::{
            iceberg::{
                types::{DropParams, Prefix},
                v1::ViewParameters,
            },
            management::v1::{view::ViewManagementService, ApiServer as ManagementApiServer},
        },
        catalog::views::{
            create::test::create_view, drop::drop_view, load::test::load_view, test::setup,
        },
        request_metadata::RequestMetadata,
        tests::random_request_metadata,
        WarehouseId,
    };

    #[sqlx::test]
    async fn test_drop_view(pool: PgPool) {
        let (api_context, namespace, whi) = setup(pool, None).await;

        let view_name = "my-view";
        let rq: CreateViewRequest =
            super::super::create::test::create_view_request(Some(view_name), None);

        let prefix = &whi.to_string();
        let created_view = Box::pin(create_view(
            api_context.clone(),
            namespace.clone(),
            rq,
            Some(prefix.into()),
        ))
        .await
        .unwrap();
        let mut table_ident = namespace.clone().inner();
        table_ident.push(view_name.into());

        let loaded_view = load_view(
            api_context.clone(),
            ViewParameters {
                prefix: Some(Prefix(prefix.to_string())),
                view: TableIdent::from_strs(&table_ident).unwrap(),
            },
        )
        .await
        .expect("View should be loadable");
        assert_eq!(loaded_view.metadata, created_view.metadata);
        drop_view(
            ViewParameters {
                prefix: Some(Prefix(prefix.to_string())),
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

        let error = load_view(
            api_context,
            ViewParameters {
                prefix: Some(Prefix(prefix.to_string())),
                view: TableIdent::from_strs(table_ident).unwrap(),
            },
        )
        .await
        .expect_err("View should no longer exist");

        assert_eq!(error.error.code, StatusCode::NOT_FOUND);
    }

    #[sqlx::test]
    async fn test_cannot_drop_protected_view(pool: PgPool) {
        let (api_context, namespace, whi) = setup(pool, None).await;

        let view_name = "my-view";
        let rq: CreateViewRequest =
            super::super::create::test::create_view_request(Some(view_name), None);

        let prefix = &whi.to_string();
        let created_view = Box::pin(create_view(
            api_context.clone(),
            namespace.clone(),
            rq,
            Some(prefix.into()),
        ))
        .await
        .unwrap();
        let mut table_ident = namespace.clone().inner();
        table_ident.push(view_name.into());

        let loaded_view = load_view(
            api_context.clone(),
            ViewParameters {
                prefix: Some(Prefix(prefix.to_string())),
                view: TableIdent::from_strs(&table_ident).unwrap(),
            },
        )
        .await
        .expect("View should be loadable");
        assert_eq!(loaded_view.metadata, created_view.metadata);

        ManagementApiServer::set_view_protection(
            loaded_view.metadata.uuid().into(),
            WarehouseId::from_str(prefix.as_str()).unwrap(),
            true,
            api_context.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();

        let e = drop_view(
            ViewParameters {
                prefix: Some(Prefix(prefix.to_string())),
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
        .expect_err("Protected View should not be droppable");

        assert_eq!(e.error.code, StatusCode::CONFLICT);

        ManagementApiServer::set_view_protection(
            loaded_view.metadata.uuid().into(),
            WarehouseId::from_str(prefix.as_str()).unwrap(),
            false,
            api_context.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();

        drop_view(
            ViewParameters {
                prefix: Some(Prefix(prefix.to_string())),
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

        let error = load_view(
            api_context,
            ViewParameters {
                prefix: Some(Prefix(prefix.to_string())),
                view: TableIdent::from_strs(table_ident).unwrap(),
            },
        )
        .await
        .expect_err("View should no longer exist");

        assert_eq!(error.error.code, StatusCode::NOT_FOUND);
    }

    #[sqlx::test]
    async fn test_can_force_drop_protected_view(pool: PgPool) {
        let (api_context, namespace, whi) = setup(pool, None).await;

        let view_name = "my-view";
        let rq: CreateViewRequest =
            super::super::create::test::create_view_request(Some(view_name), None);

        let prefix = &whi.to_string();
        let created_view = Box::pin(create_view(
            api_context.clone(),
            namespace.clone(),
            rq,
            Some(prefix.into()),
        ))
        .await
        .unwrap();
        let mut table_ident = namespace.clone().inner();
        table_ident.push(view_name.into());

        let loaded_view = load_view(
            api_context.clone(),
            ViewParameters {
                prefix: Some(Prefix(prefix.to_string())),
                view: TableIdent::from_strs(&table_ident).unwrap(),
            },
        )
        .await
        .expect("View should be loadable");
        assert_eq!(loaded_view.metadata, created_view.metadata);

        ManagementApiServer::set_view_protection(
            loaded_view.metadata.uuid().into(),
            WarehouseId::from_str(prefix.as_str()).unwrap(),
            true,
            api_context.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();

        drop_view(
            ViewParameters {
                prefix: Some(Prefix(prefix.to_string())),
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

        let error = load_view(
            api_context,
            ViewParameters {
                prefix: Some(Prefix(prefix.to_string())),
                view: TableIdent::from_strs(table_ident).unwrap(),
            },
        )
        .await
        .expect_err("View should no longer exist");

        assert_eq!(error.error.code, StatusCode::NOT_FOUND);
    }
}
