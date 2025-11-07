use std::sync::Arc;

use crate::{
    api::{
        iceberg::{types::DropParams, v1::ViewParameters},
        management::v1::{warehouse::TabularDeleteProfile, DeleteKind},
        ApiContext,
    },
    request_metadata::RequestMetadata,
    server::{require_warehouse_id, tables::validate_table_or_view_ident},
    service::{
        authz::{
            AuthZViewOps, Authorizer, AuthzNamespaceOps, AuthzWarehouseOps, CatalogViewAction,
        },
        contract_verification::ContractVerification,
        tasks::{
            tabular_expiration_queue::{TabularExpirationPayload, TabularExpirationTask},
            tabular_purge_queue::{TabularPurgePayload, TabularPurgeTask},
            EntityId, TaskMetadata,
        },
        AuthZViewInfo as _, CatalogNamespaceOps, CatalogStore, CatalogTabularOps,
        CatalogWarehouseOps, NamedEntity, Result, SecretStore, State, TabularId, TabularListFlags,
        Transaction,
    },
};

#[allow(clippy::too_many_lines)]
pub(crate) async fn drop_view<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
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
    let warehouse_id = require_warehouse_id(prefix.as_ref())?;
    validate_table_or_view_ident(view)?;

    // ------------------- AUTHZ -------------------
    let authorizer = state.v1_state.authz;

    let (warehouse, namespace, view_info) = tokio::join!(
        C::get_active_warehouse_by_id(warehouse_id, state.v1_state.catalog.clone()),
        C::get_namespace(
            warehouse_id,
            view.namespace.clone(),
            state.v1_state.catalog.clone(),
        ),
        C::get_view_info(
            warehouse_id,
            view.clone(),
            TabularListFlags::active(),
            state.v1_state.catalog.clone(),
        )
    );
    let warehouse = authorizer.require_warehouse_presence(warehouse_id, warehouse)?;
    let namespace =
        authorizer.require_namespace_presence(warehouse_id, view.namespace.clone(), namespace)?;

    let view_info = authorizer
        .require_view_action(
            &request_metadata,
            &warehouse,
            &namespace,
            view.clone(),
            view_info,
            CatalogViewAction::CanDrop,
        )
        .await?;
    let view_id = view_info.view_id();

    // ------------------- BUSINESS LOGIC -------------------
    state
        .v1_state
        .contract_verifiers
        .check_drop(TabularId::View(view_id))
        .await?
        .into_result()?;

    let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;
    match warehouse.tabular_delete_profile {
        TabularDeleteProfile::Hard {} => {
            let location = C::drop_tabular(warehouse_id, view_id, force, t.transaction()).await?;

            if purge_requested {
                TabularPurgeTask::schedule_task::<C>(
                    TaskMetadata {
                        warehouse_id,
                        entity_id: EntityId::View(view_id),
                        parent_task_id: None,
                        schedule_for: None,
                        entity_name: view.clone().into_name_parts(),
                    },
                    TabularPurgePayload {
                        tabular_location: location.to_string(),
                    },
                    t.transaction(),
                )
                .await?;
                tracing::debug!(
                    "Queued purge task for dropped view '{}' in warehouse {warehouse_id}.",
                    view_info.view_ident()
                );
            }
            t.commit().await?;

            authorizer
                .delete_view(warehouse_id, view_id)
                .await
                .inspect_err(|e| {
                    tracing::error!(?e, "Failed to delete view from authorizer: {}", e.error);
                })
                .ok();
        }
        TabularDeleteProfile::Soft { expiration_seconds } => {
            let _ = TabularExpirationTask::schedule_task::<C>(
                TaskMetadata {
                    entity_id: EntityId::View(view_info.view_id()),
                    warehouse_id,
                    parent_task_id: None,
                    schedule_for: Some(chrono::Utc::now() + expiration_seconds),
                    entity_name: view.clone().into_name_parts(),
                },
                TabularExpirationPayload {
                    deletion_kind: if purge_requested {
                        DeleteKind::Purge
                    } else {
                        DeleteKind::Default
                    },
                },
                t.transaction(),
            )
            .await?;
            C::mark_tabular_as_deleted(
                warehouse_id,
                TabularId::View(view_info.view_id()),
                force,
                t.transaction(),
            )
            .await?;

            tracing::debug!(
                "Queued expiration task for dropped view '{}' with id '{view_id}' in warehouse {warehouse_id}.",
                view_info.view_ident()
            );
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
            management::v1::{
                tasks::{ListTasksRequest, Service},
                view::ViewManagementService,
                ApiServer as ManagementApiServer,
            },
        },
        request_metadata::RequestMetadata,
        server::views::{
            create::test::create_view, drop::drop_view, load::test::load_view, test::setup,
        },
        service::tasks::TaskEntity,
        tests::{create_view_request, random_request_metadata},
        WarehouseId,
    };

    #[sqlx::test]
    async fn test_drop_view(pool: PgPool) {
        let (api_context, namespace, whi) = setup(pool, None).await;

        let view_name = "my-view";
        let rq: CreateViewRequest = create_view_request(Some(view_name), None);

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
                prefix: Some(Prefix(prefix.clone())),
                view: TableIdent::from_strs(&table_ident).unwrap(),
            },
        )
        .await
        .expect("View should be loadable");
        assert_eq!(loaded_view.metadata, created_view.metadata);
        drop_view(
            ViewParameters {
                prefix: Some(Prefix(prefix.clone())),
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
            api_context.clone(),
            ViewParameters {
                prefix: Some(Prefix(prefix.clone())),
                view: TableIdent::from_strs(table_ident).unwrap(),
            },
        )
        .await
        .expect_err("View should no longer exist");

        assert_eq!(error.error.code, StatusCode::NOT_FOUND);

        // Load expiration task
        let entity = TaskEntity::View {
            view_id: loaded_view.metadata.uuid().into(),
        };
        let expiration_tasks = ManagementApiServer::list_tasks(
            whi,
            ListTasksRequest::builder()
                .entities(Some(vec![TaskEntity::View {
                    view_id: loaded_view.metadata.uuid().into(),
                }]))
                .build(),
            api_context,
            random_request_metadata(),
        )
        .await
        .unwrap();
        assert_eq!(expiration_tasks.tasks.len(), 1);
        let task = &expiration_tasks.tasks[0];
        assert_eq!(task.entity, entity);
    }

    #[sqlx::test]
    async fn test_cannot_drop_protected_view(pool: PgPool) {
        let (api_context, namespace, whi) = setup(pool, None).await;

        let view_name = "my-view";
        let create_view_request = create_view_request(Some(view_name), None);

        let prefix = &whi.to_string();
        let created_view = Box::pin(create_view(
            api_context.clone(),
            namespace.clone(),
            create_view_request,
            Some(prefix.into()),
        ))
        .await
        .unwrap();
        let mut table_ident = namespace.clone().inner();
        table_ident.push(view_name.into());

        let view_ident = TableIdent::new(namespace.clone(), view_name.to_string());
        let loaded_view = load_view(
            api_context.clone(),
            ViewParameters {
                prefix: Some(Prefix(prefix.clone())),
                view: view_ident.clone(),
            },
        )
        .await
        .expect("View should be loadable");
        assert_eq!(loaded_view.metadata, created_view.metadata);

        ManagementApiServer::set_view_protection(
            loaded_view.metadata.uuid().into(),
            whi,
            true,
            api_context.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();

        let e = drop_view(
            ViewParameters {
                prefix: Some(Prefix(prefix.clone())),
                view: view_ident,
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

        assert_eq!(e.error.code, StatusCode::CONFLICT, "{}", e.error);

        ManagementApiServer::set_view_protection(
            loaded_view.metadata.uuid().into(),
            WarehouseId::from_str_or_internal(prefix.as_str()).unwrap(),
            false,
            api_context.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();

        drop_view(
            ViewParameters {
                prefix: Some(Prefix(prefix.clone())),
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
                prefix: Some(Prefix(prefix.clone())),
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
        let rq: CreateViewRequest = create_view_request(Some(view_name), None);

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
                prefix: Some(Prefix(prefix.clone())),
                view: TableIdent::from_strs(&table_ident).unwrap(),
            },
        )
        .await
        .expect("View should be loadable");
        assert_eq!(loaded_view.metadata, created_view.metadata);

        ManagementApiServer::set_view_protection(
            loaded_view.metadata.uuid().into(),
            WarehouseId::from_str_or_internal(prefix.as_str()).unwrap(),
            true,
            api_context.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();

        drop_view(
            ViewParameters {
                prefix: Some(Prefix(prefix.clone())),
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
                prefix: Some(Prefix(prefix.clone())),
                view: TableIdent::from_strs(table_ident).unwrap(),
            },
        )
        .await
        .expect_err("View should no longer exist");

        assert_eq!(error.error.code, StatusCode::NOT_FOUND);
    }
}
