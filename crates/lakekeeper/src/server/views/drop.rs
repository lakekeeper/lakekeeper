use std::sync::Arc;

use crate::{
    api::{
        ApiContext,
        iceberg::{types::DropParams, v1::ViewParameters},
        management::v1::{DeleteKind, warehouse::TabularDeleteProfile},
    },
    request_metadata::RequestMetadata,
    server::{require_warehouse_id, tables::validate_table_or_view_ident},
    service::{
        AuthZViewInfo as _, CatalogStore, CatalogTabularOps, NamedEntity, Result, SecretStore,
        State, TabularId, TabularListFlags, Transaction,
        authz::{AuthZViewOps, Authorizer, CatalogViewAction},
        contract_verification::ContractVerification,
        tasks::{
            EntityId, TaskMetadata,
            tabular_expiration_queue::{TabularExpirationPayload, TabularExpirationTask},
            tabular_purge_queue::{TabularPurgePayload, TabularPurgeTask},
        },
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

    let (warehouse, _namespace, view_info) = authorizer
        .load_and_authorize_view_operation::<C>(
            &request_metadata,
            warehouse_id,
            view.clone(),
            TabularListFlags::active(),
            CatalogViewAction::Drop,
            state.v1_state.catalog.clone(),
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
    let project_id = &warehouse.project_id;
    match warehouse.tabular_delete_profile {
        TabularDeleteProfile::Hard {} => {
            let location = C::drop_tabular(warehouse_id, view_id, force, t.transaction()).await?;

            if purge_requested {
                TabularPurgeTask::schedule_task::<C>(
                    TaskMetadata {
                        project_id: project_id.clone(),
                        warehouse_id: warehouse_id.into(),
                        entity_id: EntityId::View(view_id),
                        parent_task_id: None,
                        schedule_for: None,
                        entity_name: Some(view.clone().into_name_parts()),
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
                    project_id: project_id.clone(),
                    entity_id: EntityId::View(view_info.view_id()),
                    warehouse_id: warehouse_id.into(),
                    parent_task_id: None,
                    schedule_for: Some(chrono::Utc::now() + expiration_seconds),
                    entity_name: Some(view.clone().into_name_parts()),
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
        WarehouseId,
        api::{
            iceberg::{
                types::{DropParams, Prefix},
                v1::ViewParameters,
            },
            management::v1::{
                ApiServer as ManagementApiServer,
                tasks::{ListTasksRequest, Service},
                view::ViewManagementService,
            },
        },
        request_metadata::RequestMetadata,
        server::views::{
            create::test::create_view, drop::drop_view, load::test::load_view, test::setup,
        },
        service::tasks::TaskEntity,
        tests::{create_view_request, random_request_metadata},
    };

    #[sqlx::test]
    async fn test_drop_view(pool: PgPool) {
        let (api_context, namespace, whi, project_id) = setup(pool, None).await;

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
            Some(project_id),
            Some(whi),
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
        let (api_context, namespace, whi, _) = setup(pool, None).await;

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
        let (api_context, namespace, whi, _) = setup(pool, None).await;

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
