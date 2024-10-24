use crate::api::iceberg::types::{DropParams, Prefix};
use crate::api::iceberg::v1::ViewParameters;
use crate::api::management::v1::warehouse::TabularDeleteProfile;
use crate::api::management::v1::TabularType;
use crate::api::ApiContext;
use crate::catalog::require_warehouse_id;
use crate::catalog::tables::validate_table_or_view_ident;
use crate::request_metadata::RequestMetadata;
use crate::service::authz::{Authorizer, CatalogViewAction, CatalogWarehouseAction};
use crate::service::contract_verification::ContractVerification;
use crate::service::event_publisher::EventMetadata;
use crate::service::task_queue::tabular_expiration_queue::TabularExpirationInput;
use crate::service::task_queue::tabular_purge_queue::TabularPurgeInput;
use crate::service::TabularIdentUuid;
use crate::service::{Catalog, SecretStore, State, Transaction};
use crate::service::{Result, ViewIdentUuid};
use uuid::Uuid;

pub(crate) async fn drop_view<C: Catalog, A: Authorizer + Clone, S: SecretStore>(
    parameters: ViewParameters,
    DropParams { purge_requested }: DropParams,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
) -> Result<()> {
    // ------------------- VALIDATIONS -------------------
    let ViewParameters { prefix, view } = parameters;
    let warehouse_id = require_warehouse_id(prefix.clone())?;
    validate_table_or_view_ident(&view)?;

    // ------------------- AUTHZ -------------------
    let authorizer = state.v1_state.authz;
    authorizer
        .require_warehouse_action(
            &request_metadata,
            warehouse_id,
            &CatalogWarehouseAction::CanUse,
        )
        .await?;
    let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;
    let view_id = C::view_to_id(warehouse_id, &view, t.transaction()).await; // Can't fail before authz

    let view_id: ViewIdentUuid = authorizer
        .require_view_action(
            &request_metadata,
            warehouse_id,
            view_id,
            &CatalogViewAction::CanDrop,
        )
        .await?;

    // ------------------- BUSINESS LOGIC -------------------
    let purge_requested = purge_requested.unwrap_or(false);

    let warehouse = C::require_warehouse(warehouse_id, t.transaction()).await?;

    state
        .v1_state
        .contract_verifiers
        .check_drop(TabularIdentUuid::View(*view_id))
        .await?
        .into_result()?;

    tracing::debug!("Proceeding to delete view");

    match warehouse.tabular_delete_profile {
        TabularDeleteProfile::Hard {} => {
            let location = C::drop_view(view_id, t.transaction()).await?;
            // committing here means maybe dangling data if the queue fails
            // OTOH committing after queuing means we may end up with a view pointing to deleted files
            // I feel that some undeleted files are less bad than a view that cannot be loaded
            t.commit().await?;

            if purge_requested {
                state
                    .v1_state
                    .queues
                    .queue_tabular_purge(TabularPurgeInput {
                        tabular_location: location,
                        tabular_id: *view_id,
                        warehouse_ident: warehouse_id,
                        tabular_type: TabularType::View,
                        parent_id: None,
                    })
                    .await?;
                tracing::debug!("Queued purge task for dropped view '{view_id}'.");
            }
            authorizer.delete_view(view_id).await?;
        }
        TabularDeleteProfile::Soft { expiration_seconds } => {
            C::mark_tabular_as_deleted(TabularIdentUuid::View(*view_id), t.transaction()).await?;
            t.commit().await?;

            state
                .v1_state
                .queues
                .queue_tabular_expiration(TabularExpirationInput {
                    tabular_id: *view_id,
                    warehouse_ident: warehouse_id,
                    tabular_type: TabularType::View,
                    purge: purge_requested,
                    expire_at: chrono::Utc::now() + expiration_seconds,
                })
                .await?;
            tracing::debug!("Queued expiration task for dropped view '{view_id}'.");
        }
    }

    let _ = state
        .v1_state
        .publisher
        .publish(
            Uuid::now_v7(),
            "dropView",
            serde_json::Value::Null,
            EventMetadata {
                tabular_id: TabularIdentUuid::View(*view_id),
                warehouse_id: *warehouse_id,
                name: view.name.clone(),
                namespace: view.namespace.to_url_string(),
                prefix: prefix.map(Prefix::into_string).unwrap_or_default(),
                num_events: 1,
                sequence_number: 0,
                trace_id: request_metadata.request_id,
            },
        )
        .await;

    Ok(())
}

#[cfg(test)]
mod test {
    use crate::api::iceberg::types::{DropParams, Prefix};
    use crate::api::iceberg::v1::ViewParameters;
    use crate::catalog::views::create::test::create_view;
    use crate::catalog::views::drop::drop_view;
    use crate::catalog::views::load::test::load_view;
    use crate::catalog::views::test::setup;
    use crate::request_metadata::RequestMetadata;
    use http::StatusCode;
    use iceberg::TableIdent;
    use iceberg_ext::catalog::rest::CreateViewRequest;
    use sqlx::PgPool;

    #[sqlx::test]
    async fn test_load_view(pool: PgPool) {
        let (api_context, namespace, whi) = setup(pool, None).await;

        let view_name = "my-view";
        let rq: CreateViewRequest =
            super::super::create::test::create_view_request(Some(view_name), None);

        let prefix = &whi.to_string();
        let created_view = create_view(
            api_context.clone(),
            namespace.clone(),
            rq,
            Some(prefix.into()),
        )
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
                purge_requested: None,
            },
            api_context.clone(),
            RequestMetadata::new_random(),
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
}
