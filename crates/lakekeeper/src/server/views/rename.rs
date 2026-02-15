use std::sync::Arc;

use iceberg::TableIdent;
use iceberg_ext::catalog::rest::RenameTableRequest;

use crate::{
    WarehouseId,
    api::{ApiContext, iceberg::types::Prefix},
    request_metadata::RequestMetadata,
    server::{require_warehouse_id, tables::validate_table_or_view_ident},
    service::{
        AuthZViewInfo as _, CatalogNamespaceOps, CatalogStore, CatalogTabularOps,
        CatalogWarehouseOps, NamespaceHierarchy, ResolvedWarehouse, Result, SecretStore, State,
        TabularId, TabularListFlags, Transaction, ViewInfo,
        authz::{
            AuthZCannotSeeView, AuthZError, AuthZViewOps, Authorizer, AuthzNamespaceOps,
            AuthzWarehouseOps, CatalogNamespaceAction, CatalogViewAction, RequireViewActionError,
            refresh_warehouse_and_namespace_if_needed,
        },
        contract_verification::ContractVerification,
        events::{APIEventContext, context::ResolvedView},
    },
};

pub(crate) async fn rename_view<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    prefix: Option<Prefix>,
    request: RenameTableRequest,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
) -> Result<()> {
    // ------------------- VALIDATIONS -------------------
    let warehouse_id = require_warehouse_id(prefix.as_ref())?;
    let RenameTableRequest {
        source,
        destination,
    } = &request;
    validate_table_or_view_ident(source)?;
    validate_table_or_view_ident(destination)?;

    // ------------------- AUTHZ -------------------
    // Authorization is required for:
    // 1) creating a view in the destination namespace
    // 2) renaming the old view
    let authorizer = state.v1_state.authz;

    let event_ctx = APIEventContext::for_view(
        Arc::new(request_metadata),
        state.v1_state.events,
        warehouse_id,
        source.clone(),
        CatalogViewAction::Rename,
    );

    let authz_result = authorize_rename_view::<C, _>(
        event_ctx.request_metadata(),
        warehouse_id,
        source,
        destination,
        &authorizer,
        state.v1_state.catalog.clone(),
    )
    .await;
    let (
        event_ctx,
        AuthorizeRenameViewResult {
            warehouse,
            source_view_info,
            destination_namespace,
        },
    ) = event_ctx.emit_authz(authz_result)?;

    let source_id = source_view_info.view_id();
    let event_ctx = event_ctx.resolve(ResolvedView {
        warehouse: warehouse.clone(),
        view: Arc::new(source_view_info),
    });

    // ------------------- BUSINESS LOGIC -------------------
    if source == destination {
        return Ok(());
    }

    let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;
    C::rename_tabular(
        warehouse_id,
        source_id,
        source,
        destination,
        t.transaction(),
    )
    .await?;
    state
        .v1_state
        .contract_verifiers
        .check_rename(TabularId::View(source_id), destination)
        .await?
        .into_result()?;

    t.commit().await?;

    event_ctx.emit_view_renamed_async(destination_namespace.namespace, Arc::new(request));

    Ok(())
}

struct AuthorizeRenameViewResult {
    warehouse: Arc<ResolvedWarehouse>,
    source_view_info: ViewInfo,
    destination_namespace: NamespaceHierarchy,
}

async fn authorize_rename_view<C: CatalogStore, A: Authorizer + Clone>(
    request_metadata: &RequestMetadata,
    warehouse_id: WarehouseId,
    source: &TableIdent,
    destination: &TableIdent,
    authorizer: &A,
    state: C::State,
) -> Result<AuthorizeRenameViewResult, AuthZError> {
    let (warehouse, destination_namespace, source_namespace, source_view_info) = tokio::join!(
        C::get_active_warehouse_by_id(warehouse_id, state.clone(),),
        C::get_namespace(warehouse_id, &destination.namespace, state.clone(),),
        C::get_namespace(warehouse_id, &source.namespace, state.clone(),),
        C::get_view_info(
            warehouse_id,
            source.clone(),
            TabularListFlags::active(),
            state.clone(),
        )
    );
    let warehouse = authorizer.require_warehouse_presence(warehouse_id, warehouse)?;
    let source_namespace = authorizer.require_namespace_presence(
        warehouse_id,
        source.namespace.clone(),
        source_namespace,
    )?;
    let source_view_info =
        authorizer.require_view_presence(warehouse_id, source.clone(), source_view_info)?;

    let (warehouse, source_namespace) = refresh_warehouse_and_namespace_if_needed::<C, _, _>(
        &warehouse,
        source_namespace,
        &source_view_info,
        AuthZCannotSeeView::new_not_found(warehouse_id, source.clone()),
        authorizer,
        state.clone(),
    )
    .await?;

    let (destination_namespace, source_view_info) = tokio::join!(
        // Check 1)
        authorizer.require_namespace_action(
            request_metadata,
            &warehouse,
            &destination.namespace,
            destination_namespace,
            CatalogNamespaceAction::CreateView {
                properties: Arc::new(source_view_info.properties().clone().into_iter().collect()),
            },
        ),
        // Check 2)
        authorizer.require_view_action(
            request_metadata,
            &warehouse,
            &source_namespace,
            source.clone(),
            Ok::<_, RequireViewActionError>(Some(source_view_info)),
            CatalogViewAction::Rename,
        )
    );
    let source_view_info = source_view_info?;
    let destination_namespace = destination_namespace?;

    Ok(AuthorizeRenameViewResult {
        warehouse,
        source_view_info,
        destination_namespace,
    })
}

#[cfg(test)]
mod test {
    use http::StatusCode;
    use iceberg::{NamespaceIdent, TableIdent};
    use iceberg_ext::catalog::rest::CreateViewRequest;
    use sqlx::PgPool;

    use super::*;
    use crate::{
        api::iceberg::v1::ViewParameters,
        implementations::postgres::namespace::tests::initialize_namespace,
        server::views::{create::test::create_view, load::test::load_view, test::setup},
        tests::create_view_request,
    };

    #[sqlx::test]
    async fn test_rename_view_without_namespace(pool: PgPool) {
        let (api_context, namespace, whi, _) = setup(pool, None).await;

        let view_name = "my-view";
        let rq: CreateViewRequest = create_view_request(Some(view_name), None);

        let prefix = Prefix(whi.to_string());
        let created_view = Box::pin(create_view(
            api_context.clone(),
            namespace.clone(),
            rq,
            Some(prefix.clone().into_string()),
        ))
        .await
        .unwrap();
        let destination = TableIdent {
            namespace: namespace.clone(),
            name: "my-renamed-view".to_string(),
        };
        let source = TableIdent {
            namespace: namespace.clone(),
            name: view_name.to_string(),
        };
        rename_view(
            Some(prefix.clone()),
            RenameTableRequest {
                source: source.clone(),
                destination: destination.clone(),
            },
            api_context.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        let exists = load_view(
            api_context.clone(),
            ViewParameters {
                view: destination,
                prefix: Some(prefix.clone()),
            },
        )
        .await
        .unwrap();

        let not_exists = load_view(
            api_context.clone(),
            ViewParameters {
                view: source,
                prefix: Some(prefix.clone()),
            },
        )
        .await
        .expect_err("View should not exist after renaming.");

        assert_eq!(created_view, exists);
        assert_eq!(StatusCode::NOT_FOUND, not_exists.error.code);
    }

    #[sqlx::test]
    async fn test_rename_view_with_namespace(pool: PgPool) {
        let (api_context, _, whi, _) = setup(pool, None).await;
        let namespace = NamespaceIdent::from_vec(vec!["Someother-ns".to_string()]).unwrap();
        let new_ns =
            initialize_namespace(api_context.v1_state.catalog.clone(), whi, &namespace, None)
                .await
                .namespace_ident()
                .clone();

        let view_name = "my-view";
        let rq: CreateViewRequest = create_view_request(Some(view_name), None);

        let prefix = Prefix(whi.to_string());
        let created_view = Box::pin(create_view(
            api_context.clone(),
            namespace.clone(),
            rq,
            Some(prefix.clone().into_string()),
        ))
        .await
        .unwrap();
        let destination = TableIdent {
            namespace: new_ns.clone(),
            name: "my-renamed-view".to_string(),
        };
        let source = TableIdent {
            namespace: namespace.clone(),
            name: view_name.to_string(),
        };
        rename_view(
            Some(prefix.clone()),
            RenameTableRequest {
                source: source.clone(),
                destination: destination.clone(),
            },
            api_context.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        let exists = load_view(
            api_context.clone(),
            ViewParameters {
                view: destination,
                prefix: Some(prefix.clone()),
            },
        )
        .await
        .unwrap();

        let not_exists = load_view(
            api_context.clone(),
            ViewParameters {
                view: source,
                prefix: Some(prefix.clone()),
            },
        )
        .await
        .expect_err("View should not exist after renaming.");

        assert_eq!(created_view, exists);
        assert_eq!(StatusCode::NOT_FOUND, not_exists.error.code);
    }
}
