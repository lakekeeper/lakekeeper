use crate::{
    api::{iceberg::v1::ViewParameters, ApiContext},
    request_metadata::RequestMetadata,
    server::{require_warehouse_id, tables::validate_table_or_view_ident},
    service::{
        authz::{
            AuthZViewOps, Authorizer, AuthzNamespaceOps, AuthzWarehouseOps, CatalogViewAction,
        },
        CatalogNamespaceOps, CatalogStore, CatalogTabularOps, CatalogWarehouseOps, Result,
        SecretStore, State,
    },
};

pub(crate) async fn view_exists<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    parameters: ViewParameters,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
) -> Result<()> {
    // ------------------- VALIDATIONS -------------------
    let ViewParameters { prefix, view } = parameters;
    let warehouse_id = require_warehouse_id(prefix.as_ref())?;
    validate_table_or_view_ident(&view)?;

    // ------------------- BUSINESS LOGIC -------------------
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
            crate::service::TabularListFlags::active(),
            state.v1_state.catalog.clone(),
        )
    );
    let warehouse = authorizer.require_warehouse_presence(warehouse_id, warehouse)?;
    let namespace =
        authorizer.require_namespace_presence(warehouse_id, view.namespace.clone(), namespace)?;

    let _view_info = authorizer
        .require_view_action(
            &request_metadata,
            &warehouse,
            &namespace,
            view,
            view_info,
            CatalogViewAction::CanGetMetadata,
        )
        .await?;

    Ok(())
}

#[cfg(test)]
mod test {
    use iceberg::TableIdent;
    use iceberg_ext::catalog::rest::CreateViewRequest;
    use sqlx::PgPool;

    use super::*;
    use crate::{
        api::iceberg::{types::Prefix, v1::ViewParameters},
        server::views::{create::test::create_view, test::setup},
        tests::create_view_request,
    };

    #[sqlx::test]
    async fn test_view_exists(pool: PgPool) {
        let (api_context, namespace, whi) = setup(pool, None).await;

        let view_name = "my-view";
        let rq: CreateViewRequest = create_view_request(Some(view_name), None);

        let prefix = Prefix(whi.to_string());
        let _ = Box::pin(create_view(
            api_context.clone(),
            namespace.clone(),
            rq,
            Some(prefix.clone().into_string()),
        ))
        .await
        .unwrap();
        view_exists(
            ViewParameters {
                prefix: Some(prefix.clone()),
                view: TableIdent {
                    namespace: namespace.clone(),
                    name: view_name.to_string(),
                },
            },
            api_context.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        let non_exist = view_exists(
            ViewParameters {
                prefix: Some(prefix.clone()),
                view: TableIdent {
                    namespace: namespace.clone(),
                    name: "123".to_string(),
                },
            },
            api_context.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap_err();

        assert_eq!(non_exist.error.code, http::StatusCode::NOT_FOUND);
    }
}
