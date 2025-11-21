use crate::{
    api::{iceberg::v1::ViewParameters, ApiContext},
    request_metadata::RequestMetadata,
    server::{require_warehouse_id, tables::validate_table_or_view_ident},
    service::{
        authz::{AuthZViewOps, Authorizer, CatalogViewAction},
        CatalogStore, Result, SecretStore, State, TabularListFlags,
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

    let (_warehouse, _namespace, _view_info) = authorizer
        .load_and_authorize_view_operation::<C>(
            &request_metadata,
            warehouse_id,
            view.clone(),
            TabularListFlags::active(),
            CatalogViewAction::GetMetadata,
            state.v1_state.catalog.clone(),
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
