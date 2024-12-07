use crate::api::iceberg::v1::ViewParameters;
use crate::api::{set_not_found_status_code, ApiContext};
use crate::catalog::require_warehouse_id;
use crate::catalog::tables::validate_table_or_view_ident;
use crate::request_metadata::RequestMetadata;
use crate::service::authz::{Authorizer, CatalogViewAction, CatalogWarehouseAction};
use crate::service::Result;
use crate::service::{Catalog, SecretStore, State, Transaction};

pub(crate) async fn view_exists<C: Catalog, A: Authorizer + Clone, S: SecretStore>(
    parameters: ViewParameters,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
) -> Result<()> {
    // ------------------- VALIDATIONS -------------------
    let ViewParameters { prefix, view } = parameters;
    let warehouse_id = require_warehouse_id(prefix.clone())?;
    validate_table_or_view_ident(&view)?;

    // ------------------- BUSINESS LOGIC -------------------
    let authorizer = state.v1_state.authz;
    authorizer
        .require_warehouse_action(
            &request_metadata,
            warehouse_id,
            &CatalogWarehouseAction::CanUse,
        )
        .await?;
    let mut t = C::Transaction::begin_read(state.v1_state.catalog).await?;
    let view_id = C::view_to_id(warehouse_id, &view, t.transaction()).await; // Can't fail before authz

    authorizer
        .require_view_action(
            &request_metadata,
            warehouse_id,
            view_id,
            &CatalogViewAction::CanGetMetadata,
        )
        .await
        .map_err(set_not_found_status_code)?;
    t.commit().await?;

    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::api::iceberg::types::Prefix;
    use crate::api::iceberg::v1::ViewParameters;
    use crate::catalog::views::create::test::create_view;
    use crate::catalog::views::test::setup;
    use iceberg::TableIdent;
    use iceberg_ext::catalog::rest::CreateViewRequest;
    use sqlx::PgPool;

    #[sqlx::test]
    async fn test_view_exists(pool: PgPool) {
        let (api_context, namespace, whi) = setup(pool, None).await;

        let view_name = "my-view";
        let rq: CreateViewRequest =
            super::super::create::test::create_view_request(Some(view_name), None);

        let prefix = Prefix(whi.to_string());
        let _ = create_view(
            api_context.clone(),
            namespace.clone(),
            rq,
            Some(prefix.clone().into_string()),
        )
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
            RequestMetadata::new_random(),
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
            RequestMetadata::new_random(),
        )
        .await
        .unwrap_err();

        assert_eq!(non_exist.error.code, http::StatusCode::NOT_FOUND);
    }
}
