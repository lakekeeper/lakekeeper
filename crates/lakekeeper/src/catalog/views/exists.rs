use iceberg::TableIdent;

use crate::{
    api::{iceberg::v1::ViewParameters, set_not_found_status_code, ApiContext},
    catalog::{require_warehouse_id, tables::validate_table_or_view_ident},
    request_metadata::RequestMetadata,
    service::{
        authz::{Authorizer, CatalogViewAction, CatalogWarehouseAction},
        Catalog, Result, SecretStore, State, Transaction, ViewId,
    },
    WarehouseId,
};

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
    let mut t = C::Transaction::begin_read(state.v1_state.catalog).await?;
    let _view_id = authorized_view_ident_to_id::<C, _>(
        authorizer.clone(),
        &request_metadata,
        warehouse_id,
        &view,
        CatalogViewAction::CanGetMetadata,
        t.transaction(),
    )
    .await?;

    t.commit().await?;

    Ok(())
}

pub(crate) async fn authorized_view_ident_to_id<C: Catalog, A: Authorizer>(
    authorizer: A,
    metadata: &RequestMetadata,
    warehouse_id: WarehouseId,
    view_ident: &TableIdent,
    action: impl From<CatalogViewAction> + std::fmt::Display + Send,
    transaction: <C::Transaction as Transaction<C::State>>::Transaction<'_>,
) -> Result<ViewId> {
    authorizer
        .require_warehouse_action(metadata, warehouse_id, CatalogWarehouseAction::CanUse)
        .await?;
    let view_id = C::view_to_id(warehouse_id, view_ident, transaction).await; // We can't fail before AuthZ
    authorizer
        .require_view_action(metadata, view_id, action)
        .await
        .map_err(set_not_found_status_code)
}

#[cfg(test)]
mod test {
    use iceberg::TableIdent;
    use iceberg_ext::catalog::rest::CreateViewRequest;
    use sqlx::PgPool;

    use super::*;
    use crate::{
        api::iceberg::{types::Prefix, v1::ViewParameters},
        catalog::views::{create::test::create_view, test::setup},
    };

    #[sqlx::test]
    async fn test_view_exists(pool: PgPool) {
        let (api_context, namespace, whi) = setup(pool, None).await;

        let view_name = "my-view";
        let rq: CreateViewRequest =
            super::super::create::test::create_view_request(Some(view_name), None);

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
