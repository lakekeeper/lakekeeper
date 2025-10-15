use std::sync::Arc;

use http::StatusCode;
use iceberg_ext::catalog::rest::RenameTableRequest;

use crate::{
    api::{iceberg::types::Prefix, ApiContext},
    request_metadata::RequestMetadata,
    server::{require_warehouse_id, tables::validate_table_or_view_ident},
    service::{
        authz::{Authorizer, AuthzNamespaceOps, CatalogNamespaceAction, CatalogViewAction},
        contract_verification::ContractVerification,
        CatalogNamespaceOps, CatalogStore, Result, SecretStore, State, TabularId, Transaction,
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
    let authorizer = state.v1_state.authz;

    let destination_namespace = C::require_namespace(
        warehouse_id,
        &destination.namespace,
        state.v1_state.catalog.clone(),
    )
    .await;

    let _destination_namespace = authorizer
        .require_namespace_action(
            &request_metadata,
            warehouse_id,
            &destination.namespace,
            destination_namespace,
            CatalogNamespaceAction::CanCreateView,
        )
        .await?;

    let mut t_read = C::Transaction::begin_read(state.v1_state.catalog.clone()).await?;
    let source_id = C::view_to_id(warehouse_id, &request.source, t_read.transaction()).await; // We can't fail before AuthZ;
    t_read.commit().await?;
    let source_id = authorizer
        .require_view_action(
            &request_metadata,
            warehouse_id,
            source_id,
            CatalogViewAction::CanRename,
        )
        .await
        .map_err(|mut e| {
            e.error.code = StatusCode::NOT_FOUND.into();
            e
        })?;

    // ------------------- BUSINESS LOGIC -------------------
    if source == destination {
        return Ok(());
    }

    let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;

    C::rename_view(
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

    state
        .v1_state
        .hooks
        .rename_view(
            warehouse_id,
            source_id,
            Arc::new(request),
            Arc::new(request_metadata),
        )
        .await;

    Ok(())
}

#[cfg(test)]
mod test {
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
        let (api_context, namespace, whi) = setup(pool, None).await;

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
        let (api_context, _, whi) = setup(pool, None).await;
        let namespace = NamespaceIdent::from_vec(vec!["Someother-ns".to_string()]).unwrap();
        let new_ns =
            initialize_namespace(api_context.v1_state.catalog.clone(), whi, &namespace, None)
                .await
                .1
                .namespace_ident;

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
