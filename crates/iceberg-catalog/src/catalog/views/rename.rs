use http::StatusCode;
use iceberg_ext::catalog::rest::RenameTableRequest;
use uuid::Uuid;

use crate::{
    api::{iceberg::types::Prefix, ApiContext},
    catalog::{
        require_warehouse_id,
        tables::{maybe_body_to_json, validate_table_or_view_ident},
    },
    request_metadata::RequestMetadata,
    service::{
        authz::{Authorizer, CatalogNamespaceAction, CatalogViewAction, CatalogWarehouseAction},
        contract_verification::ContractVerification,
        event_publisher::EventMetadata,
        Catalog, Result, SecretStore, State, TabularIdentUuid, Transaction,
    },
};

pub(crate) async fn rename_view<C: Catalog, A: Authorizer + Clone, S: SecretStore>(
    prefix: Option<Prefix>,
    request: RenameTableRequest,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
) -> Result<()> {
    // ------------------- VALIDATIONS -------------------
    let warehouse_id = require_warehouse_id(prefix.clone())?;
    let RenameTableRequest {
        source,
        destination,
    } = &request;
    validate_table_or_view_ident(source)?;
    validate_table_or_view_ident(destination)?;

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

    let source_id = C::view_to_id(warehouse_id, &request.source, t.transaction()).await; // We can't fail before AuthZ;
    let source_id = authorizer
        .require_view_action(&request_metadata, source_id, CatalogViewAction::CanRename)
        .await
        .map_err(|mut e| {
            e.error.code = StatusCode::NOT_FOUND.into();
            e
        })?;
    // We need to be allowed to delete the old table and create the new one
    let namespace_id = C::namespace_to_id(warehouse_id, &source.namespace, t.transaction()).await; // We can't fail before AuthZ
                                                                                                   // We need to be allowed to delete the old table and create the new one
    authorizer
        .require_namespace_action(
            &request_metadata,
            namespace_id,
            CatalogNamespaceAction::CanCreateTable,
        )
        .await?;

    // ------------------- BUSINESS LOGIC -------------------
    if source == destination {
        return Ok(());
    }
    let body = maybe_body_to_json(&request);

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
        .check_rename(TabularIdentUuid::View(*source_id), destination)
        .await?
        .into_result()?;

    t.commit().await?;

    let _ = state
        .v1_state
        .publisher
        .publish(
            Uuid::now_v7(),
            "renameView",
            body,
            EventMetadata {
                tabular_id: TabularIdentUuid::View(*source_id),
                warehouse_id,
                name: request.source.name,
                namespace: request.source.namespace.to_url_string(),
                prefix: prefix.map(Prefix::into_string).unwrap_or_default(),
                num_events: 1,
                sequence_number: 0,
                trace_id: request_metadata.request_id(),
            },
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
        catalog::views::{create::test::create_view, load::test::load_view, test::setup},
        implementations::postgres::namespace::tests::initialize_namespace,
    };

    #[sqlx::test]
    async fn test_rename_view_without_namespace(pool: PgPool) {
        let (api_context, namespace, whi) = setup(pool, None).await;

        let view_name = "my-view";
        let rq: CreateViewRequest =
            super::super::create::test::create_view_request(Some(view_name), None);

        let prefix = Prefix(whi.to_string());
        let created_view = create_view(
            api_context.clone(),
            namespace.clone(),
            rq,
            Some(prefix.clone().into_string()),
        )
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
                .namespace;

        let view_name = "my-view";
        let rq: CreateViewRequest =
            super::super::create::test::create_view_request(Some(view_name), None);

        let prefix = Prefix(whi.to_string());
        let created_view = create_view(
            api_context.clone(),
            namespace.clone(),
            rq,
            Some(prefix.clone().into_string()),
        )
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
