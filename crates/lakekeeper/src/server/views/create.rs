use std::sync::Arc;

use iceberg::{TableIdent, ViewCreation, spec::ViewMetadataBuilder};
use iceberg_ext::catalog::rest::{CreateViewRequest, ErrorModel, LoadViewResult};

use crate::{
    api::{
        ApiContext,
        iceberg::v1::{DataAccessMode, NamespaceParameters},
    },
    request_metadata::RequestMetadata,
    server::{
        compression_codec::CompressionCodec,
        io::write_file,
        maybe_get_secret, require_warehouse_id,
        tables::{require_active_warehouse, validate_table_or_view_ident},
        tabular::determine_tabular_location,
        views::validate_view_properties,
    },
    service::{
        CachePolicy, CatalogStore, CatalogViewOps, Result, SecretStore, State, TabularId,
        Transaction, ViewId,
        authz::{Authorizer, AuthzNamespaceOps, CatalogNamespaceAction},
        storage::{StorageLocations as _, StoragePermissions},
    },
};

// TODO: split up into smaller functions
#[allow(clippy::too_many_lines)]
/// Create a view in the given namespace
pub(crate) async fn create_view<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    parameters: NamespaceParameters,
    request: CreateViewRequest,
    state: ApiContext<State<A, C, S>>,
    data_access: impl Into<DataAccessMode>,
    request_metadata: RequestMetadata,
) -> Result<LoadViewResult> {
    let data_access = data_access.into();
    // ------------------- VALIDATIONS -------------------
    let NamespaceParameters {
        namespace: provided_ns,
        prefix,
    } = &parameters;
    let warehouse_id = require_warehouse_id(prefix.as_ref())?;
    let view = TableIdent::new(provided_ns.clone(), request.name.clone());

    validate_table_or_view_ident(&view)?;
    validate_view_properties(request.properties.keys())?;

    if request.view_version.representations().is_empty() {
        return Err(ErrorModel::bad_request(
            "View must have at least one representation.",
            "EmptyView",
            None,
        )
        .into());
    }

    // ------------------- AUTHZ -------------------
    let authorizer = &state.v1_state.authz;
    let (warehouse, ns_hierarchy) = authorizer
        .load_and_authorize_namespace_action::<C>(
            &request_metadata,
            warehouse_id,
            provided_ns,
            CatalogNamespaceAction::CreateView {
                properties: Arc::new(request.properties.clone().into_iter().collect()),
            },
            CachePolicy::Use,
            state.v1_state.catalog.clone(),
        )
        .await?;

    // ------------------- BUSINESS LOGIC -------------------
    let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;
    require_active_warehouse(warehouse.status)?;

    let view_id: TabularId = TabularId::View(uuid::Uuid::now_v7().into());

    let view_location = determine_tabular_location(
        &ns_hierarchy.namespace.namespace,
        request.location.clone(),
        view_id,
        &warehouse.storage_profile,
    )?;

    // Update the request for event
    let mut request = request;
    request.location = Some(view_location.to_string());
    let request = request; // make it immutable

    let metadata_location = warehouse.storage_profile.default_metadata_location(
        &view_location,
        &CompressionCodec::try_from_properties(&request.properties)?,
        *view_id,
        0,
    );

    let view_creation = ViewMetadataBuilder::from_view_creation(ViewCreation {
        name: view.name.clone(),
        location: view_location.to_string(),
        representations: request.view_version.representations().clone(),
        schema: request.schema.clone(),
        properties: request.properties.clone(),
        default_namespace: request.view_version.default_namespace().clone(),
        default_catalog: request.view_version.default_catalog().cloned(),
        summary: request.view_version.summary().clone(),
    })
    .unwrap()
    .assign_uuid(*view_id.as_ref());

    let metadata_build_result = view_creation.build().map_err(|e| {
        ErrorModel::bad_request(
            format!("Failed to create view metadata: {e}"),
            "ViewMetadataCreationFailed",
            Some(Box::new(e)),
        )
    })?;

    let view_info = C::create_view(
        warehouse_id,
        ns_hierarchy.namespace_id(),
        &view,
        &metadata_build_result.metadata,
        &metadata_location,
        t.transaction(),
    )
    .await?;

    // We don't commit the transaction yet, first we need to write the metadata file.
    let storage_secret =
        maybe_get_secret(warehouse.storage_secret_id, &state.v1_state.secrets).await?;
    let storage_secret_ref = storage_secret.as_deref();

    let file_io = warehouse
        .storage_profile
        .file_io(storage_secret_ref)
        .await?;
    let compression_codec = CompressionCodec::try_from_metadata(&metadata_build_result.metadata)?;
    write_file(
        &file_io,
        &metadata_location,
        &metadata_build_result.metadata,
        compression_codec,
    )
    .await?;
    tracing::debug!("Wrote new metadata file to: '{}'", metadata_location);

    // Generate the storage profile. This requires the storage secret
    // because the table config might contain vended-credentials based
    // on the `data_access` parameter.
    // ToDo: There is a small inefficiency here: If storage credentials
    // are not required because of i.e. remote-signing and if this
    // is a stage-create, we still fetch the secret.
    let config = warehouse
        .storage_profile
        .generate_table_config(
            data_access,
            storage_secret_ref,
            &view_location,
            StoragePermissions::Read,
            &request_metadata,
            &view_info,
        )
        .await?;

    authorizer
        .create_view(
            &request_metadata,
            warehouse_id,
            ViewId::from(metadata_build_result.metadata.uuid()),
            ns_hierarchy.namespace_id(),
        )
        .await?;

    t.commit().await?;

    state
        .v1_state
        .hooks
        .create_view(
            warehouse_id,
            parameters.clone(),
            Arc::new(request),
            Arc::new(metadata_build_result.metadata.clone()),
            Arc::new(metadata_location.clone()),
            data_access,
            Arc::new(request_metadata),
        )
        .await;

    let load_view_result = LoadViewResult {
        metadata_location: metadata_location.to_string(),
        metadata: Arc::new(metadata_build_result.metadata),
        config: Some(config.config.into()),
    };

    Ok(load_view_result)
}

#[cfg(test)]
pub(crate) mod test {
    use iceberg::NamespaceIdent;
    use sqlx::PgPool;
    use uuid::Uuid;

    use super::*;
    use crate::{
        api::iceberg::{types::Prefix, v1::DataAccess},
        implementations::postgres::{
            namespace::tests::initialize_namespace, secrets::SecretsState,
        },
        service::authz::AllowAllAuthorizer,
        tests::create_view_request,
    };

    pub(crate) async fn create_view(
        api_context: ApiContext<
            State<
                AllowAllAuthorizer,
                crate::implementations::postgres::PostgresBackend,
                SecretsState,
            >,
        >,
        namespace: NamespaceIdent,
        rq: CreateViewRequest,
        prefix: Option<String>,
    ) -> Result<LoadViewResult> {
        Box::pin(super::create_view(
            NamespaceParameters {
                namespace: namespace.clone(),
                prefix: Some(Prefix(
                    prefix.unwrap_or("b8683712-3484-11ef-a305-1bc8771ed40c".to_string()),
                )),
            },
            rq,
            api_context,
            DataAccess {
                vended_credentials: true,
                remote_signing: false,
            },
            RequestMetadata::new_unauthenticated(),
        ))
        .await
    }

    #[sqlx::test]
    async fn test_create_view(pool: PgPool) {
        let (api_context, namespace, whi, _) = crate::server::views::test::setup(pool, None).await;

        let mut rq = create_view_request(None, None);

        let _view = Box::pin(create_view(
            api_context.clone(),
            namespace.clone(),
            rq.clone(),
            Some(whi.to_string()),
        ))
        .await
        .unwrap();
        let view = Box::pin(create_view(
            api_context.clone(),
            namespace.clone(),
            rq.clone(),
            Some(whi.to_string()),
        ))
        .await
        .expect_err("Recreate with same ident should fail.");
        assert_eq!(view.error.code, 409);
        let old_name = rq.name.clone();
        rq.name = "some-other-name".to_string();

        let _view = Box::pin(create_view(
            api_context.clone(),
            namespace,
            rq.clone(),
            Some(whi.to_string()),
        ))
        .await
        .expect("Recreate with with another name it should work");

        rq.name = old_name;
        let namespace = NamespaceIdent::from_vec(vec![Uuid::now_v7().to_string()]).unwrap();
        let new_ns =
            initialize_namespace(api_context.v1_state.catalog.clone(), whi, &namespace, None)
                .await
                .namespace_ident()
                .clone();

        let _view = Box::pin(create_view(api_context, new_ns, rq, Some(whi.to_string())))
            .await
            .expect("Recreate with same name but different ns should work.");
    }
}
