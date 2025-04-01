use iceberg::{spec::ViewMetadataBuilder, TableIdent, ViewCreation};
use iceberg_ext::catalog::rest::{CreateViewRequest, ErrorModel, LoadViewResult};
use uuid::Uuid;

use crate::{
    api::{
        iceberg::{
            types::Prefix,
            v1::{DataAccess, NamespaceParameters},
        },
        ApiContext,
    },
    catalog::{
        compression_codec::CompressionCodec,
        io::write_metadata_file,
        maybe_get_secret, require_warehouse_id,
        tables::{
            determine_tabular_location, maybe_body_to_json, require_active_warehouse,
            validate_table_or_view_ident,
        },
        views::validate_view_properties,
    },
    request_metadata::RequestMetadata,
    service::{
        authz::{Authorizer, CatalogNamespaceAction, CatalogWarehouseAction},
        event_publisher::EventMetadata,
        storage::{StorageLocations as _, StoragePermissions},
        Catalog, Result, SecretStore, State, TabularIdentUuid, Transaction, ViewIdentUuid,
    },
};

// TODO: split up into smaller functions
#[allow(clippy::too_many_lines)]
/// Create a view in the given namespace
pub(crate) async fn create_view<C: Catalog, A: Authorizer + Clone, S: SecretStore>(
    parameters: NamespaceParameters,
    request: CreateViewRequest,
    state: ApiContext<State<A, C, S>>,
    data_access: DataAccess,
    request_metadata: RequestMetadata,
) -> Result<LoadViewResult> {
    // ------------------- VALIDATIONS -------------------
    let NamespaceParameters { namespace, prefix } = parameters;
    let warehouse_id = require_warehouse_id(prefix.clone())?;
    let view = TableIdent::new(namespace.clone(), request.name.clone());

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
    authorizer
        .require_warehouse_action(
            &request_metadata,
            warehouse_id,
            CatalogWarehouseAction::CanUse,
        )
        .await?;
    let mut t = C::Transaction::begin_write(state.v1_state.catalog.clone()).await?;
    let namespace_id = C::namespace_to_id(warehouse_id, &namespace, t.transaction()).await; // Cannot fail before authz;
    let namespace_id = authorizer
        .require_namespace_action(
            &request_metadata,
            namespace_id,
            CatalogNamespaceAction::CanCreateView,
        )
        .await?;

    // ------------------- BUSINESS LOGIC -------------------
    let namespace = C::get_namespace(warehouse_id, namespace_id, t.transaction()).await?;
    let warehouse = C::require_warehouse(warehouse_id, t.transaction()).await?;
    let storage_profile = warehouse.storage_profile;
    require_active_warehouse(warehouse.status)?;

    let view_id: TabularIdentUuid = TabularIdentUuid::View(uuid::Uuid::now_v7());

    let view_location = determine_tabular_location(
        &namespace,
        request.location.clone(),
        view_id,
        &storage_profile,
    )?;

    // Update the request for event
    let mut request = request;
    request.location = Some(view_location.to_string());
    let request = request; // make it immutable

    let metadata_location = storage_profile.default_metadata_location(
        &view_location,
        &CompressionCodec::try_from_properties(&request.properties)?,
        *view_id,
        0,
    );

    // serialize body before moving it
    let body = maybe_body_to_json(&request);
    let view_creation = ViewMetadataBuilder::from_view_creation(ViewCreation {
        name: view.name.clone(),
        location: view_location.to_string(),
        representations: request.view_version.representations().clone(),
        schema: request.schema,
        properties: request.properties.clone(),
        default_namespace: request.view_version.default_namespace().clone(),
        default_catalog: request.view_version.default_catalog().cloned(),
        summary: request.view_version.summary().clone(),
    })
    .unwrap()
    .assign_uuid(*view_id.as_ref());

    let metadata = view_creation.build().map_err(|e| {
        ErrorModel::bad_request(
            format!("Failed to create view metadata: {e}"),
            "ViewMetadataCreationFailed",
            Some(Box::new(e)),
        )
    })?;

    C::create_view(
        namespace_id,
        &view,
        metadata.metadata.clone(),
        &metadata_location,
        &view_location,
        t.transaction(),
    )
    .await?;

    // We don't commit the transaction yet, first we need to write the metadata file.
    let storage_secret =
        maybe_get_secret(warehouse.storage_secret_id, &state.v1_state.secrets).await?;

    let file_io = storage_profile.file_io(storage_secret.as_ref())?;
    let compression_codec = CompressionCodec::try_from_metadata(&metadata.metadata)?;
    write_metadata_file(
        &metadata_location,
        &metadata.metadata,
        compression_codec,
        &file_io,
    )
    .await?;
    tracing::debug!("Wrote new metadata file to: '{}'", metadata_location);

    // Generate the storage profile. This requires the storage secret
    // because the table config might contain vended-credentials based
    // on the `data_access` parameter.
    // ToDo: There is a small inefficiency here: If storage credentials
    // are not required because of i.e. remote-signing and if this
    // is a stage-create, we still fetch the secret.
    let config = storage_profile
        .generate_table_config(
            &data_access,
            storage_secret.as_ref(),
            &view_location,
            StoragePermissions::Read,
        )
        .await?;

    authorizer
        .create_view(
            &request_metadata,
            ViewIdentUuid::from(metadata.metadata.uuid()),
            namespace_id,
        )
        .await?;

    t.commit().await?;

    let _ = state
        .v1_state
        .publisher
        .publish(
            Uuid::now_v7(),
            "createView",
            body,
            EventMetadata {
                tabular_id: TabularIdentUuid::View(*view_id),
                warehouse_id,
                name: view.name,
                namespace: view.namespace.to_url_string(),
                prefix: prefix.map(Prefix::into_string).unwrap_or_default(),
                num_events: 1,
                sequence_number: 0,
                trace_id: request_metadata.request_id(),
            },
        )
        .await;

    let load_view_result = LoadViewResult {
        metadata_location: metadata_location.to_string(),
        metadata: metadata.metadata,
        config: Some(config.config.into()),
    };

    Ok(load_view_result)
}

#[cfg(test)]
pub(crate) mod test {
    use iceberg::NamespaceIdent;
    use serde_json::json;
    use sqlx::PgPool;

    use super::*;
    use crate::{
        implementations::postgres::{
            namespace::tests::initialize_namespace, secrets::SecretsState,
        },
        service::authz::AllowAllAuthorizer,
    };

    pub(crate) async fn create_view(
        api_context: ApiContext<
            State<
                AllowAllAuthorizer,
                crate::implementations::postgres::PostgresCatalog,
                SecretsState,
            >,
        >,
        namespace: NamespaceIdent,
        rq: CreateViewRequest,
        prefix: Option<String>,
    ) -> Result<LoadViewResult> {
        super::create_view(
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
        )
        .await
    }

    #[sqlx::test]
    async fn test_create_view(pool: PgPool) {
        let (api_context, namespace, whi) = crate::catalog::views::test::setup(pool, None).await;

        let mut rq = create_view_request(None, None);

        let _view = create_view(
            api_context.clone(),
            namespace.clone(),
            rq.clone(),
            Some(whi.to_string()),
        )
        .await
        .unwrap();
        let view = create_view(
            api_context.clone(),
            namespace.clone(),
            rq.clone(),
            Some(whi.to_string()),
        )
        .await
        .expect_err("Recreate with same ident should fail.");
        assert_eq!(view.error.code, 409);
        let old_name = rq.name.clone();
        rq.name = "some-other-name".to_string();

        let _view = create_view(
            api_context.clone(),
            namespace,
            rq.clone(),
            Some(whi.to_string()),
        )
        .await
        .expect("Recreate with with another name it should work");

        rq.name = old_name;
        let namespace = NamespaceIdent::from_vec(vec![Uuid::now_v7().to_string()]).unwrap();
        let new_ns =
            initialize_namespace(api_context.v1_state.catalog.clone(), whi, &namespace, None)
                .await
                .1
                .namespace;

        let _view = create_view(api_context, new_ns, rq, Some(whi.to_string()))
            .await
            .expect("Recreate with same name but different ns should work.");
    }

    pub(crate) fn create_view_request(
        name: Option<&str>,
        location: Option<&str>,
    ) -> CreateViewRequest {
        serde_json::from_value(json!({
                                  "name": name.unwrap_or("myview"),
                                  "location": location,
                                  "schema": {
                                    "schema-id": 0,
                                    "type": "struct",
                                    "fields": [
                                      {
                                        "id": 0,
                                        "name": "id",
                                        "required": false,
                                        "type": "long"
                                      }
                                    ]
                                  },
                                  "view-version": {
                                    "version-id": 1,
                                    "schema-id": 0,
                                    "timestamp-ms": 1_719_395_654_343_i64,
                                    "summary": {
                                      "engine-version": "3.5.1",
                                      "iceberg-version": "Apache Iceberg 1.5.2 (commit cbb853073e681b4075d7c8707610dceecbee3a82)",
                                      "engine-name": "spark",
                                      "app-id": "local-1719395622847"
                                    },
                                    "representations": [
                                      {
                                        "type": "sql",
                                        "sql": "select id, xyz from spark_demo.my_table",
                                        "dialect": "spark"
                                      }
                                    ],
                                    "default-namespace": []
                                  },
                                  "properties": {
                                    "create_engine_version": "Spark 3.5.1",
                                    "engine_version": "Spark 3.5.1",
                                    "spark.query-column-names": "id"
                                  }})).unwrap()
    }
}
