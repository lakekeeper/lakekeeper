use std::sync::Arc;

use iceberg::{
    spec::{ViewFormatVersion, ViewMetadata, ViewMetadataBuilder},
    TableIdent,
};
use iceberg_ext::{
    catalog::{rest::ViewUpdate, ViewRequirement},
    configs::Location,
};
use uuid::Uuid;

use crate::{
    api::iceberg::v1::{
        ApiContext, CommitViewRequest, DataAccess, ErrorModel, LoadViewResult, Result,
        ViewParameters,
    },
    catalog::{
        compression_codec::CompressionCodec,
        io::{remove_all, write_metadata_file},
        require_warehouse_id,
        tables::{
            determine_table_ident, extract_count_from_metadata_location, require_active_warehouse,
            validate_table_or_view_ident, CONCURRENT_UPDATE_ERROR_TYPE,
            MAX_RETRIES_ON_CONCURRENT_UPDATE,
        },
        views::{parse_view_location, validate_view_updates},
    },
    request_metadata::RequestMetadata,
    service::{
        authz::{Authorizer, CatalogViewAction, CatalogWarehouseAction},
        contract_verification::ContractVerification,
        secrets::SecretStore,
        storage::{StorageLocations as _, StoragePermissions, StorageProfile},
        Catalog, NamespaceIdentUuid, State, Transaction, ViewCommit, ViewIdentUuid,
        ViewMetadataWithLocation,
    },
    SecretIdent,
};

/// Commit updates to a view
// TODO: break up into smaller fns
#[allow(clippy::too_many_lines)]
pub(crate) async fn commit_view<C: Catalog, A: Authorizer + Clone, S: SecretStore>(
    parameters: ViewParameters,
    request: CommitViewRequest,
    state: ApiContext<State<A, C, S>>,
    data_access: DataAccess,
    request_metadata: RequestMetadata,
) -> Result<LoadViewResult> {
    // ------------------- VALIDATIONS -------------------
    let warehouse_id = require_warehouse_id(parameters.prefix.clone())?;

    let CommitViewRequest {
        identifier,
        requirements,
        updates,
    } = &request;

    let identifier = determine_table_ident(&parameters.view, identifier.as_ref())?;
    validate_table_or_view_ident(&identifier)?;

    // ------------------- AUTHZ -------------------
    let authorizer = state.v1_state.authz.clone();
    authorizer
        .require_warehouse_action(
            &request_metadata,
            warehouse_id,
            CatalogWarehouseAction::CanUse,
        )
        .await?;
    let mut t = C::Transaction::begin_read(state.v1_state.catalog.clone()).await?;
    let view_id = C::view_to_id(warehouse_id, &identifier, t.transaction()).await; // We can't fail before AuthZ;
    let view_id = authorizer
        .require_view_action(&request_metadata, view_id, CatalogViewAction::CanCommit)
        .await?;

    // ------------------- BUSINESS LOGIC -------------------
    validate_view_updates(updates)?;

    // These operations only need to happen once before retries
    let namespace_id = C::namespace_to_id(warehouse_id, identifier.namespace(), t.transaction())
        .await?
        .ok_or(ErrorModel::not_found(
            "Namespace does not exist",
            "NamespaceNotFound",
            None,
        ))?;

    let warehouse = C::require_warehouse(warehouse_id, t.transaction()).await?;
    let storage_profile = &warehouse.storage_profile;
    let storage_secret_id = warehouse.storage_secret_id;
    require_active_warehouse(warehouse.status)?;
    t.commit().await?;

    // Verify assertions (only needed once)
    check_asserts(requirements.as_ref(), view_id)?;

    // Start the retry loop
    let request = Arc::new(request);
    let mut attempt = 0;
    loop {
        let result = try_commit_view::<C, A, S>(
            CommitViewContext {
                namespace_id,
                view_id,
                identifier: &identifier,
                storage_profile,
                storage_secret_id,
                request: request.as_ref(),
                data_access,
            },
            &state,
        )
        .await;

        match result {
            Ok((result, commit)) => {
                state
                    .v1_state
                    .hooks
                    .commit_view(
                        warehouse_id,
                        parameters,
                        request.clone(),
                        Arc::new(commit),
                        data_access,
                        Arc::new(request_metadata),
                    )
                    .await;

                return Ok(result);
            }
            Err(e)
                if e.error.r#type == CONCURRENT_UPDATE_ERROR_TYPE
                    && attempt < MAX_RETRIES_ON_CONCURRENT_UPDATE =>
            {
                attempt += 1;
                tracing::info!(
                    "Concurrent update detected (attempt {attempt}/{MAX_RETRIES_ON_CONCURRENT_UPDATE}), retrying view commit operation",
                );
                // Short delay before retry to reduce contention
                tokio::time::sleep(std::time::Duration::from_millis(50 * attempt as u64)).await;
            }
            Err(e) => return Err(e),
        }
    }
}

// Context structure to hold static parameters for retry function
struct CommitViewContext<'a> {
    namespace_id: NamespaceIdentUuid,
    view_id: ViewIdentUuid,
    identifier: &'a TableIdent,
    storage_profile: &'a StorageProfile,
    storage_secret_id: Option<SecretIdent>,
    request: &'a CommitViewRequest,
    data_access: DataAccess,
}

// Core commit logic that may be retried
async fn try_commit_view<C: Catalog, A: Authorizer + Clone, S: SecretStore>(
    ctx: CommitViewContext<'_>,
    state: &ApiContext<State<A, C, S>>,
) -> Result<(LoadViewResult, crate::service::endpoint_hooks::ViewCommit)> {
    let mut t = C::Transaction::begin_write(state.v1_state.catalog.clone()).await?;

    // These operations need fresh data on each retry
    let ViewMetadataWithLocation {
        metadata_location: previous_metadata_location,
        metadata: before_update_metadata,
    } = C::load_view(ctx.view_id, false, t.transaction()).await?;
    let previous_view_location = parse_view_location(before_update_metadata.location())?;
    let previous_metadata_location = parse_view_location(&previous_metadata_location)?;

    state
        .v1_state
        .contract_verifiers
        .check_view_updates(&ctx.request.updates, &before_update_metadata)
        .await?
        .into_result()?;

    let (requested_update_metadata, delete_old_location) = build_new_metadata(
        ctx.request.clone(),
        before_update_metadata.clone(),
        &previous_view_location,
    )?;

    let view_location = parse_view_location(requested_update_metadata.location())?;
    let metadata_location = ctx.storage_profile.default_metadata_location(
        &view_location,
        &CompressionCodec::try_from_properties(requested_update_metadata.properties())?,
        Uuid::now_v7(),
        extract_count_from_metadata_location(&previous_metadata_location).map_or(0, |v| v + 1),
    );

    if delete_old_location.is_some() {
        ctx.storage_profile
            .require_allowed_location(&view_location)?;
    }

    C::update_view_metadata(
        ViewCommit {
            namespace_id: ctx.namespace_id,
            view_id: ctx.view_id,
            view_ident: ctx.identifier,
            new_metadata_location: &metadata_location,
            previous_metadata_location: &previous_metadata_location,
            metadata: requested_update_metadata.clone(),
            new_location: &view_location,
        },
        t.transaction(),
    )
    .await?;

    // Get storage secret
    let storage_secret = if let Some(secret_id) = ctx.storage_secret_id {
        Some(
            state
                .v1_state
                .secrets
                .get_secret_by_id(secret_id)
                .await?
                .secret,
        )
    } else {
        None
    };

    // Write metadata file
    let file_io = ctx.storage_profile.file_io(storage_secret.as_ref()).await?;
    write_metadata_file(
        &metadata_location,
        &requested_update_metadata,
        CompressionCodec::try_from_metadata(&requested_update_metadata)?,
        &file_io,
    )
    .await?;

    tracing::debug!("Wrote new metadata file to: '{}'", metadata_location);

    // Generate config for client
    let config = ctx
        .storage_profile
        .generate_table_config(
            ctx.data_access,
            storage_secret.as_ref(),
            &metadata_location,
            StoragePermissions::ReadWriteDelete,
        )
        .await?;

    // Commit transaction
    t.commit().await?;

    // Handle file cleanup after transaction is committed
    if let Some(DeleteLocation(before_update_view_location)) = delete_old_location {
        tracing::debug!("Deleting old view location at: '{before_update_view_location}'");
        let _ = remove_all(&file_io, before_update_view_location)
            .await
            .inspect(|()| tracing::trace!("Deleted old view location"))
            .inspect_err(|e| tracing::error!("Failed to delete old view location: {e:?}"));
    }

    Ok((
        LoadViewResult {
            metadata_location: metadata_location.to_string(),
            metadata: requested_update_metadata.clone(),
            config: Some(config.config.into()),
        },
        crate::service::endpoint_hooks::ViewCommit {
            old_metadata: before_update_metadata,
            new_metadata: requested_update_metadata,
            old_metadata_location: previous_metadata_location,
            new_metadata_location: metadata_location,
        },
    ))
}

fn check_asserts(
    requirements: Option<&Vec<ViewRequirement>>,
    view_id: ViewIdentUuid,
) -> Result<()> {
    if let Some(requirements) = requirements {
        for assertion in requirements {
            match assertion {
                ViewRequirement::AssertViewUuid(uuid) => {
                    if uuid.uuid != *view_id {
                        return Err(ErrorModel::bad_request(
                            "View UUID does not match",
                            "ViewUuidMismatch",
                            None,
                        )
                        .into());
                    }
                }
            }
        }
    }

    Ok(())
}

struct DeleteLocation<'c>(&'c Location);

fn build_new_metadata(
    request: CommitViewRequest,
    before_update_metadata: ViewMetadata,
    before_location: &Location,
) -> Result<(ViewMetadata, Option<DeleteLocation<'_>>)> {
    let previous_location = before_update_metadata.location().to_string();

    let mut m = ViewMetadataBuilder::new_from_metadata(before_update_metadata);
    let mut delete_old_location = None;
    for upd in request.updates {
        m = match upd {
            ViewUpdate::AssignUuid { .. } => {
                return Err(ErrorModel::bad_request(
                    "Assigning UUIDs is not supported",
                    "AssignUuidNotSupported",
                    None,
                )
                .into());
            }
            ViewUpdate::SetLocation { location } => {
                if location != previous_location {
                    delete_old_location = Some(DeleteLocation(before_location));
                }
                m.set_location(location)
            }

            ViewUpdate::UpgradeFormatVersion { format_version } => match format_version {
                ViewFormatVersion::V1 => m,
            },
            ViewUpdate::AddSchema {
                schema,
                last_column_id: _,
            } => m.add_schema(schema),
            ViewUpdate::SetProperties { updates } => m.set_properties(updates).map_err(|e| {
                ErrorModel::bad_request(
                    format!("Error setting properties: {e}"),
                    "AddSchemaError",
                    Some(Box::new(e)),
                )
            })?,
            ViewUpdate::RemoveProperties { removals } => m.remove_properties(&removals),
            ViewUpdate::AddViewVersion { view_version } => {
                m.add_version(view_version).map_err(|e| {
                    ErrorModel::bad_request(
                        format!("Error appending view version: {e}"),
                        "AppendViewVersionError".to_string(),
                        Some(Box::new(e)),
                    )
                })?
            }
            ViewUpdate::SetCurrentViewVersion { view_version_id } => {
                m.set_current_version_id(view_version_id).map_err(|e| {
                    ErrorModel::bad_request(
                        "Error setting current view version: {e}",
                        "SetCurrentViewVersionError",
                        Some(Box::new(e)),
                    )
                })?
            }
        }
    }

    let requested_update_metadata = m.build().map_err(|e| {
        ErrorModel::bad_request(
            format!("Error building metadata: {e}"),
            "BuildMetadataError",
            Some(Box::new(e)),
        )
    })?;
    Ok((requested_update_metadata.metadata, delete_old_location))
}

#[cfg(test)]
mod test {
    use chrono::Utc;
    use iceberg::TableIdent;
    use iceberg_ext::catalog::rest::CommitViewRequest;
    use maplit::hashmap;
    use serde_json::json;
    use sqlx::PgPool;
    use uuid::Uuid;

    use crate::{
        api::iceberg::v1::{views, DataAccess, Prefix, ViewParameters},
        catalog::views::{
            create::test::{create_view, create_view_request},
            test::setup,
        },
    };

    #[sqlx::test]
    async fn test_commit_view(pool: PgPool) {
        let (api_context, namespace, whi) = setup(pool, None).await;
        let prefix = whi.to_string();
        let view_name = "myview";
        let view = create_view(
            api_context.clone(),
            namespace.clone(),
            create_view_request(Some(view_name), None),
            Some(prefix.clone()),
        )
        .await
        .unwrap();

        let rq: CommitViewRequest = spark_commit_update_request(Some(view.metadata.uuid()));

        let res = super::commit_view(
            views::ViewParameters {
                prefix: Some(Prefix(prefix.clone())),
                view: TableIdent::from_strs(
                    namespace.inner().into_iter().chain([view_name.into()]),
                )
                .unwrap(),
            },
            rq,
            api_context,
            DataAccess {
                vended_credentials: true,
                remote_signing: false,
            },
            crate::request_metadata::RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        assert_eq!(res.metadata.current_version_id(), 2);
        assert_eq!(res.metadata.schemas_iter().len(), 3);
        assert_eq!(res.metadata.versions().len(), 2);
        let max_schema = res.metadata.schemas_iter().map(|s| s.schema_id()).max();
        assert_eq!(
            res.metadata.current_version().schema_id(),
            max_schema.unwrap()
        );

        assert_eq!(
            res.metadata.properties(),
            &hashmap! {
                "create_engine_version".to_string() => "Spark 3.5.1".to_string(),
                "spark.query-column-names".to_string() => "id".to_string(),
            }
        );
    }

    #[sqlx::test]
    async fn test_commit_view_fails_with_wrong_assertion(pool: PgPool) {
        let (api_context, namespace, whi) = setup(pool, None).await;
        let prefix = whi.to_string();
        let view_name = "myview";
        let _ = create_view(
            api_context.clone(),
            namespace.clone(),
            create_view_request(Some(view_name), None),
            Some(prefix.clone()),
        )
        .await
        .unwrap();

        let rq: CommitViewRequest = spark_commit_update_request(Some(Uuid::now_v7()));

        let err = super::commit_view(
            ViewParameters {
                prefix: Some(Prefix(prefix.clone())),
                view: TableIdent::from_strs(
                    namespace.inner().into_iter().chain([view_name.into()]),
                )
                .unwrap(),
            },
            rq,
            api_context,
            DataAccess {
                vended_credentials: true,
                remote_signing: false,
            },
            crate::request_metadata::RequestMetadata::new_unauthenticated(),
        )
        .await
        .expect_err("This unexpectedly didn't fail the uuid assertion.");
        assert_eq!(err.error.code, 400);
        assert_eq!(err.error.r#type, "ViewUuidMismatch");
    }

    fn spark_commit_update_request(asserted_uuid: Option<Uuid>) -> CommitViewRequest {
        let uuid = asserted_uuid.map_or("019059cb-9277-7ff0-b71a-537df05b33f8".into(), |u| {
            u.to_string()
        });
        serde_json::from_value(json!({
  "requirements": [
    {
      "type": "assert-view-uuid",
      "uuid": &uuid
    }
  ],
  "updates": [
    {
      "action": "set-properties",
      "updates": {
        "create_engine_version": "Spark 3.5.1",
        "spark.query-column-names": "id",
        "engine_version": "Spark 3.5.1"
      }
    },
    {
      "action": "add-schema",
      "schema": {
        "schema-id": 1,
        "type": "struct",
        "fields": [
          {
            "id": 0,
            "name": "id",
            "required": false,
            "type": "long",
            "doc": "id of thing"
          }
        ]
      },
      "last-column-id": 1
    },
    {
      "action": "add-schema",
      "schema": {
        "schema-id": 2,
        "type": "struct",
        "fields": [
          {
            "id": 0,
            "name": "idx",
            "required": false,
            "type": "long",
            "doc": "idx of thing"
          }
        ]
      },
      "last-column-id": 1
    },
    {
      "action": "add-view-version",
      "view-version": {
        "version-id": 2,
        "schema-id": -1,
        "timestamp-ms": Utc::now().timestamp_millis(),
        "summary": {
          "engine-name": "spark",
          "engine-version": "3.5.1",
          "iceberg-version": "Apache Iceberg 1.5.2 (commit cbb853073e681b4075d7c8707610dceecbee3a82)",
          "app-id": "local-1719494665567"
        },
        "representations": [
          {
            "type": "sql",
            "sql": "select id from spark_demo.my_table",
            "dialect": "spark"
          }
        ],
        "default-namespace": []
      }
    },
    {
        "action": "remove-properties",
        "removals": ["engine_version"]
    },
    {
      "action": "set-current-view-version",
      "view-version-id": -1
    }
  ]
})).unwrap()
    }
}
