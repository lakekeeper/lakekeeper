use iceberg::spec::{ViewFormatVersion, ViewMetadata, ViewMetadataBuilder};
use iceberg_ext::{
    catalog::{rest::ViewUpdate, ViewRequirement},
    configs::Location,
};
use uuid::Uuid;

use crate::{
    api::iceberg::v1::{
        ApiContext, CommitViewRequest, DataAccess, ErrorModel, LoadViewResult, Prefix, Result,
        ViewParameters,
    },
    catalog::{
        compression_codec::CompressionCodec,
        io::{remove_all, write_metadata_file},
        require_warehouse_id,
        tables::{
            determine_table_ident, extract_count_from_metadata_location, maybe_body_to_json,
            require_active_warehouse, validate_table_or_view_ident,
        },
        views::{parse_view_location, validate_view_updates},
    },
    request_metadata::RequestMetadata,
    service::{
        authz::{Authorizer, CatalogViewAction, CatalogWarehouseAction},
        contract_verification::ContractVerification,
        event_publisher::EventMetadata,
        secrets::SecretStore,
        storage::{StorageLocations as _, StoragePermissions},
        Catalog, GetWarehouseResponse, State, TabularIdentUuid, Transaction, ViewIdentUuid,
        ViewMetadataWithLocation,
    },
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

    let identifier = determine_table_ident(parameters.view, identifier.as_ref())?;
    validate_table_or_view_ident(&identifier)?;

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
    let view_id = C::view_to_id(warehouse_id, &identifier, t.transaction()).await; // We can't fail before AuthZ;

    let view_id = authorizer
        .require_view_action(&request_metadata, view_id, CatalogViewAction::CanCommit)
        .await?;

    // ------------------- BUSINESS LOGIC -------------------
    validate_view_updates(updates)?;

    let namespace_id = C::namespace_to_id(warehouse_id, identifier.namespace(), t.transaction())
        .await?
        .ok_or(ErrorModel::not_found(
            "Namespace does not exist",
            "NamespaceNotFound",
            None,
        ))?;

    let GetWarehouseResponse {
        id: _,
        name: _,
        project_id: _,
        storage_profile,
        storage_secret_id,
        status,
        tabular_delete_profile: _,
    } = C::require_warehouse(warehouse_id, t.transaction()).await?;
    require_active_warehouse(status)?;

    check_asserts(requirements.as_ref(), view_id)?;

    let ViewMetadataWithLocation {
        metadata_location: before_update_metadata_location,
        metadata: before_update_metadata,
    } = C::load_view(view_id, false, t.transaction()).await?;
    let before_update_view_location = parse_view_location(before_update_metadata.location())?;
    let before_update_metadata_location = parse_view_location(&before_update_metadata_location)?;

    state
        .v1_state
        .contract_verifiers
        .check_view_updates(updates, &before_update_metadata)
        .await?
        .into_result()?;

    // serialize body before moving it
    let body = maybe_body_to_json(&request);

    let (requested_update_metadata, delete_old_location) = build_new_metadata(
        request,
        before_update_metadata,
        &before_update_view_location,
    )?;

    let view_location = parse_view_location(requested_update_metadata.location())?;
    let metadata_location = storage_profile.default_metadata_location(
        &view_location,
        &CompressionCodec::try_from_properties(requested_update_metadata.properties())?,
        Uuid::now_v7(),
        extract_count_from_metadata_location(&before_update_metadata_location).map_or(0, |v| v + 1),
    );

    if delete_old_location.is_some() {
        storage_profile.require_allowed_location(&view_location)?;
    }

    C::update_view_metadata(
        namespace_id,
        view_id,
        &identifier,
        &metadata_location,
        requested_update_metadata.clone(),
        &view_location,
        t.transaction(),
    )
    .await?;

    // We don't commit the transaction yet, first we need to write the metadata file.
    let storage_secret = if let Some(secret_id) = &storage_secret_id {
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

    let file_io = storage_profile.file_io(storage_secret.as_ref())?;
    write_metadata_file(
        &metadata_location,
        &requested_update_metadata,
        CompressionCodec::try_from_metadata(&requested_update_metadata)?,
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
            &metadata_location,
            // TODO: This should be a permission based on authz
            StoragePermissions::ReadWriteDelete,
        )
        .await?;
    t.commit().await?;

    if let Some(DeleteLocation(before_update_view_location)) = delete_old_location {
        // we discard the error since the operation went through and leaving a stale metadata file
        // seems acceptable, returning errors here will simply prompt a retry which means another
        // commit will be attempted that would still not clean this left-behind file up.
        tracing::debug!("Deleting old view location at: '{before_update_view_location}'");
        let _ = remove_all(&file_io, before_update_view_location)
            .await
            .inspect(|()| tracing::trace!("Deleted old view location"))
            .inspect_err(|e| tracing::error!("Failed to delete old view location: {e:?}"));
    }

    let _ = state
        .v1_state
        .publisher
        .publish(
            Uuid::now_v7(),
            "commitView",
            body,
            EventMetadata {
                tabular_id: TabularIdentUuid::View(*view_id),
                warehouse_id,
                name: identifier.name,
                namespace: identifier.namespace.to_url_string(),
                prefix: parameters
                    .prefix
                    .map(Prefix::into_string)
                    .unwrap_or_default(),
                num_events: 1,
                sequence_number: 0,
                trace_id: request_metadata.request_id(),
            },
        )
        .await;

    Ok(LoadViewResult {
        metadata_location: metadata_location.to_string(),
        metadata: requested_update_metadata,
        config: Some(config.config.into()),
    })
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
