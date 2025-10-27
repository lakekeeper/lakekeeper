use std::{str::FromStr as _, sync::Arc};

use iceberg::spec::{ViewFormatVersion, ViewMetadata, ViewMetadataBuilder};
use iceberg_ext::catalog::{rest::ViewUpdate, ViewRequirement};
use lakekeeper_io::Location;
use uuid::Uuid;

use crate::{
    api::iceberg::v1::{
        ApiContext, CommitViewRequest, DataAccessMode, ErrorModel, LoadViewResult, Result,
        ViewParameters,
    },
    request_metadata::RequestMetadata,
    server::{
        compression_codec::CompressionCodec,
        io::{remove_all, write_file},
        require_warehouse_id,
        tables::{
            determine_table_ident, extract_count_from_metadata_location, require_active_warehouse,
            validate_table_or_view_ident, MAX_RETRIES_ON_CONCURRENT_UPDATE,
        },
        views::validate_view_updates,
    },
    service::{
        authz::{AuthZViewOps, Authorizer, AuthzWarehouseOps, CatalogViewAction},
        contract_verification::ContractVerification,
        secrets::SecretStore,
        storage::{StorageLocations as _, StoragePermissions, StorageProfile},
        AuthZViewInfo, CatalogStore, CatalogTabularOps, CatalogView, CatalogViewOps,
        CatalogWarehouseOps, InternalParseLocationError, State, TabularListFlags, Transaction,
        ViewCommit, ViewId, ViewInfo, CONCURRENT_UPDATE_ERROR_TYPE,
    },
    SecretId,
};

/// Commit updates to a view
// TODO: break up into smaller fns
#[allow(clippy::too_many_lines)]
pub(crate) async fn commit_view<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    parameters: ViewParameters,
    request: CommitViewRequest,
    state: ApiContext<State<A, C, S>>,
    data_access: impl Into<DataAccessMode>,
    request_metadata: RequestMetadata,
) -> Result<LoadViewResult> {
    let data_access = data_access.into();
    // ------------------- VALIDATIONS -------------------
    let warehouse_id = require_warehouse_id(parameters.prefix.as_ref())?;

    let CommitViewRequest {
        identifier,
        requirements,
        updates,
    } = &request;

    let view_ident = determine_table_ident(&parameters.view, identifier.as_ref())?;
    validate_table_or_view_ident(&view_ident)?;
    validate_view_updates(updates)?;

    // ------------------- AUTHZ -------------------
    let authorizer = state.v1_state.authz.clone();

    authorizer
        .require_warehouse_use(&request_metadata, warehouse_id)
        .await?;

    let view_info = C::get_view_info(
        warehouse_id,
        view_ident.clone(),
        TabularListFlags::active(),
        state.v1_state.catalog.clone(),
    )
    .await;

    let view_info = authorizer
        .require_view_action(
            &request_metadata,
            warehouse_id,
            view_ident,
            view_info,
            CatalogViewAction::CanCommit,
        )
        .await?;

    // ------------------- BUSINESS LOGIC -------------------
    // Verify assertions
    check_requirements(requirements.as_ref(), view_info.view_id())?;

    let warehouse =
        C::require_warehouse_by_id(warehouse_id, state.v1_state.catalog.clone()).await?;
    let storage_profile = &warehouse.storage_profile;
    let storage_secret_id = warehouse.storage_secret_id;
    require_active_warehouse(warehouse.status)?;

    // Start the retry loop
    let request = Arc::new(request);
    let mut attempt = 0;
    loop {
        let result = try_commit_view::<C, A, S>(
            CommitViewContext {
                view_info: &view_info,
                storage_profile,
                storage_secret_id,
                request: request.as_ref(),
                data_access,
            },
            &state,
            &request_metadata,
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
                // Short jittered exponential backoff to reduce contention
                // First delay: 50ms, then 100ms, 200ms, ..., up to 3200ms (50*2^6)
                let exp = u32::try_from(attempt.saturating_sub(1).min(6)).unwrap_or(6); // cap growth explicitly
                let base = 50u64.saturating_mul(1u64 << exp);
                let jitter = fastrand::u64(..base / 2);
                tracing::debug!(attempt, base, jitter, "Concurrent update backoff");
                tokio::time::sleep(std::time::Duration::from_millis(base + jitter)).await;
            }
            Err(e) => return Err(e),
        }
    }
}

// Context structure to hold static parameters for retry function
struct CommitViewContext<'a> {
    view_info: &'a ViewInfo,
    storage_profile: &'a StorageProfile,
    storage_secret_id: Option<SecretId>,
    request: &'a CommitViewRequest,
    data_access: DataAccessMode,
}

// Core commit logic that may be retried
#[allow(clippy::too_many_lines)]
async fn try_commit_view<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    ctx: CommitViewContext<'_>,
    state: &ApiContext<State<A, C, S>>,
    request_metadata: &RequestMetadata,
) -> Result<(LoadViewResult, crate::service::endpoint_hooks::ViewCommit)> {
    let mut t = C::Transaction::begin_write(state.v1_state.catalog.clone()).await?;

    // These operations need fresh data on each retry
    let previous_view = C::load_view(
        ctx.view_info.warehouse_id,
        ctx.view_info.tabular_id,
        false,
        t.transaction(),
    )
    .await?;

    let previous_view_location = Location::from_str(previous_view.metadata.location())
        .map_err(InternalParseLocationError::from)?;
    let previous_metadata_location = previous_view.metadata_location.clone();

    state
        .v1_state
        .contract_verifiers
        .check_view_updates(&ctx.request.updates, &previous_view.metadata)
        .await?
        .into_result()?;

    let (new_metadata, delete_old_location) = build_new_metadata(
        ctx.request.clone(),
        (*previous_view.metadata).clone(),
        &previous_view_location,
    )?;
    let new_metadata = Arc::new(new_metadata);

    let new_location =
        Location::from_str(new_metadata.location()).map_err(InternalParseLocationError::from)?;
    let metadata_location = ctx.storage_profile.default_metadata_location(
        &new_location,
        &CompressionCodec::try_from_properties(new_metadata.properties())?,
        Uuid::now_v7(),
        extract_count_from_metadata_location(&previous_metadata_location).map_or(0, |v| v + 1),
    );

    if delete_old_location.is_some() {
        ctx.storage_profile
            .require_allowed_location(&new_location)?;
    }

    let new_view = CatalogView {
        metadata: new_metadata.clone(),
        metadata_location,
        location: new_location,
        warehouse_updated_at: previous_view.warehouse_updated_at,
    };

    C::commit_view(
        ViewCommit {
            view_ident: &ctx.view_info.tabular_ident,
            previous_view: &previous_view,
            namespace_id: ctx.view_info.namespace_id,
            warehouse_id: ctx.view_info.warehouse_id,
            new_view: &new_view,
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
    write_file(
        &file_io,
        &new_view.metadata_location,
        &new_metadata,
        CompressionCodec::try_from_metadata(&new_metadata)?,
    )
    .await?;

    tracing::debug!(
        "Wrote new view metadata file to: '{}'",
        new_view.metadata_location
    );

    // Generate config for client
    let config = ctx
        .storage_profile
        .generate_table_config(
            ctx.data_access,
            storage_secret.as_ref(),
            &new_view.metadata_location,
            StoragePermissions::ReadWriteDelete,
            request_metadata,
            ctx.view_info.warehouse_id,
            ctx.view_info.tabular_id.into(),
        )
        .await?;

    // Commit transaction
    t.commit().await?;

    // Handle file cleanup after transaction is committed
    if let Some(DeleteLocation(before_update_view_location)) = delete_old_location {
        tracing::debug!("Deleting old view location at: '{before_update_view_location}'");
        let _ = remove_all(&file_io, before_update_view_location)
            .await
            .inspect(|()| {
                tracing::debug!("Deleted old view location {before_update_view_location}");
            })
            .inspect_err(|e| {
                tracing::error!(
                    "Failed to delete old view location '{before_update_view_location}': {e:?}"
                );
            });
    }

    Ok((
        LoadViewResult {
            metadata_location: new_view.metadata_location.to_string(),
            metadata: new_metadata.clone(),
            config: Some(config.config.into()),
        },
        crate::service::endpoint_hooks::ViewCommit {
            old_metadata: previous_view.metadata,
            new_metadata,
            old_metadata_location: previous_metadata_location,
            new_metadata_location: new_view.metadata_location,
        },
    ))
}

fn check_requirements(requirements: Option<&Vec<ViewRequirement>>, view_id: ViewId) -> Result<()> {
    if let Some(requirements) = requirements {
        for assertion in requirements {
            match assertion {
                ViewRequirement::AssertViewUuid(req) => {
                    if req.uuid != *view_id {
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
                        format!("Error setting current view version: {e}"),
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
        server::views::{create::test::create_view, test::setup},
        tests::create_view_request,
        WarehouseId,
    };

    #[sqlx::test]
    async fn test_commit_view(pool: PgPool) {
        let (api_context, namespace, whi) = setup(pool, None).await;
        let prefix = whi.to_string();
        let view_name = "myview";
        let view = Box::pin(create_view(
            api_context.clone(),
            namespace.clone(),
            create_view_request(Some(view_name), None),
            Some(prefix.clone()),
        ))
        .await
        .unwrap();

        let rq: CommitViewRequest = spark_commit_update_request(whi, Some(view.metadata.uuid()));

        let res = Box::pin(super::commit_view(
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
        ))
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
        let _ = Box::pin(create_view(
            api_context.clone(),
            namespace.clone(),
            create_view_request(Some(view_name), None),
            Some(prefix.clone()),
        ))
        .await
        .unwrap();

        let rq: CommitViewRequest = spark_commit_update_request(whi, Some(Uuid::now_v7()));

        let err = Box::pin(super::commit_view(
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
        ))
        .await
        .expect_err("This unexpectedly didn't fail the uuid assertion.");
        assert_eq!(err.error.code, 400);
        assert_eq!(err.error.r#type, "ViewUuidMismatch");
    }

    fn spark_commit_update_request(
        warehouse_id: WarehouseId,
        asserted_uuid: Option<Uuid>,
    ) -> CommitViewRequest {
        let uuid = asserted_uuid.map_or("019059cb-9277-7ff0-b71a-537df05b33f8".into(), |u| {
            u.to_string()
        });
        serde_json::from_value(json!({
  "requirements": [
    {
      "type": "assert-view-uuid",
      "warehouse-uuid": *warehouse_id,
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
