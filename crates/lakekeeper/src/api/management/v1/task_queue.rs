use iceberg_ext::catalog::rest::ErrorModel;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::{
    ProjectId, WarehouseId,
    api::{ApiContext, Result},
    service::{
        CatalogStore, CatalogTaskOps, SecretStore, State, Transaction, authz::Authorizer, task_configs::TaskQueueConfigFilter, tasks::TaskQueueName
    },
};

#[derive(Debug, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct SetTaskQueueConfigRequest {
    pub queue_config: QueueConfig,
    pub max_seconds_since_last_heartbeat: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(transparent)]
pub struct QueueConfig(pub(crate) serde_json::Value);

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct GetTaskQueueConfigResponse {
    pub queue_config: QueueConfigResponse,
    pub max_seconds_since_last_heartbeat: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct QueueConfigResponse {
    #[serde(flatten)]
    pub(crate) config: serde_json::Value,
    #[cfg_attr(feature = "open-api", schema(value_type=String))]
    pub(crate) queue_name: TaskQueueName,
}

impl axum::response::IntoResponse for GetTaskQueueConfigResponse {
    fn into_response(self) -> axum::http::Response<axum::body::Body> {
        (http::StatusCode::OK, axum::Json(self)).into_response()
    }
}

pub(crate) async fn set_task_queue_config<
    C: CatalogStore,
    A: Authorizer,
    S: SecretStore,
>(
    project_id: ProjectId,
    warehouse_id: Option<WarehouseId>,
    queue_name: &TaskQueueName,
    request: SetTaskQueueConfigRequest,
    context: ApiContext<State<A, C, S>>,
) -> Result<()> {
    let task_queues = context.v1_state.registered_task_queues;

    if let Some(validate_config_fn) = task_queues.validate_config_fn(queue_name).await {
        validate_config_fn(request.queue_config.0.clone()).map_err(|e| {
            ErrorModel::bad_request(
                format!("Failed to deserialize queue config for queue-name '{queue_name}': '{e}'"),
                "InvalidQueueConfig",
                Some(Box::new(e)),
            )
        })?;
    } else {
        let mut existing_queue_names = task_queues.queue_names().await;
        existing_queue_names.sort_unstable();
        let existing_queue_names = existing_queue_names.iter().join(", ");
        return Err(ErrorModel::bad_request(
            format!("Queue '{queue_name}' not found! Existing queues: [{existing_queue_names}]"),
            "QueueNotFound",
            None,
        )
        .into());
    }
    let mut transaction = C::Transaction::begin_write(context.v1_state.catalog).await?;
    C::set_task_queue_config(
        project_id,
        warehouse_id,
        queue_name,
        request,
        transaction.transaction(),
    )
    .await?;
    transaction.commit().await?;
    Ok(())
}

pub(crate) async fn get_task_queue_config<
    C: CatalogStore,
    A: Authorizer,
    S: SecretStore,
>(
    filter: &TaskQueueConfigFilter,
    queue_name: &TaskQueueName,
    context: ApiContext<State<A, C, S>>
) -> Result<GetTaskQueueConfigResponse> {
    let config = C::get_task_queue_config(
        filter,
        queue_name,
        context.v1_state.catalog,
    )
    .await?
    .unwrap_or_else(|| GetTaskQueueConfigResponse {
        queue_config: QueueConfigResponse {
            config: serde_json::json!({}),
            queue_name: queue_name.clone(),
        },
        max_seconds_since_last_heartbeat: None,
    });
    Ok(config)
}
