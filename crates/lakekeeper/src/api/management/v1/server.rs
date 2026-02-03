use chrono::{DateTime, Utc};
use iceberg_ext::catalog::rest::ErrorModel;
use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

use super::user::{CreateUserRequest, UserLastUpdatedWith, UserType, parse_create_user_request};
use crate::{
    CONFIG, DEFAULT_PROJECT_ID, ProjectId,
    api::{ApiContext, management::v1::ApiServer},
    logging::audit::{
        AuditContext,
        events::{AccessEndpointEvent, BootstrapCreateUserEvent, BootstrapFailedEvent},
    },
    request_metadata::RequestMetadata,
    service::{
        Actor, CatalogStore, Result, SecretStore, State, Transaction,
        authz::Authorizer,
        tasks::{
            ScheduleTaskMetadata, TaskEntity,
            task_log_cleanup_queue::{self, TaskLogCleanupPayload, TaskLogCleanupTask},
        },
    },
};

#[derive(Debug, Deserialize, TypedBuilder)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct BootstrapRequest {
    /// Set to true if you accept LAKEKEEPER terms of use.
    #[builder(setter(strip_bool))]
    pub accept_terms_of_use: bool,
    /// If set to true, the calling user is treated as an operator and obtain
    /// a corresponding role. If not specified, the user is treated as a human.
    #[serde(default)]
    #[builder(setter(strip_bool))]
    pub is_operator: bool,
    /// Name of the user performing bootstrap. Optional. If not provided
    /// the server will try to parse the name from the provided token.
    /// The initial user will become the global admin.
    #[serde(default)]
    #[builder(default, setter(strip_option))]
    pub user_name: Option<String>,
    /// Email of the user performing bootstrap. Optional. If not provided
    /// the server will try to parse the email from the provided token.
    #[serde(default)]
    #[builder(default, setter(strip_option))]
    pub user_email: Option<String>,
    /// Type of the user performing bootstrap. Optional. If not provided
    /// the server will try to parse the type from the provided token.
    #[serde(default)]
    #[builder(default, setter(strip_option))]
    pub user_type: Option<UserType>,
}

pub static APACHE_LICENSE_STATUS: std::sync::LazyLock<LicenseStatus> =
    std::sync::LazyLock::new(|| LicenseStatus {
        issuer: None,
        audience: Some("lakekeeper-core".to_string()),
        license_type: "Apache-2.0".to_string(),
        valid: true,
        customer: None,
        expiration: None,
        error: None,
        license_id: None,
    });

/// Status of license validation
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct LicenseStatus {
    /// Organization or entity that issued the license for Lakekeeper
    pub issuer: Option<String>,
    /// Audience or entity the license is issued to
    pub audience: Option<String>,
    /// License type (e.g., "Apache-2.0", "Vakamo-Enterprise", etc.)
    pub license_type: String,
    /// If the license is valid and active
    pub valid: bool,
    /// Customer name the license is issued to (None for open source)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub customer: Option<String>,
    /// License expiration date (None for perpetual licenses like Apache)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expiration: Option<DateTime<Utc>>,
    /// Any validation error that occurred
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// License ID or identifier
    #[serde(skip_serializing_if = "Option::is_none")]
    pub license_id: Option<String>,
}

#[derive(Debug, Serialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
#[allow(clippy::struct_excessive_bools)]
pub struct ServerInfo {
    /// Version of the server.
    pub version: String,
    /// Whether the catalog has been bootstrapped.
    pub bootstrapped: bool,
    /// ID of the server.
    /// Returns null if the catalog has not been bootstrapped.
    pub server_id: uuid::Uuid,
    /// Default Project ID. Null if not set
    #[cfg_attr(feature = "open-api", schema(value_type = Option::<String>))]
    pub default_project_id: Option<ProjectId>,
    /// `AuthZ` backend in use.
    pub authz_backend: String,
    /// If using AWS system identities for S3 storage profiles are enabled.
    pub aws_system_identities_enabled: bool,
    /// If using Azure system identities for Azure storage profiles are enabled.
    pub azure_system_identities_enabled: bool,
    /// If using GCP system identities for GCS storage profiles are enabled.
    pub gcp_system_identities_enabled: bool,
    /// List of queues that are registered for the server.
    pub queues: Vec<String>,
    /// License status information
    pub license_status: LicenseStatus,
}

impl<C: CatalogStore, A: Authorizer, S: SecretStore> Service<C, A, S> for ApiServer<C, A, S> {}

#[async_trait::async_trait]
pub(crate) trait Service<C: CatalogStore, A: Authorizer, S: SecretStore> {
    async fn bootstrap(
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        request: BootstrapRequest,
    ) -> Result<()> {
        request_metadata.log_audit(AccessEndpointEvent {
            endpoint: "/management/v1/bootstrap".to_string(),
            method: request_metadata.request_method().to_string(),
        });
        let BootstrapRequest {
            user_name,
            user_email,
            user_type,
            accept_terms_of_use,
            is_operator,
        } = request;

        if !accept_terms_of_use {
            return Err(ErrorModel::builder()
                .code(http::StatusCode::BAD_REQUEST.into())
                .message("You must accept the terms of use to bootstrap the catalog.".to_string())
                .r#type("TermsOfUseNotAccepted".to_string())
                .build()
                .into());
        }

        // ------------------- AUTHZ -------------------
        // We check at two places if we can bootstrap: AuthZ and the catalog.
        // AuthZ just checks if the request metadata could be added as the servers
        // global admin
        let authorizer = state.v1_state.authz;
        authorizer
            .can_bootstrap(&request_metadata)
            .await
            .inspect_err(|error| {
                request_metadata.log_audit(BootstrapFailedEvent {
                    error: error.to_string(),
                })
            })?;

        // ------------------- Business Logic -------------------
        let server_info = C::get_server_info(state.v1_state.catalog.clone()).await?;
        let open_for_bootstrap = server_info.is_open_for_bootstrap();

        if !open_for_bootstrap {
            return Err(ErrorModel::bad_request(
                "Catalog is not open for bootstrap",
                "CatalogAlreadyBootstrapped",
                None,
            )
            .into());
        }

        let mut t = C::Transaction::begin_write(state.v1_state.catalog.clone()).await?;
        let success = C::bootstrap(accept_terms_of_use, t.transaction()).await?;
        if !success {
            return Err(ErrorModel::bad_request(
                "Concurrent bootstrap detected, catalog already bootstrapped",
                "ConcurrentBootstrap",
                None,
            )
            .into());
        }

        // Create user in the catalog
        if request_metadata.is_authenticated() {
            let (creation_user_id, name, user_type, email) = parse_create_user_request(
                &request_metadata,
                Some(CreateUserRequest {
                    name: user_name.clone(),
                    email: user_email.clone(),
                    user_type,
                    id: None,
                    update_if_exists: false, // Ignored in `parse_create_user_request`
                }),
            )?;
            request_metadata.log_audit(BootstrapCreateUserEvent {
                user_name: name.clone(),
                user_id: creation_user_id.clone(),
                user_type: user_type,
                user_email: email.clone().unwrap_or_default(),
            });
            C::create_or_update_user(
                &creation_user_id,
                &name,
                email.as_deref(),
                UserLastUpdatedWith::UpdateEndpoint,
                user_type,
                t.transaction(),
            )
            .await?;
        }

        authorizer
            .bootstrap(&request_metadata, is_operator)
            .await
            .inspect_err(|error| {
                request_metadata.log_audit(BootstrapFailedEvent {
                    error: error.to_string(),
                })
            })?;
        t.commit().await?;

        // If default project is specified, and the project does not exist, create it
        if let Some(default_project_id) = DEFAULT_PROJECT_ID.as_ref() {
            let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;
            let p = C::get_project(default_project_id, t.transaction()).await?;
            if p.is_none() {
                C::create_project(
                    default_project_id,
                    "Default Project".to_string(),
                    t.transaction(),
                )
                .await?;
                TaskLogCleanupTask::schedule_task::<C>(
                    ScheduleTaskMetadata {
                        project_id: default_project_id.clone(),
                        parent_task_id: None,
                        scheduled_for: None,
                        entity: TaskEntity::Project,
                    },
                    TaskLogCleanupPayload::new(),
                    t.transaction(),
                )
                .await
                .map_err(|e| {
                    e.append_detail(format!(
                        "Failed to queue `{}` task for new project with id {default_project_id}.",
                        task_log_cleanup_queue::QUEUE_NAME.as_str(),
                    ))
                })?;
                authorizer
                    .create_project(&request_metadata, default_project_id)
                    .await
                    .inspect_err(|error| {
                        request_metadata.log_audit(BootstrapFailedEvent {
                            error: error.to_string(),
                        })
                    })?;
                t.commit().await?;
            }
        }

        Ok(())
    }

    async fn server_info(
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<ServerInfo> {
        match request_metadata.actor() {
            Actor::Anonymous => {
                if CONFIG.authn_enabled() {
                    return Err(ErrorModel::unauthorized(
                        "Authentication required",
                        "AuthenticationRequired",
                        None,
                    )
                    .into());
                }
            }
            Actor::Principal(_) | Actor::Role { .. } => (),
        }

        // ------------------- Business Logic -------------------
        let version = env!("CARGO_PKG_VERSION").to_string();
        let server_data = C::get_server_info(state.v1_state.catalog).await?;

        Ok(ServerInfo {
            version,
            bootstrapped: !server_data.is_open_for_bootstrap(),
            server_id: *server_data.server_id(),
            default_project_id: DEFAULT_PROJECT_ID.clone(),
            authz_backend: A::implementation_name().to_string(),
            aws_system_identities_enabled: CONFIG.enable_aws_system_credentials,
            azure_system_identities_enabled: CONFIG.enable_azure_system_credentials,
            gcp_system_identities_enabled: CONFIG.enable_gcp_system_credentials,
            queues: {
                let mut names = state.v1_state.registered_task_queues.queue_names().await;
                names.sort_unstable();
                names.into_iter().map(ToString::to_string).collect()
            },
            license_status: state.v1_state.license_status.clone(),
        })
    }
}
