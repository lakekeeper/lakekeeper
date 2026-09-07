use std::sync::Arc;

use chrono::{DateTime, Utc};
use iceberg_ext::catalog::rest::ErrorModel;
use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

use super::user::{CreateUserRequest, UserLastUpdatedWith, UserType, parse_create_user_request};
use crate::{
    CONFIG, DEFAULT_PROJECT_ID,
    api::{ApiContext, management::v1::ApiServer},
    request_metadata::RequestMetadata,
    service::{
        Actor, ArcProjectId, CatalogStore, Result, SecretStore, State, Transaction, UserUpsertMode,
        authz::{Authorizer, GrantResource, emit_bootstrap_grants_async, write_bootstrap_grants},
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

/// Default `BuildInfo` used when a binary does not inject one.
///
/// Callers that want to surface commit SHAs, an enterprise edition version, or
/// console information via the `/management/v1/info` endpoint must provide a
/// custom `BuildInfo` via `ServeConfiguration::build_info`.
pub static DEFAULT_BUILD_INFO: std::sync::LazyLock<BuildInfo> =
    std::sync::LazyLock::new(BuildInfo::default);

/// Information about the UI (console) shipped with this binary.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct ConsoleInfo {
    /// Edition / crate name of the bundled console.
    /// e.g. `lakekeeper-console` for the OSS console or
    /// `lakekeeper-console-plus` for the enterprise console.
    pub edition: String,
    /// SemVer of the console crate.
    pub version: String,
    /// Git commit SHA of the console source, if known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub commit_sha: Option<String>,
}

/// Build-time information injected by the binary.
///
/// All fields are optional: the OSS `lakekeeper` binary leaves them empty, while
/// downstream distributions populate them from their
/// build scripts to expose upstream + enterprise versions, commit SHAs, and
/// console details via the server-info endpoint.
#[derive(Debug, Clone, Default)]
pub struct BuildInfo {
    /// Git commit SHA of the upstream `lakekeeper` dependency, if known.
    pub lakekeeper_commit_sha: Option<String>,
    /// SemVer of the enterprise binary, if this is an
    /// enterprise build.
    pub lakekeeper_enterprise_version: Option<String>,
    /// Git commit SHA of the enterprise binary, if known.
    pub lakekeeper_enterprise_commit_sha: Option<String>,
    /// Bundled console, if any.
    pub console: Option<ConsoleInfo>,
}

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
    /// Deprecated alias of `lakekeeper-version`. Always equal to it; kept
    /// for clients that read the plain `version` field. New clients should
    /// read `lakekeeper-version` and/or `lakekeeper-enterprise-version`.
    #[cfg_attr(feature = "open-api", schema(deprecated = true))]
    pub version: String,
    /// SemVer of the upstream `lakekeeper` crate the server was built
    /// against.
    pub lakekeeper_version: String,
    /// Git commit SHA of the upstream `lakekeeper` crate, if the binary
    /// reported it at build time.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lakekeeper_commit_sha: Option<String>,
    /// SemVer of the enterprise binary (e.g. `lakekeeper-plus`) when this
    /// server is an enterprise build. `None` on OSS builds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lakekeeper_enterprise_version: Option<String>,
    /// Git commit SHA of the enterprise binary, if known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lakekeeper_enterprise_commit_sha: Option<String>,
    /// Information about the bundled console (UI), if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub console: Option<ConsoleInfo>,
    /// Whether the catalog has been bootstrapped.
    pub bootstrapped: bool,
    /// ID of the server.
    /// Returns null if the catalog has not been bootstrapped.
    pub server_id: uuid::Uuid,
    /// Default Project ID. Null if not set
    #[cfg_attr(feature = "open-api", schema(value_type = Option::<String>))]
    pub default_project_id: Option<ArcProjectId>,
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
    /// Role-provider namespaces whose roles are maintained by a configured role
    /// provider (LDAP/Entra/Okta/token), sorted. Roles whose `provider-id`
    /// appears here are the provider's to maintain: creating one, or renaming,
    /// re-describing, rebinding, or deleting an existing one, is rejected with
    /// `ManagedRoleImmutable` so provider sync cannot be clobbered.
    ///
    /// This is live server configuration, not a property of the namespace
    /// string. A provider removed from config drops out of the list, and the
    /// roles it left behind become renamable and deletable again so they can be
    /// cleaned up.
    ///
    /// Empty when no role provider is configured. Two namespaces never appear,
    /// and a client gating on this list must handle both itself: `lakekeeper`,
    /// which is always writable, and the reserved `system`, whose roles reject
    /// the same mutations with `SystemRoleImmutable`.
    ///
    /// **Membership is gated differently — do not derive it from this list.**
    /// Adding or removing a role's members requires the `lakekeeper` namespace
    /// (or `system`, for an instance admin); every other namespace is refused
    /// with `RoleNotManuallyAssignable` whether or not it appears here. So an
    /// orphaned role from a since-removed provider is renamable and deletable
    /// but still not assignable — gate member editing on
    /// `provider-id == "lakekeeper"`, not on absence from this list.
    pub managed_role_providers: Vec<String>,
    /// License status information
    pub license_status: LicenseStatus,
}

/// The authorizer's provider-managed namespaces as a sorted list, for
/// [`ServerInfo::managed_role_providers`].
///
/// Sorted because the source is a `HashSet` with no inherent order: without this
/// the same server would emit different orderings across restarts and replicas,
/// which reads as a configuration change to any client diffing the response.
fn managed_role_providers_sorted<A: Authorizer>(authorizer: &A) -> Vec<String> {
    let mut ids: Vec<String> = authorizer
        .managed_role_provider_ids()
        .iter()
        .map(ToString::to_string)
        .collect();
    ids.sort_unstable();
    ids
}

impl<C: CatalogStore, A: Authorizer, S: SecretStore> Service<C, A, S> for ApiServer<C, A, S> {}

#[async_trait::async_trait]
pub trait Service<C: CatalogStore, A: Authorizer, S: SecretStore> {
    #[allow(clippy::too_many_lines)]
    async fn bootstrap(
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
        request: BootstrapRequest,
    ) -> Result<()> {
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
        authorizer.can_bootstrap(&request_metadata).await?;

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
            C::create_or_update_user(
                &creation_user_id,
                &name,
                email.as_deref(),
                UserLastUpdatedWith::UpdateEndpoint,
                user_type,
                UserUpsertMode::Overwrite,
                t.transaction(),
            )
            .await?;
        }

        authorizer.bootstrap(&request_metadata, is_operator).await?;

        // The one grant resource nothing creates: the server comes into existence here,
        // in this transaction, and the bootstrapping user's row is written above it.
        let server_grants = write_bootstrap_grants::<C, A>(
            &authorizer,
            &request_metadata,
            &GrantResource::Server,
            t.transaction(),
        )
        .await?;
        t.commit().await?;

        emit_bootstrap_grants_async(
            &state.v1_state.events,
            Arc::new(request_metadata.clone()),
            server_grants,
        );

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
                    .await?;
                // Bootstrapping without authentication has no acting identity, so the
                // default project starts with no owner. The helper answers that.
                let bootstrap_grants = write_bootstrap_grants::<C, A>(
                    &authorizer,
                    &request_metadata,
                    &GrantResource::Project((**default_project_id).clone()),
                    t.transaction(),
                )
                .await?;
                t.commit().await?;

                emit_bootstrap_grants_async(
                    &state.v1_state.events,
                    Arc::new(request_metadata.clone()),
                    bootstrap_grants,
                );
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
        let lakekeeper_version = env!("CARGO_PKG_VERSION").to_string();
        let server_data = C::get_server_info(state.v1_state.catalog).await?;
        let build_info = state.v1_state.build_info;

        Ok(ServerInfo {
            version: lakekeeper_version.clone(),
            lakekeeper_version,
            lakekeeper_commit_sha: build_info.lakekeeper_commit_sha.clone(),
            lakekeeper_enterprise_version: build_info.lakekeeper_enterprise_version.clone(),
            lakekeeper_enterprise_commit_sha: build_info.lakekeeper_enterprise_commit_sha.clone(),
            console: build_info.console.clone(),
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
            managed_role_providers: managed_role_providers_sorted(&state.v1_state.authz),
            license_status: state.v1_state.license_status.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::managed_role_providers_sorted;
    use crate::service::{RoleProviderId, authz::tests::HidingAuthorizer};

    /// The list must mirror the authorizer's deny-set, sorted. A client gates its
    /// role-editing affordances on this, so a drift between the two would offer
    /// writes the management API then rejects.
    #[test]
    fn managed_role_providers_mirror_the_authorizer_sorted() {
        let pid = |s: &str| RoleProviderId::try_new(s).expect("valid provider id");

        // No role providers — the shape every OSS authorizer produces.
        assert!(managed_role_providers_sorted(&HidingAuthorizer::new()).is_empty());

        // Non-empty: inserted out of order, reported in order.
        let authorizer = HidingAuthorizer::new().with_managed_role_providers([
            pid("okta"),
            pid("corporate-ldap"),
            pid("entra"),
        ]);
        assert_eq!(
            managed_role_providers_sorted(&authorizer),
            vec![
                "corporate-ldap".to_string(),
                "entra".to_string(),
                "okta".to_string()
            ],
        );
    }
}
