use iceberg_ext::catalog::rest::ErrorModel;
use serde::{Deserialize, Serialize};

use super::user::{parse_create_user_request, CreateUserRequest, UserLastUpdatedWith, UserType};
use crate::{
    api::{management::v1::ApiServer, ApiContext},
    config,
    request_metadata::RequestMetadata,
    service::{
        authz::Authorizer, Actor, Catalog, Result, SecretStore, StartupValidationData, State,
        Transaction,
    },
    ProjectId, CONFIG, DEFAULT_PROJECT_ID,
};

#[derive(Debug, Deserialize, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
pub enum AuthZBackend {
    AllowAll,
    #[serde(rename = "openfga")]
    OpenFGA,
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct BootstrapRequest {
    /// Set to true if you accept LAKEKEEPER terms of use.
    pub accept_terms_of_use: bool,
    /// If set to true, the calling user is treated as an operator and obtain
    /// a corresponding role. If not specified, the user is treated as a human.
    #[serde(default)]
    pub is_operator: bool,
    /// Name of the user performing bootstrap. Optional. If not provided
    /// the server will try to parse the name from the provided token.
    /// The initial user will become the global admin.
    #[serde(default)]
    pub user_name: Option<String>,
    /// Email of the user performing bootstrap. Optional. If not provided
    /// the server will try to parse the email from the provided token.
    #[serde(default)]
    pub user_email: Option<String>,
    /// Type of the user performing bootstrap. Optional. If not provided
    /// the server will try to parse the type from the provided token.
    #[serde(default)]
    pub user_type: Option<UserType>,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct ServerInfo {
    /// Version of the server.
    pub version: String,
    /// Whether the catalog has been bootstrapped.
    pub bootstrapped: bool,
    /// ID of the server.
    pub server_id: uuid::Uuid,
    /// Default Project ID. Null if not set
    #[schema(value_type = Option::<String>)]
    pub default_project_id: Option<ProjectId>,
    /// `AuthZ` backend in use.
    pub authz_backend: AuthZBackend,
}

impl<C: Catalog, A: Authorizer, S: SecretStore> Service<C, A, S> for ApiServer<C, A, S> {}

#[async_trait::async_trait]
pub(crate) trait Service<C: Catalog, A: Authorizer, S: SecretStore> {
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
        let mut t = C::Transaction::begin_write(state.v1_state.catalog.clone()).await?;
        let success = C::bootstrap(request.accept_terms_of_use, t.transaction()).await?;
        if !success {
            return Err(ErrorModel::bad_request(
                "Catalog already bootstrapped",
                "CatalogAlreadyBootstrapped",
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
                t.transaction(),
            )
            .await?;
        }

        authorizer.bootstrap(&request_metadata, is_operator).await?;
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
                authorizer
                    .create_project(&request_metadata, default_project_id)
                    .await?;
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
            bootstrapped: server_data != StartupValidationData::NotBootstrapped,
            server_id: CONFIG.server_id,
            default_project_id: DEFAULT_PROJECT_ID.clone(),
            authz_backend: match CONFIG.authz_backend {
                config::AuthZBackend::AllowAll => AuthZBackend::AllowAll,
                config::AuthZBackend::OpenFGA => AuthZBackend::OpenFGA,
            },
        })
    }
}
