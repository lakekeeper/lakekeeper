//! Management API routes that a host binary adds to Lakekeeper.

use std::{
    collections::{BTreeSet, HashSet},
    fmt::Debug,
    sync::Arc,
};

use axum::{
    handler::Handler,
    routing::{MethodFilter, MethodRouter, on},
};
use strum::IntoEnumIterator;

pub use crate::api::endpoints::ExtensionEndpoint;
use crate::{
    api::{
        ApiContext,
        endpoints::{Endpoint, ExtensionEndpoints, PERMISSIONS_PREFIX},
    },
    service::{CatalogStore, SecretStore, State, authz::Authorizer, tasks::QueueApiConfig},
};

const MANAGEMENT_V1_PREFIX: &str = "/management/v1/";

/// Reference to [`ManagementApiExtension`] that can be cheaply cloned and shared.
pub type ArcManagementApiExtension<A, C, S> = Arc<dyn ManagementApiExtension<A, C, S>>;

/// Adds routes to the management API, behind the same layers as Lakekeeper's management routes,
/// authentication included. Each handler authorizes and audits its request, with [`APIEventContext`]
/// and its [`emit_authz`]. Every router built in one process, e.g. a test binary, uses one set.
///
/// [`APIEventContext`]: crate::service::events::APIEventContext
/// [`emit_authz`]: crate::service::events::APIEventContext::emit_authz
pub trait ManagementApiExtension<A: Authorizer, C: CatalogStore, S: SecretStore>:
    Debug + Send + Sync + 'static
{
    /// Declares the extension's endpoints with their handlers.
    fn routes(&self, routes: ExtensionRoutes<A, C, S>) -> ExtensionRoutes<A, C, S>;

    /// Documents exactly the routed endpoints. Merged into the management `OpenAPI` document.
    #[cfg(feature = "open-api")]
    fn api_doc(&self) -> utoipa::openapi::OpenApi;
}

type ApiState<A, C, S> = ApiContext<State<A, C, S>>;

type DeclaredRoute<A, C, S> = (&'static ExtensionEndpoint, MethodRouter<ApiState<A, C, S>>);

type ExtensionRouter<A, C, S> = axum::Router<ApiState<A, C, S>>;

/// The endpoints an extension declares in [`ManagementApiExtension::routes`].
pub struct ExtensionRoutes<A: Authorizer, C: CatalogStore, S: SecretStore> {
    routed: Vec<DeclaredRoute<A, C, S>>,
}

impl<A: Authorizer, C: CatalogStore, S: SecretStore> Debug for ExtensionRoutes<A, C, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list()
            .entries(self.routed.iter().map(|(endpoint, _)| endpoint))
            .finish()
    }
}

impl<A: Authorizer, C: CatalogStore, S: SecretStore> ExtensionRoutes<A, C, S> {
    /// Serves `endpoint` with `handler`. A GET endpoint also serves HEAD requests, unless
    /// a HEAD endpoint for the same path exists.
    ///
    /// # Panics
    /// If axum cannot route the endpoint's method. A `static` can only use the constant
    /// `Method`s, and axum routes all of them.
    #[must_use]
    pub fn route<H, T>(mut self, endpoint: &'static ExtensionEndpoint, handler: H) -> Self
    where
        H: Handler<T, ApiState<A, C, S>>,
        T: 'static,
    {
        let filter = MethodFilter::try_from(endpoint.method().clone())
            .expect("axum routes every constant `Method`");
        self.routed.push((endpoint, on(filter, handler)));
        self
    }
}

/// Why the server cannot serve the management API extensions.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ManagementApiExtensionError {
    #[error(
        "Extension endpoint `{name}`: a name is `ext-` followed by words of lower-case letters and digits, joined by single hyphens"
    )]
    InvalidName { name: &'static str },
    #[error("Extension endpoint `{name}` is declared more than once")]
    DuplicateName { name: &'static str },
    #[error("Extension endpoint `{name}`: path `{path}` {rule}")]
    InvalidPath {
        name: &'static str,
        path: &'static str,
        rule: &'static str,
    },
    #[error("Extension endpoint `{name}`: path `{path}` overlaps Lakekeeper's `{lakekeeper_path}`")]
    OverlapsLakekeeperPath {
        name: &'static str,
        path: &'static str,
        lakekeeper_path: &'static str,
    },
    #[error(
        "Extension endpoint `{name}`: parameter `{{{parameter}}}` of `{path}` must be named `{{{expected}}}`, as in Lakekeeper's `{lakekeeper_path}`"
    )]
    ParameterName {
        name: &'static str,
        path: &'static str,
        parameter: &'static str,
        expected: &'static str,
        lakekeeper_path: &'static str,
    },
    #[error("`{method} {path}` is routed but missing from the extension's OpenAPI document")]
    Undocumented { method: String, path: String },
    #[error(
        "The OpenAPI document of an extension describes `{method} {path}`, which the extension does not route"
    )]
    NotRouted { method: String, path: String },
    #[error("The OpenAPI documents of the extensions change {part} of Lakekeeper's document")]
    ChangesLakekeeperDocument { part: String },
    #[error(
        "The OpenAPI component `{kind}/{component}` of an extension conflicts with a component of that name in Lakekeeper's document"
    )]
    ConflictingComponent { kind: String, component: String },
    #[error("The management OpenAPI document has more than one operation `{operation_id}`")]
    DuplicateOperationId { operation_id: String },
    #[error("The management OpenAPI document has more than one tag `{tag}`")]
    DuplicateTag { tag: String },
    #[error("A different set of extension endpoints is already installed in this process")]
    DifferentEndpointsInstalled,
}

const NOT_UNDER_MANAGEMENT_V1: &str = "does not start with `/management/v1/`";
const UNDER_PERMISSIONS: &str =
    "starts with `/management/v1/permissions`, which the authorizer serves";

/// Checks the extensions as the server does before it serves them. Pass the same queue API
/// configs the server registers: those of warehouse queues as `queue_api_configs`, those of
/// project queues as `project_queue_api_configs`.
///
/// Also builds their router, so that axum's own panics, e.g. on a route declared twice, fire here.
///
/// # Errors
/// The first rule that an endpoint or an `OpenAPI` document breaks.
pub fn validate_management_api_extensions<A: Authorizer, C: CatalogStore, S: SecretStore>(
    extensions: &[ArcManagementApiExtension<A, C, S>],
    queue_api_configs: &[&QueueApiConfig],
    project_queue_api_configs: &[&QueueApiConfig],
) -> Result<(), ManagementApiExtensionError> {
    checked_routes(extensions, queue_api_configs, project_queue_api_configs).map(|_| ())
}

/// Validates the extensions, installs their endpoints for this process and returns their
/// routes, relative to `/management/v1`.
#[cfg(feature = "router")]
pub(crate) fn management_extension_routes<A: Authorizer, C: CatalogStore, S: SecretStore>(
    extensions: &[ArcManagementApiExtension<A, C, S>],
    queue_api_configs: &[&QueueApiConfig],
    project_queue_api_configs: &[&QueueApiConfig],
) -> Result<ExtensionRouter<A, C, S>, ManagementApiExtensionError> {
    let (endpoints, router) =
        checked_routes(extensions, queue_api_configs, project_queue_api_configs)?;
    crate::api::endpoints::install_extension_endpoints(endpoints)?;
    Ok(router)
}

/// The checked endpoints of the extensions and their router, relative to `/management/v1`.
fn checked_routes<A: Authorizer, C: CatalogStore, S: SecretStore>(
    extensions: &[ArcManagementApiExtension<A, C, S>],
    queue_api_configs: &[&QueueApiConfig],
    project_queue_api_configs: &[&QueueApiConfig],
) -> Result<(ExtensionEndpoints, ExtensionRouter<A, C, S>), ManagementApiExtensionError> {
    let declared = extensions
        .iter()
        .map(|extension| {
            extension
                .routes(ExtensionRoutes { routed: Vec::new() })
                .routed
        })
        .collect::<Vec<_>>();
    // The endpoints of each extension.
    let declarations = declared
        .iter()
        .map(|routes| routes.iter().map(|(endpoint, _)| *endpoint).collect())
        .collect::<Vec<Vec<_>>>();
    let endpoints = check_endpoints(&declarations.concat())?;
    check_api_docs(
        extensions,
        &declarations,
        queue_api_configs,
        project_queue_api_configs,
    )?;
    let router = declared.into_iter().flatten().fold(
        axum::Router::new(),
        |router, (endpoint, method_router)| {
            let path = endpoint.path();
            router.route(
                path.strip_prefix(MANAGEMENT_V1_PREFIX.trim_end_matches('/'))
                    .unwrap_or(path),
                method_router,
            )
        },
    );
    Ok((endpoints, router))
}

fn check_endpoints(
    routed: &[&'static ExtensionEndpoint],
) -> Result<ExtensionEndpoints, ManagementApiExtensionError> {
    let lakekeeper_paths = lakekeeper_management_paths();
    let mut names = HashSet::new();
    for &endpoint in routed {
        if !names.insert(endpoint.name()) {
            return Err(ManagementApiExtensionError::DuplicateName {
                name: endpoint.name(),
            });
        }
        check_endpoint(endpoint, &lakekeeper_paths)?;
    }
    Ok(ExtensionEndpoints::new(routed.iter().copied()))
}

/// Ordered, so that an error names the same Lakekeeper path on every run.
fn lakekeeper_management_paths() -> Vec<&'static str> {
    Endpoint::iter()
        .map(Endpoint::path)
        .filter(|path| path.starts_with(MANAGEMENT_V1_PREFIX))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn check_endpoint(
    endpoint: &'static ExtensionEndpoint,
    lakekeeper_paths: &[&'static str],
) -> Result<(), ManagementApiExtensionError> {
    let (name, path) = (endpoint.name(), endpoint.path());
    if !is_extension_name(name) {
        return Err(ManagementApiExtensionError::InvalidName { name });
    }
    check_path(path).map_err(|rule| ManagementApiExtensionError::InvalidPath {
        name,
        path,
        rule,
    })?;
    // A route that overlaps a Lakekeeper route takes requests meant for it, e.g. for a
    // resource whose name equals the extension's literal segment.
    if let Some(lakekeeper_path) = lakekeeper_paths
        .iter()
        .find(|lakekeeper_path| overlaps(path, lakekeeper_path))
    {
        return Err(ManagementApiExtensionError::OverlapsLakekeeperPath {
            name,
            path,
            lakekeeper_path,
        });
    }
    // Parameter names stay consistent with Lakekeeper's; statistics need `{warehouse_id}`.
    for &lakekeeper_path in lakekeeper_paths {
        if let Some((parameter, expected)) = parameter_mismatch(path, lakekeeper_path) {
            return Err(ManagementApiExtensionError::ParameterName {
                name,
                path,
                parameter,
                expected,
                lakekeeper_path,
            });
        }
    }
    Ok(())
}

fn is_extension_name(name: &str) -> bool {
    name.strip_prefix("ext-").is_some_and(|words| {
        words.split('-').all(|word| {
            !word.is_empty()
                && word
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
        })
    })
}

fn check_path(path: &str) -> Result<(), &'static str> {
    if !path.starts_with(MANAGEMENT_V1_PREFIX) {
        Err(NOT_UNDER_MANAGEMENT_V1)
    } else if path.starts_with(PERMISSIONS_PREFIX) {
        Err(UNDER_PERMISSIONS)
    } else {
        Ok(())
    }
}

fn parameter_name(segment: &str) -> Option<&str> {
    segment.strip_prefix('{')?.strip_suffix('}')
}

/// Two paths overlap when one request can match both.
fn overlaps(path: &str, other: &str) -> bool {
    let (segments, other_segments) = (path.split('/'), other.split('/'));
    segments.clone().count() == other_segments.clone().count()
        && segments
            .zip(other_segments)
            .all(|(segment, other_segment)| {
                segment == other_segment
                    || parameter_name(segment).is_some()
                    || parameter_name(other_segment).is_some()
            })
}

/// The first parameters at the same position, after equal segments, whose names differ.
fn parameter_mismatch(
    path: &'static str,
    other: &'static str,
) -> Option<(&'static str, &'static str)> {
    path.split('/')
        .zip(other.split('/'))
        .find(|(segment, other_segment)| segment != other_segment)
        .and_then(|(segment, other_segment)| {
            parameter_name(segment).zip(parameter_name(other_segment))
        })
}

#[cfg(feature = "open-api")]
fn check_api_docs<A: Authorizer, C: CatalogStore, S: SecretStore>(
    extensions: &[ArcManagementApiExtension<A, C, S>],
    declarations: &[Vec<&'static ExtensionEndpoint>],
    queue_api_configs: &[&QueueApiConfig],
    project_queue_api_configs: &[&QueueApiConfig],
) -> Result<(), ManagementApiExtensionError> {
    let docs = extensions
        .iter()
        .map(|extension| extension.api_doc())
        .collect::<Vec<_>>();
    check_documents::<A>(
        declarations,
        &docs,
        queue_api_configs,
        project_queue_api_configs,
    )
}

#[cfg(not(feature = "open-api"))]
#[allow(clippy::unnecessary_wraps)] // Shares its callers with the `open-api` variant, which fails.
fn check_api_docs<A: Authorizer, C: CatalogStore, S: SecretStore>(
    _: &[ArcManagementApiExtension<A, C, S>],
    _: &[Vec<&'static ExtensionEndpoint>],
    _: &[&QueueApiConfig],
    _: &[&QueueApiConfig],
) -> Result<(), ManagementApiExtensionError> {
    Ok(())
}

/// The extension documents describe exactly the routed operations, and use a component name of
/// Lakekeeper's document only with its definition. Merged in, they leave Lakekeeper's own
/// document unchanged and repeat no `operationId` and no tag name.
#[cfg(feature = "open-api")]
fn check_documents<A: Authorizer>(
    declarations: &[Vec<&'static ExtensionEndpoint>],
    docs: &[utoipa::openapi::OpenApi],
    queue_api_configs: &[&QueueApiConfig],
    project_queue_api_configs: &[&QueueApiConfig],
) -> Result<(), ManagementApiExtensionError> {
    for (routed, doc) in declarations.iter().zip(docs) {
        check_documented_operations(routed, doc)?;
    }
    if docs.is_empty() {
        return Ok(());
    }
    let base_doc = super::openapi::api_doc::<A>(queue_api_configs, project_queue_api_configs, &[]);
    let base = to_json(&base_doc);
    check_component_names(&base, docs)?;
    let full = super::openapi::api_doc::<A>(queue_api_configs, project_queue_api_configs, docs);
    // An `operationId` that Lakekeeper's own document repeats does not fail the extensions.
    if let Some(operation_id) = repeated_operation_ids(&full)
        .difference(&repeated_operation_ids(&base_doc))
        .next()
    {
        return Err(ManagementApiExtensionError::DuplicateOperationId {
            operation_id: (*operation_id).to_string(),
        });
    }
    // `merge` skips only a tag equal to one it has, so a name can come with two descriptions.
    let mut tag_names = HashSet::new();
    if let Some(tag) = full
        .tags
        .iter()
        .flatten()
        .find(|tag| !tag_names.insert(tag.name.as_str()))
    {
        return Err(ManagementApiExtensionError::DuplicateTag {
            tag: tag.name.clone(),
        });
    }
    check_contains(&base, &to_json(&full))
}

#[cfg(feature = "open-api")]
fn to_json(doc: &utoipa::openapi::OpenApi) -> serde_json::Value {
    serde_json::to_value(doc).expect("an OpenAPI document serializes to JSON")
}

/// `OpenApi::merge` keeps the first of two components with the same name, and the passes
/// overwrite the names they create. A name in `base` is accepted only with its definition.
#[cfg(feature = "open-api")]
fn check_component_names(
    base: &serde_json::Value,
    docs: &[utoipa::openapi::OpenApi],
) -> Result<(), ManagementApiExtensionError> {
    for doc in docs.iter().map(to_json) {
        let kinds = doc.get("components").and_then(serde_json::Value::as_object);
        for (kind, components) in kinds.into_iter().flatten() {
            for (name, component) in components.as_object().into_iter().flatten() {
                let lakekeeper_component = &base["components"][kind][name];
                if !lakekeeper_component.is_null() && lakekeeper_component != component {
                    return Err(ManagementApiExtensionError::ConflictingComponent {
                        kind: kind.clone(),
                        component: name.clone(),
                    });
                }
            }
        }
    }
    Ok(())
}

#[cfg(feature = "open-api")]
fn check_documented_operations(
    routed: &[&'static ExtensionEndpoint],
    doc: &utoipa::openapi::OpenApi,
) -> Result<(), ManagementApiExtensionError> {
    let routed = routed
        .iter()
        .map(|endpoint| (endpoint.method().as_str(), endpoint.path()))
        .collect::<BTreeSet<_>>();
    let documented = doc
        .paths
        .paths
        .iter()
        .flat_map(|(path, item)| operations(item).map(move |(method, _)| (method, path.as_str())))
        .collect::<BTreeSet<_>>();
    if let Some((method, path)) = routed.difference(&documented).next() {
        return Err(ManagementApiExtensionError::Undocumented {
            method: (*method).to_string(),
            path: (*path).to_string(),
        });
    }
    if let Some((method, path)) = documented.difference(&routed).next() {
        return Err(ManagementApiExtensionError::NotRouted {
            method: (*method).to_string(),
            path: (*path).to_string(),
        });
    }
    Ok(())
}

#[cfg(feature = "open-api")]
fn operations(
    item: &utoipa::openapi::PathItem,
) -> impl Iterator<Item = (&'static str, &utoipa::openapi::path::Operation)> {
    [
        ("GET", &item.get),
        ("PUT", &item.put),
        ("POST", &item.post),
        ("DELETE", &item.delete),
        ("OPTIONS", &item.options),
        ("HEAD", &item.head),
        ("PATCH", &item.patch),
        ("TRACE", &item.trace),
    ]
    .into_iter()
    .filter_map(|(method, operation)| Some((method, operation.as_ref()?)))
}

/// Generated clients name their methods by `operationId`.
#[cfg(feature = "open-api")]
fn repeated_operation_ids(doc: &utoipa::openapi::OpenApi) -> BTreeSet<&str> {
    let mut operation_ids = HashSet::new();
    doc.paths
        .paths
        .values()
        .flat_map(operations)
        .filter_map(|(_, operation)| operation.operation_id.as_deref())
        .filter(|operation_id| !operation_ids.insert(*operation_id))
        .collect()
}

/// Every path and component of `base` is in `full` unchanged; the other top-level fields
/// are equal, except the tags that extensions add.
#[cfg(feature = "open-api")]
fn check_contains(
    base: &serde_json::Value,
    full: &serde_json::Value,
) -> Result<(), ManagementApiExtensionError> {
    let changed =
        |part: String| Err(ManagementApiExtensionError::ChangesLakekeeperDocument { part });
    let fields = |document: &serde_json::Value| document.as_object().cloned().unwrap_or_default();
    let (base, full) = (fields(base), fields(full));
    for field in base.keys().chain(full.keys()) {
        if !matches!(field.as_str(), "paths" | "components" | "tags")
            && base.get(field) != full.get(field)
        {
            return changed(format!("the field `{field}`"));
        }
    }
    let entries = |document: &serde_json::Map<String, serde_json::Value>, field: &str| {
        document
            .get(field)
            .and_then(serde_json::Value::as_object)
            .cloned()
            .unwrap_or_default()
    };
    let (full_paths, full_components) = (entries(&full, "paths"), entries(&full, "components"));
    for (path, item) in entries(&base, "paths") {
        if full_paths.get(&path) != Some(&item) {
            return changed(format!("the path `{path}`"));
        }
    }
    for (kind, components) in entries(&base, "components") {
        let full_kind = full_components.get(&kind);
        // Only `x-` extensions of `components` are not maps, and `merge` takes none of them.
        for (name, component) in components.as_object().into_iter().flatten() {
            if full_kind.and_then(|full_kind| full_kind.get(name)) != Some(component) {
                return changed(format!("the component `{kind}/{name}`"));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use http::Method;

    use super::*;

    static HEALTH: ExtensionEndpoint = ExtensionEndpoint::new(
        "ext-table-health",
        Method::GET,
        "/management/v1/warehouse/{warehouse_id}/table/{table_id}/health",
    );
    static RECHECK: ExtensionEndpoint = ExtensionEndpoint::new(
        "ext-table-health-recheck",
        Method::POST,
        "/management/v1/warehouse/{warehouse_id}/table/{table_id}/health",
    );
    static RECHECK_ALL: ExtensionEndpoint = ExtensionEndpoint::new(
        "ext-table-health-recheck-all",
        Method::POST,
        "/management/v1/warehouse/{warehouse_id}/table-health/recheck",
    );

    fn check(routed: &[&'static ExtensionEndpoint]) -> Result<(), ManagementApiExtensionError> {
        check_endpoints(routed).map(|_| ())
    }

    #[test]
    fn valid_extensions_pass() {
        assert_eq!(
            check_endpoints(&[&HEALTH, &RECHECK, &RECHECK_ALL]),
            Ok(ExtensionEndpoints::new([&HEALTH, &RECHECK, &RECHECK_ALL]))
        );
    }

    #[test]
    fn a_name_needs_the_extension_prefix_and_kebab_case() {
        for name in ["table-health", "ext-Table", "ext-table--health"] {
            let endpoint: &'static ExtensionEndpoint = Box::leak(Box::new(ExtensionEndpoint::new(
                name,
                Method::GET,
                "/management/v1/ext/health",
            )));
            assert_eq!(
                check(&[endpoint]),
                Err(ManagementApiExtensionError::InvalidName { name }),
                "{name}"
            );
        }
    }

    #[test]
    fn a_name_is_declared_once() {
        static OTHER_PATH: ExtensionEndpoint =
            ExtensionEndpoint::new("ext-table-health", Method::GET, "/management/v1/ext/health");
        assert_eq!(
            check(&[&HEALTH, &OTHER_PATH]),
            Err(ManagementApiExtensionError::DuplicateName {
                name: "ext-table-health"
            })
        );
    }

    #[test]
    fn a_path_follows_the_prefix_rules() {
        for (path, rule) in [
            ("/catalog/v1/ext", NOT_UNDER_MANAGEMENT_V1),
            ("/management/v1", NOT_UNDER_MANAGEMENT_V1),
            ("/management/v1/permissions", UNDER_PERMISSIONS),
            ("/management/v1/permissions-report", UNDER_PERMISSIONS),
        ] {
            assert_eq!(check_path(path), Err(rule), "{path}");
        }
        assert_eq!(check_path(HEALTH.path()), Ok(()));
    }

    #[test]
    fn a_path_does_not_overlap_a_lakekeeper_path() {
        // Would take requests for a project named `ext-health`.
        static SHADOWING: ExtensionEndpoint = ExtensionEndpoint::new(
            "ext-shadowing",
            Method::GET,
            "/management/v1/project/ext-health",
        );
        assert_eq!(
            check(&[&SHADOWING]),
            Err(ManagementApiExtensionError::OverlapsLakekeeperPath {
                name: "ext-shadowing",
                path: "/management/v1/project/ext-health",
                lakekeeper_path: "/management/v1/project/{project_id}",
            })
        );
    }

    #[test]
    fn a_parameter_is_named_like_lakekeeper_parameters_at_its_position() {
        static WAREHOUSE_ID: ExtensionEndpoint = ExtensionEndpoint::new(
            "ext-warehouse-health",
            Method::GET,
            "/management/v1/warehouse/{id}/ext-health",
        );
        assert_eq!(
            check(&[&WAREHOUSE_ID]),
            Err(ManagementApiExtensionError::ParameterName {
                name: "ext-warehouse-health",
                path: "/management/v1/warehouse/{id}/ext-health",
                parameter: "id",
                expected: "warehouse_id",
                lakekeeper_path: "/management/v1/warehouse/{warehouse_id}",
            })
        );
    }

    #[test]
    fn lakekeeper_paths_follow_the_parameter_name_rule() {
        let paths = lakekeeper_management_paths();
        for path in &paths {
            for other in &paths {
                assert_eq!(parameter_mismatch(path, other), None, "{path} {other}");
            }
        }
    }

    #[cfg(feature = "open-api")]
    mod open_api {
        use utoipa::{
            ToSchema,
            openapi::{
                ComponentsBuilder, ContentBuilder, HttpMethod, ObjectBuilder, OpenApi,
                OpenApiBuilder, PathItem, PathsBuilder, Ref, RefOr, ResponseBuilder, Schema,
                path::OperationBuilder, server::Server, tag::TagBuilder,
            },
        };

        use super::*;
        use crate::{
            api::{IcebergErrorResponse, management::v1::api_doc},
            service::{
                authz::AllowAllAuthorizer,
                tasks::{self, tabular_expiration_queue::SoftDeletionQueueConfig},
            },
        };

        /// Documents `GET` on [`HEALTH`], answering with `response`.
        fn health_doc(response: Option<&str>) -> OpenApi {
            let mut operation = OperationBuilder::new();
            if let Some(response) = response {
                let content = ContentBuilder::new()
                    .schema(Some(Ref::from_schema_name(response)))
                    .build();
                operation = operation.response(
                    "200",
                    ResponseBuilder::new()
                        .description("health")
                        .content("application/json", content),
                );
            }
            OpenApiBuilder::new()
                .paths(PathsBuilder::new().path(
                    HEALTH.path(),
                    PathItem::new(HttpMethod::Get, operation.build()),
                ))
                .build()
        }

        fn with_schema(mut doc: OpenApi, name: &str, schema: RefOr<Schema>) -> OpenApi {
            doc.components = Some(ComponentsBuilder::new().schema(name, schema).build());
            doc
        }

        fn object(description: &str) -> RefOr<Schema> {
            RefOr::T(Schema::Object(
                ObjectBuilder::new().description(Some(description)).build(),
            ))
        }

        fn check_doc(
            doc: OpenApi,
            queue_api_configs: &[&QueueApiConfig],
        ) -> Result<(), ManagementApiExtensionError> {
            check_documents::<AllowAllAuthorizer>(&[vec![&HEALTH]], &[doc], queue_api_configs, &[])
        }

        fn queue_api_configs() -> Vec<&'static QueueApiConfig> {
            tasks::BUILT_IN_API_CONFIGS.iter().collect()
        }

        /// Documents `method` on `path` as the operation `operation_id`.
        fn operation_doc(path: &str, method: HttpMethod, operation_id: &str) -> OpenApi {
            let operation = OperationBuilder::new()
                .operation_id(Some(operation_id))
                .build();
            OpenApiBuilder::new()
                .paths(PathsBuilder::new().path(path, PathItem::new(method, operation)))
                .build()
        }

        #[test]
        fn the_documented_operations_are_the_routed_ones() {
            let documents = |routed: &[&'static ExtensionEndpoint], doc: OpenApi| {
                check_documents::<AllowAllAuthorizer>(&[routed.to_vec()], &[doc], &[], &[])
            };
            assert_eq!(documents(&[&HEALTH], health_doc(None)), Ok(()));
            assert_eq!(
                documents(&[&HEALTH, &RECHECK], health_doc(None)),
                Err(ManagementApiExtensionError::Undocumented {
                    method: "POST".to_string(),
                    path: RECHECK.path().to_string(),
                })
            );
            assert_eq!(
                documents(&[], health_doc(None)),
                Err(ManagementApiExtensionError::NotRouted {
                    method: "GET".to_string(),
                    path: HEALTH.path().to_string(),
                })
            );
        }

        #[test]
        fn an_identical_copy_of_a_shared_type_is_accepted() {
            let mut doc = health_doc(Some(&IcebergErrorResponse::name()));
            doc.components = Some(
                ComponentsBuilder::new()
                    .schema_from::<IcebergErrorResponse>()
                    .build(),
            );
            assert_eq!(check_doc(doc, &queue_api_configs()), Ok(()));
        }

        #[test]
        fn an_own_component_and_a_reference_to_it_are_accepted() {
            let doc = with_schema(
                health_doc(Some("TableHealth")),
                "TableHealth",
                object("health"),
            );
            assert_eq!(check_doc(doc, &queue_api_configs()), Ok(()));
        }

        #[test]
        fn a_different_lakekeeper_component_is_rejected() {
            let doc = with_schema(
                health_doc(Some(&IcebergErrorResponse::name())),
                &IcebergErrorResponse::name(),
                object("not Lakekeeper's"),
            );
            assert_eq!(
                check_doc(doc, &queue_api_configs()),
                Err(ManagementApiExtensionError::ConflictingComponent {
                    kind: "schemas".to_string(),
                    component: "IcebergErrorResponse".to_string(),
                })
            );
        }

        /// A queue pass creates the name, and the copy equals its definition.
        #[test]
        fn an_identical_copy_of_a_queue_config_is_accepted() {
            let mut doc = health_doc(Some(&SoftDeletionQueueConfig::name()));
            doc.components = Some(
                ComponentsBuilder::new()
                    .schema_from::<SoftDeletionQueueConfig>()
                    .build(),
            );
            let project_queue_api_configs = tasks::BUILT_IN_PROJECT_API_CONFIGS
                .iter()
                .collect::<Vec<_>>();
            assert_eq!(
                check_documents::<AllowAllAuthorizer>(
                    &[vec![&HEALTH]],
                    &[doc],
                    &queue_api_configs(),
                    &project_queue_api_configs,
                ),
                Ok(())
            );
        }

        #[test]
        fn an_operation_id_is_used_once() {
            assert_eq!(
                check_doc(
                    operation_doc(HEALTH.path(), HttpMethod::Get, "get_warehouse"),
                    &queue_api_configs()
                ),
                Err(ManagementApiExtensionError::DuplicateOperationId {
                    operation_id: "get_warehouse".to_string(),
                })
            );
            assert_eq!(
                check_documents::<AllowAllAuthorizer>(
                    &[vec![&HEALTH], vec![&RECHECK_ALL]],
                    &[
                        operation_doc(HEALTH.path(), HttpMethod::Get, "table_health"),
                        operation_doc(RECHECK_ALL.path(), HttpMethod::Post, "table_health"),
                    ],
                    &queue_api_configs(),
                    &[],
                ),
                Err(ManagementApiExtensionError::DuplicateOperationId {
                    operation_id: "table_health".to_string(),
                })
            );
        }

        /// The safety net behind the component rule: no base path or component changes.
        #[test]
        fn a_changed_base_path_or_component_is_rejected() {
            let base = serde_json::json!({
                "openapi": "3.1.0",
                "paths": {"/a": {"get": {}}},
                "components": {"schemas": {"A": {"type": "object"}}},
            });
            let mut full = base.clone();
            full["paths"]["/b"] = serde_json::json!({"get": {}});
            full["components"]["schemas"]["B"] = serde_json::json!({});
            assert_eq!(check_contains(&base, &full), Ok(()));

            let mut changed_path = full.clone();
            changed_path["paths"]["/a"]["post"] = serde_json::json!({});
            assert_eq!(
                check_contains(&base, &changed_path),
                Err(ManagementApiExtensionError::ChangesLakekeeperDocument {
                    part: "the path `/a`".to_string(),
                })
            );
            let mut changed_component = full;
            changed_component["components"]["schemas"]["A"] = serde_json::json!({});
            assert_eq!(
                check_contains(&base, &changed_component),
                Err(ManagementApiExtensionError::ChangesLakekeeperDocument {
                    part: "the component `schemas/A`".to_string(),
                })
            );
        }

        #[test]
        fn a_tag_name_is_used_once() {
            let with_tag = |description: &str| {
                let mut doc = health_doc(None);
                doc.tags = Some(vec![
                    TagBuilder::new()
                        .name("server")
                        .description(Some(description))
                        .build(),
                ]);
                doc
            };
            // `merge` drops a tag that equals one of Lakekeeper's.
            assert_eq!(check_doc(with_tag("Manage Server"), &[]), Ok(()));
            assert_eq!(
                check_doc(with_tag("Manage the server"), &[]),
                Err(ManagementApiExtensionError::DuplicateTag {
                    tag: "server".to_string(),
                })
            );
        }

        #[test]
        fn document_level_servers_are_rejected() {
            let mut doc = health_doc(None);
            doc.servers = Some(vec![Server::new("https://extension.example.com")]);
            assert_eq!(
                check_doc(doc, &[]),
                Err(ManagementApiExtensionError::ChangesLakekeeperDocument {
                    part: "the field `servers`".to_string(),
                })
            );
        }

        /// Lakekeeper's own document, with and without task queues, has unique `operationId`s.
        #[test]
        fn the_lakekeeper_document_passes_the_operation_id_rule() {
            for queue_api_configs in [vec![], queue_api_configs()] {
                let document = api_doc::<AllowAllAuthorizer>(&queue_api_configs, &[], &[]);
                assert_eq!(repeated_operation_ids(&document), BTreeSet::new());
            }
        }
    }
}
