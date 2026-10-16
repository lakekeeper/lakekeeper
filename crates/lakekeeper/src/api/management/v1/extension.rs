//! Management API routes that a host binary adds to Lakekeeper.

use std::{
    collections::{BTreeSet, HashMap, HashSet},
    fmt::Debug,
    sync::Arc,
};

use axum::{
    handler::Handler,
    routing::{MethodFilter, MethodRouter, on},
};
use http::Method;
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
/// authentication included. Each handler authorizes its request itself. The endpoints are
/// installed per process: every router built in one process, e.g. a test binary, uses one set.
pub trait ManagementApiExtension<A: Authorizer, C: CatalogStore, S: SecretStore>:
    Debug + Send + Sync + 'static
{
    /// Declares the extension's endpoints with their handlers, and its retired endpoints.
    fn routes(&self, routes: ExtensionRoutes<A, C, S>) -> ExtensionRoutes<A, C, S>;

    /// Documents exactly the routed endpoints. Merged into the management `OpenAPI` document.
    #[cfg(feature = "open-api")]
    fn api_doc(&self) -> utoipa::openapi::OpenApi;
}

type ApiState<A, C, S> = ApiContext<State<A, C, S>>;

type DeclaredRoute<A, C, S> = (&'static ExtensionEndpoint, MethodRouter<ApiState<A, C, S>>);

/// The endpoints an extension declares in [`ManagementApiExtension::routes`].
pub struct ExtensionRoutes<A: Authorizer, C: CatalogStore, S: SecretStore> {
    routed: Vec<DeclaredRoute<A, C, S>>,
    retired: Vec<&'static ExtensionEndpoint>,
}

impl<A: Authorizer, C: CatalogStore, S: SecretStore> Debug for ExtensionRoutes<A, C, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExtensionRoutes")
            .field(
                "routed",
                &self
                    .routed
                    .iter()
                    .map(|(endpoint, _)| endpoint)
                    .collect::<Vec<_>>(),
            )
            .field("retired", &self.retired)
            .finish()
    }
}

impl<A: Authorizer, C: CatalogStore, S: SecretStore> ExtensionRoutes<A, C, S> {
    fn new() -> Self {
        Self {
            routed: Vec::new(),
            retired: Vec::new(),
        }
    }

    /// Serves `endpoint` with `handler`. A GET endpoint also serves HEAD requests, unless
    /// a HEAD endpoint for the same path exists.
    #[must_use]
    pub fn route<H, T>(mut self, endpoint: &'static ExtensionEndpoint, handler: H) -> Self
    where
        H: Handler<T, ApiState<A, C, S>>,
        T: 'static,
    {
        let method_router = match MethodFilter::try_from(endpoint.method().clone()) {
            Ok(filter) => on(filter, handler),
            // Validation rejects the endpoint before anything is routed.
            Err(_) => MethodRouter::new(),
        };
        self.routed.push((endpoint, method_router));
        self
    }

    /// Keeps the statistics of a removed endpoint listable.
    #[must_use]
    pub fn retired(mut self, endpoint: &'static ExtensionEndpoint) -> Self {
        self.retired.push(endpoint);
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
    #[error(
        "Extension endpoint `{name}`: method {method} is not supported; use GET, HEAD, POST, PUT or DELETE"
    )]
    UnsupportedMethod { name: &'static str, method: Method },
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
        "Extension endpoint `{name}`: parameter `{{{parameter}}}` of `{path}` must be named `{{{expected}}}`, as in `{other_path}`"
    )]
    ParameterName {
        name: &'static str,
        path: &'static str,
        parameter: &'static str,
        expected: &'static str,
        other_path: &'static str,
    },
    #[error("Extension endpoint `{name}`: `{method} {path}` is also declared by `{other}`")]
    DuplicateRoute {
        name: &'static str,
        other: &'static str,
        method: Method,
        path: &'static str,
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
        "The OpenAPI component `{kind}/{component}` of an extension conflicts with a component of that name in Lakekeeper's document or another extension"
    )]
    ConflictingComponent { kind: String, component: String },
    #[error("The management OpenAPI document refers to `{reference}`, which it does not contain")]
    UnresolvedReference { reference: String },
    #[error("The management OpenAPI document has more than one operation `{operation_id}`")]
    DuplicateOperationId { operation_id: String },
    #[error("A different set of extension endpoints is already installed in this process")]
    DifferentEndpointsInstalled,
}

const NOT_UNDER_MANAGEMENT_V1: &str = "does not start with `/management/v1/`";
const UNDER_PERMISSIONS: &str =
    "starts with `/management/v1/permissions`, which the authorizer serves";
const INVALID_SEGMENT: &str = "has a segment that is neither a literal of `a-z`, `0-9`, `_` and `-` nor a parameter `{name}` of letters, digits, `_` and `-`";
const REPEATED_PARAMETER: &str = "has a parameter name twice";

/// Checks the extensions as the server does before it serves them. Pass the same queue API
/// configs the server registers: those of warehouse queues as `queue_api_configs`, those of
/// project queues as `project_queue_api_configs`.
///
/// # Errors
/// The first rule that an endpoint or an `OpenAPI` document breaks.
pub fn validate_management_api_extensions<A: Authorizer, C: CatalogStore, S: SecretStore>(
    extensions: &[ArcManagementApiExtension<A, C, S>],
    queue_api_configs: &[&QueueApiConfig],
    project_queue_api_configs: &[&QueueApiConfig],
) -> Result<(), ManagementApiExtensionError> {
    let (declarations, _routes) = declare(extensions);
    check_endpoints(&declarations)?;
    check_api_docs(
        extensions,
        &declarations,
        queue_api_configs,
        project_queue_api_configs,
    )
}

/// Validates the extensions, installs their endpoints for this process and returns their
/// routes, relative to `/management/v1`.
#[cfg(feature = "router")]
pub(crate) fn management_extension_routes<A: Authorizer, C: CatalogStore, S: SecretStore>(
    extensions: &[ArcManagementApiExtension<A, C, S>],
    queue_api_configs: &[&QueueApiConfig],
    project_queue_api_configs: &[&QueueApiConfig],
) -> Result<axum::Router<ApiState<A, C, S>>, ManagementApiExtensionError> {
    let (declarations, routes) = declare(extensions);
    let endpoints = check_endpoints(&declarations)?;
    check_api_docs(
        extensions,
        &declarations,
        queue_api_configs,
        project_queue_api_configs,
    )?;
    crate::api::endpoints::install_extension_endpoints(endpoints)?;
    Ok(routes
        .into_iter()
        .fold(axum::Router::new(), |router, (endpoint, method_router)| {
            let path = endpoint.path();
            router.route(
                path.strip_prefix(MANAGEMENT_V1_PREFIX.trim_end_matches('/'))
                    .unwrap_or(path),
                method_router,
            )
        }))
}

/// What one extension declares.
struct Declaration {
    routed: Vec<&'static ExtensionEndpoint>,
    retired: Vec<&'static ExtensionEndpoint>,
}

fn declare<A: Authorizer, C: CatalogStore, S: SecretStore>(
    extensions: &[ArcManagementApiExtension<A, C, S>],
) -> (Vec<Declaration>, Vec<DeclaredRoute<A, C, S>>) {
    let mut declarations = Vec::with_capacity(extensions.len());
    let mut routes = Vec::new();
    for extension in extensions {
        let ExtensionRoutes { routed, retired } = extension.routes(ExtensionRoutes::new());
        declarations.push(Declaration {
            routed: routed.iter().map(|(endpoint, _)| *endpoint).collect(),
            retired,
        });
        routes.extend(routed);
    }
    (declarations, routes)
}

fn check_endpoints(
    declarations: &[Declaration],
) -> Result<ExtensionEndpoints, ManagementApiExtensionError> {
    let routed = declarations
        .iter()
        .flat_map(|declaration| declaration.routed.iter().copied())
        .collect::<Vec<_>>();
    let retired = declarations
        .iter()
        .flat_map(|declaration| declaration.retired.iter().copied())
        .collect::<Vec<_>>();

    let mut names = HashSet::new();
    let mut declared_routes = HashMap::new();
    for endpoint in routed.iter().chain(&retired).copied() {
        check_endpoint(endpoint)?;
        if !names.insert(endpoint.name()) {
            return Err(ManagementApiExtensionError::DuplicateName {
                name: endpoint.name(),
            });
        }
        if let Some(other) =
            declared_routes.insert((endpoint.method(), endpoint.path()), endpoint.name())
        {
            return Err(ManagementApiExtensionError::DuplicateRoute {
                name: endpoint.name(),
                other,
                method: endpoint.method().clone(),
                path: endpoint.path(),
            });
        }
    }

    let lakekeeper_paths = lakekeeper_management_paths();
    for endpoint in &routed {
        check_routing(endpoint, &lakekeeper_paths, &routed)?;
    }
    Ok(ExtensionEndpoints::new(routed, retired))
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

/// The rules for every endpoint, routed or retired.
fn check_endpoint(endpoint: &'static ExtensionEndpoint) -> Result<(), ManagementApiExtensionError> {
    let (name, path) = (endpoint.name(), endpoint.path());
    if !is_extension_name(name) {
        return Err(ManagementApiExtensionError::InvalidName { name });
    }
    // CORS allows no other methods.
    if !matches!(
        *endpoint.method(),
        Method::GET | Method::HEAD | Method::POST | Method::PUT | Method::DELETE
    ) {
        return Err(ManagementApiExtensionError::UnsupportedMethod {
            name,
            method: endpoint.method().clone(),
        });
    }
    check_path_syntax(path).map_err(|rule| ManagementApiExtensionError::InvalidPath {
        name,
        path,
        rule,
    })
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

fn check_path_syntax(path: &str) -> Result<(), &'static str> {
    let Some(relative) = path.strip_prefix(MANAGEMENT_V1_PREFIX) else {
        return Err(NOT_UNDER_MANAGEMENT_V1);
    };
    if path.starts_with(PERMISSIONS_PREFIX) {
        return Err(UNDER_PERMISSIONS);
    }
    let mut parameters = HashSet::new();
    for segment in relative.split('/') {
        match parameter_name(segment) {
            Some(parameter) if is_parameter_name(parameter) => {
                if !parameters.insert(parameter) {
                    return Err(REPEATED_PARAMETER);
                }
            }
            None if is_literal(segment) => {}
            _ => return Err(INVALID_SEGMENT),
        }
    }
    Ok(())
}

fn is_literal(segment: &str) -> bool {
    !segment.is_empty()
        && segment.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_' || byte == b'-'
        })
}

fn parameter_name(segment: &str) -> Option<&str> {
    segment.strip_prefix('{')?.strip_suffix('}')
}

fn is_parameter_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
}

/// The rules for a routed endpoint against the other routes of the management API.
fn check_routing(
    endpoint: &'static ExtensionEndpoint,
    lakekeeper_paths: &[&'static str],
    routed: &[&'static ExtensionEndpoint],
) -> Result<(), ManagementApiExtensionError> {
    let (name, path) = (endpoint.name(), endpoint.path());
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
    // Parameter names stay consistent with Lakekeeper's and between extensions. axum panics only
    // on two routes that differ in nothing but parameter names; statistics need `{warehouse_id}`.
    let other_paths = lakekeeper_paths
        .iter()
        .copied()
        .chain(routed.iter().map(|other| other.path()));
    for other_path in other_paths {
        if let Some((parameter, expected)) = parameter_mismatch(path, other_path) {
            return Err(ManagementApiExtensionError::ParameterName {
                name,
                path,
                parameter,
                expected,
                other_path,
            });
        }
    }
    Ok(())
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
    declarations: &[Declaration],
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
    _: &[Declaration],
    _: &[&QueueApiConfig],
    _: &[&QueueApiConfig],
) -> Result<(), ManagementApiExtensionError> {
    Ok(())
}

/// The extension documents describe exactly the routed operations and reuse no component
/// name with another definition. Merged in, they leave Lakekeeper's own document unchanged,
/// repeat no `operationId` and add no reference that does not resolve.
#[cfg(feature = "open-api")]
fn check_documents<A: Authorizer>(
    declarations: &[Declaration],
    docs: &[utoipa::openapi::OpenApi],
    queue_api_configs: &[&QueueApiConfig],
    project_queue_api_configs: &[&QueueApiConfig],
) -> Result<(), ManagementApiExtensionError> {
    for (declaration, doc) in declarations.iter().zip(docs) {
        check_documented_operations(&declaration.routed, doc)?;
    }
    if docs.is_empty() {
        return Ok(());
    }
    let base_doc = super::openapi::api_doc::<A>(queue_api_configs, project_queue_api_configs, &[]);
    let base = to_json(&base_doc);
    check_component_names(
        &to_json(&lakekeeper_api_doc::<A>()),
        &base,
        &docs.iter().map(to_json).collect::<Vec<_>>(),
    )?;
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
    let full = to_json(&full);
    check_contains(&base, &full)?;
    // A reference that Lakekeeper's own document leaves unresolved does not fail the extensions.
    let unresolved_in_base = unresolved_references(&base);
    if let Some(reference) = unresolved_references(&full)
        .difference(&unresolved_in_base)
        .next()
    {
        return Err(ManagementApiExtensionError::UnresolvedReference {
            reference: (*reference).to_string(),
        });
    }
    Ok(())
}

#[cfg(feature = "open-api")]
fn to_json(doc: &utoipa::openapi::OpenApi) -> serde_json::Value {
    serde_json::to_value(doc).expect("an OpenAPI document serializes to JSON")
}

/// Lakekeeper's management document before the passes that create, rename and remove
/// components.
#[cfg(feature = "open-api")]
fn lakekeeper_api_doc<A: Authorizer>() -> utoipa::openapi::OpenApi {
    use utoipa::OpenApi as _;

    let mut doc = super::openapi::ManagementApiDoc::openapi();
    doc.merge(A::api_doc());
    doc
}

/// `OpenApi::merge` keeps the first of two components with the same name, and the passes
/// overwrite the names they create. A name in use, by Lakekeeper's document before or after the
/// passes or by an earlier extension, is accepted only with the same definition.
#[cfg(feature = "open-api")]
fn check_component_names(
    pre_pass: &serde_json::Value,
    base: &serde_json::Value,
    docs: &[serde_json::Value],
) -> Result<(), ManagementApiExtensionError> {
    let mut earlier = HashMap::new();
    for doc in docs {
        let kinds = doc.get("components").and_then(serde_json::Value::as_object);
        for (kind, components) in kinds.into_iter().flatten() {
            for (name, component) in components.as_object().into_iter().flatten() {
                let lakekeeper_component =
                    component_in(pre_pass, kind, name).or_else(|| component_in(base, kind, name));
                let accepted = match lakekeeper_component {
                    Some(existing) => existing == component,
                    None => earlier
                        .get(&(kind, name))
                        .is_none_or(|existing| existing == &component),
                };
                if !accepted {
                    return Err(ManagementApiExtensionError::ConflictingComponent {
                        kind: kind.clone(),
                        component: name.clone(),
                    });
                }
                earlier.insert((kind, name), component);
            }
        }
    }
    Ok(())
}

#[cfg(feature = "open-api")]
fn component_in<'a>(
    document: &'a serde_json::Value,
    kind: &str,
    name: &str,
) -> Option<&'a serde_json::Value> {
    document.get("components")?.get(kind)?.get(name)
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
/// are equal.
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
        if field != "paths" && field != "components" && base.get(field) != full.get(field) {
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

/// The `$ref`s and the `#/` targets of discriminator mappings that do not resolve in `document`.
#[cfg(feature = "open-api")]
fn unresolved_references(document: &serde_json::Value) -> BTreeSet<&str> {
    fn collect<'a>(
        node: &'a serde_json::Value,
        document: &serde_json::Value,
        unresolved: &mut BTreeSet<&'a str>,
    ) {
        match node {
            serde_json::Value::Object(fields) => {
                let mapping_targets = fields
                    .get("discriminator")
                    .and_then(|discriminator| discriminator.get("mapping"))
                    .and_then(serde_json::Value::as_object)
                    .into_iter()
                    .flat_map(serde_json::Map::values)
                    .filter_map(serde_json::Value::as_str)
                    .filter(|target| target.starts_with("#/"));
                let references = fields
                    .get("$ref")
                    .and_then(serde_json::Value::as_str)
                    .into_iter()
                    .chain(mapping_targets);
                for reference in references {
                    let resolves = reference
                        .strip_prefix('#')
                        .is_some_and(|pointer| document.pointer(pointer).is_some());
                    if !resolves {
                        unresolved.insert(reference);
                    }
                }
                for value in fields.values() {
                    collect(value, document, unresolved);
                }
            }
            serde_json::Value::Array(values) => {
                for value in values {
                    collect(value, document, unresolved);
                }
            }
            _ => {}
        }
    }

    let mut unresolved = BTreeSet::new();
    collect(document, document, &mut unresolved);
    unresolved
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
    static REMOVED: ExtensionEndpoint = ExtensionEndpoint::new(
        "ext-table-health-v0",
        Method::GET,
        "/management/v1/warehouse/{warehouse_id}/table/{table_id}/health-v0",
    );

    fn declaration(
        routed: &[&'static ExtensionEndpoint],
        retired: &[&'static ExtensionEndpoint],
    ) -> Declaration {
        Declaration {
            routed: routed.to_vec(),
            retired: retired.to_vec(),
        }
    }

    fn check(routed: &[&'static ExtensionEndpoint]) -> Result<(), ManagementApiExtensionError> {
        check_endpoints(&[declaration(routed, &[])]).map(|_| ())
    }

    #[test]
    fn valid_extensions_pass() {
        let endpoints = check_endpoints(&[
            declaration(&[&HEALTH, &RECHECK], &[&REMOVED]),
            declaration(&[], &[]),
        ])
        .unwrap();
        assert_eq!(
            endpoints,
            ExtensionEndpoints::new([&HEALTH, &RECHECK], [&REMOVED])
        );
    }

    #[test]
    fn a_name_needs_the_extension_prefix_and_kebab_case() {
        for name in [
            "table-health",
            "ext-",
            "ext-Table",
            "ext-table--health",
            "ext-table-",
            "ext-table_health",
            "management-v1-get-warehouse",
        ] {
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
        assert!(is_extension_name("ext-table-health-2"));
    }

    #[test]
    fn a_name_is_declared_once() {
        static OTHER_PATH: ExtensionEndpoint =
            ExtensionEndpoint::new("ext-table-health", Method::GET, "/management/v1/ext/health");
        assert_eq!(
            check_endpoints(&[
                declaration(&[&HEALTH], &[]),
                declaration(&[], &[&OTHER_PATH])
            ]),
            Err(ManagementApiExtensionError::DuplicateName {
                name: "ext-table-health"
            })
        );
    }

    #[test]
    fn a_method_must_be_one_cors_allows() {
        static PATCH: ExtensionEndpoint =
            ExtensionEndpoint::new("ext-patch", Method::PATCH, "/management/v1/ext/patch");
        assert_eq!(
            check(&[&PATCH]),
            Err(ManagementApiExtensionError::UnsupportedMethod {
                name: "ext-patch",
                method: Method::PATCH
            })
        );
    }

    #[test]
    fn a_path_follows_the_syntax_rules() {
        for (path, rule) in [
            ("/management/v2/ext", NOT_UNDER_MANAGEMENT_V1),
            ("/catalog/v1/ext", NOT_UNDER_MANAGEMENT_V1),
            ("/management/v1", NOT_UNDER_MANAGEMENT_V1),
            ("/management/v1/permissions", UNDER_PERMISSIONS),
            ("/management/v1/permissions/ext", UNDER_PERMISSIONS),
            ("/management/v1/permissions-report", UNDER_PERMISSIONS),
            ("/management/v1/", INVALID_SEGMENT),
            ("/management/v1/ext/", INVALID_SEGMENT),
            ("/management/v1/ext//health", INVALID_SEGMENT),
            ("/management/v1/ext/{*rest}", INVALID_SEGMENT),
            ("/management/v1/ext/*rest", INVALID_SEGMENT),
            ("/management/v1/ext/{id}.json", INVALID_SEGMENT),
            ("/management/v1/ext/x{id}", INVALID_SEGMENT),
            ("/management/v1/ext/{}", INVALID_SEGMENT),
            ("/management/v1/ext/{a b}", INVALID_SEGMENT),
            ("/management/v1/ext/:id", INVALID_SEGMENT),
            ("/management/v1/ext/{{id}}", INVALID_SEGMENT),
            ("/management/v1/ext/..", INVALID_SEGMENT),
            ("/management/v1/ext/health.json", INVALID_SEGMENT),
            ("/management/v1/ext/Health", INVALID_SEGMENT),
            ("/management/v1/ext/h%20h", INVALID_SEGMENT),
            ("/management/v1/ext/über", INVALID_SEGMENT),
            ("/management/v1/ext/{id}/x/{id}", REPEATED_PARAMETER),
        ] {
            assert_eq!(check_path_syntax(path), Err(rule), "{path}");
        }
        assert_eq!(check_path_syntax(HEALTH.path()), Ok(()));
        assert_eq!(
            check_path_syntax("/management/v1/ext_1/{plan-id}/x-y"),
            Ok(())
        );
    }

    #[test]
    fn an_invalid_path_names_the_endpoint_and_rule() {
        static TRAILING: ExtensionEndpoint =
            ExtensionEndpoint::new("ext-trailing", Method::GET, "/management/v1/ext/");
        assert_eq!(
            check_endpoints(&[declaration(&[], &[&TRAILING])]).map(|_| ()),
            Err(ManagementApiExtensionError::InvalidPath {
                name: "ext-trailing",
                path: "/management/v1/ext/",
                rule: INVALID_SEGMENT,
            })
        );
    }

    #[test]
    fn a_path_does_not_overlap_a_lakekeeper_path() {
        // Would take requests for a project named `ext-health`.
        static SHADOWING: ExtensionEndpoint = ExtensionEndpoint::new(
            "ext-shadowing",
            Method::GET,
            "/management/v1/project/ext-health",
        );
        // Would add a method to Lakekeeper's route.
        static ON_WAREHOUSE: ExtensionEndpoint = ExtensionEndpoint::new(
            "ext-warehouse-put",
            Method::PUT,
            "/management/v1/warehouse/{warehouse_id}",
        );
        static RENAMED_PARAMETER: ExtensionEndpoint = ExtensionEndpoint::new(
            "ext-warehouse",
            Method::GET,
            "/management/v1/warehouse/{id}",
        );
        assert_eq!(
            check(&[&SHADOWING]),
            Err(ManagementApiExtensionError::OverlapsLakekeeperPath {
                name: "ext-shadowing",
                path: "/management/v1/project/ext-health",
                lakekeeper_path: "/management/v1/project/{project_id}",
            })
        );
        assert_eq!(
            check(&[&ON_WAREHOUSE]),
            Err(ManagementApiExtensionError::OverlapsLakekeeperPath {
                name: "ext-warehouse-put",
                path: "/management/v1/warehouse/{warehouse_id}",
                lakekeeper_path: "/management/v1/warehouse/{warehouse_id}",
            })
        );
        assert_eq!(
            check(&[&RENAMED_PARAMETER]),
            Err(ManagementApiExtensionError::OverlapsLakekeeperPath {
                name: "ext-warehouse",
                path: "/management/v1/warehouse/{id}",
                lakekeeper_path: "/management/v1/warehouse/{warehouse_id}",
            })
        );
        assert!(overlaps("/a/{x}/c", "/a/b/{y}"));
        assert!(!overlaps("/a/{x}/c", "/a/b/d"));
        assert!(!overlaps("/a/{x}", "/a/{x}/c"));
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
                other_path: "/management/v1/warehouse/{warehouse_id}",
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

    #[test]
    fn a_parameter_is_named_like_other_extension_parameters_at_its_position() {
        static BY_A: ExtensionEndpoint =
            ExtensionEndpoint::new("ext-by-a", Method::GET, "/management/v1/ext/{a}");
        static BY_B: ExtensionEndpoint =
            ExtensionEndpoint::new("ext-by-b", Method::POST, "/management/v1/ext/{b}");
        assert_eq!(
            check_endpoints(&[declaration(&[&BY_A], &[]), declaration(&[&BY_B], &[])]).map(|_| ()),
            Err(ManagementApiExtensionError::ParameterName {
                name: "ext-by-a",
                path: "/management/v1/ext/{a}",
                parameter: "a",
                expected: "b",
                other_path: "/management/v1/ext/{b}",
            })
        );

        // Without the rule, axum would panic on merging these two routes.
        let merged = std::panic::catch_unwind(|| {
            axum::Router::<()>::new()
                .route("/ext/{a}", axum::routing::get(|| async {}))
                .merge(axum::Router::new().route("/ext/{b}", axum::routing::post(|| async {})))
        });
        assert!(merged.is_err());
    }

    #[test]
    fn a_route_is_declared_once() {
        static SAME_ROUTE: ExtensionEndpoint = ExtensionEndpoint::new(
            "ext-table-health-again",
            Method::GET,
            "/management/v1/warehouse/{warehouse_id}/table/{table_id}/health",
        );
        assert_eq!(
            check_endpoints(&[
                declaration(&[&HEALTH], &[]),
                declaration(&[], &[&SAME_ROUTE])
            ])
            .map(|_| ()),
            Err(ManagementApiExtensionError::DuplicateRoute {
                name: "ext-table-health-again",
                other: "ext-table-health",
                method: Method::GET,
                path: "/management/v1/warehouse/{warehouse_id}/table/{table_id}/health",
            })
        );
    }

    #[cfg(feature = "open-api")]
    mod open_api {
        use utoipa::{
            ToSchema,
            openapi::{
                ComponentsBuilder, ContentBuilder, HttpMethod, ObjectBuilder, OpenApi,
                OpenApiBuilder, PathItem, PathsBuilder, Ref, RefOr, ResponseBuilder, Schema,
                path::OperationBuilder,
                schema::{Discriminator, OneOfBuilder, Type},
                security::SecurityRequirement,
                server::Server,
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
            check_documents::<AllowAllAuthorizer>(
                &[declaration(&[&HEALTH], &[])],
                &[doc],
                queue_api_configs,
                &[],
            )
        }

        fn queue_api_configs() -> Vec<&'static QueueApiConfig> {
            tasks::BUILT_IN_API_CONFIGS.iter().collect()
        }

        /// Checks `doc` with the queue API configs of both scopes that Lakekeeper registers.
        fn check_with_built_in_queues(doc: OpenApi) -> Result<(), ManagementApiExtensionError> {
            let project_queue_api_configs = tasks::BUILT_IN_PROJECT_API_CONFIGS
                .iter()
                .collect::<Vec<_>>();
            check_documents::<AllowAllAuthorizer>(
                &[declaration(&[&HEALTH], &[])],
                &[doc],
                &queue_api_configs(),
                &project_queue_api_configs,
            )
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
                check_documents::<AllowAllAuthorizer>(&[declaration(routed, &[])], &[doc], &[], &[])
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

        /// The queue pass would overwrite it with Lakekeeper's definition.
        #[test]
        fn a_component_that_a_queue_pass_creates_is_rejected() {
            let doc = with_schema(
                health_doc(Some("PurgeQueueConfig")),
                "PurgeQueueConfig",
                object("not Lakekeeper's"),
            );
            assert_eq!(
                check_doc(doc, &queue_api_configs()),
                Err(ManagementApiExtensionError::ConflictingComponent {
                    kind: "schemas".to_string(),
                    component: "PurgeQueueConfig".to_string(),
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
            assert_eq!(check_with_built_in_queues(doc), Ok(()));
        }

        #[test]
        fn a_reference_to_a_queue_config_resolves_with_the_queue_configs() {
            let doc = || health_doc(Some(&SoftDeletionQueueConfig::name()));
            assert_eq!(check_with_built_in_queues(doc()), Ok(()));
            assert_eq!(
                check_doc(doc(), &[]),
                Err(ManagementApiExtensionError::UnresolvedReference {
                    reference: "#/components/schemas/SoftDeletionQueueConfig".to_string(),
                })
            );
        }

        /// The hoisting pass would name Lakekeeper's variant differently.
        #[test]
        fn a_component_that_the_hoisting_pass_creates_is_rejected() {
            let doc = with_schema(
                health_doc(None),
                "ControlTaskActionCancel",
                object("not Lakekeeper's"),
            );
            assert_eq!(
                check_doc(doc, &queue_api_configs()),
                Err(ManagementApiExtensionError::ConflictingComponent {
                    kind: "schemas".to_string(),
                    component: "ControlTaskActionCancel".to_string(),
                })
            );
        }

        /// The naming pass derives `ControlTaskActionCancel` for the extension's variant first, so
        /// Lakekeeper's `ControlTaskAction` would refer to its own variant under another name.
        #[test]
        fn a_variant_name_derived_like_a_lakekeeper_one_is_rejected() {
            let variant = ObjectBuilder::new()
                .property(
                    "kind",
                    ObjectBuilder::new()
                        .schema_type(Type::String)
                        .enum_values(Some(["action-cancel"])),
                )
                .required("kind");
            let union = RefOr::T(Schema::OneOf(OneOfBuilder::new().item(variant).build()));
            let doc = with_schema(health_doc(Some("ControlTask")), "ControlTask", union);
            assert_eq!(
                check_doc(doc, &queue_api_configs()),
                Err(ManagementApiExtensionError::ChangesLakekeeperDocument {
                    part: "the component `schemas/ControlTaskAction`".to_string(),
                })
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
                    &[declaration(&[&HEALTH], &[]), declaration(&[&RECHECK], &[])],
                    &[
                        operation_doc(HEALTH.path(), HttpMethod::Get, "table_health"),
                        operation_doc(RECHECK.path(), HttpMethod::Post, "table_health"),
                    ],
                    &queue_api_configs(),
                    &[],
                ),
                Err(ManagementApiExtensionError::DuplicateOperationId {
                    operation_id: "table_health".to_string(),
                })
            );
        }

        #[test]
        fn extensions_share_a_component_only_with_the_same_definition() {
            let recheck_doc = |description: &str| {
                let mut doc = with_schema(OpenApi::default(), "TableHealth", object(description));
                doc.paths = PathsBuilder::new()
                    .path(
                        RECHECK.path(),
                        PathItem::new(HttpMethod::Post, OperationBuilder::new().build()),
                    )
                    .build();
                doc
            };
            let two_extensions = |second: OpenApi| {
                check_documents::<AllowAllAuthorizer>(
                    &[declaration(&[&HEALTH], &[]), declaration(&[&RECHECK], &[])],
                    &[
                        with_schema(
                            health_doc(Some("TableHealth")),
                            "TableHealth",
                            object("health"),
                        ),
                        second,
                    ],
                    &queue_api_configs(),
                    &[],
                )
            };
            assert_eq!(two_extensions(recheck_doc("health")), Ok(()));
            assert_eq!(
                two_extensions(recheck_doc("other")),
                Err(ManagementApiExtensionError::ConflictingComponent {
                    kind: "schemas".to_string(),
                    component: "TableHealth".to_string(),
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
        fn a_reference_that_does_not_resolve_is_rejected() {
            for name in ["ScheduleTaskRequest", "NotAComponent"] {
                assert_eq!(
                    check_doc(health_doc(Some(name)), &queue_api_configs()),
                    Err(ManagementApiExtensionError::UnresolvedReference {
                        reference: format!("#/components/schemas/{name}"),
                    }),
                    "{name}"
                );
            }
        }

        #[test]
        fn a_discriminator_mapping_that_does_not_resolve_is_rejected() {
            let union = OneOfBuilder::new()
                .item(Ref::from_schema_name("TableHealthOk"))
                .discriminator(Some(Discriminator::with_mapping(
                    "status",
                    [
                        ("ok", "#/components/schemas/TableHealthOk"),
                        ("failed", "#/components/schemas/TableHealthFailed"),
                    ],
                )))
                .build();
            let mut doc = health_doc(Some("TableHealth"));
            doc.components = Some(
                ComponentsBuilder::new()
                    .schema("TableHealth", RefOr::T(Schema::OneOf(union)))
                    .schema("TableHealthOk", object("ok"))
                    .build(),
            );
            assert_eq!(
                check_doc(doc, &queue_api_configs()),
                Err(ManagementApiExtensionError::UnresolvedReference {
                    reference: "#/components/schemas/TableHealthFailed".to_string(),
                })
            );
        }

        #[test]
        fn document_level_servers_and_security_are_rejected() {
            let mut doc = health_doc(None);
            doc.servers = Some(vec![Server::new("https://extension.example.com")]);
            assert_eq!(
                check_doc(doc, &[]),
                Err(ManagementApiExtensionError::ChangesLakekeeperDocument {
                    part: "the field `servers`".to_string(),
                })
            );

            let mut doc = health_doc(None);
            doc.security = Some(vec![SecurityRequirement::new(
                "extensionAuth",
                Vec::<String>::new(),
            )]);
            assert_eq!(
                check_doc(doc, &[]),
                Err(ManagementApiExtensionError::ChangesLakekeeperDocument {
                    part: "the field `security`".to_string(),
                })
            );
        }

        /// Lakekeeper's own document, with and without task queues, has unique `operationId`s
        /// and references that resolve.
        #[test]
        fn the_lakekeeper_document_passes_the_operation_id_and_reference_rules() {
            for queue_api_configs in [vec![], queue_api_configs()] {
                let document = api_doc::<AllowAllAuthorizer>(&queue_api_configs, &[], &[]);
                assert_eq!(repeated_operation_ids(&document), BTreeSet::new());
                assert_eq!(unresolved_references(&to_json(&document)), BTreeSet::new());
            }
        }
    }
}
