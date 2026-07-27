//! Governance-tag entities: a project-scoped vocabulary (`TagDefinition`) and
//! per-target attachments (`Tag`). Value validation (name format, scope) lives
//! here so it surfaces as typed `400`s; referential/structural integrity is in
//! the schema.

use chrono::{DateTime, Utc};
use http::StatusCode;
use iceberg_ext::catalog::rest::ErrorModel;
use serde::{Deserialize, Serialize};

use crate::{
    ProjectId,
    api::iceberg::v1::PaginationQuery,
    service::{
        CatalogBackendError, CatalogStore, InvalidPaginationToken, NamespaceId,
        ProjectIdNotFoundError, TabularId, TagDefinitionId, TagId, Transaction, WarehouseId,
        define_transparent_error, impl_error_stack_methods, impl_from_with_detail,
    },
};

/// Name prefixes only internal (catalog-managed) code may write. Derived from
/// the name; never stored.
pub const RESERVED_TAG_PREFIXES: &[&str] = &["system.", "lakekeeper."];

/// True if the name is a reserved namespace root (e.g. `system`) or lives under
/// one (e.g. `system.pii`). The bare root is reserved for every prefix, derived
/// from `RESERVED_TAG_PREFIXES` with the trailing `.` stripped.
#[must_use]
pub fn is_reserved_tag_name(name: &str) -> bool {
    let lower = name.to_lowercase();
    RESERVED_TAG_PREFIXES
        .iter()
        .any(|p| lower.starts_with(p) || lower == p.trim_end_matches('.'))
}

/// Validate a tag-definition name. `.` is reserved as the hierarchy delimiter.
///
/// # Errors
/// `400` if the name is empty/too long, has surrounding whitespace, contains
/// control characters, or has leading/trailing/empty `.` segments.
pub fn validate_tag_name(name: &str) -> Result<(), ErrorModel> {
    let reject = |why: &str| {
        Err(ErrorModel::bad_request(
            format!("Invalid tag name '{name}': {why}"),
            "InvalidTagName",
            None,
        ))
    };
    let len = name.chars().count();
    if !(1..=256).contains(&len) {
        return reject("must be 1 to 256 characters");
    }
    if name != name.trim() {
        return reject("must not have leading or trailing whitespace");
    }
    if name.chars().any(char::is_control) {
        return reject("must not contain control characters");
    }
    if name.starts_with('.') || name.ends_with('.') {
        return reject("must not start or end with '.'");
    }
    if name.contains("..") {
        return reject("must not contain empty segments ('..')");
    }
    Ok(())
}

/// Max length (in characters) of a free-text tag value and of each enumerated
/// allowed value. Values are stored in a btree index, so this must stay well under
/// Postgres's ~2704-byte per-tuple limit; 256 also matches common governance
/// conventions and mirrors the tag-name cap.
pub const MAX_TAG_VALUE_LEN: usize = 256;

/// Shared length + control-character checks for a tag value and each enumerated
/// allowed value (they share the `text` domain and both land in a btree index). On
/// failure returns the reason clause; callers prefix it with their subject and wrap
/// it in their own error type. Emptiness is checked by the caller — the two contexts
/// word it differently and one accepts `None`.
pub fn validate_tag_value_chars(value: &str) -> Result<(), String> {
    if value.chars().count() > MAX_TAG_VALUE_LEN {
        return Err(format!("must be at most {MAX_TAG_VALUE_LEN} characters"));
    }
    if value.chars().any(char::is_control) {
        return Err("must not contain control characters".to_string());
    }
    Ok(())
}

/// Validate a value against a definition's value contract.
///
/// # Errors
/// `400` if a marker carries a value, a value is missing/empty/too long/has control
/// characters for a value-taking kind, or an enumerated value is not in
/// `allowed_values` (case-sensitive exact).
pub fn validate_tag_value(
    value_kind: TagValueKind,
    allowed_values: &[String],
    value: Option<&str>,
) -> Result<(), InvalidTagValue> {
    match value_kind {
        TagValueKind::Marker => {
            if value.is_some() {
                return Err(InvalidTagValue::new("Marker tags do not take a value"));
            }
        }
        TagValueKind::FreeText => {
            let Some(value) = value.filter(|v| !v.is_empty()) else {
                return Err(InvalidTagValue::new("A value is required for this tag"));
            };
            validate_tag_value_chars(value)
                .map_err(|why| InvalidTagValue::new(format!("A tag value {why}")))?;
        }
        TagValueKind::Enumerated => {
            let Some(value) = value else {
                return Err(InvalidTagValue::new("A value is required for this tag"));
            };
            if !allowed_values.iter().any(|v| v == value) {
                return Err(InvalidTagValue::new(
                    "Value is not in the allowed values for this tag",
                ));
            }
        }
    }
    Ok(())
}

#[derive(thiserror::Error, PartialEq, Debug)]
#[error("{message}")]
pub struct InvalidTagValue {
    pub message: String,
    pub stack: Vec<String>,
}
impl InvalidTagValue {
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            stack: Vec::new(),
        }
    }
}
impl_error_stack_methods!(InvalidTagValue);
impl From<InvalidTagValue> for ErrorModel {
    fn from(err: InvalidTagValue) -> Self {
        ErrorModel::builder()
            .r#type("InvalidTagValue")
            .code(StatusCode::BAD_REQUEST.as_u16())
            .message(err.message.clone())
            .stack(err.stack)
            .build()
    }
}

/// Validate that a definition's scope permits attaching to a target of
/// `target_scope`.
///
/// # Errors
/// `400` naming the disallowed scope.
pub fn validate_tag_scope(
    allowed: &[TagScope],
    target_scope: TagScope,
) -> Result<(), TagScopeNotAllowed> {
    if allowed.contains(&target_scope) {
        return Ok(());
    }
    Err(TagScopeNotAllowed::new(format!(
        "Tag does not allow scope `{}`",
        target_scope.as_str()
    )))
}

#[derive(thiserror::Error, PartialEq, Debug)]
#[error("{message}")]
pub struct TagScopeNotAllowed {
    pub message: String,
    pub stack: Vec<String>,
}
impl TagScopeNotAllowed {
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            stack: Vec::new(),
        }
    }
}
impl_error_stack_methods!(TagScopeNotAllowed);
impl From<TagScopeNotAllowed> for ErrorModel {
    fn from(err: TagScopeNotAllowed) -> Self {
        ErrorModel::builder()
            .r#type("TagScopeNotAllowed")
            .code(StatusCode::BAD_REQUEST.as_u16())
            .message(err.message.clone())
            .stack(err.stack)
            .build()
    }
}

/// Enforce the widen-only scope policy: every element of `current` must still be
/// present in `requested` (set-superset; order-free).
///
/// # Errors
/// `400` listing the scopes that would be removed.
pub fn validate_scope_widening(
    current: &[TagScope],
    requested: &[TagScope],
) -> Result<(), TagScopeNarrowed> {
    let mut removed: Vec<&'static str> = current
        .iter()
        .filter(|s| !requested.contains(s))
        .map(|s| s.as_str())
        .collect();
    if removed.is_empty() {
        return Ok(());
    }
    removed.sort_unstable();
    removed.dedup();
    Err(TagScopeNarrowed::new(
        removed.into_iter().map(String::from).collect(),
    ))
}

#[derive(thiserror::Error, PartialEq, Debug)]
#[error("Tag definition scope cannot be narrowed. Removed scopes: {}", removed_scopes.join(", "))]
pub struct TagScopeNarrowed {
    /// The scopes that would be removed, sorted.
    pub removed_scopes: Vec<String>,
    pub stack: Vec<String>,
}
impl TagScopeNarrowed {
    #[must_use]
    pub fn new(removed_scopes: Vec<String>) -> Self {
        Self {
            removed_scopes,
            stack: Vec::new(),
        }
    }
}
impl_error_stack_methods!(TagScopeNarrowed);
impl From<TagScopeNarrowed> for ErrorModel {
    fn from(err: TagScopeNarrowed) -> Self {
        ErrorModel::builder()
            .r#type("TagScopeNarrowed")
            .code(StatusCode::BAD_REQUEST.as_u16())
            .message(err.to_string())
            .stack(err.stack)
            .build()
    }
}

/// Target types a tag definition may be applied to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub enum TagScope {
    Warehouse,
    Namespace,
    Table,
    View,
    GenericTable,
    Column,
}

impl TagScope {
    // `scope` is stored as a Postgres `text[]` (no enum type), so `TagScope` converts
    // by hand here — unlike `TagSource`/`TagValueKind`, which are PG enum types whose
    // conversion is handled entirely by their `sqlx::Type` derive.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            TagScope::Warehouse => "warehouse",
            TagScope::Namespace => "namespace",
            TagScope::Table => "table",
            TagScope::View => "view",
            TagScope::GenericTable => "generic-table",
            TagScope::Column => "column",
        }
    }

    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "warehouse" => TagScope::Warehouse,
            "namespace" => TagScope::Namespace,
            "table" => TagScope::Table,
            "view" => TagScope::View,
            "generic-table" => TagScope::GenericTable,
            "column" => TagScope::Column,
            _ => return None,
        })
    }
}

/// How a tag came to exist. Server-assigned; currently always `manual`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
#[cfg_attr(feature = "sqlx-postgres", derive(sqlx::Type))]
#[cfg_attr(
    feature = "sqlx-postgres",
    sqlx(type_name = "tag_source", rename_all = "snake_case")
)]
pub enum TagSource {
    Manual,
}

/// How a tag definition's value is constrained: presence-only, arbitrary text,
/// or one of a fixed set of allowed values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
#[cfg_attr(feature = "sqlx-postgres", derive(sqlx::Type))]
#[cfg_attr(
    feature = "sqlx-postgres",
    sqlx(type_name = "tag_value_kind", rename_all = "snake_case")
)]
pub enum TagValueKind {
    /// Presence only; no value (e.g. `Pii`, `Deprecated`).
    Marker,
    /// Arbitrary free-text value.
    FreeText,
    /// Value must be one of the definition's allowed values.
    Enumerated,
}

/// The object a tag is attached to. `warehouse_id` is the target itself
/// (warehouse) or its containing warehouse (everything else).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TagTarget {
    Warehouse(WarehouseId),
    Namespace {
        warehouse_id: WarehouseId,
        namespace_id: NamespaceId,
    },
    /// table | view | generic-table (the subtype is carried by `TabularId`).
    Tabular {
        warehouse_id: WarehouseId,
        tabular_id: TabularId,
    },
    /// A column of a tabular, keyed by Iceberg field-id.
    Column {
        warehouse_id: WarehouseId,
        tabular_id: TabularId,
        field_id: i32,
    },
}

impl TagTarget {
    #[must_use]
    pub fn warehouse_id(&self) -> WarehouseId {
        match self {
            TagTarget::Warehouse(w) => *w,
            TagTarget::Namespace { warehouse_id, .. }
            | TagTarget::Tabular { warehouse_id, .. }
            | TagTarget::Column { warehouse_id, .. } => *warehouse_id,
        }
    }

    /// The scope (target type) this target represents, for scope validation.
    #[must_use]
    pub fn scope(&self) -> TagScope {
        match self {
            TagTarget::Warehouse(_) => TagScope::Warehouse,
            TagTarget::Namespace { .. } => TagScope::Namespace,
            TagTarget::Column { .. } => TagScope::Column,
            TagTarget::Tabular { tabular_id, .. } => match tabular_id {
                TabularId::Table(_) => TagScope::Table,
                TabularId::View(_) => TagScope::View,
                TabularId::GenericTable(_) => TagScope::GenericTable,
            },
        }
    }
}

/// A registered tag name in a project's vocabulary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TagDefinition {
    pub tag_definition_id: TagDefinitionId,
    pub project_id: ProjectId,
    pub name: String,
    pub description: Option<String>,
    pub scope: Vec<TagScope>,
    pub value_kind: TagValueKind,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
}

impl TagDefinition {
    /// True if this definition lives in a reserved namespace and is read-only
    /// to customer-facing APIs.
    #[must_use]
    pub fn is_protected(&self) -> bool {
        is_reserved_tag_name(&self.name)
    }
}

/// A tag applied to a target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tag {
    pub tag_id: TagId,
    pub tag_definition_id: TagDefinitionId,
    pub target: TagTarget,
    pub value: Option<String>,
    pub source: TagSource,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
}

/// A tag together with its definition's name, joined by the store on a
/// list-by-target read so callers need no per-row definition lookup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TagWithName {
    pub tag: Tag,
    /// Name of the tag definition (`tag_definition.name`).
    pub definition_name: String,
}

// --------------------------- EFFECTIVE TAGS (inheritance) ---------------------------

/// Where an effective tag is attached, relative to the queried object. `Direct` =
/// the queried object itself; otherwise an ancestor it is inherited from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EffectiveTagSource {
    Direct,
    Namespace {
        warehouse_id: WarehouseId,
        namespace_id: NamespaceId,
    },
    Warehouse {
        warehouse_id: WarehouseId,
    },
}

/// A candidate effective tag before visibility filtering and most-specific-wins
/// resolution. `distance` 0 = attached directly to the queried object; larger =
/// farther ancestor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectiveTagCandidate {
    pub tag_id: TagId,
    pub tag_definition_id: TagDefinitionId,
    pub name: String,
    pub value: Option<String>,
    pub source: TagSource,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub distance: i32,
    pub origin: EffectiveTagSource,
}

/// Producer-source priority for the resolution tiebreak (lower wins). Distinct from
/// `distance` (containment): this only breaks a same-distance, same-definition tie
/// between producers, which cannot occur today (only `Manual`) but is defined now
/// because the `tag` unique key includes `source` and more producers are planned.
fn source_priority(source: TagSource) -> u8 {
    match source {
        TagSource::Manual => 0,
    }
}

/// Resolve candidates to the effective set: keep one row per definition under the
/// total order `(distance, source_priority, tag_id)` — the nearest (most specific)
/// attachment wins, the queried object's own `Direct` tag (distance 0) beating any
/// inherited copy. Pure and deterministic.
#[must_use]
pub fn resolve_effective_tags(
    mut candidates: Vec<EffectiveTagCandidate>,
) -> Vec<EffectiveTagCandidate> {
    candidates.sort_by(|a, b| {
        a.distance
            .cmp(&b.distance)
            .then_with(|| source_priority(a.source).cmp(&source_priority(b.source)))
            .then_with(|| a.tag_id.cmp(&b.tag_id))
    });
    let mut seen = std::collections::HashSet::new();
    candidates.retain(|c| seen.insert(*c.tag_definition_id));
    candidates
}

/// The value contract of a tag definition at write time. Bundling `allowed_values`
/// into `Enumerated` makes the illegal combinations — a marker carrying values, an
/// enumerated definition with none — unrepresentable for callers. Borrowed and never
/// serialized: the customer-facing DTO stays a flat `{value_kind, allowed_values}`,
/// so this is a zero-wire-cost internal ergonomics win. `Enumerated { allowed_values:
/// &[] }` is still constructible, so non-empty/per-value validation stays the caller's
/// job (it produces a typed `400`, not a raw constraint violation).
#[derive(Debug, Clone)]
pub enum TagValueSpec<'a> {
    Marker,
    FreeText,
    Enumerated { allowed_values: &'a [&'a str] },
}

impl TagValueSpec<'_> {
    /// The scalar discriminant persisted in the `value_kind` column.
    #[must_use]
    pub fn kind(&self) -> TagValueKind {
        match self {
            TagValueSpec::Marker => TagValueKind::Marker,
            TagValueSpec::FreeText => TagValueKind::FreeText,
            TagValueSpec::Enumerated { .. } => TagValueKind::Enumerated,
        }
    }

    /// The permitted values; empty for non-enumerated kinds.
    #[must_use]
    pub fn allowed_values(&self) -> &[&str] {
        match self {
            TagValueSpec::Marker | TagValueSpec::FreeText => &[],
            TagValueSpec::Enumerated { allowed_values } => allowed_values,
        }
    }
}

/// Spec for creating a [`TagDefinition`]; `project_id` is supplied separately
/// (the parent scope), mirroring [`crate::service::CatalogCreateRoleRequest`].
#[derive(Debug, typed_builder::TypedBuilder)]
pub struct CatalogCreateTagDefinitionRequest<'a> {
    pub tag_definition_id: TagDefinitionId,
    pub name: &'a str,
    #[builder(default)]
    pub description: Option<&'a str>,
    pub scope: &'a [TagScope],
    pub value_spec: TagValueSpec<'a>,
}

// --------------------------- CREATE TAG DEFINITION ERROR ---------------------------
define_transparent_error! {
    pub enum CreateTagDefinitionError,
    stack_message: "Error creating tag definition in catalog",
    variants: [
        TagNameAlreadyExists,
        ProjectIdNotFoundError,
        CatalogBackendError
    ]
}

#[derive(thiserror::Error, PartialEq, Debug, Default)]
#[error("A tag definition with the specified name already exists in the specified project")]
pub struct TagNameAlreadyExists {
    pub stack: Vec<String>,
}
impl TagNameAlreadyExists {
    #[must_use]
    pub fn new() -> Self {
        Self { stack: Vec::new() }
    }
}
impl_error_stack_methods!(TagNameAlreadyExists);
impl From<TagNameAlreadyExists> for ErrorModel {
    fn from(err: TagNameAlreadyExists) -> Self {
        ErrorModel::builder()
            .r#type("TagNameAlreadyExists")
            .code(StatusCode::CONFLICT.as_u16())
            .message(err.to_string())
            .stack(err.stack)
            .build()
    }
}

// --------------------------- LIST TAG DEFINITIONS ---------------------------
define_transparent_error! {
    pub enum ListTagDefinitionsError,
    stack_message: "Error listing tag definitions in catalog",
    variants: [
        CatalogBackendError,
        InvalidPaginationToken
    ]
}

/// A page of tag definitions plus the cursor for the next page (`None` when there
/// are no further rows). Definitions are scalar — allowed values are not loaded on
/// the list path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListTagDefinitionsResponse {
    pub tag_definitions: Vec<TagDefinition>,
    pub next_page_token: Option<String>,
}

// --------------------------- LIST TAG ATTACHMENTS ---------------------------
define_transparent_error! {
    pub enum ListTagAttachmentsError,
    stack_message: "Error listing tag attachments in catalog",
    variants: [
        CatalogBackendError,
        InvalidPaginationToken
    ]
}

/// A page of the targets a definition is directly attached to, plus the cursor for
/// the next page (`None` when there are no further rows). Reverse of
/// [`CatalogTagOps::list_tags_for_target`]: keyed by definition, it answers "which
/// objects carry this tag". Direct attachments only — no hierarchy expansion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListTagAttachmentsResponse {
    pub tags: Vec<Tag>,
    pub next_page_token: Option<String>,
}

/// The mutable fields of a tag definition. Storage primitive: `scope` is replaced
/// wholesale and `add_allowed_values` are inserted (never removed) — the widen-only
/// and kind-immutable policy is enforced one layer up, where the current state is
/// read to emit typed `400`s.
#[derive(Debug, typed_builder::TypedBuilder)]
pub struct UpdateTagDefinitionRequest<'a> {
    pub name: &'a str,
    #[builder(default)]
    pub description: Option<&'a str>,
    pub scope: &'a [TagScope],
    #[builder(default)]
    pub add_allowed_values: &'a [&'a str],
}

// --------------------------- UPDATE TAG DEFINITION ERROR ---------------------------
define_transparent_error! {
    pub enum UpdateTagDefinitionError,
    stack_message: "Error updating tag definition in catalog",
    variants: [
        TagNameAlreadyExists,
        TagDefinitionIdNotFound,
        TagScopeNarrowed,
        TagDefinitionReserved,
        InvalidTagDefinition,
        CatalogBackendError
    ]
}

/// The definition's name (current or requested) lives in a reserved namespace
/// (see [`RESERVED_TAG_PREFIXES`]) — such definitions are catalog-managed and
/// read-only through this API.
#[derive(thiserror::Error, PartialEq, Debug, Default)]
#[error(
    "The tag name is in a reserved namespace; such tags are managed by the catalog \
     and cannot be written through this API"
)]
pub struct TagDefinitionReserved {
    pub stack: Vec<String>,
}
impl TagDefinitionReserved {
    #[must_use]
    pub fn new() -> Self {
        Self { stack: Vec::new() }
    }
}
impl_error_stack_methods!(TagDefinitionReserved);
impl From<TagDefinitionReserved> for ErrorModel {
    fn from(err: TagDefinitionReserved) -> Self {
        ErrorModel::builder()
            .r#type("ReservedTagNamespace")
            .code(StatusCode::BAD_REQUEST.as_u16())
            .message(err.to_string())
            .stack(err.stack)
            .build()
    }
}

/// The requested definition shape is inconsistent (e.g. allowed values on a
/// non-enumerated kind, or an enumerated kind without allowed values).
#[derive(thiserror::Error, PartialEq, Debug)]
#[error("{message}")]
pub struct InvalidTagDefinition {
    pub message: String,
    pub stack: Vec<String>,
}
impl InvalidTagDefinition {
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            stack: Vec::new(),
        }
    }
}
impl_error_stack_methods!(InvalidTagDefinition);
impl From<InvalidTagDefinition> for ErrorModel {
    fn from(err: InvalidTagDefinition) -> Self {
        ErrorModel::builder()
            .r#type("InvalidTagDefinition")
            .code(StatusCode::BAD_REQUEST.as_u16())
            .message(err.message.clone())
            .stack(err.stack)
            .build()
    }
}

#[derive(thiserror::Error, PartialEq, Debug)]
#[error("No tag definition with id '{tag_definition_id}' exists in the project")]
pub struct TagDefinitionIdNotFound {
    pub tag_definition_id: TagDefinitionId,
    pub stack: Vec<String>,
}
impl TagDefinitionIdNotFound {
    #[must_use]
    pub fn new(tag_definition_id: TagDefinitionId) -> Self {
        Self {
            tag_definition_id,
            stack: Vec::new(),
        }
    }
}
impl_error_stack_methods!(TagDefinitionIdNotFound);
impl From<TagDefinitionIdNotFound> for ErrorModel {
    fn from(err: TagDefinitionIdNotFound) -> Self {
        ErrorModel::builder()
            .r#type("TagDefinitionIdNotFound")
            .code(StatusCode::NOT_FOUND.as_u16())
            .message(err.to_string())
            .stack(err.stack)
            .build()
    }
}

// --------------------------- APPLY TAG ERROR ---------------------------
define_transparent_error! {
    pub enum ApplyTagError,
    stack_message: "Error applying tag in catalog",
    variants: [
        TagDefinitionIdNotFound,
        TagTargetNotFound,
        TagScopeNotAllowed,
        InvalidTagValue,
        CatalogBackendError
    ]
}

/// No tag definition with the requested name exists in the project.
#[derive(thiserror::Error, PartialEq, Debug)]
#[error("No tag definition with name '{name}' exists in the project")]
pub struct TagNameNotFound {
    pub name: String,
    pub stack: Vec<String>,
}
impl TagNameNotFound {
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            stack: Vec::new(),
        }
    }
}
impl_error_stack_methods!(TagNameNotFound);
impl From<TagNameNotFound> for ErrorModel {
    fn from(err: TagNameNotFound) -> Self {
        ErrorModel::builder()
            .r#type("TagNameNotFound")
            .code(StatusCode::NOT_FOUND.as_u16())
            .message(err.to_string())
            .stack(err.stack)
            .build()
    }
}

/// The named column does not exist in the table's current schema.
#[derive(thiserror::Error, PartialEq, Debug)]
#[error("Column '{column}' does not exist in the table's current schema")]
pub struct ColumnNotFound {
    pub column: String,
    pub stack: Vec<String>,
}
impl ColumnNotFound {
    #[must_use]
    pub fn new(column: impl Into<String>) -> Self {
        Self {
            column: column.into(),
            stack: Vec::new(),
        }
    }
}
impl_error_stack_methods!(ColumnNotFound);
impl From<ColumnNotFound> for ErrorModel {
    fn from(err: ColumnNotFound) -> Self {
        ErrorModel::builder()
            .r#type("ColumnNotFound")
            .code(StatusCode::NOT_FOUND.as_u16())
            .message(err.to_string())
            .stack(err.stack)
            .build()
    }
}

#[derive(thiserror::Error, PartialEq, Debug, Default)]
#[error("The target of the tag does not exist")]
pub struct TagTargetNotFound {
    pub stack: Vec<String>,
}
impl TagTargetNotFound {
    #[must_use]
    pub fn new() -> Self {
        Self { stack: Vec::new() }
    }
}
impl_error_stack_methods!(TagTargetNotFound);
impl From<TagTargetNotFound> for ErrorModel {
    fn from(err: TagTargetNotFound) -> Self {
        ErrorModel::builder()
            .r#type("TagTargetNotFound")
            .code(StatusCode::NOT_FOUND.as_u16())
            .message(err.to_string())
            .stack(err.stack)
            .build()
    }
}

// --------------------------- REMOVE TAG ERROR ---------------------------
define_transparent_error! {
    pub enum RemoveTagError,
    stack_message: "Error removing tag in catalog",
    variants: [
        TagNotFound,
        CatalogBackendError
    ]
}

#[derive(thiserror::Error, PartialEq, Debug)]
#[error("No tag with id '{tag_id}' exists")]
pub struct TagNotFound {
    pub tag_id: TagId,
    pub stack: Vec<String>,
}
impl TagNotFound {
    #[must_use]
    pub fn new(tag_id: TagId) -> Self {
        Self {
            tag_id,
            stack: Vec::new(),
        }
    }
}
impl_error_stack_methods!(TagNotFound);
impl From<TagNotFound> for ErrorModel {
    fn from(err: TagNotFound) -> Self {
        ErrorModel::builder()
            .r#type("TagNotFound")
            .code(StatusCode::NOT_FOUND.as_u16())
            .message(err.to_string())
            .stack(err.stack)
            .build()
    }
}

// --------------------------- DELETE TAG DEFINITION ERROR ---------------------------
define_transparent_error! {
    pub enum DeleteTagDefinitionError,
    stack_message: "Error deleting tag definition in catalog",
    variants: [
        TagDefinitionInUse,
        TagDefinitionIdNotFound,
        TagDefinitionReserved,
        CatalogBackendError
    ]
}

#[derive(thiserror::Error, PartialEq, Debug, Default)]
#[error("The tag definition is still applied to one or more targets and cannot be deleted")]
pub struct TagDefinitionInUse {
    pub stack: Vec<String>,
}
impl TagDefinitionInUse {
    #[must_use]
    pub fn new() -> Self {
        Self { stack: Vec::new() }
    }
}
impl_error_stack_methods!(TagDefinitionInUse);
impl From<TagDefinitionInUse> for ErrorModel {
    fn from(err: TagDefinitionInUse) -> Self {
        ErrorModel::builder()
            .r#type("TagDefinitionInUse")
            .code(StatusCode::CONFLICT.as_u16())
            .message(err.to_string())
            .stack(err.stack)
            .build()
    }
}

use crate::service::events::impl_authorization_failure_source;

impl_authorization_failure_source!(CreateTagDefinitionError => InternalCatalogError);
impl_authorization_failure_source!(ListTagDefinitionsError => InternalCatalogError);
impl_authorization_failure_source!(ListTagAttachmentsError => InternalCatalogError);
impl_authorization_failure_source!(UpdateTagDefinitionError => InternalCatalogError);
impl_authorization_failure_source!(DeleteTagDefinitionError => InternalCatalogError);
impl_authorization_failure_source!(ApplyTagError => InternalCatalogError);
impl_authorization_failure_source!(RemoveTagError => InternalCatalogError);
impl_authorization_failure_source!(TagNameNotFound => ResourceNotFound);
impl_authorization_failure_source!(ColumnNotFound => ResourceNotFound);
impl_authorization_failure_source!(TagTargetNotFound => ResourceNotFound);

#[async_trait::async_trait]
pub trait CatalogTagOps
where
    Self: CatalogStore,
{
    async fn create_tag_definition<'a>(
        project_id: &ProjectId,
        request: CatalogCreateTagDefinitionRequest<'_>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<TagDefinition, CreateTagDefinitionError> {
        Self::create_tag_definition_impl(project_id, request, transaction).await
    }

    async fn get_tag_definition(
        project_id: &ProjectId,
        tag_definition_id: TagDefinitionId,
        catalog_state: Self::State,
    ) -> Result<Option<TagDefinition>, CatalogBackendError> {
        Self::get_tag_definition_impl(project_id, tag_definition_id, catalog_state).await
    }

    async fn get_tag_definition_by_name(
        project_id: &ProjectId,
        name: &str,
        catalog_state: Self::State,
    ) -> Result<Option<TagDefinition>, CatalogBackendError> {
        Self::get_tag_definition_by_name_impl(project_id, name, catalog_state).await
    }

    async fn list_tag_definitions(
        project_id: &ProjectId,
        pagination: PaginationQuery,
        catalog_state: Self::State,
    ) -> Result<ListTagDefinitionsResponse, ListTagDefinitionsError> {
        Self::list_tag_definitions_impl(project_id, pagination, catalog_state).await
    }

    async fn get_tag_allowed_values(
        tag_definition_id: TagDefinitionId,
        catalog_state: Self::State,
    ) -> Result<Vec<String>, CatalogBackendError> {
        Self::get_tag_allowed_values_impl(tag_definition_id, catalog_state).await
    }

    async fn update_tag_definition<'a>(
        project_id: &ProjectId,
        tag_definition_id: TagDefinitionId,
        request: UpdateTagDefinitionRequest<'_>,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<(TagDefinition, Vec<String>), UpdateTagDefinitionError> {
        Self::update_tag_definition_impl(project_id, tag_definition_id, request, transaction).await
    }

    async fn delete_tag_definition<'a>(
        project_id: &ProjectId,
        tag_definition_id: TagDefinitionId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<(), DeleteTagDefinitionError> {
        Self::delete_tag_definition_impl(project_id, tag_definition_id, transaction).await
    }

    /// Returns the attachment and whether it changed (`false` = idempotent re-apply of an
    /// identical value → no write, no change event).
    async fn apply_tag<'a>(
        tag_id: TagId,
        tag_definition_id: TagDefinitionId,
        target: TagTarget,
        value: Option<&str>,
        source: TagSource,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<(Tag, bool), ApplyTagError> {
        Self::apply_tag_impl(
            tag_id,
            tag_definition_id,
            target,
            value,
            source,
            transaction,
        )
        .await
    }

    async fn remove_tag<'a>(
        tag_id: TagId,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'a>,
    ) -> Result<(), RemoveTagError> {
        Self::remove_tag_impl(tag_id, transaction).await
    }

    async fn list_tags_for_target(
        target: TagTarget,
        catalog_state: Self::State,
    ) -> Result<Vec<TagWithName>, CatalogBackendError> {
        Self::list_tags_for_target_impl(target, catalog_state).await
    }

    /// The targets a definition is directly attached to (reverse lookup), optionally
    /// filtered to a single `value`, keyset-paginated. Callers must resolve and
    /// authorize the definition first; all attachments of a project-scoped definition
    /// are in-project by construction.
    async fn list_tag_attachments(
        tag_definition_id: TagDefinitionId,
        value_filter: Option<&str>,
        pagination: PaginationQuery,
        catalog_state: Self::State,
    ) -> Result<ListTagAttachmentsResponse, ListTagAttachmentsError> {
        Self::list_tag_attachments_impl(tag_definition_id, value_filter, pagination, catalog_state)
            .await
    }

    /// Gather the candidate effective tags for `target`: its own direct tags plus
    /// tags on its ancestors (parent namespaces + warehouse), each annotated with a
    /// containment `distance` and its [`EffectiveTagSource`]. Unresolved and
    /// unfiltered — the caller applies per-ancestor visibility and most-specific-wins
    /// resolution via [`resolve_effective_tags`]. Direct-only targets (warehouse has
    /// no ancestors; columns never inherit) return only distance-0 candidates.
    async fn list_effective_tag_candidates(
        target: TagTarget,
        catalog_state: Self::State,
    ) -> Result<Vec<EffectiveTagCandidate>, CatalogBackendError> {
        Self::list_effective_tag_candidates_impl(target, catalog_state).await
    }
}

impl<T> CatalogTagOps for T where T: CatalogStore {}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;
    use crate::service::{GenericTableId, TableId, ViewId};

    #[test]
    fn reserved_names_detected_case_insensitively() {
        assert!(is_reserved_tag_name("system"));
        assert!(is_reserved_tag_name("system.pii"));
        assert!(is_reserved_tag_name("System.PII"));
        // bare namespace root is reserved for every prefix, not only "system"
        assert!(is_reserved_tag_name("lakekeeper"));
        assert!(is_reserved_tag_name("Lakekeeper.Internal"));
        assert!(!is_reserved_tag_name("pii.classification"));
        // "system." is the prefix — "systematic"/"lakekeepers" are normal names
        assert!(!is_reserved_tag_name("systematic"));
        assert!(!is_reserved_tag_name("lakekeepers"));
    }

    #[test]
    fn valid_names_accepted() {
        for name in [
            "pii.classification",
            "Personal Information.Email",
            "hr-compliance.gdpr",
            "顧客情報.氏名",
            // special chars allowed (house convention); safety is at the SQL/URL/
            // policy boundaries, not name validation
            "R&D.Cost=Center",
            "region/eu",
        ] {
            assert!(validate_tag_name(name).is_ok(), "{name} should be valid");
        }
    }

    #[test]
    fn invalid_names_rejected() {
        for name in ["", " leading", "trailing ", ".pii", "pii.", "pii..ssn"] {
            assert!(
                validate_tag_name(name).is_err(),
                "{name} should be rejected"
            );
        }
        assert!(validate_tag_name(&"x".repeat(257)).is_err());
        assert!(validate_tag_name(&"x".repeat(256)).is_ok());
    }

    #[test]
    fn validate_tag_value_marker() {
        assert_eq!(validate_tag_value(TagValueKind::Marker, &[], None), Ok(()));
        assert_eq!(
            validate_tag_value(TagValueKind::Marker, &[], Some("x")),
            Err(InvalidTagValue::new("Marker tags do not take a value"))
        );
    }

    #[test]
    fn validate_tag_value_free_text() {
        assert_eq!(
            validate_tag_value(TagValueKind::FreeText, &[], Some("anything")),
            Ok(())
        );
        assert_eq!(
            validate_tag_value(TagValueKind::FreeText, &[], None),
            Err(InvalidTagValue::new("A value is required for this tag"))
        );
        assert_eq!(
            validate_tag_value(TagValueKind::FreeText, &[], Some("")),
            Err(InvalidTagValue::new("A value is required for this tag"))
        );
        // At the cap is fine; one over is rejected (would otherwise overflow the
        // btree index and surface as a 500).
        let at_cap = "x".repeat(MAX_TAG_VALUE_LEN);
        assert_eq!(
            validate_tag_value(TagValueKind::FreeText, &[], Some(&at_cap)),
            Ok(())
        );
        let over_cap = "x".repeat(MAX_TAG_VALUE_LEN + 1);
        assert_eq!(
            validate_tag_value(TagValueKind::FreeText, &[], Some(&over_cap)),
            Err(InvalidTagValue::new(format!(
                "A tag value must be at most {MAX_TAG_VALUE_LEN} characters"
            )))
        );
        // Control characters (e.g. NUL, which Postgres text rejects) are a 400.
        assert_eq!(
            validate_tag_value(TagValueKind::FreeText, &[], Some("a\0b")),
            Err(InvalidTagValue::new(
                "A tag value must not contain control characters"
            ))
        );
    }

    #[test]
    fn validate_tag_value_enumerated() {
        let allowed = vec!["public".to_string(), "internal".to_string()];
        assert_eq!(
            validate_tag_value(TagValueKind::Enumerated, &allowed, Some("public")),
            Ok(())
        );
        assert_eq!(
            validate_tag_value(TagValueKind::Enumerated, &allowed, None),
            Err(InvalidTagValue::new("A value is required for this tag"))
        );
        // Case-sensitive exact match.
        assert_eq!(
            validate_tag_value(TagValueKind::Enumerated, &allowed, Some("Public")),
            Err(InvalidTagValue::new(
                "Value is not in the allowed values for this tag"
            ))
        );
        assert_eq!(
            validate_tag_value(TagValueKind::Enumerated, &allowed, Some("restricted")),
            Err(InvalidTagValue::new(
                "Value is not in the allowed values for this tag"
            ))
        );
    }

    #[test]
    fn validate_tag_scope_accepts_allowed_scope() {
        assert_eq!(
            validate_tag_scope(&[TagScope::Table, TagScope::Column], TagScope::Table),
            Ok(())
        );
        assert_eq!(
            validate_tag_scope(&[TagScope::Table, TagScope::Column], TagScope::Column),
            Ok(())
        );
    }

    #[test]
    fn validate_tag_scope_rejects_disallowed_scope() {
        let err = validate_tag_scope(&[TagScope::Warehouse, TagScope::Column], TagScope::Table)
            .unwrap_err();
        assert_eq!(
            err,
            TagScopeNotAllowed::new("Tag does not allow scope `table`")
        );
        assert_eq!(err.to_string(), "Tag does not allow scope `table`");
        // Serde name is used for multi-word scopes.
        let err = validate_tag_scope(&[TagScope::Table], TagScope::GenericTable).unwrap_err();
        assert_eq!(err.to_string(), "Tag does not allow scope `generic-table`");
        // Empty scope list allows nothing.
        let err = validate_tag_scope(&[], TagScope::Warehouse).unwrap_err();
        assert_eq!(err.to_string(), "Tag does not allow scope `warehouse`");
    }

    #[test]
    fn validate_scope_widening_accepts_supersets() {
        // Identical sets, order-free.
        assert_eq!(
            validate_scope_widening(
                &[TagScope::Table, TagScope::Column],
                &[TagScope::Column, TagScope::Table]
            ),
            Ok(())
        );
        // Strict superset.
        assert_eq!(
            validate_scope_widening(
                &[TagScope::Table],
                &[TagScope::Table, TagScope::View, TagScope::Column]
            ),
            Ok(())
        );
        // Empty current widens to anything.
        assert_eq!(validate_scope_widening(&[], &[TagScope::Warehouse]), Ok(()));
    }

    #[test]
    fn validate_scope_widening_rejects_narrowing() {
        let err = validate_scope_widening(
            &[TagScope::Warehouse, TagScope::Table, TagScope::Column],
            &[TagScope::Table],
        )
        .unwrap_err();
        assert_eq!(
            err,
            TagScopeNarrowed::new(vec!["column".to_string(), "warehouse".to_string()])
        );
        assert_eq!(
            err.to_string(),
            "Tag definition scope cannot be narrowed. Removed scopes: column, warehouse"
        );
    }

    #[test]
    fn tag_scope_round_trips() {
        for scope in [
            TagScope::Warehouse,
            TagScope::Namespace,
            TagScope::Table,
            TagScope::View,
            TagScope::GenericTable,
            TagScope::Column,
        ] {
            assert_eq!(TagScope::parse(scope.as_str()), Some(scope));
        }
        assert_eq!(TagScope::GenericTable.as_str(), "generic-table");
        assert_eq!(TagScope::parse("function"), None);
    }

    fn eff_cand(
        def: u128,
        distance: i32,
        origin: EffectiveTagSource,
        tag: u128,
        value: Option<&str>,
    ) -> EffectiveTagCandidate {
        EffectiveTagCandidate {
            tag_id: TagId::new(Uuid::from_u128(tag)),
            tag_definition_id: TagDefinitionId::new(Uuid::from_u128(def)),
            name: format!("def-{def}"),
            value: value.map(String::from),
            source: TagSource::Manual,
            created_at: DateTime::from_timestamp(0, 0).unwrap(),
            updated_at: None,
            distance,
            origin,
        }
    }

    fn ns_source(ns: u128) -> EffectiveTagSource {
        EffectiveTagSource::Namespace {
            warehouse_id: WarehouseId::new(Uuid::from_u128(1)),
            namespace_id: NamespaceId::new(Uuid::from_u128(ns)),
        }
    }

    fn wh_source() -> EffectiveTagSource {
        EffectiveTagSource::Warehouse {
            warehouse_id: WarehouseId::new(Uuid::from_u128(1)),
        }
    }

    #[test]
    fn resolve_most_specific_wins_with_provenance() {
        // def 10 attached directly (distance 0) and at the warehouse (distance 2);
        // def 20 only at a namespace (distance 1).
        let candidates = vec![
            eff_cand(10, 2, wh_source(), 1, Some("wh")),
            eff_cand(20, 1, ns_source(5), 2, Some("ns")),
            eff_cand(10, 0, EffectiveTagSource::Direct, 3, Some("direct")),
        ];
        let resolved = resolve_effective_tags(candidates);
        // Ordered by distance: def 10 (direct) then def 20 (namespace). def 10's
        // warehouse copy is shadowed.
        assert_eq!(resolved.len(), 2);
        assert_eq!(*resolved[0].tag_definition_id, Uuid::from_u128(10));
        assert_eq!(resolved[0].value.as_deref(), Some("direct"));
        assert_eq!(resolved[0].origin, EffectiveTagSource::Direct);
        assert_eq!(*resolved[1].tag_definition_id, Uuid::from_u128(20));
        assert_eq!(resolved[1].origin, ns_source(5));
    }

    #[test]
    fn resolve_nearest_ancestor_wins() {
        // Same definition on two non-Direct ancestors: namespace (distance 1) and
        // warehouse (distance 2). The nearer must win. The farther copy is listed
        // first and given the smaller tag_id, so a sort that ignored distance (or
        // keyed only on tag_id) would wrongly pick the warehouse.
        let candidates = vec![
            eff_cand(10, 2, wh_source(), 1, Some("far")),
            eff_cand(10, 1, ns_source(5), 2, Some("near")),
        ];
        let resolved = resolve_effective_tags(candidates);
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].origin, ns_source(5));
        assert_eq!(resolved[0].value.as_deref(), Some("near"));
    }

    #[test]
    fn resolve_marker_dedup_across_levels() {
        // Marker def 10 present directly and at the warehouse -> one entry (nearest).
        let candidates = vec![
            eff_cand(10, 2, wh_source(), 2, None),
            eff_cand(10, 0, EffectiveTagSource::Direct, 1, None),
        ];
        let resolved = resolve_effective_tags(candidates);
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].origin, EffectiveTagSource::Direct);
        assert_eq!(resolved[0].value, None);
    }

    #[test]
    fn resolve_same_distance_tiebreak_is_deterministic() {
        // Synthetic same-(distance, source) tie for one definition (cannot arise via
        // the unique key, but the resolver must still be deterministic): smaller
        // tag_id wins.
        let candidates = vec![
            eff_cand(10, 1, ns_source(5), 9, Some("high-tag-id")),
            eff_cand(10, 1, ns_source(5), 2, Some("low-tag-id")),
        ];
        let resolved = resolve_effective_tags(candidates);
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].value.as_deref(), Some("low-tag-id"));
    }

    #[test]
    fn target_scope_reflects_tabular_subtype() {
        let w = WarehouseId::new(Uuid::nil());
        let id = Uuid::from_u128(1);
        assert_eq!(TagTarget::Warehouse(w).scope(), TagScope::Warehouse);
        assert_eq!(
            TagTarget::Tabular {
                warehouse_id: w,
                tabular_id: TabularId::Table(TableId::new(id))
            }
            .scope(),
            TagScope::Table
        );
        assert_eq!(
            TagTarget::Tabular {
                warehouse_id: w,
                tabular_id: TabularId::View(ViewId::new(id))
            }
            .scope(),
            TagScope::View
        );
        assert_eq!(
            TagTarget::Tabular {
                warehouse_id: w,
                tabular_id: TabularId::GenericTable(GenericTableId::new(id)),
            }
            .scope(),
            TagScope::GenericTable
        );
        assert_eq!(
            TagTarget::Column {
                warehouse_id: w,
                tabular_id: TabularId::Table(TableId::new(id)),
                field_id: 4,
            }
            .scope(),
            TagScope::Column
        );
    }
}
