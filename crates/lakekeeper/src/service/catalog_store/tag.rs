//! Governance-tag entities: a project-scoped vocabulary (`TagDefinition`) and
//! per-target attachments (`Tag`). Value validation (name format, scope) lives
//! here so it surfaces as typed `400`s; referential/structural integrity is in
//! the schema.

use chrono::{DateTime, Utc};
use iceberg_ext::catalog::rest::ErrorModel;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    ProjectId,
    service::{NamespaceId, TabularId, WarehouseId},
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

/// Target types a `TagDefinition` may be applied to. Stored as `text` so adding
/// a type is a Rust-only change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TagScope {
    Project,
    Warehouse,
    Namespace,
    Table,
    View,
    GenericTable,
    Column,
}

impl TagScope {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            TagScope::Project => "project",
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
            "project" => TagScope::Project,
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

/// How a tag came to exist. Server-stamped; only `Manual` from a public write.
/// Automated producers (classification, external-catalog sync) add variants when
/// they ship — mirrors the `tag_source` DB enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TagSource {
    Manual,
}

impl TagSource {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            TagSource::Manual => "manual",
        }
    }

    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "manual" => TagSource::Manual,
            _ => return None,
        })
    }
}

/// How a definition's value is constrained. Explicit per definition (never
/// inferred from allowed-value rows) — mirrors the `tag_value_kind` DB enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TagValueKind {
    /// Presence only; no value (e.g. `Pii`, `Deprecated`).
    Marker,
    /// Arbitrary free-text value.
    FreeText,
    /// Value must be one of the definition's allowed values.
    Enumerated,
}

impl TagValueKind {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            TagValueKind::Marker => "marker",
            TagValueKind::FreeText => "free_text",
            TagValueKind::Enumerated => "enumerated",
        }
    }

    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "marker" => TagValueKind::Marker,
            "free_text" => TagValueKind::FreeText,
            "enumerated" => TagValueKind::Enumerated,
            _ => return None,
        })
    }
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
    pub tag_definition_id: Uuid,
    pub project_id: ProjectId,
    pub name: String,
    pub description: Option<String>,
    pub scope: Vec<TagScope>,
    pub value_kind: TagValueKind,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub updated_by: String,
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
    pub tag_id: Uuid,
    pub tag_definition_id: Uuid,
    pub target: TagTarget,
    pub value: Option<String>,
    pub source: TagSource,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub updated_by: String,
}

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
            assert!(validate_tag_name(name).is_err(), "{name} should be rejected");
        }
        assert!(validate_tag_name(&"x".repeat(257)).is_err());
        assert!(validate_tag_name(&"x".repeat(256)).is_ok());
    }

    #[test]
    fn tag_scope_round_trips() {
        for scope in [
            TagScope::Project,
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

    #[test]
    fn tag_source_round_trips() {
        assert_eq!(TagSource::parse(TagSource::Manual.as_str()), Some(TagSource::Manual));
        assert_eq!(TagSource::parse("manual"), Some(TagSource::Manual));
        // values not yet added to the enum do not parse
        assert_eq!(TagSource::parse("external_catalog"), None);
        assert_eq!(TagSource::parse("unknown"), None);
    }

    #[test]
    fn tag_value_kind_round_trips() {
        for kind in [TagValueKind::Marker, TagValueKind::FreeText, TagValueKind::Enumerated] {
            assert_eq!(TagValueKind::parse(kind.as_str()), Some(kind));
        }
        assert_eq!(TagValueKind::Enumerated.as_str(), "enumerated");
        assert_eq!(TagValueKind::parse("unknown"), None);
    }

    #[test]
    fn target_scope_reflects_tabular_subtype() {
        let w = WarehouseId::new(Uuid::nil());
        let id = Uuid::from_u128(1);
        assert_eq!(TagTarget::Warehouse(w).scope(), TagScope::Warehouse);
        assert_eq!(
            TagTarget::Tabular { warehouse_id: w, tabular_id: TabularId::Table(TableId::new(id)) }
                .scope(),
            TagScope::Table
        );
        assert_eq!(
            TagTarget::Tabular { warehouse_id: w, tabular_id: TabularId::View(ViewId::new(id)) }
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
