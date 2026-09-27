//! Normalized schema storage: flatten `Schema` → `FlatField` rows; assemble rows → `Schema`.
//! Flatten/assemble are pure Rust (no DB); used by the write path (flatten) and read path
//! (assemble). One `#[sqlx::test]` guards that the PG `iceberg_type_kind` enum covers every kind.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use iceberg::spec::{
    ListType, MapType, NestedField, NestedFieldRef, PrimitiveType, Schema, SchemaId, StructType,
    Type, VariantType,
};
use serde_json::Value;

// ─── errors ──────────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum SchemaNormError {
    #[error(
        "non-null default is invalid for {kind} field '{name}' (field_id={field_id}); must default to null"
    )]
    NonNullDefaultUnsupported {
        field_id: i32,
        name: String,
        kind: &'static str,
    },
    #[error("schema assembly failed: {detail}")]
    Assembly { detail: String },
}

// ─── IcebergTypeKind ─────────────────────────────────────────────────────────

/// Discriminator matching the PG `iceberg_type_kind` enum labels exactly: snake_case of the variant
/// name (see `as_str`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::VariantArray, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum IcebergTypeKind {
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Decimal,
    Date,
    Time,
    Timestamp,
    Timestamptz,
    TimestampNs,
    TimestamptzNs,
    String,
    Uuid,
    Fixed,
    Binary,
    Variant,
    Struct,
    List,
    Map,
}

impl IcebergTypeKind {
    /// PG `iceberg_type_kind` label for this kind, derived from the variant name via
    /// `serialize_all = "snake_case"`. `test_type_kind_as_str` pins every expected value.
    pub fn as_str(self) -> &'static str {
        self.into()
    }

    /// Per the Iceberg spec, `unknown`/`variant`/`geometry`/`geography` columns must default to
    /// null — a non-null initial/write-default is invalid. Every other kind permits one.
    /// Exhaustive (no wildcard) so a future kind forces a decision here, like `type_kind_and_params`.
    fn permits_non_null_default(self) -> bool {
        match self {
            IcebergTypeKind::Variant => false,
            IcebergTypeKind::Boolean
            | IcebergTypeKind::Int
            | IcebergTypeKind::Long
            | IcebergTypeKind::Float
            | IcebergTypeKind::Double
            | IcebergTypeKind::Decimal
            | IcebergTypeKind::Date
            | IcebergTypeKind::Time
            | IcebergTypeKind::Timestamp
            | IcebergTypeKind::Timestamptz
            | IcebergTypeKind::TimestampNs
            | IcebergTypeKind::TimestamptzNs
            | IcebergTypeKind::String
            | IcebergTypeKind::Uuid
            | IcebergTypeKind::Fixed
            | IcebergTypeKind::Binary
            | IcebergTypeKind::Struct
            | IcebergTypeKind::List
            | IcebergTypeKind::Map => true,
        }
    }
}

// ─── FlatField ───────────────────────────────────────────────────────────────

/// One row per field node (including list element, map key/value nodes).
#[derive(Debug, Clone, PartialEq)]
pub struct FlatField {
    pub field_id: i32,
    pub parent_field_id: Option<i32>,
    pub ordinal: i32,
    pub name: std::string::String,
    pub required: bool,
    pub doc: Option<std::string::String>,
    pub type_kind: IcebergTypeKind,
    pub type_params: Option<Value>,
    pub initial_default: Option<Value>,
    pub write_default: Option<Value>,
    /// Whether this field is in the schema's identifier_field_ids set.
    pub is_identifier: bool,
}

// ─── SchemaFieldRow ──────────────────────────────────────────────────────────

/// DB row shape returned from `schema_field`. One row per field; `is_identifier`
/// marks membership in the schema's identifier set, collected back into
/// identifier_field_ids on assembly.
#[derive(Debug, Clone)]
pub struct SchemaFieldRow {
    pub schema_id: i32,
    pub field_id: i32,
    pub parent_field_id: Option<i32>,
    pub ordinal: i32,
    pub name: std::string::String,
    pub required: bool,
    pub doc: Option<std::string::String>,
    pub type_kind: std::string::String,
    pub type_params: Option<Value>,
    pub initial_default: Option<Value>,
    pub write_default: Option<Value>,
    pub is_identifier: bool,
}

// ─── flatten_schema ──────────────────────────────────────────────────────────

/// Walk `schema` and emit one `FlatField` per node (struct fields, list
/// elements, map keys, map values — all included).
pub fn flatten_schema(schema: &Schema) -> Result<Vec<FlatField>, SchemaNormError> {
    // identifier_field_ids is schema-level; pass it down so each field sets its own flag in one
    // pass. Referential integrity is free: an identifier can only be an existing field row.
    let identifiers: std::collections::HashSet<i32> = schema.identifier_field_ids().collect();
    let mut out = Vec::new();
    flatten_struct(schema.as_struct(), None, &identifiers, &mut out)?;
    Ok(out)
}

fn flatten_struct(
    s: &StructType,
    parent_field_id: Option<i32>,
    identifiers: &std::collections::HashSet<i32>,
    out: &mut Vec<FlatField>,
) -> Result<(), SchemaNormError> {
    for (ordinal, field) in s.fields().iter().enumerate() {
        flatten_field(field, parent_field_id, ordinal as i32, identifiers, out)?;
    }
    Ok(())
}

fn flatten_field(
    field: &NestedFieldRef,
    parent_field_id: Option<i32>,
    ordinal: i32,
    identifiers: &std::collections::HashSet<i32>,
    out: &mut Vec<FlatField>,
) -> Result<(), SchemaNormError> {
    let (type_kind, type_params) = type_kind_and_params(&field.field_type);
    let initial_default = match &field.initial_default {
        None => None,
        Some(lit) => {
            if !type_kind.permits_non_null_default() {
                return Err(SchemaNormError::NonNullDefaultUnsupported {
                    field_id: field.id,
                    name: field.name.clone(),
                    kind: type_kind.as_str(),
                });
            }
            let json = lit.clone().try_into_json(&field.field_type).map_err(|e| {
                SchemaNormError::Assembly {
                    detail: format!(
                        "initial_default serialize failed for field '{}' (field_id={}): {e}",
                        field.name, field.id
                    ),
                }
            })?;
            Some(json)
        }
    };
    let write_default = match &field.write_default {
        None => None,
        Some(lit) => {
            if !type_kind.permits_non_null_default() {
                return Err(SchemaNormError::NonNullDefaultUnsupported {
                    field_id: field.id,
                    name: field.name.clone(),
                    kind: type_kind.as_str(),
                });
            }
            let json = lit.clone().try_into_json(&field.field_type).map_err(|e| {
                SchemaNormError::Assembly {
                    detail: format!(
                        "write_default serialize failed for field '{}' (field_id={}): {e}",
                        field.name, field.id
                    ),
                }
            })?;
            Some(json)
        }
    };

    out.push(FlatField {
        field_id: field.id,
        parent_field_id,
        ordinal,
        name: field.name.clone(),
        required: field.required,
        doc: field.doc.clone(),
        type_kind,
        type_params,
        initial_default,
        write_default,
        is_identifier: identifiers.contains(&field.id),
    });

    // Recurse into children
    match field.field_type.as_ref() {
        Type::Struct(s) => {
            flatten_struct(s, Some(field.id), identifiers, out)?;
        }
        Type::List(list) => {
            flatten_field(&list.element_field, Some(field.id), 0, identifiers, out)?;
        }
        Type::Map(map) => {
            flatten_field(&map.key_field, Some(field.id), 0, identifiers, out)?;
            flatten_field(&map.value_field, Some(field.id), 1, identifiers, out)?;
        }
        // Primitives and Variant have no children
        Type::Primitive(_) | Type::Variant(_) => {}
    }

    Ok(())
}

/// Exhaustive match — a future new `Type` or `PrimitiveType` arm **must** fail the build.
fn type_kind_and_params(ty: &Type) -> (IcebergTypeKind, Option<Value>) {
    match ty {
        Type::Primitive(p) => match p {
            PrimitiveType::Boolean => (IcebergTypeKind::Boolean, None),
            PrimitiveType::Int => (IcebergTypeKind::Int, None),
            PrimitiveType::Long => (IcebergTypeKind::Long, None),
            PrimitiveType::Float => (IcebergTypeKind::Float, None),
            PrimitiveType::Double => (IcebergTypeKind::Double, None),
            PrimitiveType::Decimal { precision, scale } => (
                IcebergTypeKind::Decimal,
                Some(serde_json::json!({ "precision": precision, "scale": scale })),
            ),
            PrimitiveType::Date => (IcebergTypeKind::Date, None),
            PrimitiveType::Time => (IcebergTypeKind::Time, None),
            PrimitiveType::Timestamp => (IcebergTypeKind::Timestamp, None),
            PrimitiveType::Timestamptz => (IcebergTypeKind::Timestamptz, None),
            PrimitiveType::TimestampNs => (IcebergTypeKind::TimestampNs, None),
            PrimitiveType::TimestamptzNs => (IcebergTypeKind::TimestamptzNs, None),
            PrimitiveType::String => (IcebergTypeKind::String, None),
            PrimitiveType::Uuid => (IcebergTypeKind::Uuid, None),
            PrimitiveType::Fixed(length) => (
                IcebergTypeKind::Fixed,
                Some(serde_json::json!({ "length": length })),
            ),
            PrimitiveType::Binary => (IcebergTypeKind::Binary, None),
        },
        Type::Struct(_) => (IcebergTypeKind::Struct, None),
        Type::List(_) => (IcebergTypeKind::List, None),
        Type::Map(_) => (IcebergTypeKind::Map, None),
        Type::Variant(_) => (IcebergTypeKind::Variant, None),
    }
}

// ─── assemble_schemas ────────────────────────────────────────────────────────

/// Reassemble the schemas for a tabular from its flat `schema_field` rows.
///
/// Assembly is anchor-driven: `expected_schema_ids` is the authoritative set of schema ids (the
/// `table_schema` / `view_schema` anchor rows). Any expected id with no field rows reconstructs as
/// an empty-fields schema — a zero-column schema is valid in Iceberg (it persists an anchor but no
/// `schema_field` rows), so this keeps the round-trip lossless instead of silently dropping it.
/// The caller is responsible for rejecting an *empty current schema*, where zero rows is far more
/// likely to mean lost rows than a legitimately empty schema.
pub fn assemble_schemas(
    rows: Vec<SchemaFieldRow>,
    expected_schema_ids: &[SchemaId],
) -> Result<HashMap<SchemaId, Arc<Schema>>, SchemaNormError> {
    // Group rows by schema_id
    let mut by_schema: HashMap<i32, Vec<SchemaFieldRow>> = HashMap::new();
    let mut identifier_ids_by_schema: HashMap<i32, Vec<i32>> = HashMap::new();

    for row in rows {
        let schema_id = row.schema_id;
        // identifier_field_ids = the field_ids flagged is_identifier in this schema.
        if row.is_identifier {
            identifier_ids_by_schema
                .entry(schema_id)
                .or_default()
                .push(row.field_id);
        }
        by_schema.entry(schema_id).or_default().push(row);
    }

    let mut result = HashMap::with_capacity(by_schema.len());

    for (schema_id, rows) in by_schema {
        // One pass: bucket children by parent (top-level under None), each bucket ordinal-sorted.
        let children = build_children_index(&rows);
        let top_level = children.get(&None).map(Vec::as_slice).unwrap_or_default();

        // Track every row reached by the top-down traversal. Each field's build_field records its
        // field_id exactly once (a second visit means a duplicate row or a cycle → rejected in
        // build_field). After the walk, every input row must have been consumed.
        let mut consumed: HashSet<i32> = HashSet::with_capacity(rows.len());
        let mut fields = Vec::with_capacity(top_level.len());
        for &row in top_level {
            fields.push(build_field(row, &children, &mut consumed)?);
        }
        // A row left unconsumed is unreachable from the schema root — its parent_field_id points at a
        // primitive (which consumes no children), a nonexistent field, or a disconnected cycle. Such a
        // row would silently vanish from the assembled schema, dropping a column; reject it instead.
        if consumed.len() != rows.len() {
            let orphans: Vec<i32> = rows
                .iter()
                .map(|r| r.field_id)
                .filter(|id| !consumed.contains(id))
                .collect();
            return Err(SchemaNormError::Assembly {
                detail: format!(
                    "schema_id={schema_id}: {} field row(s) unreachable from the schema root \
                     (parent_field_id orphaned or attached to a non-container field): field_ids={orphans:?}",
                    rows.len() - consumed.len(),
                ),
            });
        }

        // remove (not get + clone): each schema_id is assembled once, so move the Vec out.
        let ident_ids = identifier_ids_by_schema
            .remove(&schema_id)
            .unwrap_or_default();

        let schema = Schema::builder()
            .with_schema_id(schema_id)
            .with_identifier_field_ids(ident_ids)
            .with_fields(fields)
            .build()
            .map_err(|e| SchemaNormError::Assembly {
                detail: format!("Schema::builder().build() failed for schema_id={schema_id}: {e}"),
            })?;

        result.insert(schema_id, Arc::new(schema));
    }

    // Seed any expected anchor that produced no rows as an empty-fields schema (see doc above).
    for &schema_id in expected_schema_ids {
        if result.contains_key(&schema_id) {
            continue;
        }
        let schema = Schema::builder()
            .with_schema_id(schema_id)
            .build()
            .map_err(|e| SchemaNormError::Assembly {
                detail: format!("empty Schema::build() failed for schema_id={schema_id}: {e}"),
            })?;
        result.insert(schema_id, Arc::new(schema));
    }

    Ok(result)
}

/// `parent_field_id → that parent's children, ordinal-sorted` (top-level fields under `None`).
/// Built once per schema so tree assembly is O(N), not O(N²) repeated scans.
type ChildrenIndex<'a> = HashMap<Option<i32>, Vec<&'a SchemaFieldRow>>;

fn build_children_index(rows: &[SchemaFieldRow]) -> ChildrenIndex<'_> {
    let mut idx: HashMap<Option<i32>, Vec<&SchemaFieldRow>> = HashMap::with_capacity(rows.len());
    for r in rows {
        idx.entry(r.parent_field_id).or_default().push(r);
    }
    for kids in idx.values_mut() {
        kids.sort_by_key(|r| r.ordinal);
    }
    idx
}

fn children_of<'idx, 'row>(
    index: &'idx ChildrenIndex<'row>,
    parent_field_id: i32,
) -> &'idx [&'row SchemaFieldRow] {
    index
        .get(&Some(parent_field_id))
        .map(Vec::as_slice)
        .unwrap_or_default()
}

fn build_field<'row>(
    row: &'row SchemaFieldRow,
    children: &ChildrenIndex<'row>,
    consumed: &mut HashSet<i32>,
) -> Result<NestedFieldRef, SchemaNormError> {
    // Reject a field reached twice — a duplicate row (same field_id) or a parent/child cycle — before
    // it can double-assemble a subtree.
    if !consumed.insert(row.field_id) {
        return Err(SchemaNormError::Assembly {
            detail: format!(
                "field '{}' (field_id={}) reached more than once during assembly (duplicate row or cycle)",
                row.name, row.field_id
            ),
        });
    }
    let field_type = build_type(row, children, consumed)?;
    let mut field = if row.required {
        NestedField::required(row.field_id, row.name.clone(), field_type)
    } else {
        NestedField::optional(row.field_id, row.name.clone(), field_type)
    };

    if let Some(doc) = &row.doc {
        field = field.with_doc(doc.clone());
    }

    if let Some(json_val) = &row.initial_default {
        let lit = iceberg::spec::Literal::try_from_json(json_val.clone(), &field.field_type)
            .map_err(|e| SchemaNormError::Assembly {
                detail: format!(
                    "initial_default parse failed for field '{}' (field_id={}): {e}",
                    row.name, row.field_id
                ),
            })?
            .ok_or_else(|| SchemaNormError::Assembly {
                detail: format!(
                    "initial_default was null JSON for field '{}' (field_id={})",
                    row.name, row.field_id
                ),
            })?;
        field = field.with_initial_default(lit);
    }

    if let Some(json_val) = &row.write_default {
        let lit = iceberg::spec::Literal::try_from_json(json_val.clone(), &field.field_type)
            .map_err(|e| SchemaNormError::Assembly {
                detail: format!(
                    "write_default parse failed for field '{}' (field_id={}): {e}",
                    row.name, row.field_id
                ),
            })?
            .ok_or_else(|| SchemaNormError::Assembly {
                detail: format!(
                    "write_default was null JSON for field '{}' (field_id={})",
                    row.name, row.field_id
                ),
            })?;
        field = field.with_write_default(lit);
    }

    Ok(Arc::new(field))
}

fn build_type<'row>(
    row: &'row SchemaFieldRow,
    children: &ChildrenIndex<'row>,
    consumed: &mut HashSet<i32>,
) -> Result<Type, SchemaNormError> {
    // A node's children play roles fixed by THIS node's type_kind, in ordinal order:
    // struct → fields; list → its single element; map → key (ordinal 0) + value (ordinal 1).
    // So no stored `role` is needed — the parent's type dictates it.
    match row.type_kind.as_str() {
        "struct" => {
            let kids = children_of(children, row.field_id);
            let mut fields = Vec::with_capacity(kids.len());
            for &child_row in kids {
                fields.push(build_field(child_row, children, consumed)?);
            }
            Ok(Type::Struct(StructType::new(fields)))
        }
        "list" => {
            let kids = children_of(children, row.field_id);
            if kids.len() != 1 {
                return Err(SchemaNormError::Assembly {
                    detail: format!(
                        "list field '{}' (field_id={}) must have exactly 1 child, found {}",
                        row.name,
                        row.field_id,
                        kids.len()
                    ),
                });
            }
            // The element must sit at ordinal 0 (as flatten writes it); a stray ordinal signals a
            // corrupt row set.
            if kids[0].ordinal != 0 {
                return Err(SchemaNormError::Assembly {
                    detail: format!(
                        "list field '{}' (field_id={}) element must have ordinal 0, found {}",
                        row.name, row.field_id, kids[0].ordinal
                    ),
                });
            }
            let elem_field = build_field(kids[0], children, consumed)?;
            Ok(Type::List(ListType::new(elem_field)))
        }
        "map" => {
            let kids = children_of(children, row.field_id);
            if kids.len() != 2 {
                return Err(SchemaNormError::Assembly {
                    detail: format!(
                        "map field '{}' (field_id={}) must have exactly 2 children (key, value), found {}",
                        row.name,
                        row.field_id,
                        kids.len()
                    ),
                });
            }
            // buckets are ordinal-sorted: key = ordinal 0, value = ordinal 1 (set by flatten).
            // Require exactly {0, 1} — a missing, duplicate, or stray ordinal would silently swap or
            // mis-map key/value.
            if kids[0].ordinal != 0 || kids[1].ordinal != 1 {
                return Err(SchemaNormError::Assembly {
                    detail: format!(
                        "map field '{}' (field_id={}) children must have ordinals 0 (key) and 1 (value), found {} and {}",
                        row.name, row.field_id, kids[0].ordinal, kids[1].ordinal
                    ),
                });
            }
            let key_field = build_field(kids[0], children, consumed)?;
            let val_field = build_field(kids[1], children, consumed)?;
            Ok(Type::Map(MapType::new(key_field, val_field)))
        }
        "variant" => Ok(Type::Variant(VariantType)),
        kind => primitive_from_row(row, kind),
    }
}

fn primitive_from_row(row: &SchemaFieldRow, kind: &str) -> Result<Type, SchemaNormError> {
    let p = match kind {
        "boolean" => PrimitiveType::Boolean,
        "int" => PrimitiveType::Int,
        "long" => PrimitiveType::Long,
        "float" => PrimitiveType::Float,
        "double" => PrimitiveType::Double,
        "decimal" => {
            let params = row
                .type_params
                .as_ref()
                .ok_or_else(|| SchemaNormError::Assembly {
                    detail: format!(
                        "decimal field '{}' (field_id={}) missing type_params",
                        row.name, row.field_id
                    ),
                })?;
            let precision_u64 = params
                .get("precision")
                .and_then(|v| v.as_u64())
                .ok_or_else(|| SchemaNormError::Assembly {
                    detail: format!(
                        "decimal field '{}' (field_id={}) missing precision in type_params",
                        row.name, row.field_id
                    ),
                })?;
            let precision =
                u32::try_from(precision_u64).map_err(|_| SchemaNormError::Assembly {
                    detail: format!(
                        "decimal field '{}' (field_id={}) precision {precision_u64} out of u32 range",
                        row.name, row.field_id
                    ),
                })?;
            let scale_u64 = params
                .get("scale")
                .and_then(|v| v.as_u64())
                .ok_or_else(|| SchemaNormError::Assembly {
                    detail: format!(
                        "decimal field '{}' (field_id={}) missing scale in type_params",
                        row.name, row.field_id
                    ),
                })?;
            let scale = u32::try_from(scale_u64).map_err(|_| SchemaNormError::Assembly {
                detail: format!(
                    "decimal field '{}' (field_id={}) scale {scale_u64} out of u32 range",
                    row.name, row.field_id
                ),
            })?;
            PrimitiveType::Decimal { precision, scale }
        }
        "date" => PrimitiveType::Date,
        "time" => PrimitiveType::Time,
        "timestamp" => PrimitiveType::Timestamp,
        "timestamptz" => PrimitiveType::Timestamptz,
        "timestamp_ns" => PrimitiveType::TimestampNs,
        "timestamptz_ns" => PrimitiveType::TimestamptzNs,
        "string" => PrimitiveType::String,
        "uuid" => PrimitiveType::Uuid,
        "fixed" => {
            let params = row
                .type_params
                .as_ref()
                .ok_or_else(|| SchemaNormError::Assembly {
                    detail: format!(
                        "fixed field '{}' (field_id={}) missing type_params",
                        row.name, row.field_id
                    ),
                })?;
            let length = params
                .get("length")
                .and_then(|v| v.as_u64())
                .ok_or_else(|| SchemaNormError::Assembly {
                    detail: format!(
                        "fixed field '{}' (field_id={}) missing length in type_params",
                        row.name, row.field_id
                    ),
                })?;
            PrimitiveType::Fixed(length)
        }
        "binary" => PrimitiveType::Binary,
        other => {
            return Err(SchemaNormError::Assembly {
                detail: format!(
                    "unknown type_kind '{other}' for field '{}' (field_id={})",
                    row.name, row.field_id
                ),
            });
        }
    };
    Ok(Type::Primitive(p))
}

// ─── Helper: FlatField → SchemaFieldRow ──────────────────────────────────────

/// Convert a `FlatField` into a `SchemaFieldRow` for a given `schema_id`.
/// Test-only bridge: production flattens to write, and assembles from DB rows; nothing
/// converts Flat→Row outside round-trip tests.
#[cfg(test)]
pub fn flat_to_row(flat: &FlatField, schema_id: i32) -> SchemaFieldRow {
    SchemaFieldRow {
        schema_id,
        field_id: flat.field_id,
        parent_field_id: flat.parent_field_id,
        ordinal: flat.ordinal,
        name: flat.name.clone(),
        required: flat.required,
        doc: flat.doc.clone(),
        type_kind: flat.type_kind.as_str().to_string(),
        type_params: flat.type_params.clone(),
        initial_default: flat.initial_default.clone(),
        write_default: flat.write_default.clone(),
        is_identifier: flat.is_identifier,
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use iceberg::spec::{
        ListType, MapType, NestedField, PrimitiveType, Schema, StructType, Type, VariantType,
    };

    use super::*;

    fn flat_schema() -> Schema {
        Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap()
    }

    fn nested_struct_schema() -> Schema {
        // struct with two sibling nested structs each with children
        let address_struct = Type::Struct(StructType::new(vec![
            NestedField::required(10, "street", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(11, "city", Type::Primitive(PrimitiveType::String)).into(),
        ]));
        let contact_struct = Type::Struct(StructType::new(vec![
            NestedField::required(20, "email", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(21, "phone", Type::Primitive(PrimitiveType::String)).into(),
        ]));
        Schema::builder()
            .with_schema_id(2)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::required(2, "address", address_struct).into(),
                NestedField::optional(3, "contact", contact_struct).into(),
            ])
            .build()
            .unwrap()
    }

    fn list_schema() -> Schema {
        let list_type = Type::List(ListType::new(
            NestedField::list_element(5, Type::Primitive(PrimitiveType::String), true).into(),
        ));
        Schema::builder()
            .with_schema_id(3)
            .with_fields(vec![NestedField::required(1, "tags", list_type).into()])
            .build()
            .unwrap()
    }

    fn two_maps_schema() -> Schema {
        // Two maps in one schema
        let map1 = Type::Map(MapType::new(
            NestedField::map_key_element(10, Type::Primitive(PrimitiveType::String)).into(),
            NestedField::map_value_element(11, Type::Primitive(PrimitiveType::Long), true).into(),
        ));
        let map2 = Type::Map(MapType::new(
            NestedField::map_key_element(12, Type::Primitive(PrimitiveType::String)).into(),
            NestedField::map_value_element(13, Type::Primitive(PrimitiveType::Int), false).into(),
        ));
        Schema::builder()
            .with_schema_id(4)
            .with_fields(vec![
                NestedField::required(1, "props", map1).into(),
                NestedField::optional(2, "counts", map2).into(),
            ])
            .build()
            .unwrap()
    }

    fn decimal_schema() -> Schema {
        Schema::builder()
            .with_schema_id(5)
            .with_fields(vec![
                NestedField::required(
                    1,
                    "amount",
                    Type::Primitive(PrimitiveType::Decimal {
                        precision: 10,
                        scale: 2,
                    }),
                )
                .into(),
            ])
            .build()
            .unwrap()
    }

    fn uuid_schema() -> Schema {
        Schema::builder()
            .with_schema_id(6)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Uuid)).into(),
            ])
            .build()
            .unwrap()
    }

    fn identifier_fields_schema() -> Schema {
        Schema::builder()
            .with_schema_id(7)
            .with_identifier_field_ids(vec![1, 2])
            .with_fields(vec![
                NestedField::required(1, "pk1", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::required(2, "pk2", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(3, "value", Type::Primitive(PrimitiveType::Double)).into(),
            ])
            .build()
            .unwrap()
    }

    fn variant_schema() -> Schema {
        Schema::builder()
            .with_schema_id(8)
            .with_fields(vec![
                NestedField::required(1, "data", Type::Variant(VariantType)).into(),
            ])
            .build()
            .unwrap()
    }

    fn fixed_schema() -> Schema {
        Schema::builder()
            .with_schema_id(9)
            .with_fields(vec![
                NestedField::required(1, "hash", Type::Primitive(PrimitiveType::Fixed(32))).into(),
            ])
            .build()
            .unwrap()
    }

    fn default_schema() -> Schema {
        // Schema with integer default value
        let field = NestedField::optional(1, "count", Type::Primitive(PrimitiveType::Int))
            .with_write_default(iceberg::spec::Literal::Primitive(
                iceberg::spec::PrimitiveLiteral::Int(0),
            ));
        Schema::builder()
            .with_schema_id(10)
            .with_fields(vec![Arc::new(field)])
            .build()
            .unwrap()
    }

    fn doc_schema() -> Schema {
        let field = NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long))
            .with_doc("The primary key");
        Schema::builder()
            .with_schema_id(11)
            .with_fields(vec![Arc::new(field)])
            .build()
            .unwrap()
    }

    // One field of every IcebergTypeKind (primitives + variant + struct/list/map containers).
    fn all_types_schema() -> Schema {
        let fields = vec![
            NestedField::required(1, "f_bool", Type::Primitive(PrimitiveType::Boolean)).into(),
            NestedField::required(2, "f_int", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(3, "f_long", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(4, "f_float", Type::Primitive(PrimitiveType::Float)).into(),
            NestedField::required(5, "f_double", Type::Primitive(PrimitiveType::Double)).into(),
            NestedField::required(
                6,
                "f_decimal",
                Type::Primitive(PrimitiveType::Decimal {
                    precision: 10,
                    scale: 2,
                }),
            )
            .into(),
            NestedField::required(7, "f_date", Type::Primitive(PrimitiveType::Date)).into(),
            NestedField::required(8, "f_time", Type::Primitive(PrimitiveType::Time)).into(),
            NestedField::required(9, "f_ts", Type::Primitive(PrimitiveType::Timestamp)).into(),
            NestedField::required(10, "f_tstz", Type::Primitive(PrimitiveType::Timestamptz)).into(),
            NestedField::required(11, "f_tsns", Type::Primitive(PrimitiveType::TimestampNs)).into(),
            NestedField::required(
                12,
                "f_tstzns",
                Type::Primitive(PrimitiveType::TimestamptzNs),
            )
            .into(),
            NestedField::required(13, "f_string", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(14, "f_uuid", Type::Primitive(PrimitiveType::Uuid)).into(),
            NestedField::required(15, "f_fixed", Type::Primitive(PrimitiveType::Fixed(16))).into(),
            NestedField::required(16, "f_binary", Type::Primitive(PrimitiveType::Binary)).into(),
            NestedField::required(17, "f_variant", Type::Variant(VariantType)).into(),
            NestedField::required(
                18,
                "f_struct",
                Type::Struct(StructType::new(vec![
                    NestedField::required(19, "s_int", Type::Primitive(PrimitiveType::Int)).into(),
                ])),
            )
            .into(),
            NestedField::required(
                20,
                "f_list",
                Type::List(ListType::new(
                    NestedField::list_element(21, Type::Primitive(PrimitiveType::Int), true).into(),
                )),
            )
            .into(),
            NestedField::required(
                22,
                "f_map",
                Type::Map(MapType::new(
                    NestedField::map_key_element(23, Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::map_value_element(24, Type::Primitive(PrimitiveType::Int), false)
                        .into(),
                )),
            )
            .into(),
        ];
        Schema::builder()
            .with_schema_id(0)
            .with_fields(fields)
            .build()
            .unwrap()
    }

    fn round_trip(schema: &Schema) {
        let flat = flatten_schema(schema).expect("flatten failed");
        let schema_id = schema.schema_id();
        let rows: Vec<SchemaFieldRow> = flat.iter().map(|f| flat_to_row(f, schema_id)).collect();
        let assembled = assemble_schemas(rows, &[]).expect("assemble failed");
        let got = assembled
            .get(&schema_id)
            .expect("assembled schema not found");
        assert_eq!(
            got.as_ref(),
            schema,
            "round-trip failed for schema_id={schema_id}"
        );
        // Fidelity is semantic, not byte-identical: identifier-field-ids is a set, so serialized
        // array order isn't stable. Assert the assembled schema serializes and PARSES BACK equal.
        let json = serde_json::to_value(got.as_ref()).expect("serialize assembled failed");
        let reparsed: Schema = serde_json::from_value(json).expect("parse-back failed");
        assert_eq!(
            &reparsed, schema,
            "JSON parse-back failed for schema_id={schema_id}"
        );
    }

    #[test]
    fn test_flat_round_trip() {
        round_trip(&flat_schema());
    }

    /// Every `IcebergTypeKind` must survive flatten → assemble. `build_type`/`primitive_from_row`
    /// match on `type_kind` strings with a wildcard, so a new type added to the enum + `flatten` but
    /// forgotten on the read side would write OK yet fail to load. The coverage assertion (produced
    /// kinds == all `VARIANTS`) forces the new type into this schema, and the round-trip then proves
    /// assemble handles it.
    #[test]
    fn test_all_type_kinds_round_trip() {
        use strum::VariantArray;

        let schema = all_types_schema();
        let flat = flatten_schema(&schema).expect("flatten failed");

        let produced: std::collections::BTreeSet<&str> =
            flat.iter().map(|f| f.type_kind.as_str()).collect();
        let all: std::collections::BTreeSet<&str> = IcebergTypeKind::VARIANTS
            .iter()
            .map(|k| k.as_str())
            .collect();
        assert_eq!(
            produced, all,
            "all_types_schema must exercise every IcebergTypeKind (add the missing type here)"
        );

        let rows: Vec<SchemaFieldRow> = flat
            .iter()
            .map(|f| flat_to_row(f, schema.schema_id()))
            .collect();
        let assembled = assemble_schemas(rows, &[]).expect("assemble failed");
        assert_eq!(
            assembled
                .get(&schema.schema_id())
                .expect("assembled schema missing")
                .as_ref(),
            &schema,
            "all-types flatten/assemble round-trip mismatch"
        );
    }

    #[test]
    fn test_nested_struct_round_trip() {
        round_trip(&nested_struct_schema());
    }

    #[test]
    fn test_list_round_trip() {
        round_trip(&list_schema());
    }

    #[test]
    fn test_two_maps_round_trip() {
        round_trip(&two_maps_schema());
    }

    #[test]
    fn test_decimal_round_trip() {
        round_trip(&decimal_schema());
    }

    #[test]
    fn test_uuid_round_trip() {
        round_trip(&uuid_schema());
    }

    #[test]
    fn test_identifier_fields_round_trip() {
        round_trip(&identifier_fields_schema());
    }

    #[test]
    fn test_variant_round_trip() {
        round_trip(&variant_schema());
    }

    #[test]
    fn test_fixed_round_trip() {
        round_trip(&fixed_schema());
    }

    #[test]
    fn test_write_default_round_trip() {
        round_trip(&default_schema());
    }

    #[test]
    fn test_doc_round_trip() {
        round_trip(&doc_schema());
    }

    #[test]
    fn test_variant_default_rejected() {
        // A variant field carrying a default must be rejected by flatten.
        let field = NestedField::required(1, "data", Type::Variant(VariantType))
            .with_initial_default(iceberg::spec::Literal::Primitive(
                iceberg::spec::PrimitiveLiteral::Int(0),
            ));
        // If iceberg-rust's builder already rejects a variant default, rejection is enforced
        // upstream and there's nothing for flatten to catch — either outcome is acceptable.
        let Ok(schema) = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![Arc::new(field)])
            .build()
        else {
            return;
        };
        let err = flatten_schema(&schema).unwrap_err();
        assert!(
            matches!(
                err,
                SchemaNormError::NonNullDefaultUnsupported { field_id: 1, .. }
            ),
            "expected NonNullDefaultUnsupported, got {err:?}"
        );
    }

    #[test]
    fn test_fixed_binary_defaults_round_trip() {
        // The pinned iceberg-rust codec round-trips fixed/binary defaults (JSON hex), so flatten →
        // assemble must preserve them (they were rejected before the rev that implemented the codec).
        let fixed = NestedField::optional(1, "f", Type::Primitive(PrimitiveType::Fixed(2)))
            .with_initial_default(iceberg::spec::Literal::Primitive(
                iceberg::spec::PrimitiveLiteral::Binary(vec![0x00, 0x01]),
            ));
        let binary = NestedField::optional(2, "b", Type::Primitive(PrimitiveType::Binary))
            .with_write_default(iceberg::spec::Literal::Primitive(
                iceberg::spec::PrimitiveLiteral::Binary(vec![0xca, 0xfe]),
            ));
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![Arc::new(fixed), Arc::new(binary)])
            .build()
            .expect("schema with fixed/binary defaults must build");
        round_trip(&schema);
    }

    #[test]
    fn test_unknown_type_kind_errors() {
        let row = SchemaFieldRow {
            schema_id: 1,
            field_id: 99,
            parent_field_id: None,
            ordinal: 0,
            name: "x".to_string(),
            required: true,
            doc: None,
            type_kind: "unknown_type_xyz".to_string(),
            type_params: None,
            initial_default: None,
            write_default: None,
            is_identifier: false,
        };
        let err = assemble_schemas(vec![row], &[]).unwrap_err();
        assert!(matches!(err, SchemaNormError::Assembly { .. }));
    }

    #[test]
    fn assemble_reconstructs_fixed_binary_defaults() {
        // The codec implements the fixed/binary JSON path, so assemble must reconstruct persisted
        // defaults (JSON hex) rather than error.
        let fixed_with_default = SchemaFieldRow {
            schema_id: 1,
            field_id: 1,
            parent_field_id: None,
            ordinal: 0,
            name: "f".to_string(),
            required: false,
            doc: None,
            type_kind: "fixed".to_string(),
            type_params: Some(serde_json::json!({"length": 16})),
            initial_default: Some(serde_json::json!("00112233445566778899aabbccddeeff")),
            write_default: None,
            is_identifier: false,
        };
        let fixed_schema = assemble_schemas(vec![fixed_with_default], &[])
            .expect("fixed default must assemble")
            .remove(&1)
            .expect("schema 1");
        assert!(
            fixed_schema
                .field_by_id(1)
                .unwrap()
                .initial_default
                .is_some(),
            "fixed initial_default must survive assembly"
        );

        let binary_with_default = SchemaFieldRow {
            schema_id: 1,
            field_id: 1,
            parent_field_id: None,
            ordinal: 0,
            name: "b".to_string(),
            required: false,
            doc: None,
            type_kind: "binary".to_string(),
            type_params: None,
            initial_default: None,
            write_default: Some(serde_json::json!("cafe")),
            is_identifier: false,
        };
        let binary_schema = assemble_schemas(vec![binary_with_default], &[])
            .expect("binary default must assemble")
            .remove(&1)
            .expect("schema 1");
        assert!(
            binary_schema
                .field_by_id(1)
                .unwrap()
                .write_default
                .is_some(),
            "binary write_default must survive assembly"
        );
    }

    /// Compact `SchemaFieldRow` builder for malformed-input assembly tests.
    fn sfr(type_kind: &str, field_id: i32, parent: Option<i32>, ordinal: i32) -> SchemaFieldRow {
        SchemaFieldRow {
            schema_id: 1,
            field_id,
            parent_field_id: parent,
            ordinal,
            name: format!("f{field_id}"),
            required: false,
            doc: None,
            type_kind: type_kind.to_string(),
            type_params: None,
            initial_default: None,
            write_default: None,
            is_identifier: false,
        }
    }

    #[test]
    fn assemble_rejects_unreachable_row() {
        // field 2 is a child of primitive field 1, which consumes no children, so it is unreachable
        // and would silently drop from the schema. Assembly must reject it, naming the orphan.
        let rows = vec![sfr("long", 1, None, 0), sfr("long", 2, Some(1), 0)];
        let err = assemble_schemas(rows, &[]).unwrap_err();
        assert!(
            matches!(&err, SchemaNormError::Assembly { detail }
                if detail.contains("unreachable") && detail.contains("[2]")),
            "expected unreachable-row rejection naming field_id 2, got {err:?}"
        );
    }

    #[test]
    fn assemble_rejects_bad_map_ordinals() {
        // A map's two children must sit at ordinals 0 (key) and 1 (value). Here both are ordinal 0,
        // which would let the sort silently pick an arbitrary key/value — reject it.
        let rows = vec![
            sfr("map", 1, None, 0),
            sfr("long", 2, Some(1), 0),
            sfr("long", 3, Some(1), 0),
        ];
        let err = assemble_schemas(rows, &[]).unwrap_err();
        assert!(
            matches!(&err, SchemaNormError::Assembly { detail } if detail.contains("ordinal")),
            "expected map ordinal rejection, got {err:?}"
        );
    }

    #[test]
    fn test_multi_version() {
        // Assemble two schema versions at once
        let s1 = flat_schema();
        let s2 = nested_struct_schema();
        let flat1 = flatten_schema(&s1).unwrap();
        let flat2 = flatten_schema(&s2).unwrap();
        let mut rows: Vec<SchemaFieldRow> = flat1
            .iter()
            .map(|f| flat_to_row(f, s1.schema_id()))
            .collect();
        rows.extend(flat2.iter().map(|f| flat_to_row(f, s2.schema_id())));
        let assembled = assemble_schemas(rows, &[]).unwrap();
        assert_eq!(assembled.len(), 2);
        assert_eq!(assembled.get(&s1.schema_id()).unwrap().as_ref(), &s1);
        assert_eq!(assembled.get(&s2.schema_id()).unwrap().as_ref(), &s2);
    }

    #[test]
    fn assemble_seeds_empty_schema_for_expected_id_without_rows() {
        // schema 5 has fields (reconstructed from rows); schema 7 is an expected anchor with no
        // field rows -> reconstructed as an empty-fields schema instead of vanishing.
        let s = flat_schema();
        let rows: Vec<SchemaFieldRow> = flatten_schema(&s)
            .unwrap()
            .iter()
            .map(|f| flat_to_row(f, 5))
            .collect();

        let assembled = assemble_schemas(rows, &[5, 7]).unwrap();

        assert_eq!(assembled.len(), 2);
        assert!(!assembled.get(&5).unwrap().as_struct().fields().is_empty());
        let empty = assembled
            .get(&7)
            .expect("expected anchor 7 must be seeded as an empty schema");
        assert_eq!(empty.schema_id(), 7);
        assert!(empty.as_struct().fields().is_empty());
    }

    #[test]
    fn test_field_row_count_flat() {
        let schema = flat_schema();
        let flat = flatten_schema(&schema).unwrap();
        // 2 top-level fields, no nesting
        assert_eq!(flat.len(), 2);
    }

    #[test]
    fn test_field_row_count_nested_struct() {
        let schema = nested_struct_schema();
        let flat = flatten_schema(&schema).unwrap();
        // Top-level: id(1), address(1), contact(1) = 3
        // address children: street(1), city(1) = 2
        // contact children: email(1), phone(1) = 2
        // Total = 7
        assert_eq!(flat.len(), 7);
    }

    #[test]
    fn test_field_row_count_list() {
        let schema = list_schema();
        let flat = flatten_schema(&schema).unwrap();
        // top-level list field + element = 2
        assert_eq!(flat.len(), 2);
    }

    #[test]
    fn test_field_row_count_two_maps() {
        let schema = two_maps_schema();
        let flat = flatten_schema(&schema).unwrap();
        // map1 field + key + value = 3; map2 field + key + value = 3 → total 6
        assert_eq!(flat.len(), 6);
    }

    #[test]
    fn test_type_kind_as_str() {
        assert_eq!(IcebergTypeKind::Boolean.as_str(), "boolean");
        assert_eq!(IcebergTypeKind::Int.as_str(), "int");
        assert_eq!(IcebergTypeKind::Long.as_str(), "long");
        assert_eq!(IcebergTypeKind::Float.as_str(), "float");
        assert_eq!(IcebergTypeKind::Double.as_str(), "double");
        assert_eq!(IcebergTypeKind::Decimal.as_str(), "decimal");
        assert_eq!(IcebergTypeKind::Date.as_str(), "date");
        assert_eq!(IcebergTypeKind::Time.as_str(), "time");
        assert_eq!(IcebergTypeKind::Timestamp.as_str(), "timestamp");
        assert_eq!(IcebergTypeKind::Timestamptz.as_str(), "timestamptz");
        assert_eq!(IcebergTypeKind::TimestampNs.as_str(), "timestamp_ns");
        assert_eq!(IcebergTypeKind::TimestamptzNs.as_str(), "timestamptz_ns");
        assert_eq!(IcebergTypeKind::String.as_str(), "string");
        assert_eq!(IcebergTypeKind::Uuid.as_str(), "uuid");
        assert_eq!(IcebergTypeKind::Fixed.as_str(), "fixed");
        assert_eq!(IcebergTypeKind::Binary.as_str(), "binary");
        assert_eq!(IcebergTypeKind::Variant.as_str(), "variant");
        assert_eq!(IcebergTypeKind::Struct.as_str(), "struct");
        assert_eq!(IcebergTypeKind::List.as_str(), "list");
        assert_eq!(IcebergTypeKind::Map.as_str(), "map");
    }

    // ─── nested-container + nested-default coverage ────────────────────────────

    fn list_of_structs_schema() -> Schema {
        // list<struct<a:int, b:string>>
        let elem = Type::Struct(StructType::new(vec![
            NestedField::required(3, "a", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(4, "b", Type::Primitive(PrimitiveType::String)).into(),
        ]));
        let list = Type::List(ListType::new(
            NestedField::list_element(2, elem, true).into(),
        ));
        Schema::builder()
            .with_schema_id(12)
            .with_fields(vec![NestedField::required(1, "items", list).into()])
            .build()
            .unwrap()
    }

    fn map_of_lists_schema() -> Schema {
        // map<string, list<int>>
        let value_list = Type::List(ListType::new(
            NestedField::list_element(4, Type::Primitive(PrimitiveType::Int), true).into(),
        ));
        let map = Type::Map(MapType::new(
            NestedField::map_key_element(2, Type::Primitive(PrimitiveType::String)).into(),
            NestedField::map_value_element(3, value_list, true).into(),
        ));
        Schema::builder()
            .with_schema_id(13)
            .with_fields(vec![NestedField::required(1, "by_key", map).into()])
            .build()
            .unwrap()
    }

    fn deeply_nested_schema() -> Schema {
        // struct<inner: list<map<string,int>>> — struct → list → map → key/value
        let inner_map = Type::Map(MapType::new(
            NestedField::map_key_element(4, Type::Primitive(PrimitiveType::String)).into(),
            NestedField::map_value_element(5, Type::Primitive(PrimitiveType::Int), true).into(),
        ));
        let inner_list = Type::List(ListType::new(
            NestedField::list_element(3, inner_map, true).into(),
        ));
        let outer = Type::Struct(StructType::new(vec![
            NestedField::required(2, "inner", inner_list).into(),
        ]));
        Schema::builder()
            .with_schema_id(14)
            .with_fields(vec![NestedField::required(1, "outer", outer).into()])
            .build()
            .unwrap()
    }

    fn nested_default_schema() -> Schema {
        // struct<count: int = 0> — write_default on a NESTED field
        let count = NestedField::optional(2, "count", Type::Primitive(PrimitiveType::Int))
            .with_write_default(iceberg::spec::Literal::Primitive(
                iceberg::spec::PrimitiveLiteral::Int(0),
            ));
        let s = Type::Struct(StructType::new(vec![Arc::new(count)]));
        Schema::builder()
            .with_schema_id(15)
            .with_fields(vec![NestedField::required(1, "wrap", s).into()])
            .build()
            .unwrap()
    }

    fn initial_default_schema() -> Schema {
        let field = NestedField::optional(1, "x", Type::Primitive(PrimitiveType::Int))
            .with_initial_default(iceberg::spec::Literal::Primitive(
                iceberg::spec::PrimitiveLiteral::Int(5),
            ));
        Schema::builder()
            .with_schema_id(16)
            .with_fields(vec![Arc::new(field)])
            .build()
            .unwrap()
    }

    #[test]
    fn test_list_of_structs_round_trip() {
        round_trip(&list_of_structs_schema());
    }

    #[test]
    fn test_map_of_lists_round_trip() {
        round_trip(&map_of_lists_schema());
    }

    #[test]
    fn test_deeply_nested_round_trip() {
        round_trip(&deeply_nested_schema());
    }

    #[test]
    fn test_nested_default_round_trip() {
        round_trip(&nested_default_schema());
    }

    #[test]
    fn test_initial_default_round_trip() {
        round_trip(&initial_default_schema());
    }

    // ─── container-VALUED defaults ─────────────────────────────────────────────
    // A list/map-valued default exercises the same flatten(try_into_json) /
    // assemble(try_from_json) path as scalar defaults, but over a non-primitive
    // Literal. iceberg-rust supports both directions for List/Map literals.

    #[test]
    fn test_list_valued_default_round_trip() {
        // list<int> with write_default [1, 2]
        let list = Type::List(ListType::new(
            NestedField::list_element(2, Type::Primitive(PrimitiveType::Int), true).into(),
        ));
        let field = NestedField::required(1, "nums", list).with_write_default(
            iceberg::spec::Literal::List(vec![
                Some(iceberg::spec::Literal::int(1)),
                Some(iceberg::spec::Literal::int(2)),
            ]),
        );
        let schema = Schema::builder()
            .with_schema_id(17)
            .with_fields(vec![Arc::new(field)])
            .build()
            .expect("builder rejected list-valued default");
        round_trip(&schema);
    }

    #[test]
    fn test_map_valued_default_round_trip() {
        use iceberg::spec::{Literal, Map, PrimitiveLiteral};
        // map<string,int> with write_default {"a": 1}
        let map = Type::Map(MapType::new(
            NestedField::map_key_element(2, Type::Primitive(PrimitiveType::String)).into(),
            NestedField::map_value_element(3, Type::Primitive(PrimitiveType::Int), true).into(),
        ));
        let field =
            NestedField::required(1, "by_key", map).with_write_default(Literal::Map(Map::from([
                (
                    Literal::Primitive(PrimitiveLiteral::String("a".to_string())),
                    Some(Literal::Primitive(PrimitiveLiteral::Int(1))),
                ),
            ])));
        let schema = Schema::builder()
            .with_schema_id(18)
            .with_fields(vec![Arc::new(field)])
            .build()
            .expect("builder rejected map-valued default");
        round_trip(&schema);
    }
}

#[cfg(test)]
mod pg_enum_db_test {
    use strum::VariantArray;

    use super::*;

    // PG-side guard (DB test): the `iceberg_type_kind` enum must contain every kind Rust can
    // emit. It is a SUPERSET — it also pre-includes released v3 types (geometry/geography/unknown)
    // not yet in iceberg-rust, so adopting those is a pure-Rust change with no enum migration.
    #[sqlx::test]
    async fn pg_enum_covers_all_type_kinds(pool: sqlx::PgPool) {
        let labels: Vec<String> = sqlx::query_scalar(
            "SELECT enumlabel FROM pg_enum e JOIN pg_type t ON e.enumtypid = t.oid \
             WHERE t.typname = 'iceberg_type_kind'",
        )
        .fetch_all(&pool)
        .await
        .expect("query pg_enum");
        for &kind in IcebergTypeKind::VARIANTS {
            assert!(
                labels.iter().any(|l| l == kind.as_str()),
                "PG iceberg_type_kind enum is missing '{}' — add it via migration",
                kind.as_str()
            );
        }
        for pre in ["geometry", "geography", "unknown"] {
            assert!(
                labels.iter().any(|l| l == pre),
                "pre-added v3 label '{pre}' missing from iceberg_type_kind"
            );
        }
    }
}
