use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogicalSchema {
    pub schema_id: i32,
    pub root_fields: Vec<LogicalField>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogicalField {
    pub field_id: i32,
    pub name: String,
    pub required: bool,
    pub doc: Option<String>,
    pub field_type: LogicalType,
    pub initial_default: Option<serde_json::Value>,
    pub write_default: Option<serde_json::Value>,
    pub is_identity_hint: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogicalType {
    Primitive(LogicalPrimitiveType),
    Struct {
        fields: Vec<LogicalField>,
    },
    List {
        element_field: Box<LogicalField>,
    },
    Map {
        key_field: Box<LogicalField>,
        value_field: Box<LogicalField>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogicalPrimitiveType {
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Decimal {
        precision: u32,
        scale: u32,
    },
    Date,
    Time,
    Timestamp,
    Timestamptz,
    TimestampNs,
    TimestamptzNs,
    String,
    Uuid,
    Fixed {
        length: u64,
    },
    Binary,
    Variant,
}

#[derive(Debug, thiserror::Error)]
pub enum LogicalSchemaError {
    #[error(
        "non-null default is invalid for {kind} field '{name}' (field_id={field_id}); must default to null"
    )]
    NonNullDefaultUnsupported {
        field_id: i32,
        name: String,
        kind: &'static str,
    },
    #[error("logical schema assembly failed: {detail}")]
    Assembly {
        detail: String,
    },
}
