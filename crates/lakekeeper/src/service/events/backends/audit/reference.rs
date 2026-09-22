//! The documentation's field reference, rendered from an emitter's schema.
//!
//! Generated, committed next to the hand-written prose, and checked against the schema by a
//! test, so a field description lives in exactly one place: the doc comment on the field.
//!
//! Available under `test-utils`, alongside the schema builders it reads.

use std::fmt::Write as _;

use serde_json::Value;

/// Render the reference page for `schema`.
#[must_use]
pub fn render_reference(schema: &Value) -> String {
    let emitter = &schema["x-audit-emitter"];
    let name = emitter["name"].as_str().unwrap_or("?");
    let format = emitter["format"].as_str().unwrap_or("?");
    let mut out = String::new();
    let _ = write!(out, "# Audit format reference: `{name}` {format}\n\n");
    out.push_str(
        "<!-- Generated from audit-format/schema.json by `just update-audit-schema`. Do not edit; \
         change the doc comment on the field and regenerate. -->\n\n",
    );
    let _ = write!(
        out,
        "Every object and every closed set of values that records emitted by `{name}` can carry. \
         Field descriptions are the doc comments of the emitting types. Optional fields are absent \
         when not recorded unless a description says otherwise.\n\n"
    );

    let defs = schema["$defs"].as_object();
    let Some(defs) = defs else {
        return out;
    };
    for (kind, heading, intro) in [
        (
            "shape",
            "Records",
            "The top-level structures, named by `record_type`.",
        ),
        ("part", "Objects", "Nested objects of a record."),
        (
            "context",
            "Operation contexts",
            "The `context` object of operation records, one per operation kind.",
        ),
        (
            "enum",
            "Values",
            "Closed sets of values, by the field that carries them.",
        ),
    ] {
        let mut entries: Vec<(&String, &Value)> = defs
            .iter()
            .filter(|(_, v)| v["x-audit-kind"].as_str() == Some(kind))
            .collect();
        if entries.is_empty() {
            continue;
        }
        if kind == "enum" {
            entries.sort_by_key(|(n, v)| {
                (
                    v["x-audit-field"].as_str().unwrap_or("").to_string(),
                    (*n).clone(),
                )
            });
        }
        let _ = write!(out, "## {heading}\n\n{intro}\n\n");
        for (name, def) in entries {
            render_def(&mut out, name, def);
        }
    }
    out
}

fn render_def(out: &mut String, name: &str, def: &Value) {
    let _ = write!(out, "### `{name}`\n\n");
    if let Some(desc) = def["description"].as_str() {
        out.push_str(desc.trim());
        out.push_str("\n\n");
    }
    if let Some(values) = def["enum"].as_array() {
        if let Some(field) = def["x-audit-field"].as_str() {
            let _ = write!(out, "Values of `{field}`:\n\n");
        }
        for v in values {
            let _ = writeln!(out, "- `{}`", v.as_str().unwrap_or("?"));
        }
        out.push('\n');
        return;
    }
    if let Some(variants) = def["anyOf"].as_array().or_else(|| def["oneOf"].as_array()) {
        out.push_str("One of:\n\n");
        for v in variants {
            let _ = writeln!(out, "- {}", type_of(v));
        }
        out.push('\n');
        return;
    }
    let Some(props) = def["properties"].as_object() else {
        if let Some(extra) = def.get("additionalProperties") {
            let _ = write!(
                out,
                "An object whose keys are data and whose values are {}.\n\n",
                type_of(extra)
            );
        }
        return;
    };
    let required: Vec<&str> = def["required"]
        .as_array()
        .map(|r| r.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();
    out.push_str("| Field | Type | Present | Description |\n|---|---|---|---|\n");
    for (prop, spec) in props {
        let presence = if required.contains(&prop.as_str()) {
            "always"
        } else {
            "optional"
        };
        let desc = spec["description"]
            .as_str()
            .unwrap_or("")
            .replace('\n', " ");
        let _ = writeln!(
            out,
            "| `{prop}` | {} | {presence} | {desc} |",
            type_of(spec)
        );
    }
    if let Some(extra) = def.get("additionalProperties")
        && extra != &Value::Bool(false)
    {
        let _ = writeln!(
            out,
            "| *any other key* | {} | optional | Keys from a closed set, see the values section. |",
            type_of(extra)
        );
    }
    out.push('\n');
}

/// A short rendering of a property's type: a link for a reference, a word for a primitive.
fn type_of(spec: &Value) -> String {
    if let Some(reference) = spec["$ref"].as_str() {
        let name = reference.rsplit('/').next().unwrap_or(reference);
        return format!("[`{name}`](#{})", name.to_lowercase());
    }
    if let Some(variants) = spec["anyOf"]
        .as_array()
        .or_else(|| spec["oneOf"].as_array())
    {
        let rendered: Vec<String> = variants
            .iter()
            .filter(|v| v["type"].as_str() != Some("null"))
            .map(type_of)
            .collect();
        return rendered.join(" or ");
    }
    match &spec["type"] {
        Value::String(t) if t == "array" => format!("array of {}", type_of(&spec["items"])),
        Value::String(t) => t.clone(),
        Value::Array(ts) => ts
            .iter()
            .filter_map(Value::as_str)
            .filter(|t| *t != "null")
            .collect::<Vec<_>>()
            .join(" or "),
        _ => "any".to_string(),
    }
}
