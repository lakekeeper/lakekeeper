//! Validation of emitted records against an emitter's committed schema.
//!
//! Available under `test-utils`, so the unit tests, the integration tests and the tests of
//! other emitting crates share one implementation.

use serde_json::{Value, json};

/// Validate `value` against the definition `def` of `schema`, naming every violation.
///
/// # Panics
///
/// If the definition does not exist, or the value violates it. That is the point: this is for
/// use in tests.
pub fn assert_valid_part(schema: &Value, def: &str, value: &Value, whence: &str) {
    assert!(
        schema["$defs"].get(def).is_some(),
        "{whence}: the schema has no definition `{def}`"
    );
    let against = json!({
        "$schema": schema["$schema"],
        "$ref": format!("#/$defs/{def}"),
        "$defs": schema["$defs"],
    });
    let validator = jsonschema::validator_for(&against).unwrap_or_else(|e| {
        panic!("{whence}: schema definition `{def}` is not a valid schema: {e}")
    });
    let errors: Vec<String> = validator
        .iter_errors(value)
        .map(|e| format!("{} at {}", e, e.instance_path()))
        .collect();
    assert!(
        errors.is_empty(),
        "{whence}: `{def}` violated:\n  - {}\n\nvalue:\n{}",
        errors.join("\n  - "),
        serde_json::to_string_pretty(value).unwrap_or_default()
    );
}

/// Whether `value` satisfies the definition `def` of `schema`.
#[must_use]
pub fn is_valid_part(schema: &Value, def: &str, value: &Value) -> bool {
    let against = json!({
        "$schema": schema["$schema"],
        "$ref": format!("#/$defs/{def}"),
        "$defs": schema["$defs"],
    });
    jsonschema::validator_for(&against).is_ok_and(|v| v.is_valid(value))
}

/// Validate every nested object of one record against the schema, by its position in the
/// record: `actor`, the action and entity objects in either arity, every `authorizations[]`
/// entry, `error`, and the `context` of an operation record against every registered context.
///
/// # Panics
///
/// On the first violation, naming the record and the definition.
pub fn assert_record_parts_valid(schema: &Value, record: &Value, whence: &str) {
    if let Some(actor) = record.get("actor") {
        assert_valid_part(schema, "ActorRecord", actor, &format!("{whence}: actor"));
    }
    for (singular, plural, def) in [
        ("action", "actions", "ActionRecord"),
        ("entity", "entities", "EntityRecord"),
    ] {
        if let Some(one) = record.get(singular) {
            assert_valid_part(schema, def, one, &format!("{whence}: {singular}"));
        }
        if let Some(many) = record.get(plural).and_then(Value::as_array) {
            for (i, one) in many.iter().enumerate() {
                assert_valid_part(schema, def, one, &format!("{whence}: {plural}[{i}]"));
            }
        }
    }
    if let Some(entries) = record.get("authorizations").and_then(Value::as_array) {
        for (i, entry) in entries.iter().enumerate() {
            assert_valid_part(
                schema,
                "DecisionRecord",
                entry,
                &format!("{whence}: authorizations[{i}]"),
            );
        }
    }
    if let Some(error) = record.get("error") {
        assert_valid_part(schema, "ErrorRecord", error, &format!("{whence}: error"));
    }
    // An operation record's context is one of the emitter's declared contexts.
    if record.get("operation").is_some()
        && record.get("decision").is_none()
        && let Some(context) = record.get("context")
    {
        let contexts: Vec<&String> = schema["$defs"]
            .as_object()
            .map(|d| {
                d.iter()
                    .filter(|(_, v)| v["x-audit-kind"].as_str() == Some("context"))
                    .map(|(k, _)| k)
                    .collect()
            })
            .unwrap_or_default();
        assert!(
            contexts
                .iter()
                .any(|def| is_valid_part(schema, def, context)),
            "{whence}: the operation record's `context` matches none of the declared contexts \
             {contexts:?}:\n{}",
            serde_json::to_string_pretty(context).unwrap_or_default()
        );
    }
}
