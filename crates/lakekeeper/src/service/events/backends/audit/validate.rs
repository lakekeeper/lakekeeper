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
    // An operation record's `context` is declared by the emitter that produced the record,
    // not by whoever owns the shape. A record from another emitter carries a context this
    // schema cannot know, and checking it here would demand that every product's contexts be
    // declared in Lakekeeper's schema — the opposite of what the emitter field is for.
    //
    // So `emitters` must positively name this schema's emitter. A record naming only others,
    // or naming none at all, is not checked: an unknown emitter is not this one, and
    // guessing otherwise would report a violation of a contract the record never claimed.
    // Nothing is lost by skipping, because `emitters` is required on every shape, so
    // `assert_valid_record` has already refused a record that carries none.
    let ours = schema["x-audit-emitter"]["name"].as_str();
    let contributed = ours.is_some_and(|ours| record["emitters"].get(ours).is_some());
    if contributed
        && record["record_type"] == "operation"
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

/// The `time` a compared record carries in place of its own.
pub const PINNED_TIME: &str = "2026-01-01T00:00:00.000000Z";

/// Check a captured record's `time` and put [`PINNED_TIME`] in its place, so a record can be
/// compared with a committed one. The clock has no test seam, so the comparison pins what can
/// be pinned: that the field is there, and that it is RFC 3339 in UTC.
///
/// # Panics
///
/// If the record has no `time`, or one that is not RFC 3339 in UTC.
pub fn pin_time(record: &mut Value, whence: &str) {
    let time = record.get("time").and_then(Value::as_str);
    assert!(
        time.is_some_and(|time| {
            time.ends_with('Z') && chrono::DateTime::parse_from_rfc3339(time).is_ok()
        }),
        "{whence}: `time` is {time:?}, not RFC 3339 in UTC"
    );
    record["time"] = PINNED_TIME.into();
}

/// The definition of the shape `record_type` names, or `None` when the schema declares none:
/// the one whose `record_type` property is pinned to that value.
#[must_use]
pub fn shape_of<'a>(schema: &'a Value, record_type: &str) -> Option<(&'a str, &'a Value)> {
    schema["$defs"].as_object()?.iter().find_map(|(name, def)| {
        (def["properties"]["record_type"]["const"] == record_type).then_some((name.as_str(), def))
    })
}

/// Validate a whole record, as a log line carries it, against the schema's root.
///
/// The root is the definition of any audit record: it holds the two stamps and routes on
/// `record_type` to the shape that describes the rest. The keys the log subscriber adds are
/// no shape's, and no shape forbids them, so the line is checked as it is.
///
/// # Panics
///
/// If the record names no shape, names one the schema does not declare, or violates it.
pub fn assert_valid_record(schema: &Value, record: &Value, whence: &str) {
    let record_type = record["record_type"].as_str().unwrap_or_else(|| {
        panic!("{whence}: no `record_type`, so nothing says which shape to check it against")
    });
    assert!(
        shape_of(schema, record_type).is_some(),
        "{whence}: `record_type` is `{record_type}`, which no shape in the schema names"
    );
    assert_valid_part(
        schema,
        super::schema::AUDIT_RECORD,
        record,
        &format!("{whence}: {record_type}"),
    );
    assert_record_parts_valid(schema, record, whence);
}
