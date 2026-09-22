//! The one bridge between serde and `tracing`.
//!
//! Every object field of an audit record is serialized to a JSON tree and handed to `tracing`
//! as a `valuable` value through [`AuditJson`]. The JSON subscriber then writes it exactly as it
//! writes any `valuable` map or list. Because the tree is built by serde, field order is struct
//! order (`serde_json` runs with `preserve_order`) and absent optionals are absent, not `null`.

use serde::Serialize;
use valuable::{Listable, Mappable, Valuable, Value, Visit};

/// A rendered record part: a JSON tree carried as one `tracing` field.
#[derive(Debug, Clone, PartialEq)]
pub struct AuditJson(serde_json::Value);

impl AuditJson {
    /// Serialize an audit type to its JSON tree.
    ///
    /// # Panics
    ///
    /// Audit types serialize infallibly: string keys, no floats that are not numbers, no maps
    /// with non-string keys. A panic here is a bug in an audit type, not a runtime condition.
    #[must_use]
    pub fn of<T: Serialize + ?Sized>(part: &T) -> Self {
        Self(serde_json::to_value(part).expect("audit types serialize to JSON"))
    }

    /// The tree.
    #[must_use]
    pub fn value(&self) -> &serde_json::Value {
        &self.0
    }
}

impl From<serde_json::Value> for AuditJson {
    fn from(value: serde_json::Value) -> Self {
        Self(value)
    }
}

/// The `valuable` form of a scalar JSON value, borrowing from `v` itself.
fn scalar(v: &serde_json::Value) -> Option<Value<'_>> {
    use serde_json::Value as J;
    Some(match v {
        J::Null => Value::Unit,
        J::Bool(b) => Value::Bool(*b),
        J::Number(n) => n
            .as_i64()
            .map(Value::I64)
            .or_else(|| n.as_u64().map(Value::U64))
            .unwrap_or_else(|| Value::F64(n.as_f64().unwrap_or(f64::NAN))),
        J::String(s) => Value::String(s),
        J::Array(_) | J::Object(_) => return None,
    })
}

fn visit_children(v: &serde_json::Value, visit: &mut dyn Visit) {
    use serde_json::Value as J;
    match v {
        J::Array(items) => {
            for item in items {
                visit.visit_value(Node(item).as_value());
            }
        }
        J::Object(map) => {
            for (k, child) in map {
                visit.visit_entry(Value::String(k), Node(child).as_value());
            }
        }
        other => {
            if let Some(value) = scalar(other) {
                visit.visit_value(value);
            }
        }
    }
}

fn list_len(v: &serde_json::Value) -> (usize, Option<usize>) {
    let n = v.as_array().map_or(0, Vec::len);
    (n, Some(n))
}

fn map_len(v: &serde_json::Value) -> (usize, Option<usize>) {
    let n = v.as_object().map_or(0, serde_json::Map::len);
    (n, Some(n))
}

impl Valuable for AuditJson {
    fn as_value(&self) -> Value<'_> {
        match &self.0 {
            serde_json::Value::Array(_) => Value::Listable(self),
            serde_json::Value::Object(_) => Value::Mappable(self),
            other => scalar(other).expect("scalar"),
        }
    }

    fn visit(&self, visit: &mut dyn Visit) {
        visit_children(&self.0, visit);
    }
}

impl Listable for AuditJson {
    fn size_hint(&self) -> (usize, Option<usize>) {
        list_len(&self.0)
    }
}

impl Mappable for AuditJson {
    fn size_hint(&self) -> (usize, Option<usize>) {
        map_len(&self.0)
    }
}

/// Render a value that still implements `valuable` the old way into a JSON tree, through the
/// same serializer `tracing-subscriber` uses for `valuable` fields, so the result is
/// byte-identical to what the subscriber wrote before. Used for the nested values that the
/// `valuable` derive still renders until they get typed shapes of their own.
pub(crate) fn legacy_json(value: &impl Valuable) -> serde_json::Value {
    serde_json::to_value(valuable_serde::Serializable::new(value))
        .expect("a valuable value serializes to JSON")
}

/// One node of the tree. `Mappable` and `Listable` need a type per node, hence the newtype.
struct Node<'a>(&'a serde_json::Value);

impl Valuable for Node<'_> {
    fn as_value(&self) -> Value<'_> {
        match self.0 {
            serde_json::Value::Array(_) => Value::Listable(self),
            serde_json::Value::Object(_) => Value::Mappable(self),
            other => scalar(other).expect("scalar"),
        }
    }

    fn visit(&self, visit: &mut dyn Visit) {
        visit_children(self.0, visit);
    }
}

impl Listable for Node<'_> {
    fn size_hint(&self) -> (usize, Option<usize>) {
        list_len(self.0)
    }
}

impl Mappable for Node<'_> {
    fn size_hint(&self) -> (usize, Option<usize>) {
        map_len(self.0)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io,
        sync::{Arc, Mutex},
    };

    use serde_json::json;
    use tracing_subscriber::fmt::MakeWriter;

    use super::*;

    #[derive(Clone, Default)]
    struct Captured(Arc<Mutex<Vec<u8>>>);

    impl io::Write for Captured {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().expect("lock").extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for Captured {
        type Writer = Self;
        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    /// Emit `value` through the bridge under the JSON formatter the binary configures, and
    /// return what the subscriber wrote for the field.
    fn through_subscriber(value: &serde_json::Value) -> serde_json::Value {
        let logs = Captured::default();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .flatten_event(true)
            .with_current_span(false)
            .with_span_list(true)
            .with_writer(logs.clone())
            .finish();
        let bridged = AuditJson::from(value.clone());
        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(probe = tracing::field::valuable(&bridged), "probe");
        });
        let line = String::from_utf8(logs.0.lock().expect("lock").clone()).expect("utf-8");
        let record: serde_json::Value = serde_json::from_str(line.trim()).expect("one JSON line");
        record["probe"].clone()
    }

    #[test]
    fn every_json_node_kind_survives_the_bridge_unchanged() {
        let value = json!({
            "string": "s",
            "bool": true,
            "int": -3,
            "uint": 18_446_744_073_709_551_615_u64,
            "float": 1.5,
            "null": null,
            "list": [1, "two", [3], {"four": 4}],
            "object": {"nested": {"deep": ["x"]}},
            "empty_list": [],
            "empty_object": {}
        });
        assert_eq!(through_subscriber(&value), value);
    }

    #[test]
    fn a_scalar_root_is_a_scalar_field() {
        assert_eq!(through_subscriber(&json!("only")), json!("only"));
        assert_eq!(through_subscriber(&json!(7)), json!(7));
        assert_eq!(through_subscriber(&json!(null)), json!(null));
    }

    #[test]
    fn key_order_is_preserved() {
        let value = json!({"z": 1, "a": 2, "m": 3});
        let rendered = through_subscriber(&value);
        let keys: Vec<&String> = rendered.as_object().expect("object").keys().collect();
        assert_eq!(keys, ["z", "a", "m"]);
    }

    #[test]
    fn of_serializes_through_serde() {
        #[derive(serde::Serialize)]
        struct S<'a> {
            a: &'a str,
            #[serde(skip_serializing_if = "Option::is_none")]
            b: Option<u8>,
        }
        assert_eq!(
            AuditJson::of(&S { a: "x", b: None }).value(),
            &json!({"a": "x"})
        );
        assert_eq!(
            AuditJson::of(&S { a: "x", b: Some(1) }).value(),
            &json!({"a": "x", "b": 1})
        );
    }
}
