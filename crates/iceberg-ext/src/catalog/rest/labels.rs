use std::collections::BTreeMap;

use typed_builder::TypedBuilder;

/// Catalog-provided labels returned with a table or view on load.
///
/// Labels are optional enrichment: clients may ignore them. A table load caches
/// them under its response `ETag`.
///
/// [`Labels::from_maps`] builds the canonical form: map keys ordered, `fields`
/// sorted by field id with one entry each, and no empty entries. Equal label sets
/// built that way serialize to the same bytes.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, TypedBuilder)]
#[serde(rename_all = "kebab-case")]
pub struct Labels {
    /// Labels on the table or view as a whole.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    #[builder(default)]
    pub object_labels: BTreeMap<String, String>,
    /// Labels on single fields. The catalog emits at most one entry per field id.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[builder(default)]
    pub fields: Vec<FieldLabels>,
}

impl Labels {
    /// The canonical labels for an object and its fields, or `None` if there are
    /// none. Fields without labels are dropped.
    #[must_use]
    pub fn from_maps(
        object_labels: BTreeMap<String, String>,
        fields: BTreeMap<i32, BTreeMap<String, String>>,
    ) -> Option<Self> {
        let labels = Self {
            object_labels,
            fields: fields
                .into_iter()
                .filter(|(_, labels)| !labels.is_empty())
                .map(|(field_id, labels)| FieldLabels { field_id, labels })
                .collect(),
        };
        (!labels.is_empty()).then_some(labels)
    }

    /// True if there are neither object nor field labels.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.object_labels.is_empty() && self.fields.is_empty()
    }
}

/// Labels on one field, identified by its field id in the table or view schema.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TypedBuilder)]
#[serde(rename_all = "kebab-case")]
pub struct FieldLabels {
    /// Field id from the table or view schema.
    pub field_id: i32,
    /// Labels on this field. Required on the wire, and may be empty.
    pub labels: BTreeMap<String, String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn labels() -> Labels {
        Labels::builder()
            .object_labels(BTreeMap::from([
                ("domain".to_string(), "payments".to_string()),
                ("classification".to_string(), "restricted".to_string()),
            ]))
            .fields(vec![
                FieldLabels::builder()
                    .field_id(7)
                    .labels(BTreeMap::from([(
                        "pii-type".to_string(),
                        "email".to_string(),
                    )]))
                    .build(),
            ])
            .build()
    }

    #[test]
    fn test_serializes_to_the_iceberg_wire_shape() {
        assert_eq!(
            serde_json::to_value(labels()).unwrap(),
            serde_json::json!({
                "object-labels": {
                    "classification": "restricted",
                    "domain": "payments"
                },
                "fields": [
                    {"field-id": 7, "labels": {"pii-type": "email"}}
                ]
            })
        );
    }

    #[test]
    fn test_object_labels_serialize_in_key_order() {
        assert_eq!(
            serde_json::to_string(&labels().object_labels).unwrap(),
            r#"{"classification":"restricted","domain":"payments"}"#
        );
    }

    #[test]
    fn test_round_trips() {
        let json = serde_json::to_string(&labels()).unwrap();
        assert_eq!(serde_json::from_str::<Labels>(&json).unwrap(), labels());
    }

    #[test]
    fn test_empty_members_are_omitted() {
        assert_eq!(
            serde_json::to_value(Labels::default()).unwrap(),
            serde_json::json!({})
        );
        let only_fields = Labels {
            object_labels: BTreeMap::new(),
            ..labels()
        };
        assert_eq!(
            serde_json::to_value(only_fields).unwrap(),
            serde_json::json!({"fields": [{"field-id": 7, "labels": {"pii-type": "email"}}]})
        );
    }

    #[test]
    fn test_missing_members_deserialize_as_empty() {
        let parsed: Labels = serde_json::from_str("{}").unwrap();
        assert_eq!(parsed, Labels::default());
        assert!(parsed.is_empty());
    }

    #[test]
    fn test_from_maps_sorts_fields_by_id() {
        let built = Labels::from_maps(
            BTreeMap::new(),
            BTreeMap::from([
                (9, BTreeMap::from([("b".to_string(), "2".to_string())])),
                (3, BTreeMap::from([("a".to_string(), "1".to_string())])),
            ]),
        )
        .unwrap();
        assert_eq!(
            built.fields,
            vec![
                FieldLabels::builder()
                    .field_id(3)
                    .labels(BTreeMap::from([("a".to_string(), "1".to_string())]))
                    .build(),
                FieldLabels::builder()
                    .field_id(9)
                    .labels(BTreeMap::from([("b".to_string(), "2".to_string())]))
                    .build(),
            ]
        );
    }

    #[test]
    fn test_from_maps_drops_empty_fields_and_returns_none_when_empty() {
        assert_eq!(Labels::from_maps(BTreeMap::new(), BTreeMap::new()), None);
        assert_eq!(
            Labels::from_maps(BTreeMap::new(), BTreeMap::from([(1, BTreeMap::new())])),
            None
        );
        let object = BTreeMap::from([("domain".to_string(), "payments".to_string())]);
        assert_eq!(
            Labels::from_maps(object.clone(), BTreeMap::from([(1, BTreeMap::new())])),
            Some(Labels::builder().object_labels(object).build())
        );
    }

    #[test]
    fn test_from_maps_builds_the_same_value_as_the_fixture() {
        let built = Labels::from_maps(
            labels().object_labels,
            BTreeMap::from([(
                7,
                BTreeMap::from([("pii-type".to_string(), "email".to_string())]),
            )]),
        );
        assert_eq!(built, Some(labels()));
    }

    #[test]
    fn test_field_entry_requires_field_id_and_labels() {
        assert!(serde_json::from_str::<FieldLabels>(r#"{"field-id": 1}"#).is_err());
        assert!(serde_json::from_str::<FieldLabels>(r#"{"labels": {}}"#).is_err());
        assert_eq!(
            serde_json::from_str::<FieldLabels>(r#"{"field-id": 1, "labels": {}}"#).unwrap(),
            FieldLabels::builder()
                .field_id(1)
                .labels(BTreeMap::new())
                .build()
        );
    }

    #[test]
    fn test_is_empty_only_without_object_and_field_labels() {
        assert!(Labels::default().is_empty());
        assert!(!labels().is_empty());
        let only_object = Labels {
            fields: vec![],
            ..labels()
        };
        assert!(!only_object.is_empty());
    }
}
