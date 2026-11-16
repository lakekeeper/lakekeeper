//! Governance Tags as Iceberg REST Catalog labels on `loadTable` and `loadView`.

use std::collections::BTreeMap;

use iceberg::spec::TableMetadata;
use iceberg_ext::catalog::rest::Labels;

use crate::{
    WarehouseId,
    api::{ErrorModel, Result},
    service::{
        CatalogStore, CatalogTagOps, EffectiveTagCandidate, TabularId, TagTarget, TagWithName,
        resolve_effective_tags,
    },
};

/// Label value for a marker tag, which has no value of its own.
const MARKER_VALUE: &str = "true";

/// The labels of a table or view, in the canonical form of [`Labels::from_maps`].
///
/// Object labels are the object's effective tags: the set the Management API lists
/// with `effective=true`, inherited from its namespaces and warehouse with the
/// nearest attachment winning. Like that endpoint, they are visible to anyone who
/// may load the object. A table also has field labels: its direct column tags,
/// keyed by field id; columns inherit nothing. A view has none.
///
/// Two queries, run concurrently on the catalog state, so callers must not hold a
/// transaction. A failed read fails the load: a response without its labels could
/// leave unmasked what they classify.
pub(crate) async fn load_labels<C: CatalogStore>(
    warehouse_id: WarehouseId,
    tabular_id: TabularId,
    catalog_state: C::State,
) -> Result<Option<Labels>> {
    let target = TagTarget::Tabular {
        warehouse_id,
        tabular_id,
    };
    // Column tags exist only on tables.
    let column_tags_read = async {
        match tabular_id {
            TabularId::Table(_) => {
                C::list_column_tags_for_tabular(warehouse_id, tabular_id, catalog_state.clone())
                    .await
            }
            TabularId::View(_) | TabularId::GenericTable(_) => Ok(Vec::new()),
        }
    };
    let (candidates, column_tags) = tokio::try_join!(
        C::list_effective_tag_candidates(target, catalog_state.clone()),
        column_tags_read
    )
    .map_err(ErrorModel::from)?;
    Ok(governance_tags_to_labels(
        resolve_effective_tags(candidates),
        column_tags,
    ))
}

/// Project resolved effective tags and direct column tags onto [`Labels`].
///
/// Tag names are label keys unchanged. A marker tag maps to `"true"`; free-text
/// and enumerated tags keep their value. Rows that are not column tags are not
/// field labels and are skipped.
fn governance_tags_to_labels(
    effective_object_tags: Vec<EffectiveTagCandidate>,
    column_tags: Vec<TagWithName>,
) -> Option<Labels> {
    let object_labels = effective_object_tags
        .into_iter()
        .map(|tag| (tag.name, label_value(tag.value)))
        .collect();
    let mut fields = BTreeMap::<i32, BTreeMap<String, String>>::new();
    for row in column_tags {
        let TagTarget::Column { field_id, .. } = row.tag.target else {
            continue;
        };
        fields
            .entry(field_id)
            .or_default()
            .insert(row.definition_name, label_value(row.tag.value));
    }
    Labels::from_maps(object_labels, fields)
}

fn label_value(value: Option<String>) -> String {
    value.unwrap_or_else(|| MARKER_VALUE.to_string())
}

/// Drop field labels whose field id is in no schema of `metadata`, keeping labels
/// for fields any schema of the table still has. A column tag outlives its column, so
/// this is where a dropped field leaves the response.
pub(crate) fn retain_schema_fields(
    labels: Option<Labels>,
    metadata: &TableMetadata,
) -> Option<Labels> {
    let mut labels = labels?;
    labels.fields.retain(|field| {
        metadata
            .schemas_iter()
            .any(|schema| schema.field_by_id(field.field_id).is_some())
    });
    (!labels.is_empty()).then_some(labels)
}

/// A label map from string pairs, for tests.
#[cfg(test)]
pub(crate) fn label_map(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use chrono::DateTime;
    use iceberg::spec::{
        FormatVersion, NestedField, PrimitiveType, Schema, SortOrder, TableMetadataBuilder, Type,
        UnboundPartitionSpec,
    };
    use iceberg_ext::catalog::rest::FieldLabels;
    use uuid::Uuid;

    use super::*;
    use crate::service::{
        EffectiveTagSource, NamespaceId, TableId, Tag, TagDefinitionId, TagId, TagSource,
    };

    fn wh() -> WarehouseId {
        WarehouseId::new(Uuid::from_u128(1))
    }

    fn table() -> TabularId {
        TabularId::Table(TableId::from(Uuid::from_u128(2)))
    }

    fn effective(name: &str, value: Option<&str>) -> EffectiveTagCandidate {
        EffectiveTagCandidate {
            tag_id: TagId::new_random(),
            tag_definition_id: TagDefinitionId::new_random(),
            name: name.to_string(),
            value: value.map(str::to_string),
            source: TagSource::Manual,
            created_at: DateTime::from_timestamp(0, 0).unwrap(),
            updated_at: None,
            distance: 1,
            origin: EffectiveTagSource::Namespace {
                warehouse_id: wh(),
                namespace_id: NamespaceId::new(Uuid::from_u128(3)),
            },
        }
    }

    fn column_tag(field_id: i32, name: &str, value: Option<&str>) -> TagWithName {
        row(
            TagTarget::Column {
                warehouse_id: wh(),
                tabular_id: table(),
                field_id,
            },
            name,
            value,
        )
    }

    fn row(target: TagTarget, name: &str, value: Option<&str>) -> TagWithName {
        TagWithName {
            tag: Tag {
                tag_id: TagId::new_random(),
                tag_definition_id: TagDefinitionId::new_random(),
                target,
                value: value.map(str::to_string),
                source: TagSource::Manual,
                created_at: DateTime::from_timestamp(0, 0).unwrap(),
                updated_at: None,
            },
            definition_name: name.to_string(),
        }
    }

    fn field(field_id: i32, pairs: &[(&str, &str)]) -> FieldLabels {
        FieldLabels::builder()
            .field_id(field_id)
            .labels(label_map(pairs))
            .build()
    }

    #[test]
    fn test_value_kinds_map_to_label_values() {
        let labels = governance_tags_to_labels(
            vec![
                effective("pii", None),
                effective("owner", Some("team-payments")),
                effective("classification", Some("restricted")),
            ],
            vec![],
        )
        .unwrap();
        assert_eq!(
            labels.object_labels,
            label_map(&[
                ("classification", "restricted"),
                ("owner", "team-payments"),
                ("pii", "true"),
            ])
        );
        assert_eq!(labels.fields, vec![]);
    }

    #[test]
    fn test_column_tags_group_per_field_in_id_order() {
        let labels = governance_tags_to_labels(
            vec![],
            vec![
                column_tag(9, "pii-type", Some("email")),
                column_tag(2, "pii", None),
                column_tag(9, "classification", Some("restricted")),
            ],
        )
        .unwrap();
        assert_eq!(labels.object_labels, BTreeMap::new());
        assert_eq!(
            labels.fields,
            vec![
                field(2, &[("pii", "true")]),
                field(
                    9,
                    &[("classification", "restricted"), ("pii-type", "email")]
                ),
            ]
        );
    }

    #[test]
    fn test_input_order_does_not_change_labels() {
        let forward = governance_tags_to_labels(
            vec![effective("a", Some("1")), effective("b", Some("2"))],
            vec![column_tag(1, "x", None), column_tag(5, "y", Some("v"))],
        );
        let reversed = governance_tags_to_labels(
            vec![effective("b", Some("2")), effective("a", Some("1"))],
            vec![column_tag(5, "y", Some("v")), column_tag(1, "x", None)],
        );
        assert_eq!(forward, reversed);
    }

    #[test]
    fn test_rows_that_are_not_column_tags_are_skipped() {
        let labels = governance_tags_to_labels(
            vec![],
            vec![row(
                TagTarget::Tabular {
                    warehouse_id: wh(),
                    tabular_id: table(),
                },
                "pii",
                None,
            )],
        );
        assert_eq!(labels, None);
    }

    #[test]
    fn test_no_tags_give_no_labels() {
        assert_eq!(governance_tags_to_labels(vec![], vec![]), None);
    }

    /// Metadata whose first schema has fields 1 and 2 and whose current schema has
    /// fields 1 and 3: field 2 was dropped, field 3 added.
    fn metadata_with_dropped_field() -> TableMetadata {
        let int = |id, name| NestedField::optional(id, name, Type::Primitive(PrimitiveType::Int));
        let first = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![int(1, "id").into(), int(2, "dropped").into()])
            .build()
            .unwrap();
        let current = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![int(1, "id").into(), int(3, "added").into()])
            .build()
            .unwrap();
        let sort_order = SortOrder::builder().with_order_id(0).build(&first).unwrap();
        TableMetadataBuilder::new(
            first,
            UnboundPartitionSpec::default(),
            sort_order,
            "memory://table".to_string(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .unwrap()
        .add_current_schema(current)
        .unwrap()
        .build()
        .unwrap()
        .metadata
    }

    #[test]
    fn test_field_labels_kept_for_any_schema_and_dropped_for_none() {
        let metadata = metadata_with_dropped_field();
        assert_eq!(metadata.schemas_iter().len(), 2);
        let labels = Labels::from_maps(
            label_map(&[("domain", "payments")]),
            BTreeMap::from([
                (1, label_map(&[("a", "1")])),
                (2, label_map(&[("b", "2")])),
                (3, label_map(&[("c", "3")])),
                (99, label_map(&[("d", "4")])),
            ]),
        );
        let retained = retain_schema_fields(labels, &metadata).unwrap();
        assert_eq!(retained.object_labels, label_map(&[("domain", "payments")]));
        assert_eq!(
            retained.fields,
            vec![
                field(1, &[("a", "1")]),
                field(2, &[("b", "2")]),
                field(3, &[("c", "3")]),
            ]
        );
    }

    #[test]
    fn test_only_unknown_field_labels_give_no_labels() {
        let labels = Labels::from_maps(
            BTreeMap::new(),
            BTreeMap::from([(99, label_map(&[("d", "4")]))]),
        );
        assert_eq!(
            retain_schema_fields(labels, &metadata_with_dropped_field()),
            None
        );
    }
}
