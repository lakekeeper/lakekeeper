use iceberg::spec::ViewMetadataRef;

#[cfg(feature = "axum")]
use crate::catalog::rest::impl_into_response;
use crate::{
    catalog::{
        TableIdent, ViewRequirement,
        rest::{Labels, ViewUpdate},
    },
    spec::{Schema, ViewVersion},
};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CreateViewRequest {
    #[serde(rename = "name")]
    pub name: String,
    #[serde(rename = "location", skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
    #[serde(rename = "schema")]
    pub schema: Schema,
    #[serde(rename = "view-version")]
    pub view_version: ViewVersion,
    #[serde(default, rename = "properties")]
    pub properties: std::collections::HashMap<String, String>,
}

/// Result used when a view is successfully loaded.
/// The view metadata JSON is returned in the `metadata` field.
/// The corresponding file location of view metadata is returned in the `metadata-location` field.
/// Clients can check whether metadata has changed by comparing metadata locations after the view
/// has been created.  The `config` map returns view-specific configuration for the view's resources.  
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LoadViewResult {
    #[serde(rename = "metadata-location")]
    pub metadata_location: String,
    #[serde(rename = "metadata")]
    pub metadata: ViewMetadataRef,
    #[serde(rename = "config", skip_serializing_if = "Option::is_none")]
    pub config: Option<std::collections::HashMap<String, String>>,
    /// Catalog-provided labels for the view. Omitted when the view has none.
    #[serde(default, rename = "labels", skip_serializing_if = "Option::is_none")]
    pub labels: Option<Labels>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct CommitViewRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identifier: Option<TableIdent>,
    #[serde(rename = "requirements", skip_serializing_if = "Option::is_none")]
    pub requirements: Option<Vec<ViewRequirement>>,
    pub updates: Vec<ViewUpdate>,
}

#[cfg(feature = "axum")]
impl_into_response!(LoadViewResult);

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use iceberg::{
        NamespaceIdent,
        spec::{
            NestedField, PrimitiveType, Schema, SqlViewRepresentation, Type, ViewFormatVersion,
            ViewMetadataBuilder, ViewRepresentation, ViewRepresentations, ViewVersion,
        },
    };

    use super::*;

    fn view_metadata() -> ViewMetadataRef {
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .unwrap();
        let representations = ViewRepresentations::builder()
            .add_representation(ViewRepresentation::Sql(SqlViewRepresentation {
                sql: "select 1".to_string(),
                dialect: "ansi".to_string(),
            }))
            .build()
            .unwrap();
        let version = ViewVersion::builder()
            .with_schema_id(1)
            .with_timestamp_ms(0)
            .with_default_namespace(NamespaceIdent::from_vec(vec!["ns".to_string()]).unwrap())
            .with_representations(representations)
            .build();
        let metadata = ViewMetadataBuilder::new(
            "s3://bucket/view".to_string(),
            schema,
            version,
            ViewFormatVersion::V1,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;
        Arc::new(metadata)
    }

    #[test]
    fn test_load_view_result_labels_are_optional_on_the_wire() {
        let without = LoadViewResult {
            metadata_location: "s3://bucket/view/metadata.json".to_string(),
            metadata: view_metadata(),
            config: None,
            labels: None,
        };
        let json = serde_json::to_value(&without).unwrap();
        assert!(json.get("labels").is_none(), "absent labels are omitted");
        let parsed: LoadViewResult = serde_json::from_value(json).unwrap();
        assert_eq!(parsed.labels, None);

        let labels = Labels::builder()
            .object_labels([("domain".to_string(), "payments".to_string())].into())
            .build();
        let with = LoadViewResult {
            labels: Some(labels.clone()),
            ..without
        };
        let json = serde_json::to_value(&with).unwrap();
        assert_eq!(
            json["labels"],
            serde_json::json!({"object-labels": {"domain": "payments"}})
        );
        let parsed: LoadViewResult = serde_json::from_value(json).unwrap();
        assert_eq!(parsed.labels, Some(labels));
    }
}
