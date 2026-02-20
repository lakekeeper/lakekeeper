use std::{borrow::Cow, slice::Iter, sync::LazyLock};

use serde::{Deserialize, Serialize};
use strum::Display;
use urlencoding;
use uuid::Uuid;

#[derive(Debug, Display, thiserror::Error)]
pub enum StorageLayoutError {
    InvalidTemplate(String),
}

pub trait PathSegmentContext {
    fn get_name(&self) -> Cow<'_, str>;

    fn get_uuid(&self) -> Uuid;
}

// Helper function for encoding path segments
fn encode_path_segment(value: &str) -> Cow<'_, str> {
    urlencoding::encode(value)
}

pub trait TemplatedPathSegmentRenderer {
    type Context: PathSegmentContext;

    fn template(&self) -> Cow<'_, str>;

    fn render(&self, context: &Self::Context) -> String {
        let template = self.template();
        let uuid = context.get_uuid();
        let name = context.get_name();
        let name = encode_path_segment(&name);
        template
            .replace("{uuid}", &uuid.to_string())
            .replace("{name}", &name)
    }
}

pub static DEFAULT_LAYOUT: LazyLock<StorageLayout> = LazyLock::new(StorageLayout::default);

pub static DEFAULT_TABLE_TEMPLATE: LazyLock<StorageLayoutTableTemplate> =
    LazyLock::new(StorageLayoutTableTemplate::default);

pub static DEFAULT_NAMESPACE_TEMPLATE: LazyLock<StorageLayoutNamespaceTemplate> =
    LazyLock::new(StorageLayoutNamespaceTemplate::default);

#[derive(Debug, Clone, Hash, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
pub struct StorageLayoutParentNamespaceAndTable {
    pub namespace: StorageLayoutNamespaceTemplate,
    pub table: StorageLayoutTableTemplate,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
pub struct StorageLayoutFullHierarchy {
    pub namespace: StorageLayoutNamespaceTemplate,
    pub table: StorageLayoutTableTemplate,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
pub struct StorageLayoutFlat {
    pub table: StorageLayoutTableTemplate,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum StorageLayout {
    #[default]
    Default,
    #[serde(rename = "table-only")]
    Flat(StorageLayoutFlat),
    #[serde(rename = "parent-namespace-and-table")]
    Parent(StorageLayoutParentNamespaceAndTable),
    #[serde(rename = "full-hierarchy")]
    Full(StorageLayoutFullHierarchy),
}

impl StorageLayout {
    pub fn try_new_flat(table_template: String) -> Result<Self, StorageLayoutError> {
        if !table_template.contains("{uuid}") {
            return Err(StorageLayoutError::InvalidTemplate(format!(
                "For the 'Flat' layout, the table template '{table_template}' must contain the {{uuid}} placeholder to prevent path collisions."
            )));
        }
        Ok(Self::Flat(StorageLayoutFlat {
            table: StorageLayoutTableTemplate(table_template),
        }))
    }

    pub fn try_new_parent(
        namespace_template: String,
        table_template: String,
    ) -> Result<Self, StorageLayoutError> {
        Ok(Self::Parent(StorageLayoutParentNamespaceAndTable {
            namespace: StorageLayoutNamespaceTemplate(namespace_template),
            table: StorageLayoutTableTemplate(table_template),
        }))
    }

    pub fn try_new_full(
        namespace_template: String,
        table_template: String,
    ) -> Result<Self, StorageLayoutError> {
        Ok(Self::Full(StorageLayoutFullHierarchy {
            namespace: StorageLayoutNamespaceTemplate(namespace_template),
            table: StorageLayoutTableTemplate(table_template),
        }))
    }

    #[must_use]
    pub fn table_template(&self) -> &StorageLayoutTableTemplate {
        match self {
            StorageLayout::Flat(template) => &template.table,
            StorageLayout::Parent(template) => &template.table,
            StorageLayout::Full(template) => &template.table,
            StorageLayout::Default => &DEFAULT_TABLE_TEMPLATE,
        }
    }

    #[must_use]
    pub fn render_table_segment(&self, context: &TableNameContext) -> String {
        self.table_template().render(context)
    }

    #[must_use]
    pub fn render_namespace_path(&self, path_context: &NamespacePath) -> Vec<String> {
        match self {
            StorageLayout::Flat(_) => vec![],
            StorageLayout::Parent(layout) => {
                render_parent_namespace_path(path_context, &layout.namespace)
            }
            StorageLayout::Full(layout) => path_context
                .into_iter()
                .map(|path| layout.namespace.render(path))
                .collect(),
            StorageLayout::Default => {
                render_parent_namespace_path(path_context, &DEFAULT_NAMESPACE_TEMPLATE)
            }
        }
    }
}

fn render_parent_namespace_path(
    path_context: &NamespacePath,
    template: &StorageLayoutNamespaceTemplate,
) -> Vec<String> {
    path_context
        .namespace()
        .map_or_else(Vec::new, |path| vec![template.render(path)])
}

#[derive(Debug, Clone, Default)]
pub struct NamespacePath(pub(super) Vec<NamespaceNameContext>);

impl NamespacePath {
    #[must_use]
    pub fn new(segments: Vec<NamespaceNameContext>) -> Self {
        Self(segments)
    }

    #[must_use]
    pub fn namespace(&self) -> Option<&NamespaceNameContext> {
        self.0.last()
    }

    #[allow(unused)]
    fn iter(&self) -> Iter<'_, NamespaceNameContext> {
        <&Self as IntoIterator>::into_iter(self)
    }
}

impl<'a> IntoIterator for &'a NamespacePath {
    type Item = &'a NamespaceNameContext;
    type IntoIter = Iter<'a, NamespaceNameContext>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

#[derive(Debug, Clone)]
pub struct NamespaceNameContext {
    pub name: String,
    pub uuid: Uuid,
}

impl PathSegmentContext for NamespaceNameContext {
    fn get_name(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.name)
    }

    fn get_uuid(&self) -> Uuid {
        self.uuid
    }
}

#[derive(Debug, Clone, Hash, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(transparent)]
#[cfg_attr(feature = "open-api", schema(
    value_type = String,
    description = "Template string for namespace names. Placeholders {uuid} and {name} (with curly braces) will be replaced with the actual namespace UUID and name respectively. Example: \"{name}-{uuid}\" renders to \"my-namespace-550e8400-e29b-41d4-a716-446655440001\".",
    example = json!("{uuid}")
))]
pub struct StorageLayoutNamespaceTemplate(pub(super) String);

impl TemplatedPathSegmentRenderer for StorageLayoutNamespaceTemplate {
    type Context = NamespaceNameContext;

    fn template(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.0)
    }
}

impl Default for StorageLayoutNamespaceTemplate {
    fn default() -> Self {
        Self("{uuid}".to_string())
    }
}

#[derive(Debug, Clone)]
pub struct TableNameContext {
    pub name: String,
    pub uuid: Uuid,
}

impl PathSegmentContext for TableNameContext {
    fn get_name(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.name)
    }

    fn get_uuid(&self) -> Uuid {
        self.uuid
    }
}

#[derive(Debug, Clone, Hash, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(transparent)]
#[cfg_attr(feature = "open-api", schema(
    value_type = String,
    description = "Template string for table names. Placeholders {uuid} and {name} (with curly braces) will be replaced with the actual table UUID and name respectively. Example: \"{name}-{uuid}\" renders to \"my-table-550e8400-e29b-41d4-a716-446655440002\".",
    example = json!("{uuid}")
))]
pub struct StorageLayoutTableTemplate(pub(super) String);

impl TemplatedPathSegmentRenderer for StorageLayoutTableTemplate {
    type Context = TableNameContext;

    fn template(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.0)
    }
}

impl Default for StorageLayoutTableTemplate {
    fn default() -> Self {
        Self("{uuid}".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage_layout_renders_flat_table_format_with_name_and_uuid() {
        let table_name_template = "{name}-{uuid}";
        let layout = StorageLayout::try_new_flat(table_name_template.to_string()).unwrap();
        let context = TableNameContext {
            name: "my_table".to_string(),
            uuid: Uuid::new_v4(),
        };

        let StorageLayout::Flat(renderer) = layout else {
            panic!("Expected flat storage layout");
        };

        assert_eq!(
            renderer.table.render(&context),
            format!("{}-{}", context.name, context.uuid)
        );
    }

    #[test]
    fn test_storage_layout_renders_parent_namespace_layout_with_namespace_name_and_uuid_and_table_name_and_uuid()
     {
        let table_name_template = "{name}-{uuid}";
        let layout = StorageLayout::try_new_parent(
            table_name_template.to_string(),
            table_name_template.to_string(),
        )
        .unwrap();
        let table_context = TableNameContext {
            name: "my_table".to_string(),
            uuid: Uuid::new_v4(),
        };
        let namespace_context = NamespaceNameContext {
            name: "my_namespace".to_string(),
            uuid: Uuid::new_v4(),
        };

        let StorageLayout::Parent(layout) = layout else {
            panic!("Expected parent storage layout");
        };

        assert_eq!(
            layout.table.render(&table_context),
            format!("{}-{}", table_context.name, table_context.uuid)
        );
        assert_eq!(
            layout.namespace.render(&namespace_context),
            format!("{}-{}", namespace_context.name, namespace_context.uuid)
        );
    }

    #[test]
    fn test_storage_layout_renders_full_layout_with_namespace_name_and_uuid_and_table_name_and_uuid()
     {
        let table_name_template = "{name}-{uuid}";
        let layout = StorageLayout::try_new_full(
            table_name_template.to_string(),
            table_name_template.to_string(),
        )
        .unwrap();
        let table_context = TableNameContext {
            name: "my_table".to_string(),
            uuid: Uuid::new_v4(),
        };
        let namespace_context = NamespaceNameContext {
            name: "my_namespace".to_string(),
            uuid: Uuid::new_v4(),
        };

        let StorageLayout::Full(layout) = layout else {
            panic!("Expected full storage layout");
        };

        assert_eq!(
            layout.table.render(&table_context),
            format!("{}-{}", table_context.name, table_context.uuid)
        );
        assert_eq!(
            layout.namespace.render(&namespace_context),
            format!("{}-{}", namespace_context.name, namespace_context.uuid)
        );
    }

    #[test]
    fn test_storage_layout_table_in_flat_layout_need_uuid() {
        let invalid_table_name_template = "{name}";
        let layout = StorageLayout::try_new_flat(invalid_table_name_template.to_string());
        let layout = layout.expect_err("Expected error due to missing {uuid} in template");
        assert!(matches!(layout, StorageLayoutError::InvalidTemplate(_)));
    }

    #[test]
    fn test_storage_layout_render_table_segment_in_flat_layout() {
        let table_name_template = "{name}-{uuid}";
        let layout = StorageLayout::try_new_flat(table_name_template.to_string()).unwrap();
        let context = TableNameContext {
            name: "my_table".to_string(),
            uuid: Uuid::now_v7(),
        };

        assert_eq!(
            layout.render_table_segment(&context),
            format!("{}-{}", context.name, context.uuid)
        );
    }

    #[test]
    fn test_storage_layout_render_table_segment_in_parent_layout() {
        let table_name_template = "{name}-{uuid}";
        let namespace_name_template = "{name}-{uuid}";
        let layout = StorageLayout::try_new_parent(
            namespace_name_template.to_string(),
            table_name_template.to_string(),
        )
        .unwrap();
        let context = TableNameContext {
            name: "my_table".to_string(),
            uuid: Uuid::now_v7(),
        };

        assert_eq!(
            layout.render_table_segment(&context),
            format!("{}-{}", context.name, context.uuid)
        );
    }

    #[test]
    fn test_storage_layout_render_table_segment_in_full_layout() {
        let table_name_template = "{name}-{uuid}";
        let namespace_name_template = "{name}-{uuid}";
        let layout = StorageLayout::try_new_full(
            namespace_name_template.to_string(),
            table_name_template.to_string(),
        )
        .unwrap();
        let context = TableNameContext {
            name: "my_table".to_string(),
            uuid: Uuid::now_v7(),
        };

        assert_eq!(
            layout.render_table_segment(&context),
            format!("{}-{}", context.name, context.uuid)
        );
    }

    #[test]
    fn test_storage_layout_render_table_segment_in_default_layout_uses_parent_layout_with_uuid_only()
     {
        let layout = StorageLayout::Default;
        let context = TableNameContext {
            name: "my_table".to_string(),
            uuid: Uuid::now_v7(),
        };

        assert_eq!(
            layout.render_table_segment(&context),
            format!("{}", context.uuid)
        );
    }

    #[test]
    fn test_storage_layout_render_namespace_segment_in_flat_layout() {
        let table_name_template = "{name}-{uuid}";
        let layout = StorageLayout::try_new_flat(table_name_template.to_string()).unwrap();
        let path = NamespacePath::new(vec![]);

        assert!(layout.render_namespace_path(&path).is_empty());
    }

    #[test]
    fn test_storage_layout_render_namespace_segment_in_flat_layout_should_never_render_namespace() {
        let table_name_template = "{name}-{uuid}";
        let layout = StorageLayout::try_new_flat(table_name_template.to_string()).unwrap();
        let parent_namespace = NamespaceNameContext {
            name: "my_namespace".to_string(),
            uuid: Uuid::now_v7(),
        };
        let path = NamespacePath::new(vec![parent_namespace]);

        assert!(layout.render_namespace_path(&path).is_empty());
    }

    #[test]
    fn test_storage_layout_render_namespace_segment_in_parent_layout() {
        let table_name_template = "{name}-{uuid}";
        let namespace_name_template = "{name}-{uuid}";
        let layout = StorageLayout::try_new_parent(
            namespace_name_template.to_string(),
            table_name_template.to_string(),
        )
        .unwrap();
        let parent_namespace = NamespaceNameContext {
            name: "my_namespace".to_string(),
            uuid: Uuid::now_v7(),
        };
        let path = NamespacePath::new(vec![parent_namespace.clone()]);

        assert_eq!(
            *layout.render_namespace_path(&path),
            vec![format!(
                "{}-{}",
                parent_namespace.name, parent_namespace.uuid
            )]
        );
    }

    #[test]
    fn test_storage_layout_render_namespace_segment_in_parent_layout_should_only_render_parent() {
        let table_name_template = "{name}-{uuid}";
        let namespace_name_template = "{name}-{uuid}";
        let layout = StorageLayout::try_new_parent(
            namespace_name_template.to_string(),
            table_name_template.to_string(),
        )
        .unwrap();
        let grand_parent_namespace = NamespaceNameContext {
            name: "grand_parent_namespace".to_string(),
            uuid: Uuid::now_v7(),
        };
        let parent_namespace = NamespaceNameContext {
            name: "parent_namespace".to_string(),
            uuid: Uuid::now_v7(),
        };
        let path = NamespacePath::new(vec![grand_parent_namespace, parent_namespace.clone()]);

        assert_eq!(
            *layout.render_namespace_path(&path),
            vec![format!(
                "{}-{}",
                parent_namespace.name, parent_namespace.uuid
            )]
        );
    }

    #[test]
    fn test_storage_layout_render_namespace_segment_in_parent_layout_should_render_empty_namespace()
    {
        let table_name_template = "{name}-{uuid}";
        let namespace_name_template = "{name}-{uuid}";
        let layout = StorageLayout::try_new_parent(
            namespace_name_template.to_string(),
            table_name_template.to_string(),
        )
        .unwrap();
        let path = NamespacePath::new(vec![]);

        assert!(layout.render_namespace_path(&path).is_empty());
    }

    #[test]
    fn test_storage_layout_render_namespace_segment_in_full_layout() {
        let table_name_template = "{name}-{uuid}";
        let namespace_name_template = "{name}-{uuid}";
        let layout = StorageLayout::try_new_full(
            namespace_name_template.to_string(),
            table_name_template.to_string(),
        )
        .unwrap();
        let parent_namespace = NamespaceNameContext {
            name: "parent_namespace".to_string(),
            uuid: Uuid::now_v7(),
        };
        let path = NamespacePath::new(vec![parent_namespace.clone()]);

        assert_eq!(
            *layout.render_namespace_path(&path),
            vec![format!(
                "{}-{}",
                parent_namespace.name, parent_namespace.uuid
            )]
        );
    }

    #[test]
    fn test_storage_layout_render_namespace_segment_in_full_layout_should_render_empty_namespace() {
        let table_name_template = "{name}-{uuid}";
        let namespace_name_template = "{name}-{uuid}";
        let layout = StorageLayout::try_new_full(
            namespace_name_template.to_string(),
            table_name_template.to_string(),
        )
        .unwrap();
        let path = NamespacePath::new(vec![]);

        assert!(layout.render_namespace_path(&path).is_empty());
    }

    #[test]
    fn test_storage_layout_render_namespace_segment_in_full_layout_should_render_all_ancestor_namespaces()
     {
        let table_name_template = "{name}-{uuid}";
        let namespace_name_template = "{name}-{uuid}";
        let layout = StorageLayout::try_new_full(
            namespace_name_template.to_string(),
            table_name_template.to_string(),
        )
        .unwrap();
        let grand_parent_namespace = NamespaceNameContext {
            name: "grand_parent_namespace".to_string(),
            uuid: Uuid::now_v7(),
        };
        let parent_namespace = NamespaceNameContext {
            name: "parent_namespace".to_string(),
            uuid: Uuid::now_v7(),
        };
        let path = NamespacePath::new(vec![
            grand_parent_namespace.clone(),
            parent_namespace.clone(),
        ]);

        assert_eq!(
            *layout.render_namespace_path(&path),
            vec![
                format!(
                    "{}-{}",
                    grand_parent_namespace.name, grand_parent_namespace.uuid
                ),
                format!("{}-{}", parent_namespace.name, parent_namespace.uuid),
            ]
        );
    }

    #[test]
    fn test_storage_layout_render_namespace_segment_in_default_layout_should_render_only_parent() {
        let layout = StorageLayout::Default;
        let grand_parent_namespace = NamespaceNameContext {
            name: "grand_parent_namespace".to_string(),
            uuid: Uuid::now_v7(),
        };
        let parent_namespace = NamespaceNameContext {
            name: "parent_namespace".to_string(),
            uuid: Uuid::now_v7(),
        };
        let path = NamespacePath::new(vec![
            grand_parent_namespace.clone(),
            parent_namespace.clone(),
        ]);

        assert_eq!(
            *layout.render_namespace_path(&path),
            vec![format!("{}", parent_namespace.uuid),]
        );
    }

    #[test]
    fn test_storage_layout_deserialization_of_full_layout() {
        let json = r#"
        {
            "type": "full-hierarchy",
            "namespace": "{uuid}",
            "table": "{name}"
        }
        "#;

        let layout: StorageLayout =
            serde_json::from_str(json).expect("Failed to deserialize StorageLayout");

        let StorageLayout::Full(full_layout) = &layout else {
            panic!("Expected full storage layout");
        };

        assert_eq!(full_layout.namespace.0, "{uuid}");
        assert_eq!(full_layout.table.0, "{name}");

        let grand_parent_namespace = NamespaceNameContext {
            name: "grand_parent_namespace".to_string(),
            uuid: Uuid::now_v7(),
        };
        let parent_namespace = NamespaceNameContext {
            name: "parent_namespace".to_string(),
            uuid: Uuid::now_v7(),
        };
        let namespace_path = NamespacePath::new(vec![
            grand_parent_namespace.clone(),
            parent_namespace.clone(),
        ]);
        let table = TableNameContext {
            name: "my_table".to_string(),
            uuid: Uuid::now_v7(),
        };

        let namespace_path_rendered = layout.render_namespace_path(&namespace_path);

        assert_eq!(
            namespace_path_rendered,
            vec![
                format!("{}", grand_parent_namespace.uuid),
                format!("{}", parent_namespace.uuid),
            ]
        );

        let table_name_rendered = layout.render_table_segment(&table);
        assert_eq!(table_name_rendered, format!("{}", table.name));
    }

    #[test]
    fn test_storage_layout_deserialization_of_parent_layout() {
        let json = r#"
        {
            "type": "parent-namespace-and-table",
            "namespace": "{uuid}",
            "table": "{name}"
        }
        "#;

        let layout: StorageLayout =
            serde_json::from_str(json).expect("Failed to deserialize StorageLayout");

        let StorageLayout::Parent(parent_layout) = &layout else {
            panic!("Expected parent storage layout");
        };

        assert_eq!(parent_layout.namespace.0, "{uuid}");
        assert_eq!(parent_layout.table.0, "{name}");

        let grand_parent_namespace = NamespaceNameContext {
            name: "grand_parent_namespace".to_string(),
            uuid: Uuid::now_v7(),
        };
        let parent_namespace = NamespaceNameContext {
            name: "parent_namespace".to_string(),
            uuid: Uuid::now_v7(),
        };
        let namespace_path = NamespacePath::new(vec![
            grand_parent_namespace.clone(),
            parent_namespace.clone(),
        ]);
        let table = TableNameContext {
            name: "my_table".to_string(),
            uuid: Uuid::now_v7(),
        };

        let namespace_path_rendered = layout.render_namespace_path(&namespace_path);

        assert_eq!(
            namespace_path_rendered,
            vec![format!("{}", parent_namespace.uuid),]
        );

        let table_name_rendered = layout.render_table_segment(&table);
        assert_eq!(table_name_rendered, format!("{}", table.name));
    }

    #[test]
    fn test_storage_layout_deserialization_of_flat_layout() {
        let json = r#"
        {
            "type": "table-only",
            "table": "{name}"
        }
        "#;

        let layout: StorageLayout =
            serde_json::from_str(json).expect("Failed to deserialize StorageLayout");

        let StorageLayout::Flat(flat_layout) = &layout else {
            panic!("Expected flat storage layout");
        };

        assert_eq!(flat_layout.table.0, "{name}");

        let grand_parent_namespace = NamespaceNameContext {
            name: "grand_parent_namespace".to_string(),
            uuid: Uuid::now_v7(),
        };
        let parent_namespace = NamespaceNameContext {
            name: "parent_namespace".to_string(),
            uuid: Uuid::now_v7(),
        };
        let namespace_path = NamespacePath::new(vec![
            grand_parent_namespace.clone(),
            parent_namespace.clone(),
        ]);
        let table = TableNameContext {
            name: "my_table".to_string(),
            uuid: Uuid::now_v7(),
        };

        let namespace_path_rendered = layout.render_namespace_path(&namespace_path);

        assert_eq!(namespace_path_rendered, Vec::<String>::new());

        let table_name_rendered = layout.render_table_segment(&table);
        assert_eq!(table_name_rendered, format!("{}", table.name));
    }

    #[test]
    fn test_storage_layout_deserialization_of_default_layout() {
        let json = r#"
        {
            "type": "default"
        }
        "#;

        let layout: StorageLayout =
            serde_json::from_str(json).expect("Failed to deserialize StorageLayout");

        let StorageLayout::Default = &layout else {
            panic!("Expected default storage layout");
        };

        let grand_parent_namespace = NamespaceNameContext {
            name: "grand_parent_namespace".to_string(),
            uuid: Uuid::now_v7(),
        };
        let parent_namespace = NamespaceNameContext {
            name: "parent_namespace".to_string(),
            uuid: Uuid::now_v7(),
        };
        let namespace_path = NamespacePath::new(vec![
            grand_parent_namespace.clone(),
            parent_namespace.clone(),
        ]);
        let table = TableNameContext {
            name: "my_table".to_string(),
            uuid: Uuid::now_v7(),
        };

        let namespace_path_rendered = layout.render_namespace_path(&namespace_path);

        assert_eq!(
            namespace_path_rendered,
            vec![format!("{}", parent_namespace.uuid),]
        );

        let table_name_rendered = layout.render_table_segment(&table);
        assert_eq!(table_name_rendered, format!("{}", table.uuid));
    }

    #[test]
    fn test_storage_layout_should_handle_special_characters_in_namespace_name() {
        let special_namespace_names = vec![
            "namespace with spaces",
            "namespace-with-hyphens",
            "namespace_with_underscores",
            "namespace!with@special#chars$",
            "naméspace_with_àccents_ñ",
            "namespace_with_ümlauts_ä_ö",
            "namespace_中文_日本語",
            "namespace_עברית_العربية",
            "namespace_🚀_emoji_✨",
            "namespace-Mix!_OF_everything_中文_ä_🎉",
            "namespace%with%percent",
            "namespace&with&ampersands",
            "namespace=with=equals",
        ];
        let expected_namespace_names = vec![
            "namespace%20with%20spaces",
            "namespace-with-hyphens",
            "namespace_with_underscores",
            "namespace%21with%40special%23chars%24",
            "nam%C3%A9space_with_%C3%A0ccents_%C3%B1",
            "namespace_with_%C3%BCmlauts_%C3%A4_%C3%B6",
            "namespace_%E4%B8%AD%E6%96%87_%E6%97%A5%E6%9C%AC%E8%AA%9E",
            "namespace_%D7%A2%D7%91%D7%A8%D7%99%D7%AA_%D8%A7%D9%84%D8%B9%D8%B1%D8%A8%D9%8A%D8%A9",
            "namespace_%F0%9F%9A%80_emoji_%E2%9C%A8",
            "namespace-Mix%21_OF_everything_%E4%B8%AD%E6%96%87_%C3%A4_%F0%9F%8E%89",
            "namespace%25with%25percent",
            "namespace%26with%26ampersands",
            "namespace%3Dwith%3Dequals",
        ];
        let namespace_template = StorageLayoutNamespaceTemplate("{name}".to_string());
        let special_namespaces = special_namespace_names
            .iter()
            .map(|special_namespace_name| NamespaceNameContext {
                name: (*special_namespace_name).to_string(),
                uuid: Uuid::now_v7(),
            })
            .map(|context| namespace_template.render(&context))
            .collect::<Vec<_>>();
        assert_eq!(special_namespaces, expected_namespace_names);
    }

    #[test]
    fn test_storage_layout_should_handle_special_characters_in_table_name() {
        let special_table_names = vec![
            "table with spaces",
            "table-with-hyphens",
            "table_with_underscores",
            "table!with@special#chars$",
            "tablé_with_àccents_ñ",
            "table_with_ümlauts_ä_ö",
            "table_中文_日本語",
            "table_עברית_العربية",
            "table_🚀_emoji_✨",
            "table-Mix!_OF_everything_中文_ä_🎉",
            "table%with%percent",
            "table&with&ampersands",
            "table=with=equals",
        ];
        let expected_table_names = vec![
            "table%20with%20spaces",
            "table-with-hyphens",
            "table_with_underscores",
            "table%21with%40special%23chars%24",
            "tabl%C3%A9_with_%C3%A0ccents_%C3%B1",
            "table_with_%C3%BCmlauts_%C3%A4_%C3%B6",
            "table_%E4%B8%AD%E6%96%87_%E6%97%A5%E6%9C%AC%E8%AA%9E",
            "table_%D7%A2%D7%91%D7%A8%D7%99%D7%AA_%D8%A7%D9%84%D8%B9%D8%B1%D8%A8%D9%8A%D8%A9",
            "table_%F0%9F%9A%80_emoji_%E2%9C%A8",
            "table-Mix%21_OF_everything_%E4%B8%AD%E6%96%87_%C3%A4_%F0%9F%8E%89",
            "table%25with%25percent",
            "table%26with%26ampersands",
            "table%3Dwith%3Dequals",
        ];
        let table_template = StorageLayoutTableTemplate("{name}".to_string());
        let special_tables = special_table_names
            .iter()
            .map(|special_table_name| TableNameContext {
                name: (*special_table_name).to_string(),
                uuid: Uuid::now_v7(),
            })
            .map(|context| table_template.render(&context))
            .collect::<Vec<_>>();
        assert_eq!(special_tables, expected_table_names);
    }
}
