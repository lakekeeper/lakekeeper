use std::{borrow::Cow, slice::Iter, sync::LazyLock};

use serde::{Deserialize, Serialize};
use urlencoding;
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum StorageLayoutError {
    #[error("Invalid Template: {0}")]
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

pub static DEFAULT_TABULAR_TEMPLATE: LazyLock<StorageLayoutTabularTemplate> =
    LazyLock::new(StorageLayoutTabularTemplate::default);

pub static DEFAULT_NAMESPACE_TEMPLATE: LazyLock<StorageLayoutNamespaceTemplate> =
    LazyLock::new(StorageLayoutNamespaceTemplate::default);

/// One directory per direct-parent namespace, one per tabular.
///
/// For a tabular `my_tabular` (uuid `…002`) in namespace `my_ns` (uuid `…001`) the path is:
/// `<base>/<namespace-segment>/<tabular-segment>`.
#[derive(Debug, Clone, Hash, Eq, PartialEq, Serialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[cfg_attr(feature = "open-api", schema(
    example = json!({"namespace": "{uuid}", "tabular": "{uuid}"})
))]
pub struct StorageLayoutParentNamespaceAndTabular {
    pub namespace: StorageLayoutNamespaceTemplate,
    pub tabular: StorageLayoutTabularTemplate,
}

impl StorageLayoutParentNamespaceAndTabular {
    pub fn try_new(
        namespace_template: String,
        tabular_template: String,
    ) -> Result<Self, StorageLayoutError> {
        if !has_template_parameter(&tabular_template) {
            return Err(StorageLayoutError::InvalidTemplate(format!(
                "For the 'parent-namespace-and-tabular' layout, the tabular template '{tabular_template}' must contain at least one placeholder."
            )));
        }

        if !has_template_parameter(&namespace_template) {
            return Err(StorageLayoutError::InvalidTemplate(format!(
                "For the 'parent-namespace-and-tabular' layout, the namespace template '{namespace_template}' must contain at least one placeholder."
            )));
        }

        Ok(Self {
            namespace: StorageLayoutNamespaceTemplate(namespace_template),
            tabular: StorageLayoutTabularTemplate(tabular_template),
        })
    }
}

impl<'de> Deserialize<'de> for StorageLayoutParentNamespaceAndTabular {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct StorageLayoutParentNamespaceAndTabularHelper {
            namespace: StorageLayoutNamespaceTemplate,
            tabular: StorageLayoutTabularTemplate,
        }

        let helper = StorageLayoutParentNamespaceAndTabularHelper::deserialize(deserializer)?;
        StorageLayoutParentNamespaceAndTabular::try_new(helper.namespace.0, helper.tabular.0)
            .map_err(serde::de::Error::custom)
    }
}

/// One directory per namespace level, one per tabular.
///
/// For a tabular `my_tabular` (uuid `…003`) in `grandparent_ns` / `parent_ns` the path is:
/// `<base>/<grandparent-segment>/<parent-segment>/<tabular-segment>`.
#[derive(Debug, Clone, Hash, Eq, PartialEq, Serialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[cfg_attr(feature = "open-api", schema(
    example = json!({"namespace": "{name}-{uuid}", "tabular": "{name}-{uuid}"})
))]
pub struct StorageLayoutFullHierarchy {
    pub namespace: StorageLayoutNamespaceTemplate,
    pub tabular: StorageLayoutTabularTemplate,
}

impl StorageLayoutFullHierarchy {
    pub fn try_new(
        namespace_template: String,
        tabular_template: String,
    ) -> Result<Self, StorageLayoutError> {
        if !has_template_parameter(&tabular_template) {
            return Err(StorageLayoutError::InvalidTemplate(format!(
                "For the 'full-hierarchy' layout, the tabular template '{tabular_template}' must contain at least one placeholder."
            )));
        }

        if !has_template_parameter(&namespace_template) {
            return Err(StorageLayoutError::InvalidTemplate(format!(
                "For the 'full-hierarchy' layout, the namespace template '{namespace_template}' must contain at least one placeholder."
            )));
        }

        Ok(Self {
            namespace: StorageLayoutNamespaceTemplate(namespace_template),
            tabular: StorageLayoutTabularTemplate(tabular_template),
        })
    }
}

impl<'de> Deserialize<'de> for StorageLayoutFullHierarchy {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct StorageLayoutFullHierarchyHelper {
            namespace: StorageLayoutNamespaceTemplate,
            tabular: StorageLayoutTabularTemplate,
        }

        let helper = StorageLayoutFullHierarchyHelper::deserialize(deserializer)?;
        StorageLayoutFullHierarchy::try_new(helper.namespace.0, helper.tabular.0)
            .map_err(serde::de::Error::custom)
    }
}

/// No namespace directories; all tabulars are placed directly under the base location.
///
/// For a tabular `my_tabular` (uuid `…002`) the path is: `<base>/<tabular-segment>`.
/// The tabular template must contain `{uuid}` to avoid collisions between tabulars with the same name.
#[derive(Debug, Clone, Hash, Eq, PartialEq, Serialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[cfg_attr(feature = "open-api", schema(
    example = json!({"tabular": "{name}-{uuid}"})
))]
pub struct StorageLayoutFlat {
    pub tabular: StorageLayoutTabularTemplate,
}

impl StorageLayoutFlat {
    pub fn try_new(tabular_template: String) -> Result<Self, StorageLayoutError> {
        if !tabular_template.contains("{uuid}") {
            return Err(StorageLayoutError::InvalidTemplate(format!(
                "For the 'tabular-only' layout, the tabular template '{tabular_template}' must contain the {{uuid}} placeholder to prevent path collisions."
            )));
        }
        Ok(Self {
            tabular: StorageLayoutTabularTemplate(tabular_template),
        })
    }
}

impl<'de> Deserialize<'de> for StorageLayoutFlat {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct StorageLayoutFlatHelper {
            tabular: StorageLayoutTabularTemplate,
        }

        let helper = StorageLayoutFlatHelper::deserialize(deserializer)?;
        StorageLayoutFlat::try_new(helper.tabular.0).map_err(serde::de::Error::custom)
    }
}

const TEMPLATE_PARAMETERS: [&str; 2] = ["{uuid}", "{name}"];

fn has_template_parameter(template: &str) -> bool {
    TEMPLATE_PARAMETERS
        .iter()
        .any(|param| template.contains(param))
}

/// Controls how namespace and tabular paths are constructed under the warehouse base location.
///
/// - `default` / omitted: one directory per direct-parent namespace, one per tabular, both with `"{uuid}"` segments.
/// - `full-hierarchy`: one directory per namespace level, one per tabular.
/// - `tabular-only`: no namespace directories; all tabulars are placed directly under the base location.
///
/// Segment templates may use `{uuid}` and `{name}` as placeholders.
#[derive(Debug, Clone, Hash, Eq, PartialEq, Serialize, Deserialize, Default, derive_more::From)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[cfg_attr(feature = "open-api", schema(
    example = json!({"type": "full-hierarchy", "namespace": "{name}-{uuid}", "tabular": "{name}-{uuid}"})
))]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum StorageLayout {
    #[default]
    Default,
    #[serde(rename = "tabular-only")]
    Flat(StorageLayoutFlat),
    #[serde(skip, rename = "parent-namespace-and-tabular")]
    Parent(StorageLayoutParentNamespaceAndTabular),
    #[serde(rename = "full-hierarchy")]
    Full(StorageLayoutFullHierarchy),
}

impl StorageLayout {
    pub fn try_new_flat(tabular_template: String) -> Result<Self, StorageLayoutError> {
        StorageLayoutFlat::try_new(tabular_template).map(Self::Flat)
    }

    pub fn try_new_parent(
        namespace_template: String,
        tabular_template: String,
    ) -> Result<Self, StorageLayoutError> {
        StorageLayoutParentNamespaceAndTabular::try_new(namespace_template, tabular_template)
            .map(Self::Parent)
    }

    pub fn try_new_full(
        namespace_template: String,
        tabular_template: String,
    ) -> Result<Self, StorageLayoutError> {
        StorageLayoutFullHierarchy::try_new(namespace_template, tabular_template).map(Self::Full)
    }

    #[must_use]
    pub fn tabular_template(&self) -> &StorageLayoutTabularTemplate {
        match self {
            StorageLayout::Flat(template) => &template.tabular,
            StorageLayout::Parent(template) => &template.tabular,
            StorageLayout::Full(template) => &template.tabular,
            StorageLayout::Default => &DEFAULT_TABULAR_TEMPLATE,
        }
    }

    #[must_use]
    pub fn render_tabular_segment(&self, context: &TabularNameContext) -> String {
        self.tabular_template().render(context)
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
    description = "Template string for namespace path segments. Placeholders {uuid} and {name} (with curly braces) will be replaced with the actual namespace UUID and name respectively. The {name} value is percent-encoded (URL percent-encoding) so spaces and special characters are escaped (e.g. \"my name\" becomes \"my%20name\"). The {uuid} value is inserted as-is without encoding. Example: \"{name}-{uuid}\" for a namespace named \"my ns\" renders to \"my%20ns-550e8400-e29b-41d4-a716-446655440001\".",
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
pub struct TabularNameContext {
    pub name: String,
    pub uuid: Uuid,
}

impl PathSegmentContext for TabularNameContext {
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
    description = "Template string for tabular names. Placeholders {uuid} and {name} (with curly braces) will be replaced with the actual tabular UUID and name respectively. The {name} value is percent-encoded (URL percent-encoding) so spaces and special characters are escaped (e.g. \"my tabular\" becomes \"my%20tabular\"). The {uuid} value is inserted as-is without encoding. Example: \"{name}-{uuid}\" for a tabular named \"my tabular\" renders to \"my%20tabular-550e8400-e29b-41d4-a716-446655440002\".",
    example = json!("{uuid}")
))]
pub struct StorageLayoutTabularTemplate(pub(super) String);

impl TemplatedPathSegmentRenderer for StorageLayoutTabularTemplate {
    type Context = TabularNameContext;

    fn template(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.0)
    }
}

impl Default for StorageLayoutTabularTemplate {
    fn default() -> Self {
        Self("{uuid}".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage_layout_renders_flat_tabular_format_with_name_and_uuid() {
        let tabular_name_template = "{name}-{uuid}";
        let layout = StorageLayout::try_new_flat(tabular_name_template.to_string()).unwrap();
        let context = TabularNameContext {
            name: "my_tabular".to_string(),
            uuid: Uuid::new_v4(),
        };

        let StorageLayout::Flat(renderer) = layout else {
            panic!("Expected flat storage layout");
        };

        assert_eq!(
            renderer.tabular.render(&context),
            format!("{}-{}", context.name, context.uuid)
        );
    }

    #[test]
    fn test_storage_layout_renders_parent_namespace_layout_with_namespace_name_and_uuid_and_tabular_name_and_uuid()
     {
        let tabular_name_template = "{name}-{uuid}";
        let layout = StorageLayout::try_new_parent(
            tabular_name_template.to_string(),
            tabular_name_template.to_string(),
        )
        .unwrap();
        let tabular_context = TabularNameContext {
            name: "my_tabular".to_string(),
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
            layout.tabular.render(&tabular_context),
            format!("{}-{}", tabular_context.name, tabular_context.uuid)
        );
        assert_eq!(
            layout.namespace.render(&namespace_context),
            format!("{}-{}", namespace_context.name, namespace_context.uuid)
        );
    }

    #[test]
    fn test_storage_layout_renders_full_layout_with_namespace_name_and_uuid_and_tabular_name_and_uuid()
     {
        let tabular_name_template = "{name}-{uuid}";
        let layout = StorageLayout::try_new_full(
            tabular_name_template.to_string(),
            tabular_name_template.to_string(),
        )
        .unwrap();
        let tabular_context = TabularNameContext {
            name: "my_tabular".to_string(),
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
            layout.tabular.render(&tabular_context),
            format!("{}-{}", tabular_context.name, tabular_context.uuid)
        );
        assert_eq!(
            layout.namespace.render(&namespace_context),
            format!("{}-{}", namespace_context.name, namespace_context.uuid)
        );
    }

    #[test]
    fn test_storage_layout_tabular_in_flat_layout_need_uuid() {
        let invalid_tabular_name_template = "{name}";
        let layout = StorageLayout::try_new_flat(invalid_tabular_name_template.to_string());
        let layout = layout.expect_err("Expected error due to missing {uuid} in template");
        assert!(matches!(layout, StorageLayoutError::InvalidTemplate(_)));
    }

    #[test]
    fn test_storage_layout_render_tabular_segment_in_flat_layout() {
        let tabular_name_template = "{name}-{uuid}";
        let layout = StorageLayout::try_new_flat(tabular_name_template.to_string()).unwrap();
        let context = TabularNameContext {
            name: "my_tabular".to_string(),
            uuid: Uuid::now_v7(),
        };

        assert_eq!(
            layout.render_tabular_segment(&context),
            format!("{}-{}", context.name, context.uuid)
        );
    }

    #[test]
    fn test_storage_layout_render_tabular_segment_in_parent_layout() {
        let tabular_name_template = "{name}-{uuid}";
        let namespace_name_template = "{name}-{uuid}";
        let layout = StorageLayout::try_new_parent(
            namespace_name_template.to_string(),
            tabular_name_template.to_string(),
        )
        .unwrap();
        let context = TabularNameContext {
            name: "my_tabular".to_string(),
            uuid: Uuid::now_v7(),
        };

        assert_eq!(
            layout.render_tabular_segment(&context),
            format!("{}-{}", context.name, context.uuid)
        );
    }

    #[test]
    fn test_storage_layout_render_tabular_segment_in_full_layout() {
        let tabular_name_template = "{name}-{uuid}";
        let namespace_name_template = "{name}-{uuid}";
        let layout = StorageLayout::try_new_full(
            namespace_name_template.to_string(),
            tabular_name_template.to_string(),
        )
        .unwrap();
        let context = TabularNameContext {
            name: "my_tabular".to_string(),
            uuid: Uuid::now_v7(),
        };

        assert_eq!(
            layout.render_tabular_segment(&context),
            format!("{}-{}", context.name, context.uuid)
        );
    }

    #[test]
    fn test_storage_layout_render_tabular_segment_in_default_layout_uses_parent_layout_with_uuid_only()
     {
        let layout = StorageLayout::Default;
        let context = TabularNameContext {
            name: "my_tabular".to_string(),
            uuid: Uuid::now_v7(),
        };

        assert_eq!(
            layout.render_tabular_segment(&context),
            format!("{}", context.uuid)
        );
    }

    #[test]
    fn test_storage_layout_render_namespace_segment_in_flat_layout() {
        let tabular_name_template = "{name}-{uuid}";
        let layout = StorageLayout::try_new_flat(tabular_name_template.to_string()).unwrap();
        let path = NamespacePath::new(vec![]);

        assert!(layout.render_namespace_path(&path).is_empty());
    }

    #[test]
    fn test_storage_layout_render_namespace_segment_in_flat_layout_should_never_render_namespace() {
        let tabular_name_template = "{name}-{uuid}";
        let layout = StorageLayout::try_new_flat(tabular_name_template.to_string()).unwrap();
        let parent_namespace = NamespaceNameContext {
            name: "my_namespace".to_string(),
            uuid: Uuid::now_v7(),
        };
        let path = NamespacePath::new(vec![parent_namespace]);

        assert!(layout.render_namespace_path(&path).is_empty());
    }

    #[test]
    fn test_storage_layout_render_namespace_segment_in_parent_layout() {
        let tabular_name_template = "{name}-{uuid}";
        let namespace_name_template = "{name}-{uuid}";
        let layout = StorageLayout::try_new_parent(
            namespace_name_template.to_string(),
            tabular_name_template.to_string(),
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
        let tabular_name_template = "{name}-{uuid}";
        let namespace_name_template = "{name}-{uuid}";
        let layout = StorageLayout::try_new_parent(
            namespace_name_template.to_string(),
            tabular_name_template.to_string(),
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
        let tabular_name_template = "{name}-{uuid}";
        let namespace_name_template = "{name}-{uuid}";
        let layout = StorageLayout::try_new_parent(
            namespace_name_template.to_string(),
            tabular_name_template.to_string(),
        )
        .unwrap();
        let path = NamespacePath::new(vec![]);

        assert!(layout.render_namespace_path(&path).is_empty());
    }

    #[test]
    fn test_storage_layout_render_namespace_segment_in_full_layout() {
        let tabular_name_template = "{name}-{uuid}";
        let namespace_name_template = "{name}-{uuid}";
        let layout = StorageLayout::try_new_full(
            namespace_name_template.to_string(),
            tabular_name_template.to_string(),
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
        let tabular_name_template = "{name}-{uuid}";
        let namespace_name_template = "{name}-{uuid}";
        let layout = StorageLayout::try_new_full(
            namespace_name_template.to_string(),
            tabular_name_template.to_string(),
        )
        .unwrap();
        let path = NamespacePath::new(vec![]);

        assert!(layout.render_namespace_path(&path).is_empty());
    }

    #[test]
    fn test_storage_layout_render_namespace_segment_in_full_layout_should_render_all_ancestor_namespaces()
     {
        let tabular_name_template = "{name}-{uuid}";
        let namespace_name_template = "{name}-{uuid}";
        let layout = StorageLayout::try_new_full(
            namespace_name_template.to_string(),
            tabular_name_template.to_string(),
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
            "tabular": "{name}"
        }
        "#;

        let layout: StorageLayout =
            serde_json::from_str(json).expect("Failed to deserialize StorageLayout");

        let StorageLayout::Full(full_layout) = &layout else {
            panic!("Expected full storage layout");
        };

        assert_eq!(full_layout.namespace.0, "{uuid}");
        assert_eq!(full_layout.tabular.0, "{name}");

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
        let tabular = TabularNameContext {
            name: "my_tabular".to_string(),
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

        let tabular_name_rendered = layout.render_tabular_segment(&tabular);
        assert_eq!(tabular_name_rendered, format!("{}", tabular.name));
    }

    #[test]
    fn test_storage_layout_deserialization_of_parent_layout_should_fail_as_it_is_internal() {
        let json = r#"
        {
            "type": "parent-namespace-and-tabular",
            "namespace": "{uuid}",
            "tabular": "{name}"
        }
        "#;

        serde_json::from_str::<StorageLayout>(json).expect_err("Storage Layout should not support deserializing the parent-namespace-and-tabular layout since it's only used internally and skipped in the enum definition");
    }

    #[test]
    fn test_storage_layout_deserialization_of_inner_parent_layout() {
        let json = r#"
        {
            "namespace": "{uuid}",
            "tabular": "{name}"
        }
        "#;

        let layout: StorageLayoutParentNamespaceAndTabular =
            serde_json::from_str(json).expect("Failed to deserialize StorageLayout");

        assert_eq!(layout.namespace.0, "{uuid}");
        assert_eq!(layout.tabular.0, "{name}");
    }

    #[test]
    fn test_storage_layout_deserialization_of_flat_layout_should_fail_without_uuid_template_parameter()
     {
        // A Flat layout without {uuid} in the tabular template must be rejected at
        // deserialization time to prevent path collisions.
        let json = r#"
        {
            "type": "tabular-only",
            "tabular": "{name}"
        }
        "#;

        let result: Result<StorageLayout, _> = serde_json::from_str(json);
        assert!(
            result.is_err(),
            "Expected deserialization to fail for Flat layout without {{uuid}} in tabular template"
        );
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
        let tabular = TabularNameContext {
            name: "my_tabular".to_string(),
            uuid: Uuid::now_v7(),
        };

        let namespace_path_rendered = layout.render_namespace_path(&namespace_path);

        assert_eq!(
            namespace_path_rendered,
            vec![format!("{}", parent_namespace.uuid),]
        );

        let tabular_name_rendered = layout.render_tabular_segment(&tabular);
        assert_eq!(tabular_name_rendered, format!("{}", tabular.uuid));
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
    fn test_storage_layout_should_handle_special_characters_in_tabular_name() {
        let special_tabular_names = vec![
            "tabular with spaces",
            "tabular-with-hyphens",
            "tabular_with_underscores",
            "tabular!with@special#chars$",
            "tabulár_with_àccents_ñ",
            "tabular_with_ümlauts_ä_ö",
            "tabular_中文_日本語",
            "tabular_עברית_العربية",
            "tabular_🚀_emoji_✨",
            "tabular-Mix!_OF_everything_中文_ä_🎉",
            "tabular%with%percent",
            "tabular&with&ampersands",
            "tabular=with=equals",
        ];
        let expected_tabular_names = vec![
            "tabular%20with%20spaces",
            "tabular-with-hyphens",
            "tabular_with_underscores",
            "tabular%21with%40special%23chars%24",
            "tabul%C3%A1r_with_%C3%A0ccents_%C3%B1",
            "tabular_with_%C3%BCmlauts_%C3%A4_%C3%B6",
            "tabular_%E4%B8%AD%E6%96%87_%E6%97%A5%E6%9C%AC%E8%AA%9E",
            "tabular_%D7%A2%D7%91%D7%A8%D7%99%D7%AA_%D8%A7%D9%84%D8%B9%D8%B1%D8%A8%D9%8A%D8%A9",
            "tabular_%F0%9F%9A%80_emoji_%E2%9C%A8",
            "tabular-Mix%21_OF_everything_%E4%B8%AD%E6%96%87_%C3%A4_%F0%9F%8E%89",
            "tabular%25with%25percent",
            "tabular%26with%26ampersands",
            "tabular%3Dwith%3Dequals",
        ];
        let tabular_template = StorageLayoutTabularTemplate("{name}".to_string());
        let special_tabulars = special_tabular_names
            .iter()
            .map(|special_tabular_name| TabularNameContext {
                name: (*special_tabular_name).to_string(),
                uuid: Uuid::now_v7(),
            })
            .map(|context| tabular_template.render(&context))
            .collect::<Vec<_>>();
        assert_eq!(special_tabulars, expected_tabular_names);
    }

    #[test]
    fn test_storage_layout_render_tabular_segment_with_slash() {
        let layout =
            StorageLayout::try_new_full("{name}/{uuid}".to_string(), "{name}/{uuid}".to_string())
                .unwrap();
        let context = TabularNameContext {
            name: "my_tabular".to_string(),
            uuid: Uuid::now_v7(),
        };

        assert_eq!(
            layout.render_tabular_segment(&context),
            format!("{}/{}", context.name, context.uuid)
        );
    }

    #[test]
    fn test_storage_layout_render_namespace_path_with_slash() {
        let layout =
            StorageLayout::try_new_full("{name}/{uuid}".to_string(), "{name}/{uuid}".to_string())
                .unwrap();
        let namespace = NamespaceNameContext {
            name: "my_namespace".to_string(),
            uuid: Uuid::now_v7(),
        };
        let namespace_path = NamespacePath::new(vec![namespace.clone()]);

        assert_eq!(
            *layout.render_namespace_path(&namespace_path),
            vec![format!("{}/{}", namespace.name, namespace.uuid)]
        );
    }

    #[test]
    fn test_storage_layout_tabular_in_parent_layout_needs_at_least_one_template_parameter() {
        let namespace_template = "{uuid}";
        let invalid_tabular_template = "invalid";
        let layout = StorageLayout::try_new_parent(
            namespace_template.to_string(),
            invalid_tabular_template.to_string(),
        );
        let layout = layout
            .expect_err("Expected error due to missing template parameter in tabular template.");
        assert!(matches!(layout, StorageLayoutError::InvalidTemplate(_)));
    }

    #[test]
    fn test_storage_layout_namespace_in_parent_layout_needs_at_least_one_template_parameter() {
        let invalid_namespace_template = "invalid";
        let tabular_template = "{uuid}";
        let layout = StorageLayout::try_new_parent(
            invalid_namespace_template.to_string(),
            tabular_template.to_string(),
        );
        let layout = layout
            .expect_err("Expected error due to missing template parameter in namespace template.");
        assert!(matches!(layout, StorageLayoutError::InvalidTemplate(_)));
    }

    #[test]
    fn test_storage_layout_deserialization_of_parent_layout_should_fail_without_at_least_one_template_parameter_for_tabular_template()
     {
        let json = r#"
        {
            "namespace": "{uuid}",
            "tabular": "invalid"
        }
        "#;

        let result: Result<StorageLayoutParentNamespaceAndTabular, _> = serde_json::from_str(json);
        assert!(
            result.is_err(),
            "Expected deserialization to fail for parent-namespace-and-tabular layout without at least one template parameter in tabular template"
        );
    }

    #[test]
    fn test_storage_layout_deserialization_of_parent_layout_should_fail_without_at_least_one_template_parameter_for_namespace_template()
     {
        let json = r#"
        {
            "namespace": "invalid",
            "tabular": "{uuid}"
        }
        "#;

        let result: Result<StorageLayoutParentNamespaceAndTabular, _> = serde_json::from_str(json);
        assert!(
            result.is_err(),
            "Expected deserialization to fail for parent-namespace-and-tabular layout without at least one template parameter in namespace template"
        );
    }

    #[test]
    fn test_storage_layout_tabular_in_full_layout_needs_at_least_one_template_parameter() {
        let namespace_template = "{uuid}";
        let invalid_tabular_template = "invalid";
        let layout = StorageLayout::try_new_full(
            namespace_template.to_string(),
            invalid_tabular_template.to_string(),
        );
        let layout = layout
            .expect_err("Expected error due to missing template parameter in tabular template.");
        assert!(matches!(layout, StorageLayoutError::InvalidTemplate(_)));
    }

    #[test]
    fn test_storage_layout_namespace_in_full_layout_needs_at_least_one_template_parameter() {
        let invalid_namespace_template = "invalid";
        let tabular_template = "{uuid}";
        let layout = StorageLayout::try_new_full(
            invalid_namespace_template.to_string(),
            tabular_template.to_string(),
        );
        let layout = layout
            .expect_err("Expected error due to missing template parameter in namespace template.");
        assert!(matches!(layout, StorageLayoutError::InvalidTemplate(_)));
    }

    #[test]
    fn test_storage_layout_deserialization_of_full_layout_should_fail_without_at_least_one_template_parameter_for_tabular_template()
     {
        let json = r#"
        {
            "type": "full-hierarchy",
            "namespace": "{uuid}",
            "tabular": "invalid"
        }
        "#;

        let result: Result<StorageLayout, _> = serde_json::from_str(json);
        assert!(
            result.is_err(),
            "Expected deserialization to fail for full-hierarchy layout without at least one template parameter in tabular template"
        );
    }

    #[test]
    fn test_storage_layout_deserialization_of_full_layout_should_fail_without_at_least_one_template_parameter_for_namespace_template()
     {
        let json = r#"
        {
            "type": "full-hierarchy",
            "namespace": "invalid",
            "tabular": "{uuid}"
        }
        "#;

        let result: Result<StorageLayout, _> = serde_json::from_str(json);
        assert!(
            result.is_err(),
            "Expected deserialization to fail for full-hierarchy layout without at least one template parameter in namespace template"
        );
    }
}
