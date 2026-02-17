use std::{borrow::Cow, slice::Iter};

use serde::{Deserialize, Serialize};
use strum::Display;
use uuid::Uuid;

#[derive(Debug, Display, thiserror::Error)]
pub enum StorageLayoutError {
    InvalidTemplate(String),
}

pub trait PathSegmentContext {
    fn get_name(&self) -> Cow<'_, str>;

    fn get_uuid(&self) -> Uuid;
}

pub trait TemplatedPathSegmentRenderer {
    type Context: PathSegmentContext;

    fn template(&self) -> Cow<'_, str>;

    fn render(&self, context: &Self::Context) -> String {
        let template = self.template();
        let uuid = context.get_uuid();
        let name = context.get_name();
        template
            .replace("{uuid}", &uuid.to_string())
            .replace("{name}", &name)
    }
}

#[derive(Debug, Clone, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub enum StorageLayout {
    Flat(TableNameRenderer),
    Parent(NamespaceNameRenderer, TableNameRenderer),
    Full(NamespaceNameRenderer, TableNameRenderer),
}

impl StorageLayout {
    pub fn try_new_flat(table_template: String) -> Result<Self, StorageLayoutError> {
        if !table_template.contains("{uuid}") {
            return Err(StorageLayoutError::InvalidTemplate(format!(
                "For the 'Flat' layout, the table template '{table_template}' must contain the {{uuid}} placeholder to prevent path collisions."
            )));
        }
        Ok(Self::Flat(TableNameRenderer(table_template)))
    }

    pub fn try_new_parent(
        namespace_template: String,
        table_template: String,
    ) -> Result<Self, StorageLayoutError> {
        Ok(Self::Parent(
            NamespaceNameRenderer(namespace_template),
            TableNameRenderer(table_template),
        ))
    }

    pub fn try_new_full(
        namespace_template: String,
        table_template: String,
    ) -> Result<Self, StorageLayoutError> {
        Ok(Self::Full(
            NamespaceNameRenderer(namespace_template),
            TableNameRenderer(table_template),
        ))
    }

    pub fn render_table_segment(&self, context: &TableNameContext) -> String {
        let renderer = match self {
            StorageLayout::Flat(renderer) => renderer,
            StorageLayout::Parent(_, renderer) => renderer,
            StorageLayout::Full(_, renderer) => renderer,
        };
        renderer.render(context)
    }

    pub fn render_namespace_path(&self, path_context: &NamespacePath) -> Vec<String> {
        match self {
            StorageLayout::Flat(_) => vec![],
            StorageLayout::Parent(renderer, _) => path_context
                .namespace()
                .map(|path| vec![renderer.render(path)])
                .unwrap_or_else(|| vec![]),
            StorageLayout::Full(renderer, _) => path_context
                .into_iter()
                .map(|path| renderer.render(path))
                .collect(),
        }
    }
}

impl Default for StorageLayout {
    fn default() -> Self {
        StorageLayout::Flat(TableNameRenderer("{uuid}".to_string()))
    }
}

#[derive(Debug, Clone, Default)]
pub struct NamespacePath(pub(super) Vec<NamespaceNameContext>);

impl NamespacePath {
    pub fn new(segments: Vec<NamespaceNameContext>) -> Self {
        Self(segments)
    }

    pub fn namespace(&self) -> Option<&NamespaceNameContext> {
        self.0.last()
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
pub struct NamespaceNameRenderer(pub(super) String);

impl TemplatedPathSegmentRenderer for NamespaceNameRenderer {
    type Context = NamespaceNameContext;

    fn template(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.0)
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
pub struct TableNameRenderer(pub(super) String);

impl TemplatedPathSegmentRenderer for TableNameRenderer {
    type Context = TableNameContext;

    fn template(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.0)
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
            renderer.render(&context),
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

        let StorageLayout::Parent(namespace_renderer, table_renderer) = layout else {
            panic!("Expected parent storage layout");
        };

        assert_eq!(
            table_renderer.render(&table_context),
            format!("{}-{}", table_context.name, table_context.uuid)
        );
        assert_eq!(
            namespace_renderer.render(&namespace_context),
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

        let StorageLayout::Full(namespace_renderer, table_renderer) = layout else {
            panic!("Expected full storage layout");
        };

        assert_eq!(
            table_renderer.render(&table_context),
            format!("{}-{}", table_context.name, table_context.uuid)
        );
        assert_eq!(
            namespace_renderer.render(&namespace_context),
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
}
