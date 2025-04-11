use crate::service::{authz::implementations::FgaType, RoleId};

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub(crate) struct RoleAssignee(RoleId);

impl RoleAssignee {
    #[must_use]
    pub(crate) fn from_role(role: RoleId) -> Self {
        RoleAssignee(role)
    }

    #[must_use]
    pub(crate) fn role(&self) -> &RoleId {
        &self.0
    }
}

impl RoleId {
    #[must_use]
    pub(crate) fn into_assignees(self) -> RoleAssignee {
        RoleAssignee::from_role(self)
    }
}

pub(crate) trait OpenFgaType {
    fn user_of(&self) -> &[FgaType];

    fn usersets(&self) -> &'static [&'static str];
}

impl OpenFgaType for FgaType {
    fn user_of(&self) -> &[FgaType] {
        match self {
            FgaType::Server => &[FgaType::Project],
            FgaType::User | FgaType::Role => &[
                FgaType::Role,
                FgaType::Server,
                FgaType::Project,
                FgaType::Warehouse,
                FgaType::Namespace,
                FgaType::Table,
                FgaType::View,
            ],
            FgaType::Project => &[FgaType::Server, FgaType::Warehouse],
            FgaType::Warehouse => &[FgaType::Project, FgaType::Namespace],
            FgaType::Namespace => &[
                FgaType::Warehouse,
                FgaType::Namespace,
                FgaType::Table,
                FgaType::View,
            ],
            FgaType::View | FgaType::Table => &[FgaType::Namespace],
            FgaType::ModelVersion => &[],
            FgaType::AuthModelId => &[FgaType::ModelVersion],
        }
    }

    /// Usersets of this type that are used in relations to other types
    fn usersets(&self) -> &'static [&'static str] {
        match self {
            FgaType::Role => &["assignee"],
            _ => &[],
        }
    }
}
