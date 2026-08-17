use crate::FgaType;

pub(crate) trait OpenFgaType {
    fn user_of(&self) -> &[FgaType];

    fn usersets(&self) -> &'static [&'static str];
}

impl OpenFgaType for FgaType {
    fn user_of(&self) -> &[FgaType] {
        match self {
            FgaType::Server => &[FgaType::Project],
            // Every object type a principal can appear on as the `user` side, which is
            // what deleting a principal must sweep. Tags belong here: `apply` and
            // `ownership` on a tag definition are held by a user or role like any other
            // grant. Omitting one leaves the principal's grants on that type behind, and
            // for a user that is not inert — the id is stable across re-login and
            // `create_user` has no `require_no_relations` guard, so a returning account
            // silently regains them.
            FgaType::User | FgaType::Role => &[
                FgaType::Role,
                FgaType::Server,
                FgaType::Project,
                FgaType::Warehouse,
                FgaType::Namespace,
                FgaType::Table,
                FgaType::View,
                FgaType::GenericTable,
                FgaType::Tag,
            ],
            FgaType::Project => &[FgaType::Server, FgaType::Warehouse],
            FgaType::Warehouse => &[FgaType::Project, FgaType::Namespace],
            FgaType::Namespace => &[
                FgaType::Warehouse,
                FgaType::Namespace,
                FgaType::Table,
                FgaType::View,
                FgaType::GenericTable,
            ],
            FgaType::View | FgaType::Table | FgaType::GenericTable => &[FgaType::Namespace],
            // The other direction: a tag definition is only ever the *object* of a
            // relation (project→tag, user/role→tag), never the `user` side, so — like
            // model_version — it is a user of nothing. That is why deleting a tag
            // definition needs no user-side sweep; it says nothing about deleting a
            // principal that holds one, which is the arm above.
            FgaType::Tag | FgaType::ModelVersion => &[],
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
