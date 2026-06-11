//! Pure tuple-shape helpers for the OpenFGA authorizer.
//!
//! Every `create_*` method in [`crate::authorizer`] writes two classes of tuples:
//!
//! * **hierarchy** — parent/child edges that are fully determined by the object
//!   graph in the Postgres catalog (server → project → warehouse → namespace →
//!   table/view, role → project).
//! * **ownership** — the `actor` who created the object. Not reconstructable from
//!   the catalog, only known at create time.
//!
//! Both the create-time path and the rebuild-from-catalog path call these
//! helpers so the two code paths cannot drift. Changing a hierarchy edge in
//! one place changes it in the other, and the drift-detector test in
//! [`crate::authorizer::tests`] asserts equivalence.
use lakekeeper::{
    ProjectId, WarehouseId,
    service::{
        Actor, GenericTableId, NamespaceId, RoleId, TableId, ViewId, authz::NamespaceParent,
    },
};
use openfga_client::client::TupleKey;

use crate::{
    entities::OpenFgaEntity,
    relations::{
        GenericTableRelation, NamespaceRelation, ProjectRelation, RoleRelation, ServerRelation,
        TableRelation, ViewRelation, WarehouseRelation,
    },
};

fn tuple(user: String, relation: String, object: String) -> TupleKey {
    TupleKey {
        user,
        relation,
        object,
        condition: None,
    }
}

/// Hierarchy tuples for a project: `server ↔ project`.
pub(crate) fn hierarchy_tuples_for_project(server: &str, project: &ProjectId) -> Vec<TupleKey> {
    let this_id = project.to_openfga();
    vec![
        tuple(
            server.to_string(),
            ProjectRelation::Server.to_string(),
            this_id.clone(),
        ),
        tuple(
            this_id,
            ServerRelation::Project.to_string(),
            server.to_string(),
        ),
    ]
}

/// Ownership tuple for a project: `actor -[project_admin]-> project`.
pub(crate) fn ownership_tuples_for_project(actor: &Actor, project: &ProjectId) -> Vec<TupleKey> {
    vec![tuple(
        actor.to_openfga(),
        ProjectRelation::ProjectAdmin.to_string(),
        project.to_openfga(),
    )]
}

/// Hierarchy tuples for a warehouse: `project ↔ warehouse`.
pub(crate) fn hierarchy_tuples_for_warehouse(
    project: &ProjectId,
    warehouse: WarehouseId,
) -> Vec<TupleKey> {
    let project_id = project.to_openfga();
    let this_id = warehouse.to_openfga();
    vec![
        tuple(
            project_id.clone(),
            WarehouseRelation::Project.to_string(),
            this_id.clone(),
        ),
        tuple(this_id, ProjectRelation::Warehouse.to_string(), project_id),
    ]
}

/// Ownership tuple for a warehouse.
pub(crate) fn ownership_tuples_for_warehouse(
    actor: &Actor,
    warehouse: WarehouseId,
) -> Vec<TupleKey> {
    vec![tuple(
        actor.to_openfga(),
        WarehouseRelation::Ownership.to_string(),
        warehouse.to_openfga(),
    )]
}

/// Hierarchy tuples for a namespace: `parent ↔ namespace`. The inverse relation
/// differs depending on whether the parent is a warehouse or another namespace.
pub(crate) fn hierarchy_tuples_for_namespace(
    parent: &NamespaceParent,
    namespace: NamespaceId,
) -> Vec<TupleKey> {
    let (parent_id, parent_child_relation) = match parent {
        NamespaceParent::Warehouse(warehouse_id) => (
            warehouse_id.to_openfga(),
            WarehouseRelation::Namespace.to_string(),
        ),
        NamespaceParent::Namespace(parent_namespace_id) => (
            parent_namespace_id.to_openfga(),
            NamespaceRelation::Child.to_string(),
        ),
    };
    let this_id = namespace.to_openfga();
    vec![
        tuple(
            parent_id.clone(),
            NamespaceRelation::Parent.to_string(),
            this_id.clone(),
        ),
        tuple(this_id, parent_child_relation, parent_id),
    ]
}

/// Ownership tuple for a namespace.
pub(crate) fn ownership_tuples_for_namespace(
    actor: &Actor,
    namespace: NamespaceId,
) -> Vec<TupleKey> {
    vec![tuple(
        actor.to_openfga(),
        NamespaceRelation::Ownership.to_string(),
        namespace.to_openfga(),
    )]
}

/// Hierarchy tuples for a table: `namespace ↔ table`.
pub(crate) fn hierarchy_tuples_for_table(
    warehouse: WarehouseId,
    table: TableId,
    parent_namespace: NamespaceId,
) -> Vec<TupleKey> {
    let parent_id = parent_namespace.to_openfga();
    let this_id = (warehouse, table).to_openfga();
    vec![
        tuple(
            parent_id.clone(),
            TableRelation::Parent.to_string(),
            this_id.clone(),
        ),
        tuple(this_id, NamespaceRelation::Child.to_string(), parent_id),
    ]
}

/// Ownership tuple for a table.
pub(crate) fn ownership_tuples_for_table(
    actor: &Actor,
    warehouse: WarehouseId,
    table: TableId,
) -> Vec<TupleKey> {
    vec![tuple(
        actor.to_openfga(),
        TableRelation::Ownership.to_string(),
        (warehouse, table).to_openfga(),
    )]
}

/// Hierarchy tuples for a generic table: `namespace ↔ generic_table`.
pub(crate) fn hierarchy_tuples_for_generic_table(
    warehouse: WarehouseId,
    generic_table: GenericTableId,
    parent_namespace: NamespaceId,
) -> Vec<TupleKey> {
    let parent_id = parent_namespace.to_openfga();
    let this_id = (warehouse, generic_table).to_openfga();
    vec![
        tuple(
            parent_id.clone(),
            GenericTableRelation::Parent.to_string(),
            this_id.clone(),
        ),
        tuple(this_id, NamespaceRelation::Child.to_string(), parent_id),
    ]
}

/// Ownership tuple for a generic table.
pub(crate) fn ownership_tuples_for_generic_table(
    actor: &Actor,
    warehouse: WarehouseId,
    generic_table: GenericTableId,
) -> Vec<TupleKey> {
    vec![tuple(
        actor.to_openfga(),
        GenericTableRelation::Ownership.to_string(),
        (warehouse, generic_table).to_openfga(),
    )]
}

/// Hierarchy tuples for a view: `namespace ↔ view`.
pub(crate) fn hierarchy_tuples_for_view(
    warehouse: WarehouseId,
    view: ViewId,
    parent_namespace: NamespaceId,
) -> Vec<TupleKey> {
    let parent_id = parent_namespace.to_openfga();
    let this_id = (warehouse, view).to_openfga();
    vec![
        tuple(
            parent_id.clone(),
            ViewRelation::Parent.to_string(),
            this_id.clone(),
        ),
        tuple(this_id, NamespaceRelation::Child.to_string(), parent_id),
    ]
}

/// Ownership tuple for a view.
pub(crate) fn ownership_tuples_for_view(
    actor: &Actor,
    warehouse: WarehouseId,
    view: ViewId,
) -> Vec<TupleKey> {
    vec![tuple(
        actor.to_openfga(),
        ViewRelation::Ownership.to_string(),
        (warehouse, view).to_openfga(),
    )]
}

/// Hierarchy tuples for a role: `project -[project]-> role`.
///
/// Note: there is no inverse `role → project` edge in the v4 schema; the role
/// type does not expose a `child`-style relation.
pub(crate) fn hierarchy_tuples_for_role(project: &ProjectId, role: RoleId) -> Vec<TupleKey> {
    vec![tuple(
        project.to_openfga(),
        RoleRelation::Project.to_string(),
        role.to_openfga(),
    )]
}

/// Ownership tuple for a role.
pub(crate) fn ownership_tuples_for_role(actor: &Actor, role: RoleId) -> Vec<TupleKey> {
    vec![tuple(
        actor.to_openfga(),
        RoleRelation::Ownership.to_string(),
        role.to_openfga(),
    )]
}

#[cfg(test)]
mod tests {
    //! Golden-value drift detector.
    //!
    //! These tests pin the exact set of tuples each `create_*` path writes,
    //! so that the rebuild-from-catalog path (which calls only the `hierarchy_*`
    //! helpers) stays provably equivalent to the create path's hierarchy tuples.
    //!
    //! Editing a helper body without updating the matching assertion here is
    //! the drift we want to catch.
    use std::{collections::HashSet, str::FromStr};

    use lakekeeper::service::{NamespaceId, ServerId, UserId};

    use super::*;

    fn uuid_of(c: char) -> uuid::Uuid {
        let s: String = std::iter::repeat_n(c, 32).collect();
        // Format: 8-4-4-4-12
        let formatted = format!(
            "{}-{}-{}-{}-{}",
            &s[0..8],
            &s[8..12],
            &s[12..16],
            &s[16..20],
            &s[20..32]
        );
        uuid::Uuid::parse_str(&formatted).unwrap()
    }

    fn fixed_project_id() -> ProjectId {
        ProjectId::from_str("11111111-1111-1111-1111-111111111111").unwrap()
    }

    fn fixed_warehouse_id() -> WarehouseId {
        WarehouseId::new(uuid_of('2'))
    }

    fn fixed_namespace_id() -> NamespaceId {
        NamespaceId::new(uuid_of('3'))
    }

    fn fixed_table_id() -> TableId {
        TableId::new(uuid_of('4'))
    }

    fn fixed_view_id() -> ViewId {
        ViewId::new(uuid_of('5'))
    }

    fn fixed_role_id() -> RoleId {
        RoleId::new(uuid_of('6'))
    }

    fn fixed_server_string() -> String {
        use crate::entities::OpenFgaEntity;
        let server = ServerId::new(uuid_of('7'));
        server.to_openfga()
    }

    fn fixed_actor() -> Actor {
        Actor::Principal(UserId::new_unchecked("oidc", "alice"))
    }

    fn tuple_set(v: Vec<TupleKey>) -> HashSet<(String, String, String)> {
        v.into_iter()
            .map(|t| (t.user, t.relation, t.object))
            .collect()
    }

    /// Golden tuples for a project: `server ↔ project` plus actor → `ProjectAdmin`.
    #[test]
    fn create_project_tuples_are_exactly_specified() {
        let server = fixed_server_string();
        let project = fixed_project_id();
        let actor = fixed_actor();

        let mut combined = hierarchy_tuples_for_project(&server, &project);
        combined.extend(ownership_tuples_for_project(&actor, &project));

        let expected: HashSet<(String, String, String)> = [
            (
                server.clone(),
                "server".to_string(),
                "project:11111111-1111-1111-1111-111111111111".to_string(),
            ),
            (
                "project:11111111-1111-1111-1111-111111111111".to_string(),
                "project".to_string(),
                server.clone(),
            ),
            (
                "user:oidc~alice".to_string(),
                "project_admin".to_string(),
                "project:11111111-1111-1111-1111-111111111111".to_string(),
            ),
        ]
        .into_iter()
        .collect();

        assert_eq!(tuple_set(combined), expected);
    }

    #[test]
    fn create_warehouse_tuples_are_exactly_specified() {
        let project = fixed_project_id();
        let warehouse = fixed_warehouse_id();
        let actor = fixed_actor();

        let mut combined = hierarchy_tuples_for_warehouse(&project, warehouse);
        combined.extend(ownership_tuples_for_warehouse(&actor, warehouse));

        let expected: HashSet<(String, String, String)> = [
            (
                "project:11111111-1111-1111-1111-111111111111".to_string(),
                "project".to_string(),
                "warehouse:22222222-2222-2222-2222-222222222222".to_string(),
            ),
            (
                "warehouse:22222222-2222-2222-2222-222222222222".to_string(),
                "warehouse".to_string(),
                "project:11111111-1111-1111-1111-111111111111".to_string(),
            ),
            (
                "user:oidc~alice".to_string(),
                "ownership".to_string(),
                "warehouse:22222222-2222-2222-2222-222222222222".to_string(),
            ),
        ]
        .into_iter()
        .collect();

        assert_eq!(tuple_set(combined), expected);
    }

    #[test]
    fn create_namespace_under_warehouse_tuples_are_exactly_specified() {
        let warehouse = fixed_warehouse_id();
        let namespace = fixed_namespace_id();
        let actor = fixed_actor();

        let parent = NamespaceParent::Warehouse(warehouse);
        let mut combined = hierarchy_tuples_for_namespace(&parent, namespace);
        combined.extend(ownership_tuples_for_namespace(&actor, namespace));

        let expected: HashSet<(String, String, String)> = [
            (
                "warehouse:22222222-2222-2222-2222-222222222222".to_string(),
                "parent".to_string(),
                "namespace:33333333-3333-3333-3333-333333333333".to_string(),
            ),
            (
                "namespace:33333333-3333-3333-3333-333333333333".to_string(),
                "namespace".to_string(),
                "warehouse:22222222-2222-2222-2222-222222222222".to_string(),
            ),
            (
                "user:oidc~alice".to_string(),
                "ownership".to_string(),
                "namespace:33333333-3333-3333-3333-333333333333".to_string(),
            ),
        ]
        .into_iter()
        .collect();

        assert_eq!(tuple_set(combined), expected);
    }

    #[test]
    fn create_namespace_under_namespace_tuples_are_exactly_specified() {
        let parent_ns = NamespaceId::new(uuid_of('8'));
        let child_ns = fixed_namespace_id();
        let actor = fixed_actor();

        let parent = NamespaceParent::Namespace(parent_ns);
        let mut combined = hierarchy_tuples_for_namespace(&parent, child_ns);
        combined.extend(ownership_tuples_for_namespace(&actor, child_ns));

        let expected: HashSet<(String, String, String)> = [
            (
                "namespace:88888888-8888-8888-8888-888888888888".to_string(),
                "parent".to_string(),
                "namespace:33333333-3333-3333-3333-333333333333".to_string(),
            ),
            (
                "namespace:33333333-3333-3333-3333-333333333333".to_string(),
                "child".to_string(),
                "namespace:88888888-8888-8888-8888-888888888888".to_string(),
            ),
            (
                "user:oidc~alice".to_string(),
                "ownership".to_string(),
                "namespace:33333333-3333-3333-3333-333333333333".to_string(),
            ),
        ]
        .into_iter()
        .collect();

        assert_eq!(tuple_set(combined), expected);
    }

    #[test]
    fn create_table_tuples_are_exactly_specified() {
        let warehouse = fixed_warehouse_id();
        let table = fixed_table_id();
        let parent_ns = fixed_namespace_id();
        let actor = fixed_actor();

        let mut combined = hierarchy_tuples_for_table(warehouse, table, parent_ns);
        combined.extend(ownership_tuples_for_table(&actor, warehouse, table));

        let expected: HashSet<(String, String, String)> = [
            (
                "namespace:33333333-3333-3333-3333-333333333333".to_string(),
                "parent".to_string(),
                "lakekeeper_table:22222222-2222-2222-2222-222222222222/44444444-4444-4444-4444-444444444444"
                    .to_string(),
            ),
            (
                "lakekeeper_table:22222222-2222-2222-2222-222222222222/44444444-4444-4444-4444-444444444444"
                    .to_string(),
                "child".to_string(),
                "namespace:33333333-3333-3333-3333-333333333333".to_string(),
            ),
            (
                "user:oidc~alice".to_string(),
                "ownership".to_string(),
                "lakekeeper_table:22222222-2222-2222-2222-222222222222/44444444-4444-4444-4444-444444444444"
                    .to_string(),
            ),
        ]
        .into_iter()
        .collect();

        assert_eq!(tuple_set(combined), expected);
    }

    #[test]
    fn create_view_tuples_are_exactly_specified() {
        let warehouse = fixed_warehouse_id();
        let view = fixed_view_id();
        let parent_ns = fixed_namespace_id();
        let actor = fixed_actor();

        let mut combined = hierarchy_tuples_for_view(warehouse, view, parent_ns);
        combined.extend(ownership_tuples_for_view(&actor, warehouse, view));

        let expected: HashSet<(String, String, String)> = [
            (
                "namespace:33333333-3333-3333-3333-333333333333".to_string(),
                "parent".to_string(),
                "lakekeeper_view:22222222-2222-2222-2222-222222222222/55555555-5555-5555-5555-555555555555"
                    .to_string(),
            ),
            (
                "lakekeeper_view:22222222-2222-2222-2222-222222222222/55555555-5555-5555-5555-555555555555"
                    .to_string(),
                "child".to_string(),
                "namespace:33333333-3333-3333-3333-333333333333".to_string(),
            ),
            (
                "user:oidc~alice".to_string(),
                "ownership".to_string(),
                "lakekeeper_view:22222222-2222-2222-2222-222222222222/55555555-5555-5555-5555-555555555555"
                    .to_string(),
            ),
        ]
        .into_iter()
        .collect();

        assert_eq!(tuple_set(combined), expected);
    }

    #[test]
    fn create_role_tuples_are_exactly_specified() {
        let project = fixed_project_id();
        let role = fixed_role_id();
        let actor = fixed_actor();

        let mut combined = hierarchy_tuples_for_role(&project, role);
        combined.extend(ownership_tuples_for_role(&actor, role));

        let expected: HashSet<(String, String, String)> = [
            (
                "project:11111111-1111-1111-1111-111111111111".to_string(),
                "project".to_string(),
                "role:66666666-6666-6666-6666-666666666666".to_string(),
            ),
            (
                "user:oidc~alice".to_string(),
                "ownership".to_string(),
                "role:66666666-6666-6666-6666-666666666666".to_string(),
            ),
        ]
        .into_iter()
        .collect();

        assert_eq!(tuple_set(combined), expected);
    }
}
