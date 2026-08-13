//! The grant facet: `/grants` served from the same tuples the
//! `/permissions/…/assignments` API writes.
//!
//! There is no second store and no migration. A grant is an assignment tuple
//! `(principal, privilege-relation, object)`, so the two APIs are two views of one
//! set of tuples, which is what lets `/grants` become the universal surface without
//! stranding anything written through the older one.
//!
//! Three mappings carry the whole facet, all derived from the per-level `API*Relation`
//! enums rather than restated here:
//!
//! * **vocabulary** — the enum's variants are exactly the assignable privileges;
//! * **assignment relation** — `ReducedRelation::to_openfga`, the relation a grant writes;
//! * **authority relation** — `GrantableRelation::grant_relation`, the relation a
//!   caller must hold to grant or revoke it.
//!
//! Adding an assignable relation to the model therefore extends `/grants` with no
//! change here.

use std::{collections::HashMap, str::FromStr, sync::LazyLock};

use lakekeeper::{
    api::{RequestMetadata, iceberg::v1::PaginationQuery},
    async_trait,
    service::authz::{
        AppliedGrants, ApplyGrantsError, AuthorizationDecision, CatalogGenericTableAction,
        CatalogNamespaceAction, CatalogProjectAction, CatalogServerAction, CatalogTableAction,
        CatalogTagAction, CatalogViewAction, CatalogWarehouseAction, GrantAuthorityCheck,
        GrantFilter, GrantListingNotImplemented, GrantNotSupported, GrantResource, GrantRow,
        GrantSpec, IsAllowedActionError, ListGrantsError, ListGrantsResultPage, MalformedGrant,
        ManagesGrants, PrivilegeDescriptor, ResourceType, UserOrRole, UserOrRoleId,
    },
};
use openfga_client::client::{
    CheckRequestTupleKey, ReadRequestTupleKey, TupleKey, TupleKeyWithoutCondition, WriteOptions,
};
use strum::IntoEnumIterator as _;

use crate::{
    authorizer::OpenFGAAuthorizer,
    entities::OpenFgaEntity,
    error::OpenFGABackendUnavailable,
    relations::{
        APIGenericTableRelation, APINamespaceRelation, APIProjectRelation, APIServerRelation,
        APITableRelation, APITagRelation, APIViewRelation, APIWarehouseRelation, GrantableRelation,
        ReducedRelation,
    },
};

/// Run `$body` with `$R` bound to the `API*Relation` enum of one resource level.
///
/// The eight-way dispatch exists once instead of once per operation, so a new level
/// is a single edit and no operation can silently omit one.
macro_rules! for_level {
    ($resource_type:expr, |$R:ident| $body:expr) => {{
        match $resource_type {
            ResourceType::Server => {
                type $R = APIServerRelation;
                $body
            }
            ResourceType::Project => {
                type $R = APIProjectRelation;
                $body
            }
            ResourceType::Warehouse => {
                type $R = APIWarehouseRelation;
                $body
            }
            ResourceType::Namespace => {
                type $R = APINamespaceRelation;
                $body
            }
            ResourceType::Table => {
                type $R = APITableRelation;
                $body
            }
            ResourceType::View => {
                type $R = APIViewRelation;
                $body
            }
            ResourceType::GenericTable => {
                type $R = APIGenericTableRelation;
                $body
            }
            ResourceType::Tag => {
                type $R = APITagRelation;
                $body
            }
        }
    }};
}

/// The privileges assignable at `resource_type`.
///
/// Built once per level. The set is fixed by the model, and this is read once per row on
/// the listing path, where rebuilding it would allocate a descriptor per privilege per
/// row.
pub(crate) fn vocabulary(resource_type: ResourceType) -> &'static [PrivilegeDescriptor] {
    static VOCABULARIES: LazyLock<HashMap<ResourceType, Vec<PrivilegeDescriptor>>> =
        LazyLock::new(|| {
            <ResourceType as strum::VariantArray>::VARIANTS
                .iter()
                .map(|resource_type| (*resource_type, build_vocabulary(*resource_type)))
                .collect()
        });
    VOCABULARIES.get(&resource_type).map_or(&[], Vec::as_slice)
}

fn build_vocabulary(resource_type: ResourceType) -> Vec<PrivilegeDescriptor> {
    for_level!(resource_type, |R| R::iter()
        .map(|relation| {
            let name: &'static str = relation.into();
            let documented = documentation_of(name);
            PrivilegeDescriptor {
                name: name.to_string(),
                display_name: name.replace('_', " "),
                description: documented.as_ref().map(|d| d.description.to_string()),
                category: documented.as_ref().map(|d| d.category.to_string()),
                resource_type,
            }
        })
        .collect())
}

/// How a privilege presents itself to a client: which group it belongs to, and what it
/// permits.
struct PrivilegeDocumentation {
    category: &'static str,
    description: &'static str,
}

/// Documentation for one privilege, for a picker that has to group and explain itself.
///
/// Keyed by name rather than by level: the same fourteen names cover all forty-odd
/// entries, because a privilege means the same thing wherever the model defines it.
/// Anything not listed reports nothing rather than a guess — a wrong explanation of a
/// permission is worse than none.
fn documentation_of(privilege: &str) -> Option<PrivilegeDocumentation> {
    let (category, description) = match privilege {
        // Server-level roles.
        "admin" => ("administration", {
            "Full administrative access to the server. Intended for people: an admin can \
             make themselves project admin on any project, and that step is recorded in the \
             audit log."
        }),
        "operator" => ("administration", {
            "Unrestricted use of every API in the catalog — the most powerful role there \
             is. Intended for machine accounts that provision resources, not for people."
        }),
        // Project-level roles.
        "project_admin" => ("administration", {
            "Full control of the project, including privileges that need their own admin \
             role. Never allowed to become empty, so a project cannot lock everyone out."
        }),
        "security_admin" => ("administration", {
            "Manage the project's security: grants, ownership, roles and tag definitions. \
             Deliberately confers no access to data and no ability to change objects."
        }),
        "data_admin" => ("administration", {
            "Manage every aspect of the project's warehouses and their contents, but not \
             grant privileges to anyone."
        }),
        "role_creator" => ("administration", {
            "Create new roles in the project. Does not confer the ability to add members to \
             roles that already exist."
        }),
        "tag_creator" => ("administration", {
            "Create new governance tag definitions in the project. Managing a definition \
             that already exists — updating it, deleting it, or delegating who may apply \
             it — is not conferred."
        }),
        // Object-level privileges.
        "ownership" => ("security", {
            "Own the object. Implies its full privilege set, including managing its grants."
        }),
        "pass_grants" => ("security", {
            "Grant others the privileges you already hold on this object. Delegating a \
             privilege you do not hold requires `manage_grants` instead."
        }),
        "manage_grants" => ("security", {
            "Grant and revoke any privilege on this object and everything beneath it."
        }),
        "manage_tags" => ("metadata", {
            "Attach and detach governance tags on this object, its columns, and everything \
             beneath it. Independent of `modify`, so a steward can classify objects without \
             the right to change them. Attaching a specific tag additionally requires the \
             right to apply that tag."
        }),
        "describe" => ("metadata", "Read the object's metadata."),
        "select" => ("read", "Read the object's data."),
        "create" => ("create", "Create new objects inside this one."),
        "modify" => (
            "write",
            "Change the object and its contents, including schema changes.",
        ),
        // Tag definitions.
        "apply" => ("metadata", {
            "Attach and detach this tag. Attaching it to an object additionally requires \
             the right to manage tags on that object."
        }),
        _ => return None,
    };
    Some(PrivilegeDocumentation {
        category,
        description,
    })
}

/// The relation a grant of `privilege` writes, or `None` if the name is not in this
/// level's vocabulary.
fn assignment_relation(resource_type: ResourceType, privilege: &str) -> Option<String> {
    for_level!(resource_type, |R| R::from_str(privilege)
        .ok()
        .map(|relation| relation.to_openfga().to_string()))
}

/// Is `privilege` in this level's vocabulary?
///
/// Parses rather than searching a rebuilt vocabulary: this runs once per listed row and
/// once per diff entry, and the vocabulary allocates a descriptor per privilege.
pub(crate) fn is_known_privilege(resource_type: ResourceType, privilege: &str) -> bool {
    for_level!(resource_type, |R| R::from_str(privilege).is_ok())
}

/// The relation a caller must hold to grant or revoke `privilege`.
fn authority_relation(resource_type: ResourceType, privilege: &str) -> Option<String> {
    for_level!(resource_type, |R| R::from_str(privilege)
        .ok()
        .map(|relation| relation.grant_relation().to_string()))
}

/// The privilege a stored `relation` represents, or `None` if it is not assignable at
/// this level — a structural relation (`parent`, `project`, `child`) or one this
/// version does not know.
///
/// Resolved by search rather than by assuming the API name and the model relation
/// share a spelling, which is true today at every level but is not a rule.
fn privilege_of_relation(resource_type: ResourceType, relation: &str) -> Option<String> {
    for_level!(resource_type, |R| R::iter()
        .find(|candidate| candidate.to_openfga().to_string() == relation)
        .map(|candidate| Into::<&'static str>::into(candidate).to_string()))
}

/// The relation that gates reading grants at `resource_type`, used as the guard when
/// answering for another principal.
fn read_grants_relation(resource_type: ResourceType) -> String {
    match resource_type {
        ResourceType::Server => CatalogServerAction::ReadGrants.to_openfga().to_string(),
        ResourceType::Project => CatalogProjectAction::ReadGrants.to_openfga().to_string(),
        ResourceType::Warehouse => CatalogWarehouseAction::ReadGrants.to_openfga().to_string(),
        ResourceType::Namespace => CatalogNamespaceAction::ReadGrants.to_openfga().to_string(),
        ResourceType::Table => CatalogTableAction::ReadGrants.to_openfga().to_string(),
        ResourceType::View => CatalogViewAction::ReadGrants.to_openfga().to_string(),
        ResourceType::GenericTable => CatalogGenericTableAction::ReadGrants
            .to_openfga()
            .to_string(),
        ResourceType::Tag => CatalogTagAction::ReadGrants.to_openfga().to_string(),
    }
}

/// The `OpenFGA` object a grant resource addresses.
pub(crate) fn grant_object(authorizer: &OpenFGAAuthorizer, resource: &GrantResource) -> String {
    match resource {
        GrantResource::Server => authorizer.openfga_server(),
        GrantResource::Project(project_id) => project_id.to_openfga(),
        GrantResource::Warehouse(warehouse_id) => warehouse_id.to_openfga(),
        // Namespace objects carry no warehouse: ids are unique across warehouses.
        GrantResource::Namespace { namespace_id, .. } => namespace_id.to_openfga(),
        GrantResource::Table {
            warehouse_id,
            table_id,
        } => (*warehouse_id, *table_id).to_openfga(),
        GrantResource::View {
            warehouse_id,
            view_id,
        } => (*warehouse_id, *view_id).to_openfga(),
        GrantResource::GenericTable {
            warehouse_id,
            generic_table_id,
        } => (*warehouse_id, *generic_table_id).to_openfga(),
        GrantResource::Tag(tag_definition_id) => tag_definition_id.to_openfga(),
    }
}

/// Below the warehouse, managed access has no public userset, so a request made under
/// an assumed role cannot be evaluated. Same restriction the assignments API applies.
fn assumed_role_restriction(
    metadata: &RequestMetadata,
    resource: &GrantResource,
) -> Result<(), GrantNotSupported> {
    let assumed_role = matches!(
        metadata.actor(),
        lakekeeper::service::Actor::Role {
            principal: _,
            assumed_role: _
        }
    );
    let below_warehouse = matches!(
        resource,
        GrantResource::Namespace { .. }
            | GrantResource::Table { .. }
            | GrantResource::View { .. }
            | GrantResource::GenericTable { .. }
    );
    if assumed_role && below_warehouse {
        return Err(GrantNotSupported::new(
            "Granting or revoking below the warehouse is not supported while acting under an assumed role",
        ));
    }
    Ok(())
}

impl OpenFGAAuthorizer {
    /// Which of `checks` the actor (or `for_user`) may grant and revoke on `resource`,
    /// in order.
    ///
    /// A privilege outside this level's vocabulary is a deny, not an error: the name
    /// may come from another authorizer's vocabulary, and answering "not allowed" is
    /// both true and safe.
    ///
    /// The grantee is ignored. Every `can_grant_*` relation is a property of the actor,
    /// the resource and the privilege — nothing in the model reads who receives the
    /// privilege — so checks that differ only in their grantee collapse into one check
    /// whose answer is repeated for each. A diff handing one privilege to a hundred
    /// principals is a single check.
    pub(crate) async fn grant_authority(
        &self,
        metadata: &RequestMetadata,
        for_user: Option<&UserOrRole>,
        resource: &GrantResource,
        checks: &[GrantAuthorityCheck<'_>],
    ) -> Result<Vec<AuthorizationDecision>, IsAllowedActionError> {
        let resource_type = resource.resource_type();
        let object = grant_object(self, resource);
        let user = for_user.map_or_else(
            || metadata.actor().to_openfga(),
            |u| u.api_user_or_role().to_openfga(),
        );

        // Keep the request dense: unknown privileges get no check, and repeated
        // privileges share one. Both are filled back in from `checked_item` afterwards.
        let mut item_of_privilege: HashMap<&str, usize> = HashMap::new();
        let mut checked_item = Vec::with_capacity(checks.len());
        let mut items: Vec<CheckRequestTupleKey> = Vec::with_capacity(checks.len());
        for check in checks {
            let Some(relation) = authority_relation(resource_type, check.privilege) else {
                checked_item.push(None);
                continue;
            };
            let item = *item_of_privilege.entry(check.privilege).or_insert_with(|| {
                items.push(CheckRequestTupleKey {
                    user: user.clone(),
                    relation,
                    object: object.clone(),
                });
                items.len() - 1
            });
            checked_item.push(Some(item));
        }

        let guard_tuples = if for_user.is_some() {
            vec![CheckRequestTupleKey {
                user: metadata.actor().to_openfga(),
                relation: read_grants_relation(resource_type),
                object: object.clone(),
            }]
        } else {
            vec![]
        };

        let checked = self
            .check_actions_with_permission_guard(metadata.actor(), items, guard_tuples)
            .await?;

        // A check with no item is an unknown privilege; a missing result would be a bug
        // in the batch, and denying is the safe reading of both.
        Ok(checked_item
            .into_iter()
            .map(|item| {
                item.and_then(|item| checked.get(item).cloned())
                    .unwrap_or_else(AuthorizationDecision::deny)
            })
            .collect())
    }
}

#[async_trait::async_trait]
impl ManagesGrants for OpenFGAAuthorizer {
    /// One idempotent `Write`, so the diff lands atomically.
    ///
    /// The management API caps a diff at 100 entries, which is also `OpenFGA`'s
    /// per-write tuple limit, so no chunking is needed — chunking would give up
    /// atomicity, which is the reason this is one method.
    ///
    /// **Over-reports.** `Write` returns no per-tuple result, so re-applying a grant
    /// that is already held reports it as created. Callers use the return value to
    /// emit events, so the `OpenFGA` arm may emit an event for a no-op.
    async fn apply_grants(
        &self,
        metadata: &RequestMetadata,
        writes: &[GrantSpec],
        deletes: &[GrantSpec],
    ) -> Result<AppliedGrants, ApplyGrantsError> {
        for spec in writes.iter().chain(deletes) {
            assumed_role_restriction(metadata, &spec.resource)?;
        }

        // `created` is built in this loop rather than from `writes`, so a spec with no
        // relation cannot be reported as created without a tuple having been written.
        // The delete side below is built the same way, for the same reason.
        let mut created = Vec::with_capacity(writes.len());
        let mut write_tuples = Vec::with_capacity(writes.len());
        for spec in writes {
            if let Some(relation) =
                assignment_relation(spec.resource.resource_type(), &spec.privilege)
            {
                write_tuples.push(TupleKey {
                    user: spec.principal.to_openfga(),
                    relation,
                    object: grant_object(self, &spec.resource),
                    condition: None,
                });
                created.push(spec.clone());
            }
        }

        // A revoke is deliberately not validated against the vocabulary, so a delete
        // may name a privilege this model has no relation for. No such tuple can
        // exist, so there is nothing to delete and nothing to report as removed.
        let mut removed = Vec::with_capacity(deletes.len());
        let mut delete_tuples = Vec::with_capacity(deletes.len());
        for spec in deletes {
            if let Some(relation) =
                assignment_relation(spec.resource.resource_type(), &spec.privilege)
            {
                delete_tuples.push(TupleKeyWithoutCondition {
                    user: spec.principal.to_openfga(),
                    relation,
                    object: grant_object(self, &spec.resource),
                });
                removed.push(spec.clone());
            }
        }

        if write_tuples.is_empty() && delete_tuples.is_empty() {
            return Ok(AppliedGrants::default());
        }

        self.client
            .write_with_options(
                Some(write_tuples).filter(|t| !t.is_empty()),
                Some(delete_tuples).filter(|t| !t.is_empty()),
                WriteOptions::new_idempotent(),
            )
            .await
            .inspect_err(|e| tracing::error!("Failed to apply grants in OpenFGA: {e}"))
            .map_err(|e| {
                ApplyGrantsError::BackendUnavailable(
                    OpenFGABackendUnavailable::from(Box::new(e)).into(),
                )
            })?;

        Ok(AppliedGrants { created, removed })
    }

    async fn list_grants(
        &self,
        _metadata: &RequestMetadata,
        filter: GrantFilter,
        pagination: PaginationQuery,
    ) -> Result<ListGrantsResultPage, ListGrantsError> {
        match filter {
            GrantFilter::ByResource {
                resource,
                principal,
            } => {
                self.list_grants_on(resource, principal.as_ref(), pagination)
                    .await
            }
            // Tuples are indexed by object, not by principal or by project, so
            // answering these would mean reading the store a level at a time and
            // resolving every object back to its project through the hierarchy: one
            // unpageable response sized by the deployment rather than the request.
            // Refused instead. A resource's own listing answers the same question
            // within its scope and pages normally, and OpenFGA can be queried
            // directly for a store-wide view.
            GrantFilter::ByPrincipal { .. } | GrantFilter::ByProject(_) => {
                Err(GrantListingNotImplemented::new(
                    "Listing grants across a project is not supported by the OpenFGA \
                     authorizer, which indexes permissions by resource. Read one \
                     resource's grants from its own endpoint instead.",
                )
                .into())
            }
        }
    }
}

impl OpenFGAAuthorizer {
    /// Every grant held on one resource: a single `Read` of that object, since the
    /// object is known exactly. Structural tuples on the same object (`parent`,
    /// `project`, `child`, `managed_access`) are dropped by the relation lookup.
    ///
    /// A principal narrows the same `Read` by its user field, so it costs no extra
    /// round trip and pages the same way.
    async fn list_grants_on(
        &self,
        resource: GrantResource,
        principal: Option<&UserOrRoleId>,
        pagination: PaginationQuery,
    ) -> Result<ListGrantsResultPage, ListGrantsError> {
        let resource_type = resource.resource_type();
        let page_size = clamp_page_size(&pagination);
        // Higher consistency, as role-assignment listings use: a caller that just
        // wrote a grant expects to read it back. Cold path; the hot `Check` path is
        // unaffected.
        let response = self
            .read_higher_consistency(
                page_size,
                ReadRequestTupleKey {
                    user: principal.map(OpenFgaEntity::to_openfga).unwrap_or_default(),
                    relation: String::new(),
                    object: grant_object(self, &resource),
                },
                pagination.page_token.as_option().map(ToString::to_string),
            )
            .await
            .map_err(unavailable)?;

        let mut grants = Vec::new();
        for tuple in response.tuples {
            let created_at = tuple_timestamp(tuple.timestamp.map(|ts| (ts.seconds, ts.nanos)));
            let key = require_key(tuple.key)?;
            let Some(privilege) = privilege_of_relation(resource_type, &key.relation) else {
                continue;
            };
            grants.push(GrantRow {
                principal: parse_grant_principal(&key.user)?,
                resource: resource.clone(),
                privilege,
                // A tuple has nowhere to record who wrote it.
                created_at,
            });
        }

        Ok(ListGrantsResultPage {
            grants,
            next_page_token: Some(response.continuation_token).filter(|t| !t.is_empty()),
        })
    }
}

/// `OpenFGA`'s `Read` caps `page_size` at 100. Clamp rather than turn an over-large
/// request into a backend error; the caller pages with the token.
fn clamp_page_size(pagination: &PaginationQuery) -> i32 {
    pagination
        .page_size
        .and_then(|s| i32::try_from(s).ok())
        .filter(|s| *s > 0)
        .unwrap_or(100)
        .min(100)
}

fn unavailable(err: OpenFGABackendUnavailable) -> ListGrantsError {
    ListGrantsError::BackendUnavailable(err.into())
}

/// A `Read` response tuple always carries a key. A missing one is a malformed
/// response, not an empty grant — dropping it would silently shorten the page.
fn require_key(
    key: Option<openfga_client::client::TupleKey>,
) -> Result<openfga_client::client::TupleKey, MalformedGrant> {
    key.ok_or_else(|| {
        MalformedGrant::new(
            "authorization backend returned a tuple without a key",
            lakekeeper::service::InternalErrorMessage(
                "OpenFGA Read response contained a tuple with no key".to_string(),
            ),
        )
    })
}

/// Lakekeeper wrote these subjects, so one it cannot parse is an invariant violation
/// (500), not a grant to skip.
fn parse_grant_principal(subject: &str) -> Result<UserOrRoleId, MalformedGrant> {
    use crate::entities::ParseOpenFgaEntity as _;

    let parsed = lakekeeper::api::management::v1::check::UserOrRole::parse_from_openfga(subject)
        .map_err(|e| {
            MalformedGrant::new("authorization backend returned an unparseable principal", e)
        })?;
    Ok(match parsed {
        lakekeeper::api::management::v1::check::UserOrRole::User(user_id) => {
            UserOrRoleId::User(user_id)
        }
        lakekeeper::api::management::v1::check::UserOrRole::Role(assignee) => {
            UserOrRoleId::Role(assignee.role_id())
        }
    })
}

/// `OpenFGA` records when a tuple was written. `nanos` outside `[0, 1e9)` is
/// malformed; clamp instead of panicking, and report `None` if the whole stamp is
/// unusable.
fn tuple_timestamp(seconds_and_nanos: Option<(i64, i32)>) -> Option<chrono::DateTime<chrono::Utc>> {
    seconds_and_nanos.and_then(|(seconds, nanos)| {
        chrono::DateTime::from_timestamp(seconds, u32::try_from(nanos).unwrap_or(0))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FgaType;

    /// The `OpenFGA` type each resource level is stored under. Lives here because the
    /// sweep test is its only caller; the match stays exhaustive, so a new resource
    /// level still cannot slip past it.
    fn fga_type(resource_type: ResourceType) -> FgaType {
        match resource_type {
            ResourceType::Server => FgaType::Server,
            ResourceType::Project => FgaType::Project,
            ResourceType::Warehouse => FgaType::Warehouse,
            ResourceType::Namespace => FgaType::Namespace,
            ResourceType::Table => FgaType::Table,
            ResourceType::View => FgaType::View,
            ResourceType::GenericTable => FgaType::GenericTable,
            ResourceType::Tag => FgaType::Tag,
        }
    }

    /// Deleting a principal sweeps the object types listed in `user_of`, so every type a
    /// grant can name must be there — otherwise that principal's grants on it survive the
    /// delete. Derived from the grantable levels rather than restated, because the bug
    /// this pins was a hand-maintained list that fell one type behind the model: tag
    /// grants outlived the user who held them, and a re-login with the same id got them
    /// back.
    #[test]
    fn every_grantable_object_type_is_swept_when_a_principal_is_deleted() {
        use crate::models::OpenFgaType as _;

        for principal in [FgaType::User, FgaType::Role] {
            let swept = principal.user_of();
            for resource_type in <ResourceType as strum::VariantArray>::VARIANTS {
                let object_type = fga_type(*resource_type);
                assert!(
                    swept.contains(&object_type),
                    "deleting a {principal} leaves its `{}` grants behind: {object_type} is \
                     missing from `user_of`",
                    resource_type.as_str()
                );
            }
        }
    }

    /// Every level, taken from the enum rather than listed: a test that enumerates the
    /// levels by hand stops covering the newest one exactly when it matters.
    fn every_level() -> impl Iterator<Item = ResourceType> {
        <ResourceType as strum::VariantArray>::VARIANTS
            .iter()
            .copied()
    }

    #[test]
    fn every_level_publishes_a_non_empty_vocabulary() {
        for resource_type in every_level() {
            let privileges = vocabulary(resource_type);
            assert!(
                !privileges.is_empty(),
                "{resource_type:?} publishes no privileges"
            );
            for privilege in privileges {
                assert_eq!(privilege.resource_type, resource_type);
            }
        }
    }

    /// The vocabulary and the parser are two derivations of the same enum —
    /// `IntoStaticStr` for the published names, `EnumString` for recognition — and
    /// nothing else ties them together. Membership pins published ⊆ recognized; the
    /// count pins recognized ⊆ published, so a variant cannot be writable while
    /// absent from `grantable-privileges`.
    #[test]
    fn the_vocabulary_and_the_parser_agree() {
        for resource_type in every_level() {
            let published = vocabulary(resource_type);
            for privilege in published {
                assert!(
                    is_known_privilege(resource_type, &privilege.name),
                    "{resource_type:?} publishes `{}` but does not recognize it",
                    privilege.name
                );
            }
            let declared = for_level!(resource_type, |R| R::iter().count());
            assert_eq!(
                published.len(),
                declared,
                "{resource_type:?} publishes {} privileges but declares {declared} \
                 assignable relations",
                published.len(),
            );
        }
    }

    #[test]
    fn the_warehouse_vocabulary_is_the_assignable_relations() {
        let names: Vec<String> = vocabulary(ResourceType::Warehouse)
            .iter()
            .map(|p| p.name.clone())
            .collect();
        assert_eq!(
            names,
            vec![
                "ownership",
                "pass_grants",
                "manage_grants",
                "describe",
                "select",
                "create",
                "modify",
                "manage_tags"
            ]
        );
    }

    #[test]
    fn a_privilege_maps_to_its_assignment_and_authority_relations() {
        assert_eq!(
            assignment_relation(ResourceType::Warehouse, "select"),
            Some("select".to_string())
        );
        assert_eq!(
            authority_relation(ResourceType::Warehouse, "select"),
            Some("can_grant_select".to_string())
        );
    }

    #[test]
    fn a_name_outside_the_level_has_no_relations() {
        // `get_metadata` is a warehouse *action*, never an assignable privilege, and
        // `select` is not in the server vocabulary.
        assert_eq!(
            assignment_relation(ResourceType::Warehouse, "get_metadata"),
            None
        );
        assert_eq!(
            authority_relation(ResourceType::Warehouse, "get_metadata"),
            None
        );
        assert_eq!(assignment_relation(ResourceType::Server, "select"), None);
    }

    #[test]
    fn every_privilege_round_trips_through_its_stored_relation() {
        for resource_type in every_level() {
            for privilege in vocabulary(resource_type) {
                let relation = assignment_relation(resource_type, &privilege.name)
                    .expect("a published privilege has an assignment relation");
                assert_eq!(
                    privilege_of_relation(resource_type, &relation),
                    Some(privilege.name.clone()),
                    "{resource_type:?} {} did not round-trip",
                    privilege.name
                );
            }
        }
    }

    #[test]
    fn every_published_privilege_is_grouped_and_explained() {
        for resource_type in every_level() {
            for privilege in vocabulary(resource_type) {
                assert!(
                    privilege.description.is_some(),
                    "{resource_type:?} publishes `{}` with no description",
                    privilege.name
                );
                assert!(
                    privilege.category.is_some(),
                    "{resource_type:?} publishes `{}` with no category",
                    privilege.name
                );
            }
        }
    }

    #[test]
    fn a_structural_relation_is_not_a_privilege() {
        assert_eq!(
            privilege_of_relation(ResourceType::Namespace, "parent"),
            None
        );
        assert_eq!(
            privilege_of_relation(ResourceType::Warehouse, "project"),
            None
        );
        assert_eq!(
            privilege_of_relation(ResourceType::Warehouse, "managed_access"),
            None
        );
    }

    #[test]
    fn the_read_gate_relation_is_can_read_assignments_at_every_level() {
        for resource_type in every_level() {
            assert_eq!(read_grants_relation(resource_type), "can_read_assignments");
        }
    }
}
