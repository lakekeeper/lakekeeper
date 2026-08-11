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
    ProjectId, WarehouseId,
    api::{
        RequestMetadata,
        iceberg::v1::{PageToken, PaginationQuery},
    },
    async_trait,
    service::{
        GenericTableId, InternalErrorMessage, NamespaceId, TableId, TagDefinitionId, ViewId,
        authz::{
            AppliedGrants, ApplyGrantsError, AuthorizationDecision, CatalogGenericTableAction,
            CatalogNamespaceAction, CatalogProjectAction, CatalogServerAction, CatalogTableAction,
            CatalogTagAction, CatalogViewAction, CatalogWarehouseAction, GrantFilter,
            GrantListingNotPageable, GrantListingTooLarge, GrantNotSupported, GrantResource,
            GrantRow, GrantSpec, IsAllowedActionError, ListGrantsError, ListGrantsResultPage,
            MalformedGrant, ManagesGrants, PrivilegeDescriptor, ResourceType, UserOrRole,
            UserOrRoleId,
        },
    },
};
use openfga_client::client::{
    CheckRequestTupleKey, ReadRequestTupleKey, TupleKey, TupleKeyWithoutCondition, WriteOptions,
};
use strum::IntoEnumIterator as _;

use crate::{
    FgaType,
    authorizer::OpenFGAAuthorizer,
    entities::OpenFgaEntity,
    error::OpenFGABackendUnavailable,
    reconcile::{parse_uuid, split_fga},
    relations::{
        APIGenericTableRelation, APINamespaceRelation, APIProjectRelation, APIServerRelation,
        APITableRelation, APITagRelation, APIViewRelation, APIWarehouseRelation, GrantableRelation,
        ReducedRelation,
    },
};

/// A namespace tree deeper than this is a cycle, not a hierarchy. Bounds the parent
/// walk that recovers a namespace's warehouse.
const MAX_NAMESPACE_DEPTH: usize = 64;

/// Resource types a project-scoped listing covers: every one except the server, which
/// belongs to no project — the same rule the catalog store applies.
///
/// Derived rather than listed, so a new resource type joins the project-wide listing
/// instead of silently going missing from it.
static PROJECT_SCOPED_LEVELS: LazyLock<Vec<ResourceType>> = LazyLock::new(|| {
    <ResourceType as strum::VariantArray>::VARIANTS
        .iter()
        .copied()
        .filter(|resource_type| *resource_type != ResourceType::Server)
        .collect()
});

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
    /// Which of `privileges` the actor (or `for_user`) may grant and revoke on
    /// `resource`, in order.
    ///
    /// A privilege outside this level's vocabulary is a deny, not an error: the name
    /// may come from another authorizer's vocabulary, and answering "not allowed" is
    /// both true and safe.
    pub(crate) async fn grant_authority(
        &self,
        metadata: &RequestMetadata,
        for_user: Option<&UserOrRole>,
        resource: &GrantResource,
        privileges: &[&str],
    ) -> Result<Vec<AuthorizationDecision>, IsAllowedActionError> {
        let resource_type = resource.resource_type();
        let object = grant_object(self, resource);
        let user = for_user.map_or_else(
            || metadata.actor().to_openfga(),
            |u| u.api_user_or_role().to_openfga(),
        );

        // Keep the request dense: unknown privileges get no check and are filled back
        // in as denials afterwards.
        let mut checked_positions = Vec::with_capacity(privileges.len());
        let mut items = Vec::with_capacity(privileges.len());
        for (position, privilege) in privileges.iter().enumerate() {
            if let Some(relation) = authority_relation(resource_type, privilege) {
                checked_positions.push(position);
                items.push(CheckRequestTupleKey {
                    user: user.clone(),
                    relation,
                    object: object.clone(),
                });
            }
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

        let mut decisions = vec![AuthorizationDecision::deny(); privileges.len()];
        for (position, decision) in checked_positions.into_iter().zip(checked) {
            decisions[position] = decision;
        }
        Ok(decisions)
    }
}

/// The `OpenFGA` type each resource level is stored under.
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

/// The grantable level an object type addresses, or `None` for a type no grant can name.
///
/// Inverse of [`fga_type`], derived from it rather than restated so the two cannot drift.
/// The server is excluded with the other non-project levels: it belongs to no project, so
/// a project-scoped scan must not surface it.
fn level_of_fga_type(fga: &FgaType) -> Option<ResourceType> {
    PROJECT_SCOPED_LEVELS
        .iter()
        .copied()
        .find(|resource_type| &fga_type(*resource_type) == fga)
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
            GrantFilter::ByPrincipal {
                principal,
                project_id,
            } => {
                reject_paging(&pagination, "principalUser/principalRole")?;
                self.scan_grants(Some(principal), &project_id).await
            }
            GrantFilter::ByProject(project_id) => {
                reject_paging(&pagination, "principalUser/principalRole")?;
                self.scan_grants(None, &project_id).await
            }
        }
    }
}

/// Refuse a paging request this listing cannot honour.
///
/// Only an *explicit* ask is refused. `PageToken::Empty` is the default a client sends
/// merely to signal it understands pagination, and `page_size` unset means no opinion —
/// neither is a claim that the response will be partial, so neither is worth failing.
fn reject_paging(
    pagination: &PaginationQuery,
    narrowing: &str,
) -> Result<(), GrantListingNotPageable> {
    let asked = match (&pagination.page_token, pagination.page_size) {
        (PageToken::Present(_), _) => "a page token",
        (_, Some(_)) => "`pageSize`",
        _ => return Ok(()),
    };
    Err(GrantListingNotPageable::new(format!(
        "This listing is assembled in one pass under the OpenFGA authorizer and cannot be \
         paged, so {asked} would be ignored and the response would look like a first page. \
         Re-send without it, narrow with {narrowing}, or list one resource's grants from \
         its own endpoint."
    )))
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

    /// Every grant in a project, optionally narrowed to one principal.
    ///
    /// Tuples are not indexed by project, so this reads the assignment tuples of each
    /// project-scoped resource type and resolves every object back to its project
    /// through the model's own hierarchy edges (`warehouse#project`,
    /// `lakekeeper_catalog_tag#project`, `namespace#parent`). Each lookup is cached
    /// per call, so the extra reads scale with the number of distinct containers in
    /// the result, not with the number of grants.
    ///
    /// **Unpaginated.** A single opaque token cannot describe a position in a fan-out
    /// over seven concurrent reads, so `page_size` is ignored and `next_page_token`
    /// is always `None`. This is a management and audit path; the per-resource
    /// listings above page normally.
    async fn scan_grants(
        &self,
        principal: Option<UserOrRoleId>,
        project_id: &ProjectId,
    ) -> Result<ListGrantsResultPage, ListGrantsError> {
        let mut containers = ContainerProjects::default();
        let mut grants = Vec::new();

        // Two read shapes, because `Read` constrains what may be left empty: an object
        // named by type alone is only legal alongside a non-empty user. With a principal
        // that lets each level be read separately; without one the only legal read is
        // the whole store, sorted into levels here instead.
        let tuples = match principal.as_ref() {
            Some(principal) => {
                let user = principal.to_openfga();
                // One read per level, all in flight together: they are independent, so
                // issuing them in sequence would make their latencies additive.
                let per_level = futures::future::try_join_all(PROJECT_SCOPED_LEVELS.iter().map(
                    |&resource_type| {
                        let user = user.clone();
                        async move {
                            self.read_all_result(Some(ReadRequestTupleKey {
                                user,
                                // Every relation on the type: one read per level rather
                                // than one per level and privilege. Non-privilege
                                // relations are dropped below.
                                relation: String::new(),
                                object: format!("{}:", fga_type(resource_type)),
                            }))
                            .await
                            .map(|tuples| (resource_type, tuples))
                            .map_err(|e| scan_read_error(e, Some(resource_type)))
                        }
                    },
                ))
                .await?;
                per_level
                    .into_iter()
                    .flat_map(|(resource_type, tuples)| {
                        tuples.into_iter().map(move |tuple| (resource_type, tuple))
                    })
                    .collect::<Vec<_>>()
            }
            None => self
                .read_all_result(None::<ReadRequestTupleKey>)
                .await
                .map_err(|e| scan_read_error(e, None))?
                .into_iter()
                .filter_map(|tuple| {
                    // Everything the store holds arrives here, so a tuple on a type no
                    // grant can name — including the server, which belongs to no
                    // project — is dropped before any resolution work.
                    let resource_type = {
                        let key = tuple.key.as_ref()?;
                        level_of_fga_type(&split_fga(&key.object)?.0)?
                    };
                    Some((resource_type, tuple))
                })
                .collect::<Vec<_>>(),
        };

        // Resolution is sequential: it shares the container cache, so racing it would
        // repeat the parent walks the cache exists to avoid.
        for (resource_type, tuple) in tuples {
            let created_at = tuple_timestamp(tuple.timestamp.map(|ts| (ts.seconds, ts.nanos)));
            let key = require_key(tuple.key)?;
            let Some(privilege) = privilege_of_relation(resource_type, &key.relation) else {
                continue;
            };
            let Some(resource) = self
                .grant_resource_of(resource_type, &key.object, &mut containers)
                .await?
            else {
                continue;
            };
            if !self
                .resource_is_in_project(&resource, project_id, &mut containers)
                .await?
            {
                continue;
            }
            grants.push(GrantRow {
                principal: parse_grant_principal(&key.user)?,
                resource,
                privilege,
                created_at,
            });
        }

        Ok(ListGrantsResultPage {
            grants,
            next_page_token: None,
        })
    }

    /// The grant resource an object string addresses, or `None` when it cannot be
    /// resolved — a malformed id, or a namespace whose warehouse edge is gone.
    ///
    /// Only the namespace level needs a lookup: its object carries no warehouse,
    /// because namespace ids are unique across warehouses. Tables, views and generic
    /// tables encode `warehouse/resource`, so they resolve from the string alone.
    async fn grant_resource_of(
        &self,
        resource_type: ResourceType,
        object: &str,
        containers: &mut ContainerProjects,
    ) -> Result<Option<GrantResource>, ListGrantsError> {
        let Some((_, id)) = split_fga(object) else {
            return Ok(None);
        };
        let composite = |id: &str| -> Option<(WarehouseId, uuid::Uuid)> {
            let (warehouse, resource) = id.split_once('/')?;
            Some((
                WarehouseId::new(parse_uuid(warehouse)?),
                parse_uuid(resource)?,
            ))
        };
        Ok(match resource_type {
            ResourceType::Server => Some(GrantResource::Server),
            ResourceType::Project => ProjectId::from_str(id).ok().map(GrantResource::Project),
            ResourceType::Warehouse => parse_uuid(id)
                .map(WarehouseId::new)
                .map(GrantResource::Warehouse),
            ResourceType::Namespace => {
                let Some(namespace_id) = parse_uuid(id).map(NamespaceId::new) else {
                    return Ok(None);
                };
                self.warehouse_of_namespace(namespace_id, containers)
                    .await?
                    .map(|warehouse_id| GrantResource::Namespace {
                        warehouse_id,
                        namespace_id,
                    })
            }
            ResourceType::Table => {
                composite(id).map(|(warehouse_id, table_id)| GrantResource::Table {
                    warehouse_id,
                    table_id: TableId::new(table_id),
                })
            }
            ResourceType::View => {
                composite(id).map(|(warehouse_id, view_id)| GrantResource::View {
                    warehouse_id,
                    view_id: ViewId::new(view_id),
                })
            }
            ResourceType::GenericTable => {
                composite(id).map(
                    |(warehouse_id, generic_table_id)| GrantResource::GenericTable {
                        warehouse_id,
                        generic_table_id: GenericTableId::new(generic_table_id),
                    },
                )
            }
            ResourceType::Tag => parse_uuid(id)
                .map(TagDefinitionId::new)
                .map(GrantResource::Tag),
        })
    }

    /// Walk `parent` up from a namespace to its warehouse.
    ///
    /// Depth is bounded: the catalog cannot produce a cycle here, so exceeding the
    /// bound means the stored graph is corrupt and is reported rather than looped on.
    async fn warehouse_of_namespace(
        &self,
        namespace_id: NamespaceId,
        containers: &mut ContainerProjects,
    ) -> Result<Option<WarehouseId>, ListGrantsError> {
        if let Some(cached) = containers.namespaces.get(&namespace_id) {
            return Ok(*cached);
        }
        let mut object = namespace_id.to_openfga();
        let mut warehouse = None;
        let mut terminated = false;
        // Every namespace passed through on the way up gets the same answer, so record
        // them all. Without this, N sibling namespaces at depth D cost N*D reads instead
        // of N+D — the walk would re-derive the shared ancestry once per sibling.
        let mut walked = vec![namespace_id];
        for _ in 0..MAX_NAMESPACE_DEPTH {
            let Some(parent) = self.single_edge_target(&object, "parent").await? else {
                // No parent edge: the container is gone and the tuple is dangling.
                terminated = true;
                break;
            };
            match split_fga(&parent) {
                Some((FgaType::Warehouse, id)) => {
                    warehouse = parse_uuid(id).map(WarehouseId::new);
                    terminated = true;
                    break;
                }
                Some((FgaType::Namespace, id)) => {
                    if let Some(parsed) = parse_uuid(id).map(NamespaceId::new) {
                        // Cached only once the walk terminates: an aborted walk has no
                        // answer to record for anything it passed through.
                        walked.push(parsed);
                    }
                    object = parent;
                }
                _ => {
                    terminated = true;
                    break;
                }
            }
        }
        if !terminated {
            return Err(MalformedGrant::new(
                "authorization backend returned a namespace hierarchy that does not terminate",
                InternalErrorMessage(format!(
                    "namespace {namespace_id} has no warehouse within {MAX_NAMESPACE_DEPTH} parents"
                )),
            )
            .into());
        }
        for id in walked {
            containers.namespaces.insert(id, warehouse);
        }
        Ok(warehouse)
    }

    /// Whether `resource` lives in `project_id`, resolving containers through the
    /// model and caching each answer for the rest of the call.
    async fn resource_is_in_project(
        &self,
        resource: &GrantResource,
        project_id: &ProjectId,
        containers: &mut ContainerProjects,
    ) -> Result<bool, ListGrantsError> {
        let owner = match resource {
            GrantResource::Project(id) => Some(id.clone()),
            GrantResource::Tag(tag_definition_id) => {
                self.project_of_tag(*tag_definition_id, containers).await?
            }
            GrantResource::Server => None,
            _ => match resource.warehouse_id() {
                Some(warehouse_id) => self.project_of_warehouse(warehouse_id, containers).await?,
                None => None,
            },
        };
        // An object with no resolvable project is one whose container has since been
        // deleted: a dangling tuple, not part of any project's grants.
        Ok(owner.as_ref() == Some(project_id))
    }

    async fn project_of_warehouse(
        &self,
        warehouse_id: WarehouseId,
        containers: &mut ContainerProjects,
    ) -> Result<Option<ProjectId>, ListGrantsError> {
        if let Some(cached) = containers.warehouses.get(&warehouse_id) {
            return Ok(cached.clone());
        }
        let project = self
            .single_edge_target(&warehouse_id.to_openfga(), "project")
            .await?
            .and_then(|target| ProjectId::from_str(split_fga(&target)?.1).ok());
        containers.warehouses.insert(warehouse_id, project.clone());
        Ok(project)
    }

    async fn project_of_tag(
        &self,
        tag_definition_id: TagDefinitionId,
        containers: &mut ContainerProjects,
    ) -> Result<Option<ProjectId>, ListGrantsError> {
        if let Some(cached) = containers.tags.get(&tag_definition_id) {
            return Ok(cached.clone());
        }
        let project = self
            .single_edge_target(&tag_definition_id.to_openfga(), "project")
            .await?
            .and_then(|target| ProjectId::from_str(split_fga(&target)?.1).ok());
        containers.tags.insert(tag_definition_id, project.clone());
        Ok(project)
    }

    /// The single object on the far side of a hierarchy edge, e.g. the project a
    /// warehouse belongs to. `None` when the edge is absent.
    async fn single_edge_target(
        &self,
        object: &str,
        relation: &str,
    ) -> Result<Option<String>, ListGrantsError> {
        let tuples = self
            .read_all(Some(ReadRequestTupleKey {
                user: String::new(),
                relation: relation.to_string(),
                object: object.to_string(),
            }))
            .await
            .map_err(unavailable)?;
        // A hierarchy edge is single-valued; extra tuples would be a model change, so
        // take the first and ignore the rest rather than inventing a merge rule.
        match tuples.into_iter().next() {
            Some(tuple) => Ok(Some(require_key(tuple.key)?.user)),
            None => Ok(None),
        }
    }
}

/// Container-to-project answers resolved during one scan. A project-wide listing
/// touches the same warehouse repeatedly; without this it would re-read the same edge
/// once per grant.
#[derive(Default)]
struct ContainerProjects {
    warehouses: HashMap<WarehouseId, Option<ProjectId>>,
    namespaces: HashMap<NamespaceId, Option<WarehouseId>>,
    tags: HashMap<TagDefinitionId, Option<ProjectId>>,
}

fn unavailable(err: OpenFGABackendUnavailable) -> ListGrantsError {
    ListGrantsError::BackendUnavailable(err.into())
}

/// A project-wide scan reads every tuple of a level, which past a few tens of thousands
/// exceeds what one request will assemble. That is a size problem, not an outage, so it
/// must not be reported as one — an operator sent to check a healthy backend looks in
/// the wrong place, and the caller is told nothing they can act on.
fn scan_read_error(
    err: openfga_client::error::Error,
    resource_type: Option<ResourceType>,
) -> ListGrantsError {
    match err {
        openfga_client::error::Error::TooManyPages { .. } => {
            // Deliberately not phrased as a count of grants. The read returns every
            // stored relation on the type — each object's structural and ownership
            // edges included — and privileges are filtered out of it afterwards, so a
            // deployment can exceed this with no grants recorded at all.
            let scope = resource_type.map_or_else(
                || "this deployment".to_string(),
                |resource_type| format!("{} objects in this deployment", resource_type.as_str()),
            );
            ListGrantsError::TooLarge(GrantListingTooLarge::new(format!(
                "Too many stored permissions on {scope} to assemble this listing in one \
                 pass. Narrow it with `principalUser` or `principalRole`, or read one \
                 resource's grants from its own endpoint."
            )))
        }
        other => unavailable(OpenFGABackendUnavailable::from(Box::new(other))),
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

    /// The page cap means "too many grants", not "backend down". Reporting it as a 503
    /// would send an operator to check a healthy backend, so the two must map apart —
    /// and the message must name the narrowing that works, since it is the only thing
    /// the caller can act on.
    #[test]
    fn the_page_cap_is_a_request_error_not_an_outage() {
        let too_many = openfga_client::error::Error::TooManyPages {
            max_pages: 500,
            tuple: None,
        };
        let model =
            lakekeeper::api::ErrorModel::from(scan_read_error(too_many, Some(ResourceType::Table)));
        assert_eq!(model.code, 400);
        assert_eq!(model.r#type, "GrantListingTooLarge");
        assert!(model.message.contains("principalUser"), "{}", model.message);

        // A genuine transport failure keeps reporting unavailability.
        let failed = openfga_client::error::Error::InvalidEndpoint("nowhere".to_string());
        let model =
            lakekeeper::api::ErrorModel::from(scan_read_error(failed, Some(ResourceType::Table)));
        assert_eq!(model.code, 503);
    }

    /// The scan cannot page, so an explicit paging request must fail rather than return
    /// a full result that a client reads as a first page and stops after.
    #[test]
    fn an_explicit_paging_request_on_the_one_pass_listing_is_refused() {
        let by_size = reject_paging(
            &PaginationQuery::new(PageToken::Empty, Some(50)),
            "principalUser/principalRole",
        )
        .expect_err("`pageSize` cannot be honoured by a one-pass listing");
        let model = lakekeeper::api::ErrorModel::from(ListGrantsError::NotPageable(by_size));
        assert_eq!(model.code, 400);
        assert_eq!(model.r#type, "GrantListingNotPageable");
        assert!(model.message.contains("pageSize"), "{}", model.message);
        assert!(
            model.message.contains("principalUser"),
            "the refusal must name the narrowing that works: {}",
            model.message
        );

        let by_token = reject_paging(
            &PaginationQuery::new(PageToken::Present("opaque".to_string()), None),
            "principalUser/principalRole",
        )
        .expect_err("a continuation token cannot be honoured either");
        let model = lakekeeper::api::ErrorModel::from(ListGrantsError::NotPageable(by_token));
        assert_eq!(model.code, 400);
        assert_eq!(model.r#type, "GrantListingNotPageable");
        assert!(model.message.contains("page token"), "{}", model.message);
    }

    /// The default a paging-aware client sends is not a claim that it expects pages.
    /// Refusing it would break every caller that simply did not opt out.
    #[test]
    fn a_listing_that_asks_for_no_paging_is_not_refused() {
        for (label, pagination) in [
            (
                "paging-aware default",
                PaginationQuery::new(PageToken::Empty, None),
            ),
            (
                "no pagination parameters at all",
                PaginationQuery::new(PageToken::NotSpecified, None),
            ),
        ] {
            assert!(
                reject_paging(&pagination, "principalUser/principalRole").is_ok(),
                "{label} must be accepted"
            );
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
                "modify"
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
