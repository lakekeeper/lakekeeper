#![allow(clippy::needless_for_each)]

use std::{collections::HashMap, sync::LazyLock};

use utoipa::{
    OpenApi, PartialSchema, ToSchema,
    openapi::{ComponentsBuilder, KnownFormat, RefOr, Schema, security::SecurityScheme},
};

use crate::{
    api::{
        endpoints::ManagementV1Endpoint,
        management::v1::task_queue::{
            GetTaskQueueConfigResponse, ScheduleTaskRequest, SetTaskQueueConfigRequest,
        },
    },
    service::{
        authz::Authorizer,
        tasks::{BUILT_IN_DEPENDENT_SCHEMAS, QueueApiConfig, QueueScope, UserScheduling},
    },
};

#[derive(Debug, OpenApi)]
#[openapi(
    info(
        title = "Lakekeeper Management API",
        description = "Lakekeeper is a rust-native Apache Iceberg REST Catalog implementation. The Management API provides endpoints to manage the server, projects, warehouses, users, and roles. If Authorization is enabled, permissions can also be managed. An interactive Swagger-UI for the specific Lakekeeper Version and configuration running is available at `/swagger-ui/#/` of Lakekeeper (by default [http://localhost:8181/swagger-ui/#/](http://localhost:8181/swagger-ui/#/)).",
    ),
    servers(
        (
            url = "{scheme}://{host}{basePath}",
            description = "Lakekeeper Management API",
            variables(
                ("scheme" = (default = "https", description = "The scheme of the URI, either http or https")),
                ("host" = (default = "localhost", description = "The host (and optional port) for the specified server")),
                ("basePath" = (default = "", description = "Optional path prefix (starting with '/') to be prepended to all routes"))
            )
        )
    ),
    tags(
        (name = "server", description = "Manage Server"),
        (name = "project", description = "Manage Projects"),
        (name = "warehouse", description = "Manage Warehouses"),
        (name = "tasks", description = "View & Manage Tasks"),
        (name = "user", description = "Manage Users"),
        (name = "role", description = "Manage Roles"),
        (name = "tag", description = "**[Preview]** Manage governance tags. This API is in preview and may change in a backward-incompatible way in a future release."),
        (name = "grant", description = "**[Preview]** Manage grants: which principal holds which privilege on which resource. This API is in preview and may change in a backward-incompatible way in a future release.")
    ),
    security(
        ("bearerAuth" = [])
    ),
    paths(
        super::activate_warehouse,
        super::batch_check_actions,
        super::bootstrap,
        super::control_tasks,
        super::control_project_tasks,
        super::create_project,
        super::create_role,
        super::create_tag_definition,
        super::create_user,
        super::create_warehouse,
        super::deactivate_warehouse,
        super::delete_generic_table_tag,
        super::delete_namespace_tag,
        super::delete_project_by_id_deprecated,
        super::delete_project,
        super::delete_role,
        super::delete_table_column_tag,
        super::delete_table_tag,
        super::delete_tag_definition,
        super::delete_user,
        super::delete_view_tag,
        super::delete_warehouse,
        super::delete_warehouse_tag,
        super::get_endpoint_statistics,
        super::get_namespace_actions,
        super::get_namespace_protection,
        super::get_project_actions,
        super::get_project_by_id_deprecated,
        super::get_project,
        super::get_project_task_details,
        super::get_project_task_queue_config,
        super::get_role_actions,
        super::get_role_metadata,
        super::get_role,
        super::get_server_actions,
        super::get_server_info,
        super::get_table_actions,
        super::get_table_protection,
        super::get_tag_definition,
        super::get_task_details,
        super::get_task_queue_config,
        super::get_user_actions,
        super::get_user,
        super::get_view_actions,
        super::get_generic_table_actions,
        super::get_tag_actions,
        super::get_generic_table_protection,
        super::get_view_protection,
        super::get_warehouse_actions,
        super::get_warehouse_statistics,
        super::get_warehouse,
        super::list_deleted_tabulars,
        super::list_projects,
        super::list_project_tasks,
        super::list_roles,
        super::list_role_members,
        super::add_role_members,
        super::remove_role_member,
        super::list_role_member_of,
        super::list_user_roles,
        super::list_role_transitive_members,
        super::list_user_transitive_roles,
        super::list_role_transitive_member_of,
        super::list_generic_table_tags,
        super::list_namespace_tags,
        super::list_table_column_tags,
        super::list_column_tags,
        super::list_table_tags,
        super::list_tag_attachments,
        super::list_tag_definitions,
        super::list_tasks,
        super::list_user,
        super::list_view_tags,
        super::list_warehouse_tags,
        super::list_grants,
        super::get_grantable_privileges,
        super::get_server_grantable_privileges,
        super::get_project_grantable_privileges,
        super::get_warehouse_grantable_privileges,
        super::get_namespace_grantable_privileges,
        super::get_table_grantable_privileges,
        super::get_view_grantable_privileges,
        super::get_generic_table_grantable_privileges,
        super::get_tag_grantable_privileges,
        super::list_warehouse_grants,
        super::apply_warehouse_grants,
        super::list_server_grants,
        super::apply_server_grants,
        super::list_project_grants,
        super::apply_project_grants,
        super::list_namespace_grants,
        super::apply_namespace_grants,
        super::list_namespace_subtree_grants,
        super::revoke_namespace_subtree_grants,
        super::list_warehouse_subtree_grants,
        super::revoke_warehouse_subtree_grants,
        super::list_tag_grants,
        super::apply_tag_grants,
        super::list_table_grants,
        super::apply_table_grants,
        super::list_view_grants,
        super::apply_view_grants,
        super::list_generic_table_grants,
        super::apply_generic_table_grants,
        super::list_warehouses,
        super::move_namespace,
        super::rename_project_by_id_deprecated,
        super::rename_project,
        super::rename_warehouse,
        super::search_role,
        super::search_tabular,
        super::search_user,
        super::set_generic_table_tag,
        super::set_namespace_protection,
        super::set_namespace_tag,
        super::set_project_task_queue_config,
        super::set_generic_table_protection,
        super::set_table_column_tag,
        super::set_table_protection,
        super::set_table_tag,
        super::schedule_task,
        super::set_task_queue_config,
        super::set_view_protection,
        super::set_view_tag,
        super::set_warehouse_protection,
        super::set_warehouse_managed_by,
        super::set_warehouse_tag,
        super::undrop_tabulars,
        super::update_role_source_system,
        super::update_role,
        super::update_storage_credential,
        super::update_storage_profile,
        super::update_tag_definition,
        super::update_user,
        super::update_warehouse_delete_profile,
        super::update_warehouse_format_version_policy,
        super::validate_storage_credential,
        super::validate_storage_profile,
        super::validate_storage_access,
        super::validate_warehouse,
        super::whoami,
    ),
    components(schemas(
        // `RoleMemberType` is referenced only through `params(...)` (the `?type=`
        // query filter and the `member_type` path segment), which utoipa does NOT
        // auto-collect into `components/schemas`. Register it explicitly so the
        // `$ref`s those params emit resolve instead of dangling.
        crate::api::management::v1::role_membership::RoleMemberType,
    )),
    modifiers(&SecurityAddon)
)]
pub(super) struct ManagementApiDoc;

struct SecurityAddon;

impl utoipa::Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        let components = openapi
            .components
            .get_or_insert_with(|| utoipa::openapi::ComponentsBuilder::new().build());
        components.add_security_scheme(
            "bearerAuth",
            SecurityScheme::Http(
                utoipa::openapi::security::HttpBuilder::new()
                    .scheme(utoipa::openapi::security::HttpAuthScheme::Bearer)
                    .bearer_format("JWT")
                    .build(),
            ),
        );
    }
}

/// Get the `OpenAPI` documentation for the management API.
///
/// # Errors
/// Never fails, but returns warnings if components cannot be patched.
#[allow(clippy::too_many_lines)]
#[must_use]
pub fn api_doc<A: Authorizer>(
    queue_api_configs: &[&QueueApiConfig],
    project_queue_api_configs: &[&QueueApiConfig],
) -> utoipa::openapi::OpenApi {
    let mut doc = ManagementApiDoc::openapi();
    doc.merge(A::api_doc());

    add_dependent_schemas(&mut doc, &BUILT_IN_DEPENDENT_SCHEMAS);

    fix_task_queue_config_paths(
        &mut doc,
        queue_api_configs,
        ManagementV1Endpoint::SetTaskQueueConfig.path(),
    );
    fix_task_queue_config_paths(
        &mut doc,
        project_queue_api_configs,
        ManagementV1Endpoint::SetProjectTaskQueueConfig.path(),
    );

    fix_task_queue_schedule_paths(
        &mut doc,
        queue_api_configs,
        ManagementV1Endpoint::ScheduleTask.path(),
    );

    // Order matters. `UserOrRole` must stop being a `oneOf` first, or the
    // `*Assignment` unions that embed it look like nested unions to the
    // expansion pass and get multiplied out pointlessly. Hoisting runs last
    // because it uses the titles the naming pass assigns as component names.
    flatten_user_or_role(&mut doc);
    expand_unions_composing_unions(&mut doc);
    name_anonymous_one_of_variants(&mut doc);
    hoist_and_discriminate_one_of_variants(&mut doc);

    doc
}

/// Flatten unions whose members compose *another* union into a single level.
///
/// `StorageCredential` is the one union of this shape: its members are
/// `allOf: [{$ref: S3Credential}, {type: <const>}]`, and `S3Credential` (like
/// `AzCredential` and `GcsCredential`) is itself a `oneOf`. Two levels of union
/// cannot survive into a generated client — a generator asked to compose a
/// `oneOf` into an object flattens it into one struct carrying every branch's
/// fields, and an `OpenAPI` `discriminator` can name only one property, so the
/// nesting cannot be described away either.
///
/// The fix is to multiply the levels out: every (outer variant × inner variant)
/// pair becomes one flat leaf carrying the inner payload plus *both* tags. Nine
/// leaves here — four S3, three Azure, two GCS.
///
/// This only produces a usable union because one property still identifies a
/// leaf on its own: `credential-type` values happen to be unique across all
/// three providers. `type` alone would not work (`s3` maps to four leaves). If
/// a future credential reuses a `credential-type` value under a different
/// `type`, no single discriminator exists and this pass leaves the union
/// untouched rather than emitting something misleading — the union then falls
/// back to needing downstream preprocessing, as it did before this pass.
///
/// Runs before the other schema passes so the expanded union looks like an
/// ordinary flat one to them.
// One linear decision procedure per union; splitting it would scatter the
// refusal reasons away from the checks that produce them.
#[allow(clippy::too_many_lines)]
fn expand_unions_composing_unions(doc: &mut utoipa::openapi::OpenApi) {
    use utoipa::openapi::{Ref, RefOr, Schema, schema::Discriminator};

    let Some(components) = doc.components.as_mut() else {
        return;
    };

    // Resolve the inner unions up front: the borrow checker will not allow
    // reading `schemas` while the union being rewritten is borrowed mutably.
    let inner_unions = components
        .schemas
        .iter()
        .filter_map(|(name, schema)| match schema {
            RefOr::T(Schema::OneOf(one_of)) => Some((name.clone(), one_of.clone())),
            _ => None,
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    // Names the leaves must avoid; see [`reserve_name`]. Claims accumulate
    // across unions, so two unions cannot derive the same leaf name.
    let mut taken = components
        .schemas
        .keys()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();

    let mut expansions = Vec::new();
    for (union_name, union) in &components.schemas {
        let RefOr::T(Schema::OneOf(one_of)) = union else {
            continue;
        };

        let leaves = match multiply_out(&one_of.items, &inner_unions) {
            Expansion::Leaves(leaves) => leaves,
            Expansion::NotNested => continue,
            Expansion::Unsupported => {
                tracing::warn!(
                    union = %union_name,
                    "OpenAPI: `{union_name}` composes another union but its members are not a \
                     shape this pass can flatten, so it is published as a nested union. Code \
                     generators cannot consume that: clients will reject valid payloads for this \
                     type until it is expanded here or downstream."
                );
                continue;
            }
        };

        // A leaf now carries two tags, so pick the property that identifies a
        // leaf on its own. Bail out entirely if none does.
        let per_leaf = leaves.iter().map(single_value_enums).collect::<Vec<_>>();
        let Some(property_name) = per_leaf
            .first()
            .into_iter()
            .flat_map(|first| first.keys().cloned())
            .find(|candidate| {
                let values = per_leaf
                    .iter()
                    .filter_map(|enums| enums.get(candidate))
                    .collect::<std::collections::BTreeSet<_>>();
                values.len() == leaves.len()
            })
        else {
            tracing::warn!(
                union = %union_name,
                leaves = leaves.len(),
                "OpenAPI: `{union_name}` was flattened into {} leaves but no single property \
                 identifies one on its own, so no discriminator can be published and the union \
                 is left nested. This happens when a tag value is reused across the outer \
                 variants — see the note on `StorageCredential`.",
                leaves.len()
            );
            continue;
        };

        let mut items = Vec::with_capacity(leaves.len());
        let mut mapping = std::collections::BTreeMap::new();
        let mut hoisted = Vec::with_capacity(leaves.len());
        let mut unnameable = None;
        for (leaf, enums) in leaves.into_iter().zip(per_leaf) {
            let value = enums[&property_name].clone();
            let preferred = format!("{union_name}{}", to_pascal_case(&value));
            let Some(title) = reserve_name(&mut taken, &preferred) else {
                unnameable = Some(preferred);
                break;
            };
            if title != preferred {
                tracing::warn!(
                    union = %union_name,
                    "OpenAPI: `{preferred}`, the name derived for a leaf of `{union_name}`, is \
                     already held by another component, so the leaf is published as `{title}` \
                     instead. Rename the composed type to publish a better name."
                );
            }
            let reference = format!("#/components/schemas/{title}");
            items.push(RefOr::Ref(Ref::new(reference.clone())));
            mapping.insert(value, reference);
            hoisted.push((title, leaf));
        }
        if let Some(preferred) = unnameable {
            // Release what this union claimed: it is left untouched, so nothing
            // will be inserted under those names and a later union may use them.
            for (title, _) in &hoisted {
                taken.remove(title);
            }
            tracing::warn!(
                union = %union_name,
                "OpenAPI: every candidate name for a leaf of `{union_name}` derived from \
                 `{preferred}` is already taken, so the union is left nested rather than \
                 rewritten to reference a schema that was never inserted."
            );
            continue;
        }
        expansions.push((
            union_name.clone(),
            items,
            Discriminator {
                property_name,
                mapping,
                extensions: None,
            },
            hoisted,
        ));
    }

    for (union_name, items, discriminator, hoisted) in expansions {
        for (title, leaf) in hoisted {
            insert_hoisted(&mut components.schemas, title, leaf);
        }
        if let Some(RefOr::T(Schema::OneOf(one_of))) = components.schemas.get_mut(&union_name) {
            one_of.items = items;
            one_of.discriminator = Some(discriminator);
        }
    }
}

/// Claim an unused component name for a union member that is about to be
/// hoisted.
///
/// `taken` holds every name already spoken for: the components the document
/// started with, plus every name claimed earlier in the same pass. Names must be
/// tracked across the whole pass and not merely looked up in `components`,
/// because a pass assigns many names before inserting any of them — two unions
/// deriving the same name would otherwise both believe it was free.
///
/// `preferred` wins when it is free. Otherwise a `Variant` suffix is appended,
/// then numbered, so a clash renames the *new* schema and leaves the established
/// one where downstream `$ref`s already point.
///
/// Returns `None` when every candidate is taken. Callers must treat that as a
/// refusal for the entire union: a union rewritten to `$ref` a name that was
/// never inserted is worse than one left alone.
fn reserve_name(taken: &mut std::collections::BTreeSet<String>, preferred: &str) -> Option<String> {
    if taken.insert(preferred.to_owned()) {
        return Some(preferred.to_owned());
    }
    std::iter::once(format!("{preferred}Variant"))
        .chain((2..=16).map(|n| format!("{preferred}Variant{n}")))
        .find(|candidate| taken.insert(candidate.clone()))
}

/// Insert a hoisted union member under a name [`reserve_name`] has claimed.
///
/// Both hoisting passes allocate through [`reserve_name`], so the name is free
/// by construction and this never replaces anything. The check stays as an
/// invariant guard because the failure it prevents is silent rather than loud:
/// `BTreeMap::insert` would drop the existing schema's properties from the
/// document while the union's `$ref` — spelled the same either way — kept
/// resolving, now to an unrelated schema. Nothing dangles, so
/// `spec_integrity_tests::assert_refs_resolve` cannot catch it; only
/// `assert_no_self_references` catches the sub-case where the hoisted member
/// composed the schema it displaced.
fn insert_hoisted(
    schemas: &mut std::collections::BTreeMap<
        String,
        utoipa::openapi::RefOr<utoipa::openapi::Schema>,
    >,
    name: String,
    schema: utoipa::openapi::Schema,
) {
    if schemas.contains_key(&name) {
        debug_assert!(
            false,
            "hoisted name `{name}` was not reserved through `reserve_name`"
        );
        tracing::error!(
            schema = %name,
            "OpenAPI: refusing to hoist a union variant as `{name}`: a component of that name \
             already exists and overwriting it would drop it from the document. The union now \
             holds a `$ref` to `{name}` that resolves to an unrelated schema."
        );
        return;
    }
    schemas.insert(name, utoipa::openapi::RefOr::T(schema));
}

/// Outcome of multiplying a union's members out — see [`multiply_out`].
enum Expansion {
    /// No member composes another union; there is nothing to expand.
    NotNested,
    /// A member composes a union but its shape is not one this pass can
    /// rewrite safely, so the union is left as it is.
    Unsupported,
    /// One leaf per (outer variant × inner variant) pair.
    Leaves(Vec<utoipa::openapi::Schema>),
}

/// Multiply a union's members by the members of any union they compose.
fn multiply_out(
    members: &[utoipa::openapi::RefOr<utoipa::openapi::Schema>],
    inner_unions: &std::collections::BTreeMap<String, utoipa::openapi::schema::OneOf>,
) -> Expansion {
    use utoipa::openapi::{AllOf, RefOr, Schema};

    let composes_any = members.iter().any(|member| match member {
        RefOr::T(Schema::AllOf(member)) => member.items.iter().any(|branch| match branch {
            RefOr::Ref(reference) => reference
                .ref_location
                .rsplit('/')
                .next()
                .is_some_and(|name| inner_unions.contains_key(name)),
            RefOr::T(_) => false,
        }),
        _ => false,
    });
    if !composes_any {
        return Expansion::NotNested;
    }

    let mut leaves = Vec::new();
    for member in members {
        let RefOr::T(Schema::AllOf(member)) = member else {
            return Expansion::Unsupported;
        };
        // Split the member into the union it composes and everything else it
        // adds (the outer tag).
        let mut inner = None;
        let mut extras = Vec::new();
        for branch in &member.items {
            let composed = match branch {
                RefOr::Ref(reference) => reference
                    .ref_location
                    .rsplit('/')
                    .next()
                    .and_then(|name| inner_unions.get(name)),
                RefOr::T(_) => None,
            };
            match composed {
                Some(one_of) if inner.is_none() => inner = Some(one_of),
                // A member composing two unions would have to be multiplied out
                // in both directions. Pushing the second into `extras` instead
                // would leave a `$ref` to a `oneOf` in every leaf and publish a
                // union that is still nested — the shape this pass exists to
                // remove. Refuse, so the caller warns and leaves it untouched.
                Some(_) => return Expansion::Unsupported,
                _ => extras.push(branch.clone()),
            }
        }
        let Some(inner) = inner else {
            leaves.push(Schema::AllOf(member.clone()));
            continue;
        };
        for inner_member in &inner.items {
            let mut items = match inner_member {
                RefOr::T(Schema::AllOf(inner_member)) => inner_member.items.clone(),
                other => vec![other.clone()],
            };
            items.extend(extras.iter().cloned());
            let mut leaf = AllOf::new();
            leaf.items = items;
            leaves.push(Schema::AllOf(leaf));
        }
    }
    if leaves.is_empty() {
        return Expansion::Unsupported;
    }
    Expansion::Leaves(leaves)
}

/// Every property the schema pins to a single-value enum, searching `allOf`
/// branches. Unlike [`sole_discriminator`] this returns all candidates, because
/// an expanded leaf carries one tag per union level.
fn single_value_enums(
    schema: &utoipa::openapi::Schema,
) -> std::collections::BTreeMap<String, String> {
    use utoipa::openapi::{RefOr, Schema};

    let mut out = std::collections::BTreeMap::new();
    let mut stack = vec![schema];
    while let Some(schema) = stack.pop() {
        match schema {
            Schema::Object(object) => {
                for (name, property) in &object.properties {
                    if let RefOr::T(Schema::Object(property)) = property
                        && let Some([value]) = property.enum_values.as_deref()
                        && let Some(value) = value.as_str()
                    {
                        out.insert(name.clone(), value.to_owned());
                    }
                }
            }
            Schema::AllOf(all_of) => stack.extend(all_of.items.iter().filter_map(|b| match b {
                RefOr::T(schema) => Some(schema),
                RefOr::Ref(_) => None,
            })),
            _ => {}
        }
    }
    out
}

/// Name of the schema flattened by [`flatten_user_or_role`].
const USER_OR_ROLE: &str = "UserOrRole";

/// Render `UserOrRole` as one object with two optional properties rather than a
/// `oneOf`.
///
/// On the wire a `UserOrRole` is `{"user": "..."}` xor `{"role": "..."}`, which
/// utoipa describes as a `oneOf` of two presence-discriminated objects. That is
/// accurate but unusable downstream: the nine `*Assignment` unions embed it as
/// `allOf: [{$ref: UserOrRole}, {type: <const>}]`, and a generator asked to
/// compose a `oneOf` into a parent object flattens it into a single struct
/// carrying *every* branch's fields as required — so `{"type": "ownership",
/// "user": "..."}` is rejected for a missing `role` it must not have.
///
/// Describing the same wire shape as one object with both properties optional
/// removes the nesting, and every embedding union then composes cleanly.
///
/// The trade-off is that the schema no longer expresses the exclusivity: a
/// document with both properties, or neither, becomes schema-valid. The server
/// still rejects those, so this moves a constraint out of the schema and into
/// runtime validation rather than dropping it.
fn flatten_user_or_role(doc: &mut utoipa::openapi::OpenApi) {
    use utoipa::openapi::{Object, RefOr, Schema};

    let Some(components) = doc.components.as_mut() else {
        return;
    };
    match components.schemas.get(USER_OR_ROLE) {
        Some(RefOr::T(Schema::OneOf(_))) => {}
        Some(_) => {
            tracing::warn!(
                "OpenAPI: `{USER_OR_ROLE}` is no longer a `oneOf`, so it was left as-is. If its \
                 shape changed deliberately, drop this pass and the note on the Rust type; if \
                 not, the `*Assignment` unions that embed it may no longer generate usable \
                 clients."
            );
            return;
        }
        None => {
            tracing::warn!(
                "OpenAPI: `{USER_OR_ROLE}` is not in components — renamed or removed? The \
                 `*Assignment` unions embed it and will be published as nested unions, which \
                 code generators cannot consume."
            );
            return;
        }
    }
    let Some(RefOr::T(Schema::OneOf(one_of))) = components.schemas.get(USER_OR_ROLE) else {
        return;
    };

    let mut flattened = Object::new();
    flattened.description.clone_from(&one_of.description);
    for member in &one_of.items {
        if let RefOr::T(Schema::Object(member)) = member {
            for (name, property) in &member.properties {
                flattened.properties.insert(name.clone(), property.clone());
            }
        } else {
            tracing::warn!(
                "OpenAPI: a `{USER_OR_ROLE}` variant is not a plain object; its properties were \
                 dropped while flattening. The published schema no longer describes every way a \
                 user or role can be identified."
            );
        }
    }

    components
        .schemas
        .insert(USER_OR_ROLE.to_owned(), RefOr::T(Schema::Object(flattened)));
}

/// Lift every `oneOf` member into a named component schema and declare the
/// discriminator that selects between them.
///
/// Naming the variants (see [`name_anonymous_one_of_variants`]) fixes what the
/// generated types are *called*; it does not tell a generator how to *choose*
/// between them. Without a discriminator the only strategy available is to try
/// each branch and see which parses, and our branches are frequently
/// indistinguishable that way — every `WarehouseAssignment` variant is
/// `{user?, role?, type}`, differing only in the value of `type`, which is a
/// single-value enum that most generators render as a plain string rather than
/// a constant. Decoding then fails with "matches more than one schema".
///
/// An `OpenAPI` `discriminator` states the rule outright, but its `mapping` can
/// only point at named schemas — hence the hoist. Applied to every union whose
/// members each pin the *same* single property to a *distinct* value.
// As with `expand_unions_composing_unions`: the refusal reasons belong next to
// the checks that set them.
#[allow(clippy::too_many_lines)]
fn hoist_and_discriminate_one_of_variants(doc: &mut utoipa::openapi::OpenApi) {
    use utoipa::openapi::{Ref, RefOr, Schema, schema::Discriminator};

    let Some(components) = doc.components.as_mut() else {
        return;
    };

    let skip = unions_left_intact(&components.schemas);
    let mut hoisted: Vec<(String, Schema)> = Vec::new();
    let mut rewrites: Vec<(String, Vec<RefOr<Schema>>, Discriminator)> = Vec::new();
    // Hoisting publishes each member as a component under its title, so a title
    // that is already spoken for would displace the schema holding it. Titles
    // derived by `name_anonymous_one_of_variants` are unique by construction, but
    // an explicit `#[schema(title = "...")]` in the Rust source is not checked
    // anywhere else. Claims accumulate across unions because nothing is inserted
    // until every union has been examined.
    let mut claimed = components
        .schemas
        .keys()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();

    for (union_name, union) in &components.schemas {
        if skip.contains(union_name) {
            continue;
        }
        let RefOr::T(Schema::OneOf(one_of)) = union else {
            continue;
        };
        if one_of.discriminator.is_some() || one_of.items.is_empty() {
            continue;
        }

        // Every member must name the same discriminator property, hold a
        // distinct value for it, and already carry a title to be hoisted under.
        let mut property_name: Option<String> = None;
        let mut members = Vec::with_capacity(one_of.items.len());
        let mut refused: Option<&'static str> = None;
        for member in &one_of.items {
            let RefOr::T(member) = member else {
                refused = Some("a member is a bare $ref, so it carries no tag to map");
                break;
            };
            let (Some((property, value)), title) =
                (sole_discriminator(member), schema_title(member).cloned())
            else {
                // No single-value enum: presence-discriminated, which this pass
                // cannot describe. Expected for a handful of unions, so quiet.
                refused = Some("");
                break;
            };
            let Some(title) = title else {
                refused =
                    Some("a member has a tag but no title, so there is nothing to hoist it under");
                break;
            };
            if *property_name.get_or_insert_with(|| property.clone()) != property {
                refused = Some("members disagree on which property is the tag");
                break;
            }
            members.push((title, value, member.clone()));
        }

        let distinct_values = members
            .iter()
            .map(|(_, value, _)| value)
            .collect::<std::collections::BTreeSet<_>>()
            .len();
        let distinct_titles = members
            .iter()
            .map(|(title, _, _)| title)
            .collect::<std::collections::BTreeSet<_>>()
            .len();
        if refused.is_none() && distinct_values != members.len() {
            refused = Some("two members share the same tag value, so a mapping would be ambiguous");
        } else if refused.is_none() && distinct_titles != members.len() {
            refused = Some("two members share the same title, so they would hoist onto each other");
        }

        if let Some(reason) = refused {
            if !reason.is_empty() {
                tracing::warn!(
                    union = %union_name,
                    "OpenAPI: no discriminator published for `{union_name}`: {reason}. Code \
                     generators must fall back to trying each variant in turn, which fails \
                     whenever two variants have the same shape."
                );
            }
            continue;
        }
        let (Some(property_name), false) = (property_name, members.is_empty()) else {
            continue;
        };

        // Preflight every title before rewriting anything. Hoisting a member
        // under a name that is taken would drop the schema holding it while the
        // union's `$ref` kept resolving — to the wrong schema. Refuse the whole
        // union instead of renaming, because a title that clashes here was
        // written by hand and silently publishing it under another name would
        // contradict the author.
        if let Some(clash) = members
            .iter()
            .map(|(title, _, _)| title)
            .find(|title| claimed.contains(title.as_str()))
        {
            tracing::warn!(
                union = %union_name,
                "OpenAPI: no discriminator published for `{union_name}`: its variant title \
                 `{clash}` is already held by another component, so hoisting the variant would \
                 overwrite that schema. Rename the variant's `#[schema(title = \"...\")]`."
            );
            continue;
        }
        for (title, _, _) in &members {
            claimed.insert(title.clone());
        }

        let mut items = Vec::with_capacity(members.len());
        let mut mapping = std::collections::BTreeMap::new();
        for (title, value, mut member) in members {
            set_schema_title_none(&mut member);
            let reference = format!("#/components/schemas/{title}");
            items.push(RefOr::Ref(Ref::new(reference.clone())));
            mapping.insert(value, reference);
            hoisted.push((title, member));
        }

        rewrites.push((
            union_name.clone(),
            items,
            Discriminator {
                property_name,
                mapping,
                extensions: None,
            },
        ));
    }

    for (name, schema) in hoisted {
        insert_hoisted(&mut components.schemas, name, schema);
    }
    for (name, items, discriminator) in rewrites {
        if let Some(RefOr::T(Schema::OneOf(one_of))) = components.schemas.get_mut(&name) {
            one_of.items = items;
            one_of.discriminator = Some(discriminator);
        }
    }
}

/// Unions that [`hoist_and_discriminate_one_of_variants`] must not touch.
///
/// Two related shapes, both rooted in `StorageCredential` — see the
/// two-level-union note on `StorageCredential` in `service::storage`, and
/// [`expand_unions_composing_unions`] for how that shape is flattened:
///
/// - a union whose members compose *another* union (`StorageCredential`'s
///   members are `allOf: [{$ref: S3Credential}, {type: <const>}]`, and
///   `S3Credential` is itself a `oneOf`). One discriminator cannot select
///   across two levels.
/// - the inner unions themselves (`S3Credential`, `AzCredential`,
///   `GcsCredential`). Rewriting their members to `$ref`s independently leaves
///   the parent composing something it can no longer flatten, and the generated
///   client does not compile.
fn unions_left_intact(
    schemas: &std::collections::BTreeMap<String, utoipa::openapi::RefOr<utoipa::openapi::Schema>>,
) -> std::collections::BTreeSet<String> {
    use utoipa::openapi::{RefOr, Schema};

    let is_union = |name: &str| {
        matches!(
            schemas.get(name),
            Some(RefOr::T(Schema::OneOf(_) | Schema::AnyOf(_)))
        )
    };
    let composed_unions = |member: &Schema| -> Vec<String> {
        let Schema::AllOf(all_of) = member else {
            return Vec::new();
        };
        all_of
            .items
            .iter()
            .filter_map(|branch| match branch {
                RefOr::Ref(reference) => reference.ref_location.rsplit('/').next(),
                RefOr::T(_) => None,
            })
            .filter(|name| is_union(name))
            .map(ToOwned::to_owned)
            .collect()
    };

    let mut skip: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for (union_name, union) in schemas {
        let RefOr::T(Schema::OneOf(one_of)) = union else {
            continue;
        };
        for member in &one_of.items {
            if let RefOr::T(member) = member {
                let nested = composed_unions(member);
                if !nested.is_empty() {
                    skip.insert(union_name.clone());
                    skip.extend(nested);
                }
            }
        }
    }
    if !skip.is_empty() {
        tracing::warn!(
            unions = ?skip,
            "OpenAPI: these unions still nest another union and are published unflattened, so \
             generated clients cannot decode them without downstream preprocessing. \
             `expand_unions_composing_unions` should have flattened them — see the note on \
             `StorageCredential`."
        );
    }
    skip
}

fn set_schema_title_none(schema: &mut utoipa::openapi::Schema) {
    use utoipa::openapi::Schema;
    match schema {
        Schema::Object(object) => object.title = None,
        Schema::AllOf(all_of) => all_of.title = None,
        Schema::OneOf(one_of) => one_of.title = None,
        Schema::Array(array) => array.title = None,
        _ => {}
    }
}

/// Give every anonymous `oneOf` variant a `title`.
///
/// utoipa renders a `#[serde(tag = "...")]` enum as a `oneOf` whose members are
/// inline schemas. Inline schemas have no name, so code generators invent one —
/// and because many of our variants are structurally identical (`{action:
/// {enum: ["delete"]}}` recurs across every `Lakekeeper*Action` enum), generators
/// that deduplicate identical schemas emit a *single* model and name it after
/// whichever union they happened to visit first. The result is both wrong and
/// unstable: `LakekeeperWarehouseActionKind` ends up referencing types called
/// `LakekeeperGenericTableActionOneOf10`, and the names shift whenever schema
/// order changes.
///
/// A `title` makes each variant distinct and names it locally. The name is
/// derived from the variant's own discriminator — the property pinned to a
/// single-value enum, which is exactly what `#[serde(tag)]` emits — so it stays
/// in sync with the Rust source automatically. Variants that already carry an
/// explicit `#[schema(title = "...")]` are left alone.
///
/// Variants with no single-value-enum property (the presence-discriminated ones
/// such as `NamespaceIdentOrUuid`) cannot be named this way and keep whatever
/// the generator picks; they need an explicit title in the Rust source.
fn name_anonymous_one_of_variants(doc: &mut utoipa::openapi::OpenApi) {
    use utoipa::openapi::{RefOr, Schema};

    let Some(components) = doc.components.as_mut() else {
        return;
    };

    // Names a derived title must avoid, because
    // `hoist_and_discriminate_one_of_variants` later inserts each variant as a
    // component under its title. Seeded from the existing components — the loop
    // below borrows `schemas` mutably, so it cannot be consulted again — and
    // extended by [`reserve_name`] as titles are handed out, so two variants
    // never leave this pass sharing a name.
    let mut taken = components
        .schemas
        .keys()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();

    for (parent_name, parent) in &mut components.schemas {
        let RefOr::T(Schema::OneOf(one_of)) = parent else {
            continue;
        };
        for member in &mut one_of.items {
            let RefOr::T(member) = member else {
                continue;
            };
            if schema_title(member).is_some() {
                continue;
            }
            let Some(value) = sole_discriminator(member)
                .map(|(_, value)| value)
                .or_else(|| sole_required_field(member))
            else {
                tracing::warn!(
                    union = %parent_name,
                    "OpenAPI: a variant of `{parent_name}` has neither a single-value tag nor a \
                     single required property, so it could not be named. Code generators will \
                     invent a name for it, and because identical inline schemas are deduplicated \
                     that name may be taken from an unrelated union. Give the variant an \
                     explicit `#[schema(title = \"...\")]`."
                );
                continue;
            };
            let preferred = format!("{parent_name}{}", to_pascal_case(&value));
            let Some(title) = reserve_name(&mut taken, &preferred) else {
                tracing::warn!(
                    union = %parent_name,
                    "OpenAPI: every candidate name derived from `{preferred}` for a variant of \
                     `{parent_name}` is already taken, so the variant is left unnamed. Code \
                     generators will invent a name for it. Give it an explicit \
                     `#[schema(title = \"...\")]`."
                );
                continue;
            };
            if title != preferred {
                // A component already holds the derived name, and the variant
                // usually *composes* that component (`allOf: [{$ref: X}, {tag}]`),
                // so hoisting under it would replace `X` with a schema that
                // references itself. `StorageLayout::Full` is exactly this case:
                // the tag `full-hierarchy` derives the name of the struct the
                // variant wraps.
                tracing::warn!(
                    union = %parent_name,
                    "OpenAPI: `{preferred}`, the name derived for a variant of `{parent_name}`, \
                     is already held by another component, so the variant is named `{title}` \
                     instead. Give the variant an explicit `#[schema(title = \"...\")]`, or \
                     rename the composed type, to publish a better name."
                );
            }
            set_schema_title(member, title);
        }
    }
}

fn schema_title(schema: &utoipa::openapi::Schema) -> Option<&String> {
    use utoipa::openapi::Schema;
    match schema {
        Schema::Object(object) => object.title.as_ref(),
        Schema::AllOf(all_of) => all_of.title.as_ref(),
        Schema::OneOf(one_of) => one_of.title.as_ref(),
        Schema::Array(array) => array.title.as_ref(),
        _ => None,
    }
}

fn set_schema_title(schema: &mut utoipa::openapi::Schema, title: String) {
    use utoipa::openapi::Schema;
    match schema {
        Schema::Object(object) => object.title = Some(title),
        Schema::AllOf(all_of) => all_of.title = Some(title),
        Schema::OneOf(one_of) => one_of.title = Some(title),
        Schema::Array(array) => array.title = Some(title),
        _ => {}
    }
}

/// The variant's discriminator as `(property, value)`, if it has exactly one
/// property pinned to a single-value enum. `allOf` branches are searched too,
/// because utoipa renders a tagged newtype variant as
/// `allOf: [{$ref}, {tag const}]`.
fn sole_discriminator(schema: &utoipa::openapi::Schema) -> Option<(String, String)> {
    use utoipa::openapi::{RefOr, Schema};

    fn collect(schema: &Schema, out: &mut Vec<(String, String)>) {
        match schema {
            Schema::Object(object) => {
                for (name, property) in &object.properties {
                    if let RefOr::T(Schema::Object(property)) = property
                        && let Some([value]) = property.enum_values.as_deref()
                        && let Some(value) = value.as_str()
                    {
                        out.push((name.clone(), value.to_owned()));
                    }
                }
            }
            Schema::AllOf(all_of) => {
                for branch in &all_of.items {
                    if let RefOr::T(branch) = branch {
                        collect(branch, out);
                    }
                }
            }
            _ => {}
        }
    }

    let mut values = Vec::new();
    collect(schema, &mut values);
    match values.as_slice() {
        [discriminator] => Some(discriminator.clone()),
        _ => None,
    }
}

/// Fallback for presence-discriminated variants (`{required: [server]}` xor
/// `{required: [project]}` …), which carry no enum to name them by: use the
/// sole required property name. Variants requiring more than one property are
/// left alone — there is no unambiguous choice, so they need an explicit
/// `#[schema(title = "...")]` in the Rust source.
fn sole_required_field(schema: &utoipa::openapi::Schema) -> Option<String> {
    use utoipa::openapi::Schema;
    match schema {
        Schema::Object(object) => match object.required.as_slice() {
            [field] => Some(field.clone()),
            _ => None,
        },
        _ => None,
    }
}

fn to_pascal_case(value: &str) -> String {
    value
        .split(['-', '_', ' '])
        .filter(|segment| !segment.is_empty())
        .map(|segment| {
            let mut chars = segment.chars();
            match chars.next() {
                Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
                None => String::new(),
            }
        })
        .collect()
}

/// Materialise per-queue schedule paths and request schemas.
///
/// The utoipa-registered placeholder uses `{queue_name}` as a path
/// parameter and a single generic `ScheduleTaskRequest` body. For each
/// queue that opted in via `TaskConfig::user_schedulable()` we:
///
/// - Clone the placeholder, hard-code its `queue_name` into the URL, and
///   rewrite `operation_id` to `schedule_task_<queue>` so each queue is a
///   distinct `OpenAPI` operation.
/// - Clone `ScheduleTaskRequest::schema()` and strip the generic `payload`
///   property when the queue declares no payload (`utoipa_payload_schema =
///   None`), or rewrite it to reference the queue's payload schema when
///   provided. Either way the published request body is type-correct per
///   queue rather than the generic "any JSON" the placeholder shows.
/// - Insert the per-queue request schema as `Schedule{TypeName}TaskRequest`
///   in components.
///
/// Finally the generic `ScheduleTaskRequest` placeholder is removed from
/// `components/schemas`. Queues that did not opt in are invisible in the
/// generated spec.
#[allow(clippy::too_many_lines)]
fn fix_task_queue_schedule_paths(
    doc: &mut utoipa::openapi::OpenApi,
    queue_api_configs: &[&QueueApiConfig],
    schedule_path: &str,
) {
    let Some(comps) = doc.components.as_mut() else {
        tracing::warn!(
            "No components found in the OpenAPI document; \
             not patching per-queue schedule schemas in."
        );
        return;
    };
    let paths = &mut doc.paths.paths;
    let Some(placeholder) = paths.remove(schedule_path) else {
        tracing::warn!(
            "No path found for ScheduleTask placeholder '{schedule_path}'; \
             skipping per-queue schedule path materialisation."
        );
        return;
    };

    for QueueApiConfig {
        queue_name,
        utoipa_type_name,
        user_scheduling,
        scope: _,
        utoipa_schema: _,
    } in queue_api_configs
    {
        let UserScheduling::Enabled { payload_schema } = user_scheduling else {
            continue;
        };

        // Build the per-queue request schema by cloning the placeholder
        // and adjusting its `payload` property to match what the queue
        // actually accepts.
        let mut per_queue_request_schema = ScheduleTaskRequest::schema();
        match &mut per_queue_request_schema {
            RefOr::Ref(_) => {
                unreachable!("ScheduleTaskRequest::schema() returns an inline schema");
            }
            RefOr::T(Schema::Object(obj)) => match payload_schema.as_ref() {
                None => {
                    obj.properties.remove("payload");
                    obj.required.retain(|r| r != "payload");
                }
                Some(payload_ref) => {
                    obj.properties
                        .insert("payload".to_string(), payload_ref.clone());
                }
            },
            RefOr::T(_) => {
                unreachable!("ScheduleTaskRequest::schema() returns an Object schema");
            }
        }
        // The display stem for the schedule request schema. We strip a
        // trailing `QueueConfig` from the config type name so we get e.g.
        // `ScheduleRemoveOrphanFilesTaskRequest` instead of
        // `ScheduleRemoveOrphanFilesQueueConfigTaskRequest`.
        let display_stem = utoipa_type_name
            .strip_suffix("QueueConfig")
            .unwrap_or(utoipa_type_name);
        let per_queue_request_name = format!("Schedule{display_stem}TaskRequest");

        comps
            .schemas
            .insert(per_queue_request_name.clone(), per_queue_request_schema);

        let concrete_path = schedule_path.replace("{queue_name}", queue_name);
        let mut path_item = placeholder.clone();

        let Some(post) = path_item.post.as_mut() else {
            // Skip this queue rather than bailing out of the entire loop —
            // one malformed item shouldn't hide the rest from the spec.
            tracing::warn!(
                "No POST method on ScheduleTask placeholder '{schedule_path}'; \
                 not materialising schedule path for queue '{queue_name}'."
            );
            continue;
        };
        post.parameters = post.parameters.take().map(|params| {
            params
                .into_iter()
                .filter(|p| p.name != "queue_name")
                .collect()
        });
        post.operation_id = Some(format!("schedule_task_{}", queue_name.replace('-', "_")));
        if let Some(body) = post.request_body.as_mut() {
            body.content.insert(
                "application/json".to_string(),
                utoipa::openapi::ContentBuilder::new()
                    .schema(Some(RefOr::Ref(
                        utoipa::openapi::schema::RefBuilder::new()
                            .ref_location_from_schema_name(per_queue_request_name)
                            .build(),
                    )))
                    .build(),
            );
        }

        paths.insert(concrete_path, path_item);
    }

    // Remove the generic placeholder schema — every callable schedule path
    // now references a concrete `Schedule{TypeName}TaskRequest`.
    comps
        .schemas
        .remove(&ScheduleTaskRequest::name().to_string());
}

#[allow(clippy::too_many_lines)]
fn fix_task_queue_config_paths(
    doc: &mut utoipa::openapi::OpenApi,
    queue_api_configs: &[&QueueApiConfig],
    set_task_queue_config_path: &str,
) {
    let Some(comps) = doc.components.as_mut() else {
        tracing::warn!(
            "No components found in the OpenAPI document, not patching queue configs in."
        );
        return;
    };
    let paths = &mut doc.paths.paths;
    let Some(config_path) = paths.remove(set_task_queue_config_path) else {
        tracing::warn!(
            "No path found for SetTaskQueueConfigRequest, not patching queue configs in."
        );
        return;
    };

    for QueueApiConfig {
        queue_name,
        utoipa_type_name,
        utoipa_schema,
        scope,
        user_scheduling: _,
    } in queue_api_configs
    {
        let operation_object = match scope {
            QueueScope::Project => "project_task_queue_config",
            QueueScope::Warehouse => "task_queue_config",
        };

        let mut set_queue_config_schema = SetTaskQueueConfigRequest::schema();
        let mut get_queue_config_schema = GetTaskQueueConfigResponse::schema();
        let set_queue_config_type_name = format!("Set{utoipa_type_name}");
        let get_queue_config_type_name = format!("Get{utoipa_type_name}");
        let queue_config_type_ref = RefOr::Ref(
            utoipa::openapi::schema::RefBuilder::new()
                .ref_location_from_schema_name(utoipa_type_name.to_string())
                .build(),
        );
        let set_queue_config_type_ref = RefOr::Ref(
            utoipa::openapi::schema::RefBuilder::new()
                .ref_location_from_schema_name(set_queue_config_type_name.clone())
                .build(),
        );
        let get_queue_config_type_ref = RefOr::Ref(
            utoipa::openapi::schema::RefBuilder::new()
                .ref_location_from_schema_name(get_queue_config_type_name.clone())
                .build(),
        );

        // replace the "queue-config" property with a ref to the actual queue config type
        match &mut set_queue_config_schema {
            RefOr::Ref(_) => {
                unreachable!("The schema for SetTaskQueueConfigRequest should not be a reference.");
            }
            RefOr::T(s) => match s {
                utoipa::openapi::schema::Schema::Object(obj) => {
                    let ins = obj
                        .properties
                        .insert("queue-config".to_string(), queue_config_type_ref.clone());
                    if ins.is_none() {
                        unreachable!(
                            "The schema for SetTaskQueueConfigRequest should have a 'queue-config' property."
                        );
                    }
                }
                _ => {
                    unreachable!("The schema for SetTaskQueueConfigRequest should be an object.");
                }
            },
        }
        match &mut get_queue_config_schema {
            RefOr::Ref(_) => {
                unreachable!(
                    "The schema for GetTaskQueueConfigResponse should not be a reference."
                );
            }
            RefOr::T(s) => match s {
                utoipa::openapi::schema::Schema::Object(obj) => {
                    let ins = obj
                        .properties
                        .insert("queue-config".to_string(), queue_config_type_ref.clone());
                    if ins.is_none() {
                        unreachable!(
                            "The schema for GetTaskQueueConfigResponse should have a 'queue-config' property."
                        );
                    }
                }
                _ => {
                    unreachable!("The schema for GetTaskQueueConfigResponse should be an object.");
                }
            },
        }

        let path = set_task_queue_config_path.replace("{queue_name}", queue_name);

        let mut p = config_path.clone();

        let Some(post) = p.post.as_mut() else {
            tracing::warn!(
                "No post method found for '{}' for queue '{queue_name}'; \
                 skipping this queue and continuing with the rest.",
                set_task_queue_config_path
            );
            continue;
        };
        post.parameters = post.parameters.take().map(|params| {
            params
                .into_iter()
                .filter(|param| param.name != "queue_name")
                .collect()
        });
        post.operation_id = Some(format!(
            "set_{operation_object}_{}",
            queue_name.replace('-', "_")
        ));
        let Some(body) = post.request_body.as_mut() else {
            tracing::warn!(
                "No request body found for '{}' for queue '{queue_name}'; \
                 skipping this queue and continuing with the rest.",
                set_task_queue_config_path
            );
            continue;
        };
        body.content.insert(
            "application/json".to_string(),
            utoipa::openapi::ContentBuilder::new()
                .schema(Some(set_queue_config_type_ref))
                .build(),
        );
        let Some(get) = p.get.as_mut() else {
            tracing::warn!(
                "No get method found for '{}' for queue '{queue_name}'; \
                 skipping this queue and continuing with the rest.",
                set_task_queue_config_path
            );
            continue;
        };
        get.parameters = get.parameters.take().map(|params| {
            params
                .into_iter()
                .filter(|param| param.name != "queue_name")
                .collect()
        });
        get.operation_id = Some(format!(
            "get_{operation_object}_{}",
            queue_name.replace('-', "_")
        ));
        let response = utoipa::openapi::response::ResponseBuilder::new()
            .content(
                "application/json",
                utoipa::openapi::content::ContentBuilder::new()
                    .schema(Some(get_queue_config_type_ref))
                    .build(),
            )
            .header(
                "x-request-id",
                utoipa::openapi::HeaderBuilder::new()
                    .schema(
                        utoipa::openapi::schema::Object::builder()
                            .schema_type(utoipa::openapi::schema::SchemaType::new(
                                utoipa::openapi::schema::Type::String,
                            ))
                            .format(Some(utoipa::openapi::schema::SchemaFormat::KnownFormat(
                                KnownFormat::Uuid,
                            ))),
                    )
                    .description(Some("Request identifier, add this to your bug reports."))
                    .build(),
            );
        get.responses
            .responses
            .insert("200".to_string(), RefOr::T(response.build()));

        paths.insert(path, p);

        comps
            .schemas
            .insert(utoipa_type_name.to_string(), utoipa_schema.clone());
        comps
            .schemas
            .insert(set_queue_config_type_name, set_queue_config_schema);
        comps
            .schemas
            .insert(get_queue_config_type_name, get_queue_config_schema);
    }

    // Remove the generic placeholder schemas — every callable path now
    // references a concrete per-queue type. Doing this after the loop
    // ensures the placeholders are dropped even when `queue_api_configs`
    // is empty.
    comps
        .schemas
        .remove(&SetTaskQueueConfigRequest::name().to_string());
    comps
        .schemas
        .remove(&GetTaskQueueConfigResponse::name().to_string());
}

fn add_dependent_schemas(
    doc: &mut utoipa::openapi::OpenApi,
    dependent_schemas: &LazyLock<HashMap<String, RefOr<Schema>>>,
) {
    let dependent_schemas = dependent_schemas
        .iter()
        .map(|(name, schema)| (name.clone(), (*schema).clone()));
    let Some(comps) = doc.components.as_mut() else {
        let mut comps = ComponentsBuilder::new().build();
        comps.schemas.extend(dependent_schemas);
        doc.components = Some(comps);
        return;
    };
    comps.schemas.extend(dependent_schemas);
}

/// Collision handling in the two hoisting passes.
///
/// The shapes here cannot be expressed from the current Rust source — no two
/// unions derive the same variant name today, and no component is named
/// `<Something>Variant`. They are built by hand because the failure they guard
/// against is silent: a hoisted member inserted under a name another schema
/// already holds displaces that schema, while the union's `$ref` keeps
/// resolving, now to something unrelated.
#[cfg(test)]
mod name_allocation_tests {
    use utoipa::openapi::{Components, Object, OneOf, RefOr, Schema, schema::Discriminator};

    use super::{
        hoist_and_discriminate_one_of_variants, name_anonymous_one_of_variants, reserve_name,
    };

    /// A member shaped like a `#[serde(tag = "...")]` variant: an object whose
    /// only single-value enum is the tag, which is what `sole_discriminator`
    /// looks for.
    fn tagged(tag: &str, value: &str, title: Option<&str>) -> RefOr<Schema> {
        let mut tag_property = Object::new();
        tag_property.enum_values = Some(vec![serde_json::json!(value)]);
        let mut member = Object::new();
        member
            .properties
            .insert(tag.to_owned(), RefOr::T(Schema::Object(tag_property)));
        member.title = title.map(ToOwned::to_owned);
        RefOr::T(Schema::Object(member))
    }

    /// A plain component carrying a marker property, so displacement is visible.
    fn marker(name: &str) -> RefOr<Schema> {
        let mut object = Object::new();
        object.properties.insert(
            format!("marker_{name}"),
            RefOr::T(Schema::Object(Object::new())),
        );
        RefOr::T(Schema::Object(object))
    }

    fn union(members: Vec<RefOr<Schema>>) -> RefOr<Schema> {
        let mut one_of = OneOf::new();
        one_of.items = members;
        RefOr::T(Schema::OneOf(one_of))
    }

    fn doc(schemas: Vec<(&str, RefOr<Schema>)>) -> utoipa::openapi::OpenApi {
        let mut components = Components::new();
        for (name, schema) in schemas {
            components.schemas.insert(name.to_owned(), schema);
        }
        utoipa::openapi::OpenApiBuilder::new()
            .components(Some(components))
            .build()
    }

    fn run_passes(doc: &mut utoipa::openapi::OpenApi) {
        name_anonymous_one_of_variants(doc);
        hoist_and_discriminate_one_of_variants(doc);
    }

    fn schema_names(doc: &utoipa::openapi::OpenApi) -> Vec<String> {
        doc.components
            .as_ref()
            .expect("components")
            .schemas
            .keys()
            .cloned()
            .collect()
    }

    /// What a union ended up as: the `$ref` targets of its members, plus whether
    /// a discriminator was published.
    fn union_refs(
        doc: &utoipa::openapi::OpenApi,
        name: &str,
    ) -> (Vec<String>, Option<Discriminator>) {
        let Some(RefOr::T(Schema::OneOf(one_of))) = doc
            .components
            .as_ref()
            .expect("components")
            .schemas
            .get(name)
        else {
            panic!("`{name}` is not a oneOf");
        };
        let refs = one_of
            .items
            .iter()
            .map(|item| match item {
                RefOr::Ref(reference) => reference
                    .ref_location
                    .rsplit('/')
                    .next()
                    .expect("ref location")
                    .to_owned(),
                RefOr::T(_) => "<inline>".to_owned(),
            })
            .collect();
        (refs, one_of.discriminator.clone())
    }

    #[test]
    fn reserve_name_walks_a_suffix_ladder_then_gives_up() {
        let mut taken = std::collections::BTreeSet::new();
        assert_eq!(reserve_name(&mut taken, "Foo").as_deref(), Some("Foo"));
        // The same preferred name must not be handed out twice, even though
        // nothing has been inserted into the document yet.
        assert_eq!(
            reserve_name(&mut taken, "Foo").as_deref(),
            Some("FooVariant")
        );
        assert_eq!(
            reserve_name(&mut taken, "Foo").as_deref(),
            Some("FooVariant2")
        );

        let mut exhausted = std::collections::BTreeSet::new();
        exhausted.insert("Bar".to_owned());
        exhausted.insert("BarVariant".to_owned());
        for n in 2..=16 {
            exhausted.insert(format!("BarVariant{n}"));
        }
        assert_eq!(
            reserve_name(&mut exhausted, "Bar"),
            None,
            "an exhausted ladder must refuse rather than return a taken name"
        );
    }

    /// Two unions whose derived variant names coincide: `Foo` + tag `bar-baz`
    /// and `FooBar` + tag `baz` both want `FooBarBaz`. Each must get its own
    /// component, and neither may reference the other's.
    #[test]
    fn colliding_derived_names_across_unions_get_distinct_components() {
        let mut doc = doc(vec![
            ("Foo", union(vec![tagged("type", "bar-baz", None)])),
            ("FooBar", union(vec![tagged("type", "baz", None)])),
        ]);
        run_passes(&mut doc);

        // BTreeMap order: `Foo` is visited before `FooBar`, so `Foo` claims the
        // preferred name and `FooBar` takes the suffixed one.
        let (foo_refs, foo_discriminator) = union_refs(&doc, "Foo");
        let (foo_bar_refs, foo_bar_discriminator) = union_refs(&doc, "FooBar");
        assert_eq!(foo_refs, vec!["FooBarBaz".to_owned()]);
        assert_eq!(foo_bar_refs, vec!["FooBarBazVariant".to_owned()]);
        assert_ne!(
            foo_refs, foo_bar_refs,
            "the two unions must not share a variant schema"
        );
        assert!(foo_discriminator.is_some() && foo_bar_discriminator.is_some());

        let names = schema_names(&doc);
        assert!(names.contains(&"FooBarBaz".to_owned()));
        assert!(names.contains(&"FooBarBazVariant".to_owned()));
    }

    /// The `Variant` fallback can itself be occupied. The ladder must continue
    /// rather than hand back a name that would displace an existing schema.
    #[test]
    fn an_occupied_variant_suffix_continues_the_ladder() {
        let mut doc = doc(vec![
            ("FooBar", marker("FooBar")),
            ("FooBarVariant", marker("FooBarVariant")),
            ("Foo", union(vec![tagged("type", "bar", None)])),
        ]);
        run_passes(&mut doc);

        let (refs, discriminator) = union_refs(&doc, "Foo");
        assert_eq!(refs, vec!["FooBarVariant2".to_owned()]);
        assert!(discriminator.is_some());

        // Both occupied names must still hold their original schemas.
        for occupied in ["FooBar", "FooBarVariant"] {
            let Some(RefOr::T(Schema::Object(object))) = doc
                .components
                .as_ref()
                .expect("components")
                .schemas
                .get(occupied)
            else {
                panic!("`{occupied}` was displaced");
            };
            assert!(
                object
                    .properties
                    .contains_key(&format!("marker_{occupied}")),
                "`{occupied}` lost its properties to a hoisted variant"
            );
        }
    }

    /// A hand-written `#[schema(title = "...")]` is never rewritten by the
    /// naming pass, so the hoisting pass is the only thing standing between it
    /// and an existing component. It must refuse the union rather than publish
    /// the variant under a name the author did not choose.
    #[test]
    fn an_explicit_title_clashing_with_a_component_refuses_the_union() {
        let mut doc = doc(vec![
            ("Taken", marker("Taken")),
            (
                "Union",
                union(vec![tagged("type", "whatever", Some("Taken"))]),
            ),
        ]);
        run_passes(&mut doc);

        let (refs, discriminator) = union_refs(&doc, "Union");
        assert_eq!(
            refs,
            vec!["<inline>".to_owned()],
            "the union must be left untouched, not rewritten to a $ref"
        );
        assert!(
            discriminator.is_none(),
            "no discriminator may be published for a refused union"
        );

        let Some(RefOr::T(Schema::Object(object))) = doc
            .components
            .as_ref()
            .expect("components")
            .schemas
            .get("Taken")
        else {
            panic!("`Taken` was displaced");
        };
        assert!(
            object.properties.contains_key("marker_Taken"),
            "`Taken` lost its properties to a hoisted variant"
        );
    }
}

/// Structural checks on the generated `OpenAPI` documents.
///
/// The schema passes above rewrite `components/schemas` in place — hoisting
/// members, rewriting them to `$ref`s and attaching discriminators. Every one of
/// those edits can leave the document internally inconsistent in a way that
/// still serialises fine and only surfaces as a broken generated client, so the
/// invariants are asserted here rather than discovered downstream.
#[cfg(test)]
mod spec_integrity_tests {
    use serde_json::Value;

    use super::api_doc;
    use crate::service::{authz::AllowAllAuthorizer, tasks};

    fn management_spec() -> Value {
        let queue_configs = tasks::BUILT_IN_API_CONFIGS.iter().collect::<Vec<_>>();
        let project_queue_configs = tasks::BUILT_IN_PROJECT_API_CONFIGS
            .iter()
            .collect::<Vec<_>>();
        serde_json::to_value(api_doc::<AllowAllAuthorizer>(
            &queue_configs,
            &project_queue_configs,
        ))
        .expect("the generated document must serialise")
    }

    fn generic_table_spec() -> Value {
        serde_json::to_value(crate::api::data::v1::generic_tables::api_doc())
            .expect("the generated document must serialise")
    }

    fn schemas(spec: &Value) -> &serde_json::Map<String, Value> {
        spec["components"]["schemas"]
            .as_object()
            .expect("components/schemas must be an object")
    }

    fn walk<'a>(node: &'a Value, path: &str, out: &mut Vec<(String, &'a Value)>) {
        match node {
            Value::Object(map) => {
                for (key, value) in map {
                    let child = format!("{path}.{key}");
                    if key == "$ref" {
                        out.push((child.clone(), value));
                    }
                    walk(value, &child, out);
                }
            }
            Value::Array(items) => {
                for (index, item) in items.iter().enumerate() {
                    walk(item, &format!("{path}[{index}]"), out);
                }
            }
            _ => {}
        }
    }

    /// A `$ref` that names a schema which does not exist serialises happily and
    /// then makes every generator fail — or worse, emit a client with a missing
    /// type.
    fn assert_refs_resolve(spec: &Value, label: &str) {
        let schemas = schemas(spec);
        let mut refs = Vec::new();
        walk(spec, "", &mut refs);
        let dangling = refs
            .iter()
            .filter_map(|(path, value)| value.as_str().map(|value| (path, value)))
            .filter(|(_, value)| value.starts_with("#/components/schemas/"))
            .filter(|(_, value)| {
                !schemas.contains_key(value.trim_start_matches("#/components/schemas/"))
            })
            .collect::<Vec<_>>();
        assert!(
            dangling.is_empty(),
            "{label}: $ref(s) point at schemas that do not exist: {dangling:?}"
        );
    }

    /// A discriminator is only usable if every mapping target is one of the
    /// union's own members and names a real schema, and if every member is
    /// reachable — an unmapped member can never be selected.
    fn assert_discriminators_are_consistent(spec: &Value, label: &str) {
        let schemas = schemas(spec);
        for (name, schema) in schemas {
            let Some(discriminator) = schema.get("discriminator") else {
                continue;
            };
            let members = schema
                .get("oneOf")
                .and_then(Value::as_array)
                .map(|members| {
                    members
                        .iter()
                        .filter_map(|member| member.get("$ref").and_then(Value::as_str))
                        .collect::<std::collections::BTreeSet<_>>()
                })
                .unwrap_or_default();
            let mapping = discriminator
                .get("mapping")
                .and_then(Value::as_object)
                .unwrap_or_else(|| panic!("{label}: `{name}` has a discriminator with no mapping"));

            assert!(
                discriminator
                    .get("propertyName")
                    .and_then(Value::as_str)
                    .is_some_and(|property| !property.is_empty()),
                "{label}: `{name}` has a discriminator without a propertyName"
            );
            for (value, target) in mapping {
                let target = target
                    .as_str()
                    .unwrap_or_else(|| panic!("{label}: `{name}` maps `{value}` to a non-string"));
                assert!(
                    members.contains(target),
                    "{label}: `{name}` maps `{value}` to `{target}`, which is not one of its \
                     oneOf members"
                );
                assert!(
                    schemas.contains_key(target.trim_start_matches("#/components/schemas/")),
                    "{label}: `{name}` maps `{value}` to `{target}`, which does not exist"
                );
            }
            assert_eq!(
                mapping.len(),
                members.len(),
                "{label}: `{name}` has {} members but {} mapped, so some variant can never be \
                 selected",
                members.len(),
                mapping.len()
            );

            // The mapping key must be the value the target actually pins for
            // the discriminator property. A key that matches nothing on the
            // wire makes the generator dispatch on a value it will never see.
            let property = discriminator["propertyName"].as_str().unwrap();
            for (value, target) in mapping {
                let target_name = target
                    .as_str()
                    .unwrap()
                    .trim_start_matches("#/components/schemas/");
                let pinned = pinned_value(&schemas[target_name], property);
                assert_eq!(
                    pinned.as_deref(),
                    Some(value.as_str()),
                    "{label}: `{name}` maps `{value}` to `{target_name}`, but that schema pins \
                     `{property}` to {pinned:?}"
                );
            }
        }
    }

    /// The single-value enum a schema pins `property` to, looking through
    /// `allOf` branches — utoipa renders a tagged variant as
    /// `allOf: [{$ref}, {tag const}]`.
    fn pinned_value(schema: &Value, property: &str) -> Option<String> {
        if let Some(Value::Array(values)) = schema
            .get("properties")
            .and_then(|properties| properties.get(property))
            .and_then(|property| property.get("enum"))
            && let [Value::String(value)] = values.as_slice()
        {
            return Some(value.clone());
        }
        schema
            .get("allOf")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .find_map(|branch| pinned_value(branch, property))
    }

    /// No schema may reference itself.
    ///
    /// The hoisting passes name a variant after its parent union plus its tag
    /// value. When that derived name is one an existing component already holds,
    /// inserting the variant replaces that component — and because a variant
    /// usually *composes* the schema it is named after
    /// (`allOf: [{$ref: X}, {tag}]`), the replacement `$ref`s itself. That
    /// resolves, so [`assert_refs_resolve`] passes; the original schema's
    /// properties are simply gone from the document, and generators emit an
    /// infinitely recursive type.
    fn assert_no_self_references(spec: &Value, label: &str) {
        let schemas = schemas(spec);
        for (name, schema) in schemas {
            let mut refs = Vec::new();
            walk(schema, "", &mut refs);
            let target = format!("#/components/schemas/{name}");
            let self_refs = refs
                .iter()
                .filter(|(_, value)| value.as_str() == Some(target.as_str()))
                .map(|(path, _)| path.as_str())
                .collect::<Vec<_>>();
            assert!(
                self_refs.is_empty(),
                "{label}: `{name}` references itself at {self_refs:?} — a name derived for a \
                 hoisted union variant collided with it and overwrote it"
            );
        }
    }

    /// Inline union members must carry a `title`. Without one a generator
    /// invents a name, and because identical inline schemas are deduplicated the
    /// invented name is frequently taken from an unrelated union.
    fn assert_union_members_are_named(spec: &Value, label: &str) {
        for (name, schema) in schemas(spec) {
            let Some(members) = schema.get("oneOf").and_then(Value::as_array) else {
                continue;
            };
            for (index, member) in members.iter().enumerate() {
                if member.get("$ref").is_some() {
                    continue;
                }
                assert!(
                    member.get("title").and_then(Value::as_str).is_some(),
                    "{label}: `{name}` member {index} is inline and has no title"
                );
            }
        }
    }

    #[test]
    fn management_spec_is_internally_consistent() {
        let spec = management_spec();
        assert_refs_resolve(&spec, "management");
        assert_no_self_references(&spec, "management");
        assert_discriminators_are_consistent(&spec, "management");
        assert_union_members_are_named(&spec, "management");
    }

    #[test]
    fn generic_table_spec_is_internally_consistent() {
        let spec = generic_table_spec();
        assert_refs_resolve(&spec, "generic-table");
        assert_no_self_references(&spec, "generic-table");
        assert_discriminators_are_consistent(&spec, "generic-table");
        assert_union_members_are_named(&spec, "generic-table");
    }
}
