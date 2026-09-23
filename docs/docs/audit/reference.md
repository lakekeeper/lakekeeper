# Audit format reference: `lakekeeper` 1.0

<!-- Generated from audit-format/schema.json by `just update-audit-schema`. Do not edit; change the doc comment on the field and regenerate. -->

Every object and every closed set of values that records emitted by `lakekeeper` can carry. Field descriptions are the doc comments of the emitting types. Optional fields are absent when not recorded unless a description says otherwise.

## Objects

Nested objects of a record.

### `ActionRecord`

An `action` object: the wire name and the action's context fields.

| Field | Type | Present | Description |
|---|---|---|---|
| `action_name` | string | always | The action's wire name, from a vocabulary enum of any emitter. |
| *any other key* | [`ContextValue`](#contextvalue) | optional | Keys from a closed set, see the values section. |

### `ActorRecord`

The `actor` object: who made the request, as authentication established it.

| Field | Type | Present | Description |
|---|---|---|---|
| `actor_type` | string | always | One of `anonymous`, `principal`, `assumed-role`, `lakekeeper-internal`. |
| `principal` | string | optional | The authenticated principal. Present for `principal` and `assumed-role`. |
| `email` | string | optional | Best-effort email of `principal`. Present only when the operator enabled it and one was available. Metadata, not identity: correlate on `principal`. |
| `assumed_role` | [`AssumedRoleRecord`](#assumedrolerecord) | optional | The role acted as. Present for `assumed-role`; `principal` is still the human. |

### `AssumedRoleRecord`

The role an `assumed-role` actor acts as.

| Field | Type | Present | Description |
|---|---|---|---|
| `role_id` | string | always | The role's id in this catalog. |
| `provider_id` | string | always | The provider that supplied the role. |
| `source_id` | string | always | The role's id at the provider. |

### `ContextValue`

One of:

- object
- array of string
- string

### `DecisionRecord`

One entry of `authorizations[]`: which action on which entity was evaluated, for whom,
with what result.

| Field | Type | Present | Description |
|---|---|---|---|
| `id` | string | optional | The client's id for this check in a batch, or its index. Absent for single checks. |
| `for-principal` | [`SubjectRecord`](#subjectrecord) | optional | The principal whose permission was evaluated, when it is not the request's actor. |
| `action` | [`ActionRecord`](#actionrecord) | always | The action evaluated. |
| `entity` | [`EntityRecord`](#entityrecord) | always | The entity the action was evaluated against. |
| `allowed` | boolean | optional | The authorizer's answer. Absent when an upstream error stopped the evaluation. |
| `determined_by` | array of any | optional | The policies or rules that determined the decision, when the authorizer reports them. |

### `EmitterRecord`

The `emitter` object: which product produced this record, and the version of the
vocabulary and context shapes it governs.

A consumer routes the core shape on `audit_format` and everything the emitter owns — its
`context`, its vocabulary, any shape it defines — on this.

| Field | Type | Present | Description |
|---|---|---|---|
| `name` | string | always | The emitter's name, unique across the products that write to this log. |
| `format` | string | always | The `MAJOR.MINOR` version of what this emitter contributes. |

### `EntityRecord`

An `entity` object: the kind of resource and its identifying fields.

| Field | Type | Present | Description |
|---|---|---|---|
| `entity_type` | string | always | The kind of resource: `table`, `namespace`, `warehouse`, … |
| *any other key* | string | optional | Keys from a closed set, see the values section. |

### `ErrorRecord`

The `error` object of a denied authorization record.

| Field | Type | Present | Description |
|---|---|---|---|
| `type` | string | always | The error type the caller received. |
| `code` | integer | always | The HTTP status the caller received. |
| `message` | string | always | The error message the caller received. |
| `stack` | array of string | optional | The error's stack of causes, innermost first. Absent when empty. |
| `error_id` | string | always | The id the caller can quote to correlate with this record. |

### `HandlerContext`

The `context` map of an authorization record: keys a handler recorded, with string values.

An object whose keys are data and whose values are string.

### `RoleSubjectRecord`

A role named as a target.

| Field | Type | Present | Description |
|---|---|---|---|
| `role` | string | always | The role's id. |

### `SubjectRecord`

A principal named as a target: `for-principal` on a decision entry, `principal` on a grant
record. `{"user": …}` or `{"role": …}`.

One of:

- [`UserSubjectRecord`](#usersubjectrecord)
- [`RoleSubjectRecord`](#rolesubjectrecord)

### `UserSubjectRecord`

A user named as a target.

| Field | Type | Present | Description |
|---|---|---|---|
| `user` | string | always | The user's principal id. |
| `email` | string | optional | Best-effort email of `user`. Present only when the operator enabled it and one was available. Metadata, not identity: correlate on `user`. |

## Operation contexts

The `context` object of operation records, one per operation kind.

### `AdmissionRejectedContext`

Context for the admission-rejection audit record.

The operation-specific fields live here rather than at the top level,
because that is the shape every `event_source="audit"` operational record
promises. `error_id` correlates with what the caller was handed, and
`request_id` is repeated out of the span so the record stands alone.

| Field | Type | Present | Description |
|---|---|---|---|
| `gate` | string | always | The gate that rejected the request. |
| `denied_by` | string | optional | The rule of the gate that decided, when the gate names one. |
| `status` | integer | always | The HTTP status the caller received. |
| `error_type` | string | always | The error type the caller received. |
| `message` | string | always | The gate's own wording. Suppressing the error-response line takes this with it, and it is what separates two rejections that share a type — a gate failing closed on a missing precondition from the same gate failing closed on an unreachable upstream. |
| `error_id` | string | always | The id the caller can quote to correlate with this record. |
| `request_id` | string | always | The request this rejection belongs to, repeated out of the span so the record stands alone. |

### `GrantContextRecord`

The `context` of a grant record: the full `(principal, privilege, resource)` triple. Grants
are hard-deleted and keep no history, so a revocation's triple exists nowhere else once the
row is gone.

| Field | Type | Present | Description |
|---|---|---|---|
| `principal` | [`SubjectRecord`](#subjectrecord) | always | Who holds the grant. |
| `privilege` | string | always | The privilege name, verbatim from the authorizer's vocabulary. |
| `resource_type` | string | always | The kind of resource the grant is on. |
| `resource_id` | string | optional | The exact resource. Absent for server grants, whose type is their whole identity. |
| `warehouse_id` | string | optional | The containing warehouse, for warehouse-scoped resources. |

### `NoContext`

The `context` of an operation record that carries none.

## Values

Closed sets of values, by the field that carries them.

### `ActionContextKey`

Values of `action-key`:

- `allow-partial`
- `base_location`
- `created-before`
- `deletes`
- `destination`
- `dry-run`
- `force`
- `format`
- `generic_table_id`
- `name`
- `narrowed_privileges`
- `principal`
- `principals`
- `privilege_scope`
- `privileges`
- `project_id`
- `properties`
- `purge`
- `recursive`
- `removed-properties`
- `requested_provider_id`
- `requested_source_id`
- `resource_types`
- `root_level`
- `source`
- `table_id`
- `target-refs`
- `update-kinds`
- `updated-properties`
- `writes`

### `AssignmentAction`

Values of `action_name`:

- `update_generic_table_assignments`
- `update_namespace_assignments`
- `update_project_assignments`
- `update_role_assignments`
- `update_server_assignments`
- `update_table_assignments`
- `update_tag_assignments`
- `update_view_assignments`
- `update_warehouse_assignments`

### `AuthnAction`

Values of `action_name`:

- `assume_role`

### `CatalogGenericTableAction`

Values of `action_name`:

- `control_tasks`
- `drop`
- `get_metadata`
- `get_tasks`
- `include_in_list`
- `manage_tags`
- `read_data`
- `read_grants`
- `rename`
- `set_protection`
- `undrop`
- `write_data`

### `CatalogNamespaceAction`

Values of `action_name`:

- `accept_moved_namespace`
- `create_generic_table`
- `create_namespace`
- `create_table`
- `create_view`
- `delete`
- `get_metadata`
- `include_in_list`
- `list_everything`
- `list_generic_tables`
- `list_namespaces`
- `list_tables`
- `list_views`
- `manage_tags`
- `move`
- `read_grants`
- `read_subtree_grants`
- `revoke_subtree_grants`
- `set_protection`
- `update_properties`

### `CatalogProjectAction`

Values of `action_name`:

- `control_project_tasks`
- `create_role`
- `create_tag`
- `create_warehouse`
- `delete`
- `get_endpoint_statistics`
- `get_metadata`
- `get_project_tasks`
- `get_task_queue_config`
- `include_in_list`
- `list_roles`
- `list_tags`
- `list_warehouses`
- `modify_task_queue_config`
- `read_grants`
- `rename`
- `search_roles`

### `CatalogRoleAction`

Values of `action_name`:

- `delete`
- `manage_role_assignments`
- `read`
- `read_metadata`
- `read_role_assignments`
- `update`
- `update_source_system`

### `CatalogServerAction`

Values of `action_name`:

- `create_project`
- `delete_users`
- `list_users`
- `provision_users`
- `read_grants`
- `update_users`

### `CatalogTableAction`

Values of `action_name`:

- `commit`
- `control_tasks`
- `drop`
- `get_metadata`
- `get_tasks`
- `include_in_list`
- `manage_tags`
- `read_data`
- `read_grants`
- `rename`
- `set_protection`
- `undrop`
- `write_data`

### `CatalogTagAction`

Values of `action_name`:

- `apply`
- `delete`
- `read`
- `read_attachments`
- `read_grants`
- `remove`
- `update`

### `CatalogUserAction`

Values of `action_name`:

- `delete`
- `read`
- `read_role_assignments`
- `update`

### `CatalogViewAction`

Values of `action_name`:

- `commit`
- `control_tasks`
- `drop`
- `get_metadata`
- `get_tasks`
- `include_in_list`
- `manage_tags`
- `read_grants`
- `rename`
- `select`
- `set_protection`
- `undrop`

### `CatalogWarehouseAction`

Values of `action_name`:

- `accept_moved_namespace`
- `activate`
- `control_all_tasks`
- `create_namespace`
- `deactivate`
- `delete`
- `get_all_tasks`
- `get_config`
- `get_endpoint_statistics`
- `get_metadata`
- `get_task_queue_config`
- `include_in_list`
- `list_deleted_tabulars`
- `list_everything`
- `list_namespaces`
- `manage_tags`
- `modify_soft_deletion`
- `modify_task_queue_config`
- `read_grants`
- `read_subtree_grants`
- `rename`
- `revoke_subtree_grants`
- `set_format_version_policy`
- `set_protection`
- `update_storage`
- `use`

### `FallbackAction`

Values of `action_name`:

- `unknown`

### `GenericTableRelation`

Values of `action_name`:

- `can_change_ownership`
- `can_control_tasks`
- `can_drop`
- `can_get_metadata`
- `can_get_tasks`
- `can_grant_describe`
- `can_grant_manage_grants`
- `can_grant_manage_tags`
- `can_grant_modify`
- `can_grant_pass_grants`
- `can_grant_select`
- `can_include_in_list`
- `can_manage_tags`
- `can_read_assignments`
- `can_read_data`
- `can_rename`
- `can_revoke_describe`
- `can_revoke_modify`
- `can_revoke_select`
- `can_set_protection`
- `can_undrop`
- `can_write_data`
- `describe`
- `manage_grants`
- `manage_tags`
- `modify`
- `ownership`
- `parent`
- `pass_grants`
- `select`

### `InstanceAdminAction`

Values of `action_name`:

- `set_warehouse_managed_by`

### `ManagementAction`

Values of `action_name`:

- `apply_grants`
- `control_tasks`
- `get_task_details`
- `introspect_permissions`
- `list_projects`
- `list_tasks`
- `revoke_subtree_grants`
- `schedule_task`
- `search_tabulars`
- `search_users`

### `NamespaceRelation`

Values of `action_name`:

- `can_accept_moved_namespace`
- `can_change_ownership`
- `can_create_generic_table`
- `can_create_namespace`
- `can_create_table`
- `can_create_view`
- `can_delete`
- `can_get_metadata`
- `can_grant_create`
- `can_grant_describe`
- `can_grant_manage_grants`
- `can_grant_manage_tags`
- `can_grant_modify`
- `can_grant_pass_grants`
- `can_grant_select`
- `can_include_in_list`
- `can_list_everything`
- `can_list_generic_tables`
- `can_list_namespaces`
- `can_list_tables`
- `can_list_views`
- `can_manage_tags`
- `can_move`
- `can_read_assignments`
- `can_read_subtree_assignments`
- `can_revoke_create`
- `can_revoke_describe`
- `can_revoke_modify`
- `can_revoke_select`
- `can_revoke_subtree_assignments`
- `can_set_managed_access`
- `can_set_protection`
- `can_update_properties`
- `child`
- `create`
- `describe`
- `manage_grants`
- `manage_tags`
- `managed_access`
- `managed_access_inheritance`
- `modify`
- `ownership`
- `parent`
- `pass_grants`
- `select`

### `ProjectRelation`

Values of `action_name`:

- `can_control_project_tasks`
- `can_create_role`
- `can_create_tag`
- `can_create_warehouse`
- `can_delete`
- `can_get_endpoint_statistics`
- `can_get_metadata`
- `can_get_project_tasks`
- `can_get_task_queue_config`
- `can_grant_create`
- `can_grant_data_admin`
- `can_grant_describe`
- `can_grant_modify`
- `can_grant_project_admin`
- `can_grant_role_creator`
- `can_grant_security_admin`
- `can_grant_select`
- `can_grant_tag_creator`
- `can_include_in_list`
- `can_list_roles`
- `can_list_tags`
- `can_list_warehouses`
- `can_modify_task_queue_config`
- `can_read_assignments`
- `can_rename`
- `can_search_roles`
- `create`
- `data_admin`
- `describe`
- `modify`
- `project_admin`
- `role_creator`
- `security_admin`
- `select`
- `server`
- `tag_creator`
- `warehouse`

### `RoleRelation`

Values of `action_name`:

- `assignee`
- `can_assume`
- `can_change_ownership`
- `can_delete`
- `can_grant_assignee`
- `can_read`
- `can_read_assignments`
- `can_read_metadata`
- `can_update`
- `can_update_source_system`
- `ownership`
- `project`

### `ServerRelation`

Values of `action_name`:

- `admin`
- `can_create_project`
- `can_delete_users`
- `can_grant_admin`
- `can_grant_operator`
- `can_list_all_projects`
- `can_list_users`
- `can_provision_users`
- `can_read_assignments`
- `can_update_users`
- `operator`
- `project`

### `TableRelation`

Values of `action_name`:

- `can_change_ownership`
- `can_commit`
- `can_control_tasks`
- `can_drop`
- `can_get_metadata`
- `can_get_tasks`
- `can_grant_describe`
- `can_grant_manage_grants`
- `can_grant_manage_tags`
- `can_grant_modify`
- `can_grant_pass_grants`
- `can_grant_select`
- `can_include_in_list`
- `can_manage_tags`
- `can_read_assignments`
- `can_read_data`
- `can_rename`
- `can_revoke_describe`
- `can_revoke_modify`
- `can_revoke_select`
- `can_set_protection`
- `can_undrop`
- `can_write_data`
- `describe`
- `manage_grants`
- `manage_tags`
- `modify`
- `ownership`
- `parent`
- `pass_grants`
- `select`

### `TagRelation`

Values of `action_name`:

- `apply`
- `can_apply`
- `can_change_ownership`
- `can_delete`
- `can_grant_apply`
- `can_read`
- `can_read_assignments`
- `can_read_attachments`
- `can_update`
- `ownership`
- `project`

### `ViewRelation`

Values of `action_name`:

- `can_change_ownership`
- `can_commit`
- `can_control_tasks`
- `can_drop`
- `can_get_metadata`
- `can_get_tasks`
- `can_grant_describe`
- `can_grant_manage_grants`
- `can_grant_manage_tags`
- `can_grant_modify`
- `can_grant_pass_grants`
- `can_grant_select`
- `can_include_in_list`
- `can_manage_tags`
- `can_read_assignments`
- `can_rename`
- `can_revoke_describe`
- `can_revoke_modify`
- `can_revoke_select`
- `can_select`
- `can_set_protection`
- `can_undrop`
- `describe`
- `manage_grants`
- `manage_tags`
- `modify`
- `ownership`
- `parent`
- `pass_grants`
- `select`

### `WarehouseRelation`

Values of `action_name`:

- `can_accept_moved_namespace`
- `can_activate`
- `can_change_ownership`
- `can_control_all_tasks`
- `can_create_namespace`
- `can_deactivate`
- `can_delete`
- `can_get_all_tasks`
- `can_get_config`
- `can_get_endpoint_statistics`
- `can_get_metadata`
- `can_get_task_queue_config`
- `can_grant_create`
- `can_grant_describe`
- `can_grant_manage_grants`
- `can_grant_manage_tags`
- `can_grant_modify`
- `can_grant_pass_grants`
- `can_grant_select`
- `can_include_in_list`
- `can_list_deleted_tabulars`
- `can_list_everything`
- `can_list_namespaces`
- `can_manage_tags`
- `can_modify_soft_deletion`
- `can_modify_task_queue_config`
- `can_read_assignments`
- `can_read_subtree_assignments`
- `can_rename`
- `can_revoke_create`
- `can_revoke_describe`
- `can_revoke_modify`
- `can_revoke_select`
- `can_revoke_subtree_assignments`
- `can_set_format_version_policy`
- `can_set_managed_access`
- `can_set_protection`
- `can_update_storage`
- `can_update_storage_credential`
- `can_use`
- `create`
- `describe`
- `manage_grants`
- `manage_tags`
- `managed_access`
- `modify`
- `namespace`
- `ownership`
- `pass_grants`
- `project`
- `select`

### `ActorType`

Values of `actor_type`:

- `anonymous`
- `assumed-role`
- `lakekeeper-internal`
- `principal`

### `HandlerContextKey`

Values of `context-key`:

- `entity_id`
- `invoked-by`
- `queue_name`
- `self-provisioning`
- `self-read`

### `Decision`

Values of `decision`:

- `allowed`
- `denied`

### `EntityField`

Values of `entity-key`:

- `generic-table`
- `generic-table-id`
- `namespace`
- `namespace-id`
- `project-id`
- `role-id`
- `role-provider-id`
- `role-source-id`
- `server-id`
- `table`
- `table-id`
- `table-location`
- `tag-definition-id`
- `task-id`
- `user-id`
- `view`
- `view-id`
- `warehouse-id`

### `EntityType`

Values of `entity_type`:

- `generic-table`
- `namespace`
- `project`
- `role`
- `server`
- `table`
- `tag`
- `task`
- `unknown`
- `user`
- `view`
- `warehouse`

### `AuthorizationFailureReason`

Values of `failure_reason`:

- `ActionForbidden`
- `CannotSeeResource`
- `InternalAuthorizationError`
- `InternalCatalogError`
- `InvalidRequestData`
- `ResourceNotFound`

### `AuditOperation`

Values of `operation`:

- `admission_decided`
- `grant_created`
- `grant_revoked`
- `idempotent_replay`

### `AuditOutcome`

Values of `outcome`:

- `forbidden`
- `replayed`
- `success`
- `unavailable`

### `PrivilegeSource`

Values of `privilege_source`:

- `authorizer`
- `instance_admin`
- `internal`

### `RecordType`

Values of `record_type`:

- `authorization`
- `operation`
- `replay`

### `ResourceType`

Values of `resource_type`:

- `generic-table`
- `namespace`
- `project`
- `server`
- `table`
- `tag-definition`
- `view`
- `warehouse`

