---
description: "Release notes for Lakekeeper+, the commercial distribution, covering Cedar authorization, UI branding, admission gates and other enterprise features."
---

# Lakekeeper+ Release Notes

## v0.14.0 (2026-10-08)

_Based on Lakekeeper OSS v0.14.0._

### Highlights

- **Cedar: grants decide access.** Predefined policies turn the grants you give in the console or through the Grants API into access, so you can hand out permissions without changing a policy file. Each project and warehouse chooses which predefined policies apply. See [Predefined policies](https://docs.lakekeeper.io/docs/0.14.x/authorization-cedar/#predefined-policies).
- **Cedar: policies per project and warehouse.** Each project and warehouse can keep its own Cedar policies, managed in the console or through the API. If a policy locks everyone out, an operator can repair it with break-glass. See [Break-Glass](https://docs.lakekeeper.io/docs/0.14.x/authorization-cedar/#break-glass).
- **Cedar: roles and governance tags.** Roles can be created and managed in Lakekeeper, and policies can decide access by governance tags. See [Tag-Based Access Control](https://docs.lakekeeper.io/docs/0.14.x/authorization-cedar/#tag-based-access-control).

### Features

- **Grants in policies.** Each resource carries what the principal holds on it as `principal_privileges`: `direct` for grants on the resource itself, `inherited` for grants above it. For example: `resource.principal_privileges.direct.select`. Grant actions carry the privilege being granted as `context.privilege`.
- **Predefined policies.** They are on by default; `LAKEKEEPER__CEDAR__PREDEFINED_POLICIES_ENABLED=false` switches all of them off. A project or warehouse can switch single policies on or off. A policy that a later release adds starts switched off where you have already chosen. With `include-policy=true`, the listing also returns each policy's rule. Policy listings say in `can-write` and `can-toggle` whether you may change them.
- **Policies per project and warehouse.** Manage them in the console or through the `…/policies` endpoints. A change is checked as a whole and applied in one transaction. `dry-run` previews it, `if-scope-version` protects against concurrent edits, and `replace: true` replaces all policies of the scope. A scope holds up to 1000 policies (`LAKEKEEPER__CEDAR__MAX_POLICIES_PER_SCOPE`).
- **Break-glass.** With the header `x-break-glass: <reason>`, an operator is judged by the server's policies alone and can repair a project's or warehouse's policies. It opens policy administration only, never data, and every use is audited with its reason. The `break-glass-status` endpoint tells you whether it is open to you. Instance admins must send the header to write a scope's stored policies.
- **New privileges for separation of duties.** `manage_policies` reads and writes a scope's policies, and `read_policies` only reads them. `read_grants` shows who holds what on an object and below it; this reveals that an object exists, even where a policy hides it. `manage_tags` tags objects; attaching a tag also needs `apply` or `manage` on the tag, so hand out `apply` carefully when policies decide access by tags. None of these privileges gives access to data by itself, but a `manage_policies` holder can write a policy that does, so `manage_policies` is never part of `manage`. `pass_grants` passes on only `describe`, `select`, `write`, `create` and, on a tag, `apply`.
- **Governance tags in policies.** Creating, applying, removing, reading, changing and deleting tags are Cedar actions. Each warehouse, namespace, table, view and generic table carries its effective governance tags, including inherited ones, as `governance_tags`. For example: `resource.governance_tags.hasTag("pii")`. Keys match the tag name exactly, including case. Column tags are not included.
- **Grant administration for whole subtrees.** Listing and revoking all grants under a warehouse or namespace, and listing everything one user or role holds in a project, are Cedar actions of their own. By default, `manage_grants` and `read_grants` holders may list, and `manage_grants` holders may revoke. Policies see what a request covers in `context.subtree`. The decision is made once, on the named warehouse or namespace; a `forbid` on objects below does not narrow it. A bulk revoke cannot be undone.
- **Roles under Cedar.** Roles managed in Lakekeeper (provider `lakekeeper`) can be created, renamed, given members and deleted, in the console and through the API. By default, project `manage_grants` holders manage them, and `describe`, `create`, `manage` and `manage_grants` holders can read them. Roles from identity and role providers can be deleted too, for example after their group was removed from the directory. A role that still holds grants needs `force=true`.
- **Moves are checked at both ends.** Moving a table, view or generic table into another namespace checks `MoveTable`, `MoveView` or `MoveGenericTable` on the object and `AcceptMovedTabularInNamespace` on the new namespace. Policies see the new namespace in `context.destination` and the old one in `context.source`. New predefined policies allow moves, including moving and renaming namespaces: `manage` and `manage_grants` on the object to move it out, and `create` or `manage` on the new namespace or warehouse to move it in.
- **Server grants.** Grants on the server go to users; a grant to a role is refused with `400 ServerGrantToRole`. No predefined policy decides server actions: load the five server-grant `permit`s from the "Server Actions" section of the schema to make server grants take effect. See [Server grants](https://docs.lakekeeper.io/docs/0.14.x/authorization-cedar/#server-grants).
- **Server policy check.** When the server loads its policies, it refuses a policy that would stop fewer users at a server action, and the log shows the fix. The check does not run when users and roles are managed externally.
- **Batch checks name the deciding policies.** Under Cedar, each result of `POST /management/v1/action/batch-check` lists the policies that decided it in `determined-by`.
- **Server policy endpoints renamed.** `policy-sources` and `policy-list` are now `server-policy-sources` and `server-policy-list`. The old paths still work but are deprecated. Each source now reports its `tier`.
- **Console v0.23.0.** Each project's and warehouse's Cedar policies and predefined policies can be managed in the console. With Cedar, grants under a warehouse or namespace can be listed and revoked in bulk. On a table's Tasks tab, snapshot expiration and orphan-file removal each have "Run now" and settings buttons. The console also includes everything in the Lakekeeper console v0.26.0.
- **Console settings.** `LAKEKEEPER__UI__STORAGE_PROVIDERS_ORDER` and `LAKEKEEPER__UI__STORAGE_PROVIDERS_HIDDEN` order and hide the storage types offered when adding a warehouse. `LAKEKEEPER__UI__SUPPORT_DOCS_URL`, `LAKEKEEPER__UI__SUPPORT_ISSUE_URL` and `LAKEKEEPER__UI__SUPPORT_CONTACT_URL` set the links of the help menu.
- **Admission gate metrics.** `lakekeeper_admission_enforce_call_duration_seconds`, `lakekeeper_admission_enforce_decisions_total` and `lakekeeper_admission_enforce_fail_closed_total` show how the gate performs. Its cache reports as `cache_type="admission_enforce"`.
- **New commands.** `openfga reconcile` repairs OpenFGA's hierarchy tuples from the catalog; OpenFGA users can now run the reconcile recommended in 0.13.6. `reopen-bootstrap --yes` allows bootstrapping again, for example after a misconfigured first bootstrap. It changes no data and no policies, but until bootstrap runs again, the next authenticated caller of `/management/v1/bootstrap` becomes the initial admin.

### Bug Fixes

- **Role providers: stored roles are used only when every unreachable provider has synced the user.** Before, a policy that forbids a group from a missing provider could be skipped.
- **Role providers: fewer directory calls.** A user's groups are fetched once and reused in every project until they are due for a sync. A user without groups keeps working while the provider is down.
- **Persisted token roles apply in every project.** While a user is active, their token roles are stored again at most every two minutes. A decision about a user who has made no request in the project, such as a DEFINER view's owner, uses their newest stored roles from any project.
- **Cedar: the project list decides each project with the caller's roles in that project.** Acting as a role lists only that role's project.
- **Looking up another user's groups writes nothing.** Permission checks about another user and DEFINER views no longer create users or roles. A caller who may not make such a check is refused before that user's groups are looked up.
- **Directory role providers resolve only the users of their identity provider.** Before, with several identity providers, a user could receive the groups of another provider's user with the same subject.
- **Roles named after an identity provider that supplies no roles can be renamed and deleted.** `managed-role-providers` in `GET /management/v1/info` lists only the configured role providers.
- **Snapshot expiration deletes only files inside the table location.** Other files are left in place and counted in the task result's `skipped-outside-table-location-count`, so tables that write data outside their location, for example with `write.data.path`, keep their expired data files. A snapshot history that loops no longer hangs expiration, and a branch or tag moved onto an expiring snapshot during expiration is kept.
- **Commands behave as in Lakekeeper.** `healthcheck` without flags checks `/health` (before, it checked nothing), `wait-for-db` reads the migration state from the primary database, `migrate` runs the steps after the migration, and `serve` reports the `lakekeeper_catalog_pg_pool_*` metrics.
- **Cedar: a request the authorizer cannot build returns `500` instead of `503`.** Retrying does not help; the details are in the server log.

### Breaking Changes

- **Cedar: schema and entity files.** With `LAKEKEEPER__CEDAR__SCHEMA_FILE`, your schema must declare everything the shipped schema declares, including the new actions, privileges and `principal_privileges`. The server names missing attributes and privileges and does not start until you add them. External entity files are checked against the schema. With externally managed identity, they must declare every user and role that a request names.
- **Cedar: groups and Lakekeeper roles at server actions.** At server actions, a user carries their groups from directories, tokens and admission gates in `principal.project_roles` and `principal.global_role_ids`. `global_role_ids` holds only such groups, never Lakekeeper roles. A `Lakekeeper::Role::"…"` id matches no user at server actions; name a group with the flat form, `principal.project_roles.contains({provider_id: "ldap", source_id: "<group>"})`. See [Role scope at server actions](https://docs.lakekeeper.io/docs/0.14.x/authorization-cedar/#role-scope-at-server-actions).
- **Cedar: act as yourself at server actions.** With `x-assume-role`, every server action is denied, except on your own user.
- **Cedar: new action groups.** Reading the server's policy sources moved from `ServerActions` to `ServerCedarPolicyActions` and `CedarPolicyReadActions`. Checking what another user or role may do (`Introspect<X>Authorization`) moved into the grant-reading groups. Name the new groups where you allow these actions.
- **Cedar: reading a user's role assignments is decided on the project.** `ReadUserRoleAssignments` is checked on the project it reads, and the predefined `describe`, `create`, `manage` and `manage_grants` policies allow it. It is in no action group, so name it to permit or forbid it. A policy that names it on the server no longer validates, and the server does not start with it.
- **Cedar: `CreateRole` and `DeleteRole` permits take effect.** A policy that permits `CreateRole`, also through `ProjectGrantActions`, now lets its holder create `lakekeeper` roles, and a permit of `DeleteRole` on a provider's role lets its holder delete it. Members of `lakekeeper` and `system` roles now count in `principal.project_roles`; in 0.13 these memberships had no effect.
- **Cedar: a request must name the project its resources are in.** A warehouse outside the project in `x-project-id` returns an error, and one request cannot span two projects.
- **Cedar: `IncludeProjectInList` decides the project list,** not `GetProjectMetadata`. The predefined policies allow both.
- **Cedar: moving into another namespace needs move permission.** `RenameTable`, `RenameView` and `RenameGenericTable` no longer allow a move into another namespace on their own. Name `MoveTable`, `MoveView` or `MoveGenericTable` and `AcceptMovedTabularInNamespace` in your policies; action groups do not include them. The predefined policies allow a move with `manage` and `manage_grants` on the object.
- **Cedar: registering a table also checks `GetNamespaceMetadata`, and cancelling a soft-deletion task checks `UndropTable`, `UndropView` or `UndropGenericTable`.** The predefined policies allow both.
- **Persisted token roles: a token without roles clears the user's stored roles in that project.** Entra ID leaves out `groups` for users in more than 200 groups; serve these users through the Entra ID role provider before you upgrade.
- **Stricter startup checks.** The server does not start when:
    - `LAKEKEEPER__AUTHZ_BACKEND` has an unknown value. Before, the server ran without authorization.
    - No authenticator is configured, unless `LAKEKEEPER__INSECURE_ALLOW_UNAUTHENTICATED=true` is set.
    - With Cedar, the server authenticates two or more identity providers (Kubernetes authentication counts) and an LDAP, Entra ID or Okta role provider has no `idp_ids`, or `idp_ids` names a provider the server does not authenticate.
    - A role provider is named `lakekeeper` or `system`.
    - An admission gate's `idp_id` names no configured provider, its configuration has an unknown key, or it still sets `auth` (`forward_caller_token`). Authenticate to your endpoint with `headers`.
    - A role provider's `sync_interval_secs` is above 31536000000 (1000 years).
- **Admission gates run before the `x-assume-role` check.** Roles that a gate grants count when deciding `x-assume-role`.
- **Only instance admins add or remove members of `system` roles,** with every authorizer. These roles no longer accept roles as members; roles that are already members can still be removed ([lakekeeper#1991](https://github.com/lakekeeper/lakekeeper/pull/1991)).
- **Audit log format.** Existing parsers of the audit log must be updated. See Audit log format below.

### Upgrade Notes

- **Plan downtime and back up the database.** Stop all 0.13 servers; with Helm, scale the deployment to zero before `helm upgrade`. With OpenFGA, remove `LAKEKEEPER__OPENFGA__AUTHORIZATION_MODEL_VERSION`. Run `migrate` to completion, then start 0.14. The migration can take several minutes on large catalogs. First run the query from "Tables or views that would share a location stop the migration" in the [Lakekeeper upgrade notes](https://docs.lakekeeper.io/about/release-notes/). Afterwards, 0.13 servers cannot run against the database; to go back, restore the backup.
- **Grants decide access as soon as they are made.** The predefined policies are on by default. To choose them per project or warehouse before they apply, start with `LAKEKEEPER__CEDAR__PREDEFINED_POLICIES_ENABLED=false`, switch off what you don't want in each scope, then set it back to `true`.
- **Bulk revoke is on by default.** `manage_grants` holders can revoke all grants under a warehouse or namespace, their own included, and a revoke cannot be undone; break-glass does not restore grants. Switch off `predefined-grants-warehouse-grant-admin-subtree-revoke` and `predefined-grants-namespace-grant-admin-subtree-revoke` where you don't want this.
- **Rewrite server-action `permit`s before you upgrade.** A `Lakekeeper::Role` id matches no user at server actions. Name groups with the flat form, or give server access to users. If the server refuses a policy, the log shows the fix. If nobody can reach server administration, sign in as one of `LAKEKEEPER__INSTANCE_ADMINS`.
- **Multi-project Cedar deployments: review past decisions on requests without `x-project-id`.** Before, such a request could be decided with another project's roles.
- **Externally managed identity: grant to users, not roles.** A grant to a role does not apply, because role membership lives in your entity file.
- **Directory outages.** A caller that has never made a request in a project, such as a provisioning bot, has no stored groups, so its server requests fail while the directory is down. A DEFINER view's owner is decided with the groups from their last own request. Let such accounts make a request now and then.
- **OPA bridge:** if you use it, deploy its policies from this release together with the server. Renames into another schema now also check `move` and `accept_moved_tabular`.

### Upstream Lakekeeper changes (up to Lakekeeper v0.14.0)

This release includes Lakekeeper v0.13.6 and v0.14.0. See the [Lakekeeper release notes](https://docs.lakekeeper.io/about/release-notes/) for all changes. Most important for Plus:

- **Grants API, governance tags, and moving and renaming namespaces** ([lakekeeper#1945](https://github.com/lakekeeper/lakekeeper/pull/1945), [lakekeeper#1914](https://github.com/lakekeeper/lakekeeper/pull/1914), [lakekeeper#1950](https://github.com/lakekeeper/lakekeeper/pull/1950)).
- **Storage validation,** with warnings for STACKIT buckets that other credentials groups can reach and for CORS settings that block the console ([lakekeeper#1936](https://github.com/lakekeeper/lakekeeper/pull/1936), [lakekeeper#2050](https://github.com/lakekeeper/lakekeeper/pull/2050), [lakekeeper#2049](https://github.com/lakekeeper/lakekeeper/pull/2049)).
- **STACKIT storage type with data platform storage, and Alibaba Cloud OSS** ([lakekeeper#1978](https://github.com/lakekeeper/lakekeeper/pull/1978), [lakekeeper#2045](https://github.com/lakekeeper/lakekeeper/pull/2045), [lakekeeper#1894](https://github.com/lakekeeper/lakekeeper/pull/1894)).
- **Audit log: user emails and role sources.** Emails are off by default, role sources on ([lakekeeper#2091](https://github.com/lakekeeper/lakekeeper/pull/2091)).
- **Required token claims, and stricter OpenID settings** that can stop the server from starting ([lakekeeper#2001](https://github.com/lakekeeper/lakekeeper/pull/2001)).
- **Task queue renamed.** Rename `LAKEKEEPER__TASK_TABULAR_EXPIRATION_WORKERS` to `LAKEKEEPER__TASK_SOFT_DELETION_WORKERS` before you upgrade; the old name stops the server from starting ([lakekeeper#1881](https://github.com/lakekeeper/lakekeeper/pull/1881)).
- **Requests may be up to 32 MiB by default** ([lakekeeper#1974](https://github.com/lakekeeper/lakekeeper/pull/1974)).
- **Role members cache removed.** The `LAKEKEEPER__CACHE__ROLE_MEMBERS__*` settings have no effect ([lakekeeper#2072](https://github.com/lakekeeper/lakekeeper/pull/2072)).
- **Catalog requests without `x-project-id` use their warehouse's project, and token roles work without a project.** Under Cedar, token and admission-gate roles are in every project's `principal.project_roles` ([lakekeeper#2069](https://github.com/lakekeeper/lakekeeper/pull/2069), [lakekeeper#2071](https://github.com/lakekeeper/lakekeeper/pull/2071)).
- **OpenFGA:** `migrate` installs model v4.13. Revoking needs `manage_grants`. Moving a table, view or generic table into another namespace needs `manage_grants` and `modify` on it. Moves and managed-access changes are not possible under an assumed role. Tools that query OpenFGA directly must use the new `_effective` relations, such as `select_effective` ([lakekeeper#1991](https://github.com/lakekeeper/lakekeeper/pull/1991), [lakekeeper#2029](https://github.com/lakekeeper/lakekeeper/pull/2029), [lakekeeper#2095](https://github.com/lakekeeper/lakekeeper/pull/2095)).
- **Cancelling a soft-deletion task needs `undrop`** with every authorizer ([lakekeeper#2095](https://github.com/lakekeeper/lakekeeper/pull/2095)).
- **Batch check: `update_storage_credential` is removed** and returns `422`; check `update_storage` instead ([lakekeeper#1936](https://github.com/lakekeeper/lakekeeper/pull/1936)).
- **Smaller changes that can affect clients:** reusing an `Idempotency-Key` for a different operation returns `400`, `referenced-by` names at most 10 views by default, tables can no longer set `write.object-storage.path` or `write.folder-storage.path`, and unreachable storage fails with `412 StorageProbeTimeout` ([lakekeeper#1959](https://github.com/lakekeeper/lakekeeper/pull/1959), [lakekeeper#1980](https://github.com/lakekeeper/lakekeeper/pull/1980), [lakekeeper#2044](https://github.com/lakekeeper/lakekeeper/pull/2044), [lakekeeper#2047](https://github.com/lakekeeper/lakekeeper/pull/2047)).
- **All fixes of Lakekeeper v0.13.6,** which Plus v0.13.6 did not include yet.

### Audit log format

**Breaking changes** — an existing parser must be updated:

- **`context.roles` on `resolve_roles` records with `outcome: "roles_resolved"` lists role objects instead of strings.**

    ```text
    before  "roles": ["corporate-ldap~engineering", "oidc~viewers"]
    after   "roles": [{"role": "1f7b…", "provider_id": "corporate-ldap", "source_id": "engineering"}, {"provider_id": "oidc", "source_id": "viewers"}]
    ```

    Each object has the keys Lakekeeper's records name a role with. `role` is the role's catalog id, absent for a role without a catalog row for this answer: token roles, and roles resolved above projects. `source_id` is absent when `LAKEKEEPER__AUDIT__TRACING__INCLUDE_ROLE_SOURCE_ID=false`. The list stays ordered by provider and source id.

    **What to do:** read each entry of `roles` as an object. To rebuild the old string, join `provider_id` and `source_id` with `~`.

- Lakekeeper's part of each record carries `audit_format` **1.0**, its first version. Lakekeeper's release notes list what changed: https://docs.lakekeeper.io/about/release-notes/

**Additions** — an existing parser keeps working:

- **A new record: `operation: "admission_enforce_check"` with `outcome: "role_withheld"`.**

    The external-enforce gate writes it when the control plane refuses one role but still admits the request. `context` carries `gate`, `check`, `role` and `cache_ttl_secs`. `role` names the admission role by `provider_id` and `source_id`, as other records name a role; `source_id` is absent when `LAKEKEEPER__AUDIT__TRACING__INCLUDE_ROLE_SOURCE_ID=false`. The gate caches each answer, so there is one record per answer, not per request.

    **What to do:** nothing, unless you want to see withheld roles. Join it with Lakekeeper's `admission_decided` records on `actor` or `request_id`.

- **Authorization records of Cedar policy requests carry what the request asked to change and how it ended.**

    New `context` keys:

    - `changes` on `toggle_predefined_policy`: the predefined-policy switches the request asked for, as `{"changes": [{"id": …, "enabled": …}], "truncated": …}`.
    - On `apply_cedar_policies`:
      - `dry_run`: `true` when the call only reports what it would do.
      - `break_glass`: the reason the caller stated, when it stated one.
      - `scope_changes`: the writes and deletes the request asked for, with `writes`, `deletes`, `replace`, `names` and `truncated`.
      - `applied`: what the apply left behind, with `created`, `updated`, `deleted`, `deleted_names`, `truncated` and `scope-version`.
      - `apply_outcome`: `refused` or `failed`, absent when the apply went through. `apply_error` says why.

    A list is cut at a fixed length, and `truncated` is `true` when it was.

    **What to do:** nothing, unless you audit policy changes. Check `truncated` before treating a list as complete.

**Also worth knowing** — the format itself did not change:

- **Every record this product writes carries the new top-level fields of every audit record: `record_type`, `emitters`, `time`, and `request_id` when a request caused it.**

    ```text
    "record_type": "operation",
    "emitters": {"lakekeeper_plus": "1.0"},
    "time": "2026-03-14T09:26:53.589793Z"
    ```

    The role-provider records (`resolve_roles`, `ldap_resolve_roles`, `cached_role_provider`) carry no `request_id`. An authorization record that carries one of this product's action names or `context` keys also has the key `lakekeeper_plus` in `emitters`.

    Lakekeeper's release notes describe these fields. This product's version is the value of `.emitters.lakekeeper_plus`.

    **What to do:** read this product's version from `.emitters.lakekeeper_plus`.

- **This product publishes a JSON Schema for what it contributes to the audit log: `audit/schema-plus.json` in the documentation, beside Lakekeeper's `audit/schema.json`.**

    It lists this product's `operation` and `outcome` values, the `context` of each of its records, the `context` keys it adds to authorization records, and its action names. Lakekeeper's schema describes the record shapes. A record whose `emitters` has the key `lakekeeper_plus` is governed by both.

    **What to do:** nothing. Validate with both schemas, or generate a parser from them.

Records from v0.14.0 carry `emitters.lakekeeper_plus` **1.0** (the first version).


## v0.13.6 (2026-09-18)

_Based on Lakekeeper OSS v0.13.5._

### Bug Fixes
- **Okta role provider: people who sign in with an email address now resolve to their groups.** The user identifier was escaped for a query string rather than for a URL path, so the `@` in a login like `alice@example.com` reached Okta as `%40` and every such lookup came back as a bare `400`. Deployments whose OIDC subject is a raw Okta user id were unaffected, which is why service principals kept working while people did not.
- **Okta role provider: a retried token request no longer replays its client assertion.** Okta accepts each assertion once, so a DPoP nonce challenge or a throttled token request could fail with `invalid_client`. Every attempt is now signed afresh.
- **Security: TLS handshake handling (RUSTSEC-2026-0285).** `rustls` is updated to 0.23.45, which rejects handshake messages spanning a key change instead of accepting them. The handshake transcript stayed authenticated, so this could not be used to alter or complete a handshake.

### Upgrade Notes
- **OpenFGA deployments that have renamed a table, view or generic table across namespaces should run `lakekeeper openfga reconcile --mode add-and-delete-drift` once after upgrading.** The upstream fix below stops the leak but cannot remove a permission edge already recorded, and the default `add-missing` mode does not repair it.

### Upstream Lakekeeper changes (bump to v0.13.5)
- **GovCloud, China and ISO region support.** Vended-credential policies now carry the correct ARN partition, derived automatically from the region, endpoint or role ARN, so `AssumeRole` succeeds for buckets outside the commercial partition ([lakekeeper#1928](https://github.com/lakekeeper/lakekeeper/pull/1928)).
- **S3 request signing for generic tables** ([lakekeeper#1910](https://github.com/lakekeeper/lakekeeper/pull/1910)).
- Fixed a permissions leak: a table, view or generic table renamed into another namespace kept inheriting grants from the namespace it left ([lakekeeper#2013](https://github.com/lakekeeper/lakekeeper/pull/2013)).
- Renaming a table onto a name that is already taken now returns `409 Conflict` instead of `404 Not Found`, matching the Iceberg REST spec, and renaming onto a soft-deleted name succeeds ([lakekeeper#1955](https://github.com/lakekeeper/lakekeeper/pull/1955)).

## v0.13.5 (2026-08-26)

_Based on Lakekeeper OSS v0.13.3._

### Highlights
- **Improved memory behaviour on long-running instances.** Conditions that could, under some circumstances, prevent freed memory from being returned to the OS are addressed.

### Features
- **New memory metrics.** `lakekeeper_jemalloc_*` separates live heap from memory the allocator is holding back; `lakekeeper_http_connections` reports open connections.
- **The table maintenance cache reports what it holds:** `lakekeeper_cache_weighted_bytes{cache_type="table_metadata"}` and `lakekeeper_cache_instances`, with hits and misses in the shared `lakekeeper_cache_*` series.

### Bug Fixes
- **Transparent huge pages could prevent freed memory from being returned to the OS.** On nodes with `THP=always`, the default on common EKS AMIs, resident memory could grow for the life of the process.
- **The manifest cache under-counted its entries**, so under some circumstances its 128 MiB budget did not bind during table maintenance.
- **The UI asset cache keyed on an unvalidated header**, so variants of `x-forwarded-prefix` could each add an entry to a cache with no expiry.
- **Allocator metrics now report on every serving path**, including `LAKEKEEPER__DEBUG__AUTO_SERVE`. Memory behaviour itself was unaffected.

### Upgrade Notes
- **Idle HTTP connections now close after 75 seconds** and carry TCP keepalive probes. Standard Iceberg and S3 clients retry; previously a connection whose peer had vanished could be held for the life of the process.
- **Request headers above 64 KiB are rejected with 431.** Request bodies are unaffected.
- **Shutdown drains for at most 10 seconds** before abandoning connections still open.
- **Unmatched request paths report `endpoint="unmatched"` in metrics** instead of each creating a permanent series. Dashboards that group by raw path lose those values.

## v0.13.4 (2026-08-14)

_Based on Lakekeeper OSS v0.13.3._

### Bug Fixes
- **Audit log fields are emitted as structured JSON.** `actor`, `action`/`actions`, `entity`/`entities`, `authorizations` and `context` were emitted as strings containing escaped pseudo-JSON, so audit pipelines could not read them as objects without decoding each field first. They are now nested JSON objects, as documented. See *Upgrade Notes*.
- **Reduced memory growth from allocator fragmentation.** The server now uses jemalloc as its global allocator. Deployments that saw `container_memory_working_set_bytes` climb steadily without returning to baseline — a glibc malloc fragmentation pattern — should see flatter memory use. The effect depends on workload, and this changes the allocator only.
- **JSON logs no longer carry duplicate span data.** Every line included both a `span` object and a `spans` array with the same content; only `spans` is emitted now. Applies to `lakekeeper-plus` and `lakekeeper-maintenance`.
- **`LAKEKEEPER__DEBUG__LOG_AUTHORIZATION_HEADER` now applies to UI-server routes.** The setting was silently ignored there, so enabling it produced no `authorization` field on those request spans.

### Upgrade Notes
- **Audit log consumers must read objects, not strings.** If your pipeline JSON-decodes the audit fields a second time to get at their contents, that step now fails or double-decodes — read them directly instead. The previous string form parsed only by luck: Rust `Debug` escapes non-ASCII as `\u{1F600}`, which is not valid JSON, so any field carrying such text would have broken the parse outright. Consumers that already tolerate both shapes need no change, and rollout order does not matter for them.

## v0.13.3 (2026-07-24)

_Based on Lakekeeper OSS v0.13.3._

### Highlights
- **Admission-gate roles now take effect.** Roles granted by a `role_granting` admission check are finally evaluated by authorization — completing the external admission gate shipped in v0.13.0, whose granted roles previously never reached Cedar.

### Features
- **Apply admission-gate roles in Cedar authorization.** A new `AdmissionRoleProvider` (an uncached role-provider-chain leaf, mirroring the token role provider) serves the caller's admission-granted roles under their configured provider id; Cedar materialises them as `Role` entities and evaluates policies against them. Plus wires one provider per role-provider id the gate can mint under, so a gate-only deployment still resolves roles.
- **No-Access page for instance-level 403.** An authenticated caller denied access to the instance (e.g. rejected by the admission gate) now lands on a dedicated No-Access page with proper 403 routing, instead of a broken view. (console v0.16.4)


### Upgrade Notes
- **Conflicting role-provider ids now fail fast.** A gate-minted `role_provider_id` that collides with a configured or token role-provider id is rejected at startup with `ExtraProviderIdConflict` — give the gate's minted roles a distinct provider id.
- **Admission roles apply under Cedar only.** With the OpenFGA authorizer, or Cedar in externally-managed mode, admission-granted roles are not applied; the server logs a warning at startup so a misconfiguration is visible.

## v0.13.2 (2026-07-23)

_Based on Lakekeeper OSS v0.13.3._

### Bug Fixes
- **Maintenance page.** Console bump to 0.16.3 (console-components 0.17.2, console-plus-components 0.11.0). Redesigned maintenance date-range filters, suppressed spurious 403 notifications for maintenance tasks, and fixed the Home page chart title.

## v0.13.1 (2026-07-20)

_Based on Lakekeeper OSS v0.13.3._

### Highlights
- **Okta role provider.** Resolve a user's Okta group memberships to Lakekeeper roles via the Okta management API — private-key-JWT client auth with DPoP (RFC 9449) proofs enabled by default.
- **Provider-synced roles are now protected.** Roles owned by a configured role provider (Okta, Entra, LDAP, or token IdP) can no longer be mutated through the management API, so the next provider sync can't silently clobber manual edits.

### Features
- **Okta role provider (with DPoP).** Group memberships resolved via `GET /users/{id}/groups` (Link-header pagination, keyed by immutable group id). OAuth2 client-credentials + private-key-JWT (JWK or PEM key); DPoP on by default with ephemeral P-256 proofs and nonce challenge/replay handling — opt out to Bearer. Requires the `okta.users.read` scope; wrapped in the shared role cache. See the Okta role-provider docs.
- **Managed-role write protection.** The Cedar authorizer now reports its configured provider namespaces to the management-API guard, so create / update / delete / source-system rebind / member (un)assignment on a provider-owned role is rejected with `400 ManagedRoleImmutable`. Native `lakekeeper` roles and the reserved `system` namespace are never included, so API-native and catalog-managed roles stay writable. Active only when a role provider is configured.
- **Post-logout redirect controls.** Two new UI env vars — `LAKEKEEPER__UI__OPENID_POST_LOGOUT_REDIRECT_URL` and `LAKEKEEPER__UI__OPENID_POST_LOGOUT_REDIRECT_DISABLED`.
- **Static-asset caching in the UI server.** Per-class `Cache-Control` plus weak `ETag`/`304` on bundled assets: content-hashed `assets/*` are cached immutably and the DuckDB WASM is no longer re-downloaded on every load, while `index.html` stays uncached so runtime config placeholders remain fresh.

### Bug Fixes
- **Maintenance page for warehouse-only permissions.** Console bump to 0.16.1 (console-components 0.17.1) fixes the maintenance view for users who hold only warehouse-level permissions.

### Upgrade Notes
- **Provider-role edits now return `400 ManagedRoleImmutable`.** If you previously edited provider-synced roles (Okta/Entra/LDAP/token) through the management API, those calls are now rejected — such edits were overwritten by the next sync anyway. Manage those roles at the source. No migration.
- **Building Plus from source:** the Kubernetes client stack moved to k8s-openapi 0.28 / kube 4.0 (pulled in by the upstream limes 0.4.2 bump). Prebuilt binaries and images are unaffected.

### Upstream Lakekeeper changes (up to Lakekeeper v0.13.3)
Notable for Plus users:
- **Configurable Kubernetes subject source.** `LAKEKEEPER__KUBERNETES_AUTHENTICATION_SUBJECT_SOURCE=username` derives a service account's Lakekeeper user id from `system:serviceaccount:<namespace>:<name>` (stable across clusters) instead of the per-cluster `uid` (default, unchanged), so Kubernetes roles and instance admins can be pre-provisioned ([lakekeeper#1899](https://github.com/lakekeeper/lakekeeper/pull/1899)).
- **Reject role writes in provider-managed namespaces** — the upstream API guard behind the managed-role protection above ([lakekeeper#1891](https://github.com/lakekeeper/lakekeeper/pull/1891)).
- Stop evaluating a discarded `Select` on target-view load, avoiding a spurious authorization check ([lakekeeper#1886](https://github.com/lakekeeper/lakekeeper/pull/1886)).

## v0.13.0 (2026-07-02)

_Based on Lakekeeper OSS v0.13.1._

### Highlights
- **Microsoft Entra ID (Graph) role provider.** Resolve a user's transitive Entra group memberships into Lakekeeper roles — with secret, certificate, managed-identity, and workload-identity credentials, sovereign-cloud support, and built-in throttling/retry.
- **External admission gate.** A new post-authentication seam can ask your control plane whether an already-authenticated caller may use this instance — for IdPs that issue broad, non-instance-scoped tokens — and contribute the caller's resolved roles.
- **Console overhaul.** The bundled UI jumps to v0.13.2: a Files/storage explorer with in-browser Parquet/Avro/CSV preview, per-entity action menus, datasets as a first-class entity, redesigned view and table-health pages, a Role Members tab, and an enterprise usage-report builder.

### Features
- **Entra ID / Microsoft Graph role provider.** Paged `transitiveMemberOf` resolution; credential methods secret / certificate / managed-identity / workload-identity; public, US-gov, and China clouds; retries on 429 (honoring `Retry-After`) and transient 5xx.
- **AD range retrieval for LDAP attribute-mode groups.** Active Directory returns >1500 group values under a ranged `memberOf;range=…` key; attribute mode now walks the range windows, so users in many groups are no longer silently truncated. OpenLDAP/389-DS behavior is unchanged.
- **External enforce-endpoint admission gate** (`lakekeeper-admission-enforce`). Configurable named checks POST to your endpoint; the HTTP status is the decision (2xx allows and grants the check's role, `403` denies, anything else fails closed with `503` + `Retry-After`). Allow and deny are both cached; caller bearer-token relay is opt-in and never logged.
- **Persist OIDC token roles for DEFINER views.** Opt-in via `LAKEKEEPER__ROLE_PROVIDER_CHAIN__PERSIST_TOKEN_ROLES` (default off) — mirrors a user's OIDC-token roles into the catalog so authorization can evaluate them when the user isn't the live caller, e.g. a DEFINER view running as its owner. Write-gated; no migration.
- **Generic-table parity in Cedar authorization.** Non-Iceberg generic tables (e.g. Lance, Delta) now resolve and authorize through the Cedar surface exactly like tables and views.
- **Destructive-delete context for Cedar policies.** `force` / `purge` / `recursive` and a warehouse `soft_delete_enabled` attribute are now in the Cedar request context, so a policy can forbid hard deletes that would bypass configured soft-deletion.
- **Role-membership actions in Cedar.** The new manage/read role-assignment actions map to dedicated fine-grained Cedar actions, so policy authors control their bundling.
- **Destination-aware role source-system rebind.** A dedicated `update_source_system` Cedar action exposes the target provider/source, so rebinds can be gated by destination — something the coarse upstream OpenFGA relation cannot express.
- **Per-decision policy trace in authorization audit.** Audit events and the `/check` endpoint now record which Cedar policies determined each allow/deny outcome.
- **Schedule maintenance directly.** `expire_snapshots` and `remove_orphan_files` can be triggered per table via the task-queue schedule endpoint, without waiting for a commit hook.

### Bug Fixes
- **Corrupt-manifest orphan-files task no longer retries forever.** A permanent failure (e.g. a corrupt Avro manifest) is now classified permanent and not requeued, instead of failing silently and re-running every day. Maintenance workers (`remove_orphan_files`, `expire_snapshots`) also persist a readable failure reason, surfaced in the task-details API — no server-log access required.

### Breaking Changes
- **Default storage layout is now flat** (inherited from upstream Lakekeeper 0.13): new namespaces use `<base>/<tabular-uuid>` instead of nesting tabulars under the parent-namespace UUID. Not retroactive — existing namespaces and paths are unchanged — so explicitly configure the full-hierarchy layout if you need the old behavior for new namespaces ([lakekeeper#1853](https://github.com/lakekeeper/lakekeeper/pull/1853)).

### Upgrade Notes
- **Encrypted tables are skipped by maintenance.** `expire_snapshots` and `remove_orphan_files` now detect Iceberg native encryption (format v3) via the immutable `encryption.key-id` property and skip such tables — Lakekeeper cannot read their encrypted manifests, and processing anyway risked deleting live data. Manually scheduling either task on an encrypted table returns `400`.
- **Downgrade protection** (upstream): `serve` refuses to start against a database already migrated by a newer binary. After a rollback, start the older binary with `serve --force-start`, accepting the schema-incompatibility risk ([lakekeeper#1861](https://github.com/lakekeeper/lakekeeper/pull/1861)).
- **Docker base images** moved from Debian 12 (bookworm) to Debian 13 (trixie).
- **Building Plus from source:** the catalog Postgres backend and the NATS/Kafka event backends are now separate upstream crates (`lakekeeper-storage-postgres`, `lakekeeper-events-nats`, `lakekeeper-events-kafka`). Prebuilt binaries and images are unaffected ([lakekeeper#1812](https://github.com/lakekeeper/lakekeeper/pull/1812), [lakekeeper#1814](https://github.com/lakekeeper/lakekeeper/pull/1814)).

### Upstream Lakekeeper changes (up to Lakekeeper v0.13.1)
Rolls up OSS **v0.12.4**, **v0.13.0**, and **v0.13.1** (full list in the [Lakekeeper release notes](https://docs.lakekeeper.io/about/release-notes/)). Notable for Plus users:
- **Generic Table API** — register non-Iceberg tables (Lance, Delta) as first-class generic tables with credential vending and full authorization ([lakekeeper#1673](https://github.com/lakekeeper/lakekeeper/pull/1673), [lakekeeper#1813](https://github.com/lakekeeper/lakekeeper/pull/1813)); surfaced in Plus through the Cedar generic-table parity above.
- **Operator-owned warehouses.** A `managed_by` marker locks warehouse spec mutations (delete, rename, (de)activate, storage profile, protection, format-version policy) to instance admins ([lakekeeper#1828](https://github.com/lakekeeper/lakekeeper/pull/1828)).
- **Authorizer-independent role-membership API** — one management surface to list/add/remove a role's members regardless of the configured authorizer ([lakekeeper#1829](https://github.com/lakekeeper/lakekeeper/pull/1829)).
- **Multiple OIDC providers** at once via `LAKEKEEPER__OPENID_PROVIDERS` (e.g. Okta for users + a cloud issuer for service accounts) ([lakekeeper#1760](https://github.com/lakekeeper/lakekeeper/pull/1760)).
- **Microsoft OneLake / Fabric storage** profile, including workspace private-link endpoints ([lakekeeper#1852](https://github.com/lakekeeper/lakekeeper/pull/1852)).
- **Per-warehouse table format-version policy** — allowed Iceberg format versions and an optional default per warehouse ([lakekeeper#1786](https://github.com/lakekeeper/lakekeeper/pull/1786)).
- **Customer-managed KMS encryption** — warehouses with `aws-kms-key-arn` advertise `s3.sse.type=kms`, so vended-credential writes use your KMS key ([lakekeeper#1847](https://github.com/lakekeeper/lakekeeper/pull/1847)).
- **Cache hardening for large fleets** — single-flight read-throughs and TTL jitter cut thundering-herd load on the database and on rate-limited STS/SAS endpoints ([lakekeeper#1833](https://github.com/lakekeeper/lakekeeper/pull/1833), [lakekeeper#1837](https://github.com/lakekeeper/lakekeeper/pull/1837)).
- `/health` now returns `503` (not `200`) when unhealthy, so Kubernetes HTTP probes detect it ([lakekeeper#1802](https://github.com/lakekeeper/lakekeeper/pull/1802)).
- Postgres migration locks are transaction-scoped, so a failed migration can't leak an advisory lock that blocks future migrations ([lakekeeper#1790](https://github.com/lakekeeper/lakekeeper/pull/1790)).

## v0.12.2 (2026-05-26)

### Highlights
- Orphan-file cleanup now schedules itself adaptively per table — running more often where files accumulate and backing off where they don't — with a new dry-run mode.
- The LDAP role provider can resolve groups via subtree **Search** and conditional **Branching**, not just the `memberOf` attribute.

### Features
- **Adaptive orphan-file scheduling.** The remove-orphan-files worker now self-tunes its cadence based on how fast reclaimable data builds up, and adds a dry-run mode that reports what it *would* delete. Orphan removal is now opt-in via `enable-remove-orphan-files`; default retention raised from 3 to 7 days. See the Table Maintenance docs for full config.
- **LDAP group resolution modes.** Resolve group memberships via `Search` (paged subtree) or `Branching` (per-user-DN rules) in addition to the `memberOf` attribute; the resolution mode is recorded in audit logs.
- **Build metadata in Server Info.** Server Info now reports Lakekeeper, Enterprise, and Console versions and commit SHAs, so deployed builds are easy to identify.

### Breaking Changes
- The orphan-files task-queue API was renamed `remove_orphaned_files` → `remove_orphan_files` (paths, config schemas, and worker/enable fields).

### Upgrade Notes
- Update any automation/IaC to the new `remove_orphan_files` task-queue path and schema names.
- Orphan removal is now **opt-in** (it ran by default in 0.12.1): set `enable-remove-orphan-files=true` to keep it active. Default retention is now 7 days.

### Upstream Lakekeeper changes (bump to v0.12.3)
- Core and extension database migrations now apply atomically — no partial-migration state ([lakekeeper#1768](https://github.com/lakekeeper/lakekeeper/pull/1768)).
- Fixed table property removal being lost when no properties remained ([lakekeeper#1767](https://github.com/lakekeeper/lakekeeper/pull/1767)).
- New read-only maintenance mode for the server ([lakekeeper#1765](https://github.com/lakekeeper/lakekeeper/pull/1765)).
- Users may now share an email address — the unique-email constraint was dropped ([lakekeeper#1755](https://github.com/lakekeeper/lakekeeper/pull/1755)).
- New `LAKEKEEPER__UI__ENABLE_SURVEYS` flag to opt out of in-console surveys ([lakekeeper#1750](https://github.com/lakekeeper/lakekeeper/pull/1750)).

## v0.12.1 (2026-05-10)

### Features
- **Remove Orphan Files.** New maintenance capability that reclaims storage by deleting data, manifest, and metadata files no longer referenced by any snapshot — available as a server background worker and as a `remove-orphan-files` subcommand (with dry-run). Enabled by default; set `LAKEKEEPER__TASK_REMOVE_ORPHANED_FILES_WORKERS=0` to disable. Respects `gc.enabled` and per-table opt-out properties. See the Table Maintenance docs for full config.
- **Bounded orphan-files runtime.** Cap how long a single orphan-files run may take with a configurable max run time.

### Bug Fixes
- Warehouse rename no longer leaves a stale name in the UI table preview (bundled UI 0.7.12).

### Upgrade Notes
- The orphan-files worker is **enabled by default** (2 workers); set `LAKEKEEPER__TASK_REMOVE_ORPHANED_FILES_WORKERS=0` to disable. By default it only deletes files older than 3 days and honors `gc.enabled` / per-table opt-out.

### Upstream Lakekeeper changes (bump to v0.12.2)
- OpenFGA: rebuild/reconcile authorization tuples from the catalog, and support switching an existing server to OpenFGA ([lakekeeper#1731](https://github.com/lakekeeper/lakekeeper/pull/1731), [lakekeeper#1733](https://github.com/lakekeeper/lakekeeper/pull/1733)).
- OPA Trino batch authorization gains a broad-access fast path for warehouses/namespaces ([lakekeeper#1727](https://github.com/lakekeeper/lakekeeper/pull/1727)).
- Storage: dropped the opendal dependency and now validates vended credentials via `lakekeeper_io` ([lakekeeper#1737](https://github.com/lakekeeper/lakekeeper/pull/1737)).
- ADLS fixes: correct SAS-token key removal and `%`-encoding in blob names ([lakekeeper#1746](https://github.com/lakekeeper/lakekeeper/pull/1746)).

## v0.12.0 (2026-04-21)

### Highlights
- **Cedar authorization matured** into a configurable, inspectable system: derive user attributes from identity fields, reference roles by global ID, and use a new resolve-entities API + Console tabs to see exactly what drives a decision.
- **Role providers** resolve user roles from external sources — including LDAP groups and table properties — with caching, metrics, and audit.
- **Console**: a visual Cedar Policy Builder (beta) with a Cedar-aware editor, authorization-inspection tabs, and new statistics dashboards.
- **Container images now default to `ubi10`** (breaking — see below).

### Features
- **Cedar user identity derivations.** Extract attributes from identity fields with named-capture regex rules (optional `lowercase`/`uppercase` transform) and match policies on the derived values.
- **Global role IDs in policies.** Reference provider-scoped global role IDs as Cedar property values, and use short-form roles without a default provider.
- **Resolve-entities API.** `POST /management/v1/permissions/cedar/resolve-entities` returns the Cedar entities for any resource — for debugging why a decision was reached.
- **SelectView action.** Adds `select` / `SelectView` (and `grant_select`) for views, aligning with the upstream data-plane authorization split.
- **Role-provider subsystem.** LDAP Group Provider + token-provider chain, roles parsed from table properties, caching with stale-fallback and metrics, and an opt-in audit event for resolved roles — wired into the Cedar authorizer. Configure via `ROLE_PROVIDER_FILE` (TOML), overridable per-field by env vars.
- **Richer permission-introspection audit.** `introspect_permissions` logs now include the inner check tuples and their individual decisions.
- **Console: Cedar Policy Builder (beta).** Visual editor/builder with a CodeMirror Cedar editor (highlighting, autocomplete, inline diagnostics, format/validate via cedar-wasm) and live Evaluate.
- **Console: authorization inspection + dashboards.** Tabs for entity/policy sources, schema, and resolve-entities; new Home and Warehouse statistics dashboards; storage-layout configuration.

### Bug Fixes
- Cedar: correctness fixes around short-form role tags, per-request provider-ID derivation, Role subjects, and resolve-entities server gating.
- Maintenance: expire-snapshots now removes statistics / partition-statistics from metadata, avoiding dangling references to deleted files.
- TLS: added webpki and native root certs to the S3 client and UBI images, fixing handshake failures in some environments.
- Console: correct handling of sub-namespaces containing dots; per-tab 403 keeps the navigation rail visible.

### Breaking Changes
- Container images now default to **`ubi10`**; the `ubi9`-based image remains available under a separate tag.

### Upgrade Notes
- If you pin the `ubi9` base image (e.g. FIPS/compliance), switch to the dedicated `ubi9` tag — the default is now `ubi10`.
- Role-provider config can be supplied via `ROLE_PROVIDER_FILE` (TOML), with env vars overriding per field — review precedence if you set both.

### Upstream Lakekeeper changes
- **Instance Admins** — server-wide admin role independent of project membership ([lakekeeper#1716](https://github.com/lakekeeper/lakekeeper/pull/1716)).
- **Idempotency keys** for safely retrying mutating requests ([lakekeeper#1671](https://github.com/lakekeeper/lakekeeper/pull/1671)).
- **`referenced-by`** to discover views referencing a table/view ([lakekeeper#1627](https://github.com/lakekeeper/lakekeeper/pull/1627)).
- Configurable **trusted engines** in request metadata for authorization ([lakekeeper#1629](https://github.com/lakekeeper/lakekeeper/pull/1629)).
- **Protect immutable table properties** (e.g. `encryption.key-id`) during commits ([lakekeeper#1700](https://github.com/lakekeeper/lakekeeper/pull/1700)).
- Faster list namespaces/tables/views ([lakekeeper#1618](https://github.com/lakekeeper/lakekeeper/pull/1618)).
