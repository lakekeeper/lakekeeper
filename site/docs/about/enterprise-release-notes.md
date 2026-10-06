---
description: "Release notes for Lakekeeper+, the commercial distribution, covering Cedar authorization, UI branding, admission gates and other enterprise features."
---

# Lakekeeper+ Release Notes

## Unreleased

### Features
- **Cedar: grants decide access.** Privileges granted through the catalog are now visible to your Cedar policies. Each resource carries what the principal being authorized holds on it as `principal_privileges` — `direct` for what is granted on the resource itself, `inherited` for everything granted above it — so a policy can simply check `resource.principal_privileges.direct.select`. Grant-administration actions carry the privilege in question as the typed `context.privilege`, so a policy can condition on exactly which privilege is being handed out.
- **Cedar: predefined policies turn grants into access.** Lakekeeper Plus now ships ready-made policies that turn recorded grants into access, so permissions can be handed out at runtime without redeploying a policy file. Every project and warehouse chooses which of these policies apply to it, through the new `predefined-policies` endpoints. The feature is on by default and can be switched off for the whole deployment with `LAKEKEEPER__CEDAR__PREDEFINED_POLICIES_ENABLED=false`. Policies added by a future release start out disabled for scopes you have already curated, so an upgrade never widens access on its own.
- **Cedar: manage a project's or warehouse's own policies through the API.** Each project and warehouse can now store its own Cedar policies, managed through the new `…/policies` endpoints. Changes are applied as a single transaction that is validated up front and reports back what actually changed. You can preview a change with `dry-run`, protect against concurrent edits with `if-scope-version`, and send your complete desired state with `replace: true`, which removes every policy you did not include. A scope holds up to 1000 policies by default (`LAKEKEEPER__CEDAR__MAX_POLICIES_PER_SCOPE`).
- **Cedar: break-glass.** A policy written too broadly can lock out the very people who could fix it. An operator who sends the header `x-break-glass: <reason>` is judged by the server's own policies alone and can then repair a project's or warehouse's policies. This path opens policy administration only, never any data, and every use is recorded as its own audit event together with the given reason. The new `break-glass-status` endpoint tells you in advance whether this path is open to you.
- **Cedar: the server-action check keeps server actions protected.** When the server loads or reloads its policies, Lakekeeper Plus refuses a policy that would stop fewer users at a server action, such as a `forbid` that names a `Role` id, and the log shows the fix. A `permit` that requires a Lakekeeper role loads with a warning. The check does not run when users and roles are externally managed.
- **Cedar: a new `manage_policies` privilege.** Granting it on a project or warehouse lets someone read and write that scope's policies without giving them access to any data. A grant on a project also covers the warehouses inside it. Because whoever writes policies can permit themselves anything on the next request, this right is never part of `manage` and always has to be granted explicitly.
- **Cedar: a `manage_tags` privilege.** It lets someone tag a warehouse, namespace, table, view or generic table and everything beneath it, without data access. Each tag also needs `apply` or `manage` on the tag; since policies may use tags to decide access, hand out `apply` on such tags as carefully as `manage_grants`.
- **Cedar: a `read_grants` privilege.** It shows who holds which privilege on an object and everything beneath it, and everything one user or role holds in a project. It changes nothing, and it reveals that an object exists even where a policy hides it. Only `manage_grants` checks what another user or role may do, because that check runs every policy.
- **Cedar: a `read_policies` privilege.** On a project or warehouse it shows the stored Cedar policies and which predefined policies are switched on. `pass_grants` hands on none of these three privileges.
- **Cedar: policy sources report their tier.** Every source listed by `server-policy-sources` now carries a `tier` field. This endpoint lists the server's configured policy sources only, so their tier is `server`.
- **Cedar: whole-subtree grant administration.** Listing and revoking every grant beneath a warehouse or namespace are now Cedar actions of their own, one pair at each level. Listing everything one user or role holds in a project is `ReadProjectSubtreeGrants`, separate from reading the project's own grants. Predefined policies map `manage_grants` and `read_grants` onto the listings and `manage_grants` onto the revokes, so access reviews can stay available while bulk revoke is switched off for a project or warehouse. Each request states what it covers in `context.subtree`: the resource types it reaches, whether the addressed object is included, the privileges it covers, and whose grants it touches. When the call names one principal it arrives as a user or role entity, so a rule about a role also covers the roles nested inside it. The decision is made once, on the resource the call names; policies on resources below it do not narrow it. A bulk revoke cannot be undone and can remove the grant that makes you an administrator.
- **Cedar: manage roles through the API and the UI.** Roles managed in Lakekeeper (provider `lakekeeper`) can now be created, renamed, assigned and deleted under Cedar; the API refuses any other provider with `400`. A `CreateRole` policy sees the requested provider and source id as `context.requested_provider_id` and `context.requested_source_id`. With `externally_managed_user_and_roles`, declare roles in the entities file; the API answers `400 CreateRolesNotSupported`.
- **Cedar: predefined policies for roles.** Four predefined policies decide role actions from grants on the role's project, and project `manage_grants` creates, lists and searches roles. `predefined-grants-role-describe` lets `describe`, `create`, `manage` and `manage_grants` holders read every role in the project, including its members. `predefined-grants-role-grant-admin` lets `manage_grants` holders add and remove members of `lakekeeper` roles, rename them and delete them: whoever joins a role holds its grants, so changing its members is granting. `predefined-grants-role-delete-provider-managed` lets `manage_grants` holders delete roles from identity and role providers, and can be switched off on its own. `predefined-grants-role-grant-admin-introspect` lets `manage_grants` holders check what another user or role may do on a role. Assuming a role and rebinding its source system stay with your own policies.
- **Role providers: roles from identity and role providers can be deleted.** Use this to clean up a role whose group was removed from the directory. If the directory still has the group, the role comes back without its grants the next time a member's request in that project asks the directory. Deleting a role that holds grants needs `force=true`; otherwise it fails with `409 RoleHasGrants`.
- **Cedar: two endpoints have new names.** `policy-sources` and `policy-list` are now called `server-policy-sources` and `server-policy-list`, which tells these server-level listings apart from the new per-scope policy endpoints. The old paths still work but are marked deprecated.

### Bug Fixes
- **Okta role provider: people who sign in with an email address now resolve to their groups.** The user identifier was escaped for a query string rather than for a URL path, so the `@` in a login like `alice@example.com` reached Okta as `%40` and every such lookup came back as a bare `400`. Deployments whose OIDC subject is a raw Okta user id were unaffected, which is why service principals kept working while people did not.
- **Okta role provider: a retried token request no longer replays its client assertion.** Okta accepts each assertion once, so a DPoP nonce challenge or a throttled token request could fail with `invalid_client`. Every attempt is now signed afresh.
- **Role providers: stored roles stand in only when every unreachable provider has synced the user.** With several role providers down at once, a user that one of them had never synced used to be served the other providers' stored roles alone, so a policy that forbids on a group from the missing provider did not apply. Such a request now fails until the providers answer again.
- **Role providers: fewer directory calls for users in several projects.** A user's LDAP, Entra ID or Okta groups are fetched once and reused in every project until they are due for a sync. While the directory is down, the newest stored groups are used. The `cache_hit` outcome of `lakekeeper_role_provider_get_roles_duration_seconds` now counts these reuses.
- **Persisted token roles: stored roles are refreshed and apply in every project.** While a user is active in a project, Lakekeeper stores their token roles there again at most every two minutes. A decision about a user who has made no request in the request's project, such as a DEFINER view's owner, uses their newest stored roles from any project. Stored token roles are a snapshot of the user's last token and go stale: to decide about users who are not signed in, use a role provider (LDAP, Entra ID, Okta).
- **Role providers: a user without groups keeps working while the provider is down.** Before, such a user got errors until the provider was back.
- **Cedar: the project list works with more than one project.** Before, it failed with `403 AuthzBadRequest`. Each project is decided as a request to it would be, and acting as a role lists only that role's project.
- **Catalog requests without `x-project-id` use their warehouse's project.** Clients no longer need the header for warehouses outside the default project. Under Cedar, a header that names a different project than the warehouse's is still refused.
- **Token roles work without a project.** A request whose token carries roles no longer fails with `400` when it names no project and no default project is set. Under Cedar, its token and admission roles are in `principal.project_roles`.
- **Role providers: looking up another user's groups writes nothing.** Grant checks, `for_user` checks and DEFINER views answer from the directory without creating users or roles, and a deleted user stays deleted.
- **Role syncs no longer stall behind a database migration.** Cached role assignments also stay current when several role changes for a user happen at once.
- **Cedar: a request the authorizer cannot build answers `500`, not `503`.** This is a server-side fault, so retrying does not help and the response does not point at an outage. The details are in the server log.

### Breaking Changes
- **Cedar: a replaced schema must declare `manage_tags`, `read_grants` and `read_policies`.** If you set `LAKEKEEPER__CEDAR__SCHEMA_FILE`, add them to the `<Level>Privileges` records and to the `<Level>Privilege` and `SubtreeGrantPrivilege` enums where the shipped schema has them (`manage_tags` everywhere but `Tag`, `read_grants` everywhere, `read_policies` on `Project` and `Warehouse`). The server refuses to start and names what is missing until you do. External entity files that declare resource entities need the same fields in `principal_privileges`.
- **Persisted token roles: a token without roles clears the user's stored roles in that project.** Most identity providers leave out the roles claim when a user has none. Entra ID also leaves out `groups` for users in more than 200 groups; serve those users through the Entra Graph role provider before upgrading.
- **Cedar: reading the server's policy sources now counts as policy administration.** The actions for listing and evaluating server policy sources moved out of `ServerActions` into the new `ServerCedarPolicyActions` and `CedarPolicyReadActions` groups. A policy that permits `ServerActions` no longer covers them, so name the new groups where you want these reads allowed.
- **Cedar: `global_role_ids` holds only groups.** Groups are roles from directories, tokens and admission gates. Lakekeeper roles (`lakekeeper` and `system`) are left out, because their names are chosen inside Lakekeeper and could match a group. Match them with `principal.project_roles` at actions inside a project. At server actions, name a group or grant the user.
- **Cedar: at server actions a user carries their groups.** At every server action, user management included, `principal.project_roles` and `principal.global_role_ids` hold the user's groups from directories, tokens and admission gates, whatever `x-project-id` says. Name a group as `principal.project_roles.contains({provider_id: "ldap", source_id: "<group>"})`; this works at every action. A `Lakekeeper::Role::"…"` id matches no user at server actions, unless users and roles are externally managed.
- **Cedar: server grants go to users.** A role belongs to a project, so a server grant to a role is refused with `400 ServerGrantToRole` and the database cannot store one. To give a team server access, name its group with the flat form. Grant users a privilege on the server and load the five server-grant `permit`s from "Server Actions" in the schema; no predefined policy decides server actions.
- **Cedar: act as yourself at server actions.** With `x-assume-role`, every server action is denied, user management included, except on your own user record; send server requests without the header. A permission check about a role is denied at server actions too, and `/management/v1/permissions/cedar/resolve-entities` answers `400` for one.
- **Cedar: reading a user's role assignments is decided on the project.** `GET /management/v1/user/{user_id}/roles` and `/roles/transitive` check `ReadUserRoleAssignments` on the project they read; the predefined `describe`, `create`, `manage` and `manage_grants` policies allow it there, and a grant on the server counts for every project. A `forbid` on an action group does not cover it: name `ReadUserRoleAssignments` to block it. A policy that names it on the server no longer validates, so write `resource is Lakekeeper::Project` there. In `GET /management/v1/user/{user_id}/actions` with `principalUser`, the role-assignments entry needs `IntrospectProjectAuthorization` on the request's project.
- **Cedar: `CreateRole` permits take effect, and roles from role providers can be deleted.** Creating a role used to be refused with `CreateRolesNotSupported` whatever the policies said, and so was deleting a role owned by a role provider. Now a policy that permits `CreateRole` lets its holder create `lakekeeper` roles — `ProjectGrantActions` contains it, so a broad permit on that group does too — and a policy that permits `DeleteRole` on a provider's role lets its holder delete it, with its grants when `force=true` is given.
- **Admission gate: `idp_id` must name a provider the server authenticates.** The server now refuses to start when `idp_id` matches none of the configured authenticators, and tells you which ones are available. Such an id never enforced anything, so this turns a silent misconfiguration into a clear startup error.
- **Admission gate: unknown configuration keys are refused.** A misspelled key under `admission_enforce` used to fall back to its default silently. The server now names the unknown key and stops.
- **Admission gate: the caller's token is no longer forwarded.** Remove the `auth` option (`forward_caller_token`); the server refuses to start while it is set. Authenticate to your endpoint with `headers`.
- **Admission gate: gates run before the `x-assume-role` check.** Roles a gate grants now count when deciding `x-assume-role`. For such a request, the `admission_decided` record names the role asked for, before it is authorized.
- **Admission gate: `lakekeeper_admission_enforce_fail_closed_total` reports only `reason="upstream"`.** The `no_bearer_token` and `no_principal` reasons are gone.
- **OpenFGA: revoking a privilege requires `manage_grants`.** Passing on a privilege you hold still needs only `pass_grants`, but taking one back is now considered administration. This applies to the `/grants` endpoint and to the older assignment deletes.
- **OpenFGA: only instance admins can manage membership of `system`-provider roles.** These roles also no longer accept other roles as members. Ordinary and `lakekeeper`-provider roles are unaffected.
- **Cedar: checking what another user or role may do is grant administration.** The `Introspect<X>Authorization` actions moved into the grant-reading groups. The predefined policies give them to `manage_grants`; in your own policies, permit `GrantReadActions` or name the action.
- **Cedar: a request must name the project its resources are in.** Addressing a warehouse outside the project given in `x-project-id` now returns an error, and one request can no longer span two projects. Single-project deployments are unaffected, and so are deployments with `externally_managed_user_and_roles`, where roles come from the entity file.
- **Cedar: a replaced schema file must declare everything the shipped schema declares.** With `LAKEKEEPER__CEDAR__SCHEMA_FILE` set, the server starts only when your schema declares every attribute the shipped one does — optional attributes included — and it names each one that is missing. Among them, every resource entity needs a single `principal_privileges` record carrying both a `direct` and an `inherited` side.
- **Cedar: external entity files are validated against the schema.** A resource entity that does not carry its `principal_privileges` record, with both sides present, fails to load and the server does not start.
- **Cedar: with externally managed identity, the entity file must declare every principal a request names.** A request that names a user or role missing from your file is now refused instead of answered. Cedar skips policies about entities it cannot find, which could otherwise turn a `forbid` into an allow.
- **Cedar: `IncludeProjectInList` decides the project list, as the schema documents.** The list checked `GetProjectMetadata`. A policy that permits `GetProjectMetadata` without `IncludeProjectInList` no longer lists a project, and `forbid IncludeProjectInList` now hides one. The predefined policies grant both together.
- **Cedar with a directory role provider: granting below the server to a user who has no user record returns `400 GrantUserNotFound`.** Looking up another user's groups, for example for a grant check, no longer creates their user record. Create it first with `POST /management/v1/user` (needs `ProvisionUsers`), or have the user connect once from the console or a catalog client.
- **Role providers: `sync_interval_secs` above 31536000000 (1000 years) is refused at startup.** The error names the setting.

### Upgrade Notes
- **Run `lakekeeper-plus migrate` to completion before rolling any new pod.** This release adds database tables for Cedar policy management, and a new replica refuses to serve while they are missing. Note that `wait-for-db -m` does not check for these tables. Rolling back is safe.
- **Grants that already exist begin granting access.** The predefined policies are on by default, so every grant recorded in your deployment starts deciding requests after the upgrade. Review your grants beforehand, or start with `LAKEKEEPER__CEDAR__PREDEFINED_POLICIES_ENABLED=false` and curate each scope before switching it on.
- **Whole-subtree grant administration is on by default.** A scope that has not curated its predefined policies runs the shipped set, so from the moment you upgrade everyone holding `manage_grants` or `read_grants` can list every grant under a warehouse or namespace and everything one user or role holds in a project, and everyone holding `manage_grants` can revoke grants under a warehouse or namespace in bulk. A bulk revoke is permanent and removes the caller's own grants along with the rest. Revoking your own `manage_grants` ends your grant administration, and break-glass will not restore it: that path opens policy administration only. If you want bulk revoke unavailable, switch its two predefined policies off for the project or warehouse.
- **Multi-project Cedar deployments: review past decisions on requests without `x-project-id`.** Before this release a request could be answered using another project's roles. Catalog requests without the header are now decided in their warehouse's project, so clients need no change. If your clients omitted the header, treat past cross-project decisions as unreliable.
- **Cedar with externally managed identity: grant to users, not roles.** A grant held by a user applies. One held by a role does not, because role membership lives in your entity file.
- **The role members cache is gone.** The `LAKEKEEPER__CACHE__ROLE_MEMBERS__*` settings have no effect and can be removed. Dashboards no longer get `lakekeeper_cache_*` series for `cache_type="role_members"`.
- **Rewrite `permit`s that require a group by `Role` id at server actions.** Unless users and roles are externally managed, a test such as `principal in Lakekeeper::Role::"<project>/ldap~<group>"` matches no user at server actions after the upgrade. When such a `permit` decides only server actions, it loads with a warning that shows the rewrite; otherwise it loads with no message. Name the group with the flat form, `(principal is Lakekeeper::User && principal.project_roles.contains({provider_id: "ldap", source_id: "<group>"}))`.
- **If the server refuses a policy, apply the fix from the log.** At startup the server stops; on reload the policies loaded before keep deciding. Most often the fix splits a `forbid` that names a group by `Role` id into two policies, and the log gives both.
- **Give server access to users or groups before upgrading.** Lakekeeper roles do not reach server actions, and server grants go to users. Grant the users a privilege on the server, or name their group with the flat form. If nobody can reach server administration in the meantime, sign in as an identity listed in `LAKEKEEPER__INSTANCE_ADMINS`.
- **Server-only callers need their directory to answer.** A caller that has never made a request in a project, such as a provisioning bot, has no stored groups. Its server requests fail while LDAP, Entra ID or Okta is down. One request in any project stores its groups.
- **DEFINER views during a directory outage.** A DEFINER view's owner is decided with the groups from their last own request. A service account that owns such views should make a request now and then.


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
