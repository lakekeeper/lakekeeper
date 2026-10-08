---
description: "Release highlights for every Lakekeeper version: new features, breaking changes and upgrade notes for the open source Iceberg REST Catalog."
---

# Release Notes

Highlights for each Lakekeeper release. For the full commit-level
changelog, see the [GitHub Releases](https://github.com/lakekeeper/lakekeeper/releases)
or [`CHANGELOG.md`](https://github.com/lakekeeper/lakekeeper/blob/main/CHANGELOG.md).

For Lakekeeper+ releases, see the [Lakekeeper+ Release Notes](enterprise-release-notes.md).

<!-- Maintainers: how to update this page at release → .github/RELEASING.md -->

--8<-- "_includes/subscribe-form.html"

_[Subscribe by email](subscribe.md) to hear about new releases, or **Watch → Releases** on [GitHub](https://github.com/lakekeeper/lakekeeper/releases)._

## v0.14.0 (2026-10-07)

### Highlights

- **Grants API.** One API lists, grants and revokes permissions on every object, such as projects, warehouses, namespaces, tables, views and tag definitions. It works the same way with every authorizer. With OpenFGA, it uses the permissions you already have, so nothing needs to be migrated. The API is in preview. See [Grants API](https://docs.lakekeeper.io/docs/0.14.x/grants/) ([#1945](https://github.com/lakekeeper/lakekeeper/pull/1945), [#1953](https://github.com/lakekeeper/lakekeeper/pull/1953), [#1957](https://github.com/lakekeeper/lakekeeper/pull/1957), [#2085](https://github.com/lakekeeper/lakekeeper/pull/2085)).
- **Governance tags.** Define tags for a project, attach them to warehouses, namespaces, tables, views, generic tables and columns, and find every object a tag is attached to. Tags are for classification and discovery. With Cedar in Lakekeeper Plus, policies can also use them to decide who can access what. The API is in preview. See [Governance Tags](https://docs.lakekeeper.io/docs/0.14.x/tags/) ([#1914](https://github.com/lakekeeper/lakekeeper/pull/1914), [#1921](https://github.com/lakekeeper/lakekeeper/pull/1921), [#1944](https://github.com/lakekeeper/lakekeeper/pull/1944), [#1951](https://github.com/lakekeeper/lakekeeper/pull/1951)).
- **Move and rename namespaces.** You can move a namespace to another parent in the same warehouse, rename it, or both, in the UI or with the management API. Everything in it moves with it, and files stay where they are. A namespace that contains other namespaces cannot be moved or renamed. See [Moving and renaming namespaces](https://docs.lakekeeper.io/docs/0.14.x/concepts/#moving-and-renaming-namespaces) ([#1950](https://github.com/lakekeeper/lakekeeper/pull/1950)).

### Features

- **Faster on large catalogs.** With OpenFGA, checking access to a warehouse, which most catalog requests do, needs far fewer OpenFGA queries. On large warehouses, this check could take too long and fail with `503`. Creating tables and views, finding namespaces and listing tables are also much faster on large warehouses ([#2029](https://github.com/lakekeeper/lakekeeper/pull/2029), [#2005](https://github.com/lakekeeper/lakekeeper/pull/2005), [#1958](https://github.com/lakekeeper/lakekeeper/pull/1958)).
- **Storage validation.** You can check a warehouse's storage before you create the warehouse or change its storage, and you can check an existing warehouse at any time. Lakekeeper reports the result of each check. It also warns about problems that are hard to find later, such as a bucket CORS policy that stops the query engine in the UI from reading data. See [Storage Validation](https://docs.lakekeeper.io/docs/0.14.x/storage-validation/) ([#1936](https://github.com/lakekeeper/lakekeeper/pull/1936), [#2047](https://github.com/lakekeeper/lakekeeper/pull/2047), [#2049](https://github.com/lakekeeper/lakekeeper/pull/2049), [#2061](https://github.com/lakekeeper/lakekeeper/pull/2061)).
- **STACKIT Object Storage.** A new `stackit` storage type asks only for the settings that STACKIT needs. Storage validation warns when other credentials groups in the same STACKIT project can also access the bucket. See [STACKIT](https://docs.lakekeeper.io/docs/0.14.x/storage-stackit/) ([#1978](https://github.com/lakekeeper/lakekeeper/pull/1978), [#2045](https://github.com/lakekeeper/lakekeeper/pull/2045), [#2050](https://github.com/lakekeeper/lakekeeper/pull/2050), [#2055](https://github.com/lakekeeper/lakekeeper/pull/2055)).
- **Alibaba Cloud OSS (beta).** Warehouses can use Alibaba Cloud OSS, with temporary credentials from Alibaba Cloud STS (thanks @yoogoc). See [Alibaba Cloud OSS](https://docs.lakekeeper.io/docs/0.14.x/storage-s3/#alibaba-cloud-oss) ([#1894](https://github.com/lakekeeper/lakekeeper/pull/1894)).
- **Required token claims.** You can set rules for the claims in a token, and Lakekeeper rejects tokens that do not match them. For example, you can allow only the users of one organization when several organizations share an identity provider. The scope check (`LAKEKEEPER__OPENID_SCOPE`) also reads the `scp` claim, which Microsoft Entra ID uses. See [Required claims](https://docs.lakekeeper.io/docs/0.14.x/configuration/#required-claims) ([#2001](https://github.com/lakekeeper/lakekeeper/pull/2001)).
- **Names for service accounts.** When a token has no name claim, `LAKEKEEPER__OPENID_DISPLAY_NAME_TEMPLATE` builds a name from its other claims, for example `Service Account {email}`. It applies to users that are registered after you set it ([#1939](https://github.com/lakekeeper/lakekeeper/pull/1939)).
- **Headless workers.** With `LAKEKEEPER__SERVE_HTTP_API=false`, `lakekeeper serve` does not serve the API or the UI. It only runs background tasks and answers health checks, so you can scale task workers separately from the API. See [Task Queues](https://docs.lakekeeper.io/docs/0.14.x/configuration/#task-queues) ([#2065](https://github.com/lakekeeper/lakekeeper/pull/2065)).
- **Postgres schema.** `LAKEKEEPER__PG_SCHEMA` keeps Lakekeeper's own database tables in a schema other than `public` (thanks @vyruss). See [Using a non-public Postgres schema](https://docs.lakekeeper.io/docs/0.14.x/configuration/#using-a-non-public-postgres-schema) ([#1926](https://github.com/lakekeeper/lakekeeper/pull/1926)).
- **Default project ID.** `LAKEKEEPER__DEFAULT_PROJECT_ID` sets the project that Lakekeeper uses when a request does not name one. Before, this was always `00000000-0000-0000-0000-000000000000` (thanks @bolma-lila) ([#1930](https://github.com/lakekeeper/lakekeeper/pull/1930)).
- **Choose the warehouse ID.** When you create a warehouse, you can set its ID yourself, so tools such as Terraform know it in advance ([#1983](https://github.com/lakekeeper/lakekeeper/pull/1983)).
- **Versioned audit log.** Every audit record states its format version, `1.0` in this release, and a published JSON Schema describes all records. New records show grant changes and retried requests that Lakekeeper answered with the result of the first request. Authorization records also contain the client's user agent. Existing parsers must be updated; see Audit log format below. See [Audit log format](https://docs.lakekeeper.io/docs/0.14.x/logging/#audit-format) ([#1973](https://github.com/lakekeeper/lakekeeper/pull/1973), [#2074](https://github.com/lakekeeper/lakekeeper/pull/2074), [#1945](https://github.com/lakekeeper/lakekeeper/pull/1945), [#1962](https://github.com/lakekeeper/lakekeeper/pull/1962), [#1995](https://github.com/lakekeeper/lakekeeper/pull/1995)).
- **User emails and role sources in the audit log.** Audit records can carry the email of the users they name. This is off by default; turn it on with `LAKEKEEPER__AUDIT__TRACING__INCLUDE_USER_EMAIL=true`. Roles on audit records also carry their provider and their ID there, for example the LDAP group behind a role. To leave that ID out, set `LAKEKEEPER__AUDIT__TRACING__INCLUDE_ROLE_SOURCE_ID=false`. See [User Emails on Audit Records](https://docs.lakekeeper.io/docs/0.14.x/logging/#audit-user-emails) ([#2091](https://github.com/lakekeeper/lakekeeper/pull/2091)).
- **Remote signing follows the latest Iceberg REST specification.** Lakekeeper supports the new way to configure remote signing that the specification now defines. Current clients keep working without changes. See [Remote Signing](https://docs.lakekeeper.io/docs/0.14.x/storage-s3/#remote-signing) ([#1961](https://github.com/lakekeeper/lakekeeper/pull/1961)).
- **Limit for `referenced-by`.** Query engines can send the list of views through which they load a table, so that Lakekeeper can check view permissions. This list may contain at most 10 views by default; longer lists are rejected. Change the limit with `LAKEKEEPER__REFERENCED_BY__MAX_NESTING_DEPTH`. See [Chain depth](https://docs.lakekeeper.io/docs/0.14.x/view-security/#chain-depth) ([#1980](https://github.com/lakekeeper/lakekeeper/pull/1980)).
- **Empty namespace directories removed on ADLS and OneLake.** Dropping a namespace with `purge=true` also removes its directory if it is empty (thanks @dadavidtseng). See [Empty namespace directories](https://docs.lakekeeper.io/docs/0.14.x/storage-layout/#empty-namespace-directories) ([#1858](https://github.com/lakekeeper/lakekeeper/pull/1858)).
- **Iceberg v3 `unknown` type.** Table and view schemas may use the `unknown` type. Before, Lakekeeper rejected these schemas ([#2063](https://github.com/lakekeeper/lakekeeper/pull/2063)).
- **Console v0.26.0.** New governance and projects pages. With OpenFGA, a Grants tab replaces the Permissions tab. Namespaces can be moved and renamed, STACKIT and Alibaba Cloud OSS warehouses can be added, new warehouses are checked before they are created, and tags and properties can be edited on the pages that show them. Fonts and the extensions of the query engine in the UI are bundled, so the console works without internet access ([#1890](https://github.com/lakekeeper/lakekeeper/pull/1890), [#1997](https://github.com/lakekeeper/lakekeeper/pull/1997), [#2061](https://github.com/lakekeeper/lakekeeper/pull/2061), [#2082](https://github.com/lakekeeper/lakekeeper/pull/2082), [#2094](https://github.com/lakekeeper/lakekeeper/pull/2094)).

### Bug Fixes

- Remote signing and the cleanup of old metadata files check more strictly that they stay inside the table location.
- **Idempotency keys.** If a client reuses an `Idempotency-Key` for a different operation, Lakekeeper rejects the request with `400`. Before, it answered with success without doing anything. Retries of staged table creation and recursive namespace drops now return the result of the first request. With a read replica, a quick retry could fail with `409` or `404`; Lakekeeper now checks the key on the primary database ([#1959](https://github.com/lakekeeper/lakekeeper/pull/1959), [#1981](https://github.com/lakekeeper/lakekeeper/pull/1981)).
- **Cached table loads.** A client that caches tables now gets a fresh copy when the warehouse settings change or when it asks for a different kind of storage access. Before, it could be told that its old copy was still valid. After the upgrade, these clients load each table in full once ([#1946](https://github.com/lakekeeper/lakekeeper/pull/1946), [#1966](https://github.com/lakekeeper/lakekeeper/pull/1966)).
- **Location checks.** Lakekeeper always refuses to create a table or view inside another one's location. Before, the check could miss this with trailing slashes, backslashes or two creates at the same time. A view commit that sets the view's current location again, with a slash at the end, no longer deletes the view's files ([#2005](https://github.com/lakekeeper/lakekeeper/pull/2005)).
- Registering a table uses the access mode the client asks for (vended credentials or remote signing), returns credentials the same way as loading a table, and enforces the warehouse's allowed table format versions ([#1954](https://github.com/lakekeeper/lakekeeper/pull/1954), [#1964](https://github.com/lakekeeper/lakekeeper/pull/1964)).
- Dropping a namespace no longer fails when a view uses it as its default namespace ([#2062](https://github.com/lakekeeper/lakekeeper/pull/2062)).
- A new namespace uses the upper and lower case of its parent. For example, creating `a.b.c` under the existing `a.B` stores `a.B.c`. `lakekeeper migrate` repairs existing namespaces, tables and views once ([#1977](https://github.com/lakekeeper/lakekeeper/pull/1977), [#2014](https://github.com/lakekeeper/lakekeeper/pull/2014)).
- Tables can no longer set `write.object-storage.path` or `write.folder-storage.path`. These old names of `write.data.path` make engines write data files outside the table location. Tables that already have them can remove them ([#2044](https://github.com/lakekeeper/lakekeeper/pull/2044)).
- Deleting a warehouse also deletes its storage credential from the secret store ([#1985](https://github.com/lakekeeper/lakekeeper/pull/1985)).
- S3 warehouses with `sts-enabled` and `aws-kms-key-arn` can be created and updated when the bucket's default KMS key is a different one (thanks @teamrunninglake) ([#2036](https://github.com/lakekeeper/lakekeeper/pull/2036)).
- GCS bucket names may contain underscores, and names with dots may be up to 222 characters long, as GCS allows (thanks @fallintoplace) ([#2080](https://github.com/lakekeeper/lakekeeper/pull/2080), [#2081](https://github.com/lakekeeper/lakekeeper/pull/2081)).
- Creating a warehouse or changing its storage fails with `412 StorageProbeTimeout` when the storage does not answer in time, and the error names the check that did not finish. Before, the request failed with `408` and no details. Connections to storage now time out after 5 seconds instead of 10 ([#2047](https://github.com/lakekeeper/lakekeeper/pull/2047)).
- When Lakekeeper cannot get temporary credentials from STS, the error shows what STS answered. When OneLake rejects vended SAS tokens, the error names the likely cause, usually a Fabric workspace setting ([#2055](https://github.com/lakekeeper/lakekeeper/pull/2055), [#2059](https://github.com/lakekeeper/lakekeeper/pull/2059)).
- When the OPA bridge cannot find a warehouse's ID, for example because the warehouse did not exist yet, it looks the ID up again within 30 seconds. Before, it kept the failed result. Deploy the updated policies from `authz/opa-bridge/policies` to get this fix (thanks @sivakumar-mahalingam) ([#2052](https://github.com/lakekeeper/lakekeeper/pull/2052)).
- With OpenFGA, a denied request to the permissions API returns `403 Forbidden` instead of `401 Unauthorized` ([#2087](https://github.com/lakekeeper/lakekeeper/pull/2087)).
- In the audit log, the authorization record for a change to permissions, roles or tags is written before the change is applied. An `allowed` record means that the caller was allowed to try the change, not that it succeeded (thanks @AndreaBozzo) ([#1924](https://github.com/lakekeeper/lakekeeper/pull/1924), [#1934](https://github.com/lakekeeper/lakekeeper/pull/1934)).
- With `LAKEKEEPER__ENABLE_DEFAULT_PROJECT=false` and roles read from the token, requests that do not name a project no longer fail with `400 MissingProjectId` ([#2071](https://github.com/lakekeeper/lakekeeper/pull/2071)).
- A server with every `LAKEKEEPER__TASK_*_WORKERS` set to `0`, for example an API-only server next to headless workers, no longer stops right after it starts ([#2065](https://github.com/lakekeeper/lakekeeper/pull/2065)).
- Management API filters that take several values, such as the warehouse status filter, work. Before, any request that used them failed with `400` ([#1942](https://github.com/lakekeeper/lakekeeper/pull/1942)).
- Listing namespaces, tables and views no longer keeps a database connection open while permissions are checked. Before, many list requests at the same time could use up the read connections and make other requests wait ([#2058](https://github.com/lakekeeper/lakekeeper/pull/2058)).

### Breaking Changes

- **Stricter OpenID settings.** Lakekeeper refuses to start when an OpenID setting cannot work as written. Check for these cases before you upgrade. See [Authentication](https://docs.lakekeeper.io/docs/0.14.x/configuration/#authentication) ([#2001](https://github.com/lakekeeper/lakekeeper/pull/2001)).
    - A scope is empty or contains a space; only one scope is allowed. `LAKEKEEPER__OPENID_SCOPE` also requires `LAKEKEEPER__OPENID_PROVIDER_URI`.
    - A setting under `LAKEKEEPER__OPENID_PROVIDERS__<IDP_ID>__` has an unknown name, for example because of a typo.
    - A provider checks a scope or required claims, and its `REQUIRE_CONNECTED_ON_STARTUP` is `false`.
    - Two providers accept the same issuer, and the later one checks a scope or required claims. The main provider comes first, then the others in alphabetical order of their ID.
    - Kubernetes authentication is enabled without `LAKEKEEPER__KUBERNETES_AUTHENTICATION_AUDIENCE`, and a provider checks a scope or required claims.
- **OpenFGA: only `manage_grants` can revoke.** A user with `pass_grants` can still grant privileges they hold, but cannot revoke privileges ([#1991](https://github.com/lakekeeper/lakekeeper/pull/1991)).
- **OpenFGA: tools that query OpenFGA directly.** In model v4.13, the relations `describe`, `select`, `create` and `modify` contain only the grants made on that object. Privileges that come from ownership, a stronger privilege, an admin role or a grant on a parent object are in the new `_effective` relations, such as `select_effective`. The Lakekeeper API is not affected ([#2029](https://github.com/lakekeeper/lakekeeper/pull/2029)).
- **OpenFGA: moving tables, views and generic tables.** To rename a table, view or generic table into another namespace, a user needs `manage_grants` and `modify` on it, and `create` on the destination namespace. If the destination namespace uses managed access, the user also needs `manage_grants` there. Renames within a namespace need the same privileges as before. As with grants, moving tables, views, generic tables and namespaces, and turning managed access of a namespace on or off, are not possible while acting under an assumed role ([#2095](https://github.com/lakekeeper/lakekeeper/pull/2095)).
- **Cancelling a soft-deletion task needs `undrop`.** Cancelling the soft-deletion task of a table, view or generic table restores it, so the user also needs `undrop` on it ([#2095](https://github.com/lakekeeper/lakekeeper/pull/2095)).
- **Batch check: `update_storage_credential` removed.** A request to `POST /management/v1/action/batch-check` that contains the warehouse action `update_storage_credential` fails with `422`. Check `update_storage` instead, which the storage-credential endpoint already required ([#1936](https://github.com/lakekeeper/lakekeeper/pull/1936)).
- **Code that builds on Lakekeeper's crates.** Many public traits and types changed, among them `Authorizer`, `CatalogStore`, `AdmissionGate` and `LakekeeperStorage`. Update your own implementations. Prebuilt binaries and images are not affected.
- **Audit log format.** Existing parsers of the audit log must be updated. See Audit log format below.

### Upgrade Notes

- **Images only on Quay.** Starting with this release, Lakekeeper images are no longer published to Docker Hub (`vakamo/lakekeeper`). Use `quay.io/lakekeeper/catalog` instead, for example `quay.io/lakekeeper/catalog:v0.14.0` ([#2090](https://github.com/lakekeeper/lakekeeper/pull/2090)).
- **Plan downtime for the migration, and back up the database first.** `lakekeeper migrate` moves table and view schemas to a new storage format, which can take several minutes on large catalogs. While it runs, changes to tables and views wait, and in its last part reads wait too. Stop all 0.13 servers before you run it: their requests can make the migration fail, and it then rolls back completely. With the Helm chart, the migration job runs while the old pods still serve, so scale the deployment to zero before `helm upgrade`. After the migration, 0.13 servers refuse to start. To go back to 0.13, restore the backup ([#1897](https://github.com/lakekeeper/lakekeeper/pull/1897)).
- **Tables or views that would share a location stop the migration.** The migration removes trailing slashes from stored table and view locations. If two entries then have the same location, for example `s3://b/t` and `s3://b/t/`, `migrate` fails near the end with `TrimWouldShareTabularLocations` and rolls back. Before you upgrade, run this query on the database. It must return no rows: `SELECT warehouse_id, rtrim(fs_location, '/'), array_agg(tabular_id || ' ' || typ || ' ' || array_to_string(tabular_namespace_name, '.') || '.' || name || CASE WHEN deleted_at IS NOT NULL THEN ' (dropped)' ELSE '' END) FROM tabular GROUP BY 1, 2 HAVING count(*) > 1 AND count(DISTINCT fs_location) > 1;` Both entries in a row point to the same files. On 0.13, remove one of them without deleting files: `DELETE /catalog/v1/{prefix}/namespaces/{namespace}/tables/{table}?purgeRequested=false&force=true` (use `/views/{view}` for a view). A normal `DROP TABLE` would also delete the files of the other entry. If the entry is marked `(dropped)`, first restore it with `POST /management/v1/warehouse/{warehouse_id}/deleted-tabulars/undrop` ([#2005](https://github.com/lakekeeper/lakekeeper/pull/2005)).
- **OpenFGA model v4.13.** `lakekeeper migrate` installs model v4.13, and servers of this version always use it. If `LAKEKEEPER__OPENFGA__AUTHORIZATION_MODEL_VERSION` is set, `migrate` skips OpenFGA, and servers do not start until v4.13 is installed. Remove this setting before you run `migrate` ([#2029](https://github.com/lakekeeper/lakekeeper/pull/2029), [#2095](https://github.com/lakekeeper/lakekeeper/pull/2095)).
- **Update the OPA bridge together with Lakekeeper.** Deploy the OPA bridge policies of this release together with this server. When Trino renames a table or view into another schema, the policies now also check `move` and `accept_moved_tabular`, which older servers do not know ([#2095](https://github.com/lakekeeper/lakekeeper/pull/2095)).
- **Larger requests by default.** `LAKEKEEPER__MAX_REQUEST_BODY_SIZE` defaults to 32 MiB instead of 2 MiB. Set it to `2097152` to keep the old limit ([#1974](https://github.com/lakekeeper/lakekeeper/pull/1974)).
- **Task queue renamed.** The `tabular_expiration` queue is now called `soft_deletion`. Rename `LAKEKEEPER__TASK_TABULAR_EXPIRATION_WORKERS` to `LAKEKEEPER__TASK_SOFT_DELETION_WORKERS` before you upgrade. If the old name is set, Lakekeeper does not start and reports ``duplicate field `task_soft_deletion_workers` ``. The task-queue config API still accepts `tabular_expiration` in its path ([#1881](https://github.com/lakekeeper/lakekeeper/pull/1881)).
- **Role members cache removed.** Lakekeeper reads role members from the database. The `LAKEKEEPER__CACHE__ROLE_MEMBERS__*` settings have no effect and log a warning at startup; remove them. Cache metrics with `cache_type="role_members"` are no longer reported ([#2072](https://github.com/lakekeeper/lakekeeper/pull/2072)).
- Building from source requires Rust 1.95 or newer ([#2063](https://github.com/lakekeeper/lakekeeper/pull/2063)).

### Audit log format

**Breaking changes** — an existing parser must be updated:

- **A field is absent when it has no value, and an empty list or map is written out. Nothing in an audit record is `null`.**

    A field the request did not supply is left out. A policy factor without a name, for example, has no `name` key instead of `"name": null`.

    An empty list or map is a value and is always written once an action can carry it:

    ```text
    before  {"action_name": "commit"}
    after   {"action_name": "commit", "updated_properties": {}, "removed_properties": [], "target_refs": [], "update_kinds": []}
    ```

    This applies to `properties`, `updated_properties`, `removed_properties`, `source`, `destination`, `target_refs` and `update_kinds` in an action, and to `authorizations[].determined_by` and `error.stack`. A flag such as `force`, `purge` or `recursive` is also always written, as `true` or `false`.

    **What to do:** read "none" from an empty value, not from a missing key. Keys that are genuinely optional, such as a name or an id, still need a presence check.

- **Authorization records always carry `actions` and `entities` as lists. The singular `action` and `entity` fields are gone.**

    ```text
    before  "action": {…}            or  "actions": [{…}, {…}]
    after   "actions": [{…}]

    before  .entity.namespace         .action.action_name
    after   .entities[0].namespace    .actions[0].action_name
    ```

    Each entry of `authorizations[]` still carries a singular `action` and `entity`.

    **What to do:** read `actions` and `entities` as lists, and iterate: a record can carry more than one.

- **A `context` value has its own JSON type. It is no longer always a string.**

    ```text
    before  "force": "true"   "dry_run": "true"   "writes": "2"
    after   "force": true     "dry_run": false    "writes": 2
    ```

    - Booleans: `force`, `purge`, `recursive`, `dry_run` and `allow_partial` in an action, and `self_provisioning` and `self_read` in `context`.
    - Numbers: `writes` and `deletes`.
    - Principals: `principals` on `apply_grants` and `principal` on `read_subtree_grants` and `revoke_subtree_grants` are `{"user": …}` or `{"role": …}`, the form `context.principal` has on grant records. A subtree request states with `principal_scope` (`every` or `one`) whether it names one principal; `principal` is present only for `one`.
    - Strings, lists and maps keep their type.

    A product that plugs into Lakekeeper may add `context` keys that hold an object. Its schema gives the object's definition.

    **What to do:** compare a flag with `true`, not with `"true"`. Read each key's type from the schema.

- **The entries of `authorizations[].determined_by` have the shape the management API returns from a permission check, and there are two new kinds.**

    ```text
    before  {"Policy": {"policy_id": "p-42", "effect": {"Permit": []}, "source": "cedar"}}
    after   {"type": "policy", "policy-id": "p-42", "effect": "permit", "source": "cedar"}
    ```

    - The kind is in `type`. Field names are kebab-case, as in the API. `effect` is `permit` or `forbid`.
    - `{"type": "system-authority"}`: a built-in authority, not a configured policy, decided the allow. It may carry `source` and `reason`.
    - `{"type": "admission-gate"}`: an admission gate would refuse the user. It carries `gate`, and `check` when the gate names one.

    **What to do:** switch on `type`. The same code can parse these entries and a `/check` response.

- **A request that names nothing to check records empty lists. The made-up `unknown` entry is gone.**

    A batch check with `checks: []`, and a transaction commit with no table changes, recorded one `authorizations` entry with `entity_type` `unknown`, and for the commit `action_name` `unknown` too.

    ```text
    before  "authorizations": [{"action": {"action_name": "unknown"}, "entity": {"entity_type": "unknown"}, …}]
    after   "actions": [], "entities": [], "authorizations": []
    ```

    `decision` still carries the outcome.

    **What to do:** stop matching `unknown` in `entity_type` and `action_name`, and handle an empty `authorizations` list.

- **Every key and every value this log names itself is spelled `snake_case`, and `failure_reason` is a plain string.**

    Keys that carried a hyphen now carry an underscore:

    - in an `entity`: `generic_table`, `generic_table_id`, `namespace_id`, `project_id`, `role_id`, `role_provider_id`, `role_source_id`, `server_id`, `table_id`, `table_location`, `tag_definition_id`, `task_id`, `user_id`, `view_id`, `warehouse_id`
    - in an action: `allow_partial`, `created_before`, `dry_run`, `removed_properties`, `target_refs`, `update_kinds`, `updated_properties`
    - in an authorization record's `context`: `invoked_by`, `self_provisioning`, `self_read`
    - on an `authorizations[]` entry: `for_principal`

    Values:

    ```text
    actor_type      before  assumed-role                after  assumed_role
                    before  lakekeeper-internal         after  lakekeeper_internal
    failure_reason  before  {"ActionForbidden": []}     after  "action_forbidden"
    ```

    Every `failure_reason` value follows the same pattern: `action_forbidden`, `resource_not_found`, `cannot_see_resource`, `internal_authorization_error`, `internal_catalog_error`, `invalid_request_data`.

    Three sets keep the spelling of the vocabulary they come from: `entity_type` and `resource_type` (`generic-table`, `tag-definition`, as in the management API) and `update_kinds` (Iceberg's update names, such as `add-schema`). The objects in `determined_by` keep the management API's field names, `policy-id` included.

    **What to do:** rename these keys and values in every query. A query on the old spelling matches nothing and raises no error, so check for the new names explicitly. In `jq`, `.failure_reason | keys[0]` becomes `.failure_reason`.

- **A role on a record carries its `provider_id` and `source_id` next to its id.** `provider_id` is always shown; `LAKEKEEPER__AUDIT__TRACING__INCLUDE_ROLE_SOURCE_ID=false` leaves `source_id` out.

    ```text
    before  {"role": "1f7b…"}
    after   {"role": "1f7b…", "provider_id": "corporate-ldap", "source_id": "engineering"}
    ```

    This applies wherever a role is named: `authorizations[].for_principal`, `context.principal` on `grant_created` and `grant_revoked`, `principals` on `apply_grants`, and `principal` on `read_subtree_grants` and `revoke_subtree_grants`. Both fields are absent when the role no longer exists or could not be looked up. With the setting off, `source_id` is absent everywhere, including on `actor.assumed_role`, which carries it otherwise.

    **What to do:** correlate on the role's id. Read `provider_id` and `source_id` as optional on a role a record names, and `source_id` as optional on `actor.assumed_role`. Some providers let a source id be a free-form name, so it might hold personal data; turn the setting off if that matters for your log.

- **Renaming a table, view or generic table into another namespace is recorded as `move`, not `rename`, and namespaces have a new action `accept_moved_tabular`.**

    ```text
    renameTable, renameView, renameGenericTable into another namespace
      before   rename
      after    move   destination
    namespace  accept_moved_tabular   source
    ```

    The record of such a rename names `move` on the renamed entity, refused or allowed, on replays as well. `destination` holds the path of the destination namespace; the entity's new name is not part of it, unlike for a namespace `move`, where `destination` is the full new path. A rename within one namespace is still recorded as `rename`. Whether the namespace changes is decided on the paths in the request, ignoring upper and lower case of ASCII letters: two paths that differ only in the case of a non-ASCII letter are recorded as `move`.

    `accept_moved_tabular` is the check on the destination namespace. `source` holds the path of the namespace the entity is moved from. It appears in records of permission checks that name it, such as `/management/v1/action/batch-check`.

    The record of cancelling soft-deletion tasks is unchanged: it still names `control_tasks`. Cancelling such a task now also requires `undrop` on its table, view or generic table, and a refusal is recorded as a denied `control_tasks`.

    **What to do:** if you match table, view or generic-table renames on `rename`, match `move` as well.

**Additions** — an existing parser keeps working:

- **Authorization records carry three new optional fields about the request: `user_agent`, `break_glass` and `idempotency_key`.**

    - `user_agent`: the request's `User-Agent` header, as sent and unverified.
    - `break_glass`: the reason the caller stated in the `x-break-glass` header, as sent and unverified.
    - `idempotency_key`: the request's `Idempotency-Key`.

    Each is absent when the request did not send it.

    **What to do:** nothing. Treat `user_agent` and `break_glass` as claims, never as identity.

- **Three more operations record the override that makes them destructive.**

    ```text
    role delete          force   revokes the grants the role still holds
    warehouse delete     force   deletes a protected warehouse and everything in it
    generic-table drop   force   skips the warehouse's soft-deletion window
                         purge   deletes the data files
    ```

    They use the same keys as namespace delete, table drop and view drop. The key is present whichever way the flag went.

    **What to do:** if you alert on destructive operations, include these three. `"force": true` is the forced form.

- **Three new kinds of audit record: grant changes, admission decisions and idempotent replays.**

    - `operation: "grant_created"` and `"grant_revoked"`: one record per grant an apply actually changed, with `outcome: "success"` and `context` `principal`, `privilege`, `resource_type`, and `resource_id` and `warehouse_id` where they apply.
    - `operation: "admission_decided"`: a request an admission gate refused. `outcome` is `forbidden` or `unavailable`; `context` carries `gate`, `status`, `error_type`, `message`, `error_id`, and `denied_by` when the gate named a rule.
    - `record_type: "replay"`: a request answered from an idempotency record without being executed. It carries the `actions` and `entities` the request named and the `idempotency_key` that matched, and no `decision`.

    **What to do:** nothing, unless you want these records. Select them by `operation` or by `record_type`.

- **Records can carry the email of the users they name.** Off by default; `LAKEKEEPER__AUDIT__TRACING__INCLUDE_USER_EMAIL=true` turns it on.

    ```text
    actor.email                          the principal's email, for principal and assumed_role actors
    authorizations[].for_principal.email  the user's email, for a user subject
    actions[].principals[].email         the user's email, for a user an apply_grants request names
    actions[].principal.email            the user's email, when a subtree grant request names one user
    context.principal.email              the recipient's email, on grant_created and grant_revoked
    ```

    Each is best-effort: absent when the email is not known, never `null`. Roles, `anonymous` and `lakekeeper_internal` actors never carry one. An email is metadata, not identity: emails are not unique and can change.

    **What to do:** correlate on `principal`, `user` and `role`, never on `email`. If you enable the setting, treat the log as holding personal data: an email stays in the log after the user is deleted.

- **Audit records carry four new top-level fields: `record_type`, `emitters`, `time`, and `request_id` when a request caused the record.**

    ```text
    "record_type": "authorization",
    "emitters": {"lakekeeper": "1.0"},
    "request_id": "019684ff-…",
    "time": "2026-03-14T09:26:53.589793Z"
    ```

    - `record_type` names the record's shape: `authorization`, `replay` or `operation`.
    - `emitters` names every product that contributed to the record. Each key is a product name, each value the `MAJOR.MINOR` version of what that product contributes. A record names two products when a component plugged into Lakekeeper supplied an action name or a `context` key.
    - `request_id` is the request that caused the record: the value of the `x-request-id` response header, as sent by the caller or generated by Lakekeeper. It is absent from an operation record no request caused.
    - `time` is when the event happened, in UTC, as RFC 3339 with microseconds.

    `audit_format` governs the record's overall shape. A product's value in `emitters` governs what that product contributes. The two are equal on a record only Lakekeeper produced; do not rely on that.

    **What to do:** route on `record_type`. Read a product's version from its key, for example `.emitters.lakekeeper`. Correlate records with a request through `request_id`, and order them by `time`.

**Also worth knowing** — the format itself did not change:

- **Audit records are emitted on the fixed `tracing` target `lakekeeper::audit`.**

    A log filter naming `lakekeeper::service::events::backends::audit`, `lakekeeper::service::admission` or a prefix of either below `lakekeeper` matches no audit record. The server warns at start-up when it finds such a filter.

    **What to do:** select audit records with `RUST_LOG=warn,lakekeeper::audit=info`, or suppress them with `RUST_LOG=info,lakekeeper::audit=warn`.

- **The audit log has a published JSON Schema: `audit/schema.json` in the documentation, linked from the "Audit Log Schema" page.**

    - Point a validator at the document to check a whole record. Its root, `AuditRecord`, requires `event_source`, `audit_format` and `record_type` and selects the shape by `record_type`: `AuthorizationRecord`, `ReplayRecord` or `OperationRecord`.
    - Every field and key is a property with its type and description. `ActionRecord` lists, per `action_name`, the keys that action carries.
    - A value set the log may extend lists its values under `x-audit-values`, so a validator accepts a value a later release adds. The closed sets `decision`, `privilege_scope`, `root_level` and a policy's `effect` use `enum`.
    - A product plugged into Lakekeeper publishes its own schema for what it contributes. `emitters` on a record says which schemas apply.

    **What to do:** nothing. Use the schema to validate records or to generate a parser.

Records from v0.14.0 carry `audit_format` **1.0** (the first version).

## v0.13.6 (2026-09-22)

### Bug Fixes

- Clients that ask for vended credentials, such as DuckDB, no longer get `403` errors on S3 warehouses that do not vend credentials. Lakekeeper sent a `storage-credentials` entry without credentials, and these clients then sent unsigned requests ([#1923](https://github.com/lakekeeper/lakekeeper/pull/1923)).
- The S3 signer now signs `ListObjectsV2` requests for a prefix inside a table location. This makes Spark's `remove_orphan_files` with `prefix_listing => true` work with remote signing ([#1925](https://github.com/lakekeeper/lakekeeper/pull/1925)).
- Catalog and management responses are now marked `Cache-Control: private` and vary on the caller's identity. A shared HTTP cache can no longer give one user's response, which may contain vended credentials, to another user ([#1947](https://github.com/lakekeeper/lakekeeper/pull/1947)).
- The Iceberg Java client now detects that Lakekeeper supports idempotency keys, because `/config` returns `idempotency-key-lifetime` as a top-level field, as the spec requires ([#1952](https://github.com/lakekeeper/lakekeeper/pull/1952)).
- The `X-Iceberg-Access-Delegation` header accepts a comma-separated list such as `vended-credentials,remote-signing`. A value with non-ASCII characters no longer causes a `500` error ([#1948](https://github.com/lakekeeper/lakekeeper/pull/1948)).
- ADLS now reuses connections, and connecting to ADLS, S3 or GCS times out after 10 seconds. Before, ADLS ignored its connection pool and connect timeout, GCS had no connect timeout, and S3 had none with access keys or anonymous access. Deleting a large table on GCS no longer keeps memory for every file at the same time, which was about 2 GB for a million files ([#2022](https://github.com/lakekeeper/lakekeeper/pull/2022)).

## v0.13.5 (2026-09-15)

### Features

- **S3 remote signing for generic tables.** Clients can now read and write generic table data through Lakekeeper's S3 signer. This makes generic tables usable on S3-compatible storage without STS (thanks @N-Clerkx) ([#1910](https://github.com/lakekeeper/lakekeeper/pull/1910)).

### Bug Fixes

- **Security:** `rustls` is updated to 0.23.45 for RUSTSEC-2026-0285. A TLS peer could send handshake messages without encryption that should have been encrypted. The handshake itself stayed protected ([f5b8993](https://github.com/lakekeeper/lakekeeper/commit/f5b899351e733906c7221337422462177608a8d5)).

## v0.13.4 (2026-09-10)

### Features

- **GovCloud, China and ISO region support.** Vended-credential policies now carry the correct ARN partition (`aws-us-gov`, `aws-cn`, `aws-iso*`, `aws-eusc`), derived automatically from the region, endpoint or role ARN, so `AssumeRole` succeeds for buckets outside the commercial partition — nothing to configure (thanks @123digits) ([#1928](https://github.com/lakekeeper/lakekeeper/pull/1928)).

### Bug Fixes

- Fixed a permissions leak: a table, view or generic table renamed into another namespace kept inheriting grants from the namespace it left ([#2013](https://github.com/lakekeeper/lakekeeper/pull/2013)).
- Renaming a table onto a name that is already taken now returns `409 Conflict` instead of `404 Not Found`, matching the Iceberg REST spec, and renaming onto a soft-deleted name succeeds — as create already did ([#1955](https://github.com/lakekeeper/lakekeeper/pull/1955)).
- Fixed four independent causes of resident memory growing until restart: jemalloc now compiles with `thp:never`, the route table is no longer rebuilt per accepted connection, connections get TCP keepalive and a header-read timeout so vanished peers are dropped, and unmatched request paths no longer become permanent Prometheus series. Settled RSS fell 68% in testing ([#1990](https://github.com/lakekeeper/lakekeeper/pull/1990)).

### Upgrade Notes

- Deployments that renamed a table, view or generic table across namespaces should run `lakekeeper openfga reconcile --mode add-and-delete-drift` once after upgrading. The default `add-missing` mode cannot repair it — the stale permission edge is a surplus tuple, not a missing one.

## v0.13.3 (2026-08-16)

### Features

- **Stable Kubernetes service-account identities.** `LAKEKEEPER__KUBERNETES_AUTHENTICATION_SUBJECT_SOURCE=username` derives a service account's user ID from `system:serviceaccount:<namespace>:<name>` instead of the token UID, so roles and instance admins can be pre-provisioned (e.g. via the Terraform provider) and survive a cluster rebuild. The default `uid` is unchanged ([#1899](https://github.com/lakekeeper/lakekeeper/pull/1899)).
- **Provider-managed roles are protected from drift.** Roles owned by a configured role provider (LDAP, Entra, Okta, token) can no longer be created, updated, deleted, rebound or have their members changed through the management API, since the next provider sync would silently clobber those edits. Nothing changes without a role provider configured ([#1891](https://github.com/lakekeeper/lakekeeper/pull/1891)).

### Bug Fixes

- Fixed two denial-of-service advisories (RUSTSEC-2026-0194 and RUSTSEC-2026-0195) reachable through the S3 remote signer, which parses a client-supplied XML body; `quick-xml` is bumped to 0.41 ([#1885](https://github.com/lakekeeper/lakekeeper/pull/1885)).
- Loading a table with `referenced-by` no longer runs a discarded authorization check on the target view, removing a wasted authorizer evaluation per request and a misleading `SelectView` entry from audit logs ([#1886](https://github.com/lakekeeper/lakekeeper/pull/1886)).

## v0.13.1 (2026-06-30)

### Bug Fixes

- Console updated to v0.19.0; the default OAuth `client_id` no longer carries a leading slash ([ac1094a](https://github.com/lakekeeper/lakekeeper/commit/ac1094a52e51da586c60a27159f9720784eb66e7)).

## v0.13.0 (2026-06-30)

### Highlights

- **Generic Table API.** Register non-Iceberg tables (e.g. Lance, Delta) as first-class generic tables and get credential vending, list/load/rename/drop, soft-delete/undrop, protection, and full authorization — without faking Iceberg metadata ([#1673](https://github.com/lakekeeper/lakekeeper/pull/1673), [#1813](https://github.com/lakekeeper/lakekeeper/pull/1813)).
- **Operator-owned warehouses.** A `managed_by` marker lets a control plane (operator/IaC) own a warehouse, locking spec mutations (delete, rename, (de)activate, storage profile, protection, format-version policy) to instance admins even when authorization would otherwise allow them ([#1828](https://github.com/lakekeeper/lakekeeper/pull/1828)).
- **Cache hardening for large fleets.** Hot read-through caches now coalesce concurrent identical misses (single-flight) and jitter their TTLs, cutting thundering-herd load on the database and on rate-limited cloud STS/SAS endpoints ([#1833](https://github.com/lakekeeper/lakekeeper/pull/1833), [#1837](https://github.com/lakekeeper/lakekeeper/pull/1837)).

### Features

- **Per-warehouse table format-version policy.** Set the allowed Iceberg table format versions and an optional default per warehouse, enforced on create/commit/upgrade ([#1786](https://github.com/lakekeeper/lakekeeper/pull/1786)).
- **Customer-managed KMS encryption.** Warehouses with `aws-kms-key-arn` now advertise `s3.sse.type=kms` to clients, so writes via vended credentials are encrypted with your KMS key ([#1847](https://github.com/lakekeeper/lakekeeper/pull/1847)).
- **Schedule a task directly.** `POST .../task-queue/{queue_name}/schedule` schedules a task for a single table without waiting for a commit hook ([#1783](https://github.com/lakekeeper/lakekeeper/pull/1783)).
- **Task failures are debuggable.** Failed tasks now surface the root-cause failure reason in task-detail responses, no server-log access required ([#1873](https://github.com/lakekeeper/lakekeeper/pull/1873)).
- **Pluggable admission gates.** A new post-authentication seam lets an external service reject already-authenticated principals that have no entitlement on this instance and contribute their resolved roles ([#1865](https://github.com/lakekeeper/lakekeeper/pull/1865), [#1866](https://github.com/lakekeeper/lakekeeper/pull/1866), [#1869](https://github.com/lakekeeper/lakekeeper/pull/1869)).
- **Authorizer-independent role-membership API.** A single management surface lists, adds, and removes a role's members (users or roles) and shows what a role or user belongs to — the same API regardless of the configured authorizer. The membership edges keep a single source of truth: the authorizer's own store when it manages assignments (e.g. OpenFGA), otherwise the catalog's Postgres tables ([#1829](https://github.com/lakekeeper/lakekeeper/pull/1829)).
- **Iceberg 1.11 remote signing.** Emits the new `signer.uri`/`signer.endpoint` properties alongside the legacy `s3.signer.*` keys, so clients ≥1.11 stop logging deprecation warnings while older clients keep working ([#1820](https://github.com/lakekeeper/lakekeeper/pull/1820)).
- **Per-decision authorization audit.** Authorization audit events now record the contributing policies behind each allow/deny outcome ([#1844](https://github.com/lakekeeper/lakekeeper/pull/1844)).
- **More observability.** New client-side Postgres connection-pool metrics and event-listener dispatch timing metrics ([#1838](https://github.com/lakekeeper/lakekeeper/pull/1838), [#1863](https://github.com/lakekeeper/lakekeeper/pull/1863)).
- **Console v0.15.1** — generic-tables (Lance/Delta) tab, Iceberg format-version policy editor, OneLake/Fabric backend, and per-queue maintenance summaries ([#1855](https://github.com/lakekeeper/lakekeeper/pull/1855)).

### Bug Fixes

- `GET /config` now authorizes only the requested warehouse (fixing timeouts on large projects) and masks hidden/unknown warehouses identically so existence and UUIDs can't leak ([#1788](https://github.com/lakekeeper/lakekeeper/pull/1788)).
- Conditional `loadTable` no longer returns `304` once the cached response's vended credentials have expired ([#1862](https://github.com/lakekeeper/lakekeeper/pull/1862)).
- S3: keep the STS vended-credential policy within the packed-size limit and emit a reliable table resource ARN for paths with special characters ([#1857](https://github.com/lakekeeper/lakekeeper/pull/1857)).
- Azure (ADLS): add a connect timeout and a larger retry budget so transient connect failures are retried; retry storage OAuth token acquisition for ADLS and GCS ([#1815](https://github.com/lakekeeper/lakekeeper/pull/1815), [#1827](https://github.com/lakekeeper/lakekeeper/pull/1827)).
- Postgres: migration locks are now transaction-scoped, so a failed migration no longer leaks an advisory lock that could permanently block future migrations ([#1790](https://github.com/lakekeeper/lakekeeper/pull/1790)).

### Breaking Changes

- **Default storage layout is now flat** — new namespaces use `<base>/<tabular-uuid>` instead of nesting tabulars under the parent-namespace UUID. Only namespaces created on/after 0.13 are affected; existing namespaces and tabulars keep their persisted paths (not retroactive, no migration), so a warehouse spanning the upgrade can hold a mix. To keep the previous behavior for new namespaces, explicitly configure the full-hierarchy layout ([#1853](https://github.com/lakekeeper/lakekeeper/pull/1853)).
- **Event backends are separate crates.** The NATS and Kafka backends moved out of the core `lakekeeper` crate into `lakekeeper-events-nats` and `lakekeeper-events-kafka`. Prebuilt binaries and images are unaffected (same env vars); building from source must depend on the new crates — the `nats`, `kafka`, and `vendored-protoc` features are removed from `lakekeeper` ([#1814](https://github.com/lakekeeper/lakekeeper/pull/1814)).
- **Postgres backend is a separate crate.** Catalog Postgres logic moved into `lakekeeper-storage-postgres`, making `lakekeeper` backend-agnostic. Source consumers of `lakekeeper::implementations::*` must now depend on the new crate; prebuilt binary/image users are unaffected ([#1812](https://github.com/lakekeeper/lakekeeper/pull/1812)).
- **Role source-system rebind is now permission-gated.** `PUT /role/{id}/source-system` requires membership-control permission (OpenFGA model v4.6→v4.7), closing a privilege-escalation gap ([#1848](https://github.com/lakekeeper/lakekeeper/pull/1848)).
- **Custom authorizers.** `are_allowed_*_actions_impl` now return `Vec<AuthorizationDecision>` instead of `Vec<bool>`, and `RoleAction` gained a non-`Copy` `UpdateSourceSystem` variant; out-of-tree authorizer implementations must adapt. Built-in OpenFGA users are unaffected ([#1844](https://github.com/lakekeeper/lakekeeper/pull/1844), [#1848](https://github.com/lakekeeper/lakekeeper/pull/1848)).
- **Regenerate action client SDKs.** `GET …/actions` now advertises action _kinds_ (`Lakekeeper*ActionKind`); the wire JSON is unchanged, but generated clients should be regenerated ([#1860](https://github.com/lakekeeper/lakekeeper/pull/1860)).

### Upgrade Notes

- **Downgrade protection.** `serve` now refuses to start against a database already migrated by a newer binary and does not retry. After a rollback, start the older binary with `serve --force-start`, accepting the schema-incompatibility risk ([#1861](https://github.com/lakekeeper/lakekeeper/pull/1861)).
- Docker base images moved from Debian 12 (bookworm) to Debian 13 (trixie) ([#1794](https://github.com/lakekeeper/lakekeeper/pull/1794)).
- The docker-compose examples now use SeaweedFS instead of MinIO for object storage ([#1811](https://github.com/lakekeeper/lakekeeper/pull/1811)).

## v0.12.4 (2026-06-17)

### Features

- **Multiple OIDC providers.** Authenticate tokens from several identity providers at once (e.g. Okta for users + a cloud OIDC issuer for service accounts) via `LAKEKEEPER__OPENID_PROVIDERS`; fully backwards-compatible with the existing single-provider config ([#1760](https://github.com/lakekeeper/lakekeeper/pull/1760)).
- **Microsoft OneLake / Fabric storage + private endpoints.** New OneLake (ADLS Gen2) storage profile with configurable endpoint modes including workspace private link ([#1852](https://github.com/lakekeeper/lakekeeper/pull/1852)); Console updated to v0.14.3 for OneLake support.

### Bug Fixes

- `/health` now returns HTTP `503` (not `200`) when aggregate health is unhealthy or unknown, so Kubernetes HTTP probes correctly detect an unhealthy server; the JSON body is unchanged ([#1802](https://github.com/lakekeeper/lakekeeper/pull/1802)).

## v0.12.3 (2026-05-26)

### Features

- **Read-only maintenance mode.** Set `LAKEKEEPER__MAINTENANCE_MODE=read-only` to reject mutating requests with `503` + `Retry-After` during planned maintenance ([#1765](https://github.com/lakekeeper/lakekeeper/pull/1765)).
- **Atomic core + extension migrations.** Schema migrations now apply in a single transaction, so an interrupted upgrade can't leave a half-migrated database ([aa734bf](https://github.com/lakekeeper/lakekeeper/commit/aa734bffcacd98566aa670f62341a4455833496c)).
- **Users may share an email address.** The unique-email constraint was dropped, so multiple users can have the same email ([#1755](https://github.com/lakekeeper/lakekeeper/pull/1755)).
- **Survey opt-out.** New `LAKEKEEPER__UI__ENABLE_SURVEYS` flag disables in-console surveys and their third-party requests ([719150b](https://github.com/lakekeeper/lakekeeper/commit/719150bed3b700308ff5954217cfad8aac5ba9cf)).
- **Reserved `system` role provider** for catalog-managed roles ([#1776](https://github.com/lakekeeper/lakekeeper/pull/1776)).
- **Console.** New "Export for GitHub" support bundle (server info + UI config, no tokens), a Feedback button, role-provider IDs in the overview, and a two-column Server Settings layout ([719150b](https://github.com/lakekeeper/lakekeeper/commit/719150bed3b700308ff5954217cfad8aac5ba9cf)).

### Bug Fixes

- Fixed table property removal being lost when no properties remained ([#1767](https://github.com/lakekeeper/lakekeeper/pull/1767)).
- Views now preserve protection (`protected=true`) across commits — previously lost on update (thanks @fallintoplace) ([#1770](https://github.com/lakekeeper/lakekeeper/pull/1770)).
- `force=true` is now respected when dropping soft-deletion warehouses that contain views ([#1779](https://github.com/lakekeeper/lakekeeper/pull/1779)).
- Fixed a memory leak from stale Vault (KV2) health status (thanks @fallintoplace) ([#1773](https://github.com/lakekeeper/lakekeeper/pull/1773)).
- Postgres: rewrote the namespace trigger so `pg_restore` can replay it ([#1781](https://github.com/lakekeeper/lakekeeper/pull/1781)).

### Upgrade Notes

- Minimum supported Rust version (MSRV) raised to 1.94 — affects building from source.

## v0.12.2 (2026-05-10)

### Features

- Storage locations are canonicalised at parse time to avoid path aliases ([#1743](https://github.com/lakekeeper/lakekeeper/pull/1743)).
- Object size is now exposed on `FileInfo` ([#1741](https://github.com/lakekeeper/lakekeeper/pull/1741)).

### Bug Fixes

- **Security:** hardened S3 STS/CEL credential-vending policies against path injection ([#1740](https://github.com/lakekeeper/lakekeeper/pull/1740)).
- Azure (ADLS): pre-encode `%` in blob names so the SDK no longer collapses distinct paths onto the same alias ([#1746](https://github.com/lakekeeper/lakekeeper/pull/1746)).
- Postgres: apply `pg_acquire_timeout` to all connection-pool initialisations ([#1744](https://github.com/lakekeeper/lakekeeper/pull/1744)).

## v0.12.1 (2026-05-04)

### Highlights

- **Instance Admins.** Designate break-glass principals that bypass control-plane authorization for management actions via `LAKEKEEPER__INSTANCE_ADMINS` (a list of `<idp_id>~<subject>` IDs). The bypass excludes data-plane operations and role-assumed requests ([#1716](https://github.com/lakekeeper/lakekeeper/pull/1716)).
- **Safe switch to OpenFGA.** Existing deployments can adopt or rebuild OpenFGA: `openfga reconcile` rebuilds hierarchy tuples from the catalog (with dry-run and drift-deletion), and `reopen-bootstrap` re-enables bootstrap for recovery ([#1731](https://github.com/lakekeeper/lakekeeper/pull/1731), [#1733](https://github.com/lakekeeper/lakekeeper/pull/1733)).
- **OpenDAL dropped.** Storage I/O now goes exclusively through the hyperscaler-native backends wrapped by `lakekeeper-io`, including vended-credential validation ([#1737](https://github.com/lakekeeper/lakekeeper/pull/1737)).
- **Security.** Upgraded `rustls-webpki` to 0.103.12 for RUSTSEC-2026-0098 ([#1713](https://github.com/lakekeeper/lakekeeper/pull/1713)).

### Features

- **Trusted engines for views (`referenced-by`).** Validates the `referenced-by` parameter and resolves view-on-view chains for batch authorization, enabling secure DEFINER-style execution for trusted query engines — configured under `LAKEKEEPER__TRUSTED_ENGINES__<NAME>` ([#1647](https://github.com/lakekeeper/lakekeeper/pull/1647)).
- **Protected security-relevant properties.** Only a matched trusted engine may set or remove view owner / run-as properties (case variants rejected), and commits can no longer overwrite immutable table properties such as `encryption.key-id` ([#1700](https://github.com/lakekeeper/lakekeeper/pull/1700), [#1724](https://github.com/lakekeeper/lakekeeper/pull/1724)).
- **Richer audit.** `introspect_permission` events now include the inner check tuples and their decisions, and events record whether access was granted internally, via an instance admin, or by the authorizer ([#1697](https://github.com/lakekeeper/lakekeeper/pull/1697)).
- **Data-plane `Select` action for views**, plus a public `resolve_principal` API for downstream API-to-authz `UserOrRole` conversion ([#1721](https://github.com/lakekeeper/lakekeeper/pull/1721), [#1703](https://github.com/lakekeeper/lakekeeper/pull/1703)).
- **OPA bridge.** View-on-view queries via `CreateViewWithSelectFromColumns`, the Trino `ADD_FILES` operation, and a warehouse/namespace broad-access fast path for batch authorization ([#1712](https://github.com/lakekeeper/lakekeeper/pull/1712), [#1727](https://github.com/lakekeeper/lakekeeper/pull/1727)).
- **Extended Server Info** with console information and commit SHAs ([#1725](https://github.com/lakekeeper/lakekeeper/pull/1725)).

### Bug Fixes

- Added `webpki_root_certs` / UBI native certs to the S3 client to fix TLS trust issues ([#1720](https://github.com/lakekeeper/lakekeeper/pull/1720)).
- Namespace/table case handling: allow renaming a table to a different case of its own name; lookups return the caller's case, ID lookups the canonical case ([7c26309](https://github.com/lakekeeper/lakekeeper/commit/7c263091f255b75ed5d66024b5bc6b29ef553508)).
- Pinned `gcloud-storage` / `gcloud-auth` to `~1.2` to avoid a `reqwest-middleware` conflict ([#1701](https://github.com/lakekeeper/lakekeeper/pull/1701)).
- ADLS: remove the actual matched SAS token key rather than its prefix ([76a091b](https://github.com/lakekeeper/lakekeeper/commit/76a091b9b01ba507cc448a56241d54f526c19a14)).
- Console: fixed base-URL trailing slash, Vite 8 authentication breakage, and a stale warehouse name after rename ([#1729](https://github.com/lakekeeper/lakekeeper/pull/1729), [#1723](https://github.com/lakekeeper/lakekeeper/pull/1723)).

### Upgrade Notes

- **Switching to OpenFGA on an existing instance is now safe:** run `openfga reconcile` (dry-run first; drift-deletion mode to also remove stale tuples), and `reopen-bootstrap` to re-enter bootstrap if needed. The minimum required OpenFGA version was raised.
- **OpenDAL removed** — storage now relies solely on the native S3/GCS/ADLS backends in `lakekeeper-io`; re-verify storage and vended-credential config after upgrade.
- Deploying into a custom Postgres schema is now supported and documented ([#1714](https://github.com/lakekeeper/lakekeeper/pull/1714)).

## v0.12.0 (2026-04-01)

### Highlights

- **Audit Event System.** Authorization decisions and catalog operations emit dedicated audit events with exactly-once-per-call delivery, giving a reliable trail of who did what ([b77c687](https://github.com/lakekeeper/lakekeeper/commit/b77c68740a67221669acaa122742b3912d48aeb5)).
- **Idempotency keys for safe retries.** Send an `Idempotency-Key` header on mutating requests and Lakekeeper replays the original response instead of applying the change twice — on by default, scoped per warehouse, with a 30-minute key lifetime ([#1671](https://github.com/lakekeeper/lakekeeper/pull/1671)).
- **Customizable storage layouts.** Choose how namespace/table paths are templated on S3/GCS/ADLS using `{uuid}` / `{name}` placeholders ([#1615](https://github.com/lakekeeper/lakekeeper/pull/1615), [#1628](https://github.com/lakekeeper/lakekeeper/pull/1628)).
- **Structured JSON logs.** Log output is now structured JSON with objects as field values, ready for log-pipeline ingestion ([b77c687](https://github.com/lakekeeper/lakekeeper/commit/b77c68740a67221669acaa122742b3912d48aeb5)).

### Features

- **Configurable STS endpoint.** Set a separate `sts-endpoint` on an S3 storage profile when your S3-compatible storage exposes STS on a different host ([#1653](https://github.com/lakekeeper/lakekeeper/pull/1653)).
- **Fallback subject claims.** OpenID subject-claim config accepts a comma-separated list; the first matching claim in the token wins, easing varied-IdP integration ([#1646](https://github.com/lakekeeper/lakekeeper/pull/1646)).
- **Request size/time limits.** New `LAKEKEEPER__MAX_REQUEST_BODY_SIZE` (default 2 MB) and `LAKEKEEPER__MAX_REQUEST_TIME` (default `30s`) guard against oversized and slow requests ([#1583](https://github.com/lakekeeper/lakekeeper/pull/1583)).
- **Trusted engines configuration.** Declare trusted engines (e.g. Trino) with a selectable Invoker/Definer security model; the engine is auto-detected from token audience and recorded in request metadata ([#1629](https://github.com/lakekeeper/lakekeeper/pull/1629)).
- **Role assignment store and cache** with provider-scoped role identifiers, plus a roles cache for faster authorization ([#1638](https://github.com/lakekeeper/lakekeeper/pull/1638), [#1623](https://github.com/lakekeeper/lakekeeper/pull/1623)).
- **Iceberg V3 Variant datatype** support, validated against Spark 4 integration tests ([daa7947](https://github.com/lakekeeper/lakekeeper/commit/daa7947333097b25e09a91281a6057d334db599c)).
- **Tokio runtime metrics** exported for runtime observability ([#1664](https://github.com/lakekeeper/lakekeeper/pull/1664)).
- **Reduced memory footprint** by switching the allocator to jemalloc ([0eaeedc](https://github.com/lakekeeper/lakekeeper/commit/0eaeedc8411120f18ec9229b4dd08c36dd294d23)).
- **OPA bridge improvements:** batch-authorization optimization, configurable admin users, system-schema handling, and request-context forwarding ([#1674](https://github.com/lakekeeper/lakekeeper/pull/1674), [#1662](https://github.com/lakekeeper/lakekeeper/pull/1662)).
- **Faster listing** of namespaces, tables, and views ([#1618](https://github.com/lakekeeper/lakekeeper/pull/1618)).
- **Console.** New Home dashboard with usage statistics and API-call charts; branch operations (create, rename, delete, rollback, fast-forward); a Properties dialog for tables/views/namespaces; storage-layout configuration; and a local query engine with memory management ([#1621](https://github.com/lakekeeper/lakekeeper/pull/1621), [#1634](https://github.com/lakekeeper/lakekeeper/pull/1634)).

### Bug Fixes

- Fixed duplicate results when paginating `list_tabulars` ([#1682](https://github.com/lakekeeper/lakekeeper/pull/1682), [#1684](https://github.com/lakekeeper/lakekeeper/pull/1684)).
- Fixed a memory leak in the S3 identity cache ([0eaeedc](https://github.com/lakekeeper/lakekeeper/commit/0eaeedc8411120f18ec9229b4dd08c36dd294d23)).
- Allowed updating the storage-profile region when an S3 endpoint is set ([#1678](https://github.com/lakekeeper/lakekeeper/pull/1678)).
- Patched security advisories in crypto dependencies (`aws-lc-sys` / `rustls-webpki`) ([#1672](https://github.com/lakekeeper/lakekeeper/pull/1672)) and an `lz4_flex` memory-leak advisory ([#1665](https://github.com/lakekeeper/lakekeeper/pull/1665)).

### Breaking Changes

- **Cache metrics unified.** Per-cache metric names are replaced by shared names distinguished by a `cache_type` label ([#1641](https://github.com/lakekeeper/lakekeeper/pull/1641)).
- **Structured log format.** Logs now emit structured JSON with objects as field values instead of flat text ([b77c687](https://github.com/lakekeeper/lakekeeper/commit/b77c68740a67221669acaa122742b3912d48aeb5)).

### Upgrade Notes

- **Cache metrics:** migrate dashboards/alerts from the old per-cache metric names to the unified `lakekeeper_cache_hits_total` / `lakekeeper_cache_misses_total` / `lakekeeper_cache_size`, filtering by the `cache_type` label (`role`, `warehouse`, `namespace`, `secrets`, `stc`).
- **Structured logs:** log consumers must parse JSON rather than plain text. Set `LAKEKEEPER__DEBUG__EXTENDED_LOGS=true` to include `filename`/`line_number` fields.
- **S3 credential fields** dropped their `aws_` prefix; the old names remain accepted as aliases, but update to the new names ([#1685](https://github.com/lakekeeper/lakekeeper/pull/1685)).
