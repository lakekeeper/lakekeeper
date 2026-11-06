---
description: "Configure Lakekeeper Warehouses on OneLake (Microsoft Fabric) lakehouses: parameters, endpoint modes and private links, Entra credentials, the Fabric settings for user-delegated SAS tokens, path restrictions and client versions."
---

# OneLake (Microsoft Fabric)

*Available since Lakekeeper 0.12.4.*

Microsoft Fabric exposes its OneLake data lake through ADLS Gen2-compatible APIs, so Lakekeeper can back warehouses directly with a Fabric lakehouse using a dedicated `onelake` storage profile. The profile derives the OneLake URL conventions (`account_name = "onelake"`, container = workspace ID, key prefix = `<lakehouse>/Files/<dir>`) from the workspace and lakehouse UUIDs you provide, and computes the workspace-scoped private-link endpoint host.

Table locations use `abfss://`. Lakekeeper vends user-delegated SAS tokens to clients; OneLake has no remote signing, and [LoQE](engines.md#loqe) does not support OneLake.

!!! note
    The generic [`adls` profile](storage-adls.md) also works against OneLake if you set the fields manually (`account-name: "onelake"`, `host: "dfs.fabric.microsoft.com"`, `filesystem: <workspace-id>`, `key-prefix: <lakehouse-id>/Files/<dir>`). The `onelake` profile is the recommended path because it validates the OneLake-specific constraints (SAS lifetime cap, supported credentials, endpoint shapes) for you and computes private-link FQDNs automatically.

## Configuration Parameters

| Parameter                      | Type    | Required | Default                             | Description |
|--------------------------------|---------|----------|-------------------------------------|-------------|
| `workspace-id`                 | UUID    | Yes      | -                                   | UUID of the Fabric workspace this warehouse lives in. |
| `lakehouse-id`                 | UUID    | Yes      | -                                   | UUID of the lakehouse within the workspace. |
| `directory-rel-path`           | String  | Yes      | -                                   | Subpath beneath `<top-level-folder>/` inside the lakehouse — the root directory under which Lakekeeper writes all warehouse data. |
| `top-level-folder`             | String  | No       | `"Files"`                           | Top-level managed folder. Either `"Files"` (recommended, Iceberg-managed data area) or `"Tables"`. Writing Iceberg metadata under `Tables/` conflicts with Fabric's automatic Delta/Iceberg virtualization — choose only with care. |
| `endpoint-mode`                | Object  | No       | `{"type": "default"}`               | OneLake endpoint selection. See [Endpoint Modes](#endpoint-modes) below. |
| `sas-enabled`                  | Boolean | No       | `true`                              | Enable SAS-token vending. Disable to force clients to use their own credentials. |
| `sas-token-validity-seconds`   | Integer | No       | `3600`                              | SAS-token validity in seconds. **Max: 3600 (OneLake hard cap).** Lakekeeper rejects values above 3600 and below 1; values below 60 are accepted with a warning and floored at mint time. |
| `authority-host`               | URL     | No       | `https://login.microsoftonline.com` | Microsoft Entra authority host. |
| `storage-layout`               | Object  | No       | `{"type": "default"}`               | **Must be `{"type": "default"}` or omitted.** See [Path and Layout Restrictions](#path-and-layout-restrictions). |

## Credentials

OneLake does not have a storage-account key. Only Microsoft Entra credentials are accepted, as `"type": "az"`:

- `client-credentials` (service principal): the standard option.
- `azure-system-identity` (managed identity): if `LAKEKEEPER__ENABLE_AZURE_SYSTEM_CREDENTIALS=true` is set server-wide, see [Azure System Identity](storage-adls.md#azure-system-identity).

Supplying `shared-access-key` to a OneLake warehouse is rejected at validation time.

Vended credentials are user-delegated SAS tokens, which depend on two Fabric settings under *OneLake settings*. **"Use short-lived user-delegated SAS tokens"** lets Lakekeeper obtain the delegation key, and **"Authenticate with OneLake user-delegated SAS tokens"** lets OneLake accept requests signed with it. Enable the second one tenant-wide, or, when the tenant admin delegates it to workspaces, in the settings of the workspace that holds the lakehouse. Both are Fabric-side settings and cannot be configured from Lakekeeper.

## Endpoint Modes

OneLake exposes three DFS endpoint shapes; the `endpoint-mode` field picks one.

| Type                   | JSON                                                    | Resulting host                                                          |
|------------------------|---------------------------------------------------------|-------------------------------------------------------------------------|
| Default                | `{"type": "default"}`                                   | `onelake.dfs.fabric.microsoft.com`                                      |
| Regional               | `{"type": "regional", "region": "westus"}`              | `westus-onelake.dfs.fabric.microsoft.com`                               |
| Workspace private link | `{"type": "workspace-private-link"}`                    | `<workspace-id-no-dashes>.z<xy>.dfs.fabric.microsoft.com` (host derived from `workspace-id` automatically; `<xy>` is the first two hex characters of the un-dashed workspace UUID) |

Use `regional` when data residency requires the request to stay within a specific Azure region. Use `workspace-private-link` when the workspace is fronted by a Fabric workspace-level private endpoint.

!!! info "Tenant-level vs workspace-level private link"
    Fabric supports two distinct private-link scopes, and only one of them needs a dedicated `endpoint-mode`:

    - **Tenant-level private link**: traffic to the global host `onelake.dfs.fabric.microsoft.com` is routed privately via DNS that points the global FQDN at a tenant-PE NIC. From Lakekeeper's perspective this is indistinguishable from public traffic — use `default`. (Same shape as a private endpoint sitting in front of a regular ADLS Gen2 storage account: the URL Lakekeeper builds doesn't change, only DNS does.)
    - **Workspace-level private link**: each workspace gets its own `<wsId>.z<xy>.dfs.fabric.microsoft.com` FQDN routed via a workspace-scoped PE. Lakekeeper has to build that FQDN — use `workspace-private-link`.

!!! note
    Even when `endpoint-mode` is set to `workspace-private-link`, the Lakekeeper server itself must retain DNS resolution and outbound TLS connectivity to the global host `onelake.dfs.fabric.microsoft.com`. SAS token minting (the `Get User Delegation Key` call) is not served by the workspace-FQDN private-link endpoint — Fabric returns `DeniedByPolicy` there — so Lakekeeper issues that single call against the global OneLake host. Vended client traffic (read/write of table data) still flows through the workspace private link.

## Path and Layout Restrictions

OneLake's request pipeline silently collapses any `%XX` percent-escape in a blob path to its decoded character before SAS validation, so a path that stores the *literal* three-character sequence `%3F` is indistinguishable from one that stores the single character `?`. Lakekeeper otherwise treats every byte in a path literally (`%41bc` is a different blob from `Abc`). On OneLake that guarantee cannot hold, which has two consequences:

- The OneLake profile **rejects any table location whose path segments contain a literal `%`** at create time. Use a different character or strip the `%` before submitting the location.
- The OneLake profile **supports only the `default` [storage layout](storage-layout.md)** and rejects `tabular-only` and `full-hierarchy` at warehouse-creation time. Those layouts can embed namespace and table names through `{name}` templates, which Lakekeeper percent-encodes, so two names whose encoded forms differ only by a `%XX` would land at the same blob and overwrite each other. Set `storage-layout` to `{"type": "default"}` or omit it.

Since 0.13 the default layout is **flat** and has no `{name}` segments, so it is OneLake-safe: OneLake warehouses place new tabulars directly under the base location (`<base>/<tabular-uuid>`). See [Default](storage-layout.md#default), including how this affects namespaces created before 0.13.

## Example

A POST request to `/management/v1/warehouse` to create a OneLake-backed warehouse:

```json
{
  "warehouse-name": "onelake_dev",
  "delete-profile": { "type": "hard" },
  "storage-credential": {
    "type": "az",
    "credential-type": "client-credentials",
    "client-id": "...",
    "client-secret": "...",
    "tenant-id": "..."
  },
  "storage-profile": {
    "type": "onelake",
    "workspace-id": "0388d6cb-27fd-4dc5-948b-32ab7aab9577",
    "lakehouse-id": "eb2b7644-2ae4-43ed-ad08-8cc295ffa7ac",
    "directory-rel-path": "my_warehouse",
    "endpoint-mode": { "type": "default" }
  }
}
```

This produces the abfss base location:

```text
abfss://0388d6cb-27fd-4dc5-948b-32ab7aab9577@onelake.dfs.fabric.microsoft.com/eb2b7644-2ae4-43ed-ad08-8cc295ffa7ac/Files/my_warehouse/
```

## Client Compatibility

OneLake's blob surface is API-compatible with regular ADLS Gen2 for the operations Lakekeeper's vended-credentials path uses, but client libraries need to be OneLake-aware: they have to honour `adls.account-host` so they target `*.fabric.microsoft.com` and not the default `<account>.blob.core.windows.net`. Lakekeeper emits the right property in the catalog response; the engine still has to be on a version that consumes it.

| Client | Minimum version | Notes |
|---|---|---|
| **PyIceberg** | `0.10.0` | First release that ships both [`adls.account-host`](https://github.com/apache/iceberg-python/pull/2016) and [`adls.credential`](https://github.com/apache/iceberg-python/pull/2299). Earlier versions don't construct OneLake URLs correctly even when Lakekeeper hands them the right properties. Transitively requires `adlfs >= 2024.7.0`. |
| **Spark + Iceberg (Java)** | `iceberg-spark-runtime` ≥ `1.5` on Spark 3.5 *or* ≥ `1.10` on Spark 4 | The Java Iceberg ADLS file IO parses the host from `abfss://<fs>@<host>/...` directly, so it's transparently OneLake-compatible for any version that supports vended ADLS credentials. |

Lakekeeper's own OneLake integration tests are run against:

- **Spark** `4.0.2` (`apache/spark:4.0.2-scala2.13-java21-python3-ubuntu`) with `iceberg-spark-runtime` `1.10.1`
- **PyIceberg** `0.10.0` (with the `adlfs` extra)

Older Spark 3 / Iceberg < 1.10 combinations are exercised by other suites in the same harness (`apache/spark:3.5.6-java17-python3`); the OneLake-specific paths in vended credentials don't depend on the Spark major version.

## Updating the Storage Profile

Most fields are immutable on `update-storage-profile` because changing them would orphan every table previously written to the warehouse: `workspace-id`, `lakehouse-id`, `top-level-folder`, `directory-rel-path`, and `endpoint-mode`. The following fields can be updated: `sas-token-validity-seconds`, `sas-enabled`, `authority-host`, `storage-layout`.

## Troubleshooting

With only the Fabric setting "Use short-lived user-delegated SAS tokens" enabled, creating the warehouse fails storage validation at the `vended-credentials-read-write` step with the error type `OneLakeSasRejected`. The `vended-credentials-read-write` check's stack holds what OneLake answered, a 401 `Authentication Failed with Access token validation failed`. Enable "Authenticate with OneLake user-delegated SAS tokens" as described in [Credentials](#credentials).
