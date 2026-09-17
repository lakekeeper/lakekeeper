---
description: "Monitor Lakekeeper with Prometheus metrics and per-project endpoint statistics, and wire them into a Kubernetes, Prometheus and Grafana stack."
---

# Monitoring Lakekeeper

Lakekeeper exposes Prometheus metrics and per-project endpoint statistics. We recommend integrating these into your Kubernetes/Grafana/Prometheus stack.

## Key Metrics

### HTTP Request Metrics

Three metrics cover all HTTP traffic:

| Metric                                                                   | Labels                               | Description |
|--------------------------------------------------------------------------|--------------------------------------|-----|
| <code class="selectable">axum_http_<wbr>requests_total</code>            | `method`, `status`, `endpoint`       | Request count broken down by HTTP method, status code, and endpoint path |
| <code class="selectable">axum_http_<wbr>requests_pending</code>          | `method`, `endpoint`                 | Requests currently in-flight per endpoint and method |
| <code class="selectable">axum_http_requests_<wbr>duration_seconds</code> | `method`, `status`, `endpoint`, `le` | Response time histogram; use the `le=1` bucket as a baseline health indicator |

!!! tip "Interpreting HTTP request metrics"
    Visualize `axum_http_requests_total` by status code for overall API health. Rising 4XX rates indicate client-side issues; rising 5XX rates indicate server or database problems requiring urgent attention. High `axum_http_requests_pending` counts signal backend bottlenecks — consider scaling Lakekeeper horizontally. For latency, monitor the `le=1` bucket of `axum_http_requests_duration_seconds` as a baseline; spikes typically point to Postgres or upstream service issues.

### Tokio Metrics

Lakekeeper emits all default [Tokio Runtime Metrics](https://github.com/tokio-rs/tokio-metrics?tab=readme-ov-file#runtime-metrics), including "unstable" metrics. A detailed description of these metrics, including how they are derived, can be found in the [tokio_metrics crate documentation](https://docs.rs/tokio-metrics/latest/tokio_metrics/struct.RuntimeMetrics.html#fields).

### Cache Metrics

Lakekeeper maintains in-memory caches for Short-Term Credentials, Warehouses, Namespaces, Secrets, Roles, User Assignments, Role Members, and Role Ancestors. All caches share three metric names, differentiated by the `cache_type` label:

| Metric                                                             | Type    | Labels       | Description |
|--------------------------------------------------------------------|---------|--------------|-----|
| <code class="selectable">lakekeeper_cache_<wbr>size</code>         | Gauge   | `cache_type` | Current number of entries in the cache |
| <code class="selectable">lakekeeper_cache_<wbr>hits_total</code>   | Counter | `cache_type` | Total cache hits |
| <code class="selectable">lakekeeper_cache_<wbr>misses_total</code> | Counter | `cache_type` | Total cache misses |

`cache_type` values: `stc`, `warehouse`, `warehouse_name_to_id`, `namespace`, `namespace_ident_to_id`, `secrets`, `role`, `role_ident_to_id`, `user_assignments`, `role_members`, `role_ancestors`, `shared_role_idents`, `shared_project_ids`. Lakekeeper Plus adds `admission_enforce` (see [External Enforce Gate](#external-enforce-gate)) and `table_metadata` (see [Table Metadata Cache](#table-metadata-cache)). A persistently low hit rate signals the cache capacity should be increased. Two caches are exceptions. For `table_metadata`, a low hit rate is expected. For `admission_enforce`, a short TTL or many distinct subjects is the more common cause. See [Configuration > Caching](./configuration.md#caching) for details.

Role-membership cache invalidation emits one additional metric:

| Metric                                                                                | Type      | Labels      | Description |
|---------------------------------------------------------------------------------------|-----------|-------------|-----|
| <code class="selectable">lakekeeper_role_<wbr>membership_edge_<wbr>fanout_users</code> | Histogram | `operation` | Users invalidated by one role-to-role membership edge change |

`operation` is `add` or `remove`. The user-assignments cache stores a fully-expanded transitive closure. One edge change can therefore invalidate many users at once. A high p99 means a single edit fans out widely. Lakekeeper also logs a `warn` when one change invalidates more than 1000 users.

The same edge change clears the role-ancestors cache in full. It alters the ancestors of the member role and everything nested below it. `lakekeeper_cache_size{cache_type="role_ancestors"}` is an approximate count maintained by background maintenance. It falls after a clear rather than at the moment of one, so do not alert on it reaching zero promptly. Under the OpenFGA backend the series is not emitted at all. OpenFGA resolves role nesting from its own tuples and never reads this cache. Alert on absence there, not on a zero value.

#### Table Metadata Cache { #table-metadata-cache .lkp }

During [table maintenance](./table-maintenance.md#expire-snapshots), Lakekeeper caches parsed Iceberg manifests and manifest lists under `cache_type="table_metadata"`. This cache is bounded by the memory its entries occupy rather than by their number, and one entry ranges from a few KiB to several MiB, so `lakekeeper_cache_size` says little about the memory held. Two further metrics cover that:

| Metric                                                                    | Type  | Labels       | Description |
|---------------------------------------------------------------------------|-------|--------------|-----|
| <code class="selectable">lakekeeper_cache_<wbr>weighted_bytes</code> | Gauge | `cache_type` | Bytes held across all live instances of the cache |
| <code class="selectable">lakekeeper_cache_<wbr>instances</code>      | Gauge | `cache_type` | Live instances of the cache in this process |

One cache is built per maintenance task rather than once per process, so its budget applies once per running task and `lakekeeper_cache_weighted_bytes` is the sum across the `lakekeeper_cache_instances` that are live. Peak memory therefore scales with how many maintenance tasks run at once: raising `LAKEKEEPER__TASK_EXPIRE_SNAPSHOTS_WORKERS` or `LAKEKEEPER__TASK_REMOVE_ORPHAN_FILES_WORKERS` raises it proportionally. Size a pod's memory limit against the observed value at your own worker counts rather than against a single task.

Both gauges return to zero once the last task finishes, because each cache is dropped with the task that built it.

A low hit rate here does not mean the cache is too small, unlike the other caches. Every task starts with an empty cache and reads most manifests once, so misses dominate by design. Judge the budget by whether `lakekeeper_cache_weighted_bytes` plateaus — at capacity, manifests are evicted and re-read from object storage — and by the headroom the peak leaves against the container memory limit.

### Role Provider Metrics { .lkp }

When a Role Provider (e.g. LDAP) is configured, Lakekeeper emits the following metrics, each labelled by `provider_id`:

| Metric                                                                                             | Type      | Labels                   | Description |
|----------------------------------------------------------------------------------------------------|-----------|--------------------------|-----|
| <code class="selectable">lakekeeper_<wbr>role_provider_up</code>                                   | Gauge     | `provider_id`            | `1` when the provider is reachable, `0` when unreachable |
| <code class="selectable">lakekeeper_<wbr>role_provider_<wbr>get_roles_<wbr>duration_seconds</code> | Histogram | `provider_id`, `outcome` | Duration of each role-lookup call |
| <code class="selectable">lakekeeper_<wbr>role_provider_<wbr>sync_errors_total</code>               | Counter   | `provider_id`            | Failures writing fresh roles back to the Postgres catalog cache |
| <code class="selectable">lakekeeper_<wbr>role_provider_<wbr>ldap_<wbr>reconnects_total</code>      | Counter   | `provider_id`, `outcome` | LDAP reconnect attempts |

Only providers with an external backend (LDAP) emit `up`, the role-lookup histogram and the reconnect counter. The periodic health-check loop refreshes `up`. On the reconnect counter, `outcome` is `success` or `error`. The OIDC token provider has no external dependency, so it reports none of those. It emits `sync_errors_total` alone, and only with `persist_token_roles` enabled.

`outcome` values on the role-lookup histogram:

| Value                         | Meaning                                      |
|-------------------------------|----------------------------------------------|
| `cache_hit`                   | All applicable providers were fresh; the external provider was not contacted. |
| `success`                     | Fresh roles were fetched from the external provider and synced to Postgres. |
| `stale_fallback` | The external provider was unreachable; cached roles from Postgres were served instead. |
| `error`                       | Unrecoverable error — the provider failed and no cached roles were available. |

**Health probe behavior.** Role provider health is intentionally *excluded* from the `/health` endpoint. The periodic health-check loop still calls `update_health` on every cycle, which drives reconnection attempts and keeps `lakekeeper_role_provider_up` current. An unreachable provider does **not** fail the pod's liveness or readiness probe. Lakekeeper keeps serving the roles it last synced to Postgres (`stale_fallback`). Authorization therefore keeps working during a provider outage, at the cost of potentially stale group memberships.

The Postgres connection differs. If Postgres becomes unreachable, the pod **will** fail its health check (see [Database Monitoring](#database-postgres-monitoring) below). `/health` returns `200 OK` only when the aggregate health state is `ok`. It returns `503 Service Unavailable` when that state is `error` or `unknown`.

!!! tip "Alerting on role provider health"
    Alert on `lakekeeper_role_provider_up == 0 or absent(lakekeeper_role_provider_up{provider_id="<your-provider>"})` to detect provider outages early. The `== 0` clause alone misses a provider that never reported. The series exists only for external-backed providers (LDAP), and only after the first health-check cycle. Pin the `absent()` clause to the `provider_id`s you expect. A sustained `stale_fallback` rate in `lakekeeper_role_provider_get_roles_duration_seconds` confirms Lakekeeper is falling back to cached roles. Rising `lakekeeper_role_provider_sync_errors_total` means roles are not reaching Postgres. For an LDAP provider that is a database connectivity or permissions problem. For the OIDC token provider (`persist_token_roles`) it is a failure persisting token roles for definer-view reuse.

### Admission Gate Metrics

[Admission gates](./admission.md) run once per authenticated request, before any handler. The enforce-endpoint gate ships with Lakekeeper Plus.

| Metric                                                                              | Type      | Labels            | Description |
|-------------------------------------------------------------------------------------|-----------|-------------------|-----|
| <code class="selectable">lakekeeper_<wbr>admission_gate_<wbr>duration_seconds</code> | Histogram | `gate`, `outcome` | Latency the caller paid for this gate, cache hits included |

`outcome` values:

- `admitted`
- `skipped` — the gate does not govern this request, for example when it is scoped to another identity provider
- `forbidden` — `403`
- `unavailable` — failed closed, `503` with `Retry-After`

!!! tip "Alerting on any admission gate"
    In `axum_http_requests_total{status="403"}` an admission denial looks identical to an authorization denial. Use this metric's `outcome` instead. Split latency quantiles by `outcome`. Cache hits and `skipped` sit in the lowest bucket, so an un-split p99 stays flat even when every cache miss pays the full timeout. A gate that is not running emits no series at all. Confirm enforcement from the startup log, which names the identity provider each gate governs.

#### External Enforce Gate { #external-enforce-gate .lkp }

| Metric                                                                                      | Type      | Labels                                | Description |
|---------------------------------------------------------------------------------------------|-----------|---------------------------------------|-----|
| <code class="selectable">lakekeeper_<wbr>admission_enforce_<wbr>call_duration_seconds</code> | Histogram | `check`, `outcome`                    | Duration of one `POST` to your enforce endpoint |
| <code class="selectable">lakekeeper_<wbr>admission_enforce_<wbr>decisions_total</code>       | Counter   | `check`, `kind`, `decision`, `source` | One per check per answered request, cache replays included |
| <code class="selectable">lakekeeper_<wbr>admission_enforce_<wbr>fail_closed_total</code>     | Counter   | `reason` | No verdict obtainable for a check |
| <code class="selectable">lakekeeper_<wbr>admission_enforce_<wbr>roles_dropped_total</code>   | Counter   | `reason` | Roles resolved, then dropped |

Label values:

| Label                                 | Values |
|---------------------------------------|--------|
| `kind`                                | `gating`, `role_granting` |
| `decision`                            | `allow`, `deny` |
| `source`                              | `cache`, `upstream` |
| `outcome`                             | `allow` (`2xx`), `deny` (exactly `403`), `unavailable` (anything else, including timeouts) |
| `reason` on `fail_closed_total`       | `upstream`, `no_bearer_token`, `no_principal` |
| `reason` on `roles_dropped_total`     | `no_project` |

Buckets stop at 10s. Cached decisions make no call, so `_count` measures load on your endpoint, not the request rate.

A check that fails closed records no decision. `upstream` counts per failed check, so one rejected request can increment it several times. `no_bearer_token` and `no_principal` (a `403`, not a `503`) are unreachable on the shipped server. Nonzero means a [custom build](./customize.md) runs the gate unauthenticated. `no_project` means the request named no project to scope roles to, so the caller ran with fewer privileges than your endpoint granted.

A `role_granting` `deny` withholds a role but still admits the request, so no error series moves. These metrics name no principal. To find who lost a role, read the audit record `operation="admission_enforce_check"` (see [Logging](./logging.md#operational-audit-events)).

Decisions are cached per `(subject, check)` and report as `cache_type="admission_enforce"` in the [cache metrics](#cache-metrics). `cache_ttl_secs` bounds how long an entitlement change takes to apply, in both directions. A revoked entitlement keeps working, and a new one keeps being refused, until the entry expires. The cache is per replica, with no invalidation and no flush. Size `cache_max_entries` for active subjects × checks.

## Prometheus Integration

Lakekeeper listens on `LAKEKEEPER__BIND_IP:LAKEKEEPER__METRICS__PORT` (defaults: `0.0.0.0:9000`). The bind address `0.0.0.0` means "listen on all interfaces" — it is not a valid scrape target. Configure Prometheus to scrape a reachable address such as `http://localhost:9000/metrics` or `http://<service-or-pod-ip>:9000/metrics`.

| Variable                                                      | Description  |
|---------------------------------------------------------------|--------------|
| <code class="selectable">LAKEKEEPER__<wbr>METRICS__PORT</code> | Port Lakekeeper listens on for the metrics endpoint (default `9000`) |
| <code class="selectable">LAKEKEEPER__<wbr>BIND_IP</code>      | Listener bind address for metrics, REST API, and Management API (default `0.0.0.0`) |

```yaml title="Example Prometheus scrape configuration"
scrape_configs:
  - job_name: "lakekeeper"
    static_configs:
      - targets: ["lakekeeper-host:9000"]
```

## Database (Postgres) Monitoring

Postgres is Lakekeeper's primary backend. Use [postgres_exporter](https://github.com/prometheus-community/postgres_exporter) for database-internal signals — kube-state-metrics covers Kubernetes API object state (pods, deployments, nodes) but not Postgres internals.

| Signal                                | Recommended tool                     |
|---------------------------------------|--------------------------------------|
| Free connection pool slots            | `postgres_exporter`                  |
| Connection failures / pool exhaustion | `postgres_exporter`                  |
| Query latency                         | `postgres_exporter`                  |
| Replication lag                       | `postgres_exporter`                  |
| Disk usage and IOPS                   | Cloud provider metrics or `node_exporter` |
| Pod restarts, deployment health       | kube-state-metrics                   |

If you run Postgres via the [CloudNativePG](https://cloudnative-pg.io/) operator, its built-in per-instance exporter (port `9187`, metrics prefixed `cnpg_collector_*`) covers WAL file counts and size, archive status, sync replica state, and basic liveness — complementing `postgres_exporter` for those signals. Connection pool slots, query latency, and replication lag are available as [user-defined custom queries](https://cloudnative-pg.io/documentation/current/monitoring/#user-defined-metrics) in CloudNativePG; disk and IOPS still require `node_exporter` or cloud provider metrics.

### Connection Pool (client-side)

`postgres_exporter` reports the Postgres *server's* connection slots. Lakekeeper additionally exposes its own *client-side* pools — the separate read and write pools each replica holds, sized by `LAKEKEEPER__PG_READ_POOL_CONNECTIONS` and `LAKEKEEPER__PG_WRITE_POOL_CONNECTIONS`. A client pool can saturate even when the server has free slots, so monitor both. The read and write pools are reported separately via the `pool` label.

| Metric                                                                          | Type    | Labels                                            | Description |
|---------------------------------------------------------------------------------|---------|---------------------------------------------------|-----|
| <code class="selectable">lakekeeper_catalog_pg_<wbr>pool_connections</code>             | Gauge   | `pool` (`read`/`write`), `state` (`in_use`/`idle`) | Live connections held by the pool |
| <code class="selectable">lakekeeper_catalog_pg_<wbr>pool_max_connections</code>         | Gauge   | `pool`                                            | Configured pool ceiling |
| <code class="selectable">lakekeeper_catalog_pg_<wbr>pool_acquire_timeouts_total</code>  | Counter | `pool`                                            | Connection acquisitions that timed out — direct evidence of pool exhaustion |

!!! tip "Alerting on pool saturation"
    Utilization `lakekeeper_catalog_pg_pool_connections{state="in_use"} / lakekeeper_catalog_pg_pool_max_connections` approaching `1` is the leading edge of exhaustion. Any nonzero rate on `lakekeeper_catalog_pg_pool_acquire_timeouts_total` means requests are already being delayed or failing — alert on it. The gauges are sampled every 15s, so brief spikes may be smoothed; the timeout counter captures every occurrence. The counter covers transaction acquisition (the path catalog reads and writes use), not ad-hoc direct-pool queries.

!!! warning
    Lakekeeper's `/health` endpoint checks the database connection. If Postgres becomes unreachable or runs out of connections, `/health` returns `503 Service Unavailable`, so standard Kubernetes HTTP probes fail and the pod is marked unhealthy or unready.

Use `/health` for readiness probes when traffic should only be sent to pods with a healthy Postgres connection. It is also suitable as a liveness probe if your deployment wants Kubernetes to restart pods whose database-dependent health remains unhealthy.

```yaml title="Example Kubernetes probes"
livenessProbe:
  httpGet:
    path: /health
    port: 8181
readinessProbe:
  httpGet:
    path: /health
    port: 8181
```

## Kubernetes and Resource Monitoring

Monitor pod CPU, memory, and restart counts with kube-state-metrics or equivalent tooling.

## Endpoint Statistics

Lakekeeper aggregates per-request statistics in memory and flushes them to the database periodically (default every 30 s). Each record captures the HTTP method, endpoint path, response status code, project, and warehouse (where applicable). This data is stored internally by Lakekeeper and is accessible without a Prometheus setup.

These statistics can be viewed in the UI under the Project View's **Statistics** tab. The Management API also exposes them directly:

- `POST /management/v1/endpoint-statistics` — query endpoint-level usage data, filterable by warehouse, status code, and time window.
- `GET /management/v1/warehouse/{warehouse_id}/statistics` — query warehouse-level table and view counts.

For real-time traffic visibility, the [HTTP request metrics](#http-request-metrics) expose per-second counters and latency histograms via Prometheus — but only with `method`, `status`, and `endpoint` labels. They carry no project or warehouse dimensions, so they cannot be used for tenant-scoped analysis. Endpoint statistics are the only source of per-project and per-warehouse breakdowns, making them the right tool for chargeback, abuse detection, and per-customer analytics in multi-tenant deployments.

The flush interval is controlled by `LAKEKEEPER__ENDPOINT_STAT_FLUSH_INTERVAL` (supports `s` and `ms` units):

```env
LAKEKEEPER__ENDPOINT_STAT_FLUSH_INTERVAL=60s
```

See [Configuration - Endpoint Statistics](./configuration.md#endpoint-statistics) for details.

## Best Practices

Split Grafana dashboards by concern: API health (status codes, pending, latency), database health, cache hit/miss ratios, role provider health, and Kubernetes resource utilization. Alert on sustained 5XX/4XX spikes, high pending request counts, low cache hit rates, and `lakekeeper_role_provider_up == 0` (combined with `absent(...)` to catch a provider that never reported).

## Troubleshooting

If Grafana shows stale or missing metrics, verify that Prometheus can reach the metrics endpoint and that the bind IP and port match your scrape configuration. For historical analysis beyond Prometheus retention, query endpoint statistics from the database.
