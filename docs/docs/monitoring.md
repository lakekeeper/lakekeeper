# Monitoring Lakekeeper

Lakekeeper exposes Prometheus metrics and per-project endpoint statistics. We recommend integrating these into your Kubernetes/Grafana/Prometheus stack.

## Key Metrics

### HTTP Request Metrics

Three metrics cover all HTTP traffic:

| Metric                                                | Labels                               | Description |
|-------------------------------------------------------|--------------------------------------|-----|
| <code style="user-select:all">axum_http_<wbr>requests_total</code>            | `method`, `status`, `endpoint`       | Request count broken down by HTTP method, status code, and endpoint path |
| <code style="user-select:all">axum_http_<wbr>requests_pending</code>          | `method`, `endpoint`                 | Requests currently in-flight per endpoint and method |
| <code style="user-select:all">axum_http_requests_<wbr>duration_seconds</code> | `method`, `status`, `endpoint`, `le` | Response time histogram; use the `le=1` bucket as a baseline health indicator |

!!! tip "Interpreting HTTP request metrics"
    Visualize `axum_http_requests_total` by status code for overall API health. Rising 4XX rates indicate client-side issues; rising 5XX rates indicate server or database problems requiring urgent attention. High `axum_http_requests_pending` counts signal backend bottlenecks — consider scaling Lakekeeper horizontally. For latency, monitor the `le=1` bucket of `axum_http_requests_duration_seconds` as a baseline; spikes typically point to Postgres or upstream service issues.

### Cache Metrics

Lakekeeper maintains in-memory caches for Short-Term Credentials, Warehouses, Namespaces, Secrets, Roles, User Assignments, and Role Members. All caches share three metric names, differentiated by the `cache_type` label:

| Metric                                          | Type    | Labels       | Description |
|-------------------------------------------------|---------|--------------|-----|
| <code style="user-select:all">lakekeeper_cache_<wbr>size</code>         | Gauge   | `cache_type` | Current number of entries in the cache |
| <code style="user-select:all">lakekeeper_cache_<wbr>hits_total</code>   | Counter | `cache_type` | Total cache hits |
| <code style="user-select:all">lakekeeper_cache_<wbr>misses_total</code> | Counter | `cache_type` | Total cache misses |

`cache_type` values: `stc`, `warehouse`, `namespace`, `secrets`, `role`, `user_assignments`, `role_members`. A persistently low hit rate signals the cache capacity should be increased. See [Configuration > Caching](./configuration.md#caching) for details.

### Role Provider Metrics <span class="lkp"></span>

When a Role Provider (e.g. LDAP) is configured, Lakekeeper emits the following metrics, each labelled by `provider_id`:

| Metric                                                                | Type      | Labels                   | Description |
|-----------------------------------------------------------------------|-----------|--------------------------|-----|
| <code style="user-select:all">lakekeeper_<wbr>role_provider_up</code>                                    | Gauge     | `provider_id`            | `1` when the provider is reachable, `0` when unreachable. Updated by the periodic health-check loop. |
| <code style="user-select:all">lakekeeper_<wbr>role_provider_<wbr>get_roles_<wbr>duration_seconds</code>  | Histogram | `provider_id`, `outcome` | Duration of each role-lookup call. The `outcome` label reflects how the request was served (see table below). |
| <code style="user-select:all">lakekeeper_<wbr>role_provider_<wbr>sync_errors_total</code>                | Counter   | `provider_id`            | Number of failures writing fresh roles back to the Postgres catalog cache. |
| <code style="user-select:all">lakekeeper_<wbr>role_provider_<wbr>ldap_<wbr>reconnects_total</code>       | Counter   | `provider_id`, `outcome` | LDAP reconnect attempts (LDAP providers only), labelled `success` or `error`. |

**`outcome` values for `lakekeeper_role_provider_get_roles_duration_seconds`** (histogram label):

| Value            | Meaning                                                   |
|------------------|-----------------------------------------------------------|
| `cache_hit`      | All applicable providers were fresh; the external provider was not contacted. |
| `success`        | Fresh roles were fetched from the external provider and synced to Postgres. |
| `stale_fallback` | The external provider was unreachable, but previously cached roles from Postgres were served instead. Authorization continues to work. |
| `error`          | Unrecoverable error — the provider failed and no cached roles were available. |

**Health probe behavior.** Role provider health is intentionally *excluded* from the `/health` endpoint. The periodic health-check loop still calls `update_health` on every cycle (to drive reconnection attempts and keep `lakekeeper_role_provider_up` current), but an unreachable provider does **not** cause the pod to fail its liveness or readiness probe. Lakekeeper continues serving the roles it last synced to Postgres (`stale_fallback`), so authorization keeps working during a provider outage — at the cost of potentially stale group memberships.

This contrasts with the Postgres connection: if Postgres becomes unreachable, the pod **will** fail its health check (see [Database Monitoring](#database-postgres-monitoring) below).

!!! tip "Alerting on role provider health"
    Alert on `lakekeeper_role_provider_up == 0` to detect provider outages early. A sustained `stale_fallback` rate in `lakekeeper_role_provider_get_roles_duration_seconds` confirms that Lakekeeper is actively falling back to cached roles. Rising `lakekeeper_role_provider_sync_errors_total` with a healthy provider indicates a separate Postgres write problem — investigate database connectivity or permissions.

## Prometheus Integration

Lakekeeper listens on `LAKEKEEPER__BIND_IP:LAKEKEEPER__METRICS_PORT` (defaults: `0.0.0.0:9000`). The bind address `0.0.0.0` means "listen on all interfaces" — it is not a valid scrape target. Configure Prometheus to scrape a reachable address such as `http://localhost:9000/metrics` or `http://<service-or-pod-ip>:9000/metrics`.

| Variable                                | Description                        |
|-----------------------------------------|------------------------------------|
| <nobr>`LAKEKEEPER__METRICS_PORT`</nobr> | Port Lakekeeper listens on for the metrics endpoint (default `9000`) |
| <nobr>`LAKEKEEPER__BIND_IP`</nobr>      | Listener bind address for metrics, REST API, and Management API (default `0.0.0.0`; use a specific IP to restrict access) |

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

!!! warning
    Lakekeeper's liveness probe checks the database connection. If Postgres becomes unreachable or runs out of connections, the pod will fail its health check and be marked unhealthy.

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

Split Grafana dashboards by concern: API health (status codes, pending, latency), database health, cache hit/miss ratios, role provider health, and Kubernetes resource utilization. Alert on sustained 5XX/4XX spikes, high pending request counts, low cache hit rates, and `lakekeeper_role_provider_up == 0`.

## Troubleshooting

If Grafana shows stale or missing metrics, verify that Prometheus can reach the metrics endpoint and that the bind IP and port match your scrape configuration. For historical analysis beyond Prometheus retention, query endpoint statistics from the database.
