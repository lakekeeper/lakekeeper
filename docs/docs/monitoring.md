# Monitoring Lakekeeper

Lakekeeper exposes Prometheus metrics and per-project endpoint statistics. We recommend integrating these into your Kubernetes/Grafana/Prometheus stack.

## Key Metrics

### HTTP Request Metrics

Three metrics cover all HTTP traffic:

| Metric                                             | Labels                               | Description |
|----------------------------------------------------|--------------------------------------|-----|
| <nobr>`axum_http_requests_total`</nobr>            | `method`, `status`, `endpoint`       | Request count broken down by HTTP method, status code, and endpoint path |
| <nobr>`axum_http_requests_pending`</nobr>          | `method`, `endpoint`                 | Requests currently in-flight per endpoint and method |
| <nobr>`axum_http_requests_duration_seconds`</nobr> | `method`, `status`, `endpoint`, `le` | Response time histogram; use the `le=1` bucket as a baseline health indicator |

!!! tip "..."
    Visualize `axum_http_requests_total` by status code for overall API health. Rising 4XX rates indicate client-side issues; rising 5XX rates indicate server or database problems requiring urgent attention. High `axum_http_requests_pending` counts signal backend bottlenecks — consider scaling Lakekeeper horizontally. For latency, monitor the `le=1` bucket of `axum_http_requests_duration_seconds` as a baseline; spikes typically point to Postgres or upstream service issues.

### Cache Metrics

Lakekeeper maintains in-memory caches for Short-Term Credentials, Warehouses, Namespaces, Secrets, and Roles. Each exposes hit/miss/size metrics (e.g. `lakekeeper_stc_cache_hits_total`, `lakekeeper_warehouse_cache_hits_total`). A persistently low hit rate signals the cache capacity should be increased. See [Configuration > Caching](./configuration.md#caching) for the full list.

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

Split Grafana dashboards by concern: API health (status codes, pending, latency), database health, cache hit/miss ratios, and Kubernetes resource utilization. Alert on sustained 5XX/4XX spikes, high pending request counts, and low cache hit rates. Use endpoint statistics for periodic API usage audits and to inform scaling and access control decisions.

## Troubleshooting

If Grafana shows stale or missing metrics, verify that Prometheus can reach the metrics endpoint and that the bind IP and port match your scrape configuration. For historical analysis beyond Prometheus retention, query endpoint statistics from the database.

## References

- [Lakekeeper Caching Configuration](./configuration.md#caching)
- [Configuration - Endpoint Statistics](./configuration.md#endpoint-statistics)
- [Lakekeeper Logging](./logging.md)
- [Production Checklist](./production.md)
- [Axum HTTP Metrics](https://docs.rs/axum-prometheus/latest/axum_prometheus/)
- [Prometheus](https://prometheus.io/) · [Grafana](https://grafana.com/)
- [kube-state-metrics](https://github.com/kubernetes/kube-state-metrics)
- [postgres_exporter](https://github.com/prometheus-community/postgres_exporter)
- [CloudNativePG Monitoring](https://cloudnative-pg.io/documentation/current/monitoring/)
