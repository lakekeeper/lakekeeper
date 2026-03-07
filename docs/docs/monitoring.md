# Monitoring Lakekeeper

Lakekeeper exposes a comprehensive set of Prometheus metrics and endpoint statistics, and we recommend integrating these into your Kubernetes/Grafana/Prometheus monitoring stack.

## Key Metrics

### HTTP Request Metrics

#### 1. `axum_http_requests_total`

- **Description:** Total number of HTTP requests broken down by endpoint, status, and HTTP method.
- **Example:** `axum_http_requests_total{method="GET",status="200",endpoint="/management/v1/whoami"} 1`
- **Recommendation:**  
  Visualize in Grafana by status code for overall API health.  
  Increased 4XX errors indicate client issues.  
  Increased 5XX errors indicate Lakekeeper/server/database problems and require urgent intervention.

#### 2. `axum_http_requests_pending`

- **Description:** Number of HTTP requests currently being processed (pending) by each endpoint/method.
- **Example:** `axum_http_requests_pending{method="POST",endpoint="/catalog/v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics"} 0`
- **Recommendation:**  
  High or growing pending requests often indicate backend bottlenecks or the need to scale Lakekeeper horizontally.

#### 3. `axum_http_requests_duration_seconds`

- **Description:** Request duration histogram by method, endpoint, and status, with buckets for response time.
- **Example:** `axum_http_requests_duration_seconds_bucket{method="GET",status="400",endpoint="/management/v1/info",le="1"} 14`
- **Recommendation:**  
  Monitor the "le=1" bucket for general response health.  
  Spikes in latencies typically point to issues with Postgres or external services.  
  Adjust thresholds based on your deployment's real-world baseline.

### Cache Metrics

Lakekeeper uses various in-memory caches (Short-Term Credentials, Warehouse, Namespace, Secrets, and Role) to improve performance. Each cache exposes Prometheus metrics for size, hit-rate, and misses.

- **Examples:**  
  `lakekeeper_stc_cache_hits_total`, `lakekeeper_warehouse_cache_hits_total`, etc.  
  For full descriptions of caching options and exposed metrics, see [Configuration > Caching](./configuration.md#caching)
- **Recommendation:**  
  Watch hit/miss ratios to identify cache sizing needs. A low hit rate may indicate the need to increase cache capacity.

## Prometheus Integration

### Metrics Endpoint

- **Default URL:**  
  `http://<bind_ip>:<metrics_port>/metrics`  
  By default, `bind_ip` is `0.0.0.0` and `metrics_port` is `9000`

- **Configuration:**  
  `LAKEKEEPER__METRICS_PORT`: Port where the Prometheus metrics endpoint is served.  
  `LAKEKEEPER__BIND_IP`: The IP address to bind for the metrics endpoint. This also changes the IP address to bind to for Iceberg REST API and Lakekeeper Management API.

**Example Prometheus scrape configuration:**

```yaml
scrape_configs:
  - job_name: "lakekeeper"
    static_configs:
      - targets: ["lakekeeper-host:9000"]
```

## Database (Postgres) Monitoring

Lakekeeper's primary backend is Postgres. It’s critical to proactively monitor Postgres using standard tools (e.g., kube-state-metrics, pg_exporter, or your cloud provider's monitoring).

**Recommended Postgres metrics:**

- Number of free pool slots
- Connection failures
- Query latency
- Replication lag (if using read replicas)
- Disk usage and IOPS

Lakekeeper’s liveness probe checks the database connection; if Postgres becomes unreachable (down, or no free connections), the pod will fail its health check and be marked unhealthy.

## Kubernetes and Resource Monitoring

Monitor Lakekeeper pods' resource usage (CPU, memory, restarts, etc.) with kube-state-metrics or similar solutions.

## Endpoint Statistics

Lakekeeper collects and persists detailed endpoint usage statistics on a per-project basis. These can help you track usage patterns, load, and potential abuse over time, complementing the live Prometheus metrics.

### What is Collected

- **Granularity:**  
  Per HTTP Method (e.g., `GET`/`POST`)  
  Per Endpoint (e.g., `/catalog/v1/warehouses/{warehouse}/namespaces`)  
  Response HTTP Status Code (e.g. 200, 404, 500)  
  Project  
  Warehouse (optional, when applicable)  
  Count (aggregated: how many times each endpoint/status/method/project/warehouse combination was seen)
- **Timing:**  
  Statistics are aggregated in memory within each Lakekeeper instance. Periodically (default: every 30 seconds), statistics are written to the backend database and the in-memory counters are reset.
- **Configuration:**  
  `LAKEKEEPER__ENDPOINT_STAT_FLUSH_INTERVAL`: Interval at which endpoint statistics are persisted to the database. Default: 30s (supports s or ms units).

### Usage and Access

- **Accessing Statistics:**  
  These statistics are not directly exposed via a user-facing API.  
  For immediate, real-time traffic and health, always use the Prometheus /metrics endpoint.  
  For longer-term, historical usage (e.g., by project, for chargeback or abuse detection), query the statistics from the database.
- **Interpreting Statistics:**  
  Use statistics to spot API hot-spots or changes in request/response volume or error distribution by tenant or group.  
  For multi-tenant setups, project- and warehouse-level segregation enables per-customer analytics and abuse monitoring.
- **Best Practice:**  
  Leverage endpoint statistics for periodic audits of API usage and to inform scaling or access control decisions.

### Example Configuration

```env
LAKEKEEPER__ENDPOINT_STAT_FLUSH_INTERVAL=60s  # Flush endpoint stats every 60 seconds
```

For more details, see [Configuration - Endpoint Statistics](./configuration.md#endpoint-statistics)

## Best Practices

- Split out metrics dashboards by topic. Example dashboard areas:
  API health (HTTP request codes, pending, durations)
  Backend/database health
  Cache statistics (hit/miss)
  Resource efficiency (Kubernetes resources)

- Set up alerting for spikes in 5XX/4XX responses, high pending requests, or degraded cache hit ratios.

- Regularly review and tune thresholds (some may need baseline adjustment based on your environment).

## Troubleshooting & Integration Tips

- If Grafana does not show fresh metrics, ensure Prometheus can access your Lakekeeper instance (metrics endpoint and network access).

- Confirm the correct bind IP/port for each environment and update your Prometheus scrape targets accordingly.

- For historical analysis beyond the span of Prometheus retention, query endpoint statistics from the database where available.

## References & Further Reading

- [Lakekeeper Caching Configuration](./configuration.md#caching)
- [Configuration - Endpoint Statistics](./configuration.md#endpoint-statistics)
- [Lakekeeper Logging](./logging.md)
- [Production Checklist](./production.md)
- [Axum HTTP Metrics](https://docs.rs/axum-prometheus/latest/axum_prometheus/)
- [Prometheus](https://prometheus.io/), [Grafana](https://grafana.com/)
- [kube-state-metrics](https://github.com/kubernetes/kube-state-metrics)
- [pg_exporter](https://github.com/prometheus-community/postgres_exporter)
