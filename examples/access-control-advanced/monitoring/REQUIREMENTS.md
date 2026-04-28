# Lakekeeper observability — requirements for engineering team

This is the gap list between what the current `/metrics` endpoint exposes and what an SRE/DevOps team needs to operate Lakekeeper in production. Items are ordered by impact-to-effort.

## What ships today (already exposed)

| Metric | Type | Source |
|---|---|---|
| `axum_http_requests_total{method,status,endpoint}` | counter | `axum-prometheus` |
| `axum_http_requests_duration_seconds_bucket{method,status,endpoint,le}` | histogram | `axum-prometheus` |
| `axum_http_requests_pending{method,endpoint}` | gauge | `axum-prometheus` |
| `lakekeeper_cache_{size,hits_total,misses_total}{cache_type}` | gauge / counter | `crates/lakekeeper/src/service/cache_metrics.rs` |
| `tokio_*` (busy ratio, queue depth, workers, busy/park durations) | various | `tokio-metrics` |

What's missing below.

---

## P0 — production blockers

### 1. Database connection pool metrics (app side)
**Why:** Connection exhaustion is a top cause of catalog incidents. Today there is no signal until requests start timing out.

**Note:** This is *not* covered by `postgres-exporter`. Postgres-exporter shows the DB-server's view (`pg_stat_activity`, deadlocks, etc.) — it can't see LK's sqlx pool wait queue. You can have a healthy postgres-exporter while LK is starving on its own pool slots.

**Required metrics:**
- `lakekeeper_db_pool_connections{pool="read|write",state="idle|in_use"}` (gauge)
- `lakekeeper_db_pool_acquire_seconds_bucket{pool, le}` (histogram) — wait time to check out a connection
- `lakekeeper_db_pool_acquire_timeouts_total{pool}` (counter)

**Where:** sqlx `PoolOptions` exposes `Pool::size()`, `Pool::num_idle()`, and `before_acquire`/`after_release` hooks. Wire these into a periodic gauge updater in `crates/lakekeeper/src/service/catalog_store/` next to the pool construction, and instrument `acquire()` calls.

**Acceptance:** `lakekeeper_db_pool_*` visible in `/metrics`; alerting on `pool_in_use / pool_size > 0.9` works.

### 2. Process-level metrics (CPU, RSS, FDs, restart count)
**Why:** Without these we can't tell whether p99 spikes are GC-like pauses, OOM pressure, or fd leaks.

**Required:** the standard `process_*` Prometheus metrics: `process_cpu_seconds_total`, `process_resident_memory_bytes`, `process_open_fds`, `process_start_time_seconds`.

**Where:** add the [`metrics-process`](https://crates.io/crates/metrics-process) crate (or equivalent) and call its collector from the same exporter setup at `crates/lakekeeper/src/metrics.rs`. Should be one extra dependency + ~5 lines.

**Acceptance:** `process_*` metrics present; dashboard can plot CPU% and RSS without an external sidecar.

### 3. Build/version info metric
**Why:** Lets us correlate metric anomalies with deploys. Already have commit SHA available (see PR #1725).

**Required:** `lakekeeper_build_info{version, git_sha, rust_version}` (gauge always = 1).

**Where:** emit at startup in `crates/lakekeeper/src/metrics.rs` using the same constants used by the server-info endpoint.

---

## P1 — high impact

### 4. Outbound call instrumentation (OpenFGA, object store, OIDC)
**Why:** Every catalog request fans out to authz + S3 + sometimes OIDC. Today we can only see the aggregate latency; we can't tell which dependency is slow.

**Required histograms (one per dependency class):**
- `lakekeeper_openfga_check_duration_seconds_bucket{check_type, le}`
- `lakekeeper_openfga_check_total{check_type, result="allow|deny|error"}`
- `lakekeeper_storage_io_duration_seconds_bucket{operation="get|put|list|delete", backend="s3|gcs|azure", le}`
- `lakekeeper_storage_io_total{operation, backend, result}`
- `lakekeeper_oidc_verify_duration_seconds_bucket{le}`

**Where:**
- OpenFGA: `crates/authz-openfga/` — wrap the gRPC client calls.
- Storage: `crates/io/` — wrap each backend's request helper.
- OIDC: token verification path in `crates/lakekeeper/src/service/authn/`.

**Acceptance:** dashboard can answer "is the spike coming from LK, OpenFGA, or S3?" without log diving.

### 5. Domain-level catalog metrics
**Why:** HTTP-path metrics conflate `POST /tables` (cheap create) with `POST /tables/{t}/commit` (expensive metadata write). SREs care about Iceberg operations, not URL templates.

**Required counters and histograms:**
- `lakekeeper_catalog_op_total{operation, warehouse, result}` — operations: `table.create|drop|commit|load`, `view.*`, `namespace.*`, `transaction.commit`, etc.
- `lakekeeper_catalog_op_duration_seconds_bucket{operation, warehouse, le}`
- `lakekeeper_catalog_table_metadata_size_bytes_bucket` (histogram of metadata.json sizes — useful for capacity planning)

**Where:** alongside the existing `endpoint-statistics` machinery. The data is already being tracked internally per the management endpoint — just emit it as Prometheus metrics in addition to the existing collection.

**Acceptance:** Grafana dashboard with a per-warehouse "table commits/sec" and "view ops/sec" view.

### 6. Background task / queue metrics
**Why:** Anything running off the request path (vended-credential refresh, soft-delete cleanup, stats rollup) is invisible today. When it falls behind, customers see stale data, not errors.

**Required:**
- `lakekeeper_task_queue_depth{queue}` (gauge)
- `lakekeeper_task_processed_total{queue, result}` (counter)
- `lakekeeper_task_duration_seconds_bucket{queue, le}` (histogram)
- `lakekeeper_task_oldest_seconds{queue}` (gauge — age of oldest pending item)

**Where:** wherever the task queue is dispatched.

---

## P2 — quality-of-life

### 7. Cache evictions counter
Distinguishes "miss because not cached yet" from "miss because evicted". Add `lakekeeper_cache_evictions_total{cache_type, reason="capacity|ttl|invalidation"}`.

**Where:** all cache modules under `crates/lakekeeper/src/service/catalog_store/*_cache.rs` and `secrets.rs`. ~1 line per eviction site.

### 8. Per-warehouse labels on existing metrics
Today `axum_http_requests_total` has no warehouse dimension. With multi-tenant warehouses on a single Lakekeeper, we can't see "warehouse X is slow". Adding the warehouse label *can* explode cardinality — discuss with the team whether to:

- a) Add it only to the new `lakekeeper_catalog_op_*` metrics (P1#5), or
- b) Provide an opt-in env var `LAKEKEEPER__METRICS__WAREHOUSE_LABEL=true`.

### 9. Configurable histogram buckets
The default `axum-prometheus` bucket set tops out at 10s — fine for most catalog ops, but stat/maintenance endpoints can run longer. Make buckets configurable via env (`LAKEKEEPER__METRICS__HTTP_BUCKETS=...`).

### 10. OpenTelemetry / tracing parity
The `/metrics` story is reasonable; the trace story isn't covered here. If the team wants a single ask: emit OTLP-compatible spans for the same outbound calls covered in P1#4, so trace UIs (Tempo/Jaeger) can drill into a slow request.

---

## What we ship from infra (no Lakekeeper changes)

These are already plumbed in `examples/access-control-advanced/monitoring/`:

- **Prometheus** scraping `lakekeeper:9000`, `openfga:2112`, `postgres-exporter:9187`
- **OpenFGA RPC histograms** enabled via `OPENFGA_METRICS_ENABLE_RPC_HISTOGRAMS=true`
- **`postgres-exporter`** for DB-side connection / deadlock / rollback metrics
- **Grafana dashboard** `Lakekeeper Overview` (auto-provisioned)
- **Alert rules** in `monitoring/alerts.yml`: `LakekeeperDown`, `LakekeeperHigh5xxRate`, `LakekeeperHigh4xxRate`, `LakekeeperHighLatencyP95`, `LakekeeperRuntimeBusy`, `LakekeeperLowCacheHitRate`, `OpenFGADown`

Things still owned by deployment (Helm chart / Terraform), not Lakekeeper code:
- **cAdvisor / kube-state-metrics** for container CPU/RSS at the orchestration layer (until P0#2 lands).
- **node_exporter** for host-level metrics.
- **Alertmanager** routing.

---

## Suggested rollout

1. Sprint 1: P0#1 (DB pool) + P0#2 (process metrics) + P0#3 (build info). Small, no API surface change.
2. Sprint 2: P1#4 (outbound call latency) — biggest single jump in incident-debugging power.
3. Sprint 3: P1#5 (domain metrics) + P1#6 (queue metrics).
4. Backlog: P2 items as needed.
