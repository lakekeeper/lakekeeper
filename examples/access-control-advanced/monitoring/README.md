# Lakekeeper monitoring stack

Local Prometheus + Grafana setup that ships with the `access-control-advanced` example. Auto-provisioned: no clicking required.

## Run

```sh
cd examples/access-control-advanced
docker compose up -d
```

Open:
- Grafana: <http://localhost:3000> (admin / admin)
- Prometheus: <http://localhost:9090>

The dashboard appears under the **Lakekeeper** folder, named **Lakekeeper Overview**.

> Tip: every panel has a small ⓘ icon in its header — hover or click it to see a description without leaving the dashboard.

## What's scraped

| Job | Target | What it provides |
|-----|--------|------------------|
| `lakekeeper` | `lakekeeper:9000` | App-level RED metrics + caches + tokio runtime |
| `openfga` | `openfga:2112` | gRPC RPS and latency for authorization calls |
| `postgres-lakekeeper` | `postgres-exporter:9187` | DB-server view of the Lakekeeper Postgres |
| `prometheus` | self | Prometheus' own health |

Alert rules live in [`alerts.yml`](alerts.yml). They show under **Alerts → Rules** in Prometheus.

## Dashboard rows

The dashboard is grouped into rows. Click a row title to collapse it.

### Overview
Single-number stats for the on-call view: are we up, how busy, how many errors, how slow.

- **Up** — Prometheus successfully scraping LK?
- **Requests / sec** — total RPS over 1m
- **Error rate (5xx)** — % of requests that 5xx'd in the last 5m
- **Apdex (T=0.25s)** — see [Apdex](#how-to-read-apdex)
- **p95 latency** — 95% of requests finished faster than this
- **In-flight** — requests currently being processed

### Requests
The traffic pattern.

- **Request rate by status class** — stacked 2xx/3xx/4xx/5xx; growing red = problems
- **Latency percentiles** — p50, p95, p99 and avg over time. Diverging p50 vs p99 means a tail-latency problem
- **Request rate by HTTP method** — separates reads (GET/HEAD) from writes (POST/PUT/DELETE)
- **Latency heatmap** — see [Reading the heatmap](#reading-the-latency-heatmap)
- **Top 10 endpoints by RPS** — what's driving traffic
- **Endpoint latency & rate (table)** — sortable per-endpoint RPS + p50/p95/p99/avg

### Errors
4xx vs 5xx, and where they come from.

- **4xx rate** — clients misbehaving (auth, bad payloads). Not LK's fault, usually
- **5xx rate** — LK's fault. Investigate when non-zero
- **Status code distribution** — per-code RPS; useful for spotting unusual codes (e.g. 429 = rate limiting)
- **Top 10 error endpoints** — which endpoints are producing the errors

### Caches
Lakekeeper has several in-memory caches (warehouses, namespaces, roles, secrets, …). All emit the same three metrics with a `cache_type` label.

- **Cache hit ratio** — hits / (hits + misses) per cache
- **Cache miss rate** — absolute misses/sec. Always read this *with* hit ratio: a 99% hit ratio still means trouble at 1000 misses/s
- **Cache size** — current number of entries

### OpenFGA (authz)
Every catalog request triggers at least one OpenFGA `Check`. If LK is slow but its own internal work is fast, look here.

- **OpenFGA RPS by gRPC method**
- **OpenFGA p95 latency by gRPC method**

### Postgres (lakekeeper db)
The DB server's view, from `postgres-exporter`. Note: this is *not* the same as Lakekeeper's sqlx pool view — see [the gap section](#what-this-dashboard-cant-show-yet).

- **Postgres connections** — by state, plus `max_connections`
- **Deadlocks & rollbacks** — non-zero deadlocks usually mean contention on a hot row

### Tokio runtime
The async runtime that drives everything. Saturation here = saturation everywhere.

- **Worker busy ratio** — fraction of time worker threads were busy. >0.8 sustained = CPU saturated
- **Live tasks & queue depth** — growing queue = scheduling faster than executing
- **Workers** — defaults to CPU core count

## Reading the latency heatmap

The heatmap is the most information-dense panel here. It's worth spending 30 seconds understanding.

**Layout:**
- X axis = time (left → right)
- Y axis = latency bucket (bottom = fast, top = slow)
- Color of each cell = how many requests fell into that latency bucket during that time slice (darker/brighter = more)

**What to look for:**

| Pattern | Meaning |
|---|---|
| One thin bright horizontal band | Healthy: most requests cluster around the same latency |
| Two horizontal bands stacked | **Bimodal** — two populations of requests with different speeds. Classic case: cache hits (fast band) vs cache misses (slow band) |
| Band drifts upward over time | Latency is creeping up — degradation |
| New bright cells appearing high up while bottom band stays | A new tail of slow requests appeared (a slow downstream, GC pauses, lock contention) |
| Vertical bright stripe across many buckets | Time-bounded incident: at that instant, requests were scattered all over |

**Why heatmaps beat percentile lines:** a single p95 line collapses everything to one number. If half your requests take 50ms (cache hit) and half take 500ms (cache miss), the p95 line shows ~500ms and looks "bad" — but the heatmap shows two clear bands and tells you exactly *why*.

## How to read Apdex

Apdex (Application Performance Index) condenses latency into a 0–1 satisfaction score against a target latency `T`:

```
Apdex = ( count(latency ≤ T) + count(latency ≤ 4T) / 2 ) / count(total)
```

With `T = 250ms`:
- A request ≤ 250ms counts as **satisfied** (full credit)
- A request ≤ 1000ms counts as **tolerating** (half credit)
- Anything slower counts as **frustrated** (no credit)

Rough interpretation:

| Apdex | What users feel |
|---|---|
| 1.00 – 0.94 | Excellent |
| 0.93 – 0.85 | Good |
| 0.84 – 0.70 | Fair — they notice |
| < 0.70 | Frustrated |

Pick a `T` that matches your SLO. The dashboard uses 250ms; change it in the panel query if your service target is different.

## Filters at the top

Two template variables:

- **Endpoint** — multi-select; filters every request panel by URL pattern
- **Method** — multi-select; filters by HTTP method

Both default to `All`. Picking a single endpoint lets you see RPS / errors / latency just for that route.

## What this dashboard *can't* show yet

Some signals an SRE would want require code changes in Lakekeeper. They're listed in [`REQUIREMENTS.md`](REQUIREMENTS.md). Highlights:

- **Lakekeeper's own DB pool stats** — pool size, in-use, acquire wait time, acquire timeouts. The Postgres panel shows the *DB-server* view, which can look healthy while LK is starving on its own pool slots.
- **Process CPU / RSS / file descriptors** — needs the `metrics-process` crate (or an external `cadvisor` sidecar).
- **Per-dependency latency** — separate histograms for OpenFGA / S3 / OIDC calls so you can attribute slow requests to the right downstream.
- **Domain-level catalog operations** — `table.commit`, `view.create`, etc., instead of HTTP-path-only metrics.

See [`REQUIREMENTS.md`](REQUIREMENTS.md) for the full list with acceptance criteria.

## Editing the dashboard

The dashboard JSON is mounted read-only in Grafana, but the provisioning is set to `allowUiUpdates: true` — meaning you can edit panels in the Grafana UI for experimentation. To persist a change, export JSON from Grafana (**Dashboard settings → JSON Model**) and overwrite [`grafana/dashboards/lakekeeper.json`](grafana/dashboards/lakekeeper.json). Grafana auto-reloads it within 30 seconds.
