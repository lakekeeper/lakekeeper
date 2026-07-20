# Apache Flink

This guide shows how to stream data into Lakekeeper from [Apache Flink](https://flink.apache.org/) using the Lakekeeper Java client. Because Lakekeeper vends short-lived, prefix-scoped storage credentials, Flink writes directly to your object store (S3, GCS, Azure) without any long-lived keys in your job configuration.

The companion [`flink` example](https://github.com/lakekeeper/lakekeeper-clients/tree/main/java/examples/flink) streams synthetic IoT sensor readings into a Lakekeeper **generic table** (`format: dataset`), rolling a new JSON file to the table's storage location on every checkpoint.

```
sensor-001 ┐
sensor-002 ┼─► Flink job ─► FileSink ─► s3://bucket/prefix/iot/sensor-readings/
   ...     ┘                              ├── part-0-0.json   ← 10 records
                                          ├── part-0-1.json   ← 10 records
                                          └── ...
```

## How it works

The job runs in three stages:

1. **Register + vend** — the Lakekeeper Java client creates the generic table (idempotent — a `409 Conflict` on an existing table is ignored), then reloads it with `vended=true` to obtain short-lived STS credentials scoped to the table's storage prefix.
2. **Credential wiring** — the vended credentials are injected into Hadoop S3A config (`fs.s3a.*`) **before** the Flink stream graph is built. Flink's `flink-s3-fs-hadoop` plugin picks them up via `ServiceLoader`.
3. **Streaming sink** — a `FileSink` with a custom bulk writer emits each rolled file as a valid JSON array. Files roll on every Flink checkpoint (every `BATCH_INTERVAL_MS` ms) and commit with a `.json` suffix.

!!! note "Generic tables, not Iceberg"
    This example uses Lakekeeper's [Generic Table API](generic-tables.md) — Lakekeeper catalogs the dataset for identity, governance, and credential vending, but does not commit format-specific metadata. Flink writes directly to storage. For Iceberg tables, use Flink's native Iceberg connector against the Iceberg REST endpoint instead.

## Prerequisites

- Java 11+
- A running Lakekeeper instance with a warehouse and namespace already created
- The warehouse's object-store backend reachable from where the job runs

## Configure

Copy the example environment file and fill in your values:

```sh
cp java/.env.local.example java/.env.local
```

**Required:**

| Variable | Description |
|---|---|
| `LAKEKEEPER` | Lakekeeper base URL (default: `http://localhost:8181`) |
| `WAREHOUSE_ID` | Warehouse UUID (used as the URL path prefix — use the UUID, not the warehouse name) |
| `TOKEN` | Static bearer token **—or—** the `OAUTH_*` variables below |
| `OAUTH_TOKEN_URL`, `OAUTH_CLIENT_ID`, `OAUTH_CLIENT_SECRET`, `OAUTH_SCOPE` | OAuth2 client-credentials flow (alternative to `TOKEN`) |

!!! note "Interactive login isn't for streaming jobs"
    The Java client also ships `DeviceCodeFlow` (RFC 8628) and `AuthorizationCodeFlow` with PKCE (RFC 7636) for signing a **human** in from a CLI or desktop — see [Client Authentication](generic-tables-auth.md). A Flink job runs unattended with no browser, so authenticate it as a **service account** with `ClientCredentials` (the `OAUTH_*` variables above) or a static `TOKEN` — not an interactive flow.

**Optional tuning:**

| Variable | Default | Description |
|---|---|---|
| `NAMESPACE` | `iot` | Lakekeeper namespace |
| `TABLE` | `sensor-readings` | Table name |
| `NUM_SENSORS` | `5` | Number of virtual sensors |
| `NUM_RECORDS` | `-1` | Total records to emit; `-1` streams forever |
| `BATCH_SIZE` | `10` | Records written per file |
| `BATCH_INTERVAL_MS` | `15000` | Milliseconds between batches (= file roll interval) |

## Run locally

```sh
cd java
./gradlew :examples:flink:run
```

Gradle reads `java/.env.local` automatically. The job streams until you press ++ctrl+c++.

Expected output:

```
Created  iot.sensor-readings → s3://your-bucket/prefix/iot/sensor-readings
Location: s3://your-bucket/prefix/iot/sensor-readings
[Lakekeeper] vended credential keys: [s3.access-key-id, s3.secret-access-key, s3.session-token]
Streaming 10 records/file every 15s from 5 sensors → s3://your-bucket/...
```

A new `.json` file appears at the storage location every ~15 seconds.

## Submit to a Flink cluster

Build the self-contained fat JAR:

```sh
cd java
./gradlew :examples:flink:shadowJar
# → examples/flink/build/libs/flink-<version>-all.jar
```

Submit it:

```sh
flink run examples/flink/build/libs/flink-*-all.jar
```

!!! warning "Passing credentials in production"
    The configuration is read from environment variables, which must be available to the **TaskManager** JVM. Use Flink's `env.java.opts` or cluster-level secret management to inject `TOKEN`/`OAUTH_*` — never pass secrets on the `flink run` command line in production.

## Credential vending details

Lakekeeper returns Iceberg-style `s3.*` config keys, but the Hadoop S3A filesystem that Flink uses reads `fs.s3a.*`. The example translates between them:

| Lakekeeper key | Hadoop S3A key |
|---|---|
| `s3.access-key-id` | `fs.s3a.access.key` |
| `s3.secret-access-key` | `fs.s3a.secret.key` |
| `s3.session-token` | `fs.s3a.session.token` |
| `s3.endpoint` / `client.endpoint` | `fs.s3a.endpoint` |

Two details matter when using **vended STS credentials**:

- Because a session token is present, the job sets `fs.s3a.aws.credentials.provider` to `TemporaryAWSCredentialsProvider`. The default `SimpleAWSCredentialsProvider` ignores the session token, and AWS rejects the bare STS access key with `InvalidAccessKeyId`.
- STS session policies typically omit `s3:DeleteObject`. S3A creates and then deletes directory-marker objects, so the job sets `fs.s3a.directory.marker.retention=keep` to avoid an `AccessDenied` error.

## Related

- [Generic Tables](generic-tables.md) — the catalog API this example writes against
- [Query Engines](engines.md) — connecting Iceberg-native engines to Lakekeeper
- [Storage](storage.md) — configuring S3, GCS, and Azure warehouses
