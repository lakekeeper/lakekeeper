---
description: "How Lakekeeper Warehouses store data: storage profiles and credentials, the supported object stores (S3, STACKIT, ADLS, OneLake, GCS), table locations, credential vending, remote signing and CORS."
---

# Storage

Each Warehouse stores its data in one location, described by the Warehouse's storage profile. Lakekeeper accesses that location with the Warehouse's storage credential and gives query engines access to the tables in it. Profile and credential are set when the Warehouse is created, and Lakekeeper [validates](#validating-a-storage-configuration) them before it saves them.

## Supported Storage

<a id="configuration-parameters"></a>

Each storage has its own page with its configuration parameters, credentials and setup steps:

| Storage | Profile `type` | Vended credentials | Remote signing | System identity | [LoQE](engines.md#loqe) |
|---|---|---|---|---|---|
| [S3](storage-s3.md): AWS, S3-compatible storage, Cloudflare R2, Alibaba Cloud OSS | `s3` | STS, where the storage offers it | Yes | AWS | Yes |
| [STACKIT Object Storage](storage-stackit.md) | `stackit` | STS on a credentials group | Yes | No | Yes |
| [Azure Data Lake Storage Gen2](storage-adls.md) | `adls` | SAS tokens | No | Yes | Read-only |
| [OneLake (Microsoft Fabric)](storage-onelake.md) | `onelake` | User-delegated SAS tokens | No | Yes | No |
| [Google Cloud Storage](storage-gcs.md) | `gcs` | Downscoped STS tokens | No | Yes | Yes |

A system identity is the identity the Lakekeeper process runs as, such as an instance profile or a managed identity. Warehouses that use it need no stored credential; each storage page explains how to enable it.

LoQE reads storage only with vended credentials, because DuckDB does not support remote signing.

## Locations

Table and view locations must use the scheme of the Warehouse's storage, which most query engines expect. Lakekeeper assigns it to tables created without a location. A location the client provides must use it, or one of the alternative schemes below when `allow-alternative-protocols` is enabled.

| Storage | Scheme | With `allow-alternative-protocols` |
|---|---|---|
| S3 and STACKIT | `s3://` | S3 also accepts `s3a://` and `s3n://` |
| ADLS and OneLake | `abfss://` | ADLS also accepts `wasbs://` |
| GCS | `gs://` | - |

Alternative protocols are meant for registering legacy Hadoop-based tables. Tables with `s3a://` paths are not accessible outside the Java ecosystem.

## How Clients Access Data

Lakekeeper gives query engines access to table data in one of two ways:

- **Vended credentials**: Lakekeeper issues temporary credentials and returns them with the table. It downscopes them to the table's location and ensures that no two table locations in a Warehouse overlap.
- **Remote signing** (S3 and STACKIT): the client sends the headers of each S3 request to Lakekeeper's sign endpoint. Lakekeeper checks that the request is allowed, signs it with its own credentials and returns the signature headers. The client then sends the request to the storage itself.

Clients choose with the `X-Iceberg-Access-Delegation` header, which takes the Iceberg REST values `vended-credentials` and `remote-signing`, plus Lakekeeper's `client-managed`:

- `client-managed` returns neither credentials nor signing information, so the client uses its own credentials.
- `vended-credentials` or `remote-signing` uses that method if the storage profile enables it.
- With both values or no header, Lakekeeper vends credentials if the profile enables it, and falls back to remote signing otherwise.
- A profile that disables both returns no credentials, whatever the header says.

If a client does not implement the method Lakekeeper offers — DuckDB, for example, does not support remote signing — it needs its own storage credentials. For S3, region, endpoint and path-style settings are still returned in the table `config`, but no `storage-credentials` entry is returned.

### Disabling Credential Vending and Remote Signing

Each storage profile turns the methods on or off for its Warehouse. Disabled methods are never offered to clients, whatever the request headers say:

| Storage | Credential vending | Remote signing |
|---|---|---|
| S3 and STACKIT | `sts-enabled` | `remote-signing-enabled` |
| ADLS and OneLake | `sas-enabled` | - |
| GCS | `sts-enabled` | - |

Remote signing applies to [Generic Tables](./generic-tables.md) as well as Iceberg tables.

Remote signing keeps a client inside its table only if the object store checks every part of a signed request, as AWS S3 does. A store that does not lets a client change a request after it was signed, for example to read or delete data outside its table, so prefer vended credentials there. Clients that may write a table can also set tags on its files, so do not base access or lifecycle rules on object tags.

## CORS

[LoQE, the in-browser query console](engines.md#loqe), reads and writes table data directly from object storage, so the bucket must return a CORS (Cross-Origin Resource Sharing) policy that allows requests from the Lakekeeper origin. This applies to S3, STACKIT, Google Cloud Storage and ADLS, which LoQE only reads; LoQE does not support OneLake. It also applies only when the Warehouse vends credentials, because LoQE cannot use remote signing. [Storage validation](storage-validation.md) reports a `cors-origin-allowed` warning when the Lakekeeper origin is not allowed.

Each storage page has the policy and how to apply it: [S3](storage-s3.md#cors), [STACKIT](storage-stackit.md#cors), [Google Cloud Storage](storage-gcs.md#cors), [ADLS](storage-adls.md#cors).

## Storage Layout

The storage layout controls how namespace and table directories are structured under the Warehouse location. It is set with the `storage-layout` field of the storage profile; see [Storage Layout](storage-layout.md).

## Validating a Storage Configuration

Creating a Warehouse or updating its storage fails if any storage or configuration check fails. The validation endpoints run the same checks without saving anything and report each one; see [Storage Validation](storage-validation.md).
