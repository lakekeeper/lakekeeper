# Storage

Storage in Lakekeeper is bound to a Warehouse. Each Warehouse stores data in a location defined by a `StorageProfile` attached to it.

Currently, we support the following storages:

- S3 (tested with AWS & Minio)
- Azure Data Lake Storage Gen 2
- OneLake (Microsoft Fabric)
- Google Cloud Storage (with and without Hierarchical Namespaces)

When creating a Warehouse or updating storage information, Lakekeeper validates the configuration.

By default, Lakekeeper Warehouses enforce specific URI schemas for tables and views to ensure compatibility with most query engines:

- **S3 / AWS Warehouses**: Must start with `s3://`
- **Azure / ADLS Warehouses**: Must start with `abfss://`
- **GCP Warehouses**: Must start with `gs://`

When a new table is created without an explicitly specified location, Lakekeeper automatically assigns the appropriate protocol based on the storage type. If a location is explicitly provided by the client, it must adhere to the required schema.

## Disabling Credential Vending & Remote Signing

Lakekeeper provides multiple ways to control how credentials and remote signing information are provided to clients.

You can disable credential vending and remote signing on a per-warehouse basis using storage profile settings. For S3 warehouses, set `remote-signing-enabled` to `false` to disable remote signing and `sts-enabled` to `false` to disable STS vended credentials. For Azure ADLS warehouses, set `sas-enabled` to `false` to disable SAS token generation. For GCS warehouses, set `sts-enabled` to `false` to disable STS token generation. When these options are disabled at the storage profile level, clients will not receive the corresponding credentials or signing information for that warehouse, regardless of the request headers. Lakekeeper downscopes vended credentials for all supported storages to the location of the table being accessed and ensures that there are no overlapping table locations within a warehouse.

Clients can also control credential delegation per request using the `X-Iceberg-Access-Delegation` header. Lakekeeper supports the standard Iceberg REST spec values (`vended-credentials` and `remote-signing`), plus a special `client-managed` value. When set to `client-managed`, no credentials or signing information are returned, regardless of storage profile configuration. This allows clients to use their own credentials for direct storage access.

## Allowing Alternative Protocols (s3a, s3n, wasbs)

For S3 / AWS and Azure / ADLS Warehouses, Lakekeeper optionally supports additional protocols. To enable these, activate the "Allow Alternative Protocols" flag in the storage profile of the Warehouse. When enabled, the following additional protocols are accepted for table creation or registration:

- **S3 / AWS Warehouses**: Supports `s3a://` and `s3n://` in addition to `s3://`
- **Azure Warehouses**: Supports `wasbs://` in addition to `abfss://`

## Storage Layout

The storage layout controls how namespace and tabular directories are structured under the warehouse base location. It is configured via the `storage-layout` field inside the `storage-profile` when creating or updating a warehouse. The layout applies to all new tabulars created in the warehouse; existing tabular locations are not changed.

### Layout Types

| Type                       | JSON `"type"` value            | Description    |
|----------------------------|--------------------------------|----------------|
| Default                    | `"default"`                    | Flat: no namespace directories; all tabulars are placed directly under the base location with a `{uuid}` segment. Used when `storage-layout` is omitted. **Changed in 0.13** — see [Default](#default). |
| Full hierarchy             | `"full-hierarchy"`             | One directory per namespace level in the full ancestry, one for the tabular. |
| Tabular-only (flat)          | `"tabular-only"`                 | No namespace directories; all tabulars are placed directly under the base location. |

!!! note "OneLake supports only the default layout"
    The [OneLake](#onelake-microsoft-fabric) storage profile currently rejects `tabular-only` and `full-hierarchy` at warehouse-creation time because OneLake silently percent-decodes `%XX` in blob paths, which would alias `{name}` segments that differ only by URL-encoding. See the [OneLake storage-layout note](#onelake-microsoft-fabric) for details.

### Default

The default layout is **flat**: tabulars are placed directly under the warehouse base location with no namespace directories, using a `{uuid}` segment.

For a tabular `orders` in any namespace the path is:

```text
<base>/<orders-tabular>
```

With the default `{uuid}` template this looks like:

```text
s3://my-bucket/warehouse/<uuid-of-tabular>/
```

To use the default layout explicitly:

```json
{
  "storage-profile": {
    "type": "s3",
    "storage-layout": {
      "type": "default"
    }
  }
}
```

!!! warning "The default layout changed in 0.13"
    Before 0.13, the default layout emitted a directory for the **direct parent namespace** (`<base>/<parent-namespace-uuid>/<tabular-uuid>`). As of 0.13 the default is flat (`<base>/<tabular-uuid>`).

    The change is **not retroactive** — storage paths are assigned once, at creation time, and are never recomputed:

    - **Existing tabulars** keep their current locations.
    - **Existing namespaces** keep their persisted `location` property, so new tabulars created in them remain nested under the old `<parent-namespace-uuid>/` directory (see [Namespace Location Property](#namespace-location-property)).
    - Only **namespaces created on or after 0.13** use the flat default.

    A warehouse spanning the upgrade can therefore hold a mix of nested (pre-0.13 namespaces) and flat (new namespaces) tabular paths. If you want namespace directories in new tabular paths, set `storage-layout` to [`full-hierarchy`](#full-hierarchy) — note that this nests *every* ancestor level, whereas the pre-0.13 default nested only the direct parent.

### Full Hierarchy

The full hierarchy layout creates one path segment for **every namespace level** in the ancestry, followed by the tabular segment.

For a tabular `orders` in namespace `europe` / `production` the path is:

```text
<base>/<europe-namespace>/<production-namespace>/<orders-tabular>
```

With `{name}-{uuid}` templates:

```text
s3://my-bucket/warehouse/europe-<namespace-uuid>/production-<namespace-uuid>/orders-<tabular-uuid>/
```

Configuration:

```json
{
  "storage-profile": {
    "type": "s3",
    "storage-layout": {
      "type": "full-hierarchy",
      "namespace": "{name}-{uuid}",
      "tabular": "{name}-{uuid}"
    }
  }
}
```

### Tabular-Only (Flat)

The flat layout places all tabulars directly under the warehouse base location with no namespace directories.

For a tabular `orders` in any namespace the path is:

```text
<base>/<orders-tabular>
```

!!! note
    The `tabular` template **must** contain `{uuid}` in the flat layout. Without it, tabulars with the same name in different namespaces would map to the same storage path, causing data corruption.

Configuration:

```json
{
  "storage-profile": {
    "type": "s3",
    "storage-layout": {
      "type": "tabular-only",
      "tabular": "{name}-{uuid}"
    }
  }
}
```

### Template Placeholders

Namespace and tabular templates support two placeholders:

| Placeholder | Description                                                    |
|-------------|----------------------------------------------------------------|
| `{uuid}`    | UUID of the namespace or tabular, inserted without any encoding. |
| `{name}`    | Name of the namespace or tabular, URL percent-encoded. For example, `my tabular` becomes `my%20tabular` and `中文` becomes `%E4%B8%AD%E6%96%87`. |

Both placeholders can be combined with each other and with literal text, e.g. `{name}-{uuid}`. When `storage-layout` is omitted from the storage profile, the `default` layout is used.

!!! warning "Always include `{uuid}` in templates"
    **We strongly recommend including `{uuid}` in every namespace and tabular template.** Storage paths are assigned once at creation time and are never updated when a tabular or namespace is renamed. If a template relies solely on `{name}` and a tabular or namespace is later renamed and re-created with the same name, Lakekeeper will reject the creation because the path is already in use by the old (now-renamed) object. Because UUIDs are unique and stable for the lifetime of an object, using `{uuid}` (alone or combined with `{name}`) guarantees that each object always has a distinct, collision-free storage path. This is the reason Lakekeeper defaults to pure `{uuid}` templates.

### Namespace Location Property

Namespaces have a `location` property that determines where their tabulars are stored:

- **With location property**: New tabulars always use the namespace's persisted location, regardless of storage layout changes.
- **Without location property**: These namespaces compute locations from the current storage layout. Layout changes affect new tabular placement.

## S3

We support remote signing and vended-credentials with S3-compatible storages & AWS. Both provide a secure way to access data on S3:

- **Remote Signing**: The client prepares an S3 request and sends its headers to the sign endpoint of Lakekeeper. Lakekeeper checks if the request is allowed, if so, it signs the request with its own credentials, creating additional headers during the process. These additional signing headers are returned to the client, which then contacts S3 directly to perform the operation on files.
- **Vended Credentials**: Lakekeeper uses the "STS" Endpoint of S3 to generate temporary credentials which are then returned to clients.

Remote signing works natively with all S3 storages that support the default `AWS Signature Version 4`. This includes almost all S3 solutions on the market today, including Rook Ceph Rados, NetApp StorageGRID 12.0 or newer, Minio and others. Vended credentials in turn depend on an additional "STS" Endpoint, that is not supported by all S3 implementations. We run our integration tests for vended credentials against Minio and AWS. We recommend to setup vended credentials for all supported stores, remote signing is not supported by all clients.

When a client requests table configuration, Lakekeeper selects between remote signing and vended credentials based on the `X-Iceberg-Access-Delegation` header and storage profile settings:

- If the header is set to `client-managed`, neither credentials nor signing information are returned
- If the header specifies `vended-credentials` or `remote-signing`, that method is used if enabled in the storage profile
- If both methods are requested or neither is specified, Lakekeeper attempts to provide vended credentials first (if STS is enabled), then falls back to remote signing (if enabled)
- If both methods are disabled at the storage profile level, no credentials are returned regardless of the header value

For maximum client compatibility, we recommend enabling both STS and remote signing when your S3 storage supports it.

For some older remote signing clients that cannot handle table-specific remote signing endpoint locations, Lakekeeper needs to identifying a table by its location in the storage. Since there are multiple canonical ways to specify S3 resources (virtual-host & path), Lakekeeper warehouses by default use a heuristic to determine which style is used. For some setups these heuristics may not work, or you may want to enforce a specific style. In this case, you can set the `remote-signing-url-style` field to either `path` or `virtual-host` in your storage profile. `path` will always use the first path segment as the bucket name. `virtual-host` will use the first subdomain if it is followed by `.s3` or `.s3-`. The default mode is `auto` which first tries `virtual-host` and falls back to `path` if it fails.

### Configuration Parameters

The following table describes all configuration parameters for an S3 storage profile:

| Parameter                     | Type    | Required | Default                    | Description |
|-------------------------------|---------|----------|----------------------------|-----|
| `bucket`                      | String  | Yes      | -                          | Name of the S3 bucket. Must be between 3-63 characters, containing only lowercase letters, numbers, dots, and hyphens. Must begin and end with a letter or number. |
| `region`                      | String  | Yes      | -                          | AWS region where the bucket is located. For S3-compatible storage, any string can be used (e.g., "local-01"). |
| `sts-enabled`                 | Boolean | Yes      | -                          | Whether to enable STS for vended credentials. Not all S3 compatible object stores support "AssumeRole" via STS. We strongly recommend to enable sts if the storage system supports it. |
| `remote-signing-enabled`      | Boolean | No       | `true`                     | Whether to enable remote signing for S3 requests. When disabled, clients cannot use remote signing for this storage profile even if STS is disabled. Defaults to `true`. |
| `key-prefix`                  | String  | No       | None                       | Subpath in the bucket to use for this warehouse. |
| `endpoint`                    | URL     | No       | None                       | Optional endpoint URL for S3 requests. If not provided, the region will be used to determine the endpoint. If both are provided, the endpoint takes precedence. Example: `http://s3-de.my-domain.com:9000` |
| `sts-endpoint`                | URL     | No       | Value of `endpoint`        | Optional separate endpoint URL for STS requests. Use this when your S3-compatible storage exposes STS on a different endpoint than S3. If not provided, the S3 `endpoint` is used for STS requests as well. |
| `flavor`                      | String  | No       | `aws`                      | S3 flavor to use. Options: `aws` (Amazon S3) or `s3-compat` (for S3-compatible solutions like MinIO). |
| `path-style-access`           | Boolean | No       | `false`                    | Whether to use path style access for S3 requests. If the underlying S3 supports both virtual host and path styles, we recommend not setting this option. |
| `assume-role-arn`             | String  | No       | None                       | Optional ARN to assume when accessing the bucket from Lakekeeper. This is also used as the default for `sts-role-arn` if that is not specified. |
| `sts-role-arn`                | String  | No       | Value of `assume-role-arn` | Optional role ARN to assume for STS vended-credentials. Either `assume-role-arn` or `sts-role-arn` must be provided if `sts-enabled` is true and `flavor` is `aws`. |
| `sts-token-validity-seconds`  | Integer | No       | `3600`                     | The validity period of STS tokens in seconds. Controls how long the vended credentials remain valid before they need to be refreshed. |
| `sts-session-tags`            | Object  | No       | `{}`                       | An optional JSON object containing key-value pairs of session tags to apply when assuming roles via STS. These tags are attached to the temporary credentials and can be used for access control, auditing, or cost allocation. Each key and value must be a string. Example: `{"Environment": "production", "Team": "data-engineering"}` |
| `allow-alternative-protocols` | Boolean | No       | `false`                    | Whether to allow `s3a://` and `s3n://` in locations. This is disabled by default and should only be enabled for migrating legacy Hadoop-based tables via the register endpoint. Tables with `s3a` paths are not accessible outside the Java ecosystem. |
| `remote-signing-url-style`    | String  | No       | `auto`                     | S3 URL style detection mode for remote signing. Options: `auto`, `path-style`, or `virtual-host`. When set to `auto`, Lakekeeper tries virtual-host style first, then path style. |
| `push-s3-delete-disabled`     | Boolean | No       | `true`                     | Controls whether the `s3.delete-enabled=false` flag is sent to clients. Only has an effect if "soft-deletion" is enabled for this Warehouse. This prevents clients like Spark from directly deleting files during operations like `DROP TABLE xxx PURGE`, ensuring soft-deletion works properly. However, it also affects operations like `expire_snapshots` that require file deletion. For more information, please check the [Soft Deletion Documentation](./concepts.md#soft-deletion). |
| `aws-kms-key-arn`             | String  | No       | None                       | ARN of the AWS KMS Key that is used to encrypt the bucket. Vended Credentials is granted `kms:Decrypt` and `kms:GenerateDataKey` on the key. |
| `legacy-md5-behavior`         | Boolean | No       | `false`                    | A flag to enable the legacy behavior of using MD5 checksums for operations that require checksums. |
| `storage-layout`              | Object  | No       | `{"type": "default"}`      | Controls how namespace and tabular directories are structured under the warehouse base location. See [Storage Layout](#storage-layout) for details. |

### AWS

###### Direct File-Access with Access Key

First create a new S3 bucket for the warehouse. Buckets can be re-used for multiple Warehouses as long as the `key-prefix` is different. We recommend to block all public access.

Secondly we need to create an AWS role that can access and delegate access to the bucket. We start by creating a new Policy that allows access to data in the bucket. We call this policy `LakekeeperWarehouseDev`:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "ListBuckets",
            "Action": [
                "s3:ListAllMyBuckets",
                "s3:GetBucketLocation"
            ],
            "Effect": "Allow",
            "Resource": [
                "arn:aws:s3:::*"
            ]
        },
        {
            "Sid": "ListBucketContent",
            "Action": [
                "s3:ListBucket"
            ],
            "Effect": "Allow",
            "Resource": "arn:aws:s3:::lakekeeper-aws-demo"
        },
        {
            "Sid": "DataAccess",
            "Effect": "Allow",
            "Action": [
                "s3:*"
            ],
            "Resource": [
                "arn:aws:s3:::lakekeeper-aws-demo/*"
            ]
        }
    ]
}
```

Now create a new user, we call the user `LakekeeperWarehouseDev`, and attach the previously created policy. When the user is created, click on "Security credentials" and "Create access key". Note down the access key and secret key for later use.

We are done if we only rely on remote signing. For vended credentials, we need to perform one more step. Create a new role that we call `LakekeeperWarehouseDevRole`. This role needs to be trusted by the user, which is achieved via with the following trust policy:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "TrustLakekeeperWarehouseDev",
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::<aws-account-id>:user/LakekeeperWarehouseDev"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}
```

Also attach the `LakekeeperWarehouseDev` policy created earlier.

We are now ready to create the Warehouse via the UI or REST-API using the following values (make sure to replace everything in `<>`):

```json
{
    "warehouse-name": "aws_docs",
    "storage-credential": {
        "type": "s3",
        "aws-access-key-id": "<Access Key of the created user>",
        "aws-secret-access-key": "<Secret Key of the created user>",
        "credential-type": "access-key"
    },
    "storage-profile": {
        "type": "s3",
        "bucket": "<name of the bucket>",
        "region": "<region of the bucket>",
        "sts-enabled": true,
        "flavor": "aws",
        "key-prefix": "lakekeeper-dev-warehouse",
        "sts-role-arn": "arn:aws:iam::<aws account id>:role/LakekeeperWarehouseDevRole"
    },
    "delete-profile": {
        "type": "hard"
    }
}
```

As part of the `storage-profile`, the field `assume-role-arn` can optionally be specified. If it is specified, this role is assumed for every IO Operation of Lakekeeper. It is also used as `sts-role-arn`, unless `sts-role-arn` is specified explicitly. If no `assume-role-arn` is specified, whatever authentication method / user os configured via the `storage-credential` is used directly for IO Operations, so needs to have S3 access policies attached directly (as shown in the example above).

##### System Identities / Managed Identities

Since Lakekeeper version 0.8, credentials for S3 access can also be loaded directly from the environment. Lakekeeper integrates with the AWS SDK to support standard environment-based authentication, including all common configuration options through AWS_* environment variables.

!!! note

    When using system identities, we **strongly recommend** configuring external-id values. This prevents unauthorized cross-account role access and ensures roles can only be assumed by authorized Lakekeeper warehouses.

Without external IDs, any user with warehouse creation permissions in Lakekeeper could potentially access any role the system identity is allowed to assume. For more information, see [AWS's documentation on external IDs](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_common-scenarios_third-party.html).

Below is a step-by-step guide for setting up a secure system identity configuration:

Firstly, create a dedicated AWS user to serve as your system identity. Do not attach any direct permissions or trust policies to this user. This user will only have the ability to assume specific roles with the proper external ID

Secondly, configure Lakekeeper with this identity by setting the following environment variables.

```bash
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_DEFAULT_REGION=...
# Required for System Credentials to work:
LAKEKEEPER__S3_REQUIRE_EXTERNAL_ID_FOR_SYSTEM_CREDENTIALS=true
```

In addition to the standard `AWS_*` environment variables, Lakekeeper supports all authentication methods available in the AWS SDK, including instance profiles, container credentials, and SSO configurations.

For enhanced security, Lakekeeper enforces that warehouses using system identities must specify both an `external-id` and an `assume-role-arn` when configured. This implementation follows AWS security best practices by preventing unauthorized role assumption. These default requirements can be adjusted through settings described in the [Configuration Guide](./configuration.md#storage).

For this example, assume the system identity has the ARN `arn:aws:iam::123:user/lakekeeper-system-identity`.

When creating a warehouse, users must configure an IAM role with an appropriate trust policy. The following trust policy template enables the Lakekeeper system identity to assume the role, while enforcing external ID validation:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::123:user/lakekeeper-system-identity"
            },
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringEquals": {
                    "sts:ExternalId": "<Use a secure random string that cannot be guessed. Treat it like a password.>"
                }
            }
        }
    ]
}
```

The role also needs S3 access, so attach a policy like this:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AllowAllAccessInWarehouseFolder",
            "Action": [
                "s3:*"
            ],
            "Resource": [
                "arn:aws:s3:::<bucket-name>/<key-prefix if used>/*"
            ],
            "Effect": "Allow"
        },
        {
            "Sid": "AllowRootAndHomeListing",
            "Action": [
                "s3:ListBucket"
            ],
            "Effect": "Allow",
            "Resource": [
                "arn:aws:s3:::<bucket-name>",
                "arn:aws:s3:::<bucket-name>/*"
            ]
        }
    ]
}
```

We are now ready to create the Warehouse using the system identity:

```json
{
    "warehouse-name": "aws_docs_managed_identity",
    "storage-credential": {
        "type": "s3",
        "credential-type": "aws-system-identity",
        "external-id": "<external id configured in the trust policy of the role>"
    },
    "storage-profile": {
        "type": "s3",
        "assume-role-arn": "<arn of the role that was created>",
        "bucket": "<name of the bucket>",
        "region": "<region of the bucket>",
        "sts-enabled": true,
        "flavor": "aws",
        "key-prefix": "<path to warehouse in bucket>"
    },
    "delete-profile": {
        "type": "hard"
    }
}
```

The specified `assume-role-arn` is used for Lakekeeper's reads and writes of the object store. It is also used as a default for `sts-role-arn`, which is the role that is assumed when generating vended credentials for clients (with an attached policy for the accessed table).

##### CORS Configuration

For browser-based access to S3 buckets (required for [DuckDB WASM](engines.md#duckdb-wasm)), you need to configure CORS (Cross-Origin Resource Sharing) on your S3 bucket.

To configure CORS for your S3 bucket:

1. In the AWS S3 Configuration Menu, click on the name of your bucket
2. Choose **Permissions** Tab
3. In the **Cross-origin resource sharing (CORS)** section, choose **Edit**
4. In the CORS configuration editor text box, type or copy and paste a new CORS configuration, or edit an existing configuration. The CORS configuration is a JSON file. The text that you type in the editor must be valid JSON. See below for an example.
5. Choose **Save changes**

Example CORS policy:

```json
[
    {
        "AllowedHeaders": [
            "*"
        ],
        "AllowedMethods": [
            "GET",
            "POST",
            "PUT",
            "DELETE",
            "HEAD"
        ],
        "AllowedOrigins": [
            "https://lakekeeper.example.com"
        ],
        "ExposeHeaders": [
            "ETag",
            "x-amz-version-id"
        ]
    }
]
```

Replace `https://lakekeeper.example.com` with the origin where your Lakekeeper instance is hosted.

##### STS Session Tags

The optional `sts-session-tags` setting can be used to provide Session Tags when assuming roles via STS. Doing so requires that the IAM Role's Trust Relationship also allow `sts:TagSession`. Here's the above example with this addition:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AllowAssumeRole",
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::123:user/lakekeeper-system-identity"
            },
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringEquals": {
                    "sts:ExternalId": "<Use a secure random string that cannot be guessed. Treat it like a password.>"
                }
            }
        },
        {
            "Sid": "AllowSessionTagging",
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::123:user/lakekeeper-system-identity"
            },
            "Action": "sts:TagSession"
        }
    ]
}
```

If wanting to use a session tag in an ABAC policy, one can reference that tag via `${aws:PrincipalTag/<tag name>}`. For example, here's a policy that dynamically sets the S3 path based on a `tenant` tag:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AllowAllAccessInTenantWarehouse",
            "Action": [
                "s3:*"
            ],
            "Resource": [
                "arn:aws:s3:::<bucket-name>/${aws:PrincipalTag/tenant}/*"
            ],
            "Effect": "Allow"
        },
        {
            "Sid": "AllowListingInTenantWarehouse",
            "Action": [
                "s3:ListBucket"
            ],
            "Effect": "Allow",
            "Resource": "arn:aws:s3:::<bucket-name>",
            "Condition": {
                "StringLike": {
                    "s3:prefix": [
                        "${aws:PrincipalTag/tenant}/*"
                    ]
                }
            }
        }
    ]
}
```

### S3 Compatible

Unlike for AWS, we do not need any special trust-setup for vended credentials / STS with most S3 compatible solutions like Minio. Instead, we just need a bucket and an access key / secret key combination that is able to read and write from it. If `sts-role-arn` is provided, it will be sent as part of the request to the STS service. Keep in mind that the specific S3 compatible solution may ignore the parameter. Conversely, if `sts-role-arn` is not specified, the request to the STS service will not contain it. Make sure to select `flavor` to have the value `s3-compat`! This setting should work for most self-hosted S3 solutions.

An warehouse create call could look like this:

```json

{
    "warehouse-name": "minio_dev",
    "storage-credential": {
        "type": "s3",
        "aws-access-key-id": "<Access Key of the created user>",
        "aws-secret-access-key": "<Secret Key of the created user>",
        "credential-type": "access-key"
    },
    "storage-profile": {
        "type": "s3",
        "bucket": "<name of the bucket>",
        "region": "local-01",
        "sts-enabled": true,
        "flavor": "s3-compat",
        "key-prefix": "lakekeeper-dev-warehouse",
    },
    "delete-profile": {
        "type": "hard"
    }
}
```

### Cloudflare R2

Lakekeeper supports Cloudflare R2 storage with all S3 compatible clients, including vended credentials via the `/accounts/{account_id}/r2/temp-access-credentials` Endpoint.

First we create a new Bucket. In the cloudflare UI, Select "R2 Object Storage" -> "Overview" and select "+ Create Bucket". We call our bucket `lakekeeper-dev`. Click on the bucket, select the "Settings" tab, and note down the "S3 API" displayed.

Secondly, we create an API Token for Lakekeeper as follows:

1. Go back to the Overview Page ("R2 Object Storage" -> "Overview") and select "Manage API tokens" in the "{} API" dropdown.
1. In the R2 token page select "Create Account API token". Give the token any name. Select the "Admin Read & Write" permission, this is unfortunately required at the time of writing, as the `/accounts/{account_id}/r2/temp-access-credentials` does not accept other tokens. Click "Create Account API Token".
1. Note down the "Token value", "Access Key ID" and "Secret Access Key"

Finally, we can create the Warehouse in Lakekeeper via the UI or API. A POST request to `/management/v1/warehouse` expects the following body:

```json
{
  "warehouse-name": "r2_dev",
  "delete-profile": { "type": "hard" },
  "storage-credential":
    {
        "credential-type": "cloudflare-r2",
        "account-id": "<Cloudflare Account ID, typically the long alphanumeric string before the first dot in the S3 API URL> ",
        "access-key-id": "access-key-id-from-above",
        "secret-access-key": "secret-access-key-from-above",
        "token": "token-from-above",
    },
  "storage-profile":
    {
        "type": "s3",
        "bucket": "<name of your cloudflare r2 bucket, lakekeeper-dev in our example>",
        "region": "<your cloudflare region, i.e. weur>",
        "key-prefix": "path/to/my/warehouse",
        "endpoint": "<S3 API Endpoint, i.e. https://<account-id>.eu.r2.cloudflarestorage.com>"
    },
}
```

For cloudflare R2 credentials, the following parameters are automatically set:

- `assume-role-arn` is set to None, as this is not supported
- `sts-enabled` is set to `true`
- `flavor` is set to `s3-compat`

It is required to specify the `endpoint`. Use a [Data Location Hint](https://developers.cloudflare.com/r2/reference/data-location/#available-hints) as region.

## Azure Data Lake Storage Gen 2

To add a Warehouse backed by ADLS, we need two Azure objects: The Storage Account itself and an App Registration which Lakekeeper can use to access it and delegate access to compute engines.

### Configuration Parameters

The following table describes all configuration parameters for an ADLS storage profile:

| Parameter                     | Type    | Required | Default                             | Description |
|-------------------------------|---------|----------|-------------------------------------|-----|
| `account-name`                | String  | Yes      | -                                   | Name of the Azure storage account. |
| `filesystem`                  | String  | Yes      | -                                   | Name of the ADLS filesystem, in blob storage also known as container. |
| `sas-enabled`                 | Boolean | No       | `true`                              | Whether to enable SAS (Shared Access Signature) token generation for Azure Data Lake Storage. When disabled, clients cannot use vended credentials for this storage profile. Defaults to `true`. |
| `key-prefix`                  | String  | No       | None                                | Subpath in the filesystem to use. |
| `allow-alternative-protocols` | Boolean | No       | `false`                             | Whether to allow `wasbs://` in locations in addition to `abfss://`. This is disabled by default and should only be enabled for migrating legacy Hadoop-based tables via the register endpoint. |
| `host`                        | String  | No       | `dfs.core.windows.net`              | The host to use for the storage account. |
| `authority-host`              | URL     | No       | `https://login.microsoftonline.com` | The authority host to use for authentication. |
| `sas-token-validity-seconds`  | Integer | No       | `3600`                              | The validity period of the SAS token in seconds. |
| `storage-layout`              | Object  | No       | `{"type": "default"}`               | Controls how namespace and tabular directories are structured under the warehouse base location. See [Storage Layout](#storage-layout) for details. |

Lets start by creating a new "App Registration":

1. Create a new "App Registration"
    - **Name**: choose any, for this example we choose `Lakekeeper Warehouse (Development)`
    - **Redirect URI**: Leave empty
2. When the App Registration is created, select "Manage" -> "Certificates & secrets" and create a "New client secret". Note down the secrets "Value".
3. In the "Overview" page of the "App Registration" note down the `Application (client) ID` and the `Directory (tenant) ID`.

Next, we create a new Storage Account. Make sure to select "Enable hierarchical namespace" in the "Advanced" section. For existing Storage Accounts make sure "Hierarchical namespace: Enabled" is shown in the "Overview" page. There are no specific requirements otherwise. Note down the name of the storage account. When the storage account is created, we need to grant the correct permissions to the "App Registration" and create the filesystem / container where the data is stored:

1. Open the Storage Account and select "Data storage" -> Containers. Add a new Container, we call it `warehouse-dev`.
2. Next, select "Access Control (IAM)" in the left menu and "Add role assignment". Grant the `Storage Blob Data Contributor` and `Storage Blob Delegator` roles to the `Lakekeeper Warehouse (Development)` App Registration that we previously created.

We are now ready to create the Warehouse via the UI or the REST API. Use the following information:

- **client-id**: The `Application (client) ID` of the `Lakekeeper Warehouse (Development)` App Registration.
- **client-secret**: The "Value" of the client secret that we noted down previously.
- **tenant-id**: The `Directory (tenant) ID` from the Applications Overview page.
- **account-name**: Name of the Storage Account
- **filesystem**: Name of the container (that Azure also calls filesystem) previously created. In our example its `warehouse-dev`.

A POST request to `/management/v1/warehouse` would expects the following body:

```json
{
  "warehouse-name": "azure_dev",
  "delete-profile": { "type": "hard" },
  "storage-credential":
    {
      "client-id": "...",
      "client-secret": "...",
      "credential-type": "client-credentials",
      "tenant-id": "...",
      "type": "az",
    },
  "storage-profile":
    {
      "account-name": "...",
      "filesystem": "warehouse-dev",
      "type": "adls",
    },
}
```

##### Azure System Identity

!!! warning
    Enabling Azure system identities allows Lakekeeper to access any storage location that the managed identity has permissions for. To minimize security risks, ensure the managed identity is restricted to only the necessary resources. Additionally, limit Warehouse creation permission in Lakekeeper to users who are authorized to access all locations that the system identity can access.

Azure system identities can be used to authenticate Lakekeeper to ADLS Gen 2, without specifying credentials explicitly on Warehouse creation. This feature is disabled by default and must be explicitly enabled system-wide by setting the following environment variable:

```bash
LAKEKEEPER__ENABLE_AZURE_SYSTEM_CREDENTIALS=true
```

When enabled, Lakekeeper will use the managed identity of the virtual machine or application it is running on to access ADLS. Ensure that the managed identity has the necessary permissions to access the storage account and container. For example, assign the `Storage Blob Data Contributor` and `Storage Blob Delegator` roles to the managed identity for the relevant storage account as described above.

## OneLake (Microsoft Fabric)

*Available since Lakekeeper 0.12.4.*

Microsoft Fabric exposes its OneLake data lake through ADLS Gen2-compatible APIs, so Lakekeeper can back warehouses directly with a Fabric lakehouse using a dedicated `onelake` storage profile. The OneLake profile is a convenience layer over the generic ADLS profile: instead of asking you to encode the OneLake URL conventions yourself (`account_name = "onelake"`, container = workspace ID, key prefix = `<lakehouse>/Files/<dir>`), it derives those from the workspace and lakehouse UUIDs you provide. It also knows how to compute the workspace-scoped private-link endpoint host.

!!! note
    The generic `adls` profile also works against OneLake if you set the fields manually (`account-name: "onelake"`, `host: "dfs.fabric.microsoft.com"`, `filesystem: <workspace-id>`, `key-prefix: <lakehouse-id>/Files/<dir>`). The `onelake` profile is the recommended path because it validates the OneLake-specific constraints (SAS lifetime cap, supported credentials, endpoint shapes) for you and computes private-link FQDNs automatically.

### Client compatibility

OneLake's blob surface is API-compatible with regular ADLS Gen2 for the operations Lakekeeper's vended-credentials path uses, but client libraries need to be OneLake-aware (specifically: they have to honour `adls.account-host` so they target `*.fabric.microsoft.com` instead of defaulting to `<account>.blob.core.windows.net`). Lakekeeper emits the right property in the catalog response; the engine still has to be on a version that consumes it.

| Client | Minimum version | Notes |
|---|---|---|
| **PyIceberg** | `0.10.0` | First release that ships both [`adls.account-host`](https://github.com/apache/iceberg-python/pull/2016) and [`adls.credential`](https://github.com/apache/iceberg-python/pull/2299). Earlier versions don't construct OneLake URLs correctly even when Lakekeeper hands them the right properties. Transitively requires `adlfs >= 2024.7.0`. |
| **Spark + Iceberg (Java)** | `iceberg-spark-runtime` ≥ `1.5` on Spark 3.5 *or* ≥ `1.10` on Spark 4 | The Java Iceberg ADLS file IO parses the host from `abfss://<fs>@<host>/...` directly, so it's transparently OneLake-compatible for any version that supports vended ADLS credentials. |

Lakekeeper's own OneLake integration tests are run against:

- **Spark** `4.0.2` (`apache/spark:4.0.2-scala2.13-java21-python3-ubuntu`) with `iceberg-spark-runtime` `1.10.1`
- **PyIceberg** `0.10.0` (with the `adlfs` extra)

Older Spark 3 / Iceberg < 1.10 combinations are exercised by other suites in the same harness (`apache/spark:3.5.6-java17-python3`); the OneLake-specific paths in vended credentials don't depend on the Spark major version.

### Configuration Parameters

| Parameter                      | Type    | Required | Default                             | Description |
|--------------------------------|---------|----------|-------------------------------------|-------------|
| `workspace-id`                 | UUID    | Yes      | -                                   | UUID of the Fabric workspace this warehouse lives in. |
| `lakehouse-id`                 | UUID    | Yes      | -                                   | UUID of the lakehouse within the workspace. |
| `directory-rel-path`           | String  | Yes      | -                                   | Subpath beneath `<top-level-folder>/` inside the lakehouse — the root directory under which Lakekeeper writes all warehouse data. |
| `top-level-folder`             | String  | No       | `"Files"`                           | Top-level managed folder. Either `"Files"` (recommended, Iceberg-managed data area) or `"Tables"`. Writing Iceberg metadata under `Tables/` conflicts with Fabric's automatic Delta/Iceberg virtualization — choose only with care. |
| `endpoint-mode`                | Object  | No       | `{"type": "default"}`               | OneLake endpoint selection. See [Endpoint modes](#endpoint-modes) below. |
| `sas-enabled`                  | Boolean | No       | `true`                              | Enable SAS-token vending. Disable to force clients to use their own credentials. |
| `sas-token-validity-seconds`   | Integer | No       | `3600`                              | SAS-token validity in seconds. **Max: 3600 (OneLake hard cap).** Lakekeeper rejects values above 3600 and below 1; values below 60 are accepted with a warning and floored at mint time. |
| `authority-host`               | URL     | No       | `https://login.microsoftonline.com` | Microsoft Entra authority host. |
| `storage-layout`               | Object  | No       | `{"type": "default"}`               | **Must be `{"type": "default"}` or omitted.** OneLake silently percent-decodes `%XX` sequences in blob paths, so layouts that embed `{name}` segments (`tabular-only`, `full-hierarchy`) would alias to the same blob after server-side decoding. Only the default `{uuid}`-only layout is currently supported. See [Storage Layout](#storage-layout). |

### Endpoint modes

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

!!! warning "OneLake path names cannot contain `%`"
    OneLake's request pipeline silently collapses any `%XX` percent-escape in a blob path to its decoded character before SAS validation, so a path that stores the *literal* three-character sequence `%3F` is indistinguishable from one that stores the single character `?`. Lakekeeper otherwise treats every byte in a path literally (`%41bc` is a different blob from `Abc`); on OneLake that guarantee can't hold, so the OneLake storage profile **rejects any table location whose path segments contain a literal `%`** at create time. Use a different character or strip the `%` before submitting the location.

!!! warning "Only the `default` storage layout is supported"
    The OneLake profile currently rejects `tabular-only` and `full-hierarchy` storage layouts at warehouse-creation time. These layouts can embed namespace and table **names** into the storage path via `{name}` templates, and Lakekeeper URL-percent-encodes those segments — but OneLake silently decodes `%XX` sequences server-side (see the percent-encoding warning above), so two distinct names whose encoded form differs only by a `%XX` would land at the same blob and overwrite each other. Until we land a layout that side-steps this encoding mismatch, only the default `{uuid}`-only layout is supported. Set `storage-layout` to `{"type": "default"}` or omit it.

    Since 0.13 the default layout is **flat** — it has no `{name}` segments to percent-decode, so it is OneLake-safe. OneLake warehouses therefore place new tabulars directly under the base location (`<base>/<tabular-uuid>`); see [Default](#default), including the note on how this affects namespaces created before 0.13.

### Credentials

OneLake does not have a storage-account key. Only Microsoft Entra credentials are accepted:

- `client-credentials` (service principal): the standard option.
- `azure-system-identity` (managed identity): if `LAKEKEEPER__ENABLE_AZURE_SYSTEM_CREDENTIALS=true` is set server-wide.

Supplying `shared-access-key` to a OneLake warehouse is rejected at validation time.

The OneLake tenant setting **"Authenticate with OneLake user-delegated SAS tokens"** must be enabled for the workspace before vended credentials work. This is a Fabric-side setting and cannot be configured from Lakekeeper.

### Example

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

### Immutability

Most fields are immutable on `update-storage-profile` because changing them would orphan every table previously written to the warehouse: `workspace-id`, `lakehouse-id`, `top-level-folder`, `directory-rel-path`, and `endpoint-mode`. The following fields can be updated: `sas-token-validity-seconds`, `sas-enabled`, `authority-host`, `storage-layout`.

## Google Cloud Storage

Google Cloud Storage can be used to store Iceberg tables through the `gs://` protocol.

### Configuration Parameters

The following table describes all configuration parameters for a GCS storage profile:

| Parameter        | Type    | Required | Default               | Description  |
|------------------|---------|----------|-----------------------|--------------|
| `bucket`         | String  | Yes      | -                     | Name of the GCS bucket. |
| `key-prefix`     | String  | No       | None                  | Subpath in the bucket to use for this warehouse. |
| `sts-enabled`    | Boolean | No       | `true`                | Whether to enable STS (Security Token Service) downscoped token generation for GCS. When disabled, clients cannot use vended credentials for this storage profile. Defaults to `true`. |
| `storage-layout` | Object  | No       | `{"type": "default"}` | Controls how namespace and tabular directories are structured under the warehouse base location. See [Storage Layout](#storage-layout) for details. |

The service account should have appropriate permissions (such as Storage Admin role) on the bucket. Since Lakekeeper Version 0.8.2, hierarchical Namespaces are supported.

### Authentication Options

Lakekeeper supports two primary authentication methods for GCS:

##### Service Account Key

You can provide a service account key directly when creating a warehouse. This is the most straightforward way to give Lakekeeper access to your GCS bucket:

```json
{
  "warehouse-name": "gcs_dev",
  "storage-profile": {
    "type": "gcs",
    "bucket": "...",
    "key-prefix": "..."
  },
  "storage-credential": {
    "type": "gcs",
    "credential-type": "service-account-key",
    "key": {
      "type": "service_account",
      "project_id": "example-project-1234",
      "private_key_id": "....",
      "private_key": "-----BEGIN PRIVATE KEY-----\n.....\n-----END PRIVATE KEY-----\n",
      "client_email": "abc@example-project-1234.iam.gserviceaccount.com",
      "client_id": "123456789012345678901",
      "auth_uri": "https://accounts.google.com/o/oauth2/auth",
      "token_uri": "https://oauth2.googleapis.com/token",
      "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
      "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/abc%example-project-1234.iam.gserviceaccount.com",
      "universe_domain": "googleapis.com"
    }
  }
}
```

The service account key should be created in the Google Cloud Console and should have the necessary permissions to access the bucket (typically Storage Admin role on the bucket).

##### GCP System Identity

!!! warning
    Enabling GCP system identities grants Lakekeeper access to any storage location the service account has permissions for. Carefully review and limit the permissions of the service account to avoid unintended access to sensitive resources. Additionally, limit Warehouse creation permissions in Lakekeeper to users who are authorized to access all locations that the system identity can access.

GCP system identities allow Lakekeeper to authenticate using the service account that the application is running as. This can be either a Compute Engine default service account or a user-assigned service account. To enable this feature system-wide, set the following environment variable:

```bash
LAKEKEEPER__ENABLE_GCP_SYSTEM_CREDENTIALS=true
```

When using system identity, Lakekeeper will use the service account associated with the application or virtual machine to access Google Cloud Storage (GCS). Ensure that the service account has the necessary permissions, such as the Storage Admin role on the target bucket.
