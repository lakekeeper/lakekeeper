---
description: "Configure Lakekeeper Warehouses on AWS S3 and S3-compatible storage such as MinIO, SeaweedFS, Cloudflare R2 and Alibaba Cloud OSS: parameters, credentials, STS vended credentials, remote signing and CORS."
---

# S3

The `s3` storage profile backs Warehouses with AWS S3 or any S3-compatible storage. Table locations use `s3://`.

Clients get access through [vended credentials or remote signing](storage.md#how-clients-access-data):

- **Vended credentials** need an STS endpoint, which not every S3 implementation offers. We test vended credentials against AWS, Silo (a maintained MinIO fork) and SeaweedFS.
- **Remote signing** works with every storage that supports AWS Signature Version 4, which includes almost all S3 implementations, such as Rook Ceph RADOS, NetApp StorageGRID 12.0 or newer and MinIO.

Not every client supports remote signing, so we recommend setting up vended credentials wherever the storage offers STS. For the widest client compatibility, enable both.

## Configuration Parameters

| Parameter                     | Type    | Required | Default                    | Description |
|-------------------------------|---------|----------|----------------------------|-----|
| `bucket`                      | String  | Yes      | -                          | Name of the S3 bucket. Must be between 3-63 characters, containing only lowercase letters, numbers, dots, and hyphens. Must begin and end with a letter or number. |
| `region`                      | String  | Yes      | -                          | AWS region where the bucket is located. For S3-compatible storage, any string can be used (e.g., "local-01"). For `flavor` `aws` without an explicit `endpoint`, a `us-gov-*`, `cn-*`, ISO or `eusc-de-*` region also selects the AWS partition of the vended-credential policy; otherwise the role ARN does. |
| `sts-enabled`                 | Boolean | Yes      | -                          | Whether to enable STS for vended credentials. Not all S3 compatible object stores support "AssumeRole" via STS. We strongly recommend enabling STS if the storage supports it. |
| `remote-signing-enabled`      | Boolean | No       | `true`                     | Whether to enable remote signing for S3 requests. When disabled, clients cannot use remote signing for this storage profile even if STS is disabled. |
| `key-prefix`                  | String  | No       | None                       | Subpath in the bucket to use for this warehouse. |
| `endpoint`                    | URL     | No       | None                       | Optional endpoint URL for S3 requests. If not provided, the region will be used to determine the endpoint. If both are provided, the endpoint takes precedence. Example: `http://s3-de.my-domain.com:9000` |
| `sts-endpoint`                | URL     | No       | Value of `endpoint`        | Optional separate endpoint URL for STS requests. Use this when your S3-compatible storage exposes STS on a different endpoint than S3. If not provided, the S3 `endpoint` is used for STS requests as well. |
| `flavor`                      | String  | No       | `aws`                      | S3 flavor to use. Options: `aws` (Amazon S3) or `s3-compat` (for S3-compatible solutions like MinIO). |
| `path-style-access`           | Boolean | No       | `false`                    | Whether to use path style access for S3 requests. If the underlying S3 supports both virtual host and path styles, we recommend not setting this option. |
| `assume-role-arn`             | String  | No       | None                       | Optional ARN to assume when accessing the bucket from Lakekeeper. This is also used as the default for `sts-role-arn` if that is not specified. |
| `sts-role-arn`                | String  | No       | Value of `assume-role-arn` | Optional role ARN to assume for STS vended-credentials. Either `assume-role-arn` or `sts-role-arn` must be provided if `sts-enabled` is true and `flavor` is `aws`. |
| `sts-token-validity-seconds`  | Integer | No       | `3600`                     | The validity period of STS tokens in seconds. Controls how long the vended credentials remain valid before they need to be refreshed. |
| `sts-session-tags`            | Object  | No       | `{}`                       | An optional JSON object containing key-value pairs of session tags to apply when assuming roles via STS. These tags are attached to the temporary credentials and can be used for access control, auditing, or cost allocation. Each key and value must be a string. Example: `{"Environment": "production", "Team": "data-engineering"}`. See [STS session tags](#sts-session-tags). |
| `allow-alternative-protocols` | Boolean | No       | `false`                    | Whether to allow `s3a://` and `s3n://` in locations. This is disabled by default and should only be enabled for migrating legacy Hadoop-based tables via the register endpoint. Tables with `s3a` paths are not accessible outside the Java ecosystem. |
| `remote-signing-url-style`    | String  | No       | `auto`                     | S3 URL style detection mode for remote signing. Options: `auto`, `path`, or `virtual_host`. See [Remote signing](#remote-signing). |
| `push-s3-delete-disabled`     | Boolean | No       | `true`                     | Controls whether the `s3.delete-enabled=false` flag is sent to clients. Only has an effect if "soft-deletion" is enabled for this Warehouse. This prevents clients like Spark from directly deleting files during operations like `DROP TABLE xxx PURGE`, ensuring soft-deletion works properly. However, it also affects operations like `expire_snapshots` that require file deletion. For more information, please check the [Soft Deletion Documentation](./concepts.md#soft-deletion). |
| `aws-kms-key-arn`             | String  | No       | None                       | ARN of the AWS KMS Key that is used to encrypt the bucket. Vended Credentials is granted `kms:Decrypt` and `kms:GenerateDataKey` on the key. |
| `legacy-md5-behavior`         | Boolean | No       | `false`                    | A flag to enable the legacy behavior of using MD5 checksums for operations that require checksums. |
| `storage-layout`              | Object  | No       | `{"type": "default"}`      | Controls how namespace and tabular directories are structured under the warehouse base location. See [Storage Layout](storage-layout.md). |

## Credentials

The `storage-credential` of an S3 Warehouse has `"type": "s3"` and one of these `credential-type` values:

| `credential-type` | Use for |
|---|---|
| `access-key` | An access key and secret key, on AWS ([example](#aws-with-an-access-key)) or on [S3-compatible storage](#s3-compatible-storage) |
| `aws-system-identity` | The AWS identity of the Lakekeeper process, see [AWS system identity](#aws-system-identity) |
| `cloudflare-r2` | [Cloudflare R2](#cloudflare-r2) |
| `aliyun-oss` | [Alibaba Cloud OSS](#alibaba-cloud-oss) |

## Remote Signing

Remote signing applies to [Generic Tables](./generic-tables.md) as well as Iceberg tables; see [Remote signing for generic tables](./generic-tables.md#remote-signing-s3-without-sts).

Remote signing also covers prefix listings (`ListObjectsV2`), which clients use for maintenance operations such as Spark's `remove_orphan_files` with `prefix_listing => true` (requires `iceberg-spark-runtime` 1.10 or newer). The `prefix` must address a directory inside the table's location, i.e. it has to end with a `/` when listing the table location itself. S3 matches list prefixes as raw strings, so the prefix `warehouse/ns/table` would also return the keys of a sibling `warehouse/ns/table_other`, and is rejected. Iceberg's `FileSystemWalker` appends the `/` before listing; clients that don't are expected to normalize their prefix.

Some older remote signing clients cannot handle table-specific signing endpoints, so Lakekeeper has to identify the table by its location in the storage. S3 resources can be addressed in path style or virtual-host style, and by default Lakekeeper detects the style with a heuristic. If the heuristic does not fit your setup, set `remote-signing-url-style`:

- `path` always uses the first path segment as the bucket name.
- `virtual_host` uses the first subdomain if it is followed by `.s3` or `.s3-`.
- `auto`, the default, tries `virtual_host` first and falls back to `path`.

## AWS

### AWS with an Access Key

First create an S3 bucket for the Warehouse. Several Warehouses can share a bucket as long as their `key-prefix` differs. We recommend blocking all public access.

Next, create a policy that allows access to data in the bucket. We call it `LakekeeperWarehouseDev`:

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

Create a user, also called `LakekeeperWarehouseDev`, and attach the policy. Once the user exists, open "Security credentials", choose "Create access key" and note down the access key and secret key.

This is enough for remote signing. Vended credentials also need a role that the user may assume. Create a role called `LakekeeperWarehouseDevRole` with this trust policy, and attach the `LakekeeperWarehouseDev` policy to it as well:

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

Now create the Warehouse through the UI or with a POST request to `/management/v1/warehouse` (replace everything in `<>`):

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

The `storage-profile` can also set `assume-role-arn`. Lakekeeper then assumes that role for all of its own reads and writes, and uses it as `sts-role-arn` unless `sts-role-arn` is set. Without `assume-role-arn`, Lakekeeper uses the `storage-credential` directly, so that identity needs the S3 access policy attached, as in the example above.

### AWS Partitions (GovCloud, China, ISO)

Buckets in AWS GovCloud (`us-gov-*` regions) and in the China regions (`cn-*`) live in their own AWS partition, so their ARNs are prefixed with `arn:aws-us-gov:` and `arn:aws-cn:` instead of `arn:aws:`. Lakekeeper builds the S3 ARNs of the downscoped policy it sends when vending credentials with the matching prefix, so there is nothing to configure beyond using ARNs of that partition for `sts-role-arn`, `assume-role-arn` and `aws-kms-key-arn`:

```json
{
    "storage-profile": {
        "type": "s3",
        "bucket": "<name of the bucket>",
        "region": "us-gov-west-1",
        "sts-enabled": true,
        "flavor": "aws",
        "sts-role-arn": "arn:aws-us-gov:iam::<aws account id>:role/LakekeeperWarehouseDevRole"
    }
}
```

A `us-gov-*` or `cn-*` region determines the partition, as do the ISO regions (`us-iso-*`, `us-isob-*`, `us-isof-*`, `eu-isoe-*`) and the European Sovereign Cloud (`eusc-de-*`). This applies to profiles that let the AWS SDK resolve the endpoint from the region. For every other profile — an explicit `endpoint`, a commercial region, or a region of a partition newer than your Lakekeeper release — the partition of `sts-role-arn` or `assume-role-arn` is used, and `aws` if neither names an AWS partition. Storage profiles with a `flavor` other than `aws` always use `aws`.

### AWS System Identity

Lakekeeper can load S3 credentials from its environment through the AWS SDK: the `AWS_*` environment variables, instance profiles, container credentials and SSO configurations. This is disabled by default and must be enabled server-wide.

!!! note
    When using system identities, we **strongly recommend** configuring external IDs. Without them, any user who may create Warehouses in Lakekeeper could use any role the system identity is allowed to assume. See [AWS's documentation on external IDs](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_common-scenarios_third-party.html).

To set up a system identity securely:

1. Create a dedicated AWS user as the system identity. Attach no permissions or trust policies to it: it only assumes roles that require the right external ID.
2. Configure Lakekeeper with this identity and enable system credentials:

    ```bash
    AWS_ACCESS_KEY_ID=...
    AWS_SECRET_ACCESS_KEY=...
    AWS_DEFAULT_REGION=...
    LAKEKEEPER__ENABLE_AWS_SYSTEM_CREDENTIALS=true
    ```

By default, a Warehouse that uses the system identity must set both an `external-id` and an `assume-role-arn`. The [Configuration Guide](./configuration.md#storage) describes the settings that relax this.

For this example, assume the system identity has the ARN `arn:aws:iam::123:user/lakekeeper-system-identity`. Each Warehouse needs an IAM role whose trust policy lets the system identity assume it with the external ID:

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

Create the Warehouse with the system identity:

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

Lakekeeper assumes `assume-role-arn` for its own reads and writes. The role is also the default `sts-role-arn`, which Lakekeeper assumes when vending credentials to clients, with a policy attached for the accessed table.

### STS Session Tags

`sts-session-tags` attaches session tags when Lakekeeper assumes a role via STS. The role's trust policy must then also allow `sts:TagSession`. The trust policy from the system identity example with this addition:

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

An ABAC policy references a session tag as `${aws:PrincipalTag/<tag name>}`. For example, this policy derives the S3 path from a `tenant` tag:

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

## S3-Compatible Storage

Most S3-compatible storages, such as MinIO, need no trust setup for vended credentials: a bucket and an access key that can read and write it are enough. Set `flavor` to `s3-compat`, which suits most self-hosted S3 storages. If `sts-role-arn` is set, Lakekeeper sends it with the STS request, though the storage may ignore it; otherwise the request carries no role.

A create-Warehouse request could look like this:

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
        "key-prefix": "lakekeeper-dev-warehouse"
    },
    "delete-profile": {
        "type": "hard"
    }
}
```

## Cloudflare R2

Lakekeeper supports Cloudflare R2 with all S3-compatible clients, including vended credentials via the `/accounts/{account_id}/r2/temp-access-credentials` endpoint.

First create a bucket: in the Cloudflare UI, select "R2 Object Storage" → "Overview" and choose "+ Create Bucket". We call ours `lakekeeper-dev`. Open the bucket, select the "Settings" tab and note down the "S3 API" URL.

Next, create an API token for Lakekeeper:

1. Go back to "R2 Object Storage" → "Overview" and select "Manage API tokens" in the "{} API" dropdown.
1. On the R2 token page, select "Create Account API token" and give the token any name. Select the "Admin Read & Write" permission: at the time of writing, `/accounts/{account_id}/r2/temp-access-credentials` accepts no other token. Click "Create Account API Token".
1. Note down the "Token value", "Access Key ID" and "Secret Access Key".

Finally, create the Warehouse through the UI or with a POST request to `/management/v1/warehouse`:

```json
{
  "warehouse-name": "r2_dev",
  "delete-profile": { "type": "hard" },
  "storage-credential": {
    "type": "s3",
    "credential-type": "cloudflare-r2",
    "account-id": "<Cloudflare Account ID, typically the long alphanumeric string before the first dot in the S3 API URL>",
    "access-key-id": "access-key-id-from-above",
    "secret-access-key": "secret-access-key-from-above",
    "token": "token-from-above"
  },
  "storage-profile": {
    "type": "s3",
    "bucket": "<name of your cloudflare r2 bucket, lakekeeper-dev in our example>",
    "region": "<your cloudflare region, i.e. weur>",
    "key-prefix": "path/to/my/warehouse",
    "endpoint": "<S3 API Endpoint, i.e. https://<account-id>.eu.r2.cloudflarestorage.com>"
  }
}
```

`cloudflare-r2` credentials set these parameters automatically:

- `assume-role-arn` is unset, as R2 does not support it
- `sts-enabled` is `true`
- `flavor` is `s3-compat`

`endpoint` is required. Use a [Data Location Hint](https://developers.cloudflare.com/r2/reference/data-location/#available-hints) as `region`.

## Alibaba Cloud OSS

!!! warning "Beta"
    Alibaba Cloud OSS support is in **beta**. The API and behavior may change in a future release.

Lakekeeper supports Alibaba Cloud Object Storage Service (OSS) with all S3-compatible clients, including vended credentials via the Alibaba Cloud STS [`AssumeRole`](https://www.alibabacloud.com/help/en/ram/developer-reference/api-sts-2015-04-01-assumerole) API. OSS is S3-compatible for data-plane operations, but its STS uses the Alibaba Cloud RPC signing scheme and not AWS SigV4, so it needs the dedicated `aliyun-oss` credential type.

First, create a bucket in the OSS console and note down its name and region (e.g. `cn-hangzhou`). Lakekeeper accesses OSS through its S3-compatible interface, so use the S3-compatible endpoint: the `s3.`-prefixed host `https://s3.oss-<region>.aliyuncs.com` (e.g. `https://s3.oss-cn-hangzhou.aliyuncs.com`), as documented in [Use Amazon S3 SDKs to access OSS](https://www.alibabacloud.com/help/en/oss/developer-reference/use-amazon-s3-sdks-to-access-oss).

Next, create the identity Lakekeeper authenticates with and the role it assumes to vend downscoped credentials:

1. In the RAM console, create a RAM user for Lakekeeper and generate an AccessKey pair for it. Note down the "AccessKey ID" and "AccessKey Secret".
1. Create a RAM role that Lakekeeper assumes to vend credentials (e.g. `lakekeeper-oss`). Grant this role the OSS permissions on your bucket, and configure its trust policy so the RAM user above is allowed to assume it (`sts:AssumeRole`). Note down the role's ARN (e.g. `acs:ram::123456789012:role/lakekeeper-oss`).

Finally, create the Warehouse through the UI or with a POST request to `/management/v1/warehouse`:

```json
{
  "warehouse-name": "oss_dev",
  "delete-profile": { "type": "hard" },
  "storage-credential": {
    "type": "s3",
    "credential-type": "aliyun-oss",
    "access-key-id": "<AccessKey ID of the RAM user>",
    "secret-access-key": "<AccessKey Secret of the RAM user>"
  },
  "storage-profile": {
    "type": "s3",
    "bucket": "<name of your OSS bucket>",
    "region": "<OSS region, i.e. cn-hangzhou>",
    "key-prefix": "path/to/my/warehouse",
    "endpoint": "<S3-compatible OSS endpoint, i.e. https://s3.oss-cn-hangzhou.aliyuncs.com>",
    "sts-role-arn": "<ARN of the RAM role, i.e. acs:ram::123456789012:role/lakekeeper-oss>"
  }
}
```

`aliyun-oss` credentials set these parameters automatically:

- `flavor` is `s3-compat`
- `sts-enabled` is `true`

`endpoint` is required, and so is either `sts-role-arn` or `assume-role-arn` (the ARN of the RAM role Lakekeeper assumes). The STS endpoint is derived from the `region` (`https://sts.<region>.aliyuncs.com`); set `sts-endpoint` to override it, for example to use a VPC endpoint. If the RAM role's trust policy requires an [`sts:ExternalId`](https://www.alibabacloud.com/help/en/ram/user-guide/use-externalid-to-prevent-the-confused-deputy-problem) condition, provide it as `external-id` in the storage credential.

OSS supports only [virtual-hosted-style addressing](https://www.alibabacloud.com/help/en/oss/developer-reference/compatibility-with-amazon-s3), so Lakekeeper rejects `aliyun-oss` profiles that enable `path-style-access`.

!!! warning "Client checksum configuration required for OSS"
    OSS does not support the `aws-chunked` streaming-checksum uploads (`STREAMING-UNSIGNED-PAYLOAD-TRAILER`) that AWS SDKs released since early 2025 enable by default, and rejects them with `NotImplemented: Aws MultiChunkedEncoding STREAMING-UNSIGNED-PAYLOAD-TRAILER is not supported` (see [Use Amazon S3 SDKs to access OSS](https://www.alibabacloud.com/help/en/oss/developer-reference/use-amazon-s3-sdks-to-access-oss)). Any engine that receives vended credentials and writes to OSS directly — PyIceberg (both its PyArrow and FSSpec/boto3 file IO), Spark, Trino, Flink, … — fails on its first write unless the request-checksum mode is set to *when required*:

    - **Universal** (honored by all recent AWS SDKs — Python, Java, Go): set the environment variables `AWS_REQUEST_CHECKSUM_CALCULATION=when_required` and `AWS_RESPONSE_CHECKSUM_VALIDATION=when_required` in the client's environment.
    - **Spark** (Iceberg `S3FileIO`): equivalently as JVM options — `--conf "spark.driver.extraJavaOptions=-Daws.requestChecksumCalculation=when_required"` and the same for `spark.executor.extraJavaOptions`.
    - **Trino / Flink** (AWS SDK for Java): the environment variables above, or `-Daws.requestChecksumCalculation=when_required` in the JVM config.
    - **boto3** configured directly: `Config(request_checksum_calculation="when_required")`.

    This affects only clients writing to OSS directly; Lakekeeper's own metadata I/O is unaffected.

## CORS

[LoQE](engines.md#loqe) needs a [CORS policy](storage.md#cors) on the bucket. We recommend:

```json
[
    {
        "AllowedHeaders": ["*"],
        "AllowedMethods": ["GET", "HEAD", "PUT", "POST", "DELETE"],
        "AllowedOrigins": ["https://lakekeeper.example.com"],
        "ExposeHeaders": ["ETag", "Content-Range"]
    }
]
```

Replace `https://lakekeeper.example.com` with the origin where your Lakekeeper instance is hosted. `ETag` must be exposed for multipart uploads and `Content-Range` for reading file sizes from range requests. Validation sends the preflight a browser would send, which shows allowed origins, methods and request headers but not exposed response headers, so it cannot verify `ExposeHeaders`.

To set the policy on AWS:

1. In the AWS S3 console, click the name of your bucket.
2. Choose the **Permissions** tab.
3. In the **Cross-origin resource sharing (CORS)** section, choose **Edit**.
4. Paste the policy into the CORS configuration editor. The text must be valid JSON.
5. Choose **Save changes**.

## Updating the Storage Profile

`bucket` and `key-prefix` cannot change on `update-storage-profile`, and `region` cannot change unless the profile sets an `endpoint`. An update that omits `allow-alternative-protocols` or `storage-layout` keeps their current values.
