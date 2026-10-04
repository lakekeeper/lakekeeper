---
description: "Configure Lakekeeper Warehouses on STACKIT Object Storage: storage services, credentials groups and trust policies for STS, bucket policies that keep other credentials groups out, CORS and troubleshooting."
---

# STACKIT Object Storage

STACKIT Object Storage is S3-compatible, and Lakekeeper backs Warehouses with it through a dedicated `stackit` storage profile. The profile exposes only the settings that apply to STACKIT: it derives the endpoint from `region` and `storage-service`, pins addressing and S3 flavor, and vends downscoped credentials through a STACKIT credentials group. Table locations use `s3://`, so engines read and write with their regular S3 file IO.

Clients get access through [vended credentials or remote signing](storage.md#how-clients-access-data). Vended credentials use STS, which assumes a credentials group; where the storage service has no STS, clients use remote signing.

## Configuration Parameters

| Parameter                    | Type    | Required | Default               | Description |
|------------------------------|---------|----------|-----------------------|-------------|
| `bucket`                     | String  | Yes      | -                     | Name of the STACKIT bucket. Must not contain `.`. |
| `region`                     | String  | Yes      | -                     | STACKIT region, e.g. `eu01`. |
| `storage-service`            | String  | No       | `object-storage`      | STACKIT storage service that holds the bucket. See [Storage Services](#storage-services) below. |
| `key-prefix`                 | String  | No       | None                  | Subpath within the bucket to use. |
| `endpoint`                   | URL     | No       | Derived               | Endpoint override for a STACKIT endpoint outside the public naming scheme, which STACKIT hands out per customer. Takes precedence over `storage-service`. |
| `sts-enabled`                | Boolean | No       | `true`                | Vend temporary downscoped credentials via STS. Requires `credentials-group-urn`. |
| `credentials-group-urn`      | String  | If STS   | None                  | URN of the STACKIT credentials group to assume when vending credentials, e.g. `urn:sgws:identity::12345678901234567890:group/credentials-group-a1b2c3`. Copy it verbatim from the credentials group. |
| `sts-token-validity-seconds` | Integer | No       | `3600`                | Validity of vended credentials in seconds. |
| `remote-signing-enabled`     | Boolean | No       | `true`                | Allow clients to have Lakekeeper sign their S3 requests. The only client path when `sts-enabled` is `false`. |
| `push-s3-delete-disabled`    | Boolean | No       | `true`                | Push `s3.delete-enabled=false` to clients, discouraging Spark from deleting files directly and bypassing soft-deletion. |
| `storage-layout`             | Object  | No       | `{"type": "default"}` | Controls how namespace and tabular directories are structured under the warehouse base location. See [Storage Layout](storage-layout.md). |

At least one of `sts-enabled` and `remote-signing-enabled` must be `true`.

### Storage Services

| `storage-service` | Endpoint                                          | Regions   |
|-------------------|---------------------------------------------------|-----------|
| `object-storage`  | `https://object.storage.<region>.onstackit.cloud` | All       |
| `data-platform`   | `https://dataplatform.storage.<region>.onstackit.cloud` | `eu01` |

Select the service that holds your bucket. When the endpoint is derived, Lakekeeper rejects `data-platform` in any region other than `eu01`; an explicit `endpoint` takes precedence over `storage-service`, and the region check does not apply.

## Credentials

Lakekeeper authenticates with an access key created inside a STACKIT credentials group, as `"type": "stackit"` with `"credential-type": "access-key"`. The same group is assumed via STS to vend downscoped credentials; set its URN as `credentials-group-urn`. The URN uses the credentials group's ID, not its display name.

To be assumed, the group needs a trust policy allowing `sts:AssumeRole`. The principal is the group's URN with `:group/` replaced by `:user/`. Set it through the STACKIT API, authenticated with a STACKIT service account token. `<group-uuid>` is the credentials group's ID as the STACKIT API lists it, a UUID, which differs from the `credentials-group-<id>` part of its URN:

```bash
curl -X POST \
  "https://dataplatform-storage.api.stackit.cloud/v2/project/<project-id>/regions/eu01/credentials-group/<group-uuid>/trust-policy" \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{
    "trustPolicy": {
      "Statement": [
        {
          "Action": "sts:AssumeRole",
          "Effect": "Allow",
          "Principal": { "AWS": "urn:sgws:identity::12345678901234567890:user/credentials-group-a1b2c3" }
        }
      ]
    }
  }'
```

Not every STACKIT storage service offers STS and trust policies yet; the data platform storage service does. Where they are not available, set `sts-enabled` to `false`; clients then use remote signing.

## Example

A POST request to `/management/v1/warehouse` to create a warehouse on the data platform storage:

```json
{
  "warehouse-name": "stackit_dev",
  "delete-profile": { "type": "hard" },
  "storage-credential": {
    "type": "stackit",
    "credential-type": "access-key",
    "access-key-id": "...",
    "secret-access-key": "..."
  },
  "storage-profile": {
    "type": "stackit",
    "bucket": "my-warehouse",
    "region": "eu01",
    "storage-service": "data-platform",
    "key-prefix": "lakekeeper-dev",
    "credentials-group-urn": "urn:sgws:identity::12345678901234567890:group/credentials-group-a1b2c3"
  }
}
```

## Restricting Bucket Access

Every credentials group of a STACKIT project can read and write every bucket of the project, including groups created later for other applications. A bucket policy is the only way to narrow this. We recommend one that denies all access to every group except Lakekeeper's and an admin group, and lets only the admin group change the policy:

```json
{
  "Statement": [
    {
      "Sid": "OnlyLakekeeperAndAdmin",
      "Effect": "Deny",
      "NotPrincipal": {
        "SGWS": [
          "urn:sgws:identity::12345678901234567890:group/credentials-group-a1b2c3",
          "urn:sgws:identity::12345678901234567890:group/credentials-group-d4e5f6"
        ]
      },
      "Action": "s3:*",
      "Resource": [
        "urn:sgws:s3:::my-warehouse",
        "urn:sgws:s3:::my-warehouse/*"
      ]
    },
    {
      "Sid": "OnlyAdminChangesPolicy",
      "Effect": "Deny",
      "NotPrincipal": {
        "SGWS": "urn:sgws:identity::12345678901234567890:group/credentials-group-d4e5f6"
      },
      "Action": ["s3:PutBucketPolicy", "s3:DeleteBucketPolicy"],
      "Resource": "urn:sgws:s3:::my-warehouse"
    }
  ]
}
```

In the example, `credentials-group-a1b2c3` is Lakekeeper's `credentials-group-urn` and `credentials-group-d4e5f6` is the admin group; replace both and `my-warehouse` with your values. The second statement keeps Lakekeeper's access key, and anyone who obtains it, from changing or removing the policy. Credentials that Lakekeeper vends act as the group in `credentials-group-urn`, so they keep working.

Apply the policy through the S3 API with the admin group's access key. Any group can set the first policy; afterwards only the admin group can change it. Use the endpoint of the Warehouse's storage service; the example uses the data platform storage service, and the object storage service uses `https://object.storage.eu01.onstackit.cloud`:

```bash
aws s3api put-bucket-policy \
  --endpoint-url https://dataplatform.storage.eu01.onstackit.cloud \
  --bucket my-warehouse \
  --policy file://policy.json
```

!!! warning "Lock-out risk"
    The policy denies the bucket to every group it does not list, including groups used by admin tooling, and only the admin group can change or remove it. Use a dedicated admin or break-glass group, store its access key safely, and keep it listed in both statements.

[Storage validation](storage-validation.md) reads the bucket policy with the Warehouse's access key and reports a `bucket-access-restricted` warning when the bucket has no policy, when the policy cannot be read, or when no `Deny` statements deny all actions (`s3:*`) on both the bucket and the Warehouse's objects without a `Condition` while sparing only named credentials groups in `NotPrincipal`. The bucket and its objects may be covered by separate statements. The warning lists, for the closest statement, what it expected and what it found. A `passed` check means the policy has this shape; it does not tell whether the groups in `NotPrincipal` are the right ones, because the S3 API does not reveal which group an access key belongs to. Access can also be restricted by other means, which is why the finding is a warning.

Reading the policy needs `s3:GetBucketPolicy`, which the recommended policy leaves to Lakekeeper's group. If you also deny it to Lakekeeper, the check reports that the policy cannot be read.

## CORS

[LoQE](engines.md#loqe) needs a [CORS policy](storage.md#cors) on the bucket. STACKIT sets CORS through the S3 API, using an access key of the bucket's credentials group. Save the policy as `cors.json`:

```json
{
  "CORSRules": [
    {
      "AllowedHeaders": ["*"],
      "AllowedMethods": ["GET", "HEAD", "PUT", "POST", "DELETE"],
      "AllowedOrigins": ["https://lakekeeper.example.com"],
      "ExposeHeaders": ["ETag", "Content-Range"]
    }
  ]
}
```

Replace `https://lakekeeper.example.com` with the origin where your Lakekeeper instance is hosted. `ETag` must be exposed for multipart uploads and `Content-Range` for reading file sizes from range requests; validation cannot verify `ExposeHeaders`. Apply the policy with the endpoint of the Warehouse's storage service; the example uses the data platform storage service, and the object storage service uses `https://object.storage.eu01.onstackit.cloud`:

```bash
aws s3api put-bucket-cors \
  --endpoint-url https://dataplatform.storage.eu01.onstackit.cloud \
  --bucket my-warehouse \
  --cors-configuration file://cors.json
```

## Updating the Storage Profile

`bucket`, `key-prefix`, `region` and the resolved endpoint are immutable on `update-storage-profile`: each storage service and endpoint is a distinct storage tenant, so changing them would point the warehouse at other data. `storage-service` and `endpoint` can be exchanged for each other as long as they resolve to the same endpoint, e.g. replacing `"endpoint": "https://dataplatform.storage.eu01.onstackit.cloud"` with `"storage-service": "data-platform"`. All other fields can be updated.

## Troubleshooting

When STS rejects a request for vended credentials, Lakekeeper explains the answers STACKIT is known to give. The error's `type` names the cause and its `message` says what to change:

| Error `type` | Cause |
|---|---|
| `StackitStsUnavailable` | The storage service has no STS. A bucket on the data platform storage service needs a Warehouse created with `storage-service` set to `data-platform`, since an existing Warehouse cannot change its storage service. Otherwise set `sts-enabled` to `false` to use remote signing. |
| `StackitCredentialsGroupNotFound` | No credentials group matches `credentials-group-urn`. The URN uses the group's ID (`credentials-group-<id>`), not its display name, and the account of the project that holds the bucket. |
| `StackitCredentialsGroupUrnInvalid` | `credentials-group-urn` is not a credentials-group URN. It must look like `urn:sgws:identity::<account>:group/credentials-group-<id>`. |
| `StackitTrustPolicyMissing` | The credentials group has no [trust policy](#credentials) that lets Lakekeeper assume it. The message contains the trust policy to add. |
