---
description: "Configure Lakekeeper Warehouses on Google Cloud Storage: parameters, service account keys, GCP system identities, downscoped vended credentials and CORS."
---

# Google Cloud Storage

The `gcs` storage profile backs Warehouses with a Google Cloud Storage bucket, with or without hierarchical namespaces (supported since Lakekeeper 0.8.2). Table locations use `gs://`. Lakekeeper vends downscoped STS tokens to clients; GCS has no remote signing.

## Configuration Parameters

| Parameter        | Type    | Required | Default               | Description  |
|------------------|---------|----------|-----------------------|--------------|
| `bucket`         | String  | Yes      | -                     | Name of the GCS bucket. |
| `key-prefix`     | String  | No       | None                  | Subpath in the bucket to use for this warehouse. |
| `sts-enabled`    | Boolean | No       | `true`                | Whether to enable STS (Security Token Service) downscoped token generation for GCS. When disabled, clients cannot use vended credentials for this storage profile. |
| `storage-layout` | Object  | No       | `{"type": "default"}` | Controls how namespace and tabular directories are structured under the warehouse base location. See [Storage Layout](storage-layout.md). |

## Credentials

The `storage-credential` of a GCS Warehouse has `"type": "gcs"` and one of these `credential-type` values:

| `credential-type` | Use for |
|---|---|
| `service-account-key` | A service account key, see [Service Account Key](#service-account-key) |
| `gcp-system-identity` | The service account the Lakekeeper process runs as, see [GCP System Identity](#gcp-system-identity) |

Either way, the service account needs access to the bucket, typically the Storage Admin role on it.

### Service Account Key

Create the key in the Google Cloud Console and pass it when creating the Warehouse:

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
      "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/abc%40example-project-1234.iam.gserviceaccount.com",
      "universe_domain": "googleapis.com"
    }
  }
}
```

### GCP System Identity

!!! warning
    Enabling GCP system identities grants Lakekeeper access to any storage location the service account has permissions for. Carefully review and limit the permissions of the service account to avoid unintended access to sensitive resources. Additionally, limit Warehouse creation permissions in Lakekeeper to users who are authorized to access all locations that the system identity can access.

With a system identity, Lakekeeper authenticates with the service account the application or virtual machine runs as, either a Compute Engine default service account or a user-assigned one. The feature is disabled by default and must be enabled server-wide:

```bash
LAKEKEEPER__ENABLE_GCP_SYSTEM_CREDENTIALS=true
```

Then create the Warehouse with:

```json
{
  "storage-credential": {
    "type": "gcs",
    "credential-type": "gcp-system-identity"
  }
}
```

## CORS

[LoQE](engines.md#loqe) needs a [CORS policy](storage.md#cors) on the bucket. Save this policy as `cors.json`, replacing `https://lakekeeper.example.com` with the origin where your Lakekeeper instance is hosted:

```json
[
    {
        "origin": ["https://lakekeeper.example.com"],
        "method": ["GET", "HEAD", "PUT", "POST", "DELETE"],
        "responseHeader": ["Authorization", "Content-Type", "Range", "ETag", "Content-Range"],
        "maxAgeSeconds": 3600
    }
]
```

GCS returns `responseHeader` as the allowed request headers of a preflight, and browsers never let a `*` there cover `Authorization`, so the headers are listed explicitly. Apply the policy with `gcloud storage buckets update gs://<bucket> --cors-file=cors.json`.

## Updating the Storage Profile

`bucket` and `key-prefix` cannot change on `update-storage-profile`. An update that omits `storage-layout` keeps the current layout.
