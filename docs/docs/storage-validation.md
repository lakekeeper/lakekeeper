---
description: "Validate Lakekeeper storage profiles and credentials before you save them: the validation endpoints, every check they run, warnings for CORS and STACKIT bucket policies, and probe timeouts."
---

# Storage Validation

Lakekeeper validates the storage profile and credential when a Warehouse is created or its storage is updated, and rejects the request with a single error if any check fails. To see the full picture before committing to a change — or to find out whether an existing Warehouse's storage still works — use the validation endpoints. They run the same storage and configuration checks, persist nothing, and report the outcome of every check individually.

## Endpoints

| Endpoint | Validates |
| --- | --- |
| `POST /management/v1/warehouse-creation-validation` | A configuration you are about to create |
| `POST /management/v1/warehouse/{warehouse_id}/storage/validate-profile` | A storage profile update |
| `POST /management/v1/warehouse/{warehouse_id}/storage/validate-credential` | A replacement credential |
| `POST /management/v1/warehouse/{warehouse_id}/storage/validate-access` | The storage the Warehouse currently runs with |

Each returns `200 OK` regardless of the outcome — a failing check is a result, not a request error. Inspect `valid` and the per-check entries. A non-200 is returned only when the request itself cannot be processed: missing project (400), insufficient permission (403), unknown Warehouse (404), or an unreachable secret store (500). A caller who cannot see the Warehouse at all gets 404 rather than 403, so an unknown Warehouse and one the caller may not access are indistinguishable by design.

Validation requires the same permission as the operation it stands in for: validating a new Warehouse requires `create_warehouse` on the project, the others require `update_storage` on the Warehouse.

Request bodies are exactly the bodies of the endpoints they stand in for — validating a new Warehouse takes the `Create Warehouse` body verbatim, so a dry-run cannot drift from what it predicts:

```json
{
  "warehouse-name": "analytics",
  "storage-profile": { "type": "s3", "bucket": "my-bucket", "region": "us-east-1", "sts-enabled": true },
  "storage-credential": { "type": "s3", "credential-type": "access-key", "aws-access-key-id": "...", "aws-secret-access-key": "..." }
}
```

```json
{
  "valid": false,
  "checks": [
    { "name": "profile-well-formed", "status": "passed", "duration-ms": 0 },
    { "name": "profile-compatible", "status": "skipped", "reason": "Only applies when updating an existing warehouse." },
    { "name": "warehouse-name-valid", "status": "passed", "duration-ms": 3 },
    { "name": "location-exclusive", "status": "passed", "duration-ms": 3 },
    { "name": "spec-mutable", "status": "skipped", "reason": "Only applies when updating an existing warehouse." },
    { "name": "format-version-policy-consistent", "status": "passed", "duration-ms": 0 },
    { "name": "managed-by-allowed", "status": "passed", "duration-ms": 0 },
    { "name": "storage-client-initialized", "status": "passed", "duration-ms": 41 },
    { "name": "lakekeeper-read-write", "status": "passed", "duration-ms": 212 },
    { "name": "vended-credentials-issued", "status": "passed", "duration-ms": 180 },
    { "name": "vended-credentials-read-write", "status": "failed", "duration-ms": 154,
      "error": { "message": "...", "type": "PermissionDenied", "code": 400 } },
    { "name": "vended-credentials-scope-enforced", "status": "passed", "duration-ms": 149 },
    { "name": "cleanup", "status": "passed", "duration-ms": 88 },
    { "name": "cors-origin-allowed", "status": "warning", "duration-ms": 41,
      "error": { "message": "CORS does not allow origin `https://lakekeeper.example.com` for `GET`, `HEAD`, `PUT`, `POST`, `DELETE`. ...", "type": "CorsOriginNotAllowed", "code": 412,
        "stack": ["`GET`, `HEAD`, `PUT`, `POST`, `DELETE`: Access-Control-Allow-Origin expected `https://lakekeeper.example.com` or `*`, found no header"] } },
    { "name": "bucket-access-restricted", "status": "skipped", "reason": "Only checked for STACKIT, ..." }
  ]
}
```

## Checks

| Check | Meaning |
| --- | --- |
| `profile-well-formed` | The storage profile is internally consistent and can be normalized |
| `profile-compatible` | The new profile is a permitted evolution of the current one |
| `warehouse-name-valid` | The name is well-formed and not already used in the project. Compared case-insensitively, matching the database's uniqueness constraint |
| `location-exclusive` | No other Warehouse in the project already occupies this location |
| `spec-mutable` | The Warehouse's spec is not locked to an external control plane |
| `format-version-policy-consistent` | The Iceberg format-version policy is self-consistent |
| `managed-by-allowed` | The caller may create a Warehouse under the requested `managed-by` |
| `storage-client-initialized` | A storage client can be built from the profile and credential |
| `lakekeeper-read-write` | Lakekeeper itself can write, read back and delete a probe file in the Warehouse's object storage |
| `vended-credentials-issued` | Temporary downscoped credentials can be issued |
| `vended-credentials-read-write` | Downscoped credentials work below the table location |
| `vended-credentials-scope-enforced` | Downscoped credentials are refused write access *outside* the table location |
| `cleanup` | Everything written during validation was removed again |
| `cors-origin-allowed` | The storage's CORS policy lets the Lakekeeper origin read and write from a browser, as [LoQE](engines.md#loqe) does; for ADLS, which LoQE only reads, read access is checked. Reported as `warning` when it does not. Skipped for OneLake, and for storage profiles that vend no credentials, since LoQE reads storage only with vended credentials |
| `bucket-access-restricted` | The STACKIT bucket policy keeps out the project's other credentials groups, see [Restricting Bucket Access](storage-stackit.md#restricting-bucket-access). Reported as `warning` when the policy is missing, leaves access open, or cannot be read. Skipped for other storage |

## Check Statuses

Each check is `passed`, `failed`, `warning` or `skipped`. Any `failed` check makes `valid` false.

A check is `skipped` when it does not apply (credential vending is disabled, or the check only applies to a different operation) or when a prerequisite failed; the `reason` field says which. Skipped checks do not make a configuration invalid.

A check is `warning` when it found a problem that does not stop the Warehouse from working, such as a CORS policy that blocks the in-browser query console or a STACKIT bucket that every credentials group of the project can reach. Its `error` says what was found; for `cors-origin-allowed`, each `stack` line names the refused methods with the expected and the found response header. Warnings never make `valid` false and never block creating or updating a Warehouse.

With `LAKEKEEPER__SKIP_STORAGE_VALIDATION=true` every storage check reports `skipped` rather than silently passing. Note that `valid` is still `true` in that case — nothing failed, but nothing was checked either, so read the individual checks before trusting a green result.

The storage pages explain known provider failures in their Troubleshooting sections, for example for [STACKIT](storage-stackit.md#troubleshooting) and [OneLake](storage-onelake.md#troubleshooting).

New checks may be added in future releases. `duration-ms` is reported for diagnostics only and is not a performance guarantee.

## Limits

Validation is advisory: a concurrent request can still take the Warehouse name or change a bucket policy between validating and creating.

Validation writes and deletes probe objects under the Warehouse location, so it needs the same storage permissions as normal operation. Cleanup removes the probe prefix recursively.

Storage probes must finish within two thirds of `LAKEKEEPER__MAX_REQUEST_TIME` (20 seconds by default), counted from the request's arrival, and cleanup within the following sixth. A probe or cleanup that runs out fails with `StorageProbeTimeout`, so storage that Lakekeeper cannot reach still produces a report that names the stalled check instead of the request timing out. Connections to storage and to its credential endpoints (STS, token services) time out after 5 seconds.
