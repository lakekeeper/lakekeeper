# Storage credential vending — follow-ups

Open improvements identified while comparing our implementation against
[Apache Polaris](https://github.com/apache/polaris). Each item is *not* a
known bug; they are gaps where Polaris does something more robust and we
should match when feasible.

## 1. Azure: build SAS canonical-resource via the SDK

**What Polaris does:** uses the Azure Java SDK's
`DataLakePathClientBuilder.pathName(path).buildDirectoryClient()
.generateUserDelegationSas(...)`. The SDK constructs the canonical-resource
string for SAS signing the same way it constructs the request URL, so a
client/server encoding mismatch is impossible by construction.

**What we do:** [`AdlsProfile::sas`](../crates/lakekeeper/src/service/storage/az.rs)
constructs the canonical-resource manually (`/blob/{account}/{fs}/{rootless_path}`)
and calls the low-level `BlobSharedAccessSignature::new(...)` signer. Any
encoding inconsistency between us and the Azure URL parser silently 403s.

**Why deferred:** neither Rust SDK exposes the high-level builder today.

- Official `azure_storage_blob` SDK: doesn't yet support user-delegated SAS.
  Tracked in [Azure/azure-sdk-for-rust#3330](https://github.com/Azure/azure-sdk-for-rust/issues/3330).
- Unofficial `azure_storage*@0.21` (legacy, unmaintained): only exposes the
  low-level `BlobSharedAccessSignature::new(canonical_resource, ...)` —
  doesn't help.

The broader migration is tracked in
[lakekeeper#1689](https://github.com/lakekeeper/lakekeeper/issues/1689).
Revisit when the official SDK gains user-delegated SAS support.

## 2. URI parser preserving `?` and `#` in object keys

**What Polaris does:** [`StorageUri.parse`](https://github.com/apache/polaris/blob/main/polaris-core/src/main/java/org/apache/polaris/core/storage/StorageUri.java)
is a hand-rolled parser that finds the path start with `indexOf('/')` and
keeps everything after as the literal `rawPath` — explicitly because
`java.net.URI` treats `?` and `#` as query/fragment delimiters even though
they're legal characters in S3/GCS/ADLS object keys.

**What we do:** `Location` is built on `url::Url`, which has the same
limitation. A location like `s3://bucket/foo?bar/data` either fails parsing
or interprets `?bar/data` as a query string, dropping it from the path.

**Why deferred:** lower priority. Iceberg-managed paths use UUIDs in
practice, so the gap rarely materialises. If we want to support arbitrary
preexisting tables / external paths, replace `Location`'s parser with an
indexOf-based one similar to Polaris's.

## 3. AWS: defensive wildcard KMS read

**What Polaris does:** [`AwsCredentialsStorageIntegration.addKmsKeyPolicy`](https://github.com/apache/polaris/blob/main/polaris-core/src/main/java/org/apache/polaris/core/storage/aws/AwsCredentialsStorageIntegration.java)
adds `arn:aws:kms:<region>:<account>:key/*` as a read-only KMS resource for
AWS S3 when no specific KMS key is configured. Lets reads work on
KMS-encrypted buckets without requiring per-warehouse key configuration;
writes still require an explicit key.

**What we do:** [`S3Profile::get_sts_policy_string`](../crates/lakekeeper/src/service/storage/s3.rs)
only adds a KMS statement when `aws_kms_key_arn` is set. Reads of
SSE-KMS-encrypted objects fail without explicit config.

**Why deferred:** the wildcard ARN requires the AWS account ID, which we
don't currently have at policy-construction time. We have the role ARN
(`assume_role_arn`/`sts_role_arn`) which encodes the account, so we could
parse it out — but that crosses a layer boundary worth a focused PR. To
implement: parse `Arn::from_str` on the configured role ARN, extract
account, gate on `flavor == Aws` and absence of `aws_kms_key_arn`.

## 4. Multiple allowed KMS keys

**What Polaris does:** supports a list `allowedKmsKeys` per warehouse.

**What we do:** single `aws_kms_key_arn` field.

Useful when the same warehouse spans buckets encrypted with different keys.
Small config schema change, plus statement construction.
