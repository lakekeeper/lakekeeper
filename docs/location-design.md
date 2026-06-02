# Location: design notes and tradeoffs

This is an internal design document, not user docs. It captures what we know
about Lakekeeper's `Location` type, the bug classes it has produced, and the
options for making it more robust.

## What `Location` is

`Location` ([crates/io/src/location.rs](../crates/io/src/location.rs)) is a
URL-string wrapper that represents an object-storage location across backends
(S3, ADLS, GCS, in-memory). It's the canonical type passed across nearly
every Lakekeeper boundary that has to refer to "a place in cloud storage".

```rust
pub struct Location {
    full_location: String,        // raw input
    scheme: String,               // before "://"
    authority_and_path: String,   // after "://", split-extracted, NOT url::Url's view
}
```

Per-backend wrappers (`S3Location`, `AdlsLocation`, `GcsLocation`) parse a
`Location` and expose backend-specific accessors:

- `S3Location::bucket_name()`, `S3Location::key()` (segments)
- `AdlsLocation::account_name()`, `AdlsLocation::blob_name()`
- `GcsLocation::bucket_name()`, `GcsLocation::object_name()`

## Where `Location` appears

| Surface | Use |
|---------|-----|
| Iceberg REST API | `createTable`, `loadTable`, namespace `properties.location` — all carry URL strings per spec. |
| Credential vending | IAM Resource ARN (S3), SAS canonical-resource (ADLS), CEL access boundary (GCS) — all derived from the table location. |
| IO | `LakekeeperStorage::{write,read,delete,list}(path: &str)` takes the URL string. |
| Database | Warehouse stores storage-profile location; tables store metadata-location. |
| Iceberg metadata | `.metadata.json` files contain absolute URL strings (location, snapshot file paths, manifests). |
| Cross-tool interop | Spark, Trino, pyiceberg expect/produce URL strings. |

## The fundamental bug class

`url::Url::parse` and our raw split-based parser **disagree** about what's
in the same string:

| Input character / form | `url::Url` view | Our raw view | Notes |
|---|---|---|---|
| Literal `?` | Query string starts | Stays in path | Two views diverge silently |
| Literal `#` | Fragment | We explicitly reject | OK |
| `%2E` / `%2E%2E` | Normalised away (RFC 3986 path-traversal) | Preserved as raw segment | Path silently shrinks when `Url::join` re-runs (e.g. inside cloud SDK) |
| `%2F` | Preserved as `%2F` literal | Preserved | Decodes to `/` → ambiguous nesting |
| Literal space, control bytes, Unicode | Percent-encoded during parse | Preserved as raw | Encoded vs raw mismatch |

The two views feed different code paths. Whichever one you sample produces
a different answer. Result: silent path-divergence, signature mismatches,
and over/under-restrictive credentials.

## Concrete bugs we've hit

1. **ADLS SAS canonical mismatch on percent-encoded chars.** We hand-rolled
   the canonical-resource string using the encoded form (e.g. literal
   `%3F`). Azure URL-decodes the request URL before recomputing the
   canonical → signature mismatch → silent 403. Fix: percent-decode before
   building the canonical.
2. **Lakekeeper writes silently to the wrong path.** When `Url::join` runs
   inside the Azure SDK to construct request URLs, it normalises encoded
   `..`/`.` away. The location Lakekeeper stores and the URL Azure receives
   diverge. Fix (defensive): reject decoded `.`/`..` / encoded slash at
   parse time.
3. **Azure rejects whitespace-only segments.** `%20` as an entire segment
   fails server-side with `InvalidUri`. Fix: reject up-front with a clear
   Lakekeeper-side error.
4. **MinIO doesn't resolve AWS IAM policy variables.** Our `${*}`/`${?}`/
   `${$}` glob escapes are treated as literal 4-char strings. **Safe** (over-
   restrict, not over-permit), but a usability gap on MinIO with `*`/`?`/`$`
   in paths.
5. **GCP CEL — encoded vs decoded form.** GCP's `resource.name` is the
   decoded object name; we built the CEL with the encoded form. Empirically
   works for the chars we've tested but the principle is fragile.

## Per-backend nuances

### S3 / S3-compat

- IAM `Resource` ARNs are compared as case-sensitive strings. AWS
  percent-decodes URL paths before constructing the ARN, so the policy
  Resource should match the **decoded** form for AWS proper.
- MinIO does not implement IAM policy-variable substitution (`${*}` etc.).
  Our escape produces over-restrictive policies on MinIO; safe but
  user-visible.
- Sub-delim chars (`*`, `'`, `+`, `$`, `,`, `;`, `=`) survive `url::Url`
  parsing unencoded. They reach the policy literally.

### ADLS

- SAS canonical-resource expects the **decoded** path. Encoded forms
  produce signature mismatches.
- DFS endpoint vs Blob endpoint: handled URL-encoding differs. Vended
  Service SAS works against the Blob endpoint with a single PUT; DFS needs
  3-step create/append/flush.
- Whitespace-only segments → `400 InvalidUri` server-side.
- `url::Url`'s path normalisation (run by `Url::join` deep inside the SDK)
  is the source of the silent-divergence bug.

### GCS

- CEL `resource.name` is the decoded object name; the access boundary
  expression should match the decoded form.
- GCP's CEL subset is restricted: no raw-string literals (`r'...'`), no
  string concat (`+`). Single-quoted literals only, with standard escape
  sequences.
- GCS bucket names are constrained (`[a-z0-9.\-_]`); object names allow
  arbitrary UTF-8 except CR/LF.

## Iceberg REST spec constraints

- The spec defines `location` as a string. Tools (Spark, Trino, pyiceberg)
  all serialise / parse URL strings here.
- Iceberg metadata files (`*.metadata.json`) embed absolute URL strings.
- We cannot escape URL strings at the API boundary — that's the spec.
  We have flexibility only on what we do **internally** with the parsed
  location.

## Validations we've added (defensive boundary)

| Validation | Rationale |
|------------|-----------|
| Reject literal `?` and `#` at `Location::from_str` | The two parser views diverge for these |
| Reject ADLS path segments that decode to whitespace-only | Azure rejects with `InvalidUri` |
| Reject ADLS path segments that decode to `.`, `..`, or contain `/` | `url::Url` strips `.`/`..`; `%2F` decoding is ambiguous |
| `reduce_scheme_string` returns `Result` (no silent fallback) | Don't sign against unvalidated input |

These are pragmatic boundary defenses. They do not change the underlying
fact that we have two parser views of the same string.

## Solution options

### Option 1: Backend-specific decoded accessors (minimal)

Add `decoded_*` accessors on each backend Location type:

```rust
impl AdlsLocation {
    pub fn blob_name(&self) -> String { /* encoded form for SDK */ }
    pub fn decoded_path(&self) -> String { /* decoded form for SAS canonical */ }
}
```

Document: "for canonical-resource / IAM / CEL purposes, use the
`decoded_*` accessor; for SDK URL construction, use the encoded one."

- **Pro:** minimal change, codifies an existing pattern, no migration.
- **Con:** discipline-based — easy to forget which accessor to call. Hidden
  invariant.

### Option 2: Normalise on store

At every `Location::from_str`, call `url::Url::parse(value)` and store
*only* its serialised form. One canonical view, no divergence.

- **Pro:** mechanically eliminates the two-view bug class.
- **Con:** `url::Url` rejects / re-encodes inputs we currently accept:
  - Literal Unicode (`üñîçødé`) → encoded as `%C3%BC...` → input differs
    from stored, breaks user-facing strings.
  - Literal whitespace → encoded as `%20`.
  - Other reserved chars get auto-encoded.
- Backwards-incompatible for stored data.

### Option 3: Typed enum (largest)

```rust
enum Location {
    S3   { bucket: String, key: String },     // key: decoded bytes
    Adls { account: String, filesystem: String, path: String },
    Gcs  { bucket: String, object: String },
    Memory { uuid: String },
}
```

Each variant stores the backend's natural identifier form. URL parsing
and serialisation move to the API boundary.

- **Pro:** eliminates the entire bug class at compile time. Cross-backend
  mistakes become type errors. Single source of truth per variant.
- **Con:**
  - Large refactor across the codebase.
  - Migration of existing stored locations.
  - URL parsing complexity moves to the API boundary, doesn't disappear.
  - Iceberg metadata files still contain URL strings — we'd parse those
    on read and serialise on write.
  - Adding a new S3-compat backend with a custom scheme = new variant +
    every match-arm update.
  - Less ergonomic for cross-backend code (`is_overlapping_location`,
    `default_metadata_location`, etc.).

### Option 4 (hybrid): typed enum + URL-string compat layer

Keep a `Location` API surface that looks like today's URL-string type, but
internally back it with the typed enum. Convert at construction (via
`Location::from_str`) and serialise back on `as_str()`.

- **Pro:** internal correctness of Option 3, no API churn.
- **Con:** all the work of Option 3, plus the compat layer. The URL
  string is still the wire format and the source of all the encoding
  rules — the bugs may just hide in the conversion layer.

## Recommendation

- **Now:** the boundary validators we've added (literal `?`/`#` rejection,
  ADLS segment rejections, `Result`-returning `reduce_scheme_string`) plus
  Option 1 (decoded accessors) where canonical/IAM/CEL paths exist. Cheap,
  keeps current API.
- **If recurring:** revisit Option 3 with an RFC. Plan database migration
  and the API-boundary parse/serialise centralisation. Don't undertake
  unless we hit the same bug class twice more.
- **Don't pick Option 2** unless we can confirm none of our users have
  literal Unicode in object names (currently working, would regress).

## How others solve this

### Apache Polaris (Java)

**`StorageUri` ([`polaris-core/.../storage/StorageUri.java`](https://github.com/apache/polaris/blob/main/polaris-core/src/main/java/org/apache/polaris/core/storage/StorageUri.java)) is a hand-rolled record (`scheme, authority, rawPath`)** built specifically to escape `java.net.URI`'s gotchas. PR #1604 (May 2025) and PR #1586 explicitly removed `URI.normalize()` from the storage path because Iceberg can produce legitimate `..`/`.` segments and Java URI was normalising them away. PR #4210 fixed double URL-decoding of table identifiers (Jersey already decodes; they were decoding again). Issue #552 was the genesis — column names like `_foo_bar_` ended up in Iceberg paths and `URI.create` rejected them. They've actively `@Deprecated(forRemoval = true)`-ed `StorageUtil.getBucket(URI)`, routing through `StorageUri.parse(String)` instead.

**Properties:**
- `rawPath` is intentionally raw. No decode, no normalize. `?` and `#` are kept inside the path.
- One parser, used end-to-end. The SDK gets exactly the bytes the catalog stored.
- Per-backend types (`S3Location`, `AzureLocation`) extract structural fields by regex from the raw string — they don't store decoded vs encoded twice.
- IAM glob escaping (`escapeIamGlobLiteral`) is a *separate, narrow concern* on top of the raw path; only escapes `*`, `?`, `$` per AWS rules; doesn't touch percent-encoded chars.
- Azure SAS path: `location.getFilePath()` (raw substring after authority) → `DataLakePathClientBuilder.pathName(path)` → SDK handles encoding.

**Limitations:**
- `StorageLocation.equals`/`isChildOf` is pure string prefix on the raw form (no normalisation). Subclasses override.
- Residual `URI.create` for `file:/` collapse.
- No `GcpLocation` class — GCS uses bare `StorageUri`.

### Apache Gravitino (Java)

Cautionary tale: **plain `String` everywhere**, no wrapper type. Per-backend ad-hoc parsing at the credential generator boundary using `URI.create(...)`. ADLS does `URI.getPath()` (decoded) → `DataLakeSasImplUtil(..., trimSlashes(path), true).generateUserDelegationSas(...)` — the *exact* SAS canonicalisation mismatch surface we just fixed. AWS partition selection is fragile (`roleArn.contains("aws-cn")`). They have the latent bugs we're trying to fix.

Worth borrowing: their `PathBasedCredentialContext { user, writePaths, readPaths }` shape and `CredentialCacheKey` caching pattern. **Don't** borrow their string-typed location flow.

### `object_store` crate (Rust, Apache Arrow)

The most sophisticated reference. **`Path { raw: String }`** stores the canonical decoded string. Two clearly-named constructors:

- **`Path::parse(s)`** — strict validator. Rejects `.`/`..`/empty segments, ASCII controls. Stores input verbatim. **Does not mutate.**
- **`Path::from(s)`** (and `From<&str>`) — infallible encoder. Per-segment percent-encodes a curated `INVALID` `AsciiSet`. Will **double-encode** an already-encoded input. Maintainers (`tustvold`) acknowledge in issue #457 this is a "historical quirk that'd now be quite difficult to change" — making `From<&str>` lossy is recognised as surprising.
- **`Path::from_url_path(s)`** — percent-decodes first, then `parse`. Used when consuming a URL's path component.

**Encoding lives at the egress boundary** — never in the type. Each backend re-encodes `path.as_ref()` with its own `AsciiSet`:

- S3: `STRICT_PATH_ENCODE_SET = NON_ALPHANUMERIC - {-._~/}` (RFC 3986 unreserved + `/`, the SigV4 canonical set; references `sigv4-create-canonical-request.html`).
- GCS: `utf8_percent_encode(path, NON_ALPHANUMERIC)` (encodes `/` too).
- Azure: `url::Url::path_segments_mut().push(...)` per `PathPart` — defers to the `url` crate.

**Equality = byte equality of canonical form.** Documented bugs: #112, #194, #223, #235, #384, #457 — almost all are about asymmetry between `parse` and `from`, control-char handling, and signing-canonical mismatch. The lesson: keep encoding rules adjacent to their signers, not in the type.

### iceberg-rust (the same problem domain, in Rust)

**`TableMetadata::location: String`** — opaque string, no wrapper, no parser. The only typed location-ish thing is `MetadataLocation { table_location: String, version: i32, id: Uuid, compression_codec: CompressionCodec }`. Path joining is bare `format!("{table_location}/data/{file_name}")`. `FileIO` takes `&str`. `url::Url::parse` only appears in the optional `iceberg-storage-opendal` crate's `relativize_path`, and only to extract `host_str()` and the scheme prefix — never to round-trip the path.

iceberg-rust **avoids the entire bug class by never round-tripping through `Url`**. They get away with it as a *client* because they trust whoever produced the metadata file. As a server-side catalog handling untrusted input, we cannot.

Worth borrowing: the `MetadataLocation` typed-wrapper pattern (`FromStr`/`Display`, separate metadata-file naming from base location). The single normalisation rule (`trim_end_matches('/')`).

## Revised solution options (post-research)

The earlier Options 1-3 still apply. Research adds nuance:

### Polaris-style — custom raw-string URL parser, no `url::Url` round-trip

This is essentially **Option 3-lite**: keep `Location` as a wrapper (no enum refactor), but **replace the `url::Url::parse` validation step** with a Polaris-`StorageUri`-style hand-rolled parser. The parser stores the raw input verbatim, never decodes, never normalises, and exposes structural fields (scheme, authority, rawPath). `?` and `#` are kept literal in the path; we no longer have to reject them at the boundary.

Why this is the most attractive single change:
- Removes the "two parser views disagree" failure mode at the source.
- Polaris validated this approach across the same backends and migration path. Their PR history shows iterative reasons (column names with special chars, dot-segments in metadata paths, etc.) we'll likely hit too.
- Smaller than Option 3 (typed enum) — no migration, no API surface change.
- Fully compatible with the boundary defenses we've added (whitespace-only segment rejection, decoded `.`/`..`/`/` rejection in ADLS) — those keep working.

What we'd lose:
- The free RFC-3986 syntax validation that `url::Url::parse` provides. We'd reimplement what we need (scheme/authority/path well-formedness) — minimal.
- Auto-rejection of literal whitespace, control chars, etc. that `url::Url` does. We'd have to add explicit rejections (we already do this for several cases).

### object_store-style — distinguished `parse` + `encode_from_raw`, per-backend egress encoders

A bigger refactor than Polaris-style, smaller than the typed enum. Adds two ideas worth keeping even if we don't go full enum:

1. **Two clearly-named constructors.** `Location::parse(url) -> Result<Self, _>` for "this string is already a URL, validate". A separate `Location::from_decoded_parts(scheme, authority, decoded_path) -> Self` for "I have the parts, build the URL". Avoid `From<&str>` entirely — that's the object_store footgun.
2. **Per-backend egress encoders, defined adjacent to the signer.** A `s3_sigv4_canonical_path(&Location) -> String`, `azure_blob_path(&Location) -> String`, `gcs_object_name(&Location) -> String`, `s3_arn_resource(&Location) -> String`. Each function's name documents its encode set.

These two patterns can be added *on top of* the Polaris-style parser without conflict.

## Critique findings (against the recommendation above)

A separate critique pass against our codebase found **concrete blockers** for the original Phase-1-first ordering. The Polaris approach works for *Polaris* because their Java SDK doesn't re-route the path through a parser that interprets `?`. **Our Rust stack does.** Specifically:

1. **Azure SDK `Url::join` truncates `?`.**
   [`azure_storage_datalake-0.21.0/src/clients/file_client.rs:18`](https://github.com/Azure/azure-sdk-for-rust/blob/legacy/sdk/storage_datalake/src/clients/file_client.rs):
   ```rust
   self.file_system_client.url()?.join(&file_path)?
   ```
   `file_path` is what we hand the SDK from [`AdlsLocation::blob_name()`](crates/io/src/adls/adls_location.rs). `Url::join` of a string containing literal `?` will treat everything after as the request query string. The blob path is silently truncated; the SDK writes/reads the wrong object — the *exact* bug class the doc warns about for `..`/`.`. This blocks lifting the `?` rejection without first adding an `azure_blob_request_path` egress encoder that re-encodes `?` to `%3F` for SDK calls.
2. **S3 signer parses inbound request URLs with `url::Url`.**
   [`crates/lakekeeper/src/server/s3_signer/sign.rs:282-287`](crates/lakekeeper/src/server/s3_signer/sign.rs#L282-L287). A client requests `https://bucket.../foo?bar/baz`; `url::Url::parse` interprets `?bar/baz` as the query string; `path_segments()` returns only `["foo"]`. Stored `Location` and inbound URL get compared via `is_sublocation_of` and don't match — legitimate signing fails. Conversely if we somehow stored the truncated form, an unrelated key authorises. Either way bad. Lifting the `?` rejection requires rewriting the inbound parser too.
3. **DB-stored locations mixing eras.** Stored `Location` strings persist across deployments. Lifting the rejection lets new rows have literal `?`; old `url::Url`-based callers see truncated forms. Mixed-state migration concern.

The critique also flags:

- **Per-backend egress granularity is too coarse.** S3 alone needs at least 4 distinct encoders: `s3_iam_resource_arn`, `s3_iam_listbucket_prefix`, `s3_sigv4_canonical_path`, `s3_object_request_url`. They share a Location input but produce different forms (IAM glob escape ≠ SigV4 unreserved ≠ JSON-safe prefix). Phase 2 should be **per-feature**, not per-backend.
- **`Location` equality is a security boundary.** `S3Location::is_sublocation_of` ([s3_signer/sign.rs:498](crates/lakekeeper/src/server/s3_signer/sign.rs#L498)) gates request authorisation. Polaris's pure-`startsWith` model would let `s3://b/foo%2Dbar/x` escape an authorisation scoped to `s3://b/foo-bar/`. Polaris callers paper over this by canonicalising upstream; we don't. Adopting Polaris-style requires explicit canonicalisation rules baked into `Location` equality, or every caller has to canonicalise (error-prone).
- **Missing option (Option 5 below).** Strictest variant of the typed-enum approach.

### What's safe vs unsafe to lift today

| Code path | Status with raw `?`/`#` |
|---|---|
| GCS CEL `startsWith('...')` | ✅ Already safe — CEL string literal containing `?`/`#` matches the decoded resource.name |
| ADLS SAS canonical | ✅ Already safe — `az.rs` percent-decodes before signing |
| S3 IAM `Resource` ARN / `s3:prefix` condition | ✅ Already safe — `escape_iam_glob_literal` handles `?` |
| Azure SDK file write/read (`Url::join`) | ❌ Truncates at `?` — silent path divergence |
| S3 signer `is_sublocation_of` (inbound `url::Url::parse`) | ❌ Path truncated, comparison misfires |
| `Location` equality / `is_sublocation_of` as security boundary | ❌ Needs canonicalisation rules |

### Option 5: decoded-bytes-only (no internal URL string)

Strictest typed variant. Internally store `(WarehouseId, decoded_path: Vec<u8>)` and a backend tag. The URL string is reconstructed *only* at API/SDK egress, by the per-feature encoder. No `as_str()`/`Display` returning a URL form — callers that need a string for an SDK call use `to_sdk_path(&Backend)` and pick the right encoder. Iceberg metadata files come in as URL strings — parsing happens at one place on read, serialisation at one place on write. Eliminates the `format!("{location}/...")` footgun (currently common in `adls_storage.rs`, `gcs_storage.rs`, `tables.rs:1739`). Adoption cost: high, boundary is mechanically enforceable.

## Recommendation (after critique)

The original "adopt Polaris-style first" recommendation is **wrong order**. Polaris's approach is incompatible with two Rust-stack realities (Azure SDK `Url::join`, S3 signer inbound `url::Url::parse`) until those are also rewritten.

Revised plan:

**Phase 1 — keep what we have.** Don't lift the `?`/`#` rejection yet. The boundary validators (literal `?`/`#`, ADLS whitespace-only/`.`/`..`/`/` decoded segments, `reduce_scheme_string` returning `Result`) are load-bearing given the SDK realities. They stay.

**Phase 2 — per-feature egress encoders.** Build the encoder set as a *prerequisite*, not a follow-up:
- `s3_iam_resource_arn`, `s3_iam_listbucket_prefix`, `s3_sigv4_canonical_path`, `s3_object_request_url`
- `azure_sas_canonical_path` (already implicitly in `az.rs::sas`), `azure_blob_request_path`
- `gcs_cel_quoted` (already implicitly in `gcs/sts.rs`), `gcs_object_request_path`

Each takes `&Location` and returns a `String` matching the consumer's required form. Constants and `AsciiSet`s defined adjacent to the signer (per object_store's pattern — encoding rules live next to their consumers, not in the type).

This phase makes encoding explicit at every egress, which is the *substantive* fix for the bug class — independent of whether we ever change `Location`'s internal form.

**Phase 3 — rewrite S3 signer inbound URL parsing**. Use the Polaris-style hand-rolled parser for inbound request URLs in `s3_signer/sign.rs`, not `url::Url`. Removes blocker (2). Independent of `Location` itself.

**Phase 4 — only after Phases 2 & 3 land.** Reconsider Polaris-style on `Location::from_str`: drop `url::Url::parse` validation, allow `?`/`#` literal, replace with our own parser. Boundaries are now tight enough that the Phase-1 failure modes don't apply.

**Phase 5 (deferred / optional).** Typed enum (Option 3) or decoded-bytes-only (Option 5). Only if the encoder discipline from Phase 2 starts to feel insufficient. Polaris hasn't needed it; iceberg-rust hasn't either; `object_store` deliberately stays one-string-with-encoders. Default to staying with that pattern.

**`Location` equality:** if the egress encoders are correct, `Location` equality stops being a security primitive — `is_sublocation_of` should compare *encoded* forms (the form the SDK and the cloud see). That's a cleaner contract than "decoded forms must canonicalise". Tracked separately.

## `Location` equality and DB uniqueness — current state

`Location` equality is used by `is_sublocation_of` (gates request authorisation in the S3 signer) and by an app-level conflict check at create-time. The combination is *more thorough* than a plain Postgres `UNIQUE` constraint would be:

- The schema already splits `location` into `(fs_protocol, fs_location)` ([migration `20250216105917`](../crates/lakekeeper/migrations/20250216105917_variant_protocols.sql)) — explicitly to support family-equivalent protocols (`s3`/`s3a`/`s3n`).
- [`create_tabular`](../crates/lakekeeper/src/implementations/postgres/tabular/mod.rs#L513-L534) runs a post-insert check that detects (a) any existing tabular whose `fs_location` matches the new tabular's path or any of its parents, and (b) any existing tabular whose `fs_location` is *under* the new tabular's path. Returns `LocationAlreadyTaken` and rolls back.

This catches the bypass classes that look most concerning at first glance:

- **Scheme alias** (`s3://b/foo` vs `s3a://b/foo`): both produce `fs_location = "b/foo"` → exact match → conflict.
- **Trailing slash** (`b/foo` vs `b/foo/`): caught by the `length(...) < length(fs_location) AND TRIM(TRAILING '/') || '/' LIKE $4 || '/%'` clause.
- **Parent/child overlap** (`b/foo` vs `b/foo/bar`): partial-locations array catches it.

### What the app check does NOT catch — and where the cloud DOES collapse them

I previously claimed cloud-side byte-equality saves us for the cases the app check misses. **That was partly wrong.** Empirical investigation found multiple classes where the cloud collapses two distinct stored forms to the same physical resource — and at least one is a real ADLS SAS bypass.

#### Confirmed bypass classes

1. **Unreserved-char encoding** (`-` ↔ `%2D`, `_` ↔ `%5F`, `~` ↔ `%7E`, also alphanumerics like `A` ↔ `%41`). RFC 3986 §6.2.2.2 says these forms are URI-equivalent. Two tables with these distinct stored fs_locations:
   - **ADLS:** [`az.rs:489-507`](../crates/lakekeeper/src/service/storage/az.rs#L489-L507) percent-decodes the path before signing the SAS canonical. Both tables produce *identical* canonical resource strings. T2's vended SAS authorises requests against T1's path. **Real cred-sharing bypass.**
   - **S3:** AWS server decodes the URL and stores at the decoded key. Both tables write/read the *same physical S3 object*. Per-table IAM policy patterns are byte-literal, so each table's vended creds match only its own pattern form — but a holder of either set of creds can construct a request URL using its own form and reach the shared physical object. **Asymmetric bypass.**
   - **GCS:** Similar shape — GCS server decodes the URL; same physical object. CEL `startsWith` is byte-literal on the policy text → encoded-form table's creds reach nothing usable, but T1's creds reach the same physical object.
2. **Mixed-case percent triplets** (`%2D` ↔ `%2d`). RFC 3986 §6.2.2.1: hex digits in percent-encoding are case-insensitive at the URI level. Same collapse class as #1.
3. **Encoded sub-delims** on S3/GCS — the unreserved-char story extends to RFC 3986 sub-delims (`!`, `$`, `&`, `'`, `(`, `)`, `*`, `+`, `,`, `;`, `=`, `:`, `@`). The server decodes; same physical object. Same bypass shape as #1 on S3 and GCS.
4. **Trailing dot in segment on Azure Blob endpoint** (`foo.` vs `foo`): server-side strip on the Blob endpoint (Azure SDK Java issue [#36674](https://github.com/Azure/azure-sdk-for-java/issues/36674)). DFS endpoint may differ — unverified. Same blob via Blob endpoint.

#### Sites that produce the bypass

| File:line | Transformation | What collapses |
|-----------|---------------|----------------|
| [`crates/lakekeeper/src/service/storage/az.rs:489-507`](../crates/lakekeeper/src/service/storage/az.rs#L489-L507) | `percent_decode_str` before SAS canonical | Class 1, 2 (unreserved + mixed case) on ADLS |
| [`crates/lakekeeper/src/server/s3_signer/sign.rs:114, 384-405, 492-528`](../crates/lakekeeper/src/server/s3_signer/sign.rs#L384-L528) | `urldecode_uri_path_segments` decodes before `is_sublocation_of`, then signs the wire-encoded form | Class 1, 2, 3 — request URL with encoded form decodes to literal, matches T1 (literal); signs T2's URL; AWS server decodes again on its side — vended creds for one work for the other's data |
| [`crates/lakekeeper/src/implementations/postgres/tabular/mod.rs:447-534`](../crates/lakekeeper/src/implementations/postgres/tabular/mod.rs#L447-L534) | Byte-exact `=` and `LIKE $4 \|\| '/%'` on raw `fs_location` | Doesn't catch any of classes 1-4 — the upstream source of duplicate rows |

#### Other findings worth noting

- **Migration lossiness:** [`migrations/20250216105917_variant_protocols.sql:8-10`](../crates/lakekeeper/migrations/20250216105917_variant_protocols.sql#L8-L10) uses `split_part(location, '://', 2)`. If a pre-migration row's location contained `://` inside the path (legal in S3 keys), the migration silently truncates `fs_location` and produces a different value than the original — possibly aliasing two pre-existing rows after migration. Should have used "first `://` only" semantics.
- **Iceberg metadata `location` field** ([`tables.rs:1585`](../crates/lakekeeper/src/server/tables.rs#L1585)): `commit_tables` byte-equality-compares the new metadata's `location()` against the previous. A writer (Spark/Trino) could rewrite the metadata file with an equivalence-class variant of the location and trigger an "immutable location" change — or worse, alternate writes between two equivalent forms.
- **WHATWG vs RFC parsing of `\` and tabs/CR/LF** in URLs: low confidence; affects `s3a`/`abfss` clients that go through Java URL layers. Worth probing if a deployment uses Hadoop FS.
- **Microsoft cautions**: trailing whitespace in segments is "suspicious until proven different"; not specifically documented as stripped, but Azure naming docs warn against. Multiple slashes preserved server-side per Azure SDK [#3277](https://github.com/Azure/azure-sdk-for-rust/issues/3277). Path segments are case-sensitive (azcopy [#1226](https://github.com/Azure/azure-storage-azcopy/issues/1226)). Unicode normalisation: not applied; byte-stored.

### Bottom line (revised)

The app-level conflict check + cloud byte-equality is **not** sufficient. There is a real ADLS SAS bypass via classes 1-2 (`-` ↔ `%2D`, `%2d` ↔ `%2D`) and an asymmetric S3/GCS bypass via class 3 (encoded sub-delims).

The fix is canonicalisation of `fs_location` at insert time — and it's now **security-critical**, not polish. Specifically:
- Decode percent-encoded unreserved chars + sub-delims at parse time.
- Uppercase or lowercase percent-encoding hex consistently (or just normalise).
- Reject percent-encoded forms that decode to chars that re-encode differently (the test for "is this URL byte-equal to its own canonical form?").
- Apply to all schemes, not just ADLS.

This fold-in to `Location::from_str` so every downstream consumer (app conflict check, IAM/SAS/CEL builders, signer) operates on the same canonical string. The ADLS-only segment validator we added today is a special case of this general rule and would be subsumed.

Until that's done, the current encoding behaviour produces real cred-scope leaks under the right table-pair construction.

### Equivalence rules per backend

A canonical-form function such that two URLs that refer to the same cloud-side resource canonicalise to the same string:

| Component | Rule |
|-----------|------|
| Scheme | Lowercase. Family collapse: `s3`/`s3a`/`s3n` → `s3`; `abfss`/`wasbs` → `abfss`. (We continue accepting `wasbs://` inputs for back-compat.) |
| Host (bucket / `account.suffix` / etc.) | Lowercase. |
| Path | RFC-3986 normalise: percent-decode unreserved chars (`-`, `.`, `_`, `~`); reject `.`/`..`/empty segments; reserved chars stay percent-encoded (so the canonical is still a valid URL string). |
| Trailing slash | Per-surface (see survey below). Tables: trim. Namespace + warehouse-root: keep one. |

Two locations are equal iff their canonical forms are byte-equal.

### Trailing-slash convention (survey)

The codebase already follows a consistent convention; it just isn't formalised:

- **Table / `metadata_location`** — no trailing slash.
- **Namespace location** — single trailing slash.
- **Warehouse storage profile base** — single trailing slash.

Worth typing these as distinct: e.g. `TableLocation`, `NamespaceLocation`, `WarehouseRoot`. Different invariants, different equality rules, different unique constraints.

### Schema is already partly in place

[Migration `20250216105917_variant_protocols`](../crates/lakekeeper/migrations/20250216105917_variant_protocols.sql) explicitly split `location` into:

- `tabular.fs_protocol` (TEXT NOT NULL) — user's chosen scheme (`s3`, `s3a`, `s3n`, `abfss`, `wasbs`, …). Preserves the user's input verbatim.
- `tabular.fs_location` (TEXT NOT NULL) — everything after `://`.

The migration comment is explicit: *"to support multiple protocols for the same filesystem, such as `s3a` and `s3`"*.

[Migration `20250904142650_reusable_table_id`](../crates/lakekeeper/migrations/20250904142650_reusable_table_id.sql) (line 328-329) creates a **non-unique index** on `(warehouse_id, fs_location)`. The infrastructure for family-agnostic uniqueness is already there; it's just not enforced.

This narrows the work considerably: **the protocol prefix is already preserved per-row. We only need to canonicalise `fs_location` on insert and turn the index into a UNIQUE constraint.** Future protocol additions (Wasbs alternatives, `s3n`, R2 schemes, …) just plug into `fs_protocol` without changing the canonicalisation rules.

### Recommended approach: try Option A first, fall back to hybrid only on test breakage

We have broad integration-test coverage, including the two test envs that exercise the scheme-aliasing concern directly:

- `spark_minio_s3a` — uses `s3a://` scheme end-to-end.
- `spark_wasbs` — uses `wasbs://` scheme end-to-end.

If those (and `spark_adls`, `spark_minio_sts`, `spark_gcs`, `pyiceberg`, `trino`) pass with **pure Option A** — canonicalise-on-input, store + return canonical only — Option A is sufficient. The theoretical Spark-Hadoop-FileSystem-dispatch concern is testable, not just speculative.

**Plan:**

1. Implement `Location::canonicalize()` (idempotent) per the rules above.
2. Make `Location::from_str` always produce the canonical form. Existing callers get it for free.
3. Run the full integration-test matrix (`pyiceberg`, all `spark_*`, `trino`).
4. **If they pass:** ship Option A. `loadTable` returns the canonical form; clients adapt (most don't care since their Hadoop config covers all schemes in a family).
5. **If `spark_minio_s3a` or `spark_wasbs` fails** with a driver-dispatch error: switch to hybrid (Postgres `location_canonical` column with the unique constraint; `loadTable` returns the raw form; internals key off canonical).

Hybrid sketch (only relevant if needed):

1. Postgres has both `location` (raw) and `location_canonical` (normalised key).
2. `UNIQUE(location_canonical)` — closes the bypass.
3. `loadTable` returns the original `location`. Spark sees what it sent.
4. Internal authorisation primitives (`is_sublocation_of`, IAM Resource ARNs, SAS canonical, GCS access boundary) all derive from the canonical form.
5. `Location::from_str` returns a struct with both views; callers pick.

Polaris does this hybrid implicitly (`isChildOf` normalises equivalences while the stored string is verbatim) — but they don't get the Postgres-level uniqueness guarantee. That's where Lakekeeper has more to gain regardless of which variant we ship.

### Migration note (important)

If we change canonicalisation rules later, the canonical form of existing rows may shift. **The migration must not trip "table location change is not allowed" guards** elsewhere in the codebase. Specifically:

- `update_table` / `commit_table` flows reject changes to `location` after creation (immutable field). A migration that rewrites the canonical column must bypass this check, or the check must be re-keyed off `location_canonical` such that a recanonicalisation that produces the same canonical form is a no-op.
- The same applies to warehouse-storage-profile updates which use `UpdateError::ImmutableField("filesystem"/"key_prefix"/etc.)` checks today.
- Migration plan: read each row, recanonicalise, **detect collisions** (two rows now produce the same canonical) — alert the operator with both rows for manual resolution; do not silently merge or drop. If no collision, write the new canonical form.
- A pre-flight test against production DB dumps should be part of the rollout.

### Implementation sketch (when we tackle this)

1. Define `Location::canonicalize() -> Self` (idempotent).
2. Make `Location::from_str` produce `(raw, canonical)` — expose both.
3. Add `location_canonical` columns + `UNIQUE` constraints; migrate existing rows.
4. Switch `is_sublocation_of` and per-backend `*Location::*` accessors used in IAM/SAS/CEL to operate on the canonical form.
5. Re-audit the immutable-field guards listed above.

This belongs higher in the priority queue than the encoder phases (Phase 2 in the recommendation above) because it's an active authorisation bypass, not a defence-in-depth tightening.

## Implementation notes for the canonicalisation work

### Canonicalisation rule (precise)

For each path segment in `fs_location`:

- **Decode** percent-encodings whose decoded byte is in *unreserved* (`A-Za-z0-9-._~`) or *sub-delims* (`!$&'()*+,;=:@`). These are URI-equivalent per RFC 3986 §6.2.2.2; keeping them encoded is purely cosmetic and creates the bypass.
- **Keep encoded** percent-encodings whose decoded byte is reserved (`?`, `#`, `/`, `[`, `]`, `%`). These need encoding for the URL to be valid; decoding them changes URL semantics.
- **Normalise hex case** of all surviving `%XX` triplets to uppercase (`%2d` → `%2D`). RFC 3986 §6.2.2.1.
- **Reject** percent-encodings whose decoded byte is a control char or whitespace (existing ADLS validator generalised to all backends).

The "is this already canonical?" predicate: `Location::from_str(s).as_str() == s`. Useful for migration audits and tests.

### Caveat: `%3F` must stay encoded

A naive "decode everything" rule would turn legitimate `%3F` (legal way to embed `?` in an object name) into literal `?`, which `Location::from_str` rejects. The decoded form would then be unrepresentable. Hence the unreserved+sub-delim restriction above.

### Where to place the canonicalisation

`Location::from_str` is the right point — every downstream consumer (`fs_location` storage, the app conflict check, IAM/SAS/CEL builders, the S3 signer's `is_sublocation_of`) gets the canonical form for free, and the existing ADLS-only segment validator becomes a special case (decoding-then-checking is the general pattern; whitespace-only / `.`/`..`/`/` rejection are the ADLS-specific extras still needed).

### Validation test (the one we discussed but didn't write)

For each backend, parametrised over a few unreserved/sub-delim chars:
1. Create T1 at `<ns>/literal-X-here/data/`.
2. Create T2 at `<ns>/literal-%XX-here/data/` (encoded form of same char).
3. **Pre-canonicalisation:** both creations succeed; T2's vended creds authorise writes against T1's stored path on ADLS (assertion fails → empirical bypass demo).
4. **Post-canonicalisation:** T2's createTable should fail with `LocationAlreadyTaken` (or equivalent).

The same test shape but with `%2d` ↔ `%2D` mixed-hex variants. And `foo.` ↔ `foo` for ADLS Blob endpoint (separate test, ADLS-only).

### Migration safety

`fs_location` is in production data. Recanonicalising live rows can:
- Produce collisions (two existing rows that map to the same canonical) — these are the latent bypasses we need to report to operators, not silently merge.
- Trip the immutable-`location` guards in `update_table` / `commit_table` flows. Migration must bypass those, or those guards must be re-keyed off canonical so that rewriting the canonical column with the same canonical value is a no-op.

Pre-flight: a read-only script that scans `tabular`, applies the canonicalisation function, and reports duplicate canonical groups. Run against a production DB dump before any schema migration.

### Order of operations

1. Implement `Location::canonicalize()` per the rule above.
2. Wire it into `Location::from_str`.
3. Run the existing integration matrix (`spark_minio_sts`, `spark_minio_s3a`, `spark_wasbs`, `spark_adls`, `spark_gcs`, `pyiceberg`, `trino`). The two scheme-alias envs (`s3a`, `wasbs`) are the compatibility canaries — see "Recommended approach" above.
4. Add the validation test from the section above and confirm bypass closure.
5. Build the migration pre-flight + collision report.
6. Schema migration to recanonicalise existing data.

## Open questions

- For Option 3: where does the URL parser live? `crates/io` (per-backend
  parsers) or a single `Location::parse(url) -> Result<TypedLocation>` in
  the REST layer? The latter is cleaner; the former matches the current
  module structure.
- Iceberg metadata-location strings inside `*.metadata.json` — they're
  written by writers (Spark/etc.), not Lakekeeper. We *read* them when
  loading tables. Does the typed enum need to round-trip those strings
  bit-exact, or just round-trip semantically?
- For multi-cloud federation: a "ForeignS3" backend pointing at non-AWS
  cloud-storage with custom domain. How does it differ from `S3Compat`
  for Location purposes? Does it need its own variant?
- Are there any places in Lakekeeper that today rely on the silent
  fallback of two-views-of-one-string? `find -F .unwrap_or(path)` style
  patterns. Worth a sweep before committing to Option 3.
