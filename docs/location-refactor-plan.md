# Location refactor — implementation plan

Companion to [location-design.md](./location-design.md). The design doc
explains the bug class, surveys options, and identifies the active
authorisation bypass (mixed-encoding cred sharing on ADLS, asymmetric
on S3/GCS). This doc is the concrete plan: what we ship, in what order,
with what tests.

## Decisions

- **Storage form: canonical-only.** `tabular.fs_location` holds the
  canonical form. No separate "raw" column. The Iceberg `metadata.json`
  `location` field — the wire form clients depend on — is owned by the
  writer (Spark/Trino) and round-tripped from the file; Lakekeeper does
  not synthesise it on read.
- **Scheme family: not collapsed.** `tabular.fs_protocol` keeps the
  user's literal scheme (`s3a`, `wasbs`, …). Uniqueness keys off
  `fs_location` only, which is already family-agnostic in the existing
  schema (migration `20250216105917_variant_protocols.sql`).
- **Trailing dot in path segments: rejected globally.** Azure Blob
  endpoint silently strips `foo.` to `foo` (Azure SDK Java issue
  [#36674](https://github.com/Azure/azure-sdk-for-java/issues/36674)).
  Reject in `Location::from_str` for all backends — one rule, one place
  to remember.
- **WHATWG smuggling chars: rejected upfront.** Reject any input
  containing tab / CR / LF / NUL / ASCII control / bidi-format chars in
  `Location::from_str` *before* `url::Url::parse` runs. Closes the
  WHATWG-vs-RFC discrepancy without rewriting the parser. The full
  hand-rolled RFC-3986 parser stays a follow-up; with the input
  restriction, `url::Url`'s silent-strip behaviour is moot because no
  valid input reaches it.
- **`Location` equality is canonical equality.** Two `Location`s are
  equal iff their canonical strings are byte-equal. `Hash` agrees.
- **`Location::extend`/`push` maintains canonical invariant.** Today
  they raw-concat. After this work they percent-encode the appended
  segment using the canonical encoding rule before joining. Otherwise
  internal `format!("{loc}/...")` patterns silently violate the
  invariant.

## Canonicalisation rule (precise, idempotent)

For `Location::from_str(s)`:

1. **Reject smuggling characters anywhere in input:** NUL, tab, CR, LF,
   any C0 control (`0x00-0x1F`, `0x7F`), bidi/format/zero-width
   (`U+200B-U+200F`, `U+202A-U+202E`, `U+2066-U+2069`, `U+FEFF`).
2. **Validate URL syntax via `url::Url::parse`.** With (1) in place,
   WHATWG/RFC discrepancies cannot be triggered.
3. **Reject** `?` (query) and `#` (fragment) at the URL level (existing
   rule).
4. **Lowercase scheme.** Keep family-distinct (`s3a` stays `s3a`).
5. **Lowercase host.** Reject trailing dot on host (`bucket.` form).
6. **Per path segment:**
   - Decode `%XX` whose decoded byte is unreserved (`A-Za-z0-9-._~`) or
     sub-delim (`!$&'()*+,;=:@`). RFC 3986 §6.2.2.2.
   - Keep `%XX` whose decoded byte is reserved (`?`, `#`, `/`, `[`,
     `]`, `%`).
   - Uppercase surviving `%XX` hex digits. RFC 3986 §6.2.2.1.
   - Reject if decoded byte is C0 control / `0x7F` / whitespace
     (generalises today's ADLS-only rule).
   - Reject decoded `.`, `..`, empty segment, `/` inside segment
     (existing ADLS rule, lifted to all backends).
   - **Reject trailing dot in any segment** (Azure Blob aliasing).
   - For multi-byte UTF-8: NFC-normalise non-ASCII strings, then
     percent-encode every non-ASCII byte. Net effect: stored path is
     ASCII, non-ASCII is NFC-stable.

**Idempotency:** every transform produces output already in its own
fixed-point set. The predicate `Location::from_str(s).as_str() == s`
holds for canonical input — useful for migration audits and tests.

**`%3F` stays encoded** (legitimate way to embed `?` in an object name);
do not decode reserved bytes.

## DB schema

No new columns. The existing `(fs_protocol, fs_location)` split from
migration `20250216105917_variant_protocols.sql` carries the model:

- `fs_protocol TEXT NOT NULL` — user's literal scheme.
- `fs_location TEXT NOT NULL` — canonical form. **Add `COLLATE "C"`**
  to defeat locale-sensitive UNIQUE behaviour.

Promote the existing non-unique index in
`migrations/20250904142650_reusable_table_id.sql:328-329` to
`UNIQUE INDEX (warehouse_id, fs_location) WHERE deleted_at IS NULL`.

The post-insert conflict check at
`crates/lakekeeper/src/implementations/postgres/tabular/mod.rs:513-534`
remains for parent/child-overlap detection (UNIQUE only catches exact
collisions).

## Per-feature egress encoders

Each takes `&{S3,Adls,Gcs}Location` and returns `String`. Defined
adjacent to the consumer (object_store discipline). Names document the
encode set and the consumer.

| Encoder | Replaces / lives next to | Form |
|---|---|---|
| `s3_iam_resource_arn` | `crates/lakekeeper/src/service/storage/s3.rs:965` | decoded path + `escape_iam_glob_literal` |
| `s3_iam_listbucket_prefix` | `crates/lakekeeper/src/service/storage/s3.rs:986` | decoded prefix + `escape_iam_glob_literal` |
| `s3_sigv4_canonical_path` | `crates/lakekeeper/src/server/s3_signer/sign.rs:282` | RFC 3986 unreserved (SigV4 `STRICT_PATH_ENCODE_SET`) |
| `azure_sas_canonical_path` | `crates/lakekeeper/src/service/storage/az.rs:489-507` | decoded form (formalises today's behaviour) |
| `azure_blob_request_path` | new — feeds Azure SDK `Url::join` | re-encodes `?` to `%3F` (closes SDK truncation bug) |
| `gcs_cel_quoted_resource_name` | `crates/lakekeeper/src/service/storage/gcs/sts.rs:138-165` | decoded form, then CEL-escape |

## Consumer reads

| Consumer | Reads | Why |
|---|---|---|
| DB insert / conflict check | canonical `fs_location` | Uniqueness guarantee |
| S3 IAM ARN / prefix / SigV4 | per-feature encoder | AWS decodes URLs server-side |
| S3 signer `is_sublocation_of` | canonical, after re-canonicalising inbound URL | Closes asymmetric bypass at `crates/lakekeeper/src/server/s3_signer/sign.rs:498` |
| ADLS SAS canonical | `azure_sas_canonical_path` | Azure server decodes |
| Azure SDK file write/read | `azure_blob_request_path` | `Url::join` truncates literal `?` |
| GCS CEL access boundary | `gcs_cel_quoted_resource_name` | GCP `resource.name` is decoded |
| `loadTable` response | `metadata.location()` from the file | The metadata.json string is the writer's bytes — not Lakekeeper's |
| `commit_table` immutability | canonical-vs-canonical | Otherwise a writer can flip encodings to bypass the check |
| Internal `format!("{loc}/...")` | canonical via `Display`; or migrate to `Location::extend` | Display returns canonical; `extend` enforces invariant on the appended segment |

## PR sequencing

Each PR is independently reviewable and revertable.

### PR 1 — Foundation

- `Location::canonicalize()` per the rule above. Idempotent. Rejects
  smuggling chars upfront.
- `Location::extend` / `push` percent-encodes appended segments to
  preserve canonical invariant.
- Map Postgres `unique_violation` (SQLSTATE 23505) on the new
  constraint name to `LocationAlreadyTaken` in
  `crates/lakekeeper/src/implementations/postgres/tabular/mod.rs:503-510`.
- Unit tests: canonicalisation rule per case (unreserved decode,
  sub-delim decode, mixed hex, reserved kept, trailing dot reject,
  whitespace reject, NFC, smuggling-char reject, idempotency,
  `extend`/`push` invariant).

No callers behave differently yet — for input that doesn't hit a bypass
shape, canonical equals the input.

### PR 2 — Per-feature egress encoders

- Extract the implicit decoders in `az.rs:489-507`, `s3.rs:965`,
  `gcs/sts.rs:138-139` into named functions per the encoder table
  above.
- Add `azure_blob_request_path` (re-encodes `?` for SDK `Url::join`).
- No behaviour change yet — naming and locality. Sets up the contract
  every consumer must satisfy.

### PR 3 — Pre-flight collision script + canonical-vs-canonical guards

- Read-only DB script: scan `tabular`, apply `Location::canonicalize`,
  report duplicate-canonical groups per warehouse with row context.
  Operators reconcile before PR 4.
- Rewire `crates/lakekeeper/src/server/tables.rs:1585`
  (`commit_table` immutability) and `update_table` `ImmutableField`
  checks to compare canonical forms.
- Re-key warehouse storage-profile guards
  (`crates/lakekeeper/src/service/storage/{s3,az,gcs}.rs`) so a
  re-canonicalisation that produces the same canonical is a no-op.

### PR 4 — Migration: enforcement

- In-place re-canonicalise `fs_location` for all rows.
- `ALTER TABLE tabular ALTER COLUMN fs_location TYPE TEXT COLLATE "C"`.
- `CREATE UNIQUE INDEX CONCURRENTLY tabular_warehouse_canonical_uq
  ON tabular (warehouse_id, fs_location) WHERE deleted_at IS NULL`,
  then drop the old non-unique index.
- Re-canonicalise inbound URLs in
  `crates/lakekeeper/src/server/s3_signer/sign.rs` before
  `is_sublocation_of` runs.
- Run integration matrix: `spark_minio_sts`, `spark_minio_s3a`,
  `spark_wasbs`, `spark_adls`, `spark_gcs`, `pyiceberg`, `trino`.
  `spark_minio_s3a` and `spark_wasbs` are the scheme-alias canaries.

### PR 5 — Phantom-typed wrappers

- `TableLocation` (no trailing slash), `NamespaceLocation` (one
  trailing slash), `WarehouseRoot` (one trailing slash). Zero runtime
  cost; codifies the convention from
  [location-design.md §"Trailing-slash convention"](./location-design.md#trailing-slash-convention-survey).

### PR 6 (deferred) — Hand-rolled RFC-3986 parser

- Replace `url::Url::parse` in `Location::from_str` and in
  `s3_signer/sign.rs:282` with a small RFC-3986 parser
  (~150 LOC). With PR 1's smuggling-char rejection in place this is
  defense-in-depth, not a security fix. Schedule when convenient.

## Validation tests

Per backend (S3 / ADLS / GCS), parametrised over a few
unreserved/sub-delim chars:

1. Create T1 at `<ns>/literal-X-here/data/`.
2. Create T2 at `<ns>/literal-%XX-here/data/` (encoded form of same
   char).
3. **Pre-PR-4:** both succeed; T2's vended creds authorise writes
   against T1's stored path on ADLS — empirical bypass demo.
4. **Post-PR-4:** T2's createTable fails with `LocationAlreadyTaken`.

Same shape for `%2d` ↔ `%2D` mixed-hex. ADLS-only test: `foo.` ↔
`foo` Blob endpoint trailing-dot.

Extension of `tests/python/tests/test_special_char_locations.py`.

## Migration safety

- Pre-flight (PR 3) is mandatory — collisions are the latent bypass
  victims; surface to operator, never silently merge.
- The immutable-field guards must be re-keyed on canonical *before*
  the re-canonicalisation migration runs (PR 3 ships before PR 4),
  otherwise the migration trips its own guards.
- Self-hosted operator runbook: dump `tabular`, run pre-flight, resolve
  duplicates, then upgrade.

## Out of scope

- WHATWG vs RFC parsing of `\` and tabs in `s3a`/`abfss` Java clients
  ([location-design.md L392](./location-design.md)). Different layer;
  track separately.
- Multi-cloud federation / `ForeignS3` variant (open question in
  design doc).
- Iceberg metadata-file path handling for snapshots and manifests —
  Lakekeeper does not own those bytes; clients write them.

## Residual risks after PR 4

- `%3F` stays encoded; literal `?` in input is rejected. Documented
  limitation.
- A maliciously-rewritten `metadata.json` `location` field by an
  adversarial engine flips between encoding-equivalent forms — caught
  by canonical-vs-canonical immutability check (PR 3).
- New backends (Wasabi, R2, …) inherit the canonical rule
  automatically; per-backend egress encoders may need additions but
  the canonicalisation contract is uniform.
