//! ADLS Gen2 `LakekeeperStorage` impl over `object_store::azure::MicrosoftAzure`.
//!
//! See the parent `adls.rs` module header for the overall file layering.
//! This file holds:
//! * [`AdlsStorage`] — the public storage backend.
//! * [`AdlsClientConfig`] — immutable builder template shared across requests.
//! * The per-container `MicrosoftAzure` cache (an `object_store` quirk; see
//!   the comment on [`AdlsStorage`] for why it exists).
//! * Pure helpers ([`decide_put_strategy`], [`next_page`]) extracted from
//!   the trait methods so the boundary semantics are unit-testable without
//!   driving the HTTP layer.
use std::{
    collections::HashMap,
    str::FromStr,
    sync::{Arc, RwLock},
};

use bytes::Bytes;
use futures::StreamExt as _;
use object_store::{
    GetOptions, GetRange, ObjectMeta, ObjectStore as _, ObjectStoreExt as _, PutPayload,
    azure::MicrosoftAzureBuilder, path::Path as ObjectStorePath,
};

use crate::{
    DeleteBatchError, DeleteError, ErrorKind, FileInfo, IOError, InitializeClientError,
    InvalidLocationError, LakekeeperFileWrite, LakekeeperStorage, Location, ReadError, WriteError,
    adls::{
        AdlsLocation, AzureCloud,
        adls_error::parse_error,
        adls_writer::AdlsFileWrite,
        credentials::{ResolvedCredential, try_cached_store},
    },
    delete_not_found_is_ok, execute_with_parallelism, validate_file_size,
};

/// Read chunk size for parallel chunked downloads. Matches the previous
/// `DataLakeClient` configuration; tuning this is orthogonal to the SDK swap.
const DEFAULT_BYTES_PER_REQUEST: usize = 4 * 1024 * 1024;
/// Above this size, `read` and `read_range` switch from one-shot GET to
/// parallel chunked GET with an eTag integrity check on each chunk.
const MAX_BYTES_PER_REQUEST: usize = 7 * 1024 * 1024;
/// Parallelism for chunked reads and per-file batch operations. Matches the
/// previous backend.
const READ_PARALLELISM: usize = 10;
/// Above this size, `write` switches from a single-shot `object_store::put`
/// to a multipart upload. The single-PUT path is otherwise bounded by
/// `object_store`'s 180s retry budget — large bodies on slow connections
/// (e.g., CI runners with ~5 MB/s upstream) can time out before completing.
/// 16 mebibytes is the empirical sweet spot: small enough to avoid the
/// timeout window, large enough that small files (the common case) skip
/// the multipart overhead.
const SINGLE_PUT_THRESHOLD: usize = 16 * 1024 * 1024;
/// Part size for the bulk-write multipart-promotion path. Same value as
/// `AdlsFileWrite::PART_SIZE` for consistency.
const WRITE_PART_SIZE: usize = 4 * 1024 * 1024;

/// Builder template — used to construct a `MicrosoftAzure` per container.
/// Held inside `AdlsStorage` and read by `store_for`.
#[derive(Debug, Clone)]
pub(crate) struct AdlsClientConfig {
    pub(crate) account_name: String,
    pub(crate) authority_host: Option<url::Url>,
    pub(crate) cloud: AzureCloud,
    pub(crate) credential: ResolvedCredential,
}

/// ADLS Gen2 storage backend backed by `object_store::azure::MicrosoftAzure`.
///
/// `object_store::MicrosoftAzure` is configured per-container (the container
/// name is fixed at build time), but `LakekeeperStorage` operations can
/// target any container under the same account in principle. We cache one
/// `MicrosoftAzure` per container so token caches (inside `object_store`'s
/// internal `TokenCredentialProvider`) are shared across calls — building a
/// fresh `MicrosoftAzure` per request would refetch OAuth tokens on every
/// call.
///
/// **Credential lifetime contract.** The credential resolved at construction
/// time (held in [`AdlsClientConfig`]) is baked into every cached
/// `MicrosoftAzure`. The cache is keyed on container name only, so to use a
/// different credential — including rotating a SAS token — construct a fresh
/// `AdlsStorage`. There is no API to swap the credential on an existing
/// instance. This matches the legacy `azure_storage_datalake` backend, where
/// `DataLakeClient` likewise carried its credential immutably.
//
// The cache (`stores`) is an `object_store` quirk, not a domain concept —
// S3/GCS backends don't need it because their clients are account-scoped.
// If `object_store::MicrosoftAzure` ever lifts the per-container binding,
// drop the field and call `MicrosoftAzureBuilder::build()` inline.
#[derive(Debug, Clone)]
pub struct AdlsStorage {
    config: Arc<AdlsClientConfig>,
    stores: Arc<RwLock<HashMap<String, Arc<object_store::azure::MicrosoftAzure>>>>,
}

impl AdlsStorage {
    #[must_use]
    pub(crate) fn new(config: Arc<AdlsClientConfig>) -> Self {
        Self {
            config,
            stores: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Test-only hook: pre-populate the per-container store cache with a
    /// caller-built `MicrosoftAzure`. Used to wire the storage backend to an
    /// in-process `wiremock` HTTP server (which `cloud.endpoint_url()` cannot
    /// produce because it hardcodes `https://`).
    #[cfg(test)]
    pub(crate) fn insert_store_for_test(
        &self,
        container: &str,
        store: Arc<object_store::azure::MicrosoftAzure>,
    ) {
        self.stores
            .write()
            .expect("ADLS store cache lock should not be poisoned")
            .insert(container.to_string(), store);
    }

    /// Delete a single blob via `object_store`. Applies `delete_not_found_is_ok`
    /// to classify a missing blob as success and returns the classified `IOError`
    /// for the caller to wrap into the appropriate error type.
    //
    // SAS-token redaction in error output is provided by `object_store` itself:
    // SAS-credentialed Azure requests are marked `sensitive_request() == true`,
    // which causes `RetryError::Display` to write "REDACTED" instead of the
    // URI, and `HttpError` strips the URL from any reqwest source error via
    // `e.without_url()`. No custom redaction wrapper is needed at this layer.
    pub(crate) async fn delete_blob(
        &self,
        location: &AdlsLocation,
        key: &ObjectStorePath,
    ) -> Result<(), IOError> {
        let error_path = location.to_string();
        let store = self.store_for_path(location)?;
        let result = store
            .delete(key)
            .await
            .map_err(|e| parse_error(e, &error_path));
        delete_not_found_is_ok(result)
    }

    /// Convenience wrapper that flattens [`InitializeClientError`] into an
    /// [`IOError`] tagged with the URL-form location. Used by every operation
    /// entry point; the typed `InitializeClientError` is only useful at the
    /// API boundary (where the `From<IOError>` impls on the outer error enums
    /// take over), so threading it through each method is pure boilerplate.
    ///
    /// Also enforces account-name binding: rejects locations whose
    /// `account_name` does not match `self.config.account_name`. The upstream
    /// `StorageProfile::is_allowed_location` check is the primary boundary;
    /// this is defense-in-depth so a future bug there cannot cause
    /// `AdlsStorage` to dispatch the configured credential against an
    /// attacker-supplied account name. Every operation routes through here
    /// (the one exception — `list()` — calls [`Self::check_account`]
    /// explicitly).
    fn store_for_path(
        &self,
        location: &AdlsLocation,
    ) -> Result<Arc<object_store::azure::MicrosoftAzure>, IOError> {
        self.check_account(location).map_err(|e| {
            let reason = e.reason.clone();
            IOError::new(ErrorKind::ConditionNotMatch, reason, location.to_string()).set_source(e)
        })?;
        self.store_for(location.filesystem()).map_err(|e| {
            let reason = e.reason.clone();
            IOError::new(ErrorKind::Unexpected, reason, location.to_string()).set_source(e)
        })
    }

    /// Reject a location whose account-name does not match the configured
    /// account. See [`Self::store_for_path`] for the rationale; this is the
    /// raw helper for the one call site (`list()`) that builds its
    /// `MicrosoftAzure` without going through `store_for_path`.
    fn check_account(&self, location: &AdlsLocation) -> Result<(), InvalidLocationError> {
        if location.account_name() != self.config.account_name {
            return Err(InvalidLocationError::new(
                location.to_string(),
                format!(
                    "Location account `{}` does not match configured storage account `{}`",
                    location.account_name(),
                    self.config.account_name
                ),
            ));
        }
        Ok(())
    }

    /// Return the (cached or freshly-built) `MicrosoftAzure` for the given
    /// container.
    ///
    /// Two caches sit in front of `MicrosoftAzureBuilder::build()`:
    ///
    /// 1. For system-identity credentials (managed identity, workload
    ///    identity, AAD client-secret SP, Azure CLI), the **process-wide**
    ///    [`crate::adls::credentials::try_cached_store`] cache is consulted
    ///    first. It survives across `AdlsStorage` instances so the inner
    ///    `TokenCredentialProvider`'s warm AAD/IMDS token is shared across
    ///    every catalog request hitting the same (account, container,
    ///    identity).
    /// 2. For static credentials (`SharedAccessKey`, `Sas`, raw
    ///    `ClientCredentials` with secret), the legacy **per-instance** DCL
    ///    cache below applies. Static credentials make no token round-trip
    ///    at build time, so the cross-instance benefit is small and we
    ///    intentionally do not put secret-bearing keys in a process-wide
    ///    `HashMap`.
    ///
    /// Synchronous because nothing inside awaits — `std::sync::RwLock` is
    /// the right primitive (cheaper than `tokio::sync::RwLock`, no fairness
    /// queue, no risk of holding across an await).
    ///
    /// Per-instance concurrency: double-checked locking. The read lock is
    /// the common-case fast path. The second `get` *after* acquiring the
    /// write lock prevents the race where two threads both miss under the
    /// read lock, both queue for the write lock, and both build a fresh
    /// `MicrosoftAzure`. Defended by
    /// `store_for_concurrent_gets_share_one_build`. The process-wide cache
    /// has its own concurrency dedup via `moka::sync::Cache::try_get_with`.
    pub(crate) fn store_for(
        &self,
        container: &str,
    ) -> Result<Arc<object_store::azure::MicrosoftAzure>, InitializeClientError> {
        if let Some(store) = try_cached_store(
            &self.config.credential,
            &self.config.account_name,
            container,
            self.config.authority_host.as_ref(),
            &self.config.cloud,
            || {
                build_microsoft_azure(&self.config, container).map_err(|e| InitializeClientError {
                    reason: format!("Failed to build object_store MicrosoftAzure: {e}"),
                    source: Some(Box::new(e)),
                })
            },
        )? {
            return Ok(store);
        }
        {
            let read = self
                .stores
                .read()
                .expect("ADLS store cache lock should not be poisoned");
            if let Some(store) = read.get(container) {
                return Ok(store.clone());
            }
        }
        let mut write = self
            .stores
            .write()
            .expect("ADLS store cache lock should not be poisoned");
        // Second check — see the DCLP note on the function doc.
        if let Some(store) = write.get(container) {
            return Ok(store.clone());
        }
        let store =
            build_microsoft_azure(&self.config, container).map_err(|e| InitializeClientError {
                reason: format!("Failed to build object_store MicrosoftAzure: {e}"),
                source: Some(Box::new(e)),
            })?;
        let arc = Arc::new(store);
        write.insert(container.to_string(), arc.clone());
        Ok(arc)
    }
}

/// Build a fresh `MicrosoftAzure` for a (account, container) pair from this
/// storage's resolved credentials.
fn build_microsoft_azure(
    config: &AdlsClientConfig,
    container: &str,
) -> Result<object_store::azure::MicrosoftAzure, object_store::Error> {
    let mut builder = MicrosoftAzureBuilder::new()
        .with_account(config.account_name.clone())
        .with_container_name(container.to_string());

    if let Some(endpoint) = config.cloud.endpoint_url() {
        builder = builder.with_endpoint(endpoint);
    }
    if let Some(authority) = &config.authority_host {
        builder = builder.with_authority_host(authority.as_str());
    }

    builder = config.credential.apply(builder);
    builder.build()
}

/// Convert `AdlsLocation` to the `object_store::Path` `MicrosoftAzure`
/// expects: container-relative, no leading slash.
///
/// Lakekeeper's `Location` preserves the original URL path string verbatim
/// — no normalisation, no percent-decoding. That string IS the byte-literal
/// blob name under Lakekeeper's storage-key model: two paths that differ
/// only by percent-encoding of an unreserved character (`Abc` vs `%41bc`)
/// must address distinct blobs. See `test_percent_encoding_does_not_alias`.
///
/// We pass that string straight to `Path::parse`. Unlike `Path::from` /
/// `Path::from_iter`, `Path::parse` does *no* percent-encoding — segments
/// are stored verbatim, including any literal `%` characters. The wire-URL
/// builder inside `object_store` then percent-encodes the segments exactly
/// once via `url::Url::path_segments_mut().push(...)`, so:
/// * `ä-file.txt` → wire `%C3%A4-file.txt` → Azure stores UTF-8 `ä-file.txt`
/// * `%41bc`     → wire `%2541bc`         → Azure stores literal `%41bc`
/// * `Abc`       → wire `Abc`             → Azure stores `Abc`
///
/// `%41bc` and `Abc` end up as distinct blobs on Azure — exactly the
/// invariant the alias-test pins. `AdlsLocation` validates its segments
/// (no `.`, `..`, `%2F`, whitespace-only) before we reach this function, so
/// `Path::parse` is expected to succeed; we still surface the parse error
/// as an `IOError` rather than panic, because `AdlsLocation`'s validator
/// and `Path::parse`'s validator are maintained independently and a future
/// drift would otherwise become a process-aborting panic.
fn to_object_store_path(adls_location: &AdlsLocation) -> Result<ObjectStorePath, IOError> {
    let path = adls_location.location().path().unwrap_or_default();
    ObjectStorePath::parse(path).map_err(|e| {
        IOError::new(
            ErrorKind::Unexpected,
            format!("Failed to parse ADLS path as object_store::Path: {e}"),
            adls_location.to_string(),
        )
    })
}

// `remove_all` is not implemented here — we deliberately fall through to
// the default `LakekeeperStorage::remove_all` (list-then-batch-delete).
//
// The legacy `azure_storage_datalake` backend issued a single DFS
// `DELETE?recursive=true` against the directory endpoint: atomic from the
// client's POV, O(1) HTTP calls, and HNS-aware (it removed the directory
// marker too). `object_store::MicrosoftAzure` doesn't expose that endpoint,
// so the default implementation enumerates and deletes per-blob.
//
// Trade-offs vs. the legacy backend:
//   * Cost / latency: O(N) HTTP DELETEs + the initial list — measurable on
//     deep prefixes on HNS accounts.
//   * Atomicity: a concurrent writer can land new files into the prefix
//     between the list and the deletes, leaving the "removed" directory
//     non-empty. The legacy single-DELETE was atomic from this angle.
//
// TODO(adls-remove-all): mandatory before Lakekeeper ships any benchmarked
// prefix-delete workload (deep HNS hierarchies in particular). Action:
// measure the regression on a real HNS account; if material, implement a
// dedicated raw HTTP `DELETE?recursive=true` against the ADLS DFS endpoint.
// Until then we accept the degradation.
#[async_trait::async_trait]
impl LakekeeperStorage for AdlsStorage {
    async fn delete(&self, path: &str) -> Result<(), DeleteError> {
        let adls_location = AdlsLocation::try_from_str(path, true)?;
        require_key(&adls_location)?;
        let key = to_object_store_path(&adls_location).map_err(DeleteError::IOError)?;
        self.delete_blob(&adls_location, &key)
            .await
            .map_err(DeleteError::IOError)
    }

    async fn delete_batch(&self, paths: &[String]) -> Result<(), DeleteBatchError> {
        // Fan out at parallelism 100 across all containers (matches the
        // legacy `azure_storage_datalake` backend). Each per-blob future
        // dispatches via `delete_blob`, which internally picks the SAS
        // raw-DELETE path or the `object_store` path based on the credential.
        //
        // Parse + validate all paths up front so a single bad path fails the
        // whole batch before any DELETE fires (matches the prior behaviour).
        let mut flat: Vec<AdlsLocation> = Vec::with_capacity(paths.len());
        for p in paths {
            let adls_location = AdlsLocation::try_from_str(p.as_str(), true)?;
            require_key(&adls_location)?;
            flat.push(adls_location);
        }

        let delete_futures = flat.into_iter().map(|adls_location| {
            // `execute_with_parallelism` spawns onto tokio, so the future
            // must be `'static`. Clone the (cheap, two-Arc) `AdlsStorage`
            // into the future rather than borrowing `&self`.
            let this = self.clone();
            async move {
                let key = to_object_store_path(&adls_location)?;
                this.delete_blob(&adls_location, &key).await
            }
        });

        let delete_stream = execute_with_parallelism(delete_futures, 100);
        tokio::pin!(delete_stream);
        while let Some(item) = delete_stream.next().await {
            match item {
                Ok(Ok(())) => {}
                Ok(Err(io_err)) => {
                    return Err(DeleteBatchError::IOError(io_err));
                }
                Err(join_err) => {
                    return Err(DeleteBatchError::IOError(IOError::new(
                        ErrorKind::Unexpected,
                        format!("Task join error during batch delete: {join_err}"),
                        "batch_operation".to_string(),
                    )));
                }
            }
        }
        Ok(())
    }

    async fn write(&self, path: &str, bytes: Bytes) -> Result<(), WriteError> {
        let adls_location = AdlsLocation::try_from_str(path, true)?;
        require_key(&adls_location)?;
        let store = self.store_for_path(&adls_location)?;
        let key = to_object_store_path(&adls_location).map_err(WriteError::IOError)?;

        match decide_put_strategy(bytes.len()) {
            PutStrategy::Single => store
                .put(&key, PutPayload::from_bytes(bytes))
                .await
                .map(|_| ())
                .map_err(|e| {
                    WriteError::IOError(parse_error(e, path).with_context("ADLS bulk write"))
                }),
            PutStrategy::Multipart => {
                multipart_upload_bytes(store.as_ref(), &key, bytes, path).await
            }
        }
    }

    async fn writer(&self, path: &str) -> Result<Box<dyn LakekeeperFileWrite>, WriteError> {
        let adls_location = AdlsLocation::try_from_str(path, true)?;
        require_key(&adls_location)?;
        let store = self.store_for_path(&adls_location)?;
        let key = to_object_store_path(&adls_location).map_err(WriteError::IOError)?;

        let upload = store.put_multipart(&key).await.map_err(|e| {
            WriteError::IOError(parse_error(e, path).with_context("ADLS open writer"))
        })?;

        Ok(Box::new(AdlsFileWrite::new(
            store,
            key,
            path.to_string(),
            upload,
        )))
    }

    async fn read_single(&self, path: &str) -> Result<Bytes, ReadError> {
        let adls_location = AdlsLocation::try_from_str(path, true)?;
        require_key(&adls_location)?;
        let store = self.store_for_path(&adls_location)?;
        let key = to_object_store_path(&adls_location).map_err(ReadError::IOError)?;

        let result = store
            .get(&key)
            .await
            .map_err(|e| ReadError::IOError(parse_error(e, path)))?;
        result
            .bytes()
            .await
            .map_err(|e| ReadError::IOError(parse_error(e, path).with_context("ADLS read_single")))
    }

    async fn metadata(&self, path: &str) -> Result<FileInfo, ReadError> {
        let adls_location = AdlsLocation::try_from_str(path, true)?;
        require_key(&adls_location)?;
        let store = self.store_for_path(&adls_location)?;
        let key = to_object_store_path(&adls_location).map_err(ReadError::IOError)?;

        let meta = store
            .head(&key)
            .await
            .map_err(|e| ReadError::IOError(parse_error(e, path).with_context("ADLS metadata")))?;
        Ok(object_meta_to_file_info(
            adls_location.location().clone(),
            &meta,
        ))
    }

    async fn read(&self, path: &str) -> Result<Bytes, ReadError> {
        let adls_location = AdlsLocation::try_from_str(path, true)?;
        require_key(&adls_location)?;
        let store = self.store_for_path(&adls_location)?;
        let key = to_object_store_path(&adls_location).map_err(ReadError::IOError)?;

        // HEAD first so we have the eTag to use as the integrity check on
        // each parallel chunk. Below the threshold, skip parallel and do a
        // single GET.
        let head = store
            .head(&key)
            .await
            .map_err(|e| ReadError::IOError(parse_error(e, path).with_context("ADLS read HEAD")))?;
        let file_size = validate_file_size(
            i64::try_from(head.size).map_err(|_| {
                IOError::new(
                    ErrorKind::Unexpected,
                    "File size from HEAD exceeds i64::MAX",
                    path.to_string(),
                )
            })?,
            path,
        )?;
        if file_size == 0 {
            return Ok(Bytes::new());
        }
        if file_size < MAX_BYTES_PER_REQUEST {
            // `usize as u64` is lossless on every Rust target (no platform
            // has `usize` wider than 64 bits). Plain cast avoids the noise of
            // a `try_from(...).expect(...)` for an infallible conversion.
            return fetch_range(store.as_ref(), &key, 0..file_size as u64, None, path).await;
        }
        let etag = require_etag_for_chunked_read(head.e_tag.as_deref(), file_size, path)?;
        parallel_chunked_read(store.clone(), key, path, 0, file_size, Some(etag)).await
    }

    async fn read_range(
        &self,
        path: &str,
        range: std::ops::Range<u64>,
    ) -> Result<Bytes, ReadError> {
        let adls_location = AdlsLocation::try_from_str(path, true)?;
        require_key(&adls_location)?;
        if range.end < range.start {
            return Err(ReadError::IOError(IOError::new(
                ErrorKind::ConditionNotMatch,
                format!(
                    "Invalid range: start ({}) > end ({})",
                    range.start, range.end
                ),
                path.to_string(),
            )));
        }
        if range.is_empty() {
            return Ok(Bytes::new());
        }

        let range_size_u64 = range.end - range.start;
        let range_size = usize::try_from(range_size_u64).map_err(|_| {
            ReadError::IOError(IOError::new(
                ErrorKind::ConditionNotMatch,
                format!("Range size {range_size_u64} too large for this platform"),
                path.to_string(),
            ))
        })?;

        let store = self.store_for_path(&adls_location)?;
        let key = to_object_store_path(&adls_location).map_err(ReadError::IOError)?;

        if range_size <= MAX_BYTES_PER_REQUEST {
            return fetch_range(store.as_ref(), &key, range, None, path).await;
        }
        let head = store.head(&key).await.map_err(|e| {
            ReadError::IOError(parse_error(e, path).with_context("ADLS read_range HEAD"))
        })?;
        // Same rationale as `read()`: the chunked path requires an ETag so
        // the per-chunk integrity check can fire and we refuse to stitch a
        // torn read silently.
        let etag = require_etag_for_chunked_read(head.e_tag.as_deref(), range_size, path)?;
        parallel_chunked_read(
            store.clone(),
            key,
            path,
            range.start,
            range_size,
            Some(etag),
        )
        .await
    }

    async fn list(
        &self,
        path: &str,
        page_size: Option<usize>,
    ) -> Result<futures::stream::BoxStream<'_, Result<Vec<FileInfo>, IOError>>, InvalidLocationError>
    {
        // Note: listing a non-existent prefix yields an empty stream, not an
        // error. `object_store::list` returns `NotFound` mid-stream in this
        // case; `next_page` swallows it as clean end-of-stream. Callers that
        // need to distinguish "empty prefix" from "missing prefix" must HEAD
        // the prefix-marker blob separately.
        //
        // Trailing slash mirrors what the previous backend did so list("foo")
        // and list("foo/") behave the same. `object_store` already filters
        // HNS pseudo-directories from the flat list, so we don't need to.
        let path_with_slash = format!("{}/", path.trim_end_matches('/'));
        let adls_location = AdlsLocation::try_from_str(&path_with_slash, true)
            .map_err(|e| e.with_context("List Operation failed"))?;
        // `list()` builds its `MicrosoftAzure` via `store_for` directly (it
        // returns `InvalidLocationError`, not `IOError`, so it doesn't go
        // through `store_for_path`). The account-name binding check therefore
        // has to fire here explicitly — keeping `list()` consistent with
        // every other operation that routes through `store_for_path`.
        self.check_account(&adls_location)
            .map_err(|e| e.with_context("List Operation failed"))?;
        let base_location = adls_location.location().clone();
        let store = self
            .store_for(adls_location.filesystem())
            .map_err(|e| InvalidLocationError::new(path.to_string(), e.reason))?;
        let prefix = to_object_store_path(&adls_location)
            .map_err(|e| InvalidLocationError::new(path.to_string(), e.to_string()))?;
        let error_path = path_with_slash.clone();

        // `object_store::list` returns a flat `BoxStream<'static>` of
        // `ObjectMeta`. We rebatch into pages of `page_size` so the existing
        // test assertions (which expect bounded page sizes when one is
        // requested) still hold. `page_size = None` → single batch of all.
        let page_size = page_size.unwrap_or(usize::MAX).max(1);
        let item_stream = store.list(Some(&prefix));

        let paged = futures::stream::try_unfold((item_stream, false), move |(mut items, done)| {
            let base_location = base_location.clone();
            let error_path = error_path.clone();
            async move {
                if done {
                    return Ok::<_, IOError>(None);
                }
                match next_page(&mut items, &base_location, &error_path, page_size).await? {
                    PageBatch::Yield { items: page, more } => Ok(Some((page, (items, !more)))),
                    PageBatch::Done => Ok(None),
                }
            }
        });

        Ok(paged.boxed())
    }
}

/// Outcome of draining the next batch from the list-stream in [`next_page`].
#[derive(Debug)]
enum PageBatch {
    /// At least one item was collected. `more` is `true` if `page_size` was
    /// reached and the underlying stream may produce another page; `false`
    /// when the stream ended (or hit `NotFound`) mid-page.
    Yield { items: Vec<FileInfo>, more: bool },
    /// The stream is exhausted (or returned `NotFound` before yielding any
    /// item) with no items to flush. Terminate the page-stream.
    Done,
}

/// Drain up to `page_size` items from `items`, converting each `ObjectMeta`
/// into a `FileInfo` via [`object_meta_to_full_location`]. Returns either a
/// non-empty page (with a flag indicating whether more pages might follow),
/// or `Done` when the stream is exhausted without producing any item.
///
/// Extracted from `list()`'s `try_unfold` so the pagination semantics
/// (`NotFound` mid-stream tolerance, empty-page suppression, `page_size`
/// boundary) are unit-testable against a synthetic `futures::stream::iter`
/// without driving any HTTP layer.
async fn next_page<S>(
    items: &mut S,
    base_location: &Location,
    error_path: &str,
    page_size: usize,
) -> Result<PageBatch, IOError>
where
    S: futures::Stream<Item = Result<ObjectMeta, object_store::Error>> + Unpin + Send,
{
    let mut buf: Vec<FileInfo> = Vec::new();
    while let Some(item) = items.next().await {
        match item {
            Ok(meta) => {
                if let Some(fi) = object_meta_to_full_location(base_location, &meta) {
                    buf.push(fi);
                }
                if buf.len() >= page_size {
                    return Ok(PageBatch::Yield {
                        items: buf,
                        more: true,
                    });
                }
            }
            // Listing a non-existent prefix surfaces as NotFound. Treat it
            // as a clean end-of-stream — flush whatever was buffered.
            Err(object_store::Error::NotFound { .. }) => {
                if buf.is_empty() {
                    return Ok(PageBatch::Done);
                }
                return Ok(PageBatch::Yield {
                    items: buf,
                    more: false,
                });
            }
            Err(e) => {
                return Err(parse_error(e, error_path).with_context("Failed to list ADLS path"));
            }
        }
    }
    if buf.is_empty() {
        Ok(PageBatch::Done)
    } else {
        Ok(PageBatch::Yield {
            items: buf,
            more: false,
        })
    }
}

/// Which upload path `AdlsStorage::write` should take for a given payload size.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum PutStrategy {
    /// One-shot `PUT` — small payloads.
    Single,
    /// Multipart upload — payloads above [`SINGLE_PUT_THRESHOLD`], where the
    /// single-PUT round-trip would otherwise risk timing out under
    /// `object_store`'s 180s retry budget on slow connections.
    Multipart,
}

/// Decide the upload strategy for `AdlsStorage::write` based on payload size.
/// Extracted so the boundary at [`SINGLE_PUT_THRESHOLD`] is unit-testable
/// without driving the full network path.
fn decide_put_strategy(bytes_len: usize) -> PutStrategy {
    if bytes_len <= SINGLE_PUT_THRESHOLD {
        PutStrategy::Single
    } else {
        PutStrategy::Multipart
    }
}

fn require_key(adls_location: &AdlsLocation) -> Result<(), InvalidLocationError> {
    if adls_location.blob_name().is_empty() || adls_location.blob_name() == "/" {
        return Err(InvalidLocationError::new(
            adls_location.to_string(),
            "Operation requires a path inside the ADLS Filesystem".to_string(),
        ));
    }
    Ok(())
}

fn object_meta_to_file_info(adls_location: Location, meta: &ObjectMeta) -> FileInfo {
    FileInfo::new(Some(meta.last_modified), adls_location, Some(meta.size))
}

/// Reconstruct a full ADLS URL from the (already-validated) base location
/// and a `Path` reported by `list`. The base location encodes scheme,
/// account, container, and the optional list-prefix; the meta path is the
/// container-relative full path of the listed object.
fn object_meta_to_full_location(base: &Location, meta: &ObjectMeta) -> Option<FileInfo> {
    // `base` was built by `try_from_str(format!("{path}/"))` so it has a
    // trailing slash on its path. `meta.location` is the container-relative
    // path. We want a full URL whose path equals the meta path.
    let full = format!(
        "{}://{}/{}",
        base.scheme(),
        base.authority_with_host(),
        meta.location.as_ref()
    );
    let location = Location::from_str(&full).ok()?;
    Some(FileInfo::new(
        Some(meta.last_modified),
        location,
        Some(meta.size),
    ))
}

/// Gate for the chunked-read path: returns the `ETag`, or refuses with a
/// classified [`IOError`] if the `HEAD` response carried none. Without an
/// `ETag` we can't enforce the `If-Match` integrity check on each chunk,
/// and a single-shot `GET` into memory is not a safer fallback for files
/// that reached this branch (definitionally > 7 mebibytes).
///
/// Extracted from `read()`/`read_range()` so the missing-ETag branch can be
/// asserted as a pure unit test. End-to-end coverage of the chunked path
/// (including the 412-on-ETag-mismatch surface) runs through `wiremock` by
/// pre-populating the `AdlsStorage` per-container cache with a
/// `MicrosoftAzure` built directly against the mock server.
fn require_etag_for_chunked_read<'a>(
    etag: Option<&'a str>,
    size_for_diagnostic: usize,
    error_path: &str,
) -> Result<&'a str, ReadError> {
    if let Some(etag) = etag {
        return Ok(etag);
    }
    tracing::warn!(
        path = %error_path,
        size_for_diagnostic,
        threshold = MAX_BYTES_PER_REQUEST,
        "ADLS HEAD returned no ETag for a chunked-read-sized payload; \
         refusing chunked read without an integrity check.",
    );
    Err(ReadError::IOError(IOError::new(
        ErrorKind::Unexpected,
        format!(
            "ADLS HEAD returned no ETag; cannot run chunked read with \
             integrity check for size={size_for_diagnostic}"
        ),
        error_path.to_string(),
    )))
}

/// Stream `bytes` to the target via `object_store::put_multipart` in
/// `WRITE_PART_SIZE` chunks. Used by `write()` when the body exceeds
/// `SINGLE_PUT_THRESHOLD`. On any part failure, drop the multipart upload
/// (Azure no-ops `abort` server-side; uncommitted blocks expire after 7
/// days) and propagate the original error — matches the `AdlsFileWrite`
/// abort-on-error contract.
async fn multipart_upload_bytes(
    store: &object_store::azure::MicrosoftAzure,
    key: &ObjectStorePath,
    bytes: Bytes,
    error_path: &str,
) -> Result<(), WriteError> {
    use object_store::MultipartUpload;

    let mut upload = store.put_multipart(key).await.map_err(|e| {
        WriteError::IOError(parse_error(e, error_path).with_context("ADLS multipart open"))
    })?;

    // `chunk_ranges` is zero-copy: each `bytes.slice(range)` is an owned
    // refcounted view, not a memcpy. Parts are uploaded sequentially —
    // `MultipartUpload::put_part` itself takes `&mut self` and `object_store`
    // handles its own internal parallelism if configured.
    for (chunk_index, range) in crate::chunk_ranges(bytes.len(), WRITE_PART_SIZE) {
        let chunk = bytes.slice(range);
        if let Err(e) = upload.put_part(PutPayload::from_bytes(chunk)).await {
            // Drop the upload to release local buffers. We do NOT explicitly
            // delete the target: `put_multipart` never calls `complete()` on
            // failure, so there is no committed blob server-side, and Azure
            // garbage-collects uncommitted blocks after 7 days. The streaming
            // writer (`AdlsFileWrite`) is stricter (it issues a defensive
            // DELETE) because it may have called `complete()` before the
            // failure — see `abort_after_failure` in `adls_writer.rs`.
            drop(upload);
            return Err(WriteError::IOError(
                parse_error(e, error_path).with_context(format!(
                    "ADLS multipart put_part failed (chunk {chunk_index})"
                )),
            ));
        }
    }

    upload.complete().await.map(|_| ()).map_err(|e| {
        WriteError::IOError(parse_error(e, error_path).with_context("ADLS multipart complete"))
    })
}

/// Parameter order matches `crates/io/src/s3/s3_storage.rs::fetch_range`:
/// `(client/store, key/location, range, if_match)`, plus an `error_path` for
/// the URL-form path used to tag any error (S3 can derive it from
/// `&S3Location`; ADLS must thread it separately).
async fn fetch_range(
    store: &object_store::azure::MicrosoftAzure,
    key: &ObjectStorePath,
    range: std::ops::Range<u64>,
    if_match: Option<&str>,
    error_path: &str,
) -> Result<Bytes, ReadError> {
    let opts = GetOptions {
        if_match: if_match.map(std::string::ToString::to_string),
        range: Some(GetRange::Bounded(range)),
        ..Default::default()
    };
    let result = store.get_opts(key, opts).await.map_err(|e| {
        ReadError::IOError(
            parse_error(e, error_path).with_context("Failed to download object range."),
        )
    })?;
    result.bytes().await.map_err(|e| {
        ReadError::IOError(parse_error(e, error_path).with_context("Failed to drain object body."))
    })
}

/// Parallel chunked read with `If-Match` eTag integrity check on every chunk.
/// If the object is overwritten mid-read, Azure returns 412 on the next
/// chunk and we surface a `ConditionNotMatch`-classified error rather than
/// silently producing a torn read.
async fn parallel_chunked_read(
    store: Arc<object_store::azure::MicrosoftAzure>,
    key: ObjectStorePath,
    error_path: &str,
    range_start: u64,
    range_size: usize,
    etag: Option<&str>,
) -> Result<Bytes, ReadError> {
    if range_size == 0 {
        return Ok(Bytes::new());
    }
    let chunk_size = DEFAULT_BYTES_PER_REQUEST;
    let etag = etag.map(str::to_owned);
    let owned_path = error_path.to_owned();

    crate::parallel_chunked_read(
        range_size,
        chunk_size,
        READ_PARALLELISM,
        error_path,
        move |rel_start, rel_end, chunk_index| {
            let store = store.clone();
            let key = key.clone();
            let etag = etag.clone();
            let path = owned_path.clone();
            // `usize as u64` is lossless on every Rust target; see the
            // matching comment in `read()`.
            let abs_start = range_start + rel_start as u64;
            let abs_end = range_start + rel_end as u64 + 1;
            async move {
                let bytes = fetch_range(
                    store.as_ref(),
                    &key,
                    abs_start..abs_end,
                    etag.as_deref(),
                    &path,
                )
                .await
                .map_err(|e| match e {
                    ReadError::IOError(io) => ReadError::IOError(io.with_context(format!(
                        "Failed to download chunk {chunk_index} (bytes {abs_start}-{abs_end})"
                    ))),
                    other @ ReadError::InvalidLocation(_) => other,
                })?;
                Ok((chunk_index, bytes))
            }
        },
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- chunked-read `ETag` gate ----

    /// An absent `ETag` on a chunked-sized payload must return a
    /// `Unexpected`-classified [`IOError`] rather than silently degrading
    /// to a torn-read-prone parallel `GET`.
    #[test]
    fn require_etag_for_chunked_read_errors_on_missing_etag() {
        let result = require_etag_for_chunked_read(None, 8 * 1024 * 1024, "abfss://x/y/z");
        let err = result.expect_err("expected ETag absence to error");
        match err {
            ReadError::IOError(io) => assert_eq!(io.kind(), ErrorKind::Unexpected),
            other @ ReadError::InvalidLocation(_) => {
                panic!("expected ReadError::IOError, got {other:?}")
            }
        }
    }

    /// A present `ETag` must pass through verbatim — no copy, no
    /// transformation.
    #[test]
    fn require_etag_for_chunked_read_passes_etag_through() {
        let etag = require_etag_for_chunked_read(Some("\"v1\""), 8 * 1024 * 1024, "abfss://x/y/z")
            .expect("expected Ok when ETag present");
        assert_eq!(etag, "\"v1\"");
    }

    /// Construct an `AdlsStorage` with a fake (but builder-valid) access-key
    /// credential. `MicrosoftAzureBuilder::with_access_key` doesn't actually
    /// validate or use the key until a request fires, so `build()` succeeds
    /// and we can exercise the local cache machinery without any network.
    fn test_storage() -> AdlsStorage {
        AdlsStorage::new(Arc::new(AdlsClientConfig {
            account_name: "testacct".to_string(),
            authority_host: None,
            cloud: crate::adls::AzureCloud::Public,
            credential: ResolvedCredential::AccessKey("dGVzdC1rZXk=".to_string()),
        }))
    }

    /// Same container twice returns identical Arc — the cache hits.
    #[test]
    fn store_for_same_container_returns_identical_arc() {
        let storage = test_storage();
        let a = storage.store_for("c1").expect("build microsoft_azure");
        let b = storage.store_for("c1").expect("build microsoft_azure");
        assert!(
            Arc::ptr_eq(&a, &b),
            "cache miss on second call to store_for(c1)",
        );
    }

    /// Distinct containers produce distinct Arcs.
    #[test]
    fn store_for_distinct_containers_are_distinct() {
        let storage = test_storage();
        let a = storage.store_for("c1").expect("build microsoft_azure");
        let b = storage.store_for("c2").expect("build microsoft_azure");
        assert!(
            !Arc::ptr_eq(&a, &b),
            "store_for(c1) and store_for(c2) returned the same Arc — wrong cache key",
        );
    }

    /// Concurrent first-time gets for the same container must all
    /// return the same Arc (the double-checked-lock pattern is the only
    /// thing preventing parallel `MicrosoftAzure` builds).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn store_for_concurrent_gets_share_one_build() {
        let storage = test_storage();
        let mut handles = Vec::new();
        for _ in 0..8 {
            let s = storage.clone();
            handles.push(tokio::spawn(async move {
                s.store_for("c1").expect("build microsoft_azure")
            }));
        }
        let arcs: Vec<_> = futures::future::join_all(handles)
            .await
            .into_iter()
            .map(|r| r.expect("task panicked"))
            .collect();
        let first = &arcs[0];
        for (i, other) in arcs.iter().enumerate().skip(1) {
            assert!(
                Arc::ptr_eq(first, other),
                "concurrent store_for(c1) at index {i} returned a different Arc",
            );
        }
    }

    /// Account-name binding regression: a location whose account-name does
    /// not match the configured `account_name` must be rejected by
    /// `check_account`, both as the raw helper and via `store_for_path`. The
    /// upstream `StorageProfile::is_allowed_location` is the primary boundary
    /// here, but this is defense-in-depth and pinning it prevents a future
    /// refactor from silently re-introducing the cross-account confused
    /// deputy `AdlsStorage` used to have before the original guard existed.
    #[test]
    fn check_account_rejects_mismatched_account() {
        let storage = test_storage(); // configured account = "testacct"
        let foreign = AdlsLocation::try_from_str(
            "abfss://test-fs@otheracct.dfs.core.windows.net/key.txt",
            true,
        )
        .expect("parse");

        let err = storage
            .check_account(&foreign)
            .expect_err("expected account-name mismatch to be rejected");
        // Both account names must appear so the message is debuggable.
        assert!(
            err.reason.contains("otheracct") && err.reason.contains("testacct"),
            "error message should name both accounts, got: {}",
            err.reason
        );

        // store_for_path must reject the same location, classified as
        // ConditionNotMatch so callers don't treat it as a transient IO error.
        let err = storage
            .store_for_path(&foreign)
            .expect_err("store_for_path must propagate the account-name check");
        assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);
    }

    /// Positive case for the account-name guard: a location that matches the
    /// configured account passes through `check_account` unchanged.
    #[test]
    fn check_account_accepts_matching_account() {
        let storage = test_storage(); // configured account = "testacct"
        let matching = AdlsLocation::try_from_str(
            "abfss://test-fs@testacct.dfs.core.windows.net/key.txt",
            true,
        )
        .expect("parse");
        storage
            .check_account(&matching)
            .expect("matching account must pass");
    }

    /// Alignment regression: pin that every input `AdlsLocation` accepts
    /// is also accepted by `object_store::Path::parse`.
    ///
    /// The `Result` return type on `to_object_store_path` was kept as a
    /// defence-in-depth measure against future drift between the two
    /// validators. This test demonstrates that *today* no such drift exists
    /// on a representative corpus — including unicode, sub-delims, the
    /// boundary cases the integration tests already cover, and a deliberate
    /// double-slash. If `Path::parse` tightens (or `AdlsLocation` loosens)
    /// in a future dependency bump, this test catches the regression and we
    /// can decide whether to (a) tighten our validator to match or (b) keep
    /// the `Result` as a real error path.
    #[test]
    fn to_object_store_path_accepts_every_valid_adls_location() {
        let inputs = [
            "abfss://test-fs@acct.dfs.core.windows.net/key",
            "abfss://test-fs@acct.dfs.core.windows.net/dir/sub/key.txt",
            "abfss://test-fs@acct.dfs.core.windows.net/file-with-dashes.txt",
            "abfss://test-fs@acct.dfs.core.windows.net/file_with_underscores.txt",
            "abfss://test-fs@acct.dfs.core.windows.net/file.with.dots.txt",
            // Unicode (encoded by URL parsing)
            "abfss://test-fs@acct.dfs.core.windows.net/file-with-ue-%C3%BC.txt",
            // Sub-delims that AdlsLocation accepts
            "abfss://test-fs@acct.dfs.core.windows.net/y%20fl%20%21%20-_%C3%A4%20oats=1.2.txt",
            // Literal-percent in path (the alias-test invariant)
            "abfss://test-fs@acct.dfs.core.windows.net/%2541bc",
            // Long-ish deep path
            "abfss://test-fs@acct.dfs.core.windows.net/a/b/c/d/e/f/g/h.txt",
            // Single character key
            "abfss://test-fs@acct.dfs.core.windows.net/a",
        ];
        for input in inputs {
            let loc = AdlsLocation::try_from_str(input, true)
                .unwrap_or_else(|e| panic!("AdlsLocation rejected `{input}`: {e}"));
            to_object_store_path(&loc).unwrap_or_else(|e| {
                panic!(
                    "to_object_store_path rejected `{input}`: {e} \
                     — validator drift between AdlsLocation and \
                     object_store::path::Path detected. \
                     Decide whether to tighten AdlsLocation's validator \
                     or document the new error path."
                )
            });
        }
    }

    /// Build a `MicrosoftAzure` pointed at the given wiremock URI so the test
    /// can drive `AdlsStorage` end-to-end without TLS or real Azure. Uses an
    /// access-key credential because the wiremock matchers don't need to
    /// verify the signature.
    fn wiremock_backed_store(
        uri: &str,
        container: &str,
    ) -> Arc<object_store::azure::MicrosoftAzure> {
        let store = MicrosoftAzureBuilder::new()
            .with_endpoint(uri.to_string())
            .with_allow_http(true)
            .with_account("testacct")
            .with_container_name(container.to_string())
            .with_access_key("dGVzdC1rZXk=")
            .build()
            .expect("MicrosoftAzureBuilder::build against wiremock");
        Arc::new(store)
    }

    // ---- list pagination (`next_page`) ----

    fn meta(name: &str, size: u64) -> ObjectMeta {
        ObjectMeta {
            location: ObjectStorePath::parse(name).expect("path parse"),
            last_modified: chrono::Utc::now(),
            size,
            e_tag: None,
            version: None,
        }
    }

    fn base_loc() -> Location {
        Location::from_str("abfss://container@acct.dfs.core.windows.net/prefix/")
            .expect("parse base")
    }

    fn err_other() -> object_store::Error {
        object_store::Error::Generic {
            store: "test",
            source: "boom".into(),
        }
    }

    fn err_not_found() -> object_store::Error {
        object_store::Error::NotFound {
            path: "x".to_string(),
            source: "missing".into(),
        }
    }

    #[tokio::test]
    async fn next_page_empty_stream_returns_done() {
        let mut s = futures::stream::iter(Vec::<Result<ObjectMeta, object_store::Error>>::new());
        let out = next_page(&mut s, &base_loc(), "ctx", 10).await.unwrap();
        assert!(matches!(out, PageBatch::Done));
    }

    #[tokio::test]
    async fn next_page_yields_partial_when_stream_ends_below_page_size() {
        let mut s = futures::stream::iter(vec![Ok(meta("prefix/a", 1)), Ok(meta("prefix/b", 2))]);
        let out = next_page(&mut s, &base_loc(), "ctx", 10).await.unwrap();
        match out {
            PageBatch::Yield { items, more } => {
                assert_eq!(items.len(), 2);
                assert!(!more, "stream end => no more pages");
            }
            PageBatch::Done => panic!("expected Yield, got Done"),
        }
    }

    #[tokio::test]
    async fn next_page_yields_exactly_page_size_and_signals_more() {
        let mut s = futures::stream::iter((0..5).map(|i| Ok(meta(&format!("prefix/f{i}"), 1))));
        let out = next_page(&mut s, &base_loc(), "ctx", 3).await.unwrap();
        match out {
            PageBatch::Yield { items, more } => {
                assert_eq!(items.len(), 3);
                assert!(more, "boundary reached => more may follow");
            }
            PageBatch::Done => panic!("expected Yield, got Done"),
        }
    }

    #[tokio::test]
    async fn next_page_not_found_at_start_terminates_cleanly() {
        let mut s = futures::stream::iter(vec![Err(err_not_found())]);
        let out = next_page(&mut s, &base_loc(), "ctx", 10).await.unwrap();
        assert!(matches!(out, PageBatch::Done));
    }

    #[tokio::test]
    async fn next_page_not_found_mid_stream_flushes_buffer_and_signals_no_more() {
        let mut s = futures::stream::iter(vec![
            Ok(meta("prefix/a", 1)),
            Ok(meta("prefix/b", 2)),
            Err(err_not_found()),
        ]);
        let out = next_page(&mut s, &base_loc(), "ctx", 10).await.unwrap();
        match out {
            PageBatch::Yield { items, more } => {
                assert_eq!(items.len(), 2);
                assert!(!more, "NotFound terminates the stream cleanly");
            }
            PageBatch::Done => panic!("expected Yield, got Done"),
        }
    }

    #[tokio::test]
    async fn next_page_non_notfound_error_propagates() {
        let mut s = futures::stream::iter(vec![Ok(meta("prefix/a", 1)), Err(err_other())]);
        let err = next_page(&mut s, &base_loc(), "ctx", 10)
            .await
            .expect_err("Generic error should propagate");
        assert_eq!(err.kind(), ErrorKind::Unexpected);
    }

    // ---- put-strategy dispatch ----

    #[test]
    fn decide_put_strategy_at_and_below_threshold_is_single() {
        assert_eq!(decide_put_strategy(0), PutStrategy::Single);
        assert_eq!(decide_put_strategy(1), PutStrategy::Single);
        assert_eq!(
            decide_put_strategy(SINGLE_PUT_THRESHOLD),
            PutStrategy::Single
        );
    }

    #[test]
    fn decide_put_strategy_above_threshold_is_multipart() {
        assert_eq!(
            decide_put_strategy(SINGLE_PUT_THRESHOLD + 1),
            PutStrategy::Multipart
        );
        assert_eq!(
            decide_put_strategy(SINGLE_PUT_THRESHOLD * 4),
            PutStrategy::Multipart
        );
    }

    /// End-to-end coverage of the integrity guard: when a chunked-read GET
    /// returns 412 (`ETag` mismatch — blob overwritten mid-read), the error
    /// must surface as `ConditionNotMatch` rather than being silently
    /// reclassified or producing a torn read.
    #[tokio::test]
    async fn parallel_chunked_read_412_surfaces_condition_not_match() {
        use wiremock::{
            Mock, MockServer, ResponseTemplate,
            matchers::{method, path_regex},
        };

        let server = MockServer::start().await;
        // File size above MAX_BYTES_PER_REQUEST (7 MiB) so `read()` enters
        // the chunked-read branch. 16 MiB → 4 chunks of 4 MiB.
        let file_size: usize = 16 * 1024 * 1024;

        // HEAD: report the size + ETag so the chunked path's gate passes.
        Mock::given(method("HEAD"))
            .and(path_regex(r"/test-container/key"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-length", file_size.to_string().as_str())
                    .insert_header("etag", "\"v1\"")
                    .insert_header("last-modified", "Thu, 01 Jan 1970 00:00:00 GMT"),
            )
            .mount(&server)
            .await;

        // GET chunks: respond 412 (PreconditionFailed) to simulate the blob
        // being overwritten between HEAD and the chunk fetch.
        Mock::given(method("GET"))
            .and(path_regex(r"/test-container/key"))
            .respond_with(ResponseTemplate::new(412))
            .mount(&server)
            .await;

        let storage = test_storage();
        storage.insert_store_for_test(
            "test-container",
            wiremock_backed_store(&server.uri(), "test-container"),
        );

        let err = storage
            .read("abfss://test-container@testacct.dfs.core.windows.net/key")
            .await
            .expect_err("412 must surface as an error");
        match err {
            ReadError::IOError(io) => {
                assert_eq!(
                    io.kind(),
                    ErrorKind::ConditionNotMatch,
                    "412 should classify as ConditionNotMatch, got {:?} \
                     (full error: {io:?})",
                    io.kind(),
                );
                // Defence against a regression where a HEAD-parse failure (or
                // any other early error) gets the same ConditionNotMatch
                // classification: the error chain must mention the 412 we
                // actually returned. A mid-implementation header-format issue
                // (the date had to be a real day-of-week) was caught by this
                // exact concern, hence the extra assertion.
                let rendered = format!("{io:?}");
                assert!(
                    rendered.contains("412"),
                    "error should mention HTTP 412 in its source chain, \
                     got: {rendered}",
                );
            }
            other @ ReadError::InvalidLocation(_) => panic!("expected IOError, got {other:?}"),
        }
    }

    /// `AdlsStorage::delete` routes through the Azure Blob Batch endpoint
    /// (`POST {container}?restype=container&comp=batch`), not a per-blob
    /// `DELETE`. This is the contract enforced by `object_store`'s Azure
    /// backend — `MicrosoftAzure` only implements `delete_stream`, and the
    /// `ObjectStoreExt::delete` convenience method drives it as a one-element
    /// stream, which still flows through the batch endpoint.
    ///
    /// Pinning this matters because the routing has authorisation
    /// consequences: directory-scoped SAS tokens cannot authorise the
    /// container-level batch endpoint (`Signed Directory Depth Invalid`).
    /// The caveat is documented on `AzureSettings::get_storage_client` and
    /// drives the design of the vended-credentials validation flow in the
    /// upper `lakekeeper` crate.
    ///
    /// The assertion is `expect(1)` on the matcher count; the response body
    /// is intentionally not a valid multipart-batch payload, so the call
    /// itself returns an error. That is fine — this test pins routing, not
    /// response classification (which is exercised end-to-end by the
    /// integration tests against Azurite).
    #[tokio::test]
    async fn delete_routes_through_blob_batch_endpoint() {
        use wiremock::{
            Mock, MockServer, ResponseTemplate,
            matchers::{method, path_regex, query_param},
        };

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path_regex(r"/test-container"))
            .and(query_param("restype", "container"))
            .and(query_param("comp", "batch"))
            .respond_with(ResponseTemplate::new(202))
            .expect(1)
            .mount(&server)
            .await;

        let storage = test_storage();
        storage.insert_store_for_test(
            "test-container",
            wiremock_backed_store(&server.uri(), "test-container"),
        );

        // Body is intentionally empty (not valid multipart-batch). The
        // response will fail to parse and the call will return Err — that
        // does not affect this test, which only asserts the request hit
        // the batch endpoint exactly once via `expect(1)`.
        let _ = storage
            .delete("abfss://test-container@testacct.dfs.core.windows.net/key")
            .await;
        drop(server);
    }
}
