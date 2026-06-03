//! Streaming writer for ADLS Gen2 backed by `object_store::MultipartUpload`.
//!
//! Differences from the underlying [`object_store::WriteMultipart`] /
//! [`object_store::MultipartUpload`]:
//!
//! * On `Drop` of an active (un-closed) writer, we issue an out-of-band
//!   `DELETE` against the target path. `object_store::MultipartUpload::abort`
//!   on Azure is a no-op because Azure has no explicit abort API; uncommitted
//!   blocks expire after 7 days. The DELETE removes the (zero-length)
//!   committed blob if `complete` had already been called once, or the
//!   uncommitted-block-only blob otherwise.
//! * State machine preserves the four-state model from the previous backend
//!   (`Active` / `Closed` / `Aborted` / `AbortFailed`) for parity with
//!   existing test coverage of error semantics.
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use bytes::{Bytes, BytesMut};
use object_store::{
    MultipartUpload, ObjectStoreExt as _, PutPayload, path::Path as ObjectStorePath,
};

use crate::{ErrorKind, IOError, LakekeeperFileWrite, WriteError, adls::adls_error::parse_error};

/// Chunk size below which we buffer locally rather than emitting a part.
/// 4 mebibytes matches the previous backend; `object_store`'s
/// [`object_store::WriteMultipart`] uses 5 mebibytes by default but we
/// control buffering ourselves because the abort-on-Drop semantics need
/// access to the still-uploading parts.
const PART_SIZE: usize = 4 * 1024 * 1024;
/// Upper bound on best-effort cleanup work spawned from `Drop`.
const DROP_CANCEL_DURATION: Duration = Duration::from_secs(10);

/// `LakekeeperFileWrite` is declared `Send + Sync`; `dyn MultipartUpload` is
/// only `Send`. We wrap in a `std::sync::Mutex` so the field is `Sync`
/// without paying any locking cost — every access goes through `&mut self`,
/// so [`Mutex::get_mut`] (synchronous, no lock taken) is sufficient.
//
// TODO(adls-writer): switch to `std::sync::Exclusive<T>` once it stabilises
// (currently nightly-only, tracking issue rust-lang/rust#98407). That type
// is the soundness-equivalent of this `Mutex` for our usage pattern — same
// `Sync`-from-`Send` guarantee via `&mut`-only access — and would remove
// the poison-handling branches in `put_part` / `close`.
pub(crate) struct AdlsFileWrite {
    store: Arc<object_store::azure::MicrosoftAzure>,
    key: ObjectStorePath,
    /// Original input path (URL form) for error diagnostics.
    error_path: String,
    upload: Mutex<Option<Box<dyn MultipartUpload>>>,
    buffer: BytesMut,
    state: AdlsWriterState,
}

#[derive(Debug)]
enum AdlsWriterState {
    Active,
    Closed,
    Aborted,
    AbortFailed,
}

impl std::fmt::Debug for AdlsFileWrite {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdlsFileWrite")
            .field("key", &self.key)
            .field("buffered_bytes", &self.buffer.len())
            .field("state", &self.state)
            .finish_non_exhaustive()
    }
}

impl AdlsFileWrite {
    pub(crate) fn new(
        store: Arc<object_store::azure::MicrosoftAzure>,
        key: ObjectStorePath,
        error_path: String,
        upload: Box<dyn MultipartUpload>,
    ) -> Self {
        Self {
            store,
            key,
            error_path,
            upload: Mutex::new(Some(upload)),
            buffer: BytesMut::new(),
            state: AdlsWriterState::Active,
        }
    }
}

#[async_trait::async_trait]
impl LakekeeperFileWrite for AdlsFileWrite {
    async fn write(&mut self, bytes_in: Bytes) -> Result<(), WriteError> {
        let msg = match self.state {
            AdlsWriterState::Closed => Some("Cannot write to closed writer"),
            AdlsWriterState::Aborted => Some("Cannot write to aborted writer"),
            AdlsWriterState::AbortFailed => Some("Cannot write to writer that failed to abort"),
            AdlsWriterState::Active => None,
        };
        if let Some(msg) = msg {
            return Err(write_err(
                &self.error_path,
                ErrorKind::ConditionNotMatch,
                msg,
            ));
        }
        self.buffer.extend_from_slice(&bytes_in);
        while self.buffer.len() >= PART_SIZE {
            let chunk = self.buffer.split_to(PART_SIZE).freeze();
            if let Err(err) = put_part(&mut self.upload, &self.error_path, chunk).await {
                abort_after_failure(self).await;
                return Err(err);
            }
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<(), WriteError> {
        // Reject double-close / close-after-abort *before* mutating state, so
        // callers observe "errored, nothing changed" rather than "errored, but
        // state silently flipped". Only after we've committed to running the
        // close sequence do we transition `Active → Closed`.
        if !matches!(self.state, AdlsWriterState::Active) {
            return Err(write_err(
                &self.error_path,
                ErrorKind::ConditionNotMatch,
                "Writer already closed or aborted",
            ));
        }
        self.state = AdlsWriterState::Closed;
        if !self.buffer.is_empty() {
            let chunk = self.buffer.split().freeze();
            if let Err(err) = put_part(&mut self.upload, &self.error_path, chunk).await {
                delete_partial_logged(
                    &self.store,
                    &self.key,
                    &self.error_path,
                    "tail put_part failed during close",
                )
                .await;
                return Err(err);
            }
        }
        let mut upload = self
            .upload
            .get_mut()
            .map_err(|_| {
                write_err(
                    &self.error_path,
                    ErrorKind::Unexpected,
                    "Writer internal state corrupted: upload mutex poisoned",
                )
            })?
            .take()
            .ok_or_else(|| {
                write_err(
                    &self.error_path,
                    ErrorKind::Unexpected,
                    "Writer internal state corrupted: upload already taken",
                )
            })?;
        if let Err(e) = upload.complete().await {
            delete_partial_logged(
                &self.store,
                &self.key,
                &self.error_path,
                "complete() failed during close",
            )
            .await;
            return Err(WriteError::IOError(
                parse_error(e, &self.error_path).with_context("Failed to complete ADLS write"),
            ));
        }
        Ok(())
    }
}

/// Free function (not method) so we don't have to thread `&mut self` borrows
/// through multiple field accesses. Takes the upload slot directly.
async fn put_part(
    upload: &mut Mutex<Option<Box<dyn MultipartUpload>>>,
    error_path: &str,
    chunk: Bytes,
) -> Result<(), WriteError> {
    let upload_ref = upload
        .get_mut()
        .map_err(|_| write_err(error_path, ErrorKind::Unexpected, "Writer mutex poisoned"))?
        .as_mut()
        .ok_or_else(|| write_err(error_path, ErrorKind::Unexpected, "Writer upload missing"))?;
    upload_ref
        .put_part(PutPayload::from_bytes(chunk))
        .await
        .map_err(|e| {
            WriteError::IOError(
                parse_error(e, error_path).with_context("ADLS multipart put_part failed"),
            )
        })
}

/// Failed-write cleanup: drop the partial upload and attempt to delete the
/// destination so a partial file doesn't linger. Updates the writer state.
/// The original write error is the caller's responsibility to surface; this
/// method's failures are logged-only.
async fn abort_after_failure(writer: &mut AdlsFileWrite) {
    // Drop the underlying upload — object_store's `abort()` on Azure is a
    // no-op, but dropping releases buffered state. We follow with an
    // explicit DELETE.
    if let Ok(slot) = writer.upload.get_mut() {
        slot.take();
    }
    match delete_partial(&writer.store, &writer.key, &writer.error_path).await {
        Ok(()) => writer.state = AdlsWriterState::Aborted,
        Err(delete_err) => {
            writer.state = AdlsWriterState::AbortFailed;
            tracing::warn!(
                path = %writer.error_path,
                error = ?delete_err,
                "Failed to delete partial ADLS file after streaming append error; \
                 partial file may exist at target location.",
            );
        }
    }
}

async fn delete_partial(
    store: &Arc<object_store::azure::MicrosoftAzure>,
    key: &ObjectStorePath,
    error_path: &str,
) -> Result<(), WriteError> {
    match store.delete(key).await {
        // 404 here is fine — if the multipart was never completed, the blob
        // may not exist server-side at all.
        Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
        Err(e) => Err(WriteError::IOError(
            parse_error(e, error_path).with_context("Failed to delete partial ADLS file"),
        )),
    }
}

async fn delete_partial_logged(
    store: &Arc<object_store::azure::MicrosoftAzure>,
    key: &ObjectStorePath,
    error_path: &str,
    context: &str,
) {
    if let Err(e) = delete_partial(store, key, error_path).await {
        tracing::warn!(
            path = %error_path,
            error = ?e,
            context = %context,
            "Failed to delete partial ADLS file. The file may persist at the target location.",
        );
    }
}

/// Build a `WriteError` against the writer's error-path location with the
/// supplied [`ErrorKind`]. Callers pick the kind based on whether the failure
/// is a *user* error (terminal-state writer — `ConditionNotMatch`) or an
/// *internal-state* bug (mutex poisoned / upload missing — `Unexpected`).
fn write_err(error_path: &str, kind: ErrorKind, msg: &'static str) -> WriteError {
    WriteError::IOError(IOError::new(kind, msg, error_path.to_string()))
}

impl Drop for AdlsFileWrite {
    fn drop(&mut self) {
        // Only act on `Active`. For terminal states (Closed/Aborted/AbortFailed),
        // an earlier code path already handled cleanup. We do NOT mutate
        // `self.state` here: the writer's memory is about to be released, so
        // any state write is unobservable. Keeping the field untouched also
        // means the post-condition matches the code path that actually ran —
        // earlier versions optimistically set `state = Aborted` before the
        // spawned task completed, which lied about what happened.
        if !matches!(self.state, AdlsWriterState::Active) {
            return;
        }

        // Active writer being dropped without an explicit `close()`. This
        // usually indicates a bug in the caller. Warn unconditionally so the
        // deviation is observable regardless of what the cleanup path does.
        tracing::warn!(
            path = %self.error_path,
            "AdlsFileWrite dropped while Active — close() was not called. \
             Spawning best-effort delete of any partial blob.",
        );

        // Drop in-flight upload state first; on Azure this is a no-op for
        // server-side cleanup but releases local buffers.
        if let Ok(slot) = self.upload.get_mut() {
            slot.take();
        }

        // Spawn a bounded delete. If we're outside a tokio runtime (shutdown
        // race), there is nothing we can do.
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            tracing::warn!(
                path = %self.error_path,
                "AdlsFileWrite dropped outside a tokio runtime; \
                 partial blob cannot be deleted.",
            );
            return;
        };

        let store = self.store.clone();
        let key = self.key.clone();
        let path = self.error_path.clone();
        tracing::debug!(
            path = %self.error_path,
            timeout = ?DROP_CANCEL_DURATION,
            "Spawning best-effort drop-delete for ADLS writer",
        );
        handle.spawn(async move {
            let delete = async {
                match store.delete(&key).await {
                    Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                    Err(e) => {
                        tracing::warn!(
                            path = %path,
                            error = ?e,
                            "Best-effort delete of partial ADLS file on Drop failed.",
                        );
                    }
                }
            };
            if tokio::time::timeout(DROP_CANCEL_DURATION, delete)
                .await
                .is_err()
            {
                tracing::warn!(
                    path = %path,
                    timeout = ?DROP_CANCEL_DURATION,
                    "Best-effort delete of partial ADLS file timed out.",
                );
            }
        });
    }
}

#[cfg(test)]
mod tests {
    //! Pure unit tests for the state machine. We construct an `AdlsFileWrite`
    //! with a fake [`MultipartUpload`] (no network) and an `Arc<MicrosoftAzure>`
    //! built from a builder-valid but functionally-unused access key (same
    //! pattern as `adls_storage.rs` tests). The store is only touched on the
    //! Drop-cleanup path, which these tests deliberately avoid driving end-to-
    //! end; coverage of that path lives in the integration suite.

    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use object_store::{
        MultipartUpload, PutPayload, PutResult, UploadPart, azure::MicrosoftAzureBuilder,
    };

    use super::*;

    /// Counter-instrumented fake `MultipartUpload`. Configurable to fail at
    /// either `put_part` or `complete` so we can drive every state transition
    /// without involving Azure or any HTTP layer.
    #[derive(Debug)]
    struct FakeUpload {
        put_parts: Arc<AtomicUsize>,
        completes: Arc<AtomicUsize>,
        aborts: Arc<AtomicUsize>,
        fail_put_part: bool,
        fail_complete: bool,
    }

    impl FakeUpload {
        fn new() -> Self {
            Self {
                put_parts: Arc::new(AtomicUsize::new(0)),
                completes: Arc::new(AtomicUsize::new(0)),
                aborts: Arc::new(AtomicUsize::new(0)),
                fail_put_part: false,
                fail_complete: false,
            }
        }

        fn failing_put_part(mut self) -> Self {
            self.fail_put_part = true;
            self
        }

        fn failing_complete(mut self) -> Self {
            self.fail_complete = true;
            self
        }
    }

    fn fake_error(msg: &'static str) -> object_store::Error {
        object_store::Error::Generic {
            store: "test",
            source: msg.into(),
        }
    }

    #[async_trait]
    impl MultipartUpload for FakeUpload {
        fn put_part(&mut self, _data: PutPayload) -> UploadPart {
            self.put_parts.fetch_add(1, Ordering::SeqCst);
            let fail = self.fail_put_part;
            Box::pin(async move {
                if fail {
                    Err(fake_error("simulated put_part failure"))
                } else {
                    Ok(())
                }
            })
        }

        async fn complete(&mut self) -> object_store::Result<PutResult> {
            self.completes.fetch_add(1, Ordering::SeqCst);
            if self.fail_complete {
                Err(fake_error("simulated complete failure"))
            } else {
                Ok(PutResult {
                    e_tag: None,
                    version: None,
                })
            }
        }

        async fn abort(&mut self) -> object_store::Result<()> {
            self.aborts.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    /// Build a real `MicrosoftAzure` with a builder-valid but functionally-
    /// unused access key. The store is only touched on the Drop-cleanup path;
    /// these tests do not drive that path, so no network call ever fires.
    fn fake_store() -> Arc<object_store::azure::MicrosoftAzure> {
        let store = MicrosoftAzureBuilder::new()
            .with_account("testacct")
            .with_container_name("test-container")
            .with_access_key("dGVzdC1rZXk=") // "test-key" base64; never used
            .build()
            .expect("MicrosoftAzureBuilder::build with stub credentials");
        Arc::new(store)
    }

    /// Build an `AdlsFileWrite` over the given fake upload. Helper because
    /// every test does the same setup.
    fn writer_over(upload: FakeUpload) -> AdlsFileWrite {
        AdlsFileWrite::new(
            fake_store(),
            ObjectStorePath::parse("blob/key.txt").expect("path parse"),
            "abfss://container@acct.dfs.core.windows.net/blob/key.txt".to_string(),
            Box::new(upload),
        )
    }

    #[tokio::test]
    async fn close_without_writes_completes_upload_and_transitions_to_closed() {
        let mut w = writer_over(FakeUpload::new());
        w.close().await.expect("close should succeed");
        assert!(matches!(w.state, AdlsWriterState::Closed));
    }

    #[tokio::test]
    async fn close_flushes_buffered_tail_smaller_than_part_size() {
        let upload = FakeUpload::new();
        let put_parts = upload.put_parts.clone();
        let completes = upload.completes.clone();

        let mut w = writer_over(upload);
        // Less than PART_SIZE so the chunk is held in buffer until close().
        w.write(Bytes::from_static(b"small payload"))
            .await
            .expect("write small");
        assert_eq!(
            put_parts.load(Ordering::SeqCst),
            0,
            "no part should fire while buffer < PART_SIZE"
        );

        w.close().await.expect("close should flush + complete");
        assert_eq!(put_parts.load(Ordering::SeqCst), 1, "tail must be flushed");
        assert_eq!(completes.load(Ordering::SeqCst), 1);
        assert!(matches!(w.state, AdlsWriterState::Closed));
    }

    #[tokio::test]
    async fn write_emits_part_when_buffer_crosses_part_size() {
        let upload = FakeUpload::new();
        let put_parts = upload.put_parts.clone();

        let mut w = writer_over(upload);
        // Exactly PART_SIZE: the inner loop fires once on the boundary.
        w.write(Bytes::from(vec![0u8; PART_SIZE]))
            .await
            .expect("write PART_SIZE");
        assert_eq!(
            put_parts.load(Ordering::SeqCst),
            1,
            "exactly one part should fire at PART_SIZE boundary"
        );
        // State stays Active; close hasn't been called.
        assert!(matches!(w.state, AdlsWriterState::Active));
    }

    #[tokio::test]
    async fn write_after_close_returns_error() {
        let mut w = writer_over(FakeUpload::new());
        w.close().await.expect("close");
        let err = w
            .write(Bytes::from_static(b"after close"))
            .await
            .expect_err("write-after-close should error");
        match err {
            WriteError::IOError(io) => assert_eq!(io.kind(), ErrorKind::ConditionNotMatch),
            other @ WriteError::InvalidLocation(_) => {
                panic!("expected IOError, got {other:?}")
            }
        }
    }

    #[tokio::test]
    async fn double_close_returns_error() {
        let mut w = writer_over(FakeUpload::new());
        w.close().await.expect("first close");
        let err = w.close().await.expect_err("second close should error");
        match err {
            WriteError::IOError(io) => assert_eq!(io.kind(), ErrorKind::ConditionNotMatch),
            other @ WriteError::InvalidLocation(_) => {
                panic!("expected IOError, got {other:?}")
            }
        }
    }

    #[tokio::test]
    async fn put_part_failure_during_write_aborts_writer() {
        let upload = FakeUpload::new().failing_put_part();
        let put_parts = upload.put_parts.clone();

        let mut w = writer_over(upload);
        // PART_SIZE bytes → buffer >= PART_SIZE → put_part fires → fails.
        let err = w
            .write(Bytes::from(vec![0u8; PART_SIZE]))
            .await
            .expect_err("failing put_part should surface");
        assert_eq!(put_parts.load(Ordering::SeqCst), 1);
        assert!(matches!(err, WriteError::IOError(_)));
        // Drop cleanup attempts a real store.delete(); on the fake store this
        // returns an Azure error, so we land in AbortFailed (not Aborted).
        // Either terminal state is acceptable; both prevent further writes.
        assert!(
            matches!(
                w.state,
                AdlsWriterState::Aborted | AdlsWriterState::AbortFailed
            ),
            "post-failure state should be terminal, got {:?}",
            w.state,
        );

        // Subsequent write must reject with ConditionNotMatch.
        let next = w
            .write(Bytes::from_static(b"after fail"))
            .await
            .expect_err("write after abort should error");
        match next {
            WriteError::IOError(io) => assert_eq!(io.kind(), ErrorKind::ConditionNotMatch),
            other @ WriteError::InvalidLocation(_) => {
                panic!("expected IOError, got {other:?}")
            }
        }
    }

    #[tokio::test]
    async fn complete_failure_during_close_surfaces_error() {
        let upload = FakeUpload::new().failing_complete();
        let completes = upload.completes.clone();

        let mut w = writer_over(upload);
        let err = w
            .close()
            .await
            .expect_err("failing complete should surface");
        assert_eq!(completes.load(Ordering::SeqCst), 1);
        match err {
            WriteError::IOError(_) => {}
            other @ WriteError::InvalidLocation(_) => {
                panic!("expected IOError, got {other:?}")
            }
        }
        // close() commits the Active → Closed transition before invoking
        // complete(); even on complete failure the writer is now terminal.
        assert!(matches!(w.state, AdlsWriterState::Closed));
    }

    /// FR-4 regression: calling `close()` on a writer that was already in a
    /// terminal non-`Closed` state must error AND leave the state untouched.
    /// (Earlier versions used `mem::replace` to flip state to `Closed` before
    /// checking the prev state, which silently overwrote `Aborted`.)
    #[tokio::test]
    async fn close_on_terminal_state_does_not_mutate_state() {
        let upload = FakeUpload::new().failing_put_part();
        let mut w = writer_over(upload);
        // Force a terminal state via a failing write.
        let _ = w
            .write(Bytes::from(vec![0u8; PART_SIZE]))
            .await
            .expect_err("forced failure to drive abort");
        // Now state is Aborted or AbortFailed (Drop-side delete runs against
        // the fake store and surfaces an error). Pin whichever non-Closed
        // terminal state we landed in.
        let pre_close_state = std::mem::discriminant(&w.state);
        assert!(!matches!(w.state, AdlsWriterState::Closed));

        let _ = w
            .close()
            .await
            .expect_err("close on terminal-non-Closed must error");
        // State must not have transitioned to Closed behind our back.
        assert_eq!(std::mem::discriminant(&w.state), pre_close_state);
    }

    /// Drop of a writer that is already in a terminal state (`Closed`) must
    /// be a no-op: no spawn, no warn, no state mutation.
    #[tokio::test]
    async fn drop_of_closed_writer_is_a_noop() {
        let mut w = writer_over(FakeUpload::new());
        w.close().await.expect("close");
        // Pre-drop snapshot.
        assert!(matches!(w.state, AdlsWriterState::Closed));
        // Drop runs at end of scope. The point of this test is that it doesn't
        // panic and doesn't try to delete (which would fire against the fake
        // store and produce a visible error log).
        drop(w);
    }

    /// Drop of an Active writer outside a tokio runtime must not panic. We
    /// can't observe the warn directly here, but we can confirm the Drop
    /// itself is panic-safe under the no-runtime branch.
    #[test]
    fn drop_of_active_writer_outside_runtime_does_not_panic() {
        let w = writer_over(FakeUpload::new());
        // Sanity: we are NOT in a tokio runtime here (no `#[tokio::test]`).
        assert!(tokio::runtime::Handle::try_current().is_err());
        // Drop the writer; the Drop impl must take the no-runtime branch.
        drop(w);
    }
}
