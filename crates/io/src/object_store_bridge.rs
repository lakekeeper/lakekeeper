//! Exposes any [`LakekeeperStorage`] as an [`object_store::ObjectStore`].
//!
//! This mirrors [`crate::iceberg_bridge::IcebergStorageBridge`], but targets the
//! `object_store` trait consumed by `DataFusion` (and thus the DataFusion-based
//! compaction path). `DataFusion` registers one `ObjectStore` per `scheme://authority`
//! URL and addresses objects with a *relative* [`object_store::path::Path`]. The
//! bridge is therefore **rooted** at a base [`Location`]: relative paths are joined
//! onto the base, and listing results are stripped back to relative paths.
//!
//! ## Multipart uploads
//!
//! [`object_store::WriteMultipart`] (used by `BufWriter` and the parquet writers)
//! submits parts in order via `put_part(&mut self)` but spawns the returned
//! `'static` futures onto a `JoinSet`, so they run concurrently and may *complete*
//! out of order. [`LakekeeperFileWrite`], however, is a strictly sequential append
//! stream. [`GatedMultipartUpload`] bridges the two with a oneshot "gate chain":
//! each part captures its submission order and waits for the previous part to finish
//! before writing, then releases the next. This preserves ordering, streams without
//! buffering the whole object, and is naturally back-pressured (a part future only
//! resolves once its bytes have been written).

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::{FutureExt, StreamExt, stream};
use object_store::{
    Attributes, CopyMode, CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload,
    PutResult, UploadPart, path::Path,
};
use tokio::sync::{Mutex, oneshot};

use crate::{
    DeleteError, ErrorKind, FileInfo, LakekeeperFileWrite, LakekeeperStorage, Location, ReadError,
    WriteError,
};

/// `store` label attached to [`object_store::Error`]s originating here.
const STORE: &str = "lakekeeper";

/// Concurrency for the individual deletes issued by [`ObjectStore::delete_stream`],
/// matching `object_store`'s own per-object delete backends (GCP/HTTP/Local).
const DELETE_CONCURRENCY: usize = 10;

/// An [`object_store::ObjectStore`] backed by a [`LakekeeperStorage`], rooted at a
/// base [`Location`] (`scheme://authority[/prefix]`).
#[derive(Debug, Clone)]
pub struct ObjectStoreBridge {
    lakekeeper_io: Arc<dyn LakekeeperStorage>,
    /// Base location without a trailing slash, e.g. `s3://bucket`. Relative
    /// `object_store` paths are appended to this to form absolute locations.
    base: String,
}

impl ObjectStoreBridge {
    /// Create a bridge rooted at `base` (typically the `scheme://authority` a
    /// `DataFusion` `ObjectStoreUrl` resolves to).
    #[must_use]
    pub fn new(lakekeeper_io: Arc<dyn LakekeeperStorage>, base: &Location) -> Self {
        Self {
            lakekeeper_io,
            base: base.as_str().trim_end_matches('/').to_string(),
        }
    }

    /// Join a relative `object_store` path onto the base to form an absolute location.
    fn absolute(&self, path: &Path) -> String {
        join(&self.base, path)
    }
}

/// Join a relative `object_store` path onto a base location string.
fn join(base: &str, path: &Path) -> String {
    let rel = path.as_ref();
    if rel.is_empty() {
        base.to_string()
    } else {
        format!("{base}/{rel}")
    }
}

/// Strip the base off an absolute location to recover the relative `object_store` path.
fn strip_to_relative(base: &str, absolute: &str) -> object_store::Result<Path> {
    absolute
        .strip_prefix(base)
        .map(|rest| Path::from(rest.strip_prefix('/').unwrap_or(rest)))
        .ok_or_else(|| object_store::Error::Generic {
            store: STORE,
            source: format!("listed location `{absolute}` is not under base `{base}`").into(),
        })
}

/// Epoch fallback for backends that don't report a modification time.
fn epoch() -> DateTime<Utc> {
    DateTime::from_timestamp(0, 0).unwrap_or_default()
}

fn file_info_to_meta(base: &str, info: &FileInfo) -> object_store::Result<ObjectMeta> {
    Ok(ObjectMeta {
        location: strip_to_relative(base, info.location().as_str())?,
        last_modified: info.last_modified().unwrap_or_else(epoch),
        size: info.size().unwrap_or(0),
        e_tag: None,
        version: None,
    })
}

fn read_err_to_os(path: &str, err: ReadError) -> object_store::Error {
    if matches!(&err, ReadError::IOError(e) if e.kind() == ErrorKind::NotFound) {
        object_store::Error::NotFound {
            path: path.to_string(),
            source: Box::new(err),
        }
    } else {
        object_store::Error::Generic {
            store: STORE,
            source: Box::new(err),
        }
    }
}

fn write_err_to_os(err: WriteError) -> object_store::Error {
    object_store::Error::Generic {
        store: STORE,
        source: Box::new(err),
    }
}

fn delete_err_to_os(path: &str, err: DeleteError) -> object_store::Error {
    if matches!(&err, DeleteError::IOError(e) if e.kind() == ErrorKind::NotFound) {
        object_store::Error::NotFound {
            path: path.to_string(),
            source: Box::new(err),
        }
    } else {
        object_store::Error::Generic {
            store: STORE,
            source: Box::new(err),
        }
    }
}

impl std::fmt::Display for ObjectStoreBridge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ObjectStoreBridge({})", self.base)
    }
}

#[async_trait]
impl ObjectStore for ObjectStoreBridge {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        // LakekeeperStorage has no conditional/atomic-create write; only plain
        // overwrite is supported. Tags/attributes are ignored (like backends that
        // don't support them).
        match opts.mode {
            PutMode::Overwrite => {}
            PutMode::Create | PutMode::Update(_) => {
                return Err(object_store::Error::NotImplemented {
                    operation: "put_opts with conditional PutMode (Create/Update)".to_string(),
                    implementer: STORE.to_string(),
                });
            }
        }

        let path = self.absolute(location);
        let bytes: Bytes = payload.into();
        self.lakekeeper_io
            .write(&path, bytes)
            .await
            .map_err(write_err_to_os)?;
        Ok(PutResult {
            e_tag: None,
            version: None,
        })
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        _opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        let path = self.absolute(location);
        let writer = self
            .lakekeeper_io
            .writer(&path)
            .await
            .map_err(write_err_to_os)?;
        Ok(Box::new(GatedMultipartUpload::new(writer, path)))
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let path = self.absolute(location);

        // HEAD: metadata only, no body.
        if options.head {
            let info = self
                .lakekeeper_io
                .metadata(&path)
                .await
                .map_err(|e| read_err_to_os(location.as_ref(), e))?;
            let meta = ObjectMeta {
                location: location.clone(),
                last_modified: info.last_modified().unwrap_or_else(epoch),
                size: info.size().unwrap_or(0),
                e_tag: None,
                version: None,
            };
            return Ok(GetResult {
                payload: GetResultPayload::Stream(stream::empty().boxed()),
                meta,
                range: 0..0,
                attributes: Attributes::default(),
            });
        }

        // Ranged read: we need the object size to resolve Offset/Suffix ranges and to
        // populate ObjectMeta, so a metadata lookup is required. Full reads avoid it —
        // the size is the length of the returned bytes.
        let (bytes, meta, range) = if let Some(get_range) = options.range {
            let info = self
                .lakekeeper_io
                .metadata(&path)
                .await
                .map_err(|e| read_err_to_os(location.as_ref(), e))?;
            let size = info.size().unwrap_or(0);
            let range = get_range
                .as_range(size)
                .map_err(|e| object_store::Error::Generic {
                    store: STORE,
                    source: Box::new(e),
                })?;
            let bytes = self
                .lakekeeper_io
                .read_range(&path, range.clone())
                .await
                .map_err(|e| read_err_to_os(location.as_ref(), e))?;
            let meta = ObjectMeta {
                location: location.clone(),
                last_modified: info.last_modified().unwrap_or_else(epoch),
                size,
                e_tag: None,
                version: None,
            };
            (bytes, meta, range)
        } else {
            let bytes = self
                .lakekeeper_io
                .read(&path)
                .await
                .map_err(|e| read_err_to_os(location.as_ref(), e))?;
            let size = bytes.len() as u64;
            let meta = ObjectMeta {
                location: location.clone(),
                last_modified: epoch(),
                size,
                e_tag: None,
                version: None,
            };
            (bytes, meta, 0..size)
        };

        let payload = GetResultPayload::Stream(
            stream::once(async move { Ok::<_, object_store::Error>(bytes) }).boxed(),
        );
        Ok(GetResult {
            payload,
            meta,
            range,
            attributes: Attributes::default(),
        })
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<'static, object_store::Result<Path>>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<Path>> {
        let io = self.lakekeeper_io.clone();
        let base = self.base.clone();
        locations
            .map(move |location| {
                let io = io.clone();
                let base = base.clone();
                async move {
                    let location = location?;
                    let path = join(&base, &location);
                    io.delete(&path)
                        .await
                        .map_err(|e| delete_err_to_os(location.as_ref(), e))?;
                    Ok(location)
                }
            })
            .buffered(DELETE_CONCURRENCY)
            .boxed()
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
        let io = self.lakekeeper_io.clone();
        let base = self.base.clone();
        let abs_prefix = prefix.map_or_else(|| base.clone(), |p| join(&base, p));

        // `async_stream` lets the generator own `io`/`abs_prefix` while holding the
        // borrowing page-stream across yields, producing a `'static` stream.
        async_stream::stream! {
            let mut pages = match io.list(&abs_prefix, None).await {
                Ok(pages) => pages,
                Err(e) => {
                    yield Err(object_store::Error::Generic { store: STORE, source: Box::new(e) });
                    return;
                }
            };
            while let Some(page) = pages.next().await {
                match page {
                    Ok(infos) => {
                        for info in &infos {
                            yield file_info_to_meta(&base, info);
                        }
                    }
                    Err(e) => {
                        yield Err(object_store::Error::Generic { store: STORE, source: Box::new(e) });
                        return;
                    }
                }
            }
        }
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let root = Path::default();
        let prefix = prefix.unwrap_or(&root);
        let abs_prefix = join(&self.base, prefix);

        let mut pages = self
            .lakekeeper_io
            .list(&abs_prefix, None)
            .await
            .map_err(|e| object_store::Error::Generic {
                store: STORE,
                source: Box::new(e),
            })?;

        let mut common_prefixes = std::collections::BTreeSet::new();
        let mut objects = Vec::new();

        while let Some(page) = pages.next().await {
            let infos = page.map_err(|e| object_store::Error::Generic {
                store: STORE,
                source: Box::new(e),
            })?;
            for info in infos {
                let relative = strip_to_relative(&self.base, info.location().as_str())?;
                // Only direct children of `prefix` are returned; deeper entries
                // collapse into their immediate common prefix (directory). Resolve
                // the common prefix (if any) in an inner scope so the borrows of
                // `relative` are released before we move it into `objects`.
                let common_prefix = {
                    let Some(mut parts) = relative.prefix_match(prefix) else {
                        continue;
                    };
                    let Some(first) = parts.next() else {
                        continue;
                    };
                    if parts.next().is_some() {
                        Some(prefix.clone().join(first))
                    } else {
                        None
                    }
                };
                match common_prefix {
                    Some(child) => {
                        common_prefixes.insert(child);
                    }
                    None => objects.push(ObjectMeta {
                        location: relative,
                        last_modified: info.last_modified().unwrap_or_else(epoch),
                        size: info.size().unwrap_or(0),
                        e_tag: None,
                        version: None,
                    }),
                }
            }
        }

        Ok(ListResult {
            common_prefixes: common_prefixes.into_iter().collect(),
            objects,
        })
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        // LakekeeperStorage has no server-side copy, so this is read-then-write.
        let from_path = self.absolute(from);
        let to_path = self.absolute(to);

        // `Create` mode is best-effort: LakekeeperStorage has no atomic create, so
        // this exists-check is racy (same limitation object_store documents for S3).
        if options.mode == CopyMode::Create
            && self
                .lakekeeper_io
                .exists(&to_path)
                .await
                .map_err(|e| read_err_to_os(to.as_ref(), e))?
        {
            return Err(object_store::Error::AlreadyExists {
                path: to.to_string(),
                source: "destination already exists".into(),
            });
        }

        let bytes = self
            .lakekeeper_io
            .read(&from_path)
            .await
            .map_err(|e| read_err_to_os(from.as_ref(), e))?;
        self.lakekeeper_io
            .write(&to_path, bytes)
            .await
            .map_err(write_err_to_os)?;
        Ok(())
    }
}

/// [`MultipartUpload`] that serialises concurrently-polled parts back into
/// submission order over a sequential [`LakekeeperFileWrite`]. See the module docs.
#[derive(Debug)]
struct GatedMultipartUpload {
    /// Shared so the `'static` part futures and `complete`/`abort` can all reach it.
    /// `None` once completed or aborted.
    writer: Arc<Mutex<Option<Box<dyn LakekeeperFileWrite>>>>,
    /// Gate the *next* submitted part must await before writing. Initialised to an
    /// already-fired receiver so the first part starts immediately.
    next_gate: oneshot::Receiver<()>,
    path: String,
}

/// A oneshot receiver whose sender has already fired — an open gate.
fn open_gate() -> oneshot::Receiver<()> {
    let (tx, rx) = oneshot::channel();
    let _ = tx.send(());
    rx
}

impl GatedMultipartUpload {
    fn new(writer: Box<dyn LakekeeperFileWrite>, path: String) -> Self {
        Self {
            writer: Arc::new(Mutex::new(Some(writer))),
            next_gate: open_gate(),
            path,
        }
    }
}

#[async_trait]
impl MultipartUpload for GatedMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        // Submission runs under `&mut self`, so it is sequential: capture this part's
        // place in the order by taking the previous part's completion gate and
        // installing a fresh gate for the next part.
        let (done_tx, done_rx) = oneshot::channel();
        let wait_for_prev = std::mem::replace(&mut self.next_gate, done_rx);
        let writer = self.writer.clone();
        let path = self.path.clone();

        async move {
            // Block until the previous part has finished writing. A dropped sender
            // means an earlier part failed or the upload was aborted.
            wait_for_prev
                .await
                .map_err(|_| object_store::Error::Generic {
                    store: STORE,
                    source: format!("multipart upload to `{path}` aborted before this part").into(),
                })?;

            {
                let mut guard = writer.lock().await;
                let w = guard.as_mut().ok_or_else(|| object_store::Error::Generic {
                    store: STORE,
                    source: format!("multipart upload to `{path}` already finished").into(),
                })?;
                for chunk in data {
                    w.write(chunk).await.map_err(write_err_to_os)?;
                }
            }

            // Release the next part (if any; a dropped receiver is fine).
            let _ = done_tx.send(());
            Ok(())
        }
        .boxed()
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        let mut guard = self.writer.lock().await;
        let mut writer = guard.take().ok_or_else(|| object_store::Error::Generic {
            store: STORE,
            source: "multipart upload already completed or aborted".into(),
        })?;
        writer.close().await.map_err(write_err_to_os)?;
        Ok(PutResult {
            e_tag: None,
            version: None,
        })
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        // Best-effort cancel: drop the writer without closing. Per LakekeeperFileWrite's
        // contract, Drop is a best-effort abort of the underlying upload.
        let mut guard = self.writer.lock().await;
        let _ = guard.take();
        Ok(())
    }
}

#[cfg(all(test, feature = "storage-in-memory"))]
mod tests {
    use std::str::FromStr;

    use object_store::{GetRange, ObjectStoreExt};

    use super::*;
    use crate::memory::MemoryStorage;

    fn bridge() -> ObjectStoreBridge {
        let io: Arc<dyn LakekeeperStorage> = Arc::new(MemoryStorage::new_isolated());
        let base = Location::from_str("memory://bucket").unwrap();
        ObjectStoreBridge::new(io, &base)
    }

    #[tokio::test]
    async fn test_put_get_roundtrip() {
        let store = bridge();
        let path = Path::from("dir/file.parquet");
        store
            .put(&path, PutPayload::from(Bytes::from_static(b"hello world")))
            .await
            .unwrap();

        let got = store.get(&path).await.unwrap();
        assert_eq!(got.meta.size, 11);
        assert_eq!(got.meta.location, path);
        let bytes = got.bytes().await.unwrap();
        assert_eq!(&bytes[..], b"hello world");
    }

    #[tokio::test]
    async fn test_put_writes_through_to_absolute_location() {
        let io: Arc<dyn LakekeeperStorage> = Arc::new(MemoryStorage::new_isolated());
        let base = Location::from_str("memory://bucket/warehouse").unwrap();
        let store = ObjectStoreBridge::new(io.clone(), &base);
        store
            .put(
                &Path::from("a/b.txt"),
                PutPayload::from(Bytes::from_static(b"x")),
            )
            .await
            .unwrap();

        // The underlying storage must see the fully-qualified, base-rooted path.
        let raw = io.read("memory://bucket/warehouse/a/b.txt").await.unwrap();
        assert_eq!(&raw[..], b"x");
    }

    #[tokio::test]
    async fn test_get_range_and_head() {
        let store = bridge();
        let path = Path::from("file.bin");
        store
            .put(&path, PutPayload::from(Bytes::from_static(b"0123456789")))
            .await
            .unwrap();

        // Bounded range.
        let bytes = store.get_range(&path, 2..5).await.unwrap();
        assert_eq!(&bytes[..], b"234");

        // Suffix range needs the object size resolved from metadata.
        let suffix = store
            .get_opts(
                &path,
                GetOptions {
                    range: Some(GetRange::Suffix(3)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(suffix.range, 7..10);
        assert_eq!(&suffix.bytes().await.unwrap()[..], b"789");

        // HEAD returns metadata without a body.
        let meta = store.head(&path).await.unwrap();
        assert_eq!(meta.size, 10);
        assert_eq!(meta.location, path);
    }

    #[tokio::test]
    async fn test_get_missing_is_not_found() {
        let store = bridge();
        let err = store.get(&Path::from("nope")).await.unwrap_err();
        assert!(
            matches!(err, object_store::Error::NotFound { .. }),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn test_conditional_put_unsupported() {
        let store = bridge();
        let err = store
            .put_opts(
                &Path::from("x"),
                PutPayload::from(Bytes::from_static(b"y")),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();
        assert!(
            matches!(err, object_store::Error::NotImplemented { .. }),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn test_multipart_roundtrip_in_order() {
        let store = bridge();
        let path = Path::from("multi/part.bin");
        let mut upload = store.put_multipart(&path).await.unwrap();
        upload
            .put_part(PutPayload::from(Bytes::from_static(b"aaaa")))
            .await
            .unwrap();
        upload
            .put_part(PutPayload::from(Bytes::from_static(b"bbbb")))
            .await
            .unwrap();
        upload
            .put_part(PutPayload::from(Bytes::from_static(b"cccc")))
            .await
            .unwrap();
        upload.complete().await.unwrap();

        let bytes = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(&bytes[..], b"aaaabbbbcccc");
    }

    /// The gate chain must preserve submission order even when part futures are
    /// polled concurrently and complete out of order — exactly how
    /// `object_store::WriteMultipart` drives uploads (spawned onto a `JoinSet`).
    #[tokio::test]
    async fn test_multipart_ordered_under_concurrent_polling() {
        let store = bridge();
        let path = Path::from("multi/concurrent.bin");
        let mut upload = store.put_multipart(&path).await.unwrap();

        // Submit in order, but await the returned futures in reverse.
        let f0 = upload.put_part(PutPayload::from(Bytes::from_static(b"000")));
        let f1 = upload.put_part(PutPayload::from(Bytes::from_static(b"111")));
        let f2 = upload.put_part(PutPayload::from(Bytes::from_static(b"222")));
        // Joining all three drives them concurrently regardless of await order.
        futures::future::try_join3(f2, f0, f1).await.unwrap();
        upload.complete().await.unwrap();

        let bytes = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(&bytes[..], b"000111222");
    }

    #[tokio::test]
    async fn test_multipart_abort_leaves_no_object() {
        let store = bridge();
        let path = Path::from("multi/aborted.bin");
        let mut upload = store.put_multipart(&path).await.unwrap();
        upload
            .put_part(PutPayload::from(Bytes::from_static(b"data")))
            .await
            .unwrap();
        upload.abort().await.unwrap();

        assert!(store.get(&path).await.is_err());
    }

    #[tokio::test]
    async fn test_list_returns_relative_paths() {
        let store = bridge();
        for p in ["a/1.txt", "a/2.txt", "a/b/3.txt", "c/4.txt"] {
            store
                .put(&Path::from(p), PutPayload::from(Bytes::from_static(b"z")))
                .await
                .unwrap();
        }

        let mut listed: Vec<String> = store
            .list(Some(&Path::from("a")))
            .map(|m| m.unwrap().location.to_string())
            .collect::<Vec<_>>()
            .await;
        listed.sort();
        assert_eq!(listed, vec!["a/1.txt", "a/2.txt", "a/b/3.txt"]);
    }

    #[tokio::test]
    async fn test_list_with_delimiter() {
        let store = bridge();
        for p in ["a/1.txt", "a/2.txt", "a/b/3.txt", "a/b/4.txt"] {
            store
                .put(&Path::from(p), PutPayload::from(Bytes::from_static(b"z")))
                .await
                .unwrap();
        }

        let result = store
            .list_with_delimiter(Some(&Path::from("a")))
            .await
            .unwrap();
        let mut objects: Vec<String> = result
            .objects
            .iter()
            .map(|o| o.location.to_string())
            .collect();
        objects.sort();
        assert_eq!(objects, vec!["a/1.txt", "a/2.txt"]);

        let prefixes: Vec<String> = result
            .common_prefixes
            .iter()
            .map(ToString::to_string)
            .collect();
        assert_eq!(prefixes, vec!["a/b"]);
    }

    #[tokio::test]
    async fn test_delete_and_delete_stream() {
        let store = bridge();
        for p in ["d/1.txt", "d/2.txt"] {
            store
                .put(&Path::from(p), PutPayload::from(Bytes::from_static(b"z")))
                .await
                .unwrap();
        }
        store.delete(&Path::from("d/1.txt")).await.unwrap();
        assert!(store.get(&Path::from("d/1.txt")).await.is_err());
        assert!(store.get(&Path::from("d/2.txt")).await.is_ok());
    }

    #[tokio::test]
    async fn test_copy_and_copy_if_not_exists() {
        let store = bridge();
        let from = Path::from("src.txt");
        let to = Path::from("dst.txt");
        store
            .put(&from, PutPayload::from(Bytes::from_static(b"payload")))
            .await
            .unwrap();

        store.copy(&from, &to).await.unwrap();
        assert_eq!(
            &store.get(&to).await.unwrap().bytes().await.unwrap()[..],
            b"payload"
        );

        // copy_if_not_exists must fail when the destination already exists.
        let err = store.copy_if_not_exists(&from, &to).await.unwrap_err();
        assert!(
            matches!(err, object_store::Error::AlreadyExists { .. }),
            "{err:?}"
        );
    }
}
