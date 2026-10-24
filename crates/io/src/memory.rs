use std::{cell::RefCell, collections::HashMap, sync::Arc};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::stream::{self, BoxStream, StreamExt};
use tokio::sync::RwLock;

use crate::{
    DeleteBatchError, DeleteError, DeleteMarker, ErrorKind, FileInfo, IOError,
    InvalidLocationError, LakekeeperFileWrite, LakekeeperStorage, Location, ObjectRead, ObjectTags,
    ObjectVersion, ObjectVersionEntry, ReadError, WriteError, WriteOutcome,
};

/// A stored version of an object.
#[derive(Debug, Clone)]
struct MemoryVersion {
    id: String,
    bytes: Bytes,
    /// The `ETag` S3 reports for an object written in one request.
    e_tag: String,
    last_modified: DateTime<Utc>,
    tags: ObjectTags,
}

impl MemoryVersion {
    fn new(bytes: Bytes, last_modified: DateTime<Utc>) -> Self {
        Self {
            id: uuid::Uuid::now_v7().to_string(),
            e_tag: format!("\"{:x}\"", md5::compute(&bytes)),
            bytes,
            last_modified,
            tags: ObjectTags::new(),
        }
    }
}

/// One version of a key, as a versioned S3 bucket keeps it.
#[derive(Debug, Clone)]
enum MemoryEntry {
    Version(MemoryVersion),
    DeleteMarker {
        id: String,
        last_modified: DateTime<Utc>,
    },
}

/// Versions of each key, oldest first.
type MemoryStore = HashMap<String, Vec<MemoryEntry>>;

/// In-memory storage implementation for testing and development purposes.
/// All data is stored in memory and persists across instances within the same thread.
///
/// It behaves like a versioned S3 bucket: a write adds a version, a delete adds a delete
/// marker, and no version is ever removed except by [`MemoryStorage::clear`].
#[derive(Debug, Clone)]
pub struct MemoryStorage {
    data: Arc<RwLock<MemoryStore>>,
    use_global_store: bool,
}

thread_local! {
    static GLOBAL_MEMORY_STORE: RefCell<Option<Arc<RwLock<MemoryStore>>>> = const { RefCell::new(None) };
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryStorage {
    /// Create a new memory storage instance.
    /// By default, this will use the global thread-local storage,
    /// preserving data across instances within the same thread.
    #[must_use]
    pub fn new() -> Self {
        Self::with_global_store(true)
    }

    /// Create a new memory storage instance, optionally using the global thread-local storage.
    /// When `use_global_store` is true, data will persist across instances within the same thread.
    #[must_use]
    pub fn with_global_store(use_global_store: bool) -> Self {
        if use_global_store {
            let data = GLOBAL_MEMORY_STORE.with(|store| {
                let mut store_ref = store.borrow_mut();
                if let Some(existing_store) = store_ref.as_ref() {
                    return existing_store.clone();
                }

                let data = Arc::new(RwLock::new(HashMap::new()));

                if store_ref.is_none() {
                    *store_ref = Some(data.clone());
                }
                data
            });

            MemoryStorage {
                data,
                use_global_store,
            }
        } else {
            MemoryStorage {
                data: Arc::new(RwLock::new(HashMap::new())),
                use_global_store,
            }
        }
    }

    #[must_use]
    pub fn is_global_store(&self) -> bool {
        self.use_global_store
    }

    /// Create a new isolated memory storage instance with no connection to the global store.
    #[must_use]
    pub fn new_isolated() -> Self {
        Self::with_global_store(false)
    }

    /// Create a new memory storage instance with pre-populated data.
    /// This creates an isolated instance not connected to the global store.
    #[must_use]
    pub fn with_data(data: HashMap<String, Bytes>) -> Self {
        let now = Utc::now();
        Self::with_timed_data(data.into_iter().map(|(k, v)| (k, (v, now))).collect())
    }

    /// Create a new memory storage instance with pre-populated data and custom timestamps.
    /// This creates an isolated instance not connected to the global store.
    #[must_use]
    pub fn with_timed_data(data: HashMap<String, (Bytes, DateTime<Utc>)>) -> Self {
        let data = data
            .into_iter()
            .map(|(k, (bytes, last_modified))| {
                (
                    k,
                    vec![MemoryEntry::Version(MemoryVersion::new(
                        bytes,
                        last_modified,
                    ))],
                )
            })
            .collect();
        MemoryStorage {
            data: Arc::new(RwLock::new(data)),
            use_global_store: false,
        }
    }

    /// Get the number of objects stored in memory: keys whose current version is not a
    /// delete marker.
    pub async fn len(&self) -> usize {
        self.data
            .read()
            .await
            .values()
            .filter(|entries| current(entries).is_some())
            .count()
    }

    /// Check if the storage is empty.
    pub async fn is_empty(&self) -> bool {
        self.len().await == 0
    }

    /// Clear all data, every version included, from storage.
    /// This will clear the global storage if this instance is using it.
    pub async fn clear(&self) {
        self.data.write().await.clear();
    }

    /// Reset the global memory store.
    /// This will clear all data from the global store and is useful for testing.
    pub fn reset_global_store() {
        GLOBAL_MEMORY_STORE.with(|store| {
            *store.borrow_mut() = None;
        });
    }
}

const MEMORY_PREFIX: &str = "memory://";

/// Normalizes a memory path by stripping the optional "memory://" prefix
/// and returning the key for storage.
fn normalize_memory_path(path: &str) -> Result<String, InvalidLocationError> {
    let key = if let Some(stripped) = path.strip_prefix(MEMORY_PREFIX) {
        stripped
    } else {
        path
    };

    if key.is_empty() {
        return Err(InvalidLocationError::new(
            path.to_string(),
            "Empty path is not valid".to_string(),
        ));
    }

    Ok(key.to_string())
}

/// The prefix `path` lists, with a trailing slash.
fn list_prefix(path: &str) -> Result<String, InvalidLocationError> {
    let key = normalize_memory_path(path)?;
    if key.ends_with('/') {
        Ok(key)
    } else {
        Ok(format!("{key}/"))
    }
}

fn memory_location(key: &str) -> Result<Location, IOError> {
    let location_str = format!("{MEMORY_PREFIX}{key}");
    location_str.parse::<Location>().map_err(|e| {
        IOError::new(
            ErrorKind::Unexpected,
            format!("Failed to parse location: {e}"),
            location_str.clone(),
        )
        .set_source(anyhow::anyhow!(e))
    })
}

/// The current version of a key, unless the key is absent or its current version is a
/// delete marker.
fn current(entries: &[MemoryEntry]) -> Option<&MemoryVersion> {
    match entries.last() {
        Some(MemoryEntry::Version(version)) => Some(version),
        Some(MemoryEntry::DeleteMarker { .. }) | None => None,
    }
}

fn not_found(key: &str) -> IOError {
    IOError::new(
        ErrorKind::NotFound,
        "Object not found in memory storage",
        key.to_string(),
    )
}

fn version_not_found(key: &str, version: &str) -> IOError {
    IOError::new(
        ErrorKind::NotFound,
        format!("Version `{version}` not found in memory storage"),
        key.to_string(),
    )
}

/// Fails unless `version` has the form of the version ids this backend issues.
fn check_version_id(key: &str, version: &str) -> Result<(), IOError> {
    uuid::Uuid::parse_str(version).map(|_| ()).map_err(|_| {
        IOError::new(
            ErrorKind::ConditionNotMatch,
            format!("`{version}` is not a memory storage version id"),
            key.to_string(),
        )
    })
}

/// The object version `version` of `key`. A delete marker is not an object version, so
/// its id is not found, as S3 refuses to read or tag one.
fn find_version<'a>(
    data: &'a MemoryStore,
    key: &str,
    version: &str,
) -> Result<&'a MemoryVersion, IOError> {
    check_version_id(key, version)?;
    data.get(key)
        .and_then(|entries| {
            entries.iter().find_map(|entry| match entry {
                MemoryEntry::Version(stored) if stored.id == version => Some(stored),
                _ => None,
            })
        })
        .ok_or_else(|| version_not_found(key, version))
}

fn find_version_mut<'a>(
    data: &'a mut MemoryStore,
    key: &str,
    version: &str,
) -> Result<&'a mut MemoryVersion, IOError> {
    check_version_id(key, version)?;
    data.get_mut(key)
        .and_then(|entries| {
            entries.iter_mut().find_map(|entry| match entry {
                MemoryEntry::Version(stored) if stored.id == version => Some(stored),
                _ => None,
            })
        })
        .ok_or_else(|| version_not_found(key, version))
}

/// The [`FileInfo`] of a stored object version.
fn version_file_info(key: &str, version: &MemoryVersion) -> Result<FileInfo, IOError> {
    Ok(FileInfo::new(
        Some(version.last_modified),
        memory_location(key)?,
        Some(version.bytes.len() as u64),
    )
    .with_e_tag(Some(version.e_tag.clone()))
    .with_version(Some(version.id.clone())))
}

/// Build the [`ObjectRead`] of a stored object version.
fn object_read(key: &str, version: &MemoryVersion) -> Result<ObjectRead, IOError> {
    Ok(ObjectRead {
        bytes: version.bytes.clone(),
        info: version_file_info(key, version)?,
    })
}

/// Most entries S3 returns in one listing page, and its page size for a requested size of
/// zero.
const MAX_PAGE_SIZE: usize = 1000;

/// Pages of at most `page_size` items, as S3 pages a listing.
fn pages<T: Clone>(items: &[T], page_size: Option<usize>) -> Vec<Vec<T>> {
    let page_size = page_size
        .filter(|size| *size > 0)
        .map_or(MAX_PAGE_SIZE, |size| size.min(MAX_PAGE_SIZE));
    items.chunks(page_size).map(<[T]>::to_vec).collect()
}

fn slice_range(key: &str, bytes: &Bytes, range: std::ops::Range<u64>) -> Result<Bytes, IOError> {
    if range.end < range.start {
        return Err(IOError::new(
            ErrorKind::ConditionNotMatch,
            format!(
                "Invalid range: start ({}) > end ({})",
                range.start, range.end
            ),
            key.to_string(),
        ));
    }
    if range.is_empty() {
        return Ok(Bytes::new());
    }
    let start = usize::try_from(range.start).map_err(|_| {
        IOError::new(
            ErrorKind::ConditionNotMatch,
            format!("Range start {} too large for this platform", range.start),
            key.to_string(),
        )
    })?;
    let end = usize::try_from(range.end).map_err(|_| {
        IOError::new(
            ErrorKind::ConditionNotMatch,
            format!("Range end {} too large for this platform", range.end),
            key.to_string(),
        )
    })?;
    if end > bytes.len() {
        return Err(IOError::new(
            ErrorKind::ConditionNotMatch,
            format!("Range end {end} exceeds file size {}", bytes.len()),
            key.to_string(),
        ));
    }
    Ok(bytes.slice(start..end))
}

/// Add `bytes` as the new current version of `key`.
fn put_version(data: &mut MemoryStore, key: String, bytes: Bytes) -> WriteOutcome {
    let version = MemoryVersion::new(bytes, Utc::now());
    let outcome = WriteOutcome {
        e_tag: Some(version.e_tag.clone()),
        version: Some(version.id.clone()),
    };
    data.entry(key)
        .or_default()
        .push(MemoryEntry::Version(version));
    outcome
}

/// Add a delete marker as the new current version of `key`. Like S3, this happens
/// whether or not the key has a current version.
fn put_delete_marker(data: &mut MemoryStore, key: String) {
    data.entry(key)
        .or_default()
        .push(MemoryEntry::DeleteMarker {
            id: uuid::Uuid::now_v7().to_string(),
            last_modified: Utc::now(),
        });
}

/// The entries of one key, newest first.
fn version_entries(key: &str, entries: &[MemoryEntry]) -> Result<Vec<ObjectVersionEntry>, IOError> {
    let location = memory_location(key)?;
    Ok(entries
        .iter()
        .rev()
        .enumerate()
        .map(|(index, entry)| match entry {
            MemoryEntry::Version(version) => ObjectVersionEntry::Version(
                ObjectVersion::builder()
                    .location(location.clone())
                    .version(version.id.clone())
                    .e_tag(Some(version.e_tag.clone()))
                    .size(Some(version.bytes.len() as u64))
                    .last_modified(Some(version.last_modified))
                    .is_latest(index == 0)
                    .build(),
            ),
            MemoryEntry::DeleteMarker { id, last_modified } => ObjectVersionEntry::DeleteMarker(
                DeleteMarker::builder()
                    .location(location.clone())
                    .version(id.clone())
                    .last_modified(Some(*last_modified))
                    .is_latest(index == 0)
                    .build(),
            ),
        })
        .collect())
}

#[async_trait::async_trait]
impl LakekeeperStorage for MemoryStorage {
    async fn delete(&self, path: &str) -> Result<(), DeleteError> {
        let key = normalize_memory_path(path)?;
        put_delete_marker(&mut *self.data.write().await, key);
        Ok(())
    }

    async fn delete_batch(&self, paths: &[String]) -> Result<(), DeleteBatchError> {
        for path_str in paths {
            self.delete(path_str).await?;
        }
        Ok(())
    }

    async fn write(&self, path: &str, bytes: Bytes) -> Result<WriteOutcome, WriteError> {
        let key = normalize_memory_path(path)?;
        Ok(put_version(&mut *self.data.write().await, key, bytes))
    }

    async fn writer(&self, path: &str) -> Result<Box<dyn LakekeeperFileWrite>, WriteError> {
        let key = normalize_memory_path(path)?;
        Ok(Box::new(MemoryFileWrite {
            data: self.data.clone(),
            key,
            buffer: Vec::new(),
            closed: false,
        }))
    }

    async fn read(&self, path: &str) -> Result<ObjectRead, ReadError> {
        let key = normalize_memory_path(path)?;
        let data = self.data.read().await;
        let version = data
            .get(&key)
            .and_then(|entries| current(entries))
            .ok_or_else(|| not_found(&key))?;
        Ok(object_read(&key, version)?)
    }

    async fn read_single(&self, path: &str) -> Result<Bytes, ReadError> {
        self.read(path).await.map(|r| r.bytes)
    }

    async fn read_range(
        &self,
        path: &str,
        range: std::ops::Range<u64>,
    ) -> Result<Bytes, ReadError> {
        let key = normalize_memory_path(path)?;
        // Invalid and empty ranges are answered without looking the key up.
        if range.is_empty() {
            return Ok(slice_range(&key, &Bytes::new(), range)?);
        }
        let read = self.read(path).await?;
        Ok(slice_range(&key, &read.bytes, range)?)
    }

    async fn metadata(&self, path: &str) -> Result<FileInfo, ReadError> {
        self.read(path).await.map(|r| r.info)
    }

    async fn list(
        &self,
        path: &str,
        page_size: Option<usize>,
    ) -> Result<BoxStream<'_, Result<Vec<FileInfo>, IOError>>, InvalidLocationError> {
        let prefix = list_prefix(path)?;

        let data = self.data.read().await;
        let mut matching_files: Vec<(&String, &MemoryVersion)> = data
            .iter()
            .filter(|(key, _)| key.starts_with(&prefix))
            .filter_map(|(key, entries)| current(entries).map(|version| (key, version)))
            .collect();

        // Sort for consistent ordering
        matching_files.sort_by(|a, b| a.0.cmp(b.0));

        let mut all_file_infos = Vec::new();
        for (key, version) in matching_files {
            // Like S3, a listing reports no version.
            match version_file_info(key, version) {
                Ok(info) => all_file_infos.push(info.with_version(None)),
                // Return an error in the stream
                Err(error) => return Ok(stream::iter(std::iter::once(Err(error))).boxed()),
            }
        }

        // Create a stream that returns pages of locations
        let stream = stream::iter(pages(&all_file_infos, page_size).into_iter().map(Ok));

        Ok(stream.boxed())
    }

    /// Adds a delete marker to every key under the prefix that has a current version,
    /// in a single write-lock acquisition.
    async fn remove_all(&self, path: &str) -> Result<(), DeleteError> {
        let prefix = list_prefix(path)?;

        let mut data = self.data.write().await;
        let live_keys: Vec<String> = data
            .iter()
            .filter(|(key, entries)| key.starts_with(&prefix) && current(entries).is_some())
            .map(|(key, _)| key.clone())
            .collect();
        for key in live_keys {
            put_delete_marker(&mut data, key);
        }
        Ok(())
    }

    async fn read_version(&self, path: &str, version: &str) -> Result<ObjectRead, ReadError> {
        let key = normalize_memory_path(path)?;
        let data = self.data.read().await;
        Ok(object_read(&key, find_version(&data, &key, version)?)?)
    }

    async fn metadata_version(&self, path: &str, version: &str) -> Result<FileInfo, ReadError> {
        let key = normalize_memory_path(path)?;
        let data = self.data.read().await;
        Ok(version_file_info(
            &key,
            find_version(&data, &key, version)?,
        )?)
    }

    async fn read_range_version(
        &self,
        path: &str,
        version: &str,
        range: std::ops::Range<u64>,
    ) -> Result<Bytes, ReadError> {
        let key = normalize_memory_path(path)?;
        // Invalid and empty ranges are answered without looking the version up.
        if range.is_empty() {
            return Ok(slice_range(&key, &Bytes::new(), range)?);
        }
        let read = self.read_version(path, version).await?;
        Ok(slice_range(&key, &read.bytes, range)?)
    }

    async fn list_versions(
        &self,
        path: &str,
        page_size: Option<usize>,
    ) -> Result<BoxStream<'_, Result<Vec<ObjectVersionEntry>, IOError>>, ReadError> {
        let prefix = list_prefix(path)?;

        let data = self.data.read().await;
        let mut keys: Vec<&String> = data.keys().filter(|key| key.starts_with(&prefix)).collect();
        keys.sort();

        let mut all_entries = Vec::new();
        for key in keys {
            match version_entries(key, &data[key]) {
                Ok(entries) => all_entries.extend(entries),
                Err(error) => return Ok(stream::iter(std::iter::once(Err(error))).boxed()),
            }
        }

        Ok(stream::iter(pages(&all_entries, page_size).into_iter().map(Ok)).boxed())
    }

    async fn get_tags(&self, path: &str, version: &str) -> Result<ObjectTags, ReadError> {
        let key = normalize_memory_path(path)?;
        let data = self.data.read().await;
        Ok(find_version(&data, &key, version)?.tags.clone())
    }

    async fn put_tags(
        &self,
        path: &str,
        version: &str,
        tags: &ObjectTags,
    ) -> Result<(), WriteError> {
        let key = normalize_memory_path(path)?;
        crate::validate_tags(tags, &key)?;
        let mut data = self.data.write().await;
        find_version_mut(&mut data, &key, version)?.tags = tags.clone();
        Ok(())
    }

    async fn remove_tags(&self, path: &str, version: &str) -> Result<(), WriteError> {
        let key = normalize_memory_path(path)?;
        let mut data = self.data.write().await;
        find_version_mut(&mut data, &key, version)?.tags.clear();
        Ok(())
    }
}

/// Streaming writer for the in-memory backend.
///
/// Buffers all written bytes locally and adds them as a new version on `close`.
/// Calling `write` after `close` returns an error.
#[derive(Debug)]
pub(crate) struct MemoryFileWrite {
    data: Arc<RwLock<MemoryStore>>,
    key: String,
    buffer: Vec<u8>,
    closed: bool,
}

#[async_trait::async_trait]
impl LakekeeperFileWrite for MemoryFileWrite {
    async fn write(&mut self, bytes: Bytes) -> Result<(), WriteError> {
        if self.closed {
            return Err(WriteError::IOError(IOError::new(
                ErrorKind::ConditionNotMatch,
                "Cannot write to closed writer",
                self.key.clone(),
            )));
        }
        self.buffer.extend_from_slice(&bytes);
        Ok(())
    }

    async fn close(&mut self) -> Result<WriteOutcome, WriteError> {
        if self.closed {
            return Err(WriteError::IOError(IOError::new(
                ErrorKind::ConditionNotMatch,
                "Writer already closed",
                self.key.clone(),
            )));
        }
        let bytes = Bytes::from(std::mem::take(&mut self.buffer));
        let outcome = put_version(&mut *self.data.write().await, self.key.clone(), bytes);
        self.closed = true;
        Ok(outcome)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use bytes::Bytes;
    use futures::StreamExt;

    use crate::{
        ErrorKind, IOError, LakekeeperStorage, ObjectTags, ObjectVersionEntry, ReadError,
        WriteError,
        memory::{MemoryStorage, pages},
    };

    async fn all_versions(storage: &MemoryStorage, prefix: &str) -> Vec<ObjectVersionEntry> {
        let pages: Vec<Result<Vec<ObjectVersionEntry>, IOError>> = storage
            .list_versions(prefix, None)
            .await
            .unwrap()
            .collect()
            .await;
        pages.into_iter().flat_map(Result::unwrap).collect()
    }

    #[tokio::test]
    async fn test_memory_storage_basic_operations() {
        let storage = MemoryStorage::new();

        // Test write and read
        let test_path = "memory://test/file.txt";
        let test_data = Bytes::from("Hello, World!");

        storage.write(test_path, test_data.clone()).await.unwrap();
        let read_data = storage.read(test_path).await.unwrap().bytes;
        assert_eq!(test_data, read_data);

        // Test delete
        storage.delete(test_path).await.unwrap();
        let read_result = storage.read(test_path).await;
        assert!(read_result.is_err());

        // Verify delete doesn't fail for non-existent paths
        storage.delete(test_path).await.unwrap();
    }

    #[tokio::test]
    async fn test_memory_writer_round_trip() {
        let storage = MemoryStorage::new_isolated();
        let path = "memory://writer/streaming.bin";
        {
            let mut writer = storage.writer(path).await.unwrap();
            writer.write(Bytes::from("hello, ")).await.unwrap();
            writer.write(Bytes::from("world!")).await.unwrap();
            writer.close().await.unwrap();
        }
        let data = storage.read(path).await.unwrap().bytes;
        assert_eq!(data, Bytes::from("hello, world!"));
    }

    #[tokio::test]
    async fn test_memory_writer_close_twice_errors() {
        let storage = MemoryStorage::new_isolated();
        let path = "memory://writer/twice.bin";
        let mut writer = storage.writer(path).await.unwrap();
        writer.write(Bytes::from("data")).await.unwrap();
        writer.close().await.unwrap();
        assert!(writer.close().await.is_err());
    }

    #[tokio::test]
    async fn test_memory_writer_write_after_close_errors() {
        let storage = MemoryStorage::new_isolated();
        let path = "memory://writer/after_close.bin";
        let mut writer = storage.writer(path).await.unwrap();
        writer.write(Bytes::from("data")).await.unwrap();
        writer.close().await.unwrap();
        assert!(writer.write(Bytes::from("more")).await.is_err());
    }

    #[tokio::test]
    async fn test_memory_storage_without_prefix() {
        let storage = MemoryStorage::new();

        // Test without memory:// prefix
        let test_path = "test/file_no_prefix.txt";
        let test_data = Bytes::from("Hello without prefix!");

        storage.write(test_path, test_data.clone()).await.unwrap();
        let read_data = storage.read(test_path).await.unwrap().bytes;
        assert_eq!(test_data, read_data);
    }

    #[tokio::test]
    async fn test_memory_storage_batch_delete() {
        let storage = MemoryStorage::new();

        // Create multiple test files
        let test_paths: Vec<String> = vec![
            "memory://test/file1.txt".to_string(),
            "memory://test/file2.txt".to_string(),
            "memory://test/file3.txt".to_string(),
        ];

        let test_data = Bytes::from("Test data");

        // Write all files
        for path in &test_paths {
            storage.write(path, test_data.clone()).await.unwrap();
        }

        // Batch delete
        storage.delete_batch(&test_paths).await.unwrap();

        // Verify files are deleted
        for path in &test_paths {
            let read_result = storage.read(path).await;
            assert!(read_result.is_err());
        }
    }

    #[tokio::test]
    async fn test_memory_storage_list() {
        let storage = MemoryStorage::new();

        // Create test files
        let test_files = vec![
            ("memory://data/subdir/file1.txt", "content1"),
            ("memory://data/subdir/file2.txt", "content2"),
            ("memory://data/other/file3.txt", "content3"),
        ];

        // Write all files
        for (path, content) in &test_files {
            storage
                .write(path, Bytes::from(content.as_bytes()))
                .await
                .unwrap();
        }

        // List files in subdir
        let list_path = "memory://data/subdir";
        let mut stream = storage.list(list_path, None).await.unwrap();

        let mut all_file_infos = Vec::new();
        while let Some(result) = stream.next().await {
            let file_infos = result.unwrap();
            all_file_infos.extend(file_infos);
        }

        assert_eq!(all_file_infos.len(), 2);

        // Verify the locations match our test files
        let location_strings: Vec<String> = all_file_infos
            .iter()
            .map(|file_info| file_info.location().to_string())
            .collect();

        assert!(location_strings.contains(&"memory://data/subdir/file1.txt".to_string()));
        assert!(location_strings.contains(&"memory://data/subdir/file2.txt".to_string()));
        assert!(!location_strings.contains(&"memory://data/other/file3.txt".to_string()));
    }

    #[tokio::test]
    async fn test_memory_storage_remove_all() {
        let storage = MemoryStorage::new();

        // Create test files
        let test_files = vec![
            ("memory://data/subdir/file1.txt", "content1"),
            ("memory://data/subdir/file2.txt", "content2"),
            ("memory://data/subdir/nested/file3.txt", "content3"),
            ("memory://data/other/file4.txt", "content4"),
        ];

        // Write all files
        for (path, content) in &test_files {
            storage
                .write(path, Bytes::from(content.as_bytes()))
                .await
                .unwrap();
        }

        // Remove all files in subdir
        storage.remove_all("memory://data/subdir").await.unwrap();

        // Verify subdir files are deleted
        for (path, _) in &test_files[0..3] {
            let read_result = storage.read(path).await;
            assert!(read_result.is_err());
        }

        // Verify other files still exist
        let read_result = storage.read("memory://data/other/file4.txt").await;
        assert!(read_result.is_ok());
    }

    #[tokio::test]
    async fn test_memory_storage_with_data() {
        let mut initial_data = HashMap::new();
        initial_data.insert("test/file.txt".to_string(), Bytes::from("initial content"));
        initial_data.insert(
            "test/file2.txt".to_string(),
            Bytes::from("initial content 2"),
        );

        let storage = MemoryStorage::with_data(initial_data);

        // Verify initial data is accessible
        let content = storage.read("test/file.txt").await.unwrap().bytes;
        assert_eq!(content, Bytes::from("initial content"));

        let content2 = storage.read("test/file2.txt").await.unwrap().bytes;
        assert_eq!(content2, Bytes::from("initial content 2"));

        // Verify we can add more data
        storage
            .write("test/file3.txt", Bytes::from("new content"))
            .await
            .unwrap();

        let content3 = storage.read("test/file3.txt").await.unwrap().bytes;
        assert_eq!(content3, Bytes::from("new content"));

        // Check length
        assert_eq!(storage.len().await, 3);
        assert!(!storage.is_empty().await);
    }

    #[tokio::test]
    async fn test_memory_storage_clear() {
        let storage = MemoryStorage::new();

        // Add some data
        storage
            .write("test/file1.txt", Bytes::from("content1"))
            .await
            .unwrap();
        storage
            .write("test/file2.txt", Bytes::from("content2"))
            .await
            .unwrap();

        assert_eq!(storage.len().await, 2);
        assert!(!storage.is_empty().await);

        // Clear all data
        storage.clear().await;

        assert_eq!(storage.len().await, 0);
        assert!(storage.is_empty().await);

        // Verify data is gone
        let read_result = storage.read("test/file1.txt").await;
        assert!(read_result.is_err());
    }

    #[tokio::test]
    async fn test_memory_versions_after_overwrite_and_delete() {
        let storage = MemoryStorage::new_isolated();
        let path = "memory://versions/dir/obj";
        let first = storage.write(path, Bytes::from("first")).await.unwrap();
        let second = storage.write(path, Bytes::from("second")).await.unwrap();
        storage.delete(path).await.unwrap();

        let first_version = first.version.clone().unwrap();
        let second_version = second.version.clone().unwrap();
        assert_ne!(first_version, second_version);
        assert_eq!(
            first.e_tag.as_deref(),
            Some("\"8b04d5e3775d298e78455efc5ca404d5\"")
        );

        let entries = all_versions(&storage, "memory://versions/dir").await;
        assert_eq!(entries.len(), 3);
        let ObjectVersionEntry::DeleteMarker(marker) = &entries[0] else {
            panic!("expected the delete marker first, got {:?}", entries[0]);
        };
        assert!(marker.is_latest);
        let ObjectVersionEntry::Version(newer) = &entries[1] else {
            panic!("expected a version, got {:?}", entries[1]);
        };
        let ObjectVersionEntry::Version(older) = &entries[2] else {
            panic!("expected a version, got {:?}", entries[2]);
        };
        assert_eq!(
            (
                newer.version.as_str(),
                newer.e_tag.as_ref(),
                newer.size,
                newer.is_latest
            ),
            (
                second_version.as_str(),
                second.e_tag.as_ref(),
                Some(6),
                false
            )
        );
        assert_eq!(
            (
                older.version.as_str(),
                older.e_tag.as_ref(),
                older.size,
                older.is_latest
            ),
            (first_version.as_str(), first.e_tag.as_ref(), Some(5), false)
        );
        assert_eq!(older.location.as_str(), path);

        // The key reads as absent, its versions stay readable.
        let current = storage.read(path).await.unwrap_err();
        assert!(
            matches!(&current, ReadError::IOError(e) if e.kind() == ErrorKind::NotFound),
            "{current}"
        );
        let read = storage.read_version(path, &first_version).await.unwrap();
        assert_eq!(read.bytes, Bytes::from("first"));
        assert_eq!(read.info.version(), Some(first_version.as_str()));
        assert_eq!(
            storage
                .read_range_version(path, &second_version, 1..4)
                .await
                .unwrap(),
            Bytes::from("eco")
        );
        assert_eq!(storage.len().await, 0);
    }

    #[tokio::test]
    async fn test_memory_reading_a_delete_marker_is_not_found() {
        let storage = MemoryStorage::new_isolated();
        let path = "memory://versions/marker";
        storage.write(path, Bytes::from("x")).await.unwrap();
        storage.delete(path).await.unwrap();
        let entries = all_versions(&storage, "memory://versions").await;
        let marker = entries[0].version().to_string();

        let error = storage.read_version(path, &marker).await.unwrap_err();
        assert!(
            matches!(&error, ReadError::IOError(e) if e.kind() == ErrorKind::NotFound),
            "{error}"
        );
    }

    #[tokio::test]
    async fn test_memory_list_reports_e_tag_but_no_version() {
        let storage = MemoryStorage::new_isolated();
        let outcome = storage
            .write("memory://listing/a", Bytes::from("a"))
            .await
            .unwrap();
        let mut stream = storage.list("memory://listing", None).await.unwrap();
        let page = stream.next().await.unwrap().unwrap();
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].e_tag(), outcome.e_tag.as_deref());
        assert_eq!(page[0].version(), None);

        let version = outcome
            .version
            .clone()
            .expect("memory storage versions every write");
        let metadata = storage.metadata("memory://listing/a").await.unwrap();
        assert_eq!(metadata.version(), Some(version.as_str()));
    }

    #[tokio::test]
    async fn test_memory_writer_close_returns_the_version_it_wrote() {
        let storage = MemoryStorage::new_isolated();
        let path = "memory://writer/outcome.bin";
        let mut writer = storage.writer(path).await.unwrap();
        writer.write(Bytes::from("streamed")).await.unwrap();
        let outcome = writer.close().await.unwrap();
        let version = outcome
            .version
            .clone()
            .expect("memory storage versions every write");

        let read = storage.read(path).await.unwrap();
        assert_eq!(read.info.version(), Some(version.as_str()));
        assert_eq!(read.info.e_tag(), outcome.e_tag.as_deref());
    }

    #[tokio::test]
    async fn test_memory_tags_address_one_version() {
        let storage = MemoryStorage::new_isolated();
        let path = "memory://tags/obj";
        let first = storage
            .write(path, Bytes::from("first"))
            .await
            .unwrap()
            .version
            .unwrap();
        let second = storage
            .write(path, Bytes::from("second"))
            .await
            .unwrap()
            .version
            .unwrap();
        let tags = ObjectTags::from([("state".to_string(), "expired".to_string())]);

        storage.put_tags(path, &first, &tags).await.unwrap();
        assert_eq!(storage.get_tags(path, &first).await.unwrap(), tags);
        assert_eq!(
            storage.get_tags(path, &second).await.unwrap(),
            ObjectTags::new()
        );

        storage.remove_tags(path, &first).await.unwrap();
        assert_eq!(
            storage.get_tags(path, &first).await.unwrap(),
            ObjectTags::new()
        );

        let unknown = uuid::Uuid::now_v7().to_string();
        let error = storage.put_tags(path, &unknown, &tags).await.unwrap_err();
        assert!(
            matches!(&error, WriteError::IOError(e) if e.kind() == ErrorKind::NotFound),
            "{error}"
        );
    }

    #[tokio::test]
    async fn test_memory_remove_all_marks_only_live_keys() {
        let storage = MemoryStorage::new_isolated();
        storage
            .write("memory://tree/a", Bytes::from("a"))
            .await
            .unwrap();
        storage
            .write("memory://tree/b", Bytes::from("b"))
            .await
            .unwrap();
        storage.delete("memory://tree/b").await.unwrap();
        storage.remove_all("memory://tree").await.unwrap();

        let entries = all_versions(&storage, "memory://tree").await;
        let kinds: Vec<(&str, bool)> = entries
            .iter()
            .map(|entry| {
                (
                    entry.location().as_str(),
                    matches!(entry, ObjectVersionEntry::DeleteMarker(_)),
                )
            })
            .collect();
        assert_eq!(
            kinds,
            vec![
                ("memory://tree/a", true),
                ("memory://tree/a", false),
                ("memory://tree/b", true),
                ("memory://tree/b", false),
            ]
        );
    }

    #[test]
    fn listing_pages_hold_at_most_a_thousand_entries() {
        let items: Vec<usize> = (0..2500).collect();
        let sizes = |page_size| {
            pages(&items, page_size)
                .iter()
                .map(Vec::len)
                .collect::<Vec<_>>()
        };
        assert_eq!(sizes(Some(5000)), vec![1000, 1000, 500]);
        assert_eq!(sizes(Some(0)), vec![1000, 1000, 500]);
        assert_eq!(sizes(None), vec![1000, 1000, 500]);
        assert_eq!(sizes(Some(1200)), vec![1000, 1000, 500]);
        assert_eq!(sizes(Some(999)), vec![999, 999, 502]);
    }

    #[tokio::test]
    async fn a_malformed_version_id_is_refused() {
        let storage = MemoryStorage::new_isolated();
        let path = "memory://versions/obj";
        storage.write(path, Bytes::from("data")).await.unwrap();
        let Err(ReadError::IOError(error)) = storage.read_version(path, "not-a-version").await
        else {
            panic!("expected the version id to be refused");
        };
        assert_eq!(error.kind(), ErrorKind::ConditionNotMatch);
        assert_eq!(
            error.reason(),
            "`not-a-version` is not a memory storage version id"
        );
        // A well-formed id the key does not have is not found.
        let unknown = uuid::Uuid::now_v7().to_string();
        let Err(ReadError::IOError(error)) = storage.read_version(path, &unknown).await else {
            panic!("expected the version to be missing");
        };
        assert_eq!(error.kind(), ErrorKind::NotFound);
    }

    #[tokio::test]
    async fn a_tag_set_beyond_the_limits_is_refused() {
        let storage = MemoryStorage::new_isolated();
        let path = "memory://tags/limits";
        let version = storage
            .write(path, Bytes::from("data"))
            .await
            .unwrap()
            .version
            .expect("memory storage versions every write");
        let tags = ObjectTags::from([("aws:reserved".to_string(), "value".to_string())]);
        let Err(WriteError::IOError(error)) = storage.put_tags(path, &version, &tags).await else {
            panic!("expected the tag set to be refused");
        };
        assert_eq!(error.kind(), ErrorKind::ConditionNotMatch);
        assert_eq!(
            error.reason(),
            "Tag key `aws:reserved` is in the reserved `aws:` namespace"
        );
        assert_eq!(
            storage.get_tags(path, &version).await.unwrap(),
            ObjectTags::new()
        );
    }
}
