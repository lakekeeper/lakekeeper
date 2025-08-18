use std::{cell::RefCell, collections::HashMap, sync::Arc};

use bytes::Bytes;
use futures::stream::{self, BoxStream, StreamExt};
use tokio::sync::RwLock;

use crate::{
    error::ErrorKind, DeleteBatchError, DeleteError, IOError, InvalidLocationError,
    LakekeeperStorage, Location, ReadError, WriteError,
};

/// In-memory storage implementation for testing and development purposes.
/// All data is stored in memory and persists across instances within the same thread.
#[derive(Debug, Clone)]
pub struct MemoryStorage {
    data: Arc<RwLock<HashMap<String, Bytes>>>,
    use_global_store: bool,
}

thread_local! {
    #[allow(clippy::type_complexity)]
    static GLOBAL_MEMORY_STORE: RefCell<Option<Arc<RwLock<HashMap<String, Bytes>>>>> = const { RefCell::new(None) };
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
        MemoryStorage {
            data: Arc::new(RwLock::new(data)),
            use_global_store: false,
        }
    }

    /// Get the number of objects stored in memory.
    pub async fn len(&self) -> usize {
        self.data.read().await.len()
    }

    /// Check if the storage is empty.
    pub async fn is_empty(&self) -> bool {
        self.data.read().await.is_empty()
    }

    /// Clear all data from storage.
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

impl LakekeeperStorage for MemoryStorage {
    async fn delete(&self, path: impl AsRef<str>) -> Result<(), DeleteError> {
        let path_str = path.as_ref();
        let key = normalize_memory_path(path_str)?;

        let mut data = self.data.write().await;
        data.remove(&key);
        // Memory storage treats deleting non-existent keys as success
        Ok(())
    }

    async fn delete_batch(
        &self,
        paths: impl IntoIterator<Item = impl AsRef<str>> + Send,
    ) -> Result<(), DeleteBatchError> {
        let paths: Vec<String> = paths.into_iter().map(|p| p.as_ref().to_string()).collect();

        for path_str in paths {
            match self.delete(&path_str).await {
                Ok(()) => {}
                Err(e) => {
                    return Err(DeleteBatchError::from(e));
                }
            }
        }

        Ok(())
    }

    async fn write(&self, path: impl AsRef<str>, bytes: Bytes) -> Result<(), WriteError> {
        let path_str = path.as_ref();
        let key = normalize_memory_path(path_str)?;

        let mut data = self.data.write().await;
        data.insert(key, bytes);
        Ok(())
    }

    async fn read(&self, path: impl AsRef<str>) -> Result<Bytes, ReadError> {
        let path_str = path.as_ref();
        let key = normalize_memory_path(path_str)?;

        let data = self.data.read().await;
        match data.get(&key) {
            Some(bytes) => Ok(bytes.clone()),
            None => Err(ReadError::IOError(IOError::new(
                ErrorKind::NotFound,
                "Object not found in memory storage",
                path_str.to_string(),
            ))),
        }
    }

    async fn read_single(&self, path: impl AsRef<str> + Send) -> Result<Bytes, ReadError> {
        self.read(path).await
    }

    async fn list(
        &self,
        path: impl AsRef<str> + Send,
        page_size: Option<usize>,
    ) -> Result<BoxStream<'_, Result<Vec<Location>, IOError>>, InvalidLocationError> {
        let path_str = path.as_ref();
        let prefix = if path_str.ends_with('/') {
            normalize_memory_path(path_str)?
        } else {
            format!("{}/", normalize_memory_path(path_str)?)
        };

        let data = self.data.read().await;
        let mut matching_keys: Vec<String> = data
            .keys()
            .filter(|key| key.starts_with(&prefix))
            .cloned()
            .collect();

        // Sort for consistent ordering
        matching_keys.sort();

        let page_size = page_size.unwrap_or(1000);

        let mut all_locations = Vec::new();
        for key in matching_keys {
            let location_str = format!("{MEMORY_PREFIX}{key}");
            match location_str.parse::<Location>() {
                Ok(location) => all_locations.push(location),
                Err(e) => {
                    let error = IOError::new(
                        ErrorKind::Unexpected,
                        format!("Failed to parse location: {e}"),
                        location_str,
                    )
                    .set_source(anyhow::anyhow!(e));
                    // Return an error in the stream
                    return Ok(stream::iter(std::iter::once(Err(error))).boxed());
                }
            }
        }

        // Convert to Vec<Vec<Location>> by chunking, then create an iterator over those chunks
        let chunks: Vec<Vec<Location>> = all_locations
            .chunks(page_size)
            .map(<[Location]>::to_vec)
            .collect();

        // Create a stream that returns pages of locations
        let stream = stream::iter(chunks.into_iter().map(Ok));

        Ok(stream.boxed())
    }

    async fn remove_all(&self, path: impl AsRef<str>) -> Result<(), DeleteError> {
        let path_str = path.as_ref();
        let prefix = if path_str.ends_with('/') {
            normalize_memory_path(path_str)?
        } else {
            format!("{}/", normalize_memory_path(path_str)?)
        };

        let mut data = self.data.write().await;
        let keys_to_remove: Vec<String> = data
            .keys()
            .filter(|key| key.starts_with(&prefix))
            .cloned()
            .collect();

        for key in keys_to_remove {
            data.remove(&key);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use bytes::Bytes;
    use futures::StreamExt;

    use crate::{memory::MemoryStorage, LakekeeperStorage};

    #[tokio::test]
    async fn test_memory_storage_basic_operations() {
        let storage = MemoryStorage::new();

        // Test write and read
        let test_path = "memory://test/file.txt";
        let test_data = Bytes::from("Hello, World!");

        storage.write(test_path, test_data.clone()).await.unwrap();
        let read_data = storage.read(test_path).await.unwrap();
        assert_eq!(test_data, read_data);

        // Test delete
        storage.delete(test_path).await.unwrap();
        let read_result = storage.read(test_path).await;
        assert!(read_result.is_err());

        // Verify delete doesn't fail for non-existent paths
        storage.delete(test_path).await.unwrap();
    }

    #[tokio::test]
    async fn test_memory_storage_without_prefix() {
        let storage = MemoryStorage::new();

        // Test without memory:// prefix
        let test_path = "test/file_no_prefix.txt";
        let test_data = Bytes::from("Hello without prefix!");

        storage.write(test_path, test_data.clone()).await.unwrap();
        let read_data = storage.read(test_path).await.unwrap();
        assert_eq!(test_data, read_data);
    }

    #[tokio::test]
    async fn test_memory_storage_batch_delete() {
        let storage = MemoryStorage::new();

        // Create multiple test files
        let test_paths = vec![
            "memory://test/file1.txt",
            "memory://test/file2.txt",
            "memory://test/file3.txt",
        ];

        let test_data = Bytes::from("Test data");

        // Write all files
        for path in &test_paths {
            storage.write(path, test_data.clone()).await.unwrap();
        }

        // Batch delete
        storage.delete_batch(test_paths.clone()).await.unwrap();

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

        let mut all_locations = Vec::new();
        while let Some(result) = stream.next().await {
            let locations = result.unwrap();
            all_locations.extend(locations);
        }

        assert_eq!(all_locations.len(), 2);

        // Verify the locations match our test files
        let location_strings: Vec<String> = all_locations
            .iter()
            .map(std::string::ToString::to_string)
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
        let content = storage.read("test/file.txt").await.unwrap();
        assert_eq!(content, Bytes::from("initial content"));

        let content2 = storage.read("test/file2.txt").await.unwrap();
        assert_eq!(content2, Bytes::from("initial content 2"));

        // Verify we can add more data
        storage
            .write("test/file3.txt", Bytes::from("new content"))
            .await
            .unwrap();

        let content3 = storage.read("test/file3.txt").await.unwrap();
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
}
