use std::{future::Future, sync::LazyLock};

use bytes::Bytes;
use futures::StreamExt;
use lakekeeper_io::{BatchDeleteResult, LakekeeperStorage, StorageBackend};
use tempfile::TempDir;
use tokio::{
    runtime::Runtime,
    time::{sleep, Duration},
};

// we need to use a shared runtime since the static client is shared between tests here
// and tokio::test creates a new runtime for each test. For now, we only encounter the
// issue here, eventually, we may want to move this to a proc macro like tokio::test or
// sqlx::test
static COMMON_RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to start Tokio runtime")
});

#[track_caller]
pub(crate) fn execute_in_common_runtime<F: Future>(f: F) -> F::Output {
    {
        COMMON_RUNTIME.block_on(f)
    }
}

/// Macro to generate parameterized tests for all available storage backends
macro_rules! test_all_storages {
    ($test_name:ident, $test_fn:ident) => {
        paste::paste! {
            #[cfg(feature = "storage-in-memory")]
            #[test]
            fn [<$test_name _memory>]() -> anyhow::Result<()> {
                execute_in_common_runtime(async {
                    let storage = StorageBackend::Memory(lakekeeper_io::memory::MemoryStorage::new());
                    let config = TestConfig {
                        base_path: format!("memory://test-{}", uuid::Uuid::new_v4()),
                        temp_dir: None,
                    };
                    $test_fn(&storage, &config).await
                })
            }

            #[cfg(feature = "storage-s3")]
            #[test]
            fn [<$test_name _s3>]() -> anyhow::Result<()> {
                execute_in_common_runtime(async {
                    let bucket = std::env::var("LAKEKEEPER_TEST__S3_BUCKET")
                        .map_err(|_| anyhow::anyhow!("LAKEKEEPER_TEST__S3_BUCKET not set"))?;
                    let region = std::env::var("LAKEKEEPER_TEST__S3_REGION")
                        .map_err(|_| anyhow::anyhow!("LAKEKEEPER_TEST__S3_REGION not set"))?;
                    let access_key = std::env::var("LAKEKEEPER_TEST__S3_ACCESS_KEY")
                        .map_err(|_| anyhow::anyhow!("LAKEKEEPER_TEST__S3_ACCESS_KEY not set"))?;
                    let secret_key = std::env::var("LAKEKEEPER_TEST__S3_SECRET_KEY")
                        .map_err(|_| anyhow::anyhow!("LAKEKEEPER_TEST__S3_SECRET_KEY not set"))?;
                    let endpoint = std::env::var("LAKEKEEPER_TEST__S3_ENDPOINT").ok();

                    let s3_settings = lakekeeper_io::s3::S3Settings {
                        assume_role_arn: None,
                        endpoint: endpoint.map(|e| e.parse().unwrap()),
                        region,
                        path_style_access: Some(true),
                        aws_kms_key_arn: None,
                    };
                    let s3_auth = lakekeeper_io::s3::S3Auth::AccessKey(
                        lakekeeper_io::s3::S3AccessKeyAuth {
                            aws_access_key_id: access_key,
                            aws_secret_access_key: secret_key,
                            external_id: None,
                        },
                    );

                    let storage = StorageBackend::S3(s3_settings.get_s3_client(Some(&s3_auth)).await);

                    let base_path = format!("s3://{}/integration-tests/{}", bucket, uuid::Uuid::new_v4());
                    let config = TestConfig {
                        base_path,
                        temp_dir: None,
                    };
                    $test_fn(&storage, &config).await
                })
            }
        }
    };
}

/// Test configuration for different storage backends
#[derive(Debug)]
pub struct TestConfig {
    /// Base path prefix for all test operations
    pub base_path: String,
    /// Optional cleanup directory (for file storage)
    pub temp_dir: Option<TempDir>,
}

impl TestConfig {
    /// Generate a unique test path with the given suffix
    pub fn test_path(&self, suffix: &str) -> String {
        let uuid = uuid::Uuid::new_v4();
        format!("{}/test-{}/{}", self.base_path, uuid, suffix)
    }

    /// Generate a unique test directory path
    pub fn test_dir_path(&self, suffix: &str) -> String {
        let uuid = uuid::Uuid::new_v4();
        format!("{}/test-dir-{}/{}/", self.base_path, uuid, suffix)
    }
}

// Generate parameterized tests for all storage backends
test_all_storages!(test_write_read, test_write_read_impl);
test_all_storages!(test_multiple_files, test_multiple_files_impl);
test_all_storages!(test_delete, test_delete_impl);
test_all_storages!(test_batch_delete, test_batch_delete_impl);
test_all_storages!(test_list, test_list_impl);
test_all_storages!(test_remove_all, test_remove_all_impl);
test_all_storages!(test_empty_files, test_empty_files_impl);
test_all_storages!(test_large_files, test_large_files_impl);
test_all_storages!(test_special_characters, test_special_characters_impl);
test_all_storages!(test_error_handling, test_error_handling_impl);

/// Basic write and read test implementation
async fn test_write_read_impl(storage: &StorageBackend, config: &TestConfig) -> anyhow::Result<()> {
    let test_path = config.test_path("basic-write-read.txt");
    let test_data = Bytes::from("Hello, World! This is a test file.");

    // Write data
    storage.write(&test_path, test_data.clone()).await?;

    // Read data back
    let read_data = storage.read(&test_path).await?;
    assert_eq!(test_data, read_data, "Read data should match written data");

    // Clean up
    storage.delete(&test_path).await?;

    Ok(())
}

/// Test writing multiple files and reading them back implementation
async fn test_multiple_files_impl(
    storage: &StorageBackend,
    config: &TestConfig,
) -> anyhow::Result<()> {
    let test_files = vec![
        ("file1.txt", "Content of file 1"),
        ("file2.txt", "Content of file 2"),
        ("subdir/file3.txt", "Content of file 3 in subdirectory"),
    ];

    let mut written_paths = Vec::new();

    // Write all files
    for (filename, content) in &test_files {
        let path = config.test_path(filename);
        storage.write(&path, Bytes::from(*content)).await?;
        written_paths.push(path);
    }

    // Read all files back and verify content
    for (i, (_, expected_content)) in test_files.iter().enumerate() {
        let read_data = storage.read(&written_paths[i]).await?;
        let read_content = String::from_utf8(read_data.to_vec())?;
        assert_eq!(read_content, *expected_content);
    }

    // Clean up
    for path in written_paths {
        storage.delete(&path).await?;
    }

    Ok(())
}

/// Test delete operations implementation
async fn test_delete_impl(storage: &StorageBackend, config: &TestConfig) -> anyhow::Result<()> {
    let test_path = config.test_path("delete-test.txt");
    let test_data = Bytes::from("This file will be deleted");

    // Write file
    storage.write(&test_path, test_data).await?;

    // Verify file exists
    storage.read(&test_path).await?;

    // Delete file
    storage.delete(&test_path).await?;

    // Verify file is deleted (should return an error)
    let read_result = storage.read(&test_path).await;
    assert!(read_result.is_err(), "Reading deleted file should fail");

    // Test deleting non-existent file (should not error)
    storage.delete(&test_path).await?;

    Ok(())
}

/// Test batch delete operations implementation
async fn test_batch_delete_impl(
    storage: &StorageBackend,
    config: &TestConfig,
) -> anyhow::Result<()> {
    let test_files = vec![
        "batch-delete-1.txt",
        "batch-delete-2.txt",
        "batch-delete-3.txt",
        "subdir/batch-delete-4.txt",
    ];

    let mut written_paths = Vec::new();

    // Write all files
    for filename in &test_files {
        let path = config.test_path(filename);
        storage
            .write(&path, Bytes::from(format!("Content of {filename}")))
            .await?;
        written_paths.push(path);
    }

    // Batch delete all files
    let result = storage.delete_batch(&written_paths).await?;

    match result {
        BatchDeleteResult::AllSuccessful(successful_paths) => {
            assert_eq!(successful_paths.len(), test_files.len());
        }
        BatchDeleteResult::PartialFailure {
            successful_paths,
            errors,
        } => {
            // Some storage implementations might have partial failures
            println!(
                "Partial failure: {} successful, {} errors",
                successful_paths.len(),
                errors.len()
            );
            for error in &errors {
                println!("Batch delete error: {error}");
            }
        }
    }

    // Verify all files are deleted
    for path in &written_paths {
        let read_result = storage.read(path).await;
        assert!(read_result.is_err(), "File should be deleted: {path}");
    }

    Ok(())
}

/// Test list operations implementation
async fn test_list_impl(storage: &StorageBackend, config: &TestConfig) -> anyhow::Result<()> {
    let base_dir = config.test_dir_path("list-test");
    let test_files = vec![
        "file1.txt",
        "file2.txt",
        "subdir/file3.txt",
        "subdir/nested/file4.txt",
        "other/file5.txt",
    ];

    let mut written_paths = Vec::new();

    // Write test files
    for filename in &test_files {
        let path = format!("{base_dir}{filename}");
        storage
            .write(&path, Bytes::from(format!("Content of {filename}")))
            .await?;
        written_paths.push(path);
    }

    // List all files in the base directory
    let mut list_stream = storage.list(&base_dir, None).await?;
    let mut all_locations = Vec::new();

    while let Some(result) = list_stream.next().await {
        let locations = result?;
        all_locations.extend(locations);
    }

    // Debug: print what we actually found
    println!(
        "Expected {} files, found {} files:",
        test_files.len(),
        all_locations.len()
    );
    for location in &all_locations {
        println!("  Found: {location}");
    }
    for path in &written_paths {
        println!("  Expected: {path}");
    }

    let min_expected_items = test_files.len();

    // Should have at least the minimum expected items
    assert!(
        all_locations.len() >= min_expected_items,
        "Should list at least {} items, found {}",
        min_expected_items,
        all_locations.len()
    );

    // Verify that we can find our test files in the results
    let location_strings: Vec<String> = all_locations.iter().map(ToString::to_string).collect();

    for expected_path in &written_paths {
        assert!(
            location_strings.iter().any(|loc| loc == expected_path),
            "Should find path {expected_path} in list results"
        );
    }

    // Test listing with a more specific prefix (subdir)
    let subdir_path = format!("{base_dir}subdir/");
    let mut subdir_stream = storage.list(&subdir_path, None).await?;
    let mut subdir_locations = Vec::new();

    while let Some(result) = subdir_stream.next().await {
        let locations = result?;
        subdir_locations.extend(locations);
    }

    // Should have files in subdir (file3.txt and nested/file4.txt)
    assert!(
        subdir_locations.len() >= 2,
        "Should find at least 2 files in subdir, found {}",
        subdir_locations.len()
    );

    // Clean up
    for path in written_paths {
        storage.delete(&path).await?;
    }

    Ok(())
}

/// Test remove_all (recursive delete) operations implementation
async fn test_remove_all_impl(storage: &StorageBackend, config: &TestConfig) -> anyhow::Result<()> {
    let base_dir = config.test_dir_path("remove-all-test");
    let test_files = vec![
        "file1.txt",
        "file2.txt",
        "subdir/file3.txt",
        "subdir/nested/file4.txt",
        "subdir/nested/deep/file5.txt",
    ];

    let mut written_paths = Vec::new();

    // Write test files
    for filename in &test_files {
        let path = format!("{base_dir}{filename}");
        storage
            .write(&path, Bytes::from(format!("Content of {filename}")))
            .await?;
        written_paths.push(path);
    }

    // Verify files exist
    for path in &written_paths {
        storage.read(path).await?;
    }

    // Remove all files in the directory
    storage.remove_all(&base_dir).await?;

    // Wait a bit for eventual consistency (important for S3)
    sleep(Duration::from_millis(100)).await;

    // Verify all files are deleted
    for path in &written_paths {
        let read_result = storage.read(path).await;
        assert!(
            read_result.is_err(),
            "File should be deleted after remove_all: {path}"
        );
    }

    Ok(())
}

/// Test with empty files implementation
async fn test_empty_files_impl(
    storage: &StorageBackend,
    config: &TestConfig,
) -> anyhow::Result<()> {
    let test_path = config.test_path("empty-file.txt");
    let empty_data = Bytes::new();

    // Write empty file
    storage.write(&test_path, empty_data.clone()).await?;

    // Read empty file back
    let read_data = storage.read(&test_path).await?;
    assert_eq!(read_data.len(), 0, "Empty file should have zero length");
    assert_eq!(read_data, empty_data, "Empty file content should match");

    // Clean up
    storage.delete(&test_path).await?;

    Ok(())
}

/// Test with large files (to test streaming behavior) implementation
async fn test_large_files_impl(
    storage: &StorageBackend,
    config: &TestConfig,
) -> anyhow::Result<()> {
    let test_path = config.test_path("large-file.txt");

    // Create a 1MB file
    let large_data = Bytes::from(vec![b'A'; 1024 * 1024]);

    // Write large file
    storage.write(&test_path, large_data.clone()).await?;

    // Read large file back
    let read_data = storage.read(&test_path).await?;
    assert_eq!(
        read_data.len(),
        large_data.len(),
        "Large file size should match"
    );
    assert_eq!(read_data, large_data, "Large file content should match");

    // Clean up
    storage.delete(&test_path).await?;

    Ok(())
}

/// Test operations with special characters in paths implementation
async fn test_special_characters_impl(
    storage: &StorageBackend,
    config: &TestConfig,
) -> anyhow::Result<()> {
    let special_files = vec![
        "file with spaces.txt",
        "file-with-dashes.txt",
        "file_with_underscores.txt",
        "file.with.dots.txt",
        "αβγ-unicode.txt", // Unicode characters
    ];

    let mut written_paths = Vec::new();

    // Write files with special characters
    for filename in &special_files {
        let path = config.test_path(filename);
        storage
            .write(&path, Bytes::from(format!("Content of {filename}")))
            .await?;
        written_paths.push(path);
    }

    // Read all files back
    for (i, filename) in special_files.iter().enumerate() {
        let read_data = storage.read(&written_paths[i]).await?;
        let read_content = String::from_utf8(read_data.to_vec())?;
        assert_eq!(read_content, format!("Content of {filename}"));
    }

    // Clean up
    for path in written_paths {
        storage.delete(&path).await?;
    }

    Ok(())
}

/// Test error handling for invalid paths implementation
async fn test_error_handling_impl(
    storage: &StorageBackend,
    config: &TestConfig,
) -> anyhow::Result<()> {
    // Test reading non-existent file using the correct scheme for this storage backend
    let non_existent_path = config.test_path("this/file/does/not/exist.txt");
    let read_result = storage.read(&non_existent_path).await;
    assert!(
        read_result.is_err(),
        "Reading non-existent file should fail"
    );

    // Test batch delete with non-existent files using the correct scheme
    let non_existent_paths = vec![
        config.test_path("does/not/exist1.txt"),
        config.test_path("does/not/exist2.txt"),
    ];
    let batch_result = storage.delete_batch(non_existent_paths).await?;

    // Different storage backends handle non-existent files differently
    // Memory and File storage typically succeed, S3 might have different behavior
    match batch_result {
        BatchDeleteResult::AllSuccessful(_) => {
            // This is fine - some storage backends treat deleting non-existent files as success
        }
        BatchDeleteResult::PartialFailure { .. } => {
            // This is also acceptable behavior
        }
    }

    Ok(())
}

// Generate retry tests for all storage backends
test_all_storages!(test_retry_functionality, test_retry_functionality_impl);

/// Test specifically for retry functionality implementation
async fn test_retry_functionality_impl(
    storage: &StorageBackend,
    config: &TestConfig,
) -> anyhow::Result<()> {
    use std::time::Duration;

    use lakekeeper_io::RetryConfig;
    use tryhard::backoff_strategies::ExponentialBackoff;

    let test_path = config.test_path("retry-test.txt");
    let test_data = Bytes::from("Retry test data");

    // Test write with retry
    let retry_config = RetryConfig::new(3, ExponentialBackoff::new(Duration::from_millis(100)))
        .with_max_delay(Duration::from_secs(1));

    storage
        .write_with_retry(&test_path, test_data.clone(), retry_config)
        .await?;

    // Test read with retry
    let retry_config = RetryConfig::new(3, ExponentialBackoff::new(Duration::from_millis(100)))
        .with_max_delay(Duration::from_secs(1));

    let read_data = storage.read_with_retry(&test_path, retry_config).await?;
    assert_eq!(read_data, test_data);

    // Test delete with retry
    let retry_config = RetryConfig::new(3, ExponentialBackoff::new(Duration::from_millis(100)))
        .with_max_delay(Duration::from_secs(1));

    storage.delete_with_retry(&test_path, retry_config).await?;

    Ok(())
}
