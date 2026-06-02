use core::panic;
use std::{future::Future, sync::LazyLock};

use bytes::Bytes;
use futures::StreamExt;
use lakekeeper_io::{
    ErrorKind, LakekeeperStorage, ReadError, StorageBackend, execute_with_parallelism,
};
use tokio::{
    runtime::Runtime,
    time::{Duration, Instant, sleep},
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
    COMMON_RUNTIME.block_on(f)
}

#[cfg(feature = "storage-in-memory")]
/// Storage backend initialization functions
async fn create_memory_storage() -> anyhow::Result<(StorageBackend, TestConfig)> {
    let storage = StorageBackend::Memory(lakekeeper_io::memory::MemoryStorage::new());
    let config = TestConfig {
        base_path: format!("memory://test-{}", uuid::Uuid::new_v4()),
    };
    Ok((storage, config))
}

#[cfg(feature = "storage-s3")]
async fn create_s3_storage() -> anyhow::Result<(StorageBackend, TestConfig)> {
    let bucket = std::env::var("LAKEKEEPER_TEST__S3_BUCKET")
        .map_err(|_| anyhow::anyhow!("LAKEKEEPER_TEST__S3_BUCKET not set"))?;
    let region = std::env::var("LAKEKEEPER_TEST__S3_REGION")
        .map_err(|_| anyhow::anyhow!("LAKEKEEPER_TEST__S3_REGION not set"))?;
    let access_key = std::env::var("LAKEKEEPER_TEST__S3_ACCESS_KEY")
        .map_err(|_| anyhow::anyhow!("LAKEKEEPER_TEST__S3_ACCESS_KEY not set"))?;
    let secret_key = std::env::var("LAKEKEEPER_TEST__S3_SECRET_KEY")
        .map_err(|_| anyhow::anyhow!("LAKEKEEPER_TEST__S3_SECRET_KEY not set"))?;
    let endpoint = std::env::var("LAKEKEEPER_TEST__S3_ENDPOINT").ok();

    let s3_settings = lakekeeper_io::s3::S3Settings::builder()
        .endpoint(
            endpoint
                .map(|e| e.parse())
                .transpose()
                .map_err(|e| anyhow::anyhow!("Invalid S3 endpoint URL: {e}"))?,
        )
        .region(region)
        .path_style_access(Some(true))
        .build();
    let s3_auth = lakekeeper_io::s3::S3Auth::AccessKey(lakekeeper_io::s3::S3AccessKeyAuth {
        aws_access_key_id: access_key,
        aws_secret_access_key: secret_key,
        aws_session_token: None,
        external_id: None,
    });

    let storage = StorageBackend::S3(s3_settings.get_storage_client(Some(&s3_auth)).await);
    let base_path = format!(
        "s3://{}/lakekeeper-io-integration-tests/{}",
        bucket,
        uuid::Uuid::new_v4()
    );
    let config = TestConfig { base_path };

    Ok((storage, config))
}

#[cfg(feature = "storage-adls")]
async fn create_adls_storage() -> anyhow::Result<(StorageBackend, TestConfig)> {
    let client_id = std::env::var("LAKEKEEPER_TEST__AZURE_CLIENT_ID")
        .map_err(|_| anyhow::anyhow!("LAKEKEEPER_TEST__AZURE_CLIENT_ID not set"))?;
    let tenant_id = std::env::var("LAKEKEEPER_TEST__AZURE_TENANT_ID")
        .map_err(|_| anyhow::anyhow!("LAKEKEEPER_TEST__AZURE_TENANT_ID not set"))?;
    let client_secret = std::env::var("LAKEKEEPER_TEST__AZURE_CLIENT_SECRET")
        .map_err(|_| anyhow::anyhow!("LAKEKEEPER_TEST__AZURE_CLIENT_SECRET not set"))?;

    let account = std::env::var("LAKEKEEPER_TEST__AZURE_STORAGE_ACCOUNT_NAME")
        .map_err(|_| anyhow::anyhow!("LAKEKEEPER_TEST__AZURE_STORAGE_ACCOUNT_NAME not set"))?;
    let filesystem = std::env::var("LAKEKEEPER_TEST__AZURE_STORAGE_FILESYSTEM")
        .map_err(|_| anyhow::anyhow!("LAKEKEEPER_TEST__AZURE_STORAGE_FILESYSTEM not set"))?;

    let settings = lakekeeper_io::adls::AzureSettings {
        authority_host: None,
        cloud_location: lakekeeper_io::adls::CloudLocation::Public {
            account: account.clone(),
        },
    };
    let auth = lakekeeper_io::adls::AzureAuth::ClientCredentials(
        lakekeeper_io::adls::AzureClientCredentialsAuth {
            client_id,
            client_secret,
            tenant_id,
        },
    );

    let storage = StorageBackend::Adls(
        settings
            .get_storage_client(&auth)
            .await
            .map_err(|e| anyhow::anyhow!(e))?,
    );
    let base_path = format!(
        "abfss://{filesystem}@{account}.dfs.core.windows.net/lakekeeper-io-integration-tests/{}",
        uuid::Uuid::new_v4()
    );
    let config = TestConfig { base_path };

    Ok((storage, config))
}

#[cfg(feature = "storage-gcs")]
async fn create_gcs_storage(bucket_env_var: &str) -> anyhow::Result<(StorageBackend, TestConfig)> {
    let credential = std::env::var("LAKEKEEPER_TEST__GCS_CREDENTIAL")
        .map_err(|_| anyhow::anyhow!("LAKEKEEPER_TEST__GCS_CREDENTIAL not set"))?;
    let bucket =
        std::env::var(bucket_env_var).map_err(|_| anyhow::anyhow!("{bucket_env_var} not set"))?;

    let credential_file: lakekeeper_io::gcs::CredentialsFile = serde_json::from_str(&credential)
        .map_err(|e| anyhow::anyhow!("Failed to parse GCS credential file: {e}"))?;

    let settings = lakekeeper_io::gcs::GCSSettings {};
    let auth = lakekeeper_io::gcs::GcsAuth::CredentialsFile {
        file: credential_file,
    };

    let storage = StorageBackend::Gcs(
        settings
            .get_storage_client(&auth)
            .await
            .map_err(|e| anyhow::anyhow!(e))?,
    );
    let base_path = format!(
        "gs://{bucket}/lakekeeper-io-integration-tests/{}",
        uuid::Uuid::new_v4()
    );
    let config = TestConfig { base_path };

    Ok((storage, config))
}

/// Macro to generate parameterized tests for all available storage backends
macro_rules! test_all_storages {
    ($test_name:ident, $test_fn:ident) => {
        pastey::paste! {
            #[cfg(feature = "storage-in-memory")]
            #[test]
            fn [<$test_name _memory>]() -> anyhow::Result<()> {
                execute_in_common_runtime(async {
                    let (storage, config) = create_memory_storage().await?;
                    $test_fn(&storage, &config).await
                })
            }

            #[cfg(feature = "storage-s3")]
            #[test]
            fn [<$test_name _s3>]() -> anyhow::Result<()> {
                execute_in_common_runtime(async {
                    let (storage, config) = create_s3_storage().await?;
                    $test_fn(&storage, &config).await
                })
            }

            #[cfg(feature = "storage-adls")]
            #[test]
            fn [<$test_name _adls>]() -> anyhow::Result<()> {
                execute_in_common_runtime(async {
                    let (storage, config) = create_adls_storage().await?;
                    $test_fn(&storage, &config).await
                })
            }

            #[cfg(feature = "storage-gcs")]
            #[test]
            fn [<$test_name _gcs_regular>]() -> anyhow::Result<()> {
                execute_in_common_runtime(async {
                    let (storage, config) = create_gcs_storage("LAKEKEEPER_TEST__GCS_BUCKET").await?;
                    $test_fn(&storage, &config).await
                })
            }

            #[cfg(feature = "storage-gcs")]
            #[test]
            fn [<$test_name _gcs_hns>]() -> anyhow::Result<()> {
                execute_in_common_runtime(async {
                    let (storage, config) = create_gcs_storage("LAKEKEEPER_TEST__GCS_HNS_BUCKET").await?;
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
test_all_storages!(test_list_with_page_size, test_list_with_page_size_impl);
test_all_storages!(
    test_list_prefix_boundaries,
    test_list_prefix_boundaries_impl
);
test_all_storages!(test_remove_all, test_remove_all_impl);
test_all_storages!(
    test_remove_all_treats_input_as_dir,
    test_remove_all_treats_input_as_dir_impl
);
test_all_storages!(test_empty_files, test_empty_files_impl);
test_all_storages!(test_large_files, test_large_files_impl);
test_all_storages!(test_special_characters, test_special_characters_impl);
test_all_storages!(
    test_special_characters_in_url_segments,
    test_special_characters_in_url_segments_impl
);
test_all_storages!(test_error_handling, test_error_handling_impl);
test_all_storages!(
    test_delete_non_existent_files,
    test_delete_non_existent_files_impl
);
test_all_storages!(
    test_remove_all_deletes_directory,
    test_remove_all_deletes_directory_impl
);
test_all_storages!(
    test_batch_delete_many_items_some_nonexistant,
    test_batch_delete_many_items_some_nonexistant_impl
);
test_all_storages!(
    test_percent_encoding_does_not_alias,
    test_percent_encoding_does_not_alias_impl
);
test_all_storages!(
    test_list_non_existent_directory,
    test_list_non_existent_directory_impl
);
test_all_storages!(test_writer_basic, test_writer_basic_impl);
test_all_storages!(test_writer_multi_chunks, test_writer_multi_chunks_impl);
test_all_storages!(
    test_writer_large_streaming,
    test_writer_large_streaming_impl
);
test_all_storages!(
    test_writer_close_twice_errors,
    test_writer_close_twice_errors_impl
);
test_all_storages!(
    test_writer_write_after_close_errors,
    test_writer_write_after_close_errors_impl
);
test_all_storages!(test_writer_drop_cleanup, test_writer_drop_cleanup_impl);
test_all_storages!(test_read_range_basic, test_read_range_basic_impl);
test_all_storages!(test_read_range_large, test_read_range_large_impl);
test_all_storages!(test_metadata_basic, test_metadata_basic_impl);
test_all_storages!(test_metadata_not_found, test_metadata_not_found_impl);
test_all_storages!(test_exists, test_exists_impl);
test_all_storages!(
    test_writer_then_read_range,
    test_writer_then_read_range_impl
);
test_all_storages!(test_writer_then_metadata, test_writer_then_metadata_impl);
test_all_storages!(
    test_write_then_read_single_and_read,
    test_write_then_read_single_and_read_impl
);

// // Performance tests for storage backend initialization
// #[cfg(feature = "storage-in-memory")]
// #[test]
// fn test_initialization_performance_memory() -> anyhow::Result<()> {
//     execute_in_common_runtime(async {
//         test_initialization_performance_impl(|| Box::pin(create_memory_storage())).await
//     })
// }

// #[cfg(feature = "storage-s3")]
// #[test]
// fn test_initialization_performance_s3() -> anyhow::Result<()> {
//     execute_in_common_runtime(async {
//         test_initialization_performance_impl(|| Box::pin(create_s3_storage())).await
//     })
// }

// #[cfg(feature = "storage-adls")]
// #[test]
// fn test_initialization_performance_adls() -> anyhow::Result<()> {
//     execute_in_common_runtime(async {
//         test_initialization_performance_impl(|| Box::pin(create_adls_storage())).await
//     })
// }

// #[cfg(feature = "storage-gcs")]
// #[test]
// fn test_initialization_performance_gcs_regular() -> anyhow::Result<()> {
//     execute_in_common_runtime(async {
//         test_initialization_performance_impl(|| {
//             Box::pin(create_gcs_storage("LAKEKEEPER_TEST__GCS_BUCKET"))
//         })
//         .await
//     })
// }

// #[cfg(feature = "storage-gcs")]
// #[test]
// fn test_initialization_performance_gcs_hns() -> anyhow::Result<()> {
//     execute_in_common_runtime(async {
//         test_initialization_performance_impl(|| {
//             Box::pin(create_gcs_storage("LAKEKEEPER_TEST__GCS_HNS_BUCKET"))
//         })
//         .await
//     })
// }

/// Performance test implementation for storage backend initialization
#[allow(dead_code)]
async fn test_initialization_performance_impl<F, Fut>(create_storage: F) -> anyhow::Result<()>
where
    F: Fn() -> Fut + Clone,
    Fut: std::future::Future<Output = anyhow::Result<(StorageBackend, TestConfig)>>,
{
    println!("Testing storage backend initialization and write performance...");

    // First initialization (cold start)
    let start_first = Instant::now();
    let (storage1, config1) = create_storage().await?;
    let first_init_duration = start_first.elapsed();

    // Measure first write operation
    let test_path = config1.test_path("perf-test.txt");
    let test_data = Bytes::from("Performance test data");

    let start_first_write = Instant::now();
    storage1.write(&test_path, test_data.clone()).await?;
    let first_write_duration = start_first_write.elapsed();

    // Verify the write worked
    let read_data = storage1.read(&test_path).await?;
    assert_eq!(test_data, read_data);
    storage1.delete(&test_path).await?;

    // Second initialization (potential caching effects)
    let start_second = Instant::now();
    let (storage2, config2) = create_storage().await?;
    let second_init_duration = start_second.elapsed();

    // Measure second write operation
    let test_path2 = config2.test_path("perf-test-2.txt");

    let start_second_write = Instant::now();
    storage2.write(&test_path2, test_data.clone()).await?;
    let second_write_duration = start_second_write.elapsed();

    // Verify the write worked
    let read_data2 = storage2.read(&test_path2).await?;
    assert_eq!(test_data, read_data2);
    storage2.delete(&test_path2).await?;

    // Log initialization times
    println!("First initialization took: {first_init_duration:?}");
    println!("Second initialization took: {second_init_duration:?}");

    // Log write times
    println!("First write operation took: {first_write_duration:?}");
    println!("Second write operation took: {second_write_duration:?}");

    // Log the ratios to see if there's significant difference
    let init_ratio =
        first_init_duration.as_secs_f64() / second_init_duration.as_secs_f64().max(0.001);
    let write_ratio =
        first_write_duration.as_secs_f64() / second_write_duration.as_secs_f64().max(0.001);

    println!("First/Second initialization time ratio: {init_ratio:.2}x");
    println!("First/Second write time ratio: {write_ratio:.2}x");

    // Log total time for first vs second complete operation
    let total_first = first_init_duration + first_write_duration;
    let total_second = second_init_duration + second_write_duration;
    let total_ratio = total_first.as_secs_f64() / total_second.as_secs_f64().max(0.001);

    println!("Total first operation (init + write): {total_first:?}");
    println!("Total second operation (init + write): {total_second:?}");
    println!("First/Second total time ratio: {total_ratio:.2}x");

    // Basic validation that both operations succeeded
    assert!(
        first_init_duration.as_millis() > 0,
        "First initialization should take some time"
    );
    assert!(
        second_init_duration.as_millis() > 0,
        "Second initialization should take some time"
    );
    Ok(())
}

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

    // Should not be able to read after deletion
    let read_result = storage.read(&test_path).await;
    assert!(
        read_result.is_err(),
        "Reading deleted file should fail, but succeeded"
    );

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

    // Verify all files can be read
    for path in &written_paths {
        let read_result = storage.read(path).await;
        assert!(read_result.is_ok(), "File should be readable: {path}");
    }

    // Batch delete all files
    storage.delete_batch(&written_paths).await?;

    // Verify all files are deleted
    for path in &written_paths {
        let read_result = storage.read(path).await;
        assert!(read_result.is_err(), "File should be deleted: {path}");
    }

    Ok(())
}

/// Test batch delete operations implementation
/// This test verifies that batch delete works even if some files don't exist
/// Uses parallelism for faster execution and minimal verification
async fn test_batch_delete_many_items_some_nonexistant_impl(
    storage: &StorageBackend,
    config: &TestConfig,
) -> anyhow::Result<()> {
    // Create a base directory for this test
    let base_dir = config.test_dir_path("batch-delete-mixed");

    // Define the number of files to create and delete
    const EXISTING_FILES_COUNT: usize = 1100; // Larger than single S3 batch
    const NON_EXISTENT_FILES_COUNT: usize = 200;
    const PARALLEL_BATCH_SIZE: usize = 50; // Number of files to create in parallel

    let mut written_paths = Vec::with_capacity(EXISTING_FILES_COUNT);
    let mut non_existent_paths = Vec::with_capacity(NON_EXISTENT_FILES_COUNT);

    // Prepare all paths first
    for i in 0..EXISTING_FILES_COUNT {
        let filename = format!("file-{i:04}.txt");
        let path = format!("{base_dir}{filename}");
        written_paths.push(path);
    }

    // Generate paths for non-existent files
    for i in 0..NON_EXISTENT_FILES_COUNT {
        let filename = format!("non-existent-{i:04}.txt");
        let path = format!("{base_dir}{filename}");
        non_existent_paths.push(path);
    }

    let write_futures = written_paths.iter().map(|path| {
        let path = path.clone();
        let content = Bytes::from(format!("Content of {path}"));
        let storage = storage.clone();
        async move { storage.write(&path, content).await }
    });
    let write_execution = execute_with_parallelism(write_futures, PARALLEL_BATCH_SIZE);
    tokio::pin!(write_execution);

    // Wait for all write operations to complete
    while let Some(result) = write_execution.next().await {
        result??;
    }
    println!("Write complete.");

    // Verify files exist by listing directory (much faster than reading each file)
    let mut list_stream = storage.list(&base_dir, None).await?;
    let mut listed_file_infos = Vec::new();

    while let Some(result) = list_stream.next().await {
        let file_infos = result?;
        listed_file_infos.extend(file_infos);
    }

    // Filter out directory entries (ending with '/')
    let listed_files: Vec<_> = listed_file_infos
        .iter()
        .filter(|file_info| !file_info.location().to_string().ends_with('/'))
        .collect();

    // Just verify we have at least as many files as we wrote
    assert!(
        listed_files.len() == EXISTING_FILES_COUNT,
        "Should find {} files in directory, found {}",
        EXISTING_FILES_COUNT,
        listed_files.len()
    );

    // Combine both lists for batch deletion
    let all_paths: Vec<String> = written_paths
        .iter()
        .chain(non_existent_paths.iter())
        .cloned()
        .collect();

    // Batch delete all files (including non-existent ones)
    let delete_result = storage.delete_batch(&all_paths).await;

    // The operation should succeed even with non-existent files
    assert!(
        delete_result.is_ok(),
        "Batch delete should succeed even with non-existent files"
    );

    // Verify deletion using list operation instead of individual reads
    let mut list_stream = storage.list(&base_dir, None).await?;
    let mut remaining_file_infos = Vec::new();

    while let Some(result) = list_stream.next().await {
        let file_infos = result?;
        remaining_file_infos.extend(file_infos);
    }

    // Filter out directory entries (ending with '/')
    let remaining_files: Vec<_> = remaining_file_infos
        .iter()
        .filter(|file_info| !file_info.location().to_string().ends_with('/'))
        .collect();

    assert!(
        remaining_files.is_empty(),
        "All files should be deleted, but found {} remaining files",
        remaining_files.len()
    );

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
    let mut all_file_infos = Vec::new();

    while let Some(result) = list_stream.next().await {
        let file_infos = result?;
        all_file_infos.extend(file_infos);
    }

    // Debug: print what we actually found
    println!(
        "Expected {} files, found {} files:",
        test_files.len(),
        all_file_infos.len()
    );
    for file_info in &all_file_infos {
        println!("  Found:    {}", file_info.location());
    }
    for path in &written_paths {
        println!("  Expected: {path}");
    }

    let min_expected_items = test_files.len();

    // Should have at least the minimum expected items
    assert!(
        all_file_infos.len() >= min_expected_items,
        "Should list at least {} items, found {}",
        min_expected_items,
        all_file_infos.len()
    );

    // Verify that we can find our test files in the results
    let location_strings: Vec<String> = all_file_infos
        .iter()
        .map(|file_info| file_info.location().to_string())
        .collect();

    for expected_path in &written_paths {
        assert!(
            location_strings.iter().any(|loc| loc == expected_path),
            "Should find path {expected_path} in list results"
        );
    }

    // Make sure all that was found but not expected are directories that end with a slash
    for file_info in &all_file_infos {
        if !written_paths.contains(&file_info.location().to_string()) {
            assert!(
                file_info.location().to_string().ends_with('/'),
                "Unexpected location found that is not a directory: {}",
                file_info.location(),
            );
        }
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

/// Test list operations with page size implementation
async fn test_list_with_page_size_impl(
    storage: &StorageBackend,
    config: &TestConfig,
) -> anyhow::Result<()> {
    let base_dir = config.test_dir_path("list-page-size-test");

    // Create a larger number of files to test pagination
    let num_files = 15;
    let mut written_paths = Vec::new();

    // Write test files
    for i in 0..num_files {
        let filename = format!("file{i:03}.txt");
        let path = format!("{base_dir}{filename}");
        storage
            .write(&path, Bytes::from(format!("Content of file {i}")))
            .await?;
        written_paths.push(path);
    }

    // Test with different page sizes
    let page_sizes = vec![3, 5, 7, 10];

    for page_size in page_sizes {
        println!("Testing with page size: {page_size}");

        let mut list_stream = storage.list(&base_dir, Some(page_size)).await?;
        let mut all_file_infos = Vec::new();
        let mut page_count = 0;

        while let Some(result) = list_stream.next().await {
            let file_infos = result?;
            page_count += 1;

            // Each page (except possibly the last) should have at most page_size items
            assert!(
                file_infos.len() <= page_size,
                "Page {page_count} has {} items, which exceeds page size {page_size}",
                file_infos.len()
            );

            // If this is not the last page, it should have exactly page_size items
            // (we can't easily check if it's the last page without consuming the stream)

            all_file_infos.extend(file_infos);
        }

        // Should have collected all our files
        assert!(
            all_file_infos.len() >= num_files,
            "Should list at least {num_files} items with page size {page_size}, found {}",
            all_file_infos.len()
        );

        // Verify we got multiple pages for smaller page sizes
        if page_size < num_files {
            assert!(
                page_count > 1,
                "With page size {page_size} and {num_files} files, should have multiple pages, got {page_count}"
            );
        }

        // Verify that we can find our test files in the results
        let location_strings: Vec<String> = all_file_infos
            .iter()
            .map(|file_info| file_info.location().to_string())
            .collect();
        for expected_path in &written_paths {
            assert!(
                location_strings.iter().any(|loc| loc == expected_path),
                "Should find path {expected_path} in paginated list results with page size {page_size}"
            );
        }
    }

    // Test with page size of 1 (edge case)
    let mut list_stream = storage.list(&base_dir, Some(1)).await?;
    let mut single_page_file_infos = Vec::new();
    let mut single_page_count = 0;

    while let Some(result) = list_stream.next().await {
        let file_infos = result?;
        single_page_count += 1;

        // Each page should have exactly 1 item (except empty pages which shouldn't happen)
        if !file_infos.is_empty() {
            assert_eq!(
                file_infos.len(),
                1,
                "With page size 1, each non-empty page should have exactly 1 item, got {}",
                file_infos.len()
            );
        }

        single_page_file_infos.extend(file_infos);
    }

    // Should have at least as many pages as files
    assert!(
        single_page_count >= num_files,
        "With page size 1, should have at least {num_files} pages, got {single_page_count}"
    );

    // Test with very large page size (should get everything in one page)
    let mut list_stream = storage.list(&base_dir, Some(1000)).await?;
    let mut large_page_file_infos = Vec::new();
    let mut large_page_count = 0;

    while let Some(result) = list_stream.next().await {
        let file_infos = result?;
        large_page_count += 1;
        large_page_file_infos.extend(file_infos);
    }

    // Should get everything in one or very few pages
    assert!(
        large_page_count <= 2,
        "With large page size, should have at most 2 pages, got {large_page_count}"
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

/// Test remove_all (recursive delete) operations implementation
async fn test_remove_all_treats_input_as_dir_impl(
    storage: &StorageBackend,
    config: &TestConfig,
) -> anyhow::Result<()> {
    let base_dir = config.test_dir_path("remove-all-test");
    let test_files = vec![
        "file1.txt",
        "file2.txt",
        "subdir/file3.txt",
        "subdir/nested/file4.txt",
        "subdir/nested/deep/file5.txt",
        "subdir-2/file6.txt",
        "subdir-2/nested/file7.txt",
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
    let remove_dir = format!("{}/subdir", base_dir.trim_end_matches('/'));
    storage.remove_all(&remove_dir).await?;

    // Wait a bit for eventual consistency (important for S3)
    sleep(Duration::from_millis(100)).await;

    // Verify all files are deleted
    for path in &written_paths {
        let read_result = storage.read(path).await;
        if path.contains("subdir/") {
            assert!(
                read_result.is_err(),
                "File should be deleted after remove_all: {path}"
            );
        } else {
            assert!(
                read_result.is_ok(),
                "File should still exist outside of removed subdir: {path}"
            );
        }
    }

    Ok(())
}

/// Test with empty files implementation
async fn test_empty_files_impl(
    storage: &StorageBackend,
    config: &TestConfig,
) -> anyhow::Result<()> {
    // ToDo: Revisit with new Azure storage. Azure blob client currently
    // can't delete empty files, which fails with: <Error><Code>InvalidRange</Code><Message>The range specified is invalid for the current size of the resource
    if matches!(storage, StorageBackend::Adls(_)) {
        println!("Skipping empty files test for ADLS due to known issue with empty file deletion");
        return Ok(());
    }

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

    // Create a 128MB file
    let large_data = generate_test_data(128);

    // Write large file
    storage.write(&test_path, large_data.clone()).await?;

    // Read large file back
    let read_data = storage.read(&test_path).await?;
    let read_single = storage.read_single(&test_path).await?;

    assert_eq!(
        read_data.len(),
        large_data.len(),
        "Large file size for multi-part download should match"
    );
    assert_eq!(
        read_single.len(),
        large_data.len(),
        "Large file size for single-part download should match"
    );
    assert!(
        read_single == large_data,
        "Large file content for single-part download should match"
    );
    assert!(
        read_data == large_data,
        "Large file content for multi-part download should match"
    );

    // Clean up
    storage.delete(&test_path).await?;

    Ok(())
}

/// Test operations with special characters in paths implementation
async fn test_special_characters_impl(
    storage: &StorageBackend,
    config: &TestConfig,
) -> anyhow::Result<()> {
    // Sub-delims (`!`, `=`, `-`, `_`, `.`, `+`, `*`, `'`, `$`, `,`, `;`) and
    // multibyte UTF-8 are accepted literally. Reserved chars `?` and `#`
    // are rejected by `Location::from_str` and must be percent-encoded;
    // their round-trip is covered by `test_special_characters_in_url_segments`.
    let special_files = vec![
        "file with spaces.txt",
        "file-with-dashes.txt",
        "y fl ! -_ä oats=1.2.txt",
        "file_with_underscores.txt",
        "file.with.dots.txt",
        "file-with-ue-ü.txt",
        "alpha-beta-gamma-encoded-αβγ-unicode.txt", // Unicode characters
    ];

    // Create a specific directory for this test to make listing easier
    let base_dir = config.test_dir_path("special-chars-test");
    let mut written_paths = Vec::new();

    // Write files with special characters
    for filename in &special_files {
        let path = format!("{base_dir}{filename}");
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

    // Test listing files with special characters
    let mut list_stream = storage.list(&base_dir, None).await?;
    let mut all_file_infos = Vec::new();

    while let Some(result) = list_stream.next().await {
        let file_infos = result?;
        all_file_infos.extend(file_infos);
    }

    // Verify all files with special characters are listed
    let listed_locations: Vec<String> = all_file_infos
        .iter()
        .map(|file_info| file_info.location().to_string())
        .collect();
    assert_eq!(
        listed_locations.len(),
        special_files.len(),
        "Number of listed files should match the number of written files"
    );

    for expected_path in &written_paths {
        assert!(
            listed_locations.iter().any(|loc| loc == expected_path),
            "Should find path {expected_path} in list results: {listed_locations:?}"
        )
    }

    // Clean up
    for path in &written_paths {
        storage.delete(path).await?;
    }

    // Ensure we cannot read any of the special character files anymore
    for (i, filename) in special_files.iter().enumerate() {
        let read_data = storage.read(&written_paths[i]).await;
        assert!(
            read_data.is_err(),
            "Reading deleted file with special characters should fail: {filename}"
        );
    }

    Ok(())
}

/// Like `test_special_characters_impl` but the special chars appear as
/// pre-percent-encoded URL segments (e.g. `%3F`, `%20`) — what
/// Lakekeeper REST receives when a client provides a URL-style location.
async fn test_special_characters_in_url_segments_impl(
    storage: &StorageBackend,
    config: &TestConfig,
) -> anyhow::Result<()> {
    // Each entry: a URL-encoded path segment that should round-trip through
    // write → read → list when used as a directory name in a URL location.
    // Positive: segments that must round-trip end-to-end.
    let positive_segments = vec![
        "%3F",     // ?
        "%22",     // "
        "x%20y",   // space in the middle
        "%20x",    // leading space
        "x%20",    // trailing space
        "x%20%20", // trailing double space
        "%2A",     // *
        "%24",     // $
        "%27",     // '
        "%2B",     // +
        "üñîçødé",
        "日本語",
    ];
    // Negative: segments that must be rejected up-front. The reasons differ
    // (Azure InvalidUri for whitespace-only; `url::Url` normalises encoded
    // dot-segments and encoded `/`) but the outcome is the same: silent
    // path divergence that we surface as a clean parse-time error.
    let negative_segments = vec![
        "%20", "%09", "%20%20", // whitespace-only
        "%2E", "%2e", "%2E%2E", "%2e%2e", // dot-segments
        "%2F",    // encoded slash
    ];

    let base_dir = config.test_dir_path("special-chars-url-segments");
    let mut written_paths = Vec::new();
    let mut failures = Vec::new();

    for seg in &positive_segments {
        let path = format!("{base_dir}{seg}/data/metadata/00000-test.metadata.json");
        match storage
            .write(&path, Bytes::from(format!("Content for {seg}")))
            .await
        {
            Ok(()) => written_paths.push((seg.to_string(), path)),
            Err(e) => failures.push(format!("write({seg}): {e}")),
        }
    }

    for (seg, path) in &written_paths {
        match storage.read(path).await {
            Ok(read) => {
                let s = String::from_utf8(read.to_vec())?;
                if s != format!("Content for {seg}") {
                    failures.push(format!("read({seg}): mismatch (got {s:?})"));
                }
            }
            Err(e) => failures.push(format!("read({seg}): {e}")),
        }
    }
    for (_, path) in &written_paths {
        let _ = storage.delete(path).await;
    }

    // The decoded-segment rejections live in `AdlsLocation` only — S3 keys
    // and GCS object names accept these chars literally, so the test is
    // ADLS-specific.
    if matches!(storage, StorageBackend::Adls(_)) {
        for seg in &negative_segments {
            let path = format!("{base_dir}{seg}/data/metadata/00000-test.metadata.json");
            match storage.write(&path, Bytes::from("x")).await {
                Ok(()) => {
                    failures.push(format!("write({seg}): expected reject, got Ok"));
                    let _ = storage.delete(&path).await;
                }
                Err(_) => { /* expected */ }
            }
        }
    }

    if !failures.is_empty() {
        anyhow::bail!(
            "{} segment(s) failed:\n  {}",
            failures.len(),
            failures.join("\n  ")
        );
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
    storage.delete_batch(&non_existent_paths).await?;

    Ok(())
}

/// Test delete non-existent files implementation
async fn test_delete_non_existent_files_impl(
    storage: &StorageBackend,
    config: &TestConfig,
) -> anyhow::Result<()> {
    let non_existent_path = config.test_path("non-existent-file.txt");
    let delete_result = storage.delete(&non_existent_path).await;
    assert!(
        delete_result.is_ok(),
        "Deleting non-existent file should not fail" // S3 natively works this way
    );
    Ok(())
}

/// Test list non-existent directory implementation
async fn test_list_non_existent_directory_impl(
    storage: &StorageBackend,
    config: &TestConfig,
) -> anyhow::Result<()> {
    let non_existent_dir = config.test_dir_path("non-existent-directory/");
    let mut list_stream = storage.list(&non_existent_dir, None).await?;
    let mut all_locations = Vec::new();
    while let Some(result) = list_stream.next().await {
        let locations = result?;
        all_locations.extend(locations);
    }

    // If the directory does not exist, we should get an empty list
    assert!(
        all_locations.is_empty(),
        "Listing non-existent directory should return no items"
    );

    Ok(())
}

/// Test that remove_all deletes the directory itself implementation
async fn test_remove_all_deletes_directory_impl(
    storage: &StorageBackend,
    config: &TestConfig,
) -> anyhow::Result<()> {
    // Create a unique parent directory for this test
    let parent_dir = config.test_dir_path("remove-all-dir-test");
    let target_dir = format!("{parent_dir}target-directory/");

    let test_files = vec![
        "file1.txt",
        "file2.txt",
        "subdir/file3.txt",
        "subdir/nested/file4.txt",
    ];

    let mut written_paths = Vec::new();

    // Write test files in the target directory
    for filename in &test_files {
        let path = format!("{target_dir}{filename}");
        storage
            .write(&path, Bytes::from(format!("Content of {filename}")))
            .await?;
        written_paths.push(path);
    }

    // Also create a sibling directory to ensure we don't delete too much
    let sibling_dir = format!("{parent_dir}sibling-directory/");
    let sibling_file = format!("{sibling_dir}sibling-file.txt");
    storage
        .write(&sibling_file, Bytes::from("Sibling content"))
        .await?;

    // Verify files exist before removal
    for path in &written_paths {
        storage.read(path).await?;
    }
    storage.read(&sibling_file).await?;

    // List parent directory before removal to confirm target directory exists
    let mut pre_list_stream = storage.list(&parent_dir, None).await?;
    let mut pre_file_infos = Vec::new();
    while let Some(result) = pre_list_stream.next().await {
        let file_infos = result?;
        pre_file_infos.extend(file_infos);
    }

    // Should find both target and sibling directories
    let pre_location_strings: Vec<String> = pre_file_infos
        .iter()
        .map(|file_info| file_info.location().to_string())
        .collect();
    let has_target_dir = pre_location_strings
        .iter()
        .any(|loc| loc.starts_with(&target_dir));
    let has_sibling_dir = pre_location_strings
        .iter()
        .any(|loc| loc.starts_with(&sibling_dir));

    assert!(
        has_target_dir,
        "Target directory should exist before removal"
    );
    assert!(
        has_sibling_dir,
        "Sibling directory should exist before removal"
    );

    // Remove all files and the directory itself
    storage.remove_all(&target_dir).await?;

    // Wait a bit for eventual consistency (important for S3)
    sleep(Duration::from_millis(100)).await;

    // Verify all files in target directory are deleted
    for path in &written_paths {
        let read_result = storage.read(path).await;
        assert!(
            read_result.is_err(),
            "File should be deleted after remove_all: {path}"
        );
    }

    // Verify sibling file still exists
    let sibling_read = storage.read(&sibling_file).await;
    assert!(
        sibling_read.is_ok(),
        "Sibling file should still exist after remove_all on target directory"
    );

    // List parent directory after removal to confirm target directory is gone
    let mut post_list_stream = storage.list(&parent_dir, None).await?;
    let mut post_file_infos = Vec::new();
    while let Some(result) = post_list_stream.next().await {
        let file_infos = result?;
        post_file_infos.extend(file_infos);
    }

    let post_location_strings: Vec<String> = post_file_infos
        .iter()
        .map(|file_infos| file_infos.location().to_string())
        .collect();
    let still_has_target_dir = post_location_strings
        .iter()
        .any(|loc| loc.starts_with(&target_dir));
    let still_has_sibling_dir = post_location_strings
        .iter()
        .any(|loc| loc.starts_with(&sibling_dir));

    // The target directory should be completely gone
    assert!(
        !still_has_target_dir,
        "Target directory should be completely removed after remove_all. Found locations: {post_location_strings:?}"
    );

    // The sibling directory should still exist
    assert!(
        still_has_sibling_dir,
        "Sibling directory should still exist after remove_all on target directory"
    );

    // Clean up sibling file
    storage.delete(&sibling_file).await?;

    Ok(())
}

/// Test that list operations correctly handle directory prefix boundaries
/// Ensures that when listing 'a/b/' the results contain 'a/b/c' but not 'a/b-c' or 'a/b-c/d'
async fn test_list_prefix_boundaries_impl(
    storage: &StorageBackend,
    config: &TestConfig,
) -> anyhow::Result<()> {
    // Create a test directory structure with specific paths to test boundary conditions
    let base_dir = config.test_dir_path("list-prefix-boundaries");

    // Define the test directory structure:
    // - base/dir/ (the directory we'll list)
    // - base/dir/file.txt (should be included in listing)
    // - base/dir/subdir/nested.txt (should be included in listing)
    // - base/dir-similar/file.txt (should NOT be included - different directory)
    // - base/dir-similar/subdir/file.txt (should NOT be included - different directory path)

    let files_to_create = vec![
        // Files that should be included when listing base/dir/
        "dir/file.txt",
        "dir/subdir/nested.txt",
        // Files that should NOT be included when listing base/dir/
        "dir-similar/file.txt",
        "dir-similar/subdir/file.txt",
    ];

    let mut all_paths = Vec::new();

    // Create all the test files
    for file in &files_to_create {
        let path = format!("{base_dir}{file}");
        storage
            .write(&path, Bytes::from(format!("Content of {file}")))
            .await?;
        all_paths.push(path);
    }

    for list_dir in &[format!("{base_dir}dir"), format!("{base_dir}dir/")] {
        // List contents of the specific directory
        let mut list_stream = storage.list(list_dir, None).await?;
        let mut listed_file_infos = Vec::new();

        while let Some(result) = list_stream.next().await {
            let file_infos = result?;
            listed_file_infos.extend(file_infos);
        }

        // Debug output
        // println!("Listed {} items in {}", listed_locations.len(), list_dir);
        for file_info in &listed_file_infos {
            println!("  Found: {}", file_info.location());
        }

        // Convert locations to strings for easier comparison
        let location_strings: Vec<String> = listed_file_infos
            .iter()
            .map(|file_info| file_info.location().to_string())
            .collect();

        // Verify that only the correct files are included in the results
        // Should include: base/dir/file.txt and base/dir/subdir/nested.txt
        let expected_in_dir = vec![
            format!("{base_dir}dir/file.txt"),
            format!("{base_dir}dir/subdir/nested.txt"),
            format!("{base_dir}dir/subdir/"), // The subdirectory itself might be listed
        ];

        // Check that expected files are included
        for expected_path in &expected_in_dir {
            // Skip directory entries that might not be consistently returned by all storage backends
            if expected_path.ends_with('/') {
                continue;
            }

            assert!(
                location_strings.iter().any(|loc| loc == expected_path),
                "Expected path {expected_path} should be included in list results"
            );
        }

        for listed_location in location_strings.iter() {
            if listed_location.contains("dir-similar") {
                panic!(
                    "Listed location {listed_location} should NOT be included in results for {list_dir}"
                );
            }
        }

        // Also verify that all returned paths start with the requested directory prefix
        for location in &location_strings {
            let list_dir_with_slash = format!("{}/", list_dir.trim_end_matches('/'));
            assert!(
                location.starts_with(&list_dir_with_slash),
                "Listed path {location} should start with {list_dir}"
            );
        }
    }

    // Clean up
    for path in all_paths {
        let _ = storage.delete(&path).await; // Ignore errors during cleanup
    }

    Ok(())
}

/// Write `data` through the streaming `writer` API in fixed-size slices.
async fn writer_write_in_chunks(
    storage: &StorageBackend,
    path: &str,
    data: &Bytes,
    chunk_size: usize,
) -> anyhow::Result<()> {
    let mut writer = storage.writer(path).await?;
    let mut offset = 0usize;
    while offset < data.len() {
        let end = (offset + chunk_size).min(data.len());
        writer.write(data.slice(offset..end)).await?;
        offset = end;
    }
    writer.close().await?;
    Ok(())
}

/// Streaming writer: single write call then close, content survives round-trip.
async fn test_writer_basic_impl(
    storage: &StorageBackend,
    config: &TestConfig,
) -> anyhow::Result<()> {
    let path = config.test_path("writer-basic.bin");
    let data = Bytes::from_static(b"streaming writer payload");

    let mut writer = storage.writer(&path).await?;
    writer.write(data.clone()).await?;
    writer.close().await?;

    let read_back = storage.read(&path).await?;
    assert_eq!(read_back, data);

    storage.delete(&path).await?;
    Ok(())
}

/// Streaming writer: many small write calls accumulate correctly across close.
async fn test_writer_multi_chunks_impl(
    storage: &StorageBackend,
    config: &TestConfig,
) -> anyhow::Result<()> {
    let path = config.test_path("writer-multi-chunks.bin");

    let chunks: Vec<Bytes> = (0..16u8).map(|i| Bytes::from(vec![i; 1024])).collect();
    let mut expected = bytes::BytesMut::new();
    for chunk in &chunks {
        expected.extend_from_slice(chunk);
    }
    let expected = expected.freeze();

    let mut writer = storage.writer(&path).await?;
    for chunk in chunks {
        writer.write(chunk).await?;
    }
    writer.close().await?;

    let read_back = storage.read(&path).await?;
    assert_eq!(read_back, expected);

    storage.delete(&path).await?;
    Ok(())
}

/// Streaming writer with payload large enough to trigger backend-specific
/// multipart promotion (S3/GCS at 25 MiB, ADLS at 7 MiB; 30 MiB triggers all).
async fn test_writer_large_streaming_impl(
    storage: &StorageBackend,
    config: &TestConfig,
) -> anyhow::Result<()> {
    let path = config.test_path("writer-large.bin");
    let data = generate_test_data(30);

    writer_write_in_chunks(storage, &path, &data, 8 * 1024 * 1024).await?;

    let read_back = storage.read(&path).await?;
    assert_eq!(read_back.len(), data.len());
    assert!(
        read_back == data,
        "streaming-written large file content mismatch"
    );

    storage.delete(&path).await?;
    Ok(())
}

/// Closing an already-closed writer must fail.
async fn test_writer_close_twice_errors_impl(
    storage: &StorageBackend,
    config: &TestConfig,
) -> anyhow::Result<()> {
    let path = config.test_path("writer-close-twice.bin");

    let mut writer = storage.writer(&path).await?;
    writer.write(Bytes::from_static(b"hi")).await?;
    writer.close().await?;

    let second = writer.close().await;
    assert!(second.is_err(), "second close() should fail");

    storage.delete(&path).await?;
    Ok(())
}

/// Writing after `close` must fail.
async fn test_writer_write_after_close_errors_impl(
    storage: &StorageBackend,
    config: &TestConfig,
) -> anyhow::Result<()> {
    let path = config.test_path("writer-write-after-close.bin");

    let mut writer = storage.writer(&path).await?;
    writer.write(Bytes::from_static(b"hi")).await?;
    writer.close().await?;

    let after = writer.write(Bytes::from_static(b"more")).await;
    assert!(after.is_err(), "write() after close() should fail");

    storage.delete(&path).await?;
    Ok(())
}

/// Dropping a streaming writer without `close` should(!) not leave a file at the
/// target path (best-effort `Drop` impl may fail for other reasons).
/// Writes 30 MiB to force backend-side state (S3 multipart, GCS
/// resumable session, ADLS up-front file create), then drops the writer and
/// polls every 20 ms for absence with a 10s budget — matching the
/// `DROP_CANCEL_DURATION` upper bound on the spawned cleanup task. Memory
/// backend never persists buffered bytes, so absence is immediate.
/// Unfortunately this test is flaky by its' design, so a failure is not
/// indicative for a broken `Drop` impl.
async fn test_writer_drop_cleanup_impl(
    storage: &StorageBackend,
    config: &TestConfig,
) -> anyhow::Result<()> {
    let path = config.test_path("writer-drop-cleanup.bin");
    let data = generate_test_data(30);

    {
        let mut writer = storage.writer(&path).await?;
        writer.write(data).await?;
        // Drop without close — Drop impl spawns best-effort cleanup.
    }

    let deadline = Instant::now() + Duration::from_secs(10);
    let poll_interval = Duration::from_millis(20);
    loop {
        if !storage.exists(&path).await? {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(anyhow::anyhow!(
                "file {path} still exists 10s after writer Drop; cleanup task did not succeed within its' budget"
            ));
        }
        sleep(poll_interval).await;
    }
}

/// `read_range` returns exactly the requested slice on a small file, including
/// tail-aligned, interior, and empty ranges.
async fn test_read_range_basic_impl(
    storage: &StorageBackend,
    config: &TestConfig,
) -> anyhow::Result<()> {
    let path = config.test_path("range-basic.bin");
    let data: Vec<u8> = (0..=255u8).cycle().take(4096).collect();
    let data = Bytes::from(data);
    storage.write(&path, data.clone()).await?;

    // Full range
    let full = storage.read_range(&path, 0..4096).await?;
    assert_eq!(full, data);

    // Tail-aligned
    let tail = storage.read_range(&path, 4000..4096).await?;
    assert_eq!(tail, data.slice(4000..4096));

    // Interior
    let mid = storage.read_range(&path, 100..200).await?;
    assert_eq!(mid, data.slice(100..200));

    // Empty (start == end) must yield empty bytes without backend round-trip
    let empty = storage.read_range(&path, 50..50).await?;
    assert_eq!(empty.len(), 0);

    storage.delete(&path).await?;
    Ok(())
}

/// `read_range` over a large file: small interior range and a >25 MiB span
/// that forces the parallel-chunked download path on every cloud backend.
async fn test_read_range_large_impl(
    storage: &StorageBackend,
    config: &TestConfig,
) -> anyhow::Result<()> {
    let path = config.test_path("range-large.bin");
    let data = generate_test_data(30);
    storage.write(&path, data.clone()).await?;

    // Small range near start (single fetch path)
    let head = storage.read_range(&path, 0..1024).await?;
    assert_eq!(head, data.slice(0..1024));

    // Large interior range (>25 MiB) forces parallel chunked read
    let big_range = 1024usize..(29 * 1024 * 1024);
    let big = storage
        .read_range(&path, big_range.start as u64..big_range.end as u64)
        .await?;
    let expected = data.slice(big_range.clone());
    assert_eq!(big.len(), expected.len());
    assert!(big == expected, "chunked range-read content mismatch");

    storage.delete(&path).await?;
    Ok(())
}

/// `metadata` returns size matching the bytes written and a location ending
/// with the requested suffix.
async fn test_metadata_basic_impl(
    storage: &StorageBackend,
    config: &TestConfig,
) -> anyhow::Result<()> {
    let path = config.test_path("metadata-basic.bin");
    let data = Bytes::from(vec![0xab; 4096]);
    storage.write(&path, data.clone()).await?;

    let info = storage.metadata(&path).await?;
    assert_eq!(info.size(), Some(data.len() as u64));
    // Backends may canonicalize the URL; assert the suffix matches.
    assert!(
        info.location().to_string().ends_with("metadata-basic.bin"),
        "metadata location {} does not end with expected suffix",
        info.location()
    );

    storage.delete(&path).await?;
    Ok(())
}

/// `metadata` on a missing path surfaces `ErrorKind::NotFound`.
async fn test_metadata_not_found_impl(
    storage: &StorageBackend,
    config: &TestConfig,
) -> anyhow::Result<()> {
    let path = config.test_path("metadata-missing.bin");
    let result = storage.metadata(&path).await;
    match result {
        Err(ReadError::IOError(e)) if e.kind() == ErrorKind::NotFound => Ok(()),
        Err(other) => Err(anyhow::anyhow!("expected NotFound IOError, got {other:?}")),
        Ok(_) => Err(anyhow::anyhow!("expected metadata to fail on missing file")),
    }
}

/// `exists` flips false → true → false across the file lifecycle.
async fn test_exists_impl(storage: &StorageBackend, config: &TestConfig) -> anyhow::Result<()> {
    let path = config.test_path("exists.bin");

    assert!(
        !storage.exists(&path).await?,
        "file must not exist before write"
    );

    storage.write(&path, Bytes::from_static(b"hi")).await?;
    assert!(storage.exists(&path).await?, "file must exist after write");

    storage.delete(&path).await?;
    assert!(
        !storage.exists(&path).await?,
        "file must not exist after delete"
    );

    Ok(())
}

/// Mixed flow: write via streaming `writer`, then `read_range` interior bytes.
async fn test_writer_then_read_range_impl(
    storage: &StorageBackend,
    config: &TestConfig,
) -> anyhow::Result<()> {
    let path = config.test_path("writer-then-range.bin");
    let data: Vec<u8> = (0..=255u8).cycle().take(8192).collect();
    let data = Bytes::from(data);

    let mut writer = storage.writer(&path).await?;
    writer.write(data.slice(0..4096)).await?;
    writer.write(data.slice(4096..8192)).await?;
    writer.close().await?;

    let middle = storage.read_range(&path, 4000..4200).await?;
    assert_eq!(middle, data.slice(4000..4200));

    storage.delete(&path).await?;
    Ok(())
}

/// Mixed flow: write via streaming `writer` with multipart payload, then
/// `metadata` reports the correct size.
async fn test_writer_then_metadata_impl(
    storage: &StorageBackend,
    config: &TestConfig,
) -> anyhow::Result<()> {
    let path = config.test_path("writer-then-metadata.bin");
    let data = generate_test_data(30);

    writer_write_in_chunks(storage, &path, &data, 8 * 1024 * 1024).await?;

    let info = storage.metadata(&path).await?;
    assert_eq!(info.size(), Some(data.len() as u64));

    storage.delete(&path).await?;
    Ok(())
}

/// Mixed flow: bulk `write`, then read with both `read_single` and `read`
/// on a payload large enough to exercise the multipart download path.
async fn test_write_then_read_single_and_read_impl(
    storage: &StorageBackend,
    config: &TestConfig,
) -> anyhow::Result<()> {
    let path = config.test_path("write-then-reads.bin");
    let data = generate_test_data(30);
    storage.write(&path, data.clone()).await?;

    let read_single = storage.read_single(&path).await?;
    let read_multi = storage.read(&path).await?;

    assert_eq!(read_single.len(), data.len());
    assert_eq!(read_multi.len(), data.len());
    assert!(read_single == data, "read_single content mismatch");
    assert!(read_multi == data, "read content mismatch");

    storage.delete(&path).await?;
    Ok(())
}

/// Pin the byte-literal storage-key model: two paths that differ only by
/// percent-encoding of an unreserved/sub-delim character must address two
/// physically distinct objects. If any backend silently aliases them, the
/// catalog cannot rely on raw `fs_location` bytes for uniqueness — and
/// canonicalisation (or backend-specific rejection) becomes mandatory.
///
/// This is the empirical premise behind dropping `Location::from_str`
/// canonicalisation. A failure here is the signal that the byte-literal
/// model does NOT hold for the failing backend, and policy-level mitigation
/// is required for that backend specifically.
async fn test_percent_encoding_does_not_alias_impl(
    storage: &StorageBackend,
    config: &TestConfig,
) -> anyhow::Result<()> {
    // Pairs of paths that share the same URI-decoded form but differ
    // byte-for-byte. Under the byte-literal model each pair produces two
    // distinct storage objects.
    //
    // Each entry: (decoded form, percent-encoded form, label).
    // - "Abc" vs "%41bc": alphanumeric — pchar unreserved
    // - "foo-bar" vs "foo%2Dbar": `-` — pchar unreserved
    // - "foo+bar" vs "foo%2Bbar": `+` — pchar sub-delim
    // - "%3F" vs "%3f": hex case in surviving %XX
    let pairs: &[(&str, &str, &str)] = &[
        ("Abc", "%41bc", "alpha-A"),
        ("foo-bar", "foo%2Dbar", "dash"),
        ("foo+bar", "foo%2Bbar", "plus"),
        ("%3F", "%3f", "hex-case-Q"),
    ];

    let base_dir = config.test_dir_path("percent-alias-test");
    let mut failures = Vec::new();
    let mut to_cleanup = Vec::new();

    for (decoded, encoded, label) in pairs {
        let path_decoded = format!("{base_dir}{decoded}/data.bin");
        let path_encoded = format!("{base_dir}{encoded}/data.bin");

        // Distinct payloads so we can detect aliasing by content swap.
        let payload_decoded = Bytes::from(format!("DECODED:{label}"));
        let payload_encoded = Bytes::from(format!("ENCODED:{label}"));

        // Write the decoded path first, then the encoded path. If the
        // backend aliases, the second write overwrites the first.
        if let Err(e) = storage.write(&path_decoded, payload_decoded.clone()).await {
            failures.push(format!("{label}: write decoded `{decoded}` failed: {e}"));
            continue;
        }
        to_cleanup.push(path_decoded.clone());

        if let Err(e) = storage.write(&path_encoded, payload_encoded.clone()).await {
            failures.push(format!("{label}: write encoded `{encoded}` failed: {e}"));
            continue;
        }
        to_cleanup.push(path_encoded.clone());

        // Read back from the originally-written paths. If the backend
        // aliases the two, the decoded-path read returns the encoded-path
        // payload (or vice versa, depending on which write "won").
        match storage.read(&path_decoded).await {
            Ok(got) if got == payload_decoded => {} // expected — distinct
            Ok(got) if got == payload_encoded => {
                failures.push(format!(
                    "{label}: ALIAS DETECTED — decoded path `{decoded}` returned encoded payload (write to `{encoded}` overwrote it)"
                ));
            }
            Ok(got) => {
                failures.push(format!(
                    "{label}: decoded path returned unexpected payload: {got:?}"
                ));
            }
            Err(e) => failures.push(format!("{label}: read decoded `{decoded}` failed: {e}")),
        }

        match storage.read(&path_encoded).await {
            Ok(got) if got == payload_encoded => {} // expected — distinct
            Ok(got) if got == payload_decoded => {
                failures.push(format!(
                    "{label}: ALIAS DETECTED — encoded path `{encoded}` returned decoded payload"
                ));
            }
            Ok(got) => {
                failures.push(format!(
                    "{label}: encoded path returned unexpected payload: {got:?}"
                ));
            }
            Err(e) => failures.push(format!("{label}: read encoded `{encoded}` failed: {e}")),
        }
    }

    // Cleanup regardless of outcome.
    for path in to_cleanup {
        let _ = storage.delete(&path).await;
    }

    if !failures.is_empty() {
        anyhow::bail!(
            "{} percent-encoding alias check(s) failed:\n  {}",
            failures.len(),
            failures.join("\n  ")
        );
    }
    Ok(())
}

/// Generate test data of specified size in MB
///
/// This function efficiently creates a Bytes object containing random data
/// of the specified size without allocating all of it at once.
fn generate_test_data(size_mb: usize) -> Bytes {
    use bytes::{BufMut, BytesMut};
    const CHUNK_SIZE: usize = 1024 * 1024; // 1MB chunks
    let total_size = size_mb * CHUNK_SIZE;

    let mut buffer = BytesMut::with_capacity(total_size);
    let mut rng = fastrand::Rng::with_seed(42);

    // Generate data in 1MB chunks to avoid large allocations
    let mut remaining = total_size;
    while remaining > 0 {
        let chunk_size = remaining.min(CHUNK_SIZE);
        let mut chunk = vec![0u8; chunk_size];
        rng.fill(&mut chunk);
        buffer.put_slice(&chunk);
        remaining -= chunk_size;
    }

    buffer.freeze()
}
