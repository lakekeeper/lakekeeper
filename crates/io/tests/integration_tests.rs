use std::{future::Future, sync::LazyLock};

use bytes::Bytes;
use futures::StreamExt;
use lakekeeper_io::{BatchDeleteResult, LakekeeperStorage, StorageBackend};
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

// #[tokio::test]
// async fn test_foo() -> anyhow::Result<()> {
//     let client_id = std::env::var("LAKEKEEPER_TEST__AZURE_CLIENT_ID")
//         .map_err(|_| anyhow::anyhow!("LAKEKEEPER_TEST__AZURE_CLIENT_ID not set"))?;
//     let tenant_id = std::env::var("LAKEKEEPER_TEST__AZURE_TENANT_ID")
//         .map_err(|_| anyhow::anyhow!("LAKEKEEPER_TEST__AZURE_TENANT_ID not set"))?;
//     let client_secret = std::env::var("LAKEKEEPER_TEST__AZURE_CLIENT_SECRET")
//         .map_err(|_| anyhow::anyhow!("LAKEKEEPER_TEST__AZURE_CLIENT_SECRET not set"))?;

//     let account = std::env::var("LAKEKEEPER_TEST__AZURE_STORAGE_ACCOUNT_NAME")
//         .map_err(|_| anyhow::anyhow!("LAKEKEEPER_TEST__AZURE_STORAGE_ACCOUNT_NAME not set"))?;
//     let filesystem = std::env::var("LAKEKEEPER_TEST__AZURE_STORAGE_FILESYSTEM_NAME")
//         .map_err(|_| anyhow::anyhow!("LAKEKEEPER_TEST__AZURE_STORAGE_FILESYSTEM_NAME not set"))?;

//     let settings = lakekeeper_io::adls::AzureSettings {
//         authority_host: None,
//         cloud_location: lakekeeper_io::adls::CloudLocation::Public {
//             account: account.clone(),
//         },
//     };
//     let auth = lakekeeper_io::adls::AzureAuth::ClientCredentials(
//         lakekeeper_io::adls::AzureClientCredentialsAuth {
//             client_id,
//             client_secret,
//             tenant_id,
//         },
//     );

//     let storage = StorageBackend::Adls(
//         settings
//             .get_storage_client(&auth)
//             .map_err(|e| anyhow::anyhow!(e))?,
//     );

//     let base_path = format!(
//         "abfss://{filesystem}@{account}.dfs.core.windows.net/integration-tests/{}",
//         uuid::Uuid::new_v4()
//     );
//     let config = TestConfig { base_path };

//     let dl_client = settings.get_datalake_client(&auth).unwrap();
//     let fs_client = dl_client.file_system_client(&filesystem);

//     // Create a test file
//     let file_client = fs_client.get_file_client("manual-test/test.txt");
//     file_client.create().await.unwrap();
//     let content = Bytes::from("This is a test file.");
//     let content_length = content.len() as i64;
//     file_client.append(0, content).await.unwrap();
//     file_client.flush(content_length).await.unwrap();

//     let file_client = fs_client.get_file_client("manual-test/sub/test.txt");
//     file_client.create().await.unwrap();
//     let content = Bytes::from("This is a test file.");
//     let content_length = content.len() as i64;
//     file_client.append(0, content).await.unwrap();
//     file_client.flush(content_length).await.unwrap();

//     fs_client
//         .list_paths()
//         .directory("manual-test/sub")
//         .into_stream()
//         .for_each(|path| {
//             let path = path.unwrap();
//             println!("Found path: {:?}", path);
//             async {}
//         })
//         .await;

//     fs_client
//         .get_file_client("manual-test")
//         .read()
//         .await
//         .unwrap();

//     let folder_client = fs_client.get_file_client("manual-test/sub");
//     let mut stream = folder_client.delete().recursive(true).into_stream();

//     while let Some(result) = stream.next().await {
//         match result {
//             Ok(_) => println!("Deleted successfully"),
//             Err(e) => eprintln!("Error deleting: {}", e),
//         }
//     }

//     // // List files
//     // let dir_client = fs_client.get_directory_client("/");
//     // let mut list_stream = dir_client.list_paths().into_stream();
//     // while let Some(path) = list_stream.next().await {
//     //     let path = path.unwrap();
//     //     println!("Found path: {:?}", path);
//     // }

//     Ok(())
// }

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

                    let storage = StorageBackend::S3(s3_settings.get_storage_client(Some(&s3_auth)).await);

                    let base_path = format!("s3://{}/lakekeeper-io-integration-tests/{}", bucket, uuid::Uuid::new_v4());
                    let config = TestConfig { base_path };

                    $test_fn(&storage, &config).await
                })
            }

            #[cfg(feature = "storage-adls")]
            #[test]
            fn [<$test_name _adls>]() -> anyhow::Result<()> {
                execute_in_common_runtime(async {
                    let client_id = std::env::var("LAKEKEEPER_TEST__AZURE_CLIENT_ID")
                        .map_err(|_| anyhow::anyhow!("LAKEKEEPER_TEST__AZURE_CLIENT_ID not set"))?;
                    let tenant_id = std::env::var("LAKEKEEPER_TEST__AZURE_TENANT_ID")
                        .map_err(|_| anyhow::anyhow!("LAKEKEEPER_TEST__AZURE_TENANT_ID not set"))?;
                    let client_secret = std::env::var("LAKEKEEPER_TEST__AZURE_CLIENT_SECRET")
                        .map_err(|_| anyhow::anyhow!("LAKEKEEPER_TEST__AZURE_CLIENT_SECRET not set"))?;

                    let account = std::env::var("LAKEKEEPER_TEST__AZURE_STORAGE_ACCOUNT_NAME")
                        .map_err(|_| anyhow::anyhow!("LAKEKEEPER_TEST__AZURE_STORAGE_ACCOUNT_NAME not set"))?;
                    let filesystem = std::env::var("LAKEKEEPER_TEST__AZURE_STORAGE_FILESYSTEM_NAME")
                        .map_err(|_| anyhow::anyhow!("LAKEKEEPER_TEST__AZURE_STORAGE_FILESYSTEM_NAME not set"))?;

                    let settings = lakekeeper_io::adls::AzureSettings {
                        authority_host: None,
                        cloud_location: lakekeeper_io::adls::CloudLocation::Public { account: account.clone() },
                    };
                    let auth = lakekeeper_io::adls::AzureAuth::ClientCredentials(
                        lakekeeper_io::adls::AzureClientCredentialsAuth {
                            client_id,
                            client_secret,
                            tenant_id,
                        },
                    );

                    let storage = StorageBackend::Adls(settings.get_storage_client(&auth).map_err(|e| anyhow::anyhow!(e))?);

                    let base_path = format!("abfss://{filesystem}@{account}.dfs.core.windows.net/lakekeeper-io-integration-tests/{}", uuid::Uuid::new_v4());
                    let config = TestConfig { base_path };

                    $test_fn(&storage, &config).await
                })
            }

            #[cfg(feature = "storage-gcs")]
            #[test]
            fn [<$test_name _gcs_regular>]() -> anyhow::Result<()> {
                execute_in_common_runtime(async {
                    let credential = std::env::var("LAKEKEEPER_TEST__GCS_CREDENTIAL")
                        .map_err(|_| anyhow::anyhow!("LAKEKEEPER_TEST__GCS_CREDENTIAL not set"))?;
                    let bucket = std::env::var("LAKEKEEPER_TEST__GCS_BUCKET")
                        .map_err(|_| anyhow::anyhow!("LAKEKEEPER_TEST__GCS_BUCKET not set"))?;

                    let credential_file: lakekeeper_io::gcs::CredentialsFile = serde_json::from_str(&credential)
                        .map_err(|e| anyhow::anyhow!("Failed to parse GCS credential file: {e}"))?;

                    let settings = lakekeeper_io::gcs::GCSSettings {};
                    let auth = lakekeeper_io::gcs::GcsAuth::CredentialsFile { file: credential_file};

                    let storage = StorageBackend::Gcs(settings.get_storage_client(&auth).await.map_err(|e| anyhow::anyhow!(e))?);

                    let base_path = format!("gs://{bucket}/lakekeeper-io-integration-tests/{}", uuid::Uuid::new_v4());
                    let config = TestConfig { base_path };

                    $test_fn(&storage, &config).await
                })
            }

            #[cfg(feature = "storage-gcs")]
            #[test]
            fn [<$test_name _gcs_hns>]() -> anyhow::Result<()> {
                execute_in_common_runtime(async {
                    let credential = std::env::var("LAKEKEEPER_TEST__GCS_CREDENTIAL")
                        .map_err(|_| anyhow::anyhow!("LAKEKEEPER_TEST__GCS_CREDENTIAL not set"))?;
                    let bucket = std::env::var("LAKEKEEPER_TEST__GCS_HNS_BUCKET")
                        .map_err(|_| anyhow::anyhow!("LAKEKEEPER_TEST__GCS_HNS_BUCKET not set"))?;

                    let credential_file: lakekeeper_io::gcs::CredentialsFile = serde_json::from_str(&credential)
                        .map_err(|e| anyhow::anyhow!("Failed to parse GCS credential file: {e}"))?;

                    let settings = lakekeeper_io::gcs::GCSSettings {};
                    let auth = lakekeeper_io::gcs::GcsAuth::CredentialsFile { file: credential_file};

                    let storage = StorageBackend::Gcs(settings.get_storage_client(&auth).await.map_err(|e| anyhow::anyhow!(e))?);

                    let base_path = format!("gs://{bucket}/lakekeeper-io-integration-tests/{}", uuid::Uuid::new_v4());
                    let config = TestConfig { base_path };

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
test_all_storages!(test_remove_all, test_remove_all_impl);
test_all_storages!(
    test_remove_all_treats_input_as_dir,
    test_remove_all_treats_input_as_dir_impl
);
test_all_storages!(test_empty_files, test_empty_files_impl);
test_all_storages!(test_large_files, test_large_files_impl);
test_all_storages!(test_special_characters, test_special_characters_impl);
test_all_storages!(test_error_handling, test_error_handling_impl);
test_all_storages!(
    test_delete_non_existent_files,
    test_delete_non_existent_files_impl
);
test_all_storages!(
    test_remove_all_deletes_directory,
    test_remove_all_deletes_directory_impl
);

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
    let result = storage.delete_batch(&written_paths).await?;

    match result {
        BatchDeleteResult::AllSuccessful() => {}
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
        println!("  Found:    {location}");
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

    // Make sure all that was found but not expected are directories that end with a slash
    for location in &all_locations {
        if !written_paths.contains(&location.to_string()) {
            assert!(
                location.to_string().ends_with('/'),
                "Unexpected location found that is not a directory: {location}",
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
        let mut all_locations = Vec::new();
        let mut page_count = 0;

        while let Some(result) = list_stream.next().await {
            let locations = result?;
            page_count += 1;

            // Each page (except possibly the last) should have at most page_size items
            assert!(
                locations.len() <= page_size,
                "Page {page_count} has {} items, which exceeds page size {page_size}",
                locations.len()
            );

            // If this is not the last page, it should have exactly page_size items
            // (we can't easily check if it's the last page without consuming the stream)

            all_locations.extend(locations);
        }

        // Should have collected all our files
        assert!(
            all_locations.len() >= num_files,
            "Should list at least {num_files} items with page size {page_size}, found {}",
            all_locations.len()
        );

        // Verify we got multiple pages for smaller page sizes
        if page_size < num_files {
            assert!(
                page_count > 1,
                "With page size {page_size} and {num_files} files, should have multiple pages, got {page_count}"
            );
        }

        // Verify that we can find our test files in the results
        let location_strings: Vec<String> = all_locations.iter().map(ToString::to_string).collect();
        for expected_path in &written_paths {
            assert!(
                location_strings.iter().any(|loc| loc == expected_path),
                "Should find path {expected_path} in paginated list results with page size {page_size}"
            );
        }
    }

    // Test with page size of 1 (edge case)
    let mut list_stream = storage.list(&base_dir, Some(1)).await?;
    let mut single_page_locations = Vec::new();
    let mut single_page_count = 0;

    while let Some(result) = list_stream.next().await {
        let locations = result?;
        single_page_count += 1;

        // Each page should have exactly 1 item (except empty pages which shouldn't happen)
        if !locations.is_empty() {
            assert_eq!(
                locations.len(),
                1,
                "With page size 1, each non-empty page should have exactly 1 item, got {}",
                locations.len()
            );
        }

        single_page_locations.extend(locations);
    }

    // Should have at least as many pages as files
    assert!(
        single_page_count >= num_files,
        "With page size 1, should have at least {num_files} pages, got {single_page_count}"
    );

    // Test with very large page size (should get everything in one page)
    let mut list_stream = storage.list(&base_dir, Some(1000)).await?;
    let mut large_page_locations = Vec::new();
    let mut large_page_count = 0;

    while let Some(result) = list_stream.next().await {
        let locations = result?;
        large_page_count += 1;
        large_page_locations.extend(locations);
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
    // Names are path of URL string, which may contain urlencoded chars
    let special_files = vec![
        "file with spaces.txt",
        "file-with-dashes.txt",
        "y fl !? -_ä oats=1.2.txt",
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
    let mut all_locations = Vec::new();

    while let Some(result) = list_stream.next().await {
        let locations = result?;
        all_locations.extend(locations);
    }

    // Verify all files with special characters are listed
    let listed_locations: Vec<String> = all_locations.iter().map(ToString::to_string).collect();
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
        BatchDeleteResult::AllSuccessful() => {
            // This is fine - some storage backends treat deleting non-existent files as success
        }
        BatchDeleteResult::PartialFailure { errors, .. } => {
            return Err(anyhow::anyhow!(
                "Batch delete should not have partial failures for non-existent files: {errors:?}"
            ));
        }
    }

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
    let mut pre_locations = Vec::new();
    while let Some(result) = pre_list_stream.next().await {
        let locations = result?;
        pre_locations.extend(locations);
    }

    // Should find both target and sibling directories
    let pre_location_strings: Vec<String> = pre_locations.iter().map(ToString::to_string).collect();
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
    let mut post_locations = Vec::new();
    while let Some(result) = post_list_stream.next().await {
        let locations = result?;
        post_locations.extend(locations);
    }

    let post_location_strings: Vec<String> =
        post_locations.iter().map(ToString::to_string).collect();
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

/// Generate test data of specified size in MB
///
/// This function efficiently creates a Bytes object containing random data
/// of the specified size without allocating all of it at once.
fn generate_test_data(size_mb: usize) -> Bytes {
    use bytes::{BufMut, BytesMut};
    use rand::RngCore;

    const CHUNK_SIZE: usize = 1024 * 1024; // 1MB chunks
    let total_size = size_mb * CHUNK_SIZE;

    let mut buffer = BytesMut::with_capacity(total_size);
    let mut rng = rand::rng();

    // Generate data in 1MB chunks to avoid large allocations
    let mut remaining = total_size;
    while remaining > 0 {
        let chunk_size = remaining.min(CHUNK_SIZE);
        let mut chunk = vec![0u8; chunk_size];
        rng.fill_bytes(&mut chunk);
        buffer.put_slice(&chunk);
        remaining -= chunk_size;
    }

    buffer.freeze()
}
