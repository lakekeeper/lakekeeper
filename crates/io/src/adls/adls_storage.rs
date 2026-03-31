use std::{
    collections::HashMap,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use azure_storage_blob::{
    BlobServiceClient,
    models::{
        BlobClientDownloadOptions, BlobClientDownloadResultHeaders,
        BlobClientGetPropertiesResultHeaders, BlobContainerClientListBlobsOptions,
        ListBlobsIncludeItem,
    },
};
use bytes::Bytes;
use chrono::DateTime;
use futures::{StreamExt as _, TryStreamExt as _};
use tokio;

use crate::{
    DeleteBatchError, DeleteError, FileInfo, IOError, InvalidLocationError, LakekeeperStorage,
    Location, ReadError, WriteError,
    adls::{AdlsLocation, CloudLocation, adls_error::parse_error},
    calculate_ranges, delete_not_found_is_ok,
    error::ErrorKind,
    execute_with_parallelism, validate_file_size,
};

#[derive(Clone)]
pub struct AdlsStorage {
    blob_service_client: Arc<BlobServiceClient>,
    cloud_location: CloudLocation,
}

impl std::fmt::Debug for AdlsStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdlsStorage")
            .field("account", &self.cloud_location.account())
            .finish_non_exhaustive()
    }
}

const MAX_BYTES_PER_REQUEST: usize = 7 * 1024 * 1024;
const DEFAULT_BYTES_PER_REQUEST: usize = 4 * 1024 * 1024;

impl AdlsStorage {
    /// Validates that the location matches this storage's account and returns the container name.
    fn check_account(&self, location: &AdlsLocation) -> Result<(), InvalidLocationError> {
        if self.cloud_location.account() != location.account_name() {
            return Err(InvalidLocationError::new(
                location.to_string(),
                format!(
                    "Location account name `{}` does not match storage account `{}`",
                    location.account_name(),
                    self.cloud_location.account()
                ),
            ));
        }
        Ok(())
    }
}

impl AdlsStorage {
    #[must_use]
    pub fn new(client: BlobServiceClient, cloud_location: CloudLocation) -> Self {
        Self {
            blob_service_client: Arc::new(client),
            cloud_location,
        }
    }

    #[must_use]
    pub fn client(&self) -> &BlobServiceClient {
        &self.blob_service_client
    }
}

impl LakekeeperStorage for AdlsStorage {
    async fn delete(&self, path: impl AsRef<str>) -> Result<(), DeleteError> {
        let path = path.as_ref();
        let adls_location = AdlsLocation::try_from_str(path, true)?;

        require_key(&adls_location)?;
        self.check_account(&adls_location)?;

        let blob_client = self
            .blob_service_client
            .blob_client(adls_location.filesystem(), &adls_location.blob_name());

        let result = blob_client
            .delete(None)
            .await
            .map_err(|e| parse_error(e, path))
            .map(|_| ());
        let result = delete_not_found_is_ok(result);
        if let Err(e) = result {
            return Err(e.into());
        }

        Ok(())
    }

    async fn delete_batch(
        &self,
        paths: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Result<(), DeleteBatchError> {
        let grouped_paths = group_paths_by_container(paths)?;

        let mut delete_futures = Vec::new();

        for ((_account, _filesystem), paths) in grouped_paths {
            if paths.is_empty() {
                continue;
            }

            for adls_loc in paths {
                let svc = Arc::clone(&self.blob_service_client);
                let path_str = adls_loc.location().to_string();

                let future = async move {
                    let blob_client = svc.blob_client(adls_loc.filesystem(), &adls_loc.blob_name());
                    let result = blob_client
                        .delete(None)
                        .await
                        .map_err(|e| parse_error(e, &path_str))
                        .map(|_| ());
                    let result = delete_not_found_is_ok(result);

                    match result {
                        Ok(()) => Ok::<(AdlsLocation, Option<IOError>), DeleteBatchError>((
                            adls_loc, None,
                        )),
                        Err(e) => Ok((adls_loc, Some(e))),
                    }
                };

                delete_futures.push(future);
            }
        }

        let completed_batches = AtomicU64::new(0);
        let total_batches = delete_futures.len();

        let delete_stream = execute_with_parallelism(delete_futures, 100).map(|result| {
            result
                .map_err(|join_err| {
                    DeleteBatchError::IOError(IOError::new(
                        ErrorKind::Unexpected,
                        format!("Task join error during batch delete: {join_err}"),
                        "batch_operation".to_string(),
                    ))
                })
                .and_then(|inner_result| inner_result)
        });
        tokio::pin!(delete_stream);

        while let Some(result) = delete_stream.next().await {
            let completed_batch = completed_batches.fetch_add(1, Ordering::Relaxed);
            match result? {
                (_path, None) => {}
                (_location, Some(error)) => {
                    return Err(DeleteBatchError::IOError(error.with_context(format!(
                        "Delete batch {completed_batch} out of {total_batches} failed",
                    ))));
                }
            }
        }

        Ok(())
    }

    async fn write(&self, path: impl AsRef<str>, bytes: Bytes) -> Result<(), WriteError> {
        let path = path.as_ref();
        let adls_location = AdlsLocation::try_from_str(path, true)?;

        require_key(&adls_location)?;
        self.check_account(&adls_location)
            .map_err(WriteError::from)?;

        let blob_client = self
            .blob_service_client
            .blob_client(adls_location.filesystem(), &adls_location.blob_name());

        // The new SDK's upload() does a single-call block blob upload and overwrites by default.
        // This replaces the old create + append + flush pattern.
        blob_client
            .upload(bytes.into(), None)
            .await
            .map_err(|e| WriteError::IOError(parse_error(e, path)))?;

        Ok(())
    }

    async fn read_single(&self, path: impl AsRef<str>) -> Result<Bytes, ReadError> {
        let path = path.as_ref();
        let adls_location = AdlsLocation::try_from_str(path, true)?;

        require_key(&adls_location)?;
        self.check_account(&adls_location)
            .map_err(ReadError::from)?;

        let blob_client = self
            .blob_service_client
            .blob_client(adls_location.filesystem(), &adls_location.blob_name());

        let response = blob_client.download(None).await.map_err(|e| {
            ReadError::IOError(
                parse_error(e, path).with_context("Failed to read ADLS blob in single request."),
            )
        })?;

        let bytes: Bytes = response
            .into_body()
            .collect()
            .await
            .map_err(|e| ReadError::IOError(parse_error(e, path)))?;

        Ok(bytes)
    }

    async fn read(&self, path: impl AsRef<str> + Send) -> Result<Bytes, ReadError> {
        let path = path.as_ref();
        let adls_location = AdlsLocation::try_from_str(path, true)?;

        require_key(&adls_location)?;
        self.check_account(&adls_location)
            .map_err(ReadError::from)?;

        let blob_client = self
            .blob_service_client
            .blob_client(adls_location.filesystem(), &adls_location.blob_name());

        // Get properties to check file size and last_modified for multi-part read
        let props = blob_client.get_properties(None).await.map_err(|e| {
            ReadError::IOError(
                parse_error(e, path).with_context("Failed to get ADLS blob properties"),
            )
        })?;

        let content_length: Option<u64> = props
            .content_length()
            .map_err(|e| ReadError::IOError(parse_error(e, path)))?;

        let Some(content_length) = content_length else {
            return self.read_single(path).await;
        };

        let file_size = validate_file_size(
            i64::try_from(content_length).unwrap_or(i64::MAX),
            adls_location.location().as_str(),
        )?;

        if file_size < MAX_BYTES_PER_REQUEST {
            return self.read_single(path).await;
        }

        // For multi-part reads, get last_modified from properties for consistency check
        let props_last_modified = props
            .last_modified()
            .map_err(|e| ReadError::IOError(parse_error(e, path)))?;

        let chunks = calculate_ranges(file_size, DEFAULT_BYTES_PER_REQUEST);

        let svc = Arc::clone(&self.blob_service_client);
        let filesystem = adls_location.filesystem().to_string();
        let blob_name = adls_location.blob_name().clone();

        let download_futures: Vec<_> = chunks
            .into_iter()
            .enumerate()
            .map(|(chunk_index, (start, end))| {
                let svc = Arc::clone(&svc);
                let filesystem = filesystem.clone();
                let blob_name = blob_name.clone();
                let path = path.to_string();
                let expected_last_modified = props_last_modified;

                async move {
                    let blob_client = svc.blob_client(&filesystem, &blob_name);
                    // Azure range header format: "bytes=start-end" (inclusive)
                    let range_str = format!("bytes={start}-{end}");
                    let opts = BlobClientDownloadOptions {
                        range: Some(range_str),
                        ..Default::default()
                    };

                    let response = blob_client
                        .download(Some(opts))
                        .await
                        .map_err(|e| {
                            ReadError::IOError(parse_error(e, &path).with_context(format!(
                                "Failed to download chunk {chunk_index} (bytes {start}-{end})"
                            )))
                        })?;

                    let chunk_last_modified = response
                        .last_modified()
                        .map_err(|e| ReadError::IOError(parse_error(e, &path)))?;

                    if chunk_last_modified != expected_last_modified {
                        return Err(ReadError::IOError(IOError::new(
                            ErrorKind::Unexpected,
                            format!(
                                "File was modified during multi-part download: expected last modified time {expected_last_modified:?}, got {chunk_last_modified:?}"
                            ),
                            path.clone(),
                        )));
                    }

                    let chunk_data: Bytes = response
                        .into_body()
                        .collect()
                        .await
                        .map_err(|e| ReadError::IOError(parse_error(e, &path)))?;

                    Ok::<(usize, Bytes), ReadError>((chunk_index, chunk_data))
                }
            })
            .collect();

        let download_stream = execute_with_parallelism(download_futures, 10).map(|result| {
            result
                .map_err(|join_err| {
                    ReadError::IOError(IOError::new(
                        ErrorKind::Unexpected,
                        format!("Task join error during parallel download: {join_err}"),
                        "parallel_download".to_string(),
                    ))
                })
                .and_then(|inner_result| inner_result)
        });
        tokio::pin!(download_stream);

        let bytes =
            crate::assemble_chunks(download_stream, file_size, DEFAULT_BYTES_PER_REQUEST).await?;

        Ok(bytes)
    }

    async fn list(
        &self,
        path: impl AsRef<str> + Send,
        page_size: Option<usize>,
    ) -> Result<futures::stream::BoxStream<'_, Result<Vec<FileInfo>, IOError>>, InvalidLocationError>
    {
        let path = format!("{}/", path.as_ref().trim_end_matches('/'));
        let adls_location = AdlsLocation::try_from_str(&path, true)
            .map_err(|e| e.with_context("List Operation failed"))?;
        let base_location = adls_location.location().clone();

        self.check_account(&adls_location)?;

        let container_client = self
            .blob_service_client
            .blob_container_client(adls_location.filesystem());

        // Prefix: strip leading '/' since blob names don't start with '/'
        let blob_prefix = adls_location
            .blob_name()
            .trim_start_matches('/')
            .to_string();
        // Ensure prefix ends with '/' so we only get items under this directory
        let prefix = if blob_prefix.is_empty() {
            None
        } else {
            let p = if blob_prefix.ends_with('/') {
                blob_prefix.clone()
            } else {
                format!("{blob_prefix}/")
            };
            Some(p)
        };

        let maxresults = page_size
            .and_then(|s| i32::try_from(s).ok())
            .and_then(|s| if s > 0 { Some(s) } else { None });

        let opts = BlobContainerClientListBlobsOptions {
            prefix,
            maxresults,
            // Include metadata so we can detect and skip ADLS Gen2 virtual directories
            // (identified by hdi_isfolder=true in their metadata).
            include: Some(vec![ListBlobsIncludeItem::Metadata]),
            ..Default::default()
        };

        // list_blobs() returns a Pager<ListBlobsResponse, XmlFormat> which implements Stream
        // yielding Result<BlobItem>. We want to emit pages of FileInfo.
        // Use into_pages() to iterate page-by-page.
        let pager = container_client
            .list_blobs(Some(opts))
            .map_err(|e| {
                InvalidLocationError::new(
                    path.clone(),
                    format!("Failed to create list_blobs pager: {e}"),
                )
            })?
            .into_pages();

        let stream = pager.map(move |page_result| {
            let base_location = base_location.clone();
            match page_result {
                Err(e) => {
                    let io_err =
                        parse_error(e, path.as_str()).with_context("Failed to list ADLS blobs");
                    if io_err.kind() == ErrorKind::NotFound {
                        Ok(vec![]) // Return empty list if path does not exist
                    } else {
                        Err(io_err)
                    }
                }
                Ok(page) => {
                    let page_model = page
                        .into_model()
                        .map_err(|e| parse_error(e, path.as_str()))?;
                    let file_infos = page_model
                        .segment
                        .blob_items
                        .into_iter()
                        .filter_map(|item| try_parse_file_info_from_blob_item(&base_location, item))
                        .collect::<Vec<_>>();
                    Ok(file_infos)
                }
            }
        });

        Ok(stream.boxed())
    }

    async fn remove_all(&self, path: impl AsRef<str>) -> Result<(), DeleteError> {
        let path = path.as_ref().trim_end_matches('/');
        let adls_location = AdlsLocation::try_from_str(path, true)?;

        require_key(&adls_location)?;
        self.check_account(&adls_location)?;

        // The new SDK has no recursive delete — list all blobs under the prefix and delete each.
        let container_client = self
            .blob_service_client
            .blob_container_client(adls_location.filesystem());

        let blob_prefix = adls_location
            .blob_name()
            .trim_start_matches('/')
            .to_string();
        let prefix = if blob_prefix.ends_with('/') {
            blob_prefix.clone()
        } else {
            format!("{blob_prefix}/")
        };

        let opts = BlobContainerClientListBlobsOptions {
            prefix: Some(prefix),
            // Include metadata so we can skip ADLS Gen2 virtual directory blobs.
            include: Some(vec![ListBlobsIncludeItem::Metadata]),
            ..Default::default()
        };

        let mut pager = container_client.list_blobs(Some(opts)).map_err(|e| {
            DeleteError::IOError(IOError::new(
                ErrorKind::Unexpected,
                format!("Failed to create list_blobs pager for remove_all: {e}"),
                path.to_string(),
            ))
        })?;

        // Collect all blob names first (skip virtual ADLS directory entries).
        let mut blob_names: Vec<String> = Vec::new();
        while let Some(blob_item) = pager.try_next().await.map_err(|e| {
            DeleteError::IOError(
                parse_error(e, path).with_context("Failed to list blobs during remove_all"),
            )
        })? {
            // Skip ADLS Gen2 virtual directory objects — deleting them while they
            // still contain blobs would result in a 409 DirectoryIsNotEmpty error.
            if is_adls_directory(&blob_item) {
                continue;
            }
            if let Some(name) = blob_item.name {
                blob_names.push(name);
            }
        }

        // Delete each blob
        for name in blob_names {
            let blob_client = self
                .blob_service_client
                .blob_client(adls_location.filesystem(), &name);
            blob_client
                .delete(None)
                .await
                .map_err(|e| DeleteError::IOError(parse_error(e, path)))?;
        }

        Ok(())
    }
}

fn try_parse_file_info_from_blob_item(
    base_location: &Location,
    item: azure_storage_blob::models::BlobItem,
) -> Option<FileInfo> {
    // Skip deleted blobs
    if item.deleted.unwrap_or(false) {
        return None;
    }

    // Skip ADLS Gen2 virtual directory objects (hdi_isfolder=true in metadata)
    if is_adls_directory(&item) {
        return None;
    }

    let name = item.name?;

    // Decode any percent-encoded characters that `AdlsLocation::blob_name()` may have
    // encoded when the file was written (e.g. `%3F` → `?`). Azure stores the encoded form
    // in the blob name, so we need to reverse the encoding to reconstruct the original path.
    let decoded_name = decode_blob_name_from_storage(&name);

    // Percent-encode characters that would be misinterpreted as URL structural markers
    // by Location::from_str's URL validation (i.e., '#' → %23).
    // Note: '?' does NOT need encoding here because Location stores the raw string and
    // the URL-parse validation does not fail on query-strings.
    let encoded_name = encode_blob_name_for_url(&decoded_name);

    let full_path = format!(
        "{}://{}/{}",
        base_location.scheme(),
        base_location.authority_with_host(),
        encoded_name
    );
    let location = Location::from_str(&full_path).ok()?;

    let last_modified = item
        .properties
        .as_ref()
        .and_then(|p| p.last_modified)
        .and_then(|lm| DateTime::from_timestamp(lm.unix_timestamp(), 0));

    Some(FileInfo {
        last_modified,
        location,
    })
}

/// Returns `true` if the blob item is an ADLS Gen2 virtual directory
/// (`hdi_isfolder = "true"` in its metadata).
fn is_adls_directory(item: &azure_storage_blob::models::BlobItem) -> bool {
    item.metadata
        .as_ref()
        .and_then(|m| m.additional_properties.as_ref())
        .and_then(|p| p.get("hdi_isfolder"))
        .is_some_and(|v| v.eq_ignore_ascii_case("true"))
}

/// Decodes percent-encoded characters that `AdlsLocation::blob_name()` encodes when
/// writing to Azure Storage. Specifically converts `%3F` → `?` and `%23` → `#`.
///
/// Azure stores the encoded form; we need the raw form to reconstruct the original path.
fn decode_blob_name_from_storage(name: &str) -> String {
    name.replace("%3F", "?")
        .replace("%3f", "?")
        .replace("%23", "#")
}

/// Percent-encodes characters in a blob name that would break URL parsing.
///
/// `#` must be encoded so that `Location::from_str` does not reject the URL
/// as having a fragment. `?` does NOT need encoding because `Location` stores
/// the raw string and the URL-parse validation does not fail on `?`.
fn encode_blob_name_for_url(name: &str) -> String {
    // Only '#' causes a hard failure in Location::from_str (fragment rejection).
    name.replace('#', "%23")
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

/// Groups paths by account and filesystem (container).
fn group_paths_by_container(
    paths: impl IntoIterator<Item = impl AsRef<str>>,
) -> Result<HashMap<(String, String), Vec<AdlsLocation>>, InvalidLocationError> {
    let mut grouped_paths: HashMap<(String, String), Vec<AdlsLocation>> = HashMap::new();

    for p in paths {
        let path = p.as_ref();
        let adls_location = AdlsLocation::try_from_str(path, true)?;

        require_key(&adls_location)?;

        let account = adls_location.account_name().to_string();
        let filesystem = adls_location.filesystem().to_string();

        grouped_paths
            .entry((account, filesystem))
            .or_default()
            .push(adls_location);
    }

    Ok(grouped_paths)
}
