use std::sync::{Arc, LazyLock};

use futures::{StreamExt, stream::BoxStream};
use iceberg::spec::TableMetadata;
use iceberg_ext::catalog::rest::IcebergErrorResponse;
use lakekeeper_io::{
    DeleteError, IOError, InvalidLocationError, LakekeeperStorage, Location, ReadError, WriteError,
};
use serde::Serialize;
use tokio::sync::Semaphore;

use super::compression_codec::CompressionCodec;
use crate::{
    CONFIG,
    api::{ErrorModel, Result},
    service::COMMIT_STATE_UNKNOWN_ERROR_TYPE,
};

pub(crate) async fn write_file(
    io: &impl LakekeeperStorage,
    location: &Location,
    data: impl Serialize,
    compression_codec: CompressionCodec,
) -> Result<(), IOErrorExt> {
    tracing::debug!("Writing file to {}", location);
    let buf = serde_json::to_vec(&data).map_err(IOErrorExt::Serialization)?;
    let metadata_bytes = compression_codec.compress(buf).await?;

    io.write(location.as_str(), metadata_bytes.into())
        .await
        .map(|_| ())
        .map_err(Into::into)
}

pub(crate) async fn delete_file(
    io: &impl LakekeeperStorage,
    location: &Location,
) -> Result<(), IOErrorExt> {
    io.delete(location.as_str()).await.map_err(Into::into)
}

/// How long a request waits for its best-effort cleanup before it responds.
const CLEANUP_WAIT: std::time::Duration = std::time::Duration::from_secs(5);

/// Most background cleanups that run at once in this process. It bounds the connections they
/// hold when the storage slows down while commits keep arriving.
const MAX_BACKGROUND_CLEANUPS: usize = 256;

static BACKGROUND_CLEANUPS: LazyLock<Arc<Semaphore>> =
    LazyLock::new(|| Arc::new(Semaphore::new(MAX_BACKGROUND_CLEANUPS)));

/// Runs a best-effort `cleanup` of a request that arrived at `received_at` with
/// [`spawn_cleanup`], and waits for it at most until [`cleanup_deadline`] with [`CLEANUP_WAIT`].
/// A cleanup that takes longer continues in the background, so a slow or retried delete does not
/// decide the response.
pub(crate) async fn cleanup_in_background(
    received_at: tokio::time::Instant,
    cleanup: impl Future<Output = ()> + Send + 'static,
) {
    if let Some(cleanup) = spawn_cleanup(cleanup) {
        let _ = tokio::time::timeout_at(request_cleanup_deadline(received_at), cleanup).await;
    }
}

/// Runs a best-effort `cleanup` of a request that arrived at `received_at` until
/// [`cleanup_deadline`] with [`CLEANUP_WAIT`], and stops it there. Returns whether it finished.
/// For a cleanup that must end before the client can send its next request.
pub(crate) async fn cleanup_before_response(
    received_at: tokio::time::Instant,
    cleanup: impl Future<Output = ()>,
) -> bool {
    tokio::time::timeout_at(request_cleanup_deadline(received_at), cleanup)
        .await
        .is_ok()
}

/// Runs a best-effort `cleanup` in the background. If [`MAX_BACKGROUND_CLEANUPS`] cleanups are
/// running, it is skipped and the files it would delete stay in storage.
pub(crate) fn spawn_cleanup(
    cleanup: impl Future<Output = ()> + Send + 'static,
) -> Option<tokio::task::JoinHandle<()>> {
    spawn_limited(&BACKGROUND_CLEANUPS, cleanup)
}

fn spawn_limited(
    limit: &Arc<Semaphore>,
    cleanup: impl Future<Output = ()> + Send + 'static,
) -> Option<tokio::task::JoinHandle<()>> {
    let Ok(permit) = Arc::clone(limit).try_acquire_owned() else {
        tracing::warn!(
            "Skipped a best-effort cleanup because too many cleanups are running. The files it would delete stay in storage."
        );
        return None;
    };
    let cleanup = tracing::Instrument::in_current_span(async move {
        cleanup.await;
        drop(permit);
    });
    Some(tokio::spawn(cleanup))
}

fn request_cleanup_deadline(received_at: tokio::time::Instant) -> tokio::time::Instant {
    cleanup_deadline(
        tokio::time::Instant::now(),
        CLEANUP_WAIT,
        received_at,
        CONFIG.max_request_time,
    )
}

/// `wait` from `now`, ending by five sixths of the request limit from the request's arrival so
/// that a slow backend cannot time out the response of a committed change.
pub(crate) fn cleanup_deadline(
    now: tokio::time::Instant,
    wait: std::time::Duration,
    received_at: tokio::time::Instant,
    request_limit: std::time::Duration,
) -> tokio::time::Instant {
    // Divided first so that an effectively unbounded limit cannot overflow.
    [
        now.checked_add(wait),
        received_at.checked_add(request_limit / 6 * 5),
    ]
    .into_iter()
    .flatten()
    .min()
    .unwrap_or_else(crate::service::storage::validation::far_future)
}

/// Run `change`, which commits the files written to `locations`. If it fails, delete them, best
/// effort, unless the commit may have been applied. Nothing refers to the files, so the error
/// returns without waiting for the delete, which runs with [`spawn_cleanup`].
pub(crate) async fn delete_on_failure<T>(
    io: &(impl LakekeeperStorage + Clone + 'static),
    locations: &[&Location],
    change: impl Future<Output = Result<T>>,
) -> Result<T> {
    let result = change.await;
    if let Err(e) = &result {
        if e.error.r#type == COMMIT_STATE_UNKNOWN_ERROR_TYPE {
            tracing::warn!(
                locations = ?locations.iter().map(|l| l.as_str()).collect::<Vec<_>>(),
                "Commit may have been applied, keeping its new metadata files"
            );
        } else {
            let io = io.clone();
            let locations: Vec<Location> = locations.iter().map(|l| (*l).clone()).collect();
            spawn_cleanup(async move { delete_files(&io, &locations).await });
        }
    }
    result
}

/// Delete files, best effort: failures are logged.
pub(crate) async fn delete_files<'a>(
    io: &impl LakekeeperStorage,
    locations: impl IntoIterator<Item = &'a Location>,
) {
    let results = futures::future::join_all(
        locations
            .into_iter()
            .map(|location| async move { (location, delete_file(io, location).await) }),
    )
    .await;
    for (location, result) in results {
        if let Err(e) = result {
            tracing::warn!("Failed to delete file {location}: {e:?}");
        }
    }
}

pub(crate) async fn read_file(
    io: &impl LakekeeperStorage,
    file: &Location,
    compression_codec: CompressionCodec,
) -> Result<Vec<u8>, IOErrorExt> {
    let content: Vec<_> = io.read(file.as_str()).await.map(|o| o.bytes.into())?;

    if matches!(compression_codec, CompressionCodec::None) {
        Ok(content)
    } else {
        let content = compression_codec
            .decompress(content)
            .await
            .map_err(|e| IOErrorExt::FileDecompression(Box::new(e)))?;
        tracing::debug!("Read file {} with codec {:?}", file, compression_codec);
        Ok(content)
    }
}

pub(crate) async fn read_metadata_file(
    io: &impl LakekeeperStorage,
    file: &Location,
) -> Result<TableMetadata, IOErrorExt> {
    let compression_codec = if file.as_str().ends_with(".gz.metadata.json") {
        CompressionCodec::Gzip
    } else {
        CompressionCodec::None
    };

    let content = read_file(io, file, compression_codec).await?;
    match tokio::task::spawn_blocking(move || {
        serde_json::from_slice(&content).map_err(IOErrorExt::Deserialization)
    })
    .await
    {
        Ok(result) => result,
        Err(e) => Err(IOErrorExt::FileDecompression(Box::new(e))),
    }
}

pub(crate) async fn remove_all(
    io: &impl LakekeeperStorage,
    location: &Location,
) -> Result<(), IOErrorExt> {
    io.remove_all(location.as_str()).await.map_err(Into::into)
}

pub(crate) async fn list_location<'a>(
    io: &'a impl LakekeeperStorage,
    location: &'a Location,
    page_size: Option<usize>,
) -> Result<BoxStream<'a, std::result::Result<Vec<Location>, IOError>>, InvalidLocationError> {
    tracing::debug!("Listing location: {}", location);
    let entries = io.list(location.as_str(), page_size).await?;
    let entries = entries
        .map(|entry| {
            entry.map(|file_infos| {
                file_infos
                    .into_iter()
                    .map(|file_info| file_info.location().clone())
                    .collect::<Vec<_>>()
            })
        })
        .boxed();
    Ok(entries)
}

#[derive(thiserror::Error, Debug, strum::IntoStaticStr)]
pub enum IOErrorExt {
    #[error("Failed to serialize data: {0}")]
    Serialization(#[source] serde_json::Error),
    #[error("Failed to deserialize table metadata: {0}")]
    Deserialization(#[source] serde_json::Error),
    #[error("Failed to finish compressing file: {0}")]
    FileCompression(#[source] Box<dyn std::error::Error + Sync + Send + 'static>),
    #[error("Failed to finish decompressing file: {0}")]
    FileDecompression(#[source] Box<dyn std::error::Error + Sync + Send + 'static>),
    #[error("Invalid file location: {0}")]
    InvalidLocation(#[from] InvalidLocationError),
    #[error("{0}")]
    IOError(#[from] IOError),
}

impl From<WriteError> for IOErrorExt {
    fn from(value: WriteError) -> Self {
        match value {
            WriteError::IOError(e) => e.into(),
            WriteError::InvalidLocation(e) => e.into(),
        }
    }
}

impl From<DeleteError> for IOErrorExt {
    fn from(value: DeleteError) -> Self {
        match value {
            DeleteError::IOError(e) => e.into(),
            DeleteError::InvalidLocation(e) => e.into(),
        }
    }
}

impl From<ReadError> for IOErrorExt {
    fn from(value: ReadError) -> Self {
        match value {
            ReadError::IOError(e) => e.into(),
            ReadError::InvalidLocation(e) => e.into(),
        }
    }
}

impl IOErrorExt {
    pub fn to_type(&self) -> &'static str {
        self.into()
    }
}

impl From<IOErrorExt> for ErrorModel {
    fn from(value: IOErrorExt) -> Self {
        let typ = value.to_type();
        let message = value.to_string();

        match value {
            IOErrorExt::FileDecompression(e) => {
                ErrorModel::failed_dependency(message, typ, Some(e))
            }
            IOErrorExt::FileCompression(e) => ErrorModel::internal(message, typ, Some(e)),
            IOErrorExt::Serialization(e) => ErrorModel::internal(message, typ, Some(Box::new(e))),
            IOErrorExt::Deserialization(e) => {
                ErrorModel::bad_request(message, typ, Some(Box::new(e)))
            }
            IOErrorExt::InvalidLocation(e) => {
                ErrorModel::bad_request(message, typ, Some(Box::new(e)))
            }
            IOErrorExt::IOError(e) => {
                ErrorModel::from_io_error(e, "Lakekeeper IO operation failed.")
            }
        }
    }
}

impl From<IOErrorExt> for IcebergErrorResponse {
    fn from(value: IOErrorExt) -> Self {
        let error_model: ErrorModel = value.into();
        IcebergErrorResponse::from(error_model)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    };

    use futures::StreamExt;

    use super::*;
    use crate::service::storage::{StorageCredential, StorageProfile};

    #[test]
    fn the_cleanup_deadline_fits_the_request_limit() {
        let limit = Duration::from_secs(30);
        let received_at = tokio::time::Instant::now();
        let at = |secs| received_at + Duration::from_secs(secs);
        let wait = Duration::from_secs(10);
        // Early in the request the cleanup gets its wait.
        assert_eq!(cleanup_deadline(at(1), wait, received_at, limit), at(11));
        // Late in the request it ends at five sixths of the limit.
        assert_eq!(cleanup_deadline(at(20), wait, received_at, limit), at(25));
        // An unbounded limit does not overflow, and the wait applies.
        assert_eq!(
            cleanup_deadline(at(0), wait, received_at, Duration::MAX),
            at(10)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_slow_cleanup_finishes_after_the_request_stops_waiting() {
        let done = Arc::new(AtomicBool::new(false));
        let start = tokio::time::Instant::now();
        cleanup_in_background(start, {
            let done = done.clone();
            async move {
                tokio::time::sleep(Duration::from_secs(20)).await;
                done.store(true, Ordering::SeqCst);
            }
        })
        .await;

        assert_eq!(start.elapsed(), CLEANUP_WAIT);
        assert!(!done.load(Ordering::SeqCst));
        tokio::time::sleep(Duration::from_secs(20)).await;
        assert!(done.load(Ordering::SeqCst));
    }

    #[tokio::test(start_paused = true)]
    async fn a_cleanup_late_in_the_request_stops_waiting_before_the_request_limit() {
        let start = tokio::time::Instant::now();
        // Five sixths of the request limit end 2 s from now.
        let received_at = (start + Duration::from_secs(2))
            .checked_sub(CONFIG.max_request_time / 6 * 5)
            .unwrap();
        cleanup_in_background(received_at, tokio::time::sleep(Duration::from_secs(20))).await;

        assert_eq!(start.elapsed(), Duration::from_secs(2));
    }

    #[tokio::test(start_paused = true)]
    async fn a_cleanup_beyond_the_limit_is_skipped() {
        let limit = Arc::new(Semaphore::new(1));
        let running = spawn_limited(&limit, tokio::time::sleep(Duration::from_secs(20))).unwrap();
        assert!(spawn_limited(&limit, async {}).is_none());

        running.await.unwrap();
        assert!(spawn_limited(&limit, async {}).is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn a_cleanup_before_the_response_stops_at_the_deadline() {
        let done = Arc::new(AtomicBool::new(false));
        let start = tokio::time::Instant::now();
        let finished = cleanup_before_response(start, {
            let done = done.clone();
            async move {
                tokio::time::sleep(Duration::from_secs(20)).await;
                done.store(true, Ordering::SeqCst);
            }
        })
        .await;

        assert!(!finished);
        assert_eq!(start.elapsed(), CLEANUP_WAIT);
        tokio::time::sleep(Duration::from_secs(20)).await;
        assert!(!done.load(Ordering::SeqCst));
    }

    #[tokio::test(start_paused = true)]
    async fn a_fast_cleanup_finishes_before_the_request_responds() {
        let done = Arc::new(AtomicBool::new(false));
        cleanup_in_background(tokio::time::Instant::now(), {
            let done = done.clone();
            async move {
                tokio::time::sleep(Duration::from_secs(1)).await;
                done.store(true, Ordering::SeqCst);
            }
        })
        .await;

        assert!(done.load(Ordering::SeqCst));
    }

    async fn test_list_and_remove_all(cred: StorageCredential, profile: StorageProfile) {
        async fn list_simple(io: &impl LakekeeperStorage, location: &Location) -> Vec<Location> {
            let list = list_location(io, location, Some(10)).await.unwrap();

            list.collect::<Vec<_>>()
                .await
                .into_iter()
                .map(Result::unwrap)
                .next()
                .unwrap_or_default()
        }

        let mut location = profile.base_location().unwrap();
        location.without_trailing_slash();

        let folder_1 = location.clone().push("folder").clone();
        let file_1 = folder_1.clone().push("file1").clone();
        let folder_2 = location.clone().push("folder-2").clone();
        let file_2 = folder_2.clone().push("file2").clone();

        let data_1 = serde_json::json!({"file": "1"});
        let data_2 = serde_json::json!({"file": "2"});

        let io = profile.file_io(Some(&cred)).await.unwrap();
        write_file(&io, &file_1, data_1, CompressionCodec::Gzip)
            .await
            .unwrap();
        write_file(&io, &file_2, data_2, CompressionCodec::Gzip)
            .await
            .unwrap();

        // Test list - when we list folder 1, we should not see anything related to folder-2
        let list_f1 = list_simple(&io, &folder_1).await;
        // Assert that one of the items contains file1
        assert!(list_f1.iter().any(|entry| entry.as_str().contains("file1")));
        // Assert that "folder-2" is nowhere in the list
        assert!(
            !list_f1
                .iter()
                .any(|entry| entry.as_str().contains("folder-2"))
        );

        // List full location - we should see both folders
        let list = list_simple(&io, &location).await;
        assert!(
            list.iter()
                .any(|entry| entry.as_str().contains("folder/file1"))
        );
        assert!(
            list.iter()
                .any(|entry| entry.as_str().contains("folder-2/file2"))
        );

        // Remove folder 1 - file 2 should still be here:
        remove_all(&io, &folder_1).await.unwrap();
        assert!(
            read_file(&io, &file_2, CompressionCodec::Gzip)
                .await
                .is_ok()
        );

        let list = list_simple(&io, &location).await;
        // Assert that "folder/" / file1 is gone
        assert!(!list.iter().any(|entry| entry.as_str().contains("file1")));
        // and that "folder-2/" / file2 is still here
        assert!(
            list.iter()
                .any(|entry| entry.as_str().contains("folder-2/file2"))
        );

        // Listing location 1 should return an empty list
        let folder_1_list = list_simple(&io, &folder_1).await;
        assert!(
            folder_1_list.is_empty(),
            "Folder 1 should be empty, got: {folder_1_list:?}"
        );

        // Cleanup
        remove_all(&io, &folder_2).await.unwrap();
    }

    pub(crate) mod aws_integration_tests {
        use super::*;
        use crate::service::storage::{
            StorageCredential, StorageProfile, s3::test::aws_integration_tests::get_storage_profile,
        };

        #[tokio::test]
        async fn test_remove_all_s3() {
            let (profile, cred) = get_storage_profile();
            let cred: StorageCredential = cred.into();
            let profile: StorageProfile = profile.into();

            test_list_and_remove_all(cred, profile).await;
        }
    }

    pub(crate) mod azure_integration_tests {
        use super::*;
        use crate::service::storage::{
            StorageCredential, StorageProfile,
            az::test::azure_integration_tests::{azure_profile, client_creds},
        };

        #[tokio::test]
        async fn test_remove_all_az() {
            let cred: StorageCredential = client_creds().into();
            let profile: StorageProfile = azure_profile().into();

            test_list_and_remove_all(cred, profile).await;
        }
    }

    pub(crate) mod gcs_integration_tests {
        use super::*;
        use crate::service::storage::{
            StorageCredential, StorageProfile,
            gcs::test::gcs_integration_tests::get_storage_profile,
        };

        #[tokio::test]
        async fn test_remove_all_gcs() {
            let (profile, cred) = get_storage_profile();
            let cred: StorageCredential = cred.into();
            let profile: StorageProfile = profile.into();

            test_list_and_remove_all(cred, profile).await;
        }
    }
}
