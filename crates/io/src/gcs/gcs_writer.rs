use bytes::{Bytes, BytesMut};
use google_cloud_gax::retry_policy::RetryPolicyExt as _;
use google_cloud_storage::{
    client::Storage, retry_policy::RetryableErrors, streaming_source::StreamingSource,
};
use tokio::{sync::mpsc, task::JoinHandle};
use tracing::Instrument as _;

use crate::{
    ErrorKind, IOError, LakekeeperFileWrite, WriteError,
    gcs::{
        GcsLocation, RESUMABLE_UPLOAD_THRESHOLD, UPLOAD_RETRY_TIME_LIMIT,
        gcs_error::parse_error,
        gcs_storage::{bucket_resource, upload_bytes},
    },
};

/// Messages queued between the upload task and the writer.
const CHANNEL_CAPACITY: usize = 4;

enum WriterMessage {
    Data(Bytes),
    /// All data was sent; the upload may be finalized.
    Finish,
}

#[derive(Debug, thiserror::Error)]
#[error("GCS writer was dropped or failed before it was closed")]
struct WriterNotClosed;

/// Upload source fed by a [`GcsFileWrite`].
///
/// It ends only after [`WriterMessage::Finish`]. A writer that goes away without finishing
/// closes the channel, which yields an error, so the SDK never finalizes a truncated object.
struct ChannelSource {
    receiver: mpsc::Receiver<WriterMessage>,
    finished: bool,
}

impl StreamingSource for ChannelSource {
    type Error = WriterNotClosed;

    async fn next(&mut self) -> Option<Result<Bytes, Self::Error>> {
        if self.finished {
            return None;
        }
        match self.receiver.recv().await {
            Some(WriterMessage::Data(bytes)) => Some(Ok(bytes)),
            Some(WriterMessage::Finish) => {
                self.finished = true;
                None
            }
            None => Some(Err(WriterNotClosed)),
        }
    }
}

/// Streaming writer for GCS.
///
/// Buffers bytes locally. A writer closed below `RESUMABLE_UPLOAD_THRESHOLD` uploads the
/// buffer in one request. Once the threshold is reached, a background task streams the
/// data into a resumable upload, which the SDK finalizes on `close`.
pub(crate) struct GcsFileWrite {
    storage: Storage,
    location: GcsLocation,
    state: GcsWriterState,
}

enum GcsWriterState {
    Buffering(BytesMut),
    Streaming {
        sender: mpsc::Sender<WriterMessage>,
        upload: JoinHandle<Result<(), WriteError>>,
    },
    Closed,
    Aborted,
}

impl std::fmt::Debug for GcsFileWrite {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GcsFileWrite")
            .field("location", &self.location)
            .field("state", &self.state)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for GcsWriterState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GcsWriterState::Buffering(buffer) => f
                .debug_tuple("Buffering")
                .field(&format_args!("{} bytes", buffer.len()))
                .finish(),
            GcsWriterState::Streaming { .. } => f.write_str("Streaming"),
            GcsWriterState::Closed => f.write_str("Closed"),
            GcsWriterState::Aborted => f.write_str("Aborted"),
        }
    }
}

impl GcsFileWrite {
    pub(crate) fn new(storage: Storage, location: GcsLocation) -> Self {
        Self {
            storage,
            location,
            state: GcsWriterState::Buffering(BytesMut::new()),
        }
    }

    /// Starts the resumable upload with `first` as its first data. Nothing is awaited, so a
    /// cancelled `write` cannot lose the buffered bytes.
    fn start_upload(
        &self,
        first: Bytes,
    ) -> (
        mpsc::Sender<WriterMessage>,
        JoinHandle<Result<(), WriteError>>,
    ) {
        let (sender, receiver) = mpsc::channel(CHANNEL_CAPACITY);
        sender
            .try_send(WriterMessage::Data(first))
            .expect("a new channel has room for one message");
        let source = ChannelSource {
            receiver,
            finished: false,
        };
        let request = self
            .storage
            .write_object(
                bucket_resource(&self.location),
                self.location.object_name(),
                source,
            )
            .with_retry_policy(RetryableErrors.with_time_limit(UPLOAD_RETRY_TIME_LIMIT));
        let location = self.location.clone();
        let upload = tokio::spawn(
            async move {
                Box::pin(request.send_buffered())
                    .await
                    .map(|_| ())
                    .map_err(|e| {
                        parse_error(e, location.as_str())
                            .with_context("Failed to upload object.")
                            .into()
                    })
            }
            .in_current_span(),
        );
        (sender, upload)
    }

    /// Hands `message` to the upload task. If the task has stopped, the writer is aborted
    /// and the upload error is returned.
    async fn send(&mut self, message: WriterMessage) -> Result<(), WriteError> {
        let GcsWriterState::Streaming { sender, .. } = &self.state else {
            return Ok(());
        };
        if sender.send(message).await.is_ok() {
            return Ok(());
        }
        let GcsWriterState::Streaming { mut upload, .. } =
            std::mem::replace(&mut self.state, GcsWriterState::Aborted)
        else {
            unreachable!("state was checked above");
        };
        Err(self
            .upload_result(&mut upload)
            .await
            .err()
            .unwrap_or_else(|| {
                WriteError::IOError(IOError::new(
                    ErrorKind::Unexpected,
                    "GCS upload stopped before all data was sent",
                    self.location.to_string(),
                ))
            }))
    }

    async fn upload_result(
        &self,
        upload: &mut JoinHandle<Result<(), WriteError>>,
    ) -> Result<(), WriteError> {
        upload.await.unwrap_or_else(|join_error| {
            Err(WriteError::IOError(IOError::new(
                ErrorKind::Unexpected,
                format!("GCS upload task failed: {join_error}"),
                self.location.to_string(),
            )))
        })
    }
}

#[async_trait::async_trait]
impl LakekeeperFileWrite for GcsFileWrite {
    async fn write(&mut self, bytes_in: Bytes) -> Result<(), WriteError> {
        match &mut self.state {
            GcsWriterState::Closed => Err(WriteError::IOError(IOError::new(
                ErrorKind::ConditionNotMatch,
                "Cannot write to closed writer",
                self.location.to_string(),
            ))),
            GcsWriterState::Aborted => Err(WriteError::IOError(IOError::new(
                ErrorKind::ConditionNotMatch,
                "Cannot write to aborted writer",
                self.location.to_string(),
            ))),
            GcsWriterState::Buffering(buffer) => {
                buffer.extend_from_slice(&bytes_in);
                if buffer.len() < RESUMABLE_UPLOAD_THRESHOLD {
                    return Ok(());
                }
                let buffered = std::mem::take(buffer).freeze();
                let (sender, upload) = self.start_upload(buffered);
                self.state = GcsWriterState::Streaming { sender, upload };
                Ok(())
            }
            GcsWriterState::Streaming { .. } => self.send(WriterMessage::Data(bytes_in)).await,
        }
    }

    async fn close(&mut self) -> Result<(), WriteError> {
        if matches!(self.state, GcsWriterState::Streaming { .. }) {
            self.send(WriterMessage::Finish).await?;
        }
        match std::mem::replace(&mut self.state, GcsWriterState::Closed) {
            GcsWriterState::Closed | GcsWriterState::Aborted => {
                Err(WriteError::IOError(IOError::new(
                    ErrorKind::ConditionNotMatch,
                    "Writer already closed or aborted",
                    self.location.to_string(),
                )))
            }
            GcsWriterState::Buffering(buffer) => {
                upload_bytes(&self.storage, &self.location, buffer.freeze()).await
            }
            GcsWriterState::Streaming { sender, upload } => {
                drop(sender);
                // A cancelled `close` aborts the upload.
                let mut upload = AbortOnDrop(upload);
                self.upload_result(&mut upload.0).await
            }
        }
    }
}

/// Aborts the task when dropped.
struct AbortOnDrop<T>(JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl Drop for GcsFileWrite {
    fn drop(&mut self) {
        // Nothing is finalized: the source never ends without `Finish`, so GCS discards the
        // unfinished resumable session when it expires.
        if let GcsWriterState::Streaming { upload, .. } = &self.state {
            upload.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use axum::{
        Router,
        http::{HeaderMap, Method, StatusCode},
        response::IntoResponse,
    };
    use google_cloud_auth::credentials::anonymous;

    use super::*;
    use crate::gcs::CHUNK_SIZE;

    /// Resumable upload endpoint that records the `Content-Range` of every chunk.
    async fn mock_resumable_endpoint() -> (String, mpsc::UnboundedReceiver<String>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let (ranges, received) = mpsc::unbounded_channel();
        let session = format!("{base}/upload-session");
        let app = Router::new().fallback(move |method: Method, headers: HeaderMap| {
            let ranges = ranges.clone();
            let session = session.clone();
            async move {
                if method == Method::POST {
                    return (StatusCode::OK, [("location", session)]).into_response();
                }
                let content_range = headers["content-range"].to_str().unwrap().to_string();
                ranges.send(content_range.clone()).unwrap();
                if !content_range.ends_with("/*") {
                    // The request names the total size: the upload is finalized.
                    return (StatusCode::OK, "{}").into_response();
                }
                // "bytes 0-N/*": the server confirms bytes up to N.
                let end = content_range
                    .trim_start_matches("bytes 0-")
                    .split('/')
                    .next()
                    .unwrap()
                    .to_string();
                (
                    StatusCode::PERMANENT_REDIRECT,
                    [("range", format!("bytes=0-{end}"))],
                )
                    .into_response()
            }
        });
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (base, received)
    }

    #[tokio::test]
    async fn upload_without_finish_is_never_finalized() {
        let (base, mut ranges) = mock_resumable_endpoint().await;
        let storage = Storage::builder()
            .with_endpoint(base)
            .with_credentials(anonymous::Builder::new().build())
            .with_resumable_upload_buffer_size(CHUNK_SIZE)
            .build()
            .await
            .unwrap();

        let (sender, receiver) = mpsc::channel(CHANNEL_CAPACITY);
        sender
            .send(WriterMessage::Data(Bytes::from(vec![0; CHUNK_SIZE + 1024])))
            .await
            .unwrap();
        drop(sender);
        let source = ChannelSource {
            receiver,
            finished: false,
        };

        let upload = storage
            .write_object("projects/_/buckets/test-bucket", "object", source)
            .send_buffered();
        let err = Box::pin(upload).await.unwrap_err();
        assert!(err.is_serialization(), "{err:?}");

        let mut sent = Vec::new();
        while let Ok(range) = ranges.try_recv() {
            sent.push(range);
        }
        // Only the full first chunk was sent; a finalizing request names the total size.
        assert_eq!(sent, vec![format!("bytes 0-{}/*", CHUNK_SIZE - 1)]);
    }

    fn source() -> (mpsc::Sender<WriterMessage>, ChannelSource) {
        let (sender, receiver) = mpsc::channel(CHANNEL_CAPACITY);
        (
            sender,
            ChannelSource {
                receiver,
                finished: false,
            },
        )
    }

    #[tokio::test]
    async fn source_without_finish_errors() {
        let (sender, mut source) = source();
        sender
            .send(WriterMessage::Data(Bytes::from_static(b"a")))
            .await
            .unwrap();
        drop(sender);

        assert_eq!(
            source.next().await.unwrap().unwrap(),
            Bytes::from_static(b"a")
        );
        assert!(matches!(source.next().await, Some(Err(WriterNotClosed))));
        // The SDK may ask again after an error: the source must not end now.
        assert!(matches!(source.next().await, Some(Err(WriterNotClosed))));
    }

    #[tokio::test]
    async fn source_with_finish_ends() {
        let (sender, mut source) = source();
        sender
            .send(WriterMessage::Data(Bytes::from_static(b"a")))
            .await
            .unwrap();
        sender.send(WriterMessage::Finish).await.unwrap();
        drop(sender);

        assert_eq!(
            source.next().await.unwrap().unwrap(),
            Bytes::from_static(b"a")
        );
        assert!(source.next().await.is_none());
        assert!(source.next().await.is_none());
    }
}
