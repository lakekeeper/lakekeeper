use crate::catalog::io::{IoError, DEFAULT_LIST_LOCATION_PAGE_SIZE};
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use hdfs_native::client::FileStatus;
use hdfs_native::{file::FileWriter, HdfsError, WriteOptions};
use std::path::PathBuf;
use std::sync::Arc;

pub(crate) async fn atomic_write(
    client: &hdfs_native::Client,
    file_path: &str,
    bytes: Arc<[bytes::Bytes]>,
    overwrite: bool,
) -> crate::api::Result<(), IoError> {
    let (mut writer, tmp_file_path) = open_tmp_file(client, file_path)
        .await
        .map_err(|e| IoError::FileWrite(Box::new(e)))?;
    for b in bytes.into_iter() {
        writer
            .write(b)
            .await
            .map_err(|e| IoError::FileWrite(Box::new(e)))?;
    }
    writer
        .close()
        .await
        .map_err(|e| IoError::FileWrite(Box::new(e)))?;

    // Rename the temporary file to the final file name
    client
        .rename(&tmp_file_path, file_path, overwrite)
        .await
        .map_err(|e| IoError::FileWrite(Box::new(e)))?;

    Ok(())
}

pub(crate) async fn list_dir(
    client: &hdfs_native::Client,
    location: &str,
    page_size: Option<usize>,
) -> crate::api::Result<BoxStream<'static, std::result::Result<Vec<String>, IoError>>, IoError> {
    Ok(list_raw(client, location)
        .map_ok(|status| status.path)
        .chunks(page_size.unwrap_or(DEFAULT_LIST_LOCATION_PAGE_SIZE))
        .map(|c| {
            c.into_iter()
                .collect::<crate::api::Result<Vec<String>, _>>()
                .map_err(|e| {
                    IoError::List(
                        iceberg::Error::new(
                            iceberg::ErrorKind::RequirementFailed,
                            format!("Failed to list hdfs file: {e}"),
                        )
                        .with_source(e),
                    )
                })
        })
        .boxed())
}

pub(crate) fn list_raw(
    client: &hdfs_native::Client,
    location: &str,
) -> BoxStream<'static, Result<FileStatus, IoError>> {
    let its = client.list_status_iter(location, true);
    its.into_stream()
        .filter(|res| {
            futures::future::ready(match res {
                Ok(status) => !status.isdir,
                // Listing by prefix should just return an empty list if the prefix isn't found
                Err(HdfsError::FileNotFound(_)) => false,
                _ => true,
            })
        })
        .map_err(|e| IoError::FileRead(Box::new(e)))
        .boxed()
}

pub(crate) async fn get(
    client: &hdfs_native::Client,
    location: &str,
) -> crate::api::Result<Vec<u8>, IoError> {
    let (mut stream, size) = get_raw(client, location).await?;
    let mut buf = Vec::with_capacity(size);
    while let Some(next) = stream.next().await.transpose()? {
        buf.extend_from_slice(next.as_ref());
    }
    Ok(buf)
}

pub(crate) async fn get_raw(
    client: &hdfs_native::Client,
    location: &str,
) -> crate::api::Result<(BoxStream<'static, Result<bytes::Bytes, IoError>>, usize), IoError> {
    let meta = client
        .get_file_info(location)
        .await
        .map_err(|e| IoError::FileRead(Box::new(e)))?;
    let reader = client
        .read(location)
        .await
        .map_err(|e| IoError::FileRead(Box::new(e)))?;
    let stream = reader
        .read_range_stream(0, meta.length)
        .map_err(|e| IoError::FileRead(Box::new(e)))
        .boxed();
    Ok((stream, meta.length))
}

async fn open_tmp_file(
    client: &hdfs_native::Client,
    file_path: &str,
) -> hdfs_native::Result<(FileWriter, String)> {
    let path_buf = PathBuf::from(file_path);

    let file_name = path_buf
        .file_name()
        .ok_or(HdfsError::InvalidPath("path missing filename".to_string()))?
        .to_str()
        .ok_or(HdfsError::InvalidPath("path not valid unicode".to_string()))?
        .to_string();

    let tmp_file_path = path_buf
        .with_file_name(format!(".{file_name}.tmp"))
        .to_str()
        .ok_or(HdfsError::InvalidPath("path not valid unicode".to_string()))?
        .to_string();

    // Try to create a file with an incrementing index until we find one that doesn't exist yet
    let mut index = 1;
    loop {
        let path = format!("{tmp_file_path}.{index}");
        match client.create(&path, WriteOptions::default()).await {
            Ok(writer) => break Ok((writer, path)),
            Err(HdfsError::AlreadyExists(_)) => index += 1,
            Err(e) => break Err(e),
        }
    }
}
