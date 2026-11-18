use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap},
    io::{Read as _, Write},
    sync::Arc,
};

use flate2::{Compression, read::MultiGzDecoder, write::GzEncoder};
use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};

use super::{MetadataProperties, io::IOErrorExt};

pub trait PropertyMap {
    fn get_property(&self, key: &str) -> Option<&String>;
}

impl PropertyMap for HashMap<String, String> {
    fn get_property(&self, key: &str) -> Option<&String> {
        self.get(key)
    }
}

impl PropertyMap for BTreeMap<String, String> {
    fn get_property(&self, key: &str) -> Option<&String> {
        self.get(key)
    }
}

impl<T: PropertyMap> PropertyMap for Arc<T> {
    fn get_property(&self, key: &str) -> Option<&String> {
        self.as_ref().get_property(key)
    }
}

pub(crate) const PROPERTY_METADATA_COMPRESSION_CODEC: &str = "write.metadata.compression-codec";

/// Most bytes a gzip-compressed file is decompressed to. A larger one is refused: a small
/// compressed file can expand without bound.
const MAX_DECOMPRESSED_BYTES: u64 = 512 * 1024 * 1024;

const GZIP_MAGIC: [u8; 2] = [0x1F, 0x8B];

/// A metadata file's content, decompressed if it is gzip-compressed. Compression is told by the
/// gzip magic number, not by the file name.
///
/// # Errors
/// Fails on a corrupt gzip stream, or one that decompresses to more than 512 `MiB`.
pub fn decompress_metadata(content: &[u8]) -> std::io::Result<Cow<'_, [u8]>> {
    if content.starts_with(&GZIP_MAGIC) {
        gunzip(content, MAX_DECOMPRESSED_BYTES).map(Cow::Owned)
    } else {
        Ok(Cow::Borrowed(content))
    }
}

fn gunzip(payload: &[u8], limit: u64) -> std::io::Result<Vec<u8>> {
    let mut decompressed = Vec::new();
    MultiGzDecoder::new(payload)
        .take(limit + 1)
        .read_to_end(&mut decompressed)?;
    if decompressed.len() as u64 > limit {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("gzip stream decompresses to more than {limit} bytes"),
        ));
    }
    Ok(decompressed)
}

#[derive(thiserror::Error, Debug)]
#[error("Unsupported compression codec: {0}")]
pub struct UnsupportedCompressionCodec(String);

impl From<UnsupportedCompressionCodec> for IcebergErrorResponse {
    fn from(value: UnsupportedCompressionCodec) -> Self {
        let typ = "UnsupportedCompressionCodec";
        let boxed = Box::new(value);
        let message = boxed.to_string();

        ErrorModel::bad_request(message, typ, Some(boxed)).into()
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub enum CompressionCodec {
    None,
    #[default]
    Gzip,
}

impl CompressionCodec {
    pub async fn compress(self, payload: Vec<u8>) -> Result<Vec<u8>, IOErrorExt> {
        match self {
            CompressionCodec::None => Ok(payload),
            CompressionCodec::Gzip => tokio::task::spawn_blocking(move || {
                let mut compressed_metadata = GzEncoder::new(Vec::new(), Compression::default());
                compressed_metadata
                    .write_all(&payload)
                    .map_err(|e| IOErrorExt::FileCompression(Box::new(e)))?;

                compressed_metadata
                    .finish()
                    .map_err(|e| IOErrorExt::FileCompression(Box::new(e)))
            })
            .await
            .unwrap_or_else(|e| Err(IOErrorExt::FileCompression(Box::new(e)))),
        }
    }

    pub async fn decompress(self, payload: Vec<u8>) -> Result<Vec<u8>, IOErrorExt> {
        match self {
            CompressionCodec::None => Ok(payload),
            CompressionCodec::Gzip => tokio::task::spawn_blocking(move || {
                gunzip(&payload, MAX_DECOMPRESSED_BYTES)
                    .map_err(|e| IOErrorExt::FileDecompression(Box::new(e)))
            })
            .await
            .unwrap_or_else(|e| Err(IOErrorExt::FileDecompression(Box::new(e)))),
        }
    }

    pub fn as_file_extension(self) -> &'static str {
        match self {
            CompressionCodec::None => "",
            CompressionCodec::Gzip => ".gz",
        }
    }

    pub fn try_from_properties<M>(properties: &M) -> Result<Self, UnsupportedCompressionCodec>
    where
        M: PropertyMap + ?Sized,
    {
        properties
            .get_property(PROPERTY_METADATA_COMPRESSION_CODEC)
            .map(String::as_str)
            .map_or(Ok(Self::default()), |value| match value {
                "gzip" => Ok(Self::Gzip),
                "none" => Ok(Self::None),
                unknown => Err(UnsupportedCompressionCodec(unknown.into())),
            })
    }

    pub fn try_from_maybe_properties<M>(
        maybe_properties: Option<&M>,
    ) -> Result<Self, UnsupportedCompressionCodec>
    where
        M: PropertyMap + ?Sized,
    {
        match maybe_properties {
            Some(properties) => Self::try_from_properties(properties),
            None => Ok(Self::default()),
        }
    }

    pub fn try_from_metadata<T: MetadataProperties>(
        metadata: &T,
    ) -> Result<Self, UnsupportedCompressionCodec> {
        Self::try_from_properties(metadata.properties())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gzip(payload: &[u8]) -> Vec<u8> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(payload).unwrap();
        encoder.finish().unwrap()
    }

    #[test]
    fn test_decompress_metadata_tells_gzip_by_magic_number() {
        let json = br#"{"format-version":2}"#;
        assert!(matches!(decompress_metadata(json).unwrap(), Cow::Borrowed(c) if c == json));
        assert_eq!(&*decompress_metadata(&gzip(json)).unwrap(), json);
    }

    #[test]
    fn test_decompress_metadata_refuses_a_corrupt_stream() {
        let mut corrupt = gzip(br#"{"format-version":2}"#);
        corrupt.truncate(corrupt.len() - 4);
        assert_eq!(
            decompress_metadata(&corrupt).unwrap_err().kind(),
            std::io::ErrorKind::UnexpectedEof
        );
    }

    #[test]
    fn test_gunzip_refuses_more_than_the_limit() {
        let payload = gzip(&[0; 10]);
        assert_eq!(gunzip(&payload, 10).unwrap(), vec![0; 10]);
        assert_eq!(
            gunzip(&payload, 9).unwrap_err().kind(),
            std::io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn test_gunzip_counts_every_member_toward_the_limit() {
        let two_members = [gzip(&[0; 6]), gzip(&[0; 6])].concat();
        assert_eq!(gunzip(&two_members, 12).unwrap(), vec![0; 12]);
        assert_eq!(
            gunzip(&two_members, 11).unwrap_err().kind(),
            std::io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn test_gunzip_refuses_bytes_after_the_last_member() {
        let trailing = [gzip(&[0; 6]), b"not another gzip member".to_vec()].concat();
        assert_eq!(
            gunzip(&trailing, 12).unwrap_err().kind(),
            std::io::ErrorKind::InvalidInput
        );
    }
}
