use super::io::IoError;
use super::CommonMetadata;
use flate2::{write::GzEncoder, Compression};
use iceberg::spec::view_properties::METADATA_COMPRESSION;
use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};
use std::collections::HashMap;
use std::io::{Read as _, Write};

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
    pub fn compress(self, payload: &[u8]) -> Result<Vec<u8>, IoError> {
        match self {
            CompressionCodec::None => Ok(payload.to_vec()),
            CompressionCodec::Gzip => {
                let mut compressed_metadata = GzEncoder::new(Vec::new(), Compression::default());
                compressed_metadata
                    .write_all(payload)
                    .map_err(IoError::FileCompression)?;

                compressed_metadata
                    .finish()
                    .map_err(IoError::FileCompression)
            }
        }
    }

    pub fn decompress(self, payload: &[u8]) -> Result<Vec<u8>, IoError> {
        match self {
            CompressionCodec::None => Ok(payload.to_vec()),
            CompressionCodec::Gzip => {
                let mut decompressed_metadata = Vec::new();
                let mut decoder = flate2::read::GzDecoder::new(payload);
                decoder
                    .read_to_end(&mut decompressed_metadata)
                    .map_err(IoError::FileDecompression)?;

                Ok(decompressed_metadata)
            }
        }
    }

    pub fn as_file_extension(self) -> &'static str {
        match self {
            CompressionCodec::None => "",
            CompressionCodec::Gzip => ".gz",
        }
    }

    pub fn try_from_properties(
        properties: &HashMap<String, String>,
    ) -> Result<Self, UnsupportedCompressionCodec> {
        properties
            .get(METADATA_COMPRESSION)
            .map(String::as_str)
            .map_or(Ok(Self::default()), |value| match value {
                "gzip" => Ok(Self::Gzip),
                "none" => Ok(Self::None),
                unknown => Err(UnsupportedCompressionCodec(unknown.into())),
            })
    }

    pub fn try_from_maybe_properties(
        maybe_properties: Option<&HashMap<String, String>>,
    ) -> Result<Self, UnsupportedCompressionCodec> {
        match maybe_properties {
            Some(properties) => Self::try_from_properties(properties),
            None => Ok(Self::default()),
        }
    }

    pub fn try_from_metadata<T: CommonMetadata>(
        metadata: &T,
    ) -> Result<Self, UnsupportedCompressionCodec> {
        Self::try_from_properties(metadata.properties())
    }
}
