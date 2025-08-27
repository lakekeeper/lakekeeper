use std::str::FromStr;

use aws_sdk_s3::{
    error::SdkError,
    operation::{
        complete_multipart_upload::CompleteMultipartUploadError,
        create_multipart_upload::CreateMultipartUploadError, delete_object::DeleteObjectError,
        delete_objects::DeleteObjectsError, list_objects_v2::ListObjectsV2Error,
        put_object::PutObjectError, upload_part::UploadPartError,
    },
};

use crate::{error::ErrorKind, IOError};

pub(crate) fn parse_delete_error(err: SdkError<DeleteObjectError>, location: &str) -> IOError {
    let e = err.into_service_error();

    let lakekeeper_kind = e
        .meta()
        .code()
        .and_then(|s| S3ErrorCode::from_str(s).ok())
        .map_or(ErrorKind::Unexpected, |kind| kind.as_lakekeeper_kind());

    let msg = e.meta().message().map_or_else(
        || format!("Unknown S3 error during deletion: {e}"),
        |m| format!("S3 deletion failed: {m}"),
    );

    IOError::new(lakekeeper_kind, msg, location.to_string()).set_source(e)
}

pub(crate) fn parse_batch_delete_error(err: SdkError<DeleteObjectsError>) -> IOError {
    let e = err.into_service_error();

    let lakekeeper_kind = e
        .meta()
        .code()
        .and_then(|s| S3ErrorCode::from_str(s).ok())
        .map_or(ErrorKind::Unexpected, |kind| kind.as_lakekeeper_kind());

    let msg = e.meta().message().map_or_else(
        || format!("Unknown S3 error during batch deletion: {e}"),
        |m| format!("S3 batch deletion failed: {m}"),
    );

    IOError::new_without_location(lakekeeper_kind, msg).set_source(e)
}

pub(crate) fn parse_put_object_error(err: SdkError<PutObjectError>, location: &str) -> IOError {
    let e = err.into_service_error();

    let lakekeeper_kind = match e {
        PutObjectError::EncryptionTypeMismatch(_) => ErrorKind::ConditionNotMatch,
        PutObjectError::InvalidRequest(_) | PutObjectError::TooManyParts(_) => {
            ErrorKind::ConfigInvalid
        }
        PutObjectError::InvalidWriteOffset(_) => ErrorKind::Unexpected,
        _ => e
            .meta()
            .code()
            .and_then(|s| S3ErrorCode::from_str(s).ok())
            .map_or(ErrorKind::Unexpected, |kind| kind.as_lakekeeper_kind()),
    };

    let msg = e.meta().message().map_or_else(
        || format!("Unknown S3 error during write: {e}"),
        |m| format!("S3 write failed: {m}"),
    );

    IOError::new(lakekeeper_kind, msg, location.to_string()).set_source(e)
}

pub(crate) fn parse_create_multipart_upload_error(
    err: SdkError<CreateMultipartUploadError>,
    location: &str,
) -> IOError {
    let e = err.into_service_error();

    let lakekeeper_kind = e
        .meta()
        .code()
        .and_then(|s| S3ErrorCode::from_str(s).ok())
        .map_or(ErrorKind::Unexpected, |kind| kind.as_lakekeeper_kind());

    let msg = e.meta().message().map_or_else(
        || format!("Unknown S3 error during multipart upload creation: {e}"),
        |m| format!("S3 create multipart upload failed: {m}"),
    );

    IOError::new(lakekeeper_kind, msg, location.to_string()).set_source(e)
}

pub(crate) fn parse_upload_part_error(err: SdkError<UploadPartError>, location: &str) -> IOError {
    let e = err.into_service_error();

    let lakekeeper_kind = e
        .meta()
        .code()
        .and_then(|s| S3ErrorCode::from_str(s).ok())
        .map_or(ErrorKind::Unexpected, |kind| kind.as_lakekeeper_kind());

    let msg = e.meta().message().map_or_else(
        || format!("Unknown S3 error during part upload: {e}"),
        |m| format!("S3 upload part failed: {m}"),
    );

    IOError::new(lakekeeper_kind, msg, location.to_string()).set_source(e)
}

pub(crate) fn parse_complete_multipart_upload_error(
    err: SdkError<CompleteMultipartUploadError>,
    location: &str,
) -> IOError {
    let e = err.into_service_error();

    let lakekeeper_kind = e
        .meta()
        .code()
        .and_then(|s| S3ErrorCode::from_str(s).ok())
        .map_or(ErrorKind::Unexpected, |kind| kind.as_lakekeeper_kind());

    let msg = e.meta().message().map_or_else(
        || format!("Unknown S3 error during multipart upload completion: {e}"),
        |m| format!("S3 complete multipart upload failed: {m}"),
    );

    IOError::new(lakekeeper_kind, msg, location.to_string()).set_source(e)
}

pub(crate) fn parse_get_object_error(
    err: SdkError<aws_sdk_s3::operation::get_object::GetObjectError>,
    location: &str,
) -> IOError {
    let e = err.into_service_error();

    let lakekeeper_kind = e
        .meta()
        .code()
        .and_then(|s| S3ErrorCode::from_str(s).ok())
        .map_or(ErrorKind::Unexpected, |kind| kind.as_lakekeeper_kind());

    let msg = e.meta().message().map_or_else(
        || format!("Unknown S3 error during read: {e}"),
        |m| format!("S3 get failed: {m}"),
    );

    IOError::new(lakekeeper_kind, msg, location.to_string()).set_source(e)
}

pub(crate) fn parse_head_object_error(
    err: SdkError<aws_sdk_s3::operation::head_object::HeadObjectError>,
    location: &str,
) -> IOError {
    let e = err.into_service_error();

    let lakekeeper_kind = e
        .meta()
        .code()
        .and_then(|s| S3ErrorCode::from_str(s).ok())
        .map_or(ErrorKind::Unexpected, |kind| kind.as_lakekeeper_kind());

    let msg = e.meta().message().map_or_else(
        || format!("Unknown S3 error during head operation: {e}"),
        |m| format!("S3 head failed: {m}"),
    );

    IOError::new(lakekeeper_kind, msg, location.to_string()).set_source(e)
}

pub(crate) fn parse_list_objects_v2_error(
    err: SdkError<ListObjectsV2Error>,
    location: &str,
) -> IOError {
    let e = err.into_service_error();

    let lakekeeper_kind = e
        .meta()
        .code()
        .and_then(|s| S3ErrorCode::from_str(s).ok())
        .map_or(ErrorKind::Unexpected, |kind| kind.as_lakekeeper_kind());

    let msg = e.meta().message().map_or_else(
        || format!("Unknown S3 error during list: {e}"),
        |m| format!("S3 list failed: {m}"),
    );

    IOError::new(lakekeeper_kind, msg, location.to_string()).set_source(e)
}

pub(crate) fn parse_aws_sdk_error(err: &aws_sdk_s3::types::Error, location: &str) -> IOError {
    let lakekeeper_kind = err
        .code()
        .and_then(|s| S3ErrorCode::from_str(s).ok())
        .map_or(ErrorKind::Unexpected, |kind| kind.as_lakekeeper_kind());

    let msg = err.message().map_or_else(
        || format!("Unknown S3 SDK error: {err:?}"),
        |m| format!("S3 SDK error: {m}"),
    );

    IOError::new(lakekeeper_kind, msg, location.to_string())
}

#[derive(Debug, Clone, PartialEq, Eq, strum_macros::EnumString, strum_macros::Display)]
enum S3ErrorCode {
    SlowDown,
    AccessDenied,
    AllAccessDisabled,
    InvalidAccessKeyId,
    InvalidBucketName,
    #[strum(serialize = "KMS.DisabledException")]
    KMSDisabledException,
    #[strum(serialize = "KMS.NotFoundException")]
    KMSNotFoundException,
    NoSuchBucket,
    NoSuchKey,
    #[strum(serialize = "503 SlowDown")]
    SlowDown503,
    TokenRefreshRequired,
    UnauthorizedAccessError,
    RequestTimeout,
    InternalError,
    ServiceUnavailable,
}

impl S3ErrorCode {
    fn as_lakekeeper_kind(&self) -> ErrorKind {
        match self {
            S3ErrorCode::SlowDown | S3ErrorCode::SlowDown503 => ErrorKind::RateLimited,
            S3ErrorCode::AccessDenied
            | S3ErrorCode::AllAccessDisabled
            | S3ErrorCode::UnauthorizedAccessError => ErrorKind::PermissionDenied,
            S3ErrorCode::InvalidAccessKeyId
            | S3ErrorCode::InvalidBucketName
            | S3ErrorCode::KMSDisabledException
            | S3ErrorCode::KMSNotFoundException => ErrorKind::ConfigInvalid,
            S3ErrorCode::NoSuchBucket | S3ErrorCode::NoSuchKey => ErrorKind::NotFound,
            S3ErrorCode::TokenRefreshRequired => ErrorKind::CredentialsExpired,
            S3ErrorCode::RequestTimeout => ErrorKind::RequestTimeout,
            S3ErrorCode::ServiceUnavailable => ErrorKind::ServiceUnavailable,
            S3ErrorCode::InternalError => ErrorKind::Unexpected,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn test_known_s3_error_kinds() {
        let error_codes = [
            "SlowDown",
            "AccessDenied",
            "AllAccessDisabled",
            "InvalidAccessKeyId",
            "InvalidBucketName",
            "KMS.DisabledException",
            "KMS.NotFoundException",
            "NoSuchBucket",
            "NoSuchKey",
            "503 SlowDown",
            "TokenRefreshRequired",
            "UnauthorizedAccessError",
            "RequestTimeout",
            "InternalError",
            "ServiceUnavailable",
        ];

        for code in error_codes {
            assert!(S3ErrorCode::from_str(code).is_ok());
        }
    }
}
