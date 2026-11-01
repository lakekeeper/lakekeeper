use std::str::FromStr;

use aws_sdk_s3::{
    error::{ProvideErrorMetadata, SdkError},
    operation::{
        complete_multipart_upload::CompleteMultipartUploadError,
        create_multipart_upload::CreateMultipartUploadError, delete_object::DeleteObjectError,
        delete_objects::DeleteObjectsError, get_bucket_policy::GetBucketPolicyError,
        list_objects_v2::ListObjectsV2Error, put_object::PutObjectError,
        upload_part::UploadPartError,
    },
};

use crate::{ErrorKind, IOError};

/// Kind of an error without a service error: a timed out attempt, a failed connection or an
/// unreadable response. `None` for every other error; its kind comes from the service error.
fn transport_error_kind<E, R>(err: &SdkError<E, R>) -> Option<ErrorKind> {
    match err {
        SdkError::TimeoutError(_) => Some(ErrorKind::RequestTimeout),
        SdkError::DispatchFailure(failure) if failure.is_timeout() => {
            Some(ErrorKind::RequestTimeout)
        }
        SdkError::DispatchFailure(failure)
            if failure.is_io()
                || failure.as_other()
                    == Some(aws_smithy_types::retry::ErrorKind::TransientError) =>
        {
            Some(ErrorKind::ServiceUnavailable)
        }
        SdkError::ResponseError(_) => Some(ErrorKind::ServiceUnavailable),
        _ => None,
    }
}

pub(crate) fn parse_delete_error(err: SdkError<DeleteObjectError>, location: &str) -> IOError {
    let transport_kind = transport_error_kind(&err);
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

    IOError::new(
        transport_kind.unwrap_or(lakekeeper_kind),
        msg,
        location.to_string(),
    )
    .set_source(e)
}

pub(crate) fn parse_batch_delete_error(err: SdkError<DeleteObjectsError>) -> IOError {
    let transport_kind = transport_error_kind(&err);
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

    IOError::new_without_location(transport_kind.unwrap_or(lakekeeper_kind), msg).set_source(e)
}

pub(crate) fn parse_put_object_error(err: SdkError<PutObjectError>, location: &str) -> IOError {
    let transport_kind = transport_error_kind(&err);
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

    IOError::new(
        transport_kind.unwrap_or(lakekeeper_kind),
        msg,
        location.to_string(),
    )
    .set_source(e)
}

pub(crate) fn parse_create_multipart_upload_error(
    err: SdkError<CreateMultipartUploadError>,
    location: &str,
) -> IOError {
    let transport_kind = transport_error_kind(&err);
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

    IOError::new(
        transport_kind.unwrap_or(lakekeeper_kind),
        msg,
        location.to_string(),
    )
    .set_source(e)
}

pub(crate) fn parse_upload_part_error(err: SdkError<UploadPartError>, location: &str) -> IOError {
    let transport_kind = transport_error_kind(&err);
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

    IOError::new(
        transport_kind.unwrap_or(lakekeeper_kind),
        msg,
        location.to_string(),
    )
    .set_source(e)
}

pub(crate) fn parse_complete_multipart_upload_error(
    err: SdkError<CompleteMultipartUploadError>,
    location: &str,
) -> IOError {
    let transport_kind = transport_error_kind(&err);
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

    IOError::new(
        transport_kind.unwrap_or(lakekeeper_kind),
        msg,
        location.to_string(),
    )
    .set_source(e)
}

pub(crate) fn parse_get_object_error(
    err: SdkError<aws_sdk_s3::operation::get_object::GetObjectError>,
    location: &str,
) -> IOError {
    let transport_kind = transport_error_kind(&err);
    let e = err.into_service_error();

    let lakekeeper_kind = match &e {
        aws_sdk_s3::operation::get_object::GetObjectError::NoSuchKey(_) => ErrorKind::NotFound,
        _ => e
            .meta()
            .code()
            .and_then(|s| S3ErrorCode::from_str(s).ok())
            .map_or(ErrorKind::Unexpected, |kind| kind.as_lakekeeper_kind()),
    };

    let msg = e.meta().message().map_or_else(
        || format!("Unknown S3 error during read: {e}"),
        |m| format!("S3 get failed: {m}"),
    );

    IOError::new(
        transport_kind.unwrap_or(lakekeeper_kind),
        msg,
        location.to_string(),
    )
    .set_source(e)
}

/// Parse the error of a `HeadObject` request, which names `version` if set.
pub(crate) fn parse_head_object_error(
    err: SdkError<aws_sdk_s3::operation::head_object::HeadObjectError>,
    location: &str,
    version: Option<&str>,
) -> IOError {
    let transport_kind = transport_error_kind(&err);
    let status = err
        .raw_response()
        .map(|response| response.status().as_u16());
    let e = err.into_service_error();

    // `HeadObjectError::NotFound` is a typed variant. S3 HEAD responses
    // carry no body, so `e.meta().code()` is empty — fall back to the
    // typed variant and then to the HTTP status.
    let lakekeeper_kind = match &e {
        aws_sdk_s3::operation::head_object::HeadObjectError::NotFound(_) => ErrorKind::NotFound,
        _ => e
            .meta()
            .code()
            .and_then(|s| S3ErrorCode::from_str(s).ok())
            .map(|kind| kind.as_lakekeeper_kind())
            .or_else(|| status.and_then(|status| head_status_kind(status, version.is_some())))
            .unwrap_or(ErrorKind::Unexpected),
    };

    let msg = e.meta().message().map_or_else(
        || format!("Unknown S3 error during head operation: {e}"),
        |m| format!("S3 head failed: {m}"),
    );

    IOError::new(
        transport_kind.unwrap_or(lakekeeper_kind),
        msg,
        location.to_string(),
    )
    .set_source(e)
}

/// The kind of a bodiless `HeadObject` error, by HTTP status. S3 answers a version id it
/// cannot parse with 400; without a version id, a 400 can mean anything, such as an
/// expired token.
fn head_status_kind(status: u16, names_version: bool) -> Option<ErrorKind> {
    match status {
        400 if names_version => Some(ErrorKind::ConditionNotMatch),
        412 => Some(ErrorKind::ConditionNotMatch),
        401 | 403 => Some(ErrorKind::PermissionDenied),
        404 => Some(ErrorKind::NotFound),
        405 => Some(ErrorKind::Unsupported),
        429 => Some(ErrorKind::RateLimited),
        503 => Some(ErrorKind::ServiceUnavailable),
        _ => None,
    }
}

pub(crate) fn parse_list_objects_v2_error(
    err: SdkError<ListObjectsV2Error>,
    location: &str,
) -> IOError {
    let transport_kind = transport_error_kind(&err);
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

    IOError::new(
        transport_kind.unwrap_or(lakekeeper_kind),
        msg,
        location.to_string(),
    )
    .set_source(e)
}

/// `Ok(None)` when the bucket has no policy, the error otherwise.
pub(crate) fn parse_get_bucket_policy_error(
    err: SdkError<GetBucketPolicyError>,
    bucket: &str,
) -> Result<Option<String>, IOError> {
    let transport_kind = transport_error_kind(&err);
    let e = err.into_service_error();
    let code = e.meta().code();
    if code == Some("NoSuchBucketPolicy") {
        return Ok(None);
    }

    let lakekeeper_kind = code
        .and_then(|s| S3ErrorCode::from_str(s).ok())
        .map_or(ErrorKind::Unexpected, |kind| kind.as_lakekeeper_kind());

    let msg = e.meta().message().map_or_else(
        || format!("Unknown S3 error while reading the bucket policy: {e}"),
        |m| format!("S3 get bucket policy failed: {m}"),
    );

    Err(IOError::new(
        transport_kind.unwrap_or(lakekeeper_kind),
        msg,
        format!("s3://{bucket}"),
    )
    .set_source(e))
}

/// Parse the error of an operation whose service errors carry no variant of their own
/// worth mapping. `operation` names it in the message, for example "version listing".
pub(crate) fn parse_s3_operation_error<E>(
    err: SdkError<E>,
    operation: &str,
    location: &str,
) -> IOError
where
    E: ProvideErrorMetadata
        + aws_smithy_runtime_api::client::result::CreateUnhandledError
        + std::error::Error
        + Send
        + Sync
        + 'static,
{
    let transport_kind = transport_error_kind(&err);
    let e = err.into_service_error();

    let lakekeeper_kind = e
        .code()
        .and_then(|s| S3ErrorCode::from_str(s).ok())
        .map_or(ErrorKind::Unexpected, |kind| kind.as_lakekeeper_kind());

    let msg = e.message().map_or_else(
        || format!("Unknown S3 error during {operation}: {e}"),
        |m| format!("S3 {operation} failed: {m}"),
    );

    IOError::new(
        transport_kind.unwrap_or(lakekeeper_kind),
        msg,
        location.to_string(),
    )
    .set_source(e)
}

/// Whether S3 refused the request because the version it names is a delete marker: a 405
/// with `x-amz-delete-marker: true`.
pub(crate) fn is_delete_marker<E>(err: &SdkError<E>) -> bool {
    err.raw_response().is_some_and(|response| {
        response.status().as_u16() == 405
            && response.headers().get("x-amz-delete-marker") == Some("true")
    })
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
    BadRequest,
    InvalidAccessKeyId,
    InvalidArgument,
    InvalidBucketName,
    InvalidTag,
    #[strum(serialize = "KMS.DisabledException")]
    KMSDisabledException,
    #[strum(serialize = "KMS.NotFoundException")]
    KMSNotFoundException,
    MalformedXML,
    MethodNotAllowed,
    NoSuchBucket,
    NoSuchKey,
    NoSuchVersion,
    NotImplemented,
    PreconditionFailed,
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
            S3ErrorCode::NoSuchBucket | S3ErrorCode::NoSuchKey | S3ErrorCode::NoSuchVersion => {
                ErrorKind::NotFound
            }
            S3ErrorCode::PreconditionFailed
            | S3ErrorCode::BadRequest
            | S3ErrorCode::InvalidArgument
            | S3ErrorCode::InvalidTag
            | S3ErrorCode::MalformedXML => ErrorKind::ConditionNotMatch,
            S3ErrorCode::NotImplemented | S3ErrorCode::MethodNotAllowed => ErrorKind::Unsupported,
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

    use aws_smithy_runtime_api::client::result::ConnectorError;

    use super::*;

    #[test]
    fn test_known_s3_error_kinds() {
        let error_codes = [
            "SlowDown",
            "AccessDenied",
            "AllAccessDisabled",
            "BadRequest",
            "InvalidAccessKeyId",
            "InvalidArgument",
            "InvalidBucketName",
            "InvalidTag",
            "KMS.DisabledException",
            "KMS.NotFoundException",
            "MalformedXML",
            "MethodNotAllowed",
            "NoSuchBucket",
            "NoSuchKey",
            "NoSuchVersion",
            "NotImplemented",
            "PreconditionFailed",
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

    #[test]
    fn attempt_timeout_is_a_request_timeout() {
        let err = parse_delete_error(SdkError::timeout_error("attempt timed out"), "s3://b/k");
        assert_eq!(err.kind(), ErrorKind::RequestTimeout);
    }

    #[test]
    fn connection_resets_are_service_unavailable() {
        let err = parse_batch_delete_error(SdkError::dispatch_failure(ConnectorError::io(
            "connection reset".into(),
        )));
        assert_eq!(err.kind(), ErrorKind::ServiceUnavailable);
    }

    #[test]
    fn connect_timeouts_are_request_timeouts() {
        let err = parse_list_objects_v2_error(
            SdkError::dispatch_failure(ConnectorError::timeout("connect timed out".into())),
            "s3://b/",
        );
        assert_eq!(err.kind(), ErrorKind::RequestTimeout);
    }

    #[test]
    fn connections_closed_during_a_response_are_service_unavailable() {
        let err = parse_delete_error(
            SdkError::dispatch_failure(ConnectorError::other(
                "connection closed before message completed".into(),
                Some(aws_smithy_types::retry::ErrorKind::TransientError),
            )),
            "s3://b/k",
        );
        assert_eq!(err.kind(), ErrorKind::ServiceUnavailable);
    }

    #[test]
    fn unreadable_responses_are_service_unavailable() {
        let response = aws_smithy_runtime_api::http::Response::new(
            200.try_into().unwrap(),
            aws_smithy_types::body::SdkBody::empty(),
        );
        let err = parse_batch_delete_error(SdkError::response_error("body read failed", response));
        assert_eq!(err.kind(), ErrorKind::ServiceUnavailable);
    }
}
