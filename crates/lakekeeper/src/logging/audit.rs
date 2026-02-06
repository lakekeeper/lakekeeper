pub mod events;
#[cfg(feature = "subscriber-filters")]
pub mod filters;

use crate::api::RequestMetadata;

pub const AUDIT_LOG_EVENT_SOURCE: &str = "audit";

pub trait AuditEvent {
    fn action(&self) -> &'static str;
    fn log<D: AuditContextData>(&self, ctx: &D);
    fn log_without_context(&self);
}

pub trait AuditContext {
    fn log_audit<E: AuditEvent>(&self, event: E);
}

impl<T> AuditContext for T
where
    T: AuditContextData,
{
    fn log_audit<E: AuditEvent>(&self, event: E) {
        event.log(self);
    }
}

pub trait AuditContextData {
    fn request_metadata(&self) -> &RequestMetadata;
}

impl AuditContextData for RequestMetadata {
    fn request_metadata(&self) -> &RequestMetadata {
        self
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Result as IOResult, Write},
        sync::{Arc, Mutex},
    };

    use lakekeeper_logging_derive::AuditEvent;

    use super::*;
    use crate::service::UserId;

    #[derive(AuditEvent)]
    struct DummyEvent {}

    #[derive(AuditEvent)]
    struct OptionalFieldsEvent {
        resource_id: String,
        operation_count: i32,
    }

    #[derive(AuditEvent)]
    #[audit("custom_action")]
    struct CustomActionEvent {}

    fn capture_logs<F>(f: F) -> Vec<String>
    where
        F: FnOnce(),
    {
        use std::sync::{Arc, Mutex};

        let logs = Arc::new(Mutex::new(Vec::new()));
        let logs_clone = Arc::clone(&logs);

        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .with_ansi(false)
            .with_writer(move || LogWriter {
                logs: Arc::clone(&logs_clone),
            })
            .finish();

        tracing::subscriber::with_default(subscriber, f);

        let logs = logs.lock().unwrap();
        logs.clone()
    }

    struct LogWriter {
        logs: Arc<Mutex<Vec<String>>>,
    }

    impl Write for LogWriter {
        fn write(&mut self, buf: &[u8]) -> IOResult<usize> {
            let s = String::from_utf8_lossy(buf).to_string();
            self.logs.lock().unwrap().push(s);
            Ok(buf.len())
        }

        fn flush(&mut self) -> IOResult<()> {
            Ok(())
        }
    }

    #[test]
    fn test_basic_audit_logging_works() {
        let request_metadata = RequestMetadata::new_unauthenticated();
        let event = DummyEvent {};

        let logs = capture_logs(|| {
            event.log(&request_metadata);
        });

        let combined = logs.join("");

        assert!(
            combined.contains("event_source=\"audit\""),
            "Missing event_source in: {combined}"
        );
        assert!(
            combined.contains("user=\"anonymous\""),
            "Missing user in: {combined}"
        );

        assert!(
            combined.contains("dummy_event{"),
            "Missing action as span name in: {combined}"
        );
    }

    #[test]
    fn test_audit_logging_with_authenticated_user() {
        let user_id = UserId::new_unchecked("test-idp", "test-user");
        let request_metadata = RequestMetadata::test_user(user_id.clone());
        let event = DummyEvent {};

        let logs = capture_logs(|| {
            event.log(&request_metadata);
        });

        let combined = logs.join("");
        assert!(combined.contains(&format!("user=\"{user_id}\"")));

        assert!(combined.contains("dummy_event{"));
        assert!(!combined.contains("anonymous"));
    }

    #[test]
    fn test_optional_fields_logging() {
        let request_metadata = RequestMetadata::new_unauthenticated();
        let event = OptionalFieldsEvent {
            resource_id: "sales_data".to_string(),
            operation_count: 5432,
        };

        let logs = capture_logs(|| {
            event.log(&request_metadata);
        });

        let combined = logs.join("");
        assert!(combined.contains("resource_id=\"sales_data\""));
        assert!(combined.contains("operation_count=5432"));
    }

    #[test]
    fn test_request_id_included() {
        let request_metadata = RequestMetadata::new_unauthenticated();
        let request_id = request_metadata.request_id();
        let event = DummyEvent {};

        let logs = capture_logs(|| {
            event.log(&request_metadata);
        });

        let combined = logs.join("");
        assert!(combined.contains(&format!("request_id={request_id}")));
    }

    #[test]
    fn test_multiple_events_independent() {
        let request_metadata = RequestMetadata::new_unauthenticated();

        let logs = capture_logs(|| {
            let event1 = OptionalFieldsEvent {
                resource_id: "resource1".to_string(),
                operation_count: 100,
            };
            let event2 = OptionalFieldsEvent {
                resource_id: "resource2".to_string(),
                operation_count: 200,
            };

            event1.log(&request_metadata);
            event2.log(&request_metadata);
        });

        let combined = logs.join("");

        assert!(combined.contains("resource_id=\"resource1\""));
        assert!(combined.contains("resource_id=\"resource2\""));

        let request_id = request_metadata.request_id().to_string();
        assert_eq!(combined.matches(&request_id).count(), 2);
    }

    #[test]
    fn test_action_method() {
        let dummy = DummyEvent {};
        let optional = OptionalFieldsEvent {
            resource_id: "res".to_string(),
            operation_count: 1,
        };

        assert_eq!(dummy.action(), "dummy_event");
        assert_eq!(optional.action(), "optional_fields_event");
    }

    #[test]
    fn test_custom_action_method() {
        let custom = CustomActionEvent {};

        assert_eq!(custom.action(), "custom_action");
    }

    #[test]
    fn test_basic_audit_logging_works_on_audit_context() {
        let user = UserId::new_unchecked("1", "test-user");
        let context = RequestMetadata::test_user(user.clone());

        let logs = capture_logs(|| {
            context.log_audit(OptionalFieldsEvent {
                resource_id: "resource1".to_string(),
                operation_count: 42,
            });
        });

        let combined = logs.join("");

        assert!(
            combined.contains("event_source=\"audit\""),
            "Missing event_source in: {combined}"
        );
        assert!(
            combined.contains(&format!("user=\"{user}\"")),
            "Missing user in: {combined}"
        );

        assert!(
            combined.contains("optional_fields_event{"),
            "Missing action as span name in: {combined}"
        );
        assert!(
            combined.contains("resource_id=\"resource1\""),
            "Missing resource_id in: {combined}"
        );
    }

    #[derive(AuditEvent)]
    #[allow(dead_code)]
    struct SkipFieldEvent {
        visible_field: String,
        #[audit(skip)]
        secret_field: String,
    }

    #[test]
    fn test_skip_field_not_logged() {
        let request_metadata = RequestMetadata::new_unauthenticated();
        let event = SkipFieldEvent {
            visible_field: "visible_value".to_string(),
            secret_field: "secret_value".to_string(),
        };

        let logs = capture_logs(|| {
            event.log(&request_metadata);
        });

        let combined = logs.join("");
        assert!(
            combined.contains("visible_field=\"visible_value\""),
            "Missing visible_field in: {combined}"
        );
        assert!(
            !combined.contains("secret_field"),
            "secret_field should not be logged: {combined}"
        );
        assert!(
            !combined.contains("secret_value"),
            "secret_value should not appear: {combined}"
        );
    }

    #[derive(AuditEvent)]
    struct SkipNoneEvent {
        always_present: String,
        #[audit(skip_none)]
        optional_field: Option<String>,
    }

    #[test]
    fn test_skip_none_some_value_logged() {
        let request_metadata = RequestMetadata::new_unauthenticated();
        let event = SkipNoneEvent {
            always_present: "always".to_string(),
            optional_field: Some("optional_value".to_string()),
        };

        let logs = capture_logs(|| {
            event.log(&request_metadata);
        });

        let combined = logs.join("");
        assert!(
            combined.contains("always_present=\"always\""),
            "Missing always_present in: {combined}"
        );
        assert!(
            combined.contains("optional_field=\"optional_value\""),
            "Missing optional_field in: {combined}"
        );
    }

    #[test]
    fn test_skip_none_none_value_not_logged() {
        let request_metadata = RequestMetadata::new_unauthenticated();
        let event = SkipNoneEvent {
            always_present: "always".to_string(),
            optional_field: None,
        };

        let logs = capture_logs(|| {
            event.log(&request_metadata);
        });

        let combined = logs.join("");
        assert!(
            combined.contains("always_present=\"always\""),
            "Missing always_present in: {combined}"
        );
        // Field should not appear when None
        assert!(
            !combined.contains("optional_field"),
            "optional_field should not be logged when None: {combined}"
        );
    }

    #[derive(AuditEvent)]
    struct SkipNoneDebugEvent {
        #[audit(skip_none, debug)]
        debug_optional: Option<Vec<String>>,
    }

    #[test]
    fn test_skip_none_with_debug_combination() {
        let request_metadata = RequestMetadata::new_unauthenticated();
        let event = SkipNoneDebugEvent {
            debug_optional: Some(vec!["item1".to_string(), "item2".to_string()]),
        };

        let logs = capture_logs(|| {
            event.log(&request_metadata);
        });

        let combined = logs.join("");
        assert!(
            combined.contains("debug_optional="),
            "Missing debug_optional in: {combined}"
        );

        assert!(
            combined.contains('[') && combined.contains(']'),
            "Should use debug format with brackets for Vec: {combined}"
        );
        assert!(
            combined.contains("item1") && combined.contains("item2"),
            "Missing values in debug output: {combined}"
        );
    }

    #[test]
    fn test_skip_none_with_debug_none_skipped() {
        let request_metadata = RequestMetadata::new_unauthenticated();
        let event = SkipNoneDebugEvent {
            debug_optional: None,
        };

        let logs = capture_logs(|| {
            event.log(&request_metadata);
        });

        let combined = logs.join("");
        assert!(
            !combined.contains("debug_optional"),
            "debug_optional should not be logged when None: {combined}"
        );
    }

    #[derive(AuditEvent)]
    struct DebugFieldEvent {
        #[audit(debug)]
        debug_field: Vec<String>,
    }

    #[test]
    fn test_debug_field_format() {
        let request_metadata = RequestMetadata::new_unauthenticated();
        let event = DebugFieldEvent {
            debug_field: vec!["a".to_string(), "b".to_string()],
        };

        let logs = capture_logs(|| {
            event.log(&request_metadata);
        });

        let combined = logs.join("");
        assert!(
            combined.contains("debug_field="),
            "Missing debug_field in: {combined}"
        );

        assert!(
            combined.contains('[') && combined.contains(']'),
            "Should use debug format with brackets: {combined}"
        );
    }
}
