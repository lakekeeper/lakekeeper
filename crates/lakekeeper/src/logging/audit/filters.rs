use std::fmt::Debug;

use tracing::{Event, Metadata, Subscriber, field::Field, subscriber::Interest};
use tracing_subscriber::layer::{Context, Filter};

use crate::logging::audit::AUDIT_LOG_EVENT_SOURCE;

#[derive(Debug)]
pub struct AuditFilter;

impl<S: Subscriber> Filter<S> for AuditFilter {
    fn enabled(&self, _meta: &Metadata<'_>, _cx: &Context<'_, S>) -> bool {
        true
    }

    fn callsite_enabled(&self, _meta: &'static Metadata<'static>) -> Interest {
        Interest::sometimes()
    }

    fn event_enabled(&self, event: &Event<'_>, _cx: &Context<'_, S>) -> bool {
        let mut has_audit_source = false;
        event.record(&mut |field: &Field, value: &dyn Debug| {
            if field.name() == "event_source" {
                has_audit_source = format!("{:?}", value).contains(AUDIT_LOG_EVENT_SOURCE);
            }
        });
        has_audit_source
    }
}

#[derive(Debug)]
pub struct NotFilter<F> {
    filter: F,
}

impl<F> NotFilter<F> {
    pub fn new(filter: F) -> Self {
        Self { filter }
    }
}

impl<S: Subscriber, F: Filter<S>> Filter<S> for NotFilter<F> {
    fn enabled(&self, meta: &Metadata<'_>, cx: &Context<'_, S>) -> bool {
        self.filter.enabled(meta, cx)
    }

    fn callsite_enabled(&self, meta: &'static Metadata<'static>) -> Interest {
        self.filter.callsite_enabled(meta)
    }

    fn event_enabled(&self, event: &Event<'_>, cx: &Context<'_, S>) -> bool {
        !self.filter.event_enabled(event, cx)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use tracing::{Level, event};
    use tracing_subscriber::{Layer, layer::SubscriberExt, registry, util::SubscriberInitExt};

    use super::*;

    #[derive(Default)]
    struct EventCollector {
        events: Arc<Mutex<Vec<String>>>,
    }

    impl<S: Subscriber> Layer<S> for EventCollector {
        fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
            let mut records = Vec::new();
            event.record(&mut |field: &Field, value: &dyn Debug| {
                records.push(format!("{}={value:?}", field.name()))
            });
            self.events.lock().unwrap().push(records.join(", "))
        }
    }

    #[test]
    fn test_audit_filter_allows_audit_events() {
        let collector = EventCollector::default();
        let events_ref = collector.events.clone();

        registry().with(collector.with_filter(AuditFilter)).init();

        event!(
            Level::INFO,
            event_source = AUDIT_LOG_EVENT_SOURCE,
            action = "user_login",
            user_id = 42,
        );

        let events = events_ref.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert!(events[0].contains("event_source=\"audit\""));
        assert!(events[0].contains("action=\"user_login\""));
        assert!(events[0].contains("user_id=42"));
    }

    #[test]
    fn test_audit_filter_ignores_non_audit_event_sources() {
        let collector = EventCollector::default();
        let events_ref = collector.events.clone();

        registry().with(collector.with_filter(AuditFilter)).init();

        event!(
            Level::INFO,
            event_source = "application",
            action = "user_login",
            user_id = 42,
        );

        let events = events_ref.lock().unwrap();
        assert!(events.is_empty());
    }

    #[test]
    fn test_audit_filter_ignores_events_without_event_source_field() {
        let collector = EventCollector::default();
        let events_ref = collector.events.clone();

        registry().with(collector.with_filter(AuditFilter)).init();

        event!(Level::INFO, action = "user_login", user_id = 42,);

        let events = events_ref.lock().unwrap();
        assert!(events.is_empty());
    }

    #[test]
    fn test_not_filter_to_accept_the_opposite_of_base_filter() {
        let collector = EventCollector::default();
        let events_ref = collector.events.clone();

        registry()
            .with(collector.with_filter(NotFilter::new(AuditFilter)))
            .init();

        event!(Level::INFO, action = "user_login");

        let events = events_ref.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert!(events[0].contains("action=\"user_login\""));
    }

    #[test]
    fn test_not_filter_to_ignore_the_result_of_base_filter() {
        let collector = EventCollector::default();
        let events_ref = collector.events.clone();

        registry()
            .with(collector.with_filter(NotFilter::new(AuditFilter)))
            .init();

        event!(
            Level::INFO,
            event_source = AUDIT_LOG_EVENT_SOURCE,
            action = "user_login"
        );

        let events = events_ref.lock().unwrap();
        assert!(events.is_empty());
    }
}
