//! Where audit records go: the sinks, and the list a record is handed to.

use std::sync::{Arc, OnceLock};

use super::{
    AUDIT_FORMAT, AUDIT_TARGET, EVENT_SOURCE, RecordType,
    shapes::{ArcAuditFields, AuditFields, authorization_keys, operation_keys, replay_keys},
};

/// One record on its way to the sinks.
#[derive(Debug)]
pub struct AuditDelivery {
    /// The record's fields, shared by every sink.
    fields: ArcAuditFields,
    /// The text of the record's log line. Only the log sink reads it.
    message: &'static str,
}

impl AuditDelivery {
    /// The record's fields. A sink that queues the record clones this `Arc`.
    #[must_use]
    pub fn fields(&self) -> &ArcAuditFields {
        &self.fields
    }
}

/// A destination for audit records, such as the log or a Kafka topic.
#[async_trait::async_trait]
pub trait AuditSink: Send + Sync + std::fmt::Debug {
    /// A short name for this sink, for startup messages.
    fn name(&self) -> &str;

    /// Hand one record to this sink.
    ///
    /// Called on the request path, so it must not block: take what you need out of
    /// `delivery`, such as a clone of its fields, then queue it.
    fn deliver(&self, delivery: &AuditDelivery);

    /// Send the records that are still queued. The default does nothing.
    async fn shutdown(&self) {}
}

/// Reference to [`AuditSink`] that can be cheaply cloned and shared.
pub type ArcAuditSink = Arc<dyn AuditSink>;

/// Write one audit record as one log line: the stamped keys, then each key of the record as a
/// field, in the order the shape's key-list macro gives them.
macro_rules! write_audit_line {
    ($fields:expr, $message:expr ; $($key:ident),*) => {
        ::tracing::info!(
            target: AUDIT_TARGET,
            event_source = EVENT_SOURCE,
            audit_format = AUDIT_FORMAT,
            record_type = $fields.record_type().as_wire().text(),
            $($key = $fields.get(stringify!($key)).map(::tracing::field::valuable),)*
            "{}",
            $message
        )
    };
}

/// Writes every audit record to the log, as one line.
///
/// It asks no gate: a record reaches it only while it is in the dispatch, and `tracing` drops
/// the line when the log filter does not keep [`AUDIT_TARGET`].
#[derive(Debug, Default)]
pub(crate) struct LogSink;

#[async_trait::async_trait]
impl AuditSink for LogSink {
    fn name(&self) -> &'static str {
        "log"
    }

    fn deliver(&self, delivery: &AuditDelivery) {
        let fields = &delivery.fields;
        let message = delivery.message;
        match fields.record_type() {
            RecordType::Authorization => authorization_keys!(write_audit_line!(fields, message)),
            RecordType::Replay => replay_keys!(write_audit_line!(fields, message)),
            RecordType::Operation => operation_keys!(write_audit_line!(fields, message)),
        }
    }
}

/// What the audit gates need to know about the sinks chosen at startup.
///
/// The log is the one sink with rules of its own: the `tracing` filter can drop its records,
/// and only it lets a plain error line be dropped. Every other sink counts only by being
/// there, so one flag covers them all, whatever crate provides them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ActiveSinks {
    /// The log sink is active.
    pub(super) log: bool,
    /// A sink other than the log is active.
    pub(super) others: bool,
}

/// Set once, by `activate_sinks`.
static ACTIVE: OnceLock<ActiveSinks> = OnceLock::new();

/// The sinks chosen at startup. Until then, as in unit tests: the log sink as
/// `LAKEKEEPER__AUDIT__TRACING__LOG__ENABLED` sets it, and no other sink.
pub(super) fn active() -> ActiveSinks {
    ACTIVE.get().copied().unwrap_or(ActiveSinks {
        log: crate::CONFIG.audit.tracing.log.enabled,
        others: false,
    })
}

/// The dispatch a listener starts with: the log sink while it is active, and no other sink.
pub(super) fn default_dispatch() -> AuditDispatch {
    let sinks: Vec<ArcAuditSink> = if active().log {
        vec![Arc::new(LogSink)]
    } else {
        Vec::new()
    };
    AuditDispatch::new(sinks)
}

/// The dispatch to `others`, after the log sink when `log` is set. Called once, at startup.
#[cfg(feature = "router")]
pub(crate) fn activate_sinks(log: bool, others: Vec<ArcAuditSink>) -> AuditDispatch {
    let chosen = ActiveSinks {
        log,
        others: !others.is_empty(),
    };
    let mut sinks: Vec<ArcAuditSink> = Vec::new();
    if log {
        sinks.push(Arc::new(LogSink));
    }
    sinks.extend(others);

    if let Err(rejected) = ACTIVE.set(chosen)
        && ACTIVE.get() != Some(&rejected)
    {
        tracing::warn!(
            "Audit sinks were chosen twice with different sinks; the audit gates keep the first choice"
        );
    }
    AuditDispatch::new(sinks)
}

/// The sinks every audit record is handed to, fixed at startup.
#[derive(Debug, Clone)]
pub(crate) struct AuditDispatch {
    sinks: Vec<ArcAuditSink>,
}

impl AuditDispatch {
    /// A dispatch to `sinks`, in this order.
    #[must_use]
    pub(crate) fn new(sinks: Vec<ArcAuditSink>) -> Self {
        Self { sinks }
    }

    /// Whether no sink is active.
    #[cfg(feature = "router")]
    #[must_use]
    pub(crate) fn is_empty(&self) -> bool {
        self.sinks.is_empty()
    }

    /// Hand a record's `fields` to every sink, in order. All sinks share one
    /// [`AuditDelivery`].
    pub(super) fn deliver(&self, fields: AuditFields, message: &'static str) {
        let delivery = AuditDelivery {
            fields: Arc::new(fields),
            message,
        };
        for sink in &self.sinks {
            sink.deliver(&delivery);
        }
    }

    /// The names of the sinks, in order.
    #[cfg(feature = "router")]
    pub(crate) fn sink_names(&self) -> Vec<&str> {
        self.sinks.iter().map(|sink| sink.name()).collect()
    }
}
