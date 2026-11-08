//! Where audit records go: the sinks, and the list a record is handed to.

use std::{cell::OnceCell, sync::Arc};

use super::shapes::{AuthorizationRecord, OperationWire, ReplayRecord};

/// One audit record, of any shape.
#[derive(Debug)]
pub(super) enum AuditRecord<'a> {
    Authorization(AuthorizationRecord<'a>),
    Replay(ReplayRecord<'a>),
    Operation(OperationWire),
}

impl AuditRecord<'_> {
    /// This record as one JSON object
    pub(super) fn to_json(&self) -> serde_json::Value {
        match self {
            Self::Authorization(record) => record.to_json(),
            Self::Replay(record) => record.to_json(),
            Self::Operation(record) => record.to_json(),
        }
    }

    /// Write this record as one log line, with `message` as its text.
    pub(super) fn emit(&self, message: &'static str) {
        match self {
            Self::Authorization(record) => record.emit(message),
            Self::Replay(record) => record.emit(message),
            Self::Operation(record) => record.emit(message),
        }
    }
}

/// One record on its way to the sinks.
///
/// A sink outside this crate reads the record only through [`Self::json`], so the audit
/// format is defined in one place.
#[derive(Debug)]
pub struct AuditDelivery<'a> {
    record: AuditRecord<'a>,
    /// The text of the record's log line.
    message: &'static str,
    /// The record as JSON, built when a sink first asks for it.
    json: OnceCell<serde_json::Value>,
}

impl<'a> AuditDelivery<'a> {
    #[must_use]
    pub(super) fn new(record: AuditRecord<'a>, message: &'static str) -> Self {
        Self {
            record,
            message,
            json: OnceCell::new(),
        }
    }

    /// The record as one JSON object. Built on the first call and shared by later callers
    #[must_use]
    pub fn json(&self) -> &serde_json::Value {
        self.json.get_or_init(|| self.record.to_json())
    }

    /// The typed record, for the sink that writes the log line.
    #[must_use]
    pub(super) fn record(&self) -> &AuditRecord<'a> {
        &self.record
    }

    /// The text of the record's log line.
    #[must_use]
    pub(super) fn message(&self) -> &'static str {
        self.message
    }
}

/// A destination for audit records, such as the log or a Kafka topic.
#[async_trait::async_trait]
pub trait AuditSink: Send + Sync + std::fmt::Debug {
    /// A short name for this sink, for startup messages.
    fn name(&self) -> &str;

    /// Hand one record to this sink.
    ///
    /// Called on the request path, so it must not block: copy what you need out of `record`,
    /// then queue it.
    fn deliver(&self, record: &AuditDelivery<'_>);

    /// Send the records that are still queued. Called once, when the server stops.
    async fn shutdown(&self) {}
}

/// Reference to an [`AuditSink`] that can be cheaply cloned and shared.
pub type ArcAuditSink = Arc<dyn AuditSink>;

/// Writes every audit record to the log, as one line.
#[derive(Debug, Default)]
pub(crate) struct LogSink;

#[async_trait::async_trait]
impl AuditSink for LogSink {
    fn name(&self) -> &str {
        "log"
    }

    fn deliver(&self, record: &AuditDelivery<'_>) {
        record.record().emit(record.message());
    }
}

/// The sinks every audit record is handed to, fixed at startup.
#[derive(Debug, Clone, Default)]
pub struct AuditDispatch {
    sinks: Vec<ArcAuditSink>,
}

impl AuditDispatch {
    /// A dispatch to `sinks`, in this order.
    #[must_use]
    pub fn new(sinks: Vec<ArcAuditSink>) -> Self {
        Self { sinks }
    }

    /// Whether no sink is active
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.sinks.is_empty()
    }

    /// Hand `record` to every sink, in order. All sinks share one [`AuditDelivery`]
    pub(super) fn deliver(&self, record: AuditRecord<'_>, message: &'static str) {
        let delivery = AuditDelivery::new(record, message);
        for sink in &self.sinks {
            sink.deliver(&delivery);
        }
    }

    /// Let every sink send what it still has queued, one after the other.
    pub async fn shutdown(&self) {
        for sink in &self.sinks {
            sink.shutdown().await;
        }
    }
}
