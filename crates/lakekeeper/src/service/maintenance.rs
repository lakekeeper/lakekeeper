//! Cross-cutting primitives for long-running maintenance flows that must
//! serialize across replicas (catalog reconciliation, schema-migration
//! coordination, etc.).
//!
//! [`MaintenanceLockGuard`] is an unsealed marker trait: storage backend
//! crates (`lakekeeper-storage-postgres`, future SQLite/Foundation
//! adapters, …) implement it on their lock primitives so callers can
//! pass them to maintenance functions without inventing per-backend
//! generic plumbing. The trait carries no methods — its job is to make
//! "I am a lock guard for a maintenance operation" a thing the type
//! system can talk about.

/// RAII guard for a maintenance operation that must not run concurrently
/// across replicas. Implementations are owned values whose `Drop`
/// releases whatever distributed-mutex primitive backs them (Postgres
/// advisory lock, etc.). The maintenance function holds the guard for
/// the operation's lifetime and never inspects it.
pub trait MaintenanceLockGuard: Send + 'static {}

/// Sentinel guard for deployments where concurrency control is not
/// needed (single-replica, single-writer). Passing this is an explicit
/// opt-out — the named token forces the caller to think about whether
/// their deployment actually needs serialization.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoMaintenanceLock;

impl MaintenanceLockGuard for NoMaintenanceLock {}
