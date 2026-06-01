//! Cross-cutting primitives for long-running maintenance flows that must
//! serialize across replicas (catalog reconciliation, schema-migration
//! coordination, etc.).
//!
//! [`MaintenanceLockGuard`] is a sealed marker trait. The defining crate
//! is the only place that can grant the marker to a type, which makes
//! `impl Send + 'static` foot-shots (e.g. passing `()` to silently
//! disable concurrency control) a compile error at the function
//! boundary.

mod sealed {
    pub trait Sealed {}
}

/// RAII guard for a maintenance operation that must not run concurrently
/// across replicas. The trait carries no methods; implementations are
/// owned values whose `Drop` releases whatever distributed-mutex
/// primitive backs them (Postgres advisory lock, etc.). The maintenance
/// function holds the guard for the operation's lifetime and never
/// inspects it.
pub trait MaintenanceLockGuard: sealed::Sealed + Send + 'static {}

/// Sentinel guard for deployments where concurrency control is not
/// needed (single-replica, single-writer). Passing this is an explicit
/// opt-out — the named token forces the caller to think about whether
/// their deployment actually needs serialization.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoMaintenanceLock;

impl sealed::Sealed for NoMaintenanceLock {}
impl MaintenanceLockGuard for NoMaintenanceLock {}

// Crate-internal re-export so other modules in the lakekeeper crate
// (e.g. `implementations::postgres::advisory_lock`) can opt their lock
// types into the sealed marker without needing a public seal API.
pub(crate) use sealed::Sealed as MaintenanceLockGuardSealed;
