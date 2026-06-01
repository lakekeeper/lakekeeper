//! Cross-cutting primitives for long-running maintenance flows that must
//! serialize across replicas (catalog reconciliation, schema-migration
//! coordination, etc.).
//!
//! [`MaintenanceLockGuard`] is a sealed marker trait. The defining crate
//! is the only place that can grant the marker to a type, which makes
//! `impl Send + 'static` foot-shots (e.g. passing `()` to silently
//! disable concurrency control) a compile error at the function
//! boundary.

/// Sealing module for [`MaintenanceLockGuard`]. The trait `Sealed` is
/// `pub` so other modules in the lakekeeper crate can implement it for
/// their lock primitives (e.g.
/// [`crate::implementations::postgres::PostgresAdvisoryLock`]), but the
/// `sealed` module is `pub(crate)` — downstream crates cannot reach it
/// and therefore cannot satisfy the supertrait of `MaintenanceLockGuard`.
pub(crate) mod sealed {
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
