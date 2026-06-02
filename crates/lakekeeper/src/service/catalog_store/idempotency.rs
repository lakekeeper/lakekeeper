use crate::{
    WarehouseId,
    service::{
        CatalogStore, Transaction,
        idempotency::{IdempotencyCheck, IdempotencyInfo, IdempotencyKey},
    },
};

/// Idempotency operations on the catalog store.
#[allow(async_fn_in_trait)]
pub trait CatalogIdempotencyOps
where
    Self: CatalogStore,
{
    /// Check if an idempotency key exists and return its status.
    ///
    /// Called before authz, outside any transaction. Uses the write pool
    /// to avoid replica lag.
    async fn check_idempotency_key(
        warehouse_id: WarehouseId,
        key: &IdempotencyKey,
        state: Self::State,
    ) -> super::Result<IdempotencyCheck> {
        Self::check_idempotency_key_impl(warehouse_id, key, state).await
    }

    /// Insert an idempotency key inside the mutation transaction.
    ///
    /// Called right before `commit()`. Uses `INSERT ... ON CONFLICT DO NOTHING`.
    /// Returns `true` if inserted (we won), `false` if conflict (another request
    /// committed the same key concurrently — caller should rollback and replay).
    async fn try_insert_idempotency_key(
        warehouse_id: WarehouseId,
        info: &IdempotencyInfo,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> super::Result<bool> {
        Self::try_insert_idempotency_key_impl(warehouse_id, info, transaction).await
    }
}

impl<T> CatalogIdempotencyOps for T where T: CatalogStore {}
