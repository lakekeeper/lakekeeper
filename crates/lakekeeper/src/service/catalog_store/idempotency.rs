use crate::{
    WarehouseId,
    api::endpoints::EndpointFlat,
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
    /// Called before authz, outside any transaction, and against the **write
    /// pool**. A replica that misses a just-committed record does not merely
    /// delay the replay, it loses it: most handlers run their mutation before
    /// reaching `try_insert_idempotency_key`, so the retry dies on the
    /// mutation's own conflict — `createTable` returns 409 `TableAlreadyExists`
    /// and the drop paths 404 — rather than replaying. The insert is a backstop
    /// only for the handlers that reach it.
    ///
    /// `endpoint` must be the endpoint currently handling the request — the same
    /// one it will store via [`IdempotencyInfo`]. A key found under a *different*
    /// endpoint is rejected rather than replayed: the spec requires keys to be
    /// globally unique across operations, and replaying a `dropTable` record for
    /// an incoming `createTable` would hand the client a response of the wrong
    /// shape.
    async fn check_idempotency_key(
        warehouse_id: WarehouseId,
        key: &IdempotencyKey,
        endpoint: EndpointFlat,
        state: Self::State,
    ) -> super::Result<IdempotencyCheck> {
        Self::check_idempotency_key_impl(warehouse_id, key, endpoint, state).await
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
