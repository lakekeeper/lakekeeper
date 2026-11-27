use crate::{
    WarehouseId,
    api::{
        iceberg::v1::{PaginatedMapping, PaginationQuery},
        management::v1::health::{GetTableHealthResponse, TableHealthOrderBy, TableHealthSummary},
    },
    service::{
        CatalogStore, TableId, Transaction,
        table_health::{TableHealthWrite, TableHealthWriteOutcome},
    },
};

/// Table health operations on the catalog store.
#[allow(async_fn_in_trait)]
pub trait CatalogTableHealthOps
where
    Self: CatalogStore,
{
    /// Store the result of a health run. Replaces the table's stored result unless that comes
    /// from a newer measurement version; then nothing is written and
    /// [`TableHealthWriteOutcome::NewerExists`] is returned.
    async fn write_table_health(
        write: &TableHealthWrite,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> super::Result<TableHealthWriteOutcome> {
        Self::write_table_health_impl(write, transaction).await
    }

    /// A page of the stored health of the Iceberg tables of a warehouse, in the given order.
    /// Tables without a result are not listed.
    async fn list_table_health(
        warehouse_id: WarehouseId,
        order_by: TableHealthOrderBy,
        include_soft_deleted: bool,
        pagination_query: PaginationQuery,
        transaction: <Self::Transaction as Transaction<Self::State>>::Transaction<'_>,
    ) -> super::Result<PaginatedMapping<TableId, TableHealthSummary>> {
        Self::list_table_health_impl(
            warehouse_id,
            order_by,
            include_soft_deleted,
            pagination_query,
            transaction,
        )
        .await
    }

    /// The stored health of a table. `None` if the table has no result. `task` is not set.
    async fn get_table_health(
        warehouse_id: WarehouseId,
        table_id: TableId,
        state: Self::State,
    ) -> super::Result<Option<GetTableHealthResponse>> {
        Self::get_table_health_impl(warehouse_id, table_id, state).await
    }
}

impl<T> CatalogTableHealthOps for T where T: CatalogStore {}
