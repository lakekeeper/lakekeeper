use crate::service::{
    CatalogStore, Transaction,
    table_health::{TableHealthWrite, TableHealthWriteOutcome},
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
}

impl<T> CatalogTableHealthOps for T where T: CatalogStore {}
