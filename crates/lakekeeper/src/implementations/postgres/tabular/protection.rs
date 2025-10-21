use super::TabularType;
use crate::{
    implementations::postgres::{
        dbutils::DBErrorHandler,
        tabular::{FromTabularRowError, TabularRow},
    },
    service::{SetTabularProtectionError, TabularId, TabularNotFound, ViewOrTableInfo},
    WarehouseId,
};

impl From<FromTabularRowError> for SetTabularProtectionError {
    fn from(err: FromTabularRowError) -> Self {
        match err {
            FromTabularRowError::InternalParseLocationError(e) => e.into(),
            FromTabularRowError::InvalidNamespaceIdentifier(e) => e.into(),
        }
    }
}

pub(crate) async fn set_tabular_protected(
    warehouse_id: WarehouseId,
    tabular_id: TabularId,
    protected: bool,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<ViewOrTableInfo, SetTabularProtectionError> {
    tracing::debug!(
        "Setting tabular protection for {} ({}) to {}",
        tabular_id,
        tabular_id.typ_str(),
        protected
    );
    let tabular_type = TabularType::from(tabular_id);

    let row = sqlx::query_as!(
        TabularRow,
        r#"
        WITH selected_tabular AS (
            SELECT tabular_id
            FROM tabular
            WHERE warehouse_id = $1 AND tabular_id = $2 AND typ = $4
            FOR UPDATE
        ),
        w AS (
            SELECT warehouse_id
            FROM warehouse
            WHERE warehouse_id = $1 AND status = 'active'
        )
        UPDATE tabular t
        SET protected = $3
        FROM selected_tabular as st, w
        WHERE t.tabular_id = st.tabular_id
        RETURNING 
            t.tabular_id,
            t.namespace_id,
            t.name as tabular_name,
            t.tabular_namespace_name as namespace_name,
            t.typ as "typ: TabularType",
            t.metadata_location,
            t.updated_at,
            t.protected,
            t.fs_location,
            t.fs_protocol
        "#,
        *warehouse_id,
        *tabular_id,
        protected,
        tabular_type as _
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|e| {
        if let sqlx::Error::RowNotFound = e {
            SetTabularProtectionError::from(TabularNotFound::new(warehouse_id, tabular_id))
        } else {
            e.into_catalog_backend_error().into()
        }
    })?;

    row.try_into_table_or_view(warehouse_id).map_err(Into::into)
}
