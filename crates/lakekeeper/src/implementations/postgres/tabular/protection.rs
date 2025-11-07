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
            SELECT tabular_id, namespace_id, typ
            FROM tabular
            WHERE warehouse_id = $1 AND tabular_id = $2 AND typ = $4
            FOR UPDATE
        ),
        selected_views AS (
            SELECT tabular_id FROM selected_tabular WHERE typ = 'view'
        ),
        selected_tables AS (
            SELECT tabular_id FROM selected_tabular WHERE typ = 'table'
        ),
        ns AS (
            SELECT namespace_name, version as namespace_version
            FROM namespace
            WHERE warehouse_id = $1 AND namespace_id = (SELECT namespace_id FROM selected_tabular)
        ),
        w AS (
            SELECT warehouse_id, version as warehouse_version
            FROM warehouse
            WHERE warehouse_id = $1 AND status = 'active'
        ),
        updated_tabular AS (
            UPDATE tabular t
            SET protected = $3
            FROM selected_tabular as st, w, ns
            WHERE t.tabular_id = st.tabular_id AND t.warehouse_id = $1
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
                t.fs_protocol,
                w.warehouse_version,
                ns.namespace_version
        )
        SELECT ut.*,
               vp.view_properties_keys,
               vp.view_properties_values,
               tp.keys as table_properties_keys,
               tp.values as table_properties_values
        FROM updated_tabular ut
        LEFT JOIN (SELECT view_id,
                    ARRAY_AGG(key)   AS view_properties_keys,
                    ARRAY_AGG(value) AS view_properties_values
            FROM view_properties
            WHERE warehouse_id = $1 and view_id in (SELECT tabular_id FROM selected_views)
            GROUP BY view_id) vp ON ut.tabular_id = vp.view_id
        LEFT JOIN (SELECT table_id,
                    ARRAY_AGG(key) as keys,
                    ARRAY_AGG(value) as values
                FROM table_properties
                WHERE warehouse_id = $1 AND table_id in (SELECT tabular_id FROM selected_tables)
                GROUP BY table_id) tp ON ut.tabular_id = tp.table_id
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
