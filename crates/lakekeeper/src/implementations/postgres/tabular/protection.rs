use iceberg::TableIdent;

use super::super::namespace::parse_namespace_identifier_from_vec;
use super::TabularType;
use crate::{
    implementations::postgres::dbutils::DBErrorHandler,
    service::{
        ExpirationTaskInfo, SetTabularProtectionError, TabularId, TabularInfo, TabularNotFound,
    },
    WarehouseId,
};

pub(crate) async fn set_tabular_protected(
    warehouse_id: WarehouseId,
    tabular_id: TabularId,
    protected: bool,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<TabularInfo, SetTabularProtectionError> {
    tracing::debug!(
        "Setting tabular protection for {} ({}) to {}",
        tabular_id,
        tabular_id.typ_str(),
        protected
    );
    let tabular_type = TabularType::from(tabular_id);

    let row = sqlx::query!(
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
        ),
        et AS (
            SELECT task_id, scheduled_for
            FROM task
            WHERE warehouse_id = $1 AND entity_id = $2 AND entity_type in ('table', 'view') AND queue_name = 'tabular_expiration'
        )
        UPDATE tabular t
        SET protected = $3
        FROM selected_tabular as st, w, et
        WHERE t.tabular_id = st.tabular_id
        RETURNING 
            t.metadata_location,
            t.protected,
            t.created_at,
            t.updated_at,
            t.deleted_at,
            t.name,
            t.tabular_namespace_name, 
            t.typ as "typ: TabularType",
            et.scheduled_for as "cleanup_at?",
            et.task_id as "cleanup_task_id?"
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
            tracing::warn!("Error setting tabular as protected: {}", e);
            e.into_catalog_backend_error().into()
        }
    })?;

    let namespace_ident =
        parse_namespace_identifier_from_vec(&row.tabular_namespace_name, warehouse_id, None)?;
    let tabular_ident = TableIdent::new(namespace_ident, row.name.clone());

    let expiration_info = match (row.cleanup_task_id, row.cleanup_at) {
        (Some(task_id), Some(scheduled_for)) => Some(ExpirationTaskInfo {
            expiration_task_id: task_id.into(),
            expiration_date: scheduled_for,
        }),
        _ => None,
    };

    Ok(TabularInfo {
        tabular_ident,
        tabular_id,
        metadata_location: row.metadata_location,
        updated_at: row.updated_at,
        created_at: row.created_at,
        deleted_at: row.deleted_at,
        protected: row.protected,
        expiration_task: expiration_info,
    })
}
