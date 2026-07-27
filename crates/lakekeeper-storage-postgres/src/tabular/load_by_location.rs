use sqlx::FromRow;

use lakekeeper::{
    WarehouseId,
    service::{GetTabularInfoByLocationError, GetTabularInfoError, TabularId},
};
use lakekeeper_io::Location;

use crate::{
    CatalogState,
    dbutils::DBErrorHandler,
    tabular::{TabularType, get_partial_fs_locations},
};

#[derive(Debug, FromRow)]
struct TabularLocationRow {
    tabular_id: uuid::Uuid,
    typ: TabularType,
}

#[allow(clippy::too_many_lines)]
pub(crate) async fn get_tabular_infos_by_s3_location(
    warehouse_id: WarehouseId,
    location: &Location,
    list_flags: lakekeeper::service::TabularListFlags,
    catalog_state: CatalogState,
) -> Result<Option<lakekeeper::service::ViewOrTableInfo>, GetTabularInfoByLocationError> {
    let fs_location = location.authority_and_path();
    let partial_locations = get_partial_fs_locations(location)?;

    tracing::trace!(
        "Looking for tabular in warehouse {warehouse_id} at location {location} (partial locations: {partial_locations:?})",
    );

    let row = sqlx::query_as::<_, TabularLocationRow>(
        r#"
        SELECT
            ti.tabular_id,
            ti.typ
        FROM tabular ti
        INNER JOIN warehouse w ON w.warehouse_id = $1
        WHERE ti.warehouse_id = $1
            AND ti.fs_location = ANY($2)
            AND LENGTH(ti.fs_location) <= $3
            AND w.status = 'active'
            AND (ti.deleted_at IS NULL OR $4)
        ORDER BY LENGTH(ti.fs_location) DESC, ti.tabular_id DESC
        LIMIT 1
        "#,
    )
    .bind(*warehouse_id)
    .bind(partial_locations.as_slice())
    .bind(i32::try_from(fs_location.len()).unwrap_or(i32::MAX) + 1)
    .bind(list_flags.include_deleted)
    .fetch_optional(&catalog_state.read_pool())
    .await
    .map_err(DBErrorHandler::into_catalog_backend_error)?;

    let Some(row) = row else {
        tracing::debug!("Tabular at location {} not found", location);
        return Ok(None);
    };

    let tabular_id = match row.typ {
        TabularType::View => TabularId::View(row.tabular_id.into()),
        TabularType::Table => TabularId::Table(row.tabular_id.into()),
        TabularType::GenericTable => TabularId::GenericTable(row.tabular_id.into()),
        TabularType::PaimonTable => TabularId::Table(row.tabular_id.into()),
    };

    let mut tabulars =
        super::get_tabular_infos_by_ids(warehouse_id, &[tabular_id], list_flags, &catalog_state.read_pool())
            .await
            .map_err(|err| match err {
                GetTabularInfoError::CatalogBackendError(e) => {
                    GetTabularInfoByLocationError::from(e)
                }
                GetTabularInfoError::SerializationError(e) => {
                    GetTabularInfoByLocationError::from(e)
                }
                GetTabularInfoError::InvalidNamespaceIdentifier(e) => {
                    GetTabularInfoByLocationError::from(e)
                }
                GetTabularInfoError::UnexpectedTabularInResponse(e) => {
                    GetTabularInfoByLocationError::from(e)
                }
                GetTabularInfoError::InternalParseLocationError(e) => {
                    GetTabularInfoByLocationError::from(e)
                }
            })?;

    Ok(tabulars.pop())
}
