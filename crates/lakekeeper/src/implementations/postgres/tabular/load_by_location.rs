use std::str::FromStr as _;

use iceberg::TableIdent;
use lakekeeper_io::Location;
use sqlx::types::Json;

use crate::{
    implementations::{
        postgres::{
            dbutils::DBErrorHandler,
            namespace::parse_namespace_identifier_from_vec,
            tabular::{get_partial_fs_locations, TabularType},
        },
        CatalogState,
    },
    service::{
        storage::{join_location, StorageProfile},
        GetTabularInfoByLocationError, InternalParseLocationError, TableInfo, ViewInfo,
        ViewOrTableInfo,
    },
    WarehouseId,
};

pub(crate) async fn get_tabular_infos_by_s3_location(
    warehouse_id: WarehouseId,
    location: &Location,
    list_flags: crate::service::TabularListFlags,
    catalog_state: CatalogState,
) -> Result<Option<ViewOrTableInfo>, GetTabularInfoByLocationError> {
    let fs_location = location.authority_and_path();
    let partial_locations = get_partial_fs_locations(location)?;

    tracing::trace!(
        "Looking for tabular in warehouse {warehouse_id} at location {location} (partial locations: {partial_locations:?})",
    );

    // Location might also be a subpath of the table location.
    // We need to make sure that the location starts with the table location.
    let row = sqlx::query!(
        r#"
         SELECT
             ti.tabular_id,
             ti.name as "table_name",
             ti.fs_location,
             ti.fs_protocol,
             ti.typ as "typ: TabularType",
             ti.tabular_namespace_name as namespace_name,
             ti.namespace_id,
             ti.metadata_location,
             ti.protected,
             ti.updated_at,
             w.storage_profile as "storage_profile: Json<StorageProfile>",
             w."storage_secret_id"
         FROM tabular ti
         INNER JOIN warehouse w ON w.warehouse_id = $1
         WHERE ti.warehouse_id = $1
             AND ti.fs_location = ANY($2)
             AND LENGTH(ti.fs_location) <= $3
             AND w.status = 'active'
             AND (ti.deleted_at IS NULL OR $4)
         "#,
        *warehouse_id,
        partial_locations.as_slice(),
        i32::try_from(fs_location.len()).unwrap_or(i32::MAX) + 1, // account for maybe trailing
        list_flags.include_deleted
    )
    .fetch_one(&catalog_state.read_pool())
    .await;

    let row = match row {
        Ok(row) => row,
        Err(sqlx::Error::RowNotFound) => {
            tracing::debug!("Tabular at location {} not found", location);
            return Ok(None);
        }
        Err(e) => {
            return Err(e.into_catalog_backend_error().into());
        }
    };

    if !list_flags.include_staged && row.metadata_location.is_none() {
        return Ok(None);
    }

    let namespace = parse_namespace_identifier_from_vec(
        &row.namespace_name,
        warehouse_id,
        Some(row.namespace_id),
    )?;
    let tabular_ident = TableIdent::new(namespace, row.table_name);
    let location = join_location(&row.fs_protocol, &row.fs_location)
        .map_err(InternalParseLocationError::from)?;
    let metadata_location = row
        .metadata_location
        .map(|s| Location::from_str(&s))
        .transpose()
        .map_err(InternalParseLocationError::from)?;

    let view_or_table_info = match row.typ {
        TabularType::View => ViewInfo {
            namespace_id: row.namespace_id.into(),
            tabular_ident,
            warehouse_id,
            tabular_id: row.tabular_id.into(),
            protected: row.protected,
            metadata_location,
            updated_at: row.updated_at,
            location,
        }
        .into(),
        TabularType::Table => TableInfo {
            namespace_id: row.namespace_id.into(),
            tabular_ident,
            warehouse_id,
            tabular_id: row.tabular_id.into(),
            protected: row.protected,
            metadata_location,
            updated_at: row.updated_at,
            location,
        }
        .into(),
    };

    Ok(Some(view_or_table_info))
}
