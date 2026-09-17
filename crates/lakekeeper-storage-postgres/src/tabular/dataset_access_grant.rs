//! Access grants: a reader's standing to have one snapshot's files signed.
use lakekeeper::{
    WarehouseId,
    service::{
        DatasetAccessGrant, DatasetAccessGrantCreation, DatasetAccessGrantError,
        DatasetAccessGrantId, DatasetId, DatasetSnapshotNotFound, idempotency::IdempotencyKey,
    },
};
use uuid::Uuid;

use super::{super::dbutils::DBErrorHandler, dataset_version::lock_active_snapshot};

struct GrantRow {
    grant_id: Uuid,
    warehouse_id: Uuid,
    dataset_id: Uuid,
    snapshot_id: Uuid,
    ref_name: String,
    actor: String,
    content_type: Option<String>,
    created_at: chrono::DateTime<chrono::Utc>,
    expires_at: chrono::DateTime<chrono::Utc>,
    revoked_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl From<GrantRow> for DatasetAccessGrant {
    fn from(row: GrantRow) -> Self {
        Self {
            grant_id: row.grant_id.into(),
            warehouse_id: row.warehouse_id.into(),
            dataset_id: row.dataset_id.into(),
            snapshot_id: row.snapshot_id.into(),
            ref_name: row.ref_name,
            actor: row.actor,
            content_type: row.content_type,
            created_at: row.created_at,
            expires_at: row.expires_at,
            revoked_at: row.revoked_at,
        }
    }
}

pub(crate) async fn create_dataset_access_grant(
    grant: DatasetAccessGrantCreation,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<DatasetAccessGrant, DatasetAccessGrantError> {
    // The snapshot the ref pointed at when it was read. Locked so an expiry cannot
    // slip in before the grant holds it; inactive only if the ref moved on and the
    // snapshot expired since. Locked before the sweep below, which a purge's
    // cascade waits on, so the two never wait on each other.
    if !lock_active_snapshot(
        grant.warehouse_id,
        grant.dataset_id,
        grant.snapshot_id,
        transaction,
    )
    .await
    .map_err(DatasetAccessGrantError::from)?
    {
        return Err(DatasetSnapshotNotFound::new().into());
    }

    // Expired grants validate as expired either way; sweeping them here, where
    // grants are made, keeps the table to the ones that can still sign.
    sqlx::query!(
        r#"
        DELETE FROM dataset_access_grant
        WHERE warehouse_id = $1 AND dataset_id = $2 AND expires_at < now()
        "#,
        *grant.warehouse_id,
        *grant.dataset_id,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| DatasetAccessGrantError::from(e.into_catalog_backend_error()))?;

    let row = sqlx::query_as!(
        GrantRow,
        r#"
        INSERT INTO dataset_access_grant
            (warehouse_id, grant_id, dataset_id, snapshot_id, ref_name, actor,
             content_type, expires_at, idempotency_key)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        RETURNING grant_id, warehouse_id, dataset_id, snapshot_id, ref_name, actor,
                  content_type, created_at, expires_at, revoked_at
        "#,
        *grant.warehouse_id,
        *grant.grant_id,
        *grant.dataset_id,
        *grant.snapshot_id,
        grant.ref_name,
        grant.actor,
        grant.content_type,
        grant.expires_at,
        grant.idempotency_key.map(|key| key.as_uuid()),
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|e| DatasetAccessGrantError::from(e.into_catalog_backend_error()))?;
    Ok(row.into())
}

pub(crate) async fn get_dataset_access_grant(
    warehouse_id: WarehouseId,
    grant_id: DatasetAccessGrantId,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<Option<DatasetAccessGrant>, DatasetAccessGrantError> {
    let row = sqlx::query_as!(
        GrantRow,
        r#"
        SELECT grant_id, warehouse_id, dataset_id, snapshot_id, ref_name, actor,
               content_type, created_at, expires_at, revoked_at
        FROM dataset_access_grant
        WHERE warehouse_id = $1 AND grant_id = $2
        "#,
        *warehouse_id,
        *grant_id,
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|e| DatasetAccessGrantError::from(e.into_catalog_backend_error()))?;
    Ok(row.map(Into::into))
}

/// The grant a key issued, whichever dataset and caller it was for.
pub(crate) async fn get_dataset_access_grant_by_idempotency_key(
    warehouse_id: WarehouseId,
    key: IdempotencyKey,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<Option<DatasetAccessGrant>, DatasetAccessGrantError> {
    let row = sqlx::query_as!(
        GrantRow,
        r#"
        SELECT grant_id, warehouse_id, dataset_id, snapshot_id, ref_name, actor,
               content_type, created_at, expires_at, revoked_at
        FROM dataset_access_grant
        WHERE warehouse_id = $1 AND idempotency_key = $2
        "#,
        *warehouse_id,
        key.as_uuid(),
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|e| DatasetAccessGrantError::from(e.into_catalog_backend_error()))?;
    Ok(row.map(Into::into))
}

pub(crate) async fn revoke_dataset_access_grant(
    warehouse_id: WarehouseId,
    dataset_id: DatasetId,
    grant_id: DatasetAccessGrantId,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<Option<DatasetAccessGrant>, DatasetAccessGrantError> {
    let row = sqlx::query_as!(
        GrantRow,
        r#"
        UPDATE dataset_access_grant
        SET revoked_at = coalesce(revoked_at, now())
        WHERE warehouse_id = $1 AND dataset_id = $2 AND grant_id = $3
        RETURNING grant_id, warehouse_id, dataset_id, snapshot_id, ref_name, actor,
                  content_type, created_at, expires_at, revoked_at
        "#,
        *warehouse_id,
        *dataset_id,
        *grant_id,
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|e| DatasetAccessGrantError::from(e.into_catalog_backend_error()))?;
    Ok(row.map(Into::into))
}
