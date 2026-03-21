use std::sync::atomic::{AtomicU64, Ordering};

use http::StatusCode;

use super::{PostgresBackend, dbutils::DBErrorHandler as _};
use crate::{
    WarehouseId,
    api::{Result, endpoints::EndpointFlat},
    service::idempotency::{IdempotencyCheck, IdempotencyKey},
};

/// Epoch second when the last cleanup started. 0 = idle.
/// If a cleanup is running, stores the start time. If it's been more than
/// the configured `cleanup_timeout`, the previous run is assumed dead
/// (task killed/aborted) and we take over.
static CLEANUP_STARTED_AT: AtomicU64 = AtomicU64::new(0);

fn current_epoch_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Try to claim the cleanup slot. Returns the claimed timestamp if we won.
fn try_claim_cleanup() -> Option<u64> {
    let now = current_epoch_secs();
    let prev = CLEANUP_STARTED_AT.load(Ordering::Relaxed);
    let timeout_secs = crate::CONFIG.idempotency.cleanup_timeout.as_secs();
    // Slot is free (0) or previous run timed out
    if prev == 0 || now.saturating_sub(prev) > timeout_secs {
        CLEANUP_STARTED_AT
            .compare_exchange(prev, now, Ordering::Relaxed, Ordering::Relaxed)
            .ok()
            .map(|_| now)
    } else {
        None
    }
}

/// Release the cleanup slot, but only if we still own it.
/// A newer takeover (after timeout) must not be clobbered.
fn release_cleanup(claimed_at: u64) {
    let _ =
        CLEANUP_STARTED_AT.compare_exchange(claimed_at, 0, Ordering::Relaxed, Ordering::Relaxed);
}

impl PostgresBackend {
    pub(crate) async fn check_idempotency_key_impl(
        warehouse_id: WarehouseId,
        key: &IdempotencyKey,
        state: <Self as crate::service::CatalogStore>::State,
    ) -> Result<IdempotencyCheck> {
        // Uses read_pool for performance. This is a fast-path optimization only —
        // correctness is guaranteed by the INSERT at commit time. A stale replica
        // might miss a recently committed key, causing duplicate work that the
        // INSERT will catch. This is acceptable: the probability of replica lag
        // AND a duplicate request is very low.
        let record = sqlx::query!(
            r#"
            SELECT http_status
            FROM idempotency_record
            WHERE warehouse_id = $1 AND idempotency_key = $2
            "#,
            *warehouse_id,
            key.as_uuid(),
        )
        .fetch_optional(&state.read_pool())
        .await
        .map_err(|e: sqlx::Error| {
            e.into_error_model("Error checking idempotency key".to_string())
        })?;

        // Probabilistic inline cleanup: ~1% of check calls spawn a fire-and-forget
        // cleanup task. A global mutex inside the task ensures only one runs at a
        // time per process — if another is already running, the task exits
        // immediately. Self-recovers on poison.
        if fastrand::f32() < 0.01 {
            let pool = state.write_pool();
            tokio::spawn(async move {
                let Some(claimed_at) = try_claim_cleanup() else {
                    return; // Another cleanup is already running
                };

                let max_age = crate::CONFIG.idempotency.total_retention();
                let cutoff = chrono::Utc::now()
                    - chrono::Duration::from_std(max_age).unwrap_or_else(|_| {
                        tracing::warn!(
                            max_age_secs = max_age.as_secs(),
                            "Failed to convert max_age to chrono::Duration, using 1 hour fallback"
                        );
                        chrono::Duration::hours(1)
                    });
                match sqlx::query!(
                    r#"
                    DELETE FROM ONLY idempotency_record
                    WHERE ctid IN (
                        SELECT ctid FROM idempotency_record
                        WHERE created_at < $1
                        LIMIT 1000
                    )
                    "#,
                    cutoff,
                )
                .execute(&pool)
                .await
                .map(|r| r.rows_affected())
                {
                    Ok(0) => {}
                    Ok(count) => {
                        tracing::debug!(count, "Cleaned up expired idempotency records");
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "Failed to clean up idempotency records");
                    }
                }
                release_cleanup(claimed_at);
            });
        }

        let Some(record) = record else {
            return Ok(IdempotencyCheck::NewRequest);
        };

        match record.http_status {
            204 => Ok(IdempotencyCheck::ReplayNoContent),
            status @ 200..300 => Ok(IdempotencyCheck::ReplaySuccess {
                http_status: u16::try_from(status)
                    .ok()
                    .and_then(|s| StatusCode::from_u16(s).ok())
                    .unwrap_or(StatusCode::OK),
            }),
            status => {
                // Unexpected status in DB — log and treat as new request
                tracing::warn!(
                    warehouse_id = %warehouse_id,
                    idempotency_key = %key.as_uuid(),
                    status,
                    "Unexpected http_status in idempotency record, treating as new request"
                );
                Ok(IdempotencyCheck::NewRequest)
            }
        }
    }

    pub(crate) async fn try_insert_idempotency_key_impl(
        warehouse_id: WarehouseId,
        info: &crate::service::idempotency::IdempotencyInfo,
        transaction: &mut sqlx::Transaction<'static, sqlx::Postgres>,
    ) -> Result<bool> {
        let result = sqlx::query_scalar!(
            r#"
            INSERT INTO idempotency_record (idempotency_key, warehouse_id, operation, http_status)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (warehouse_id, idempotency_key)
            DO NOTHING
            RETURNING TRUE as "inserted!"
            "#,
            info.key.as_uuid(),
            *warehouse_id,
            info.endpoint as EndpointFlat,
            i32::from(info.http_status.as_u16()),
        )
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|e: sqlx::Error| {
            e.into_error_model("Error inserting idempotency key".to_string())
        })?;

        Ok(result.is_some())
    }
}
