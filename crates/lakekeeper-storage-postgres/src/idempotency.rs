use std::sync::atomic::{AtomicU64, Ordering};

use lakekeeper::{
    WarehouseId,
    api::{Result, endpoints::EndpointFlat},
    service::idempotency::{IdempotencyCheck, IdempotencyKey},
};

use super::{PostgresBackend, dbutils::DBErrorHandler as _};

/// Epoch second when the last cleanup started. 0 = idle.
/// If a cleanup is running, stores the start time. If it's been more than
/// the configured `cleanup_timeout`, the previous run is assumed dead
/// (task killed/aborted) and we take over.
static CLEANUP_STARTED_AT: AtomicU64 = AtomicU64::new(0);

/// Wall-clock seconds since the epoch, or `None` if the clock reads at or
/// before it.
///
/// Never `0`, because that is [`CLEANUP_STARTED_AT`]'s idle sentinel. Folding a
/// bad clock into it would make the claim below compare-exchange `0` for `0` —
/// which succeeds for every caller at once, against a slot whose value never
/// changes, so the claim would stop excluding anyone.
fn current_epoch_secs() -> Option<u64> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .map(|d| d.as_secs())
        .filter(|&secs| secs != 0)
}

/// Try to claim the cleanup slot. Returns the claimed timestamp if we won.
fn try_claim_cleanup() -> Option<u64> {
    let now = current_epoch_secs()?;
    let prev = CLEANUP_STARTED_AT.load(Ordering::Relaxed);
    let timeout_secs = lakekeeper::CONFIG.idempotency.cleanup_timeout.as_secs();
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
        endpoint: EndpointFlat,
        state: <Self as lakekeeper::service::CatalogStore>::State,
    ) -> Result<IdempotencyCheck> {
        // Deliberately the write pool. The insert at commit time is not a
        // sufficient backstop: handlers run their mutation first and only insert
        // the key just before commit, so a replica that misses a just-committed
        // record loses the replay entirely — `createTable` re-runs and dies on
        // the tabular-name unique violation as a 409, the drop paths 404 on the
        // already-gone entity. Retry-after-timeout is the whole point of the
        // feature and is exactly when lag is likely, so this read has to be
        // authoritative. It is a primary-key lookup and only runs when the
        // client sent the header.
        //
        // `operation` is compared as text: after a rolling downgrade this replica
        // can read a name written by a newer one, and a mismatch is the right
        // answer for any endpoint this binary serves.
        let record = sqlx::query!(
            r#"
            SELECT http_status, operation
            FROM idempotency_record
            WHERE warehouse_id = $1 AND idempotency_key = $2
            "#,
            *warehouse_id,
            key.as_uuid(),
        )
        .fetch_optional(&state.write_pool())
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
                // Unreachable: `lifetime` and `grace_period` parse from ISO-8601
                // with years and months refused, so each is at most `u32::MAX`
                // weeks and their sum stays under chrono's i64-millisecond
                // ceiling. Skipping this run is nonetheless the right failure —
                // retention has no upper bound to violate, whereas a shorter
                // substitute would delete records inside their advertised
                // lifetime, and a retry that finds its record gone re-executes
                // the mutation. Computed before the claim so it cannot leak the
                // slot.
                let max_age = lakekeeper::CONFIG.idempotency.total_retention();
                let Ok(max_age) = chrono::Duration::from_std(max_age) else {
                    tracing::error!(
                        max_age_secs = max_age.as_secs(),
                        "Idempotency retention exceeds the representable range; skipping cleanup"
                    );
                    return;
                };

                let Some(claimed_at) = try_claim_cleanup() else {
                    return; // Another cleanup is already running
                };

                let cutoff = chrono::Utc::now() - max_age;
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

        // The spec makes the key globally unique — never reused across operations.
        // A client that breaks that rule must not be handed a replay of the other
        // operation's response, which would be of the wrong shape entirely.
        if record.operation != endpoint.name() {
            return Err(lakekeeper::api::ErrorModel::bad_request(
                "Idempotency-Key was already used for a different operation. \
                 Keys must be globally unique and must not be reused across operations.",
                "IdempotencyKeyReused",
                None,
            )
            .into());
        }

        // Records are written only for committed successes, so any row means the
        // mutation already happened. A non-2xx status is therefore unreachable
        // today; it is refused rather than swallowed, because both ways of
        // swallowing it are worse than a loud failure — re-running the mutation
        // would break at-most-once, and replaying it as a success would report an
        // outcome that never occurred.
        match record.http_status {
            200..300 => Ok(IdempotencyCheck::Replay),
            status => {
                tracing::error!(
                    warehouse_id = %warehouse_id,
                    idempotency_key = %key.as_uuid(),
                    status,
                    "Idempotency record holds a non-success status, which the write path cannot produce"
                );
                Err(lakekeeper::api::ErrorModel::internal(
                    "Idempotency record holds an unexpected status and cannot be replayed.",
                    "CorruptIdempotencyRecord",
                    None,
                )
                .into())
            }
        }
    }

    pub(crate) async fn try_insert_idempotency_key_impl(
        warehouse_id: WarehouseId,
        info: &lakekeeper::service::idempotency::IdempotencyInfo,
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
            info.endpoint.name(),
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

#[cfg(test)]
mod tests {
    use super::*;

    /// The claim protocol, end to end. Deliberately **one** test: it drives a
    /// process-global static, and `cargo test` shares one process across a
    /// crate's tests, so a second test touching the slot would race this one.
    #[test]
    fn the_cleanup_slot_admits_one_holder_until_released() {
        CLEANUP_STARTED_AT.store(0, Ordering::Relaxed);

        let claimed = try_claim_cleanup().expect("an idle slot is claimable");
        assert_ne!(claimed, 0, "a claim must never store the idle sentinel");
        assert!(
            try_claim_cleanup().is_none(),
            "a held slot must exclude a second claimant"
        );

        // Releasing someone else's claim is a no-op, so a run that overran its
        // timeout cannot clear the claim that took over from it.
        release_cleanup(claimed - 1);
        assert!(
            try_claim_cleanup().is_none(),
            "the slot was released by a stale holder"
        );

        release_cleanup(claimed);
        assert_eq!(CLEANUP_STARTED_AT.load(Ordering::Relaxed), 0);
        assert!(
            try_claim_cleanup().is_some(),
            "a released slot is claimable again"
        );

        CLEANUP_STARTED_AT.store(0, Ordering::Relaxed);
    }

    /// Servers of the previous release cache a query whose result type includes the collation
    /// of `operation`, so it keeps the default collation. `information_schema` reports NULL for
    /// the default collation.
    #[sqlx::test(migrations = false)]
    async fn the_operation_column_keeps_the_default_collation(pool: sqlx::PgPool) {
        crate::migrations::migrate_core_only(&pool).await.unwrap();

        let collations: Vec<(String, String, Option<String>)> = sqlx::query_as(
            "SELECT table_name::text, column_name::text, collation_name::text
             FROM information_schema.columns
             WHERE table_schema = current_schema()
               AND (table_name, column_name) IN
                   (('idempotency_record', 'operation'), ('endpoint_statistics', 'matched_path'))
             ORDER BY table_name",
        )
        .fetch_all(&pool)
        .await
        .unwrap();

        assert_eq!(
            collations,
            vec![
                (
                    "endpoint_statistics".to_string(),
                    "matched_path".to_string(),
                    Some("C".to_string())
                ),
                (
                    "idempotency_record".to_string(),
                    "operation".to_string(),
                    None
                ),
            ]
        );
    }
}
