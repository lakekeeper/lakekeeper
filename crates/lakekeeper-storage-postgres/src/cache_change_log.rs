//! The cache change log: which cache keys each transaction changed.

use lakekeeper::service::cache_invalidation::{self, ChangeSet};
use sqlx::PgConnection;

/// The most keys one change-log row carries. A larger key set is split into several rows.
const KEYS_PER_ROW: usize = 1000;

/// Record `change_set` in the transaction open on `conn`, in rows of at most 1000 keys. An
/// empty set inserts nothing, and then no xid is assigned for it. Nothing is recorded while
/// [`cache_invalidation::recording_enabled`] is off.
///
/// # Errors
/// Returns an error if the insert fails.
pub async fn record_cache_changes(
    change_set: &ChangeSet,
    conn: &mut PgConnection,
) -> sqlx::Result<()> {
    if !cache_invalidation::recording_enabled() {
        return Ok(());
    }
    let mut domains: Vec<&str> = Vec::new();
    let mut keys: Vec<serde_json::Value> = Vec::new();
    for (domain, domain_keys) in change_set.encoded() {
        let Some(domain_keys) = domain_keys else {
            domains.push(domain.as_str());
            keys.push(serde_json::Value::Array(Vec::new()));
            continue;
        };
        let domain_keys: Vec<&str> = domain_keys.collect();
        for chunk in domain_keys.chunks(KEYS_PER_ROW) {
            domains.push(domain.as_str());
            keys.push(serde_json::Value::from_iter(chunk.iter().copied()));
        }
    }
    if domains.is_empty() {
        return Ok(());
    }
    sqlx::query!(
        r#"
        INSERT INTO cache_change_log (domain, keys)
        SELECT r.domain,
               ARRAY(SELECT e.key
                     FROM jsonb_array_elements_text(r.keys) WITH ORDINALITY AS e(key, n)
                     ORDER BY e.n)
        FROM unnest($1::text[], $2::jsonb[]) WITH ORDINALITY AS r(domain, keys, n)
        ORDER BY r.n
        "#,
        &domains as &[&str],
        &keys,
    )
    .execute(conn)
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use lakekeeper::{
        WarehouseId,
        service::{
            RoleId,
            cache_invalidation::{CacheDomain, CacheKey},
        },
    };
    use sqlx::PgPool;

    use super::*;

    type LogRow = (String, Vec<String>, Option<i64>, String);

    async fn log_rows(pool: &PgPool) -> Vec<LogRow> {
        sqlx::query_as("SELECT domain, keys, version, xid::text FROM cache_change_log ORDER BY id")
            .fetch_all(pool)
            .await
            .unwrap()
    }

    fn warehouse(n: u128) -> CacheKey {
        CacheKey::Warehouse(WarehouseId::from(uuid::Uuid::from_u128(n)))
    }

    #[sqlx::test]
    async fn records_sorted_keys_under_the_current_xid(pool: PgPool) {
        let mut set = ChangeSet::new();
        set.insert(warehouse(2))
            .insert(warehouse(1))
            .insert_whole_domain(CacheDomain::RoleAncestors);

        let mut tx = pool.begin().await.unwrap();
        record_cache_changes(&set, &mut tx).await.unwrap();
        let current: String = sqlx::query_scalar("SELECT pg_current_xact_id()::text")
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();

        assert_eq!(
            log_rows(&pool).await,
            vec![
                (
                    "warehouse".to_string(),
                    vec![warehouse(1).encode(), warehouse(2).encode()],
                    None,
                    current.clone(),
                ),
                ("role_ancestors".to_string(), vec![], None, current),
            ]
        );
    }

    #[sqlx::test]
    async fn many_keys_are_split_into_rows_of_at_most_1000(pool: PgPool) {
        let mut set = ChangeSet::with_fanout_cap(5000);
        for n in 0..2001 {
            set.insert(warehouse(n));
        }

        let mut tx = pool.begin().await.unwrap();
        record_cache_changes(&set, &mut tx).await.unwrap();
        tx.commit().await.unwrap();

        let rows = log_rows(&pool).await;
        let sizes: Vec<_> = rows.iter().map(|row| row.1.len()).collect();
        assert_eq!(sizes, [1000, 1000, 1]);
        let keys: Vec<String> = rows.into_iter().flat_map(|row| row.1).collect();
        let expected: Vec<String> = (0..2001).map(|n| warehouse(n).encode()).collect();
        assert_eq!(keys, expected);
    }

    #[sqlx::test]
    async fn an_empty_change_set_records_nothing_and_assigns_no_xid(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();
        record_cache_changes(&ChangeSet::new(), &mut tx)
            .await
            .unwrap();
        let assigned: Option<String> =
            sqlx::query_scalar("SELECT pg_current_xact_id_if_assigned()::text")
                .fetch_one(&mut *tx)
                .await
                .unwrap();
        tx.commit().await.unwrap();

        assert_eq!(assigned, None);
        assert_eq!(log_rows(&pool).await, vec![]);
    }

    #[sqlx::test]
    async fn a_rolled_back_savepoint_discards_its_rows(pool: PgPool) {
        let mut set = ChangeSet::new();
        set.insert(CacheKey::Role(RoleId::from(uuid::Uuid::from_u128(1))));

        let mut tx = pool.begin().await.unwrap();
        let mut savepoint = sqlx::Connection::begin(&mut *tx).await.unwrap();
        record_cache_changes(&set, &mut savepoint).await.unwrap();
        savepoint.rollback().await.unwrap();
        tx.commit().await.unwrap();

        assert_eq!(log_rows(&pool).await, vec![]);
    }

    #[sqlx::test]
    async fn the_extension_function_records_a_versioned_row(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();
        let inserted: i64 = sqlx::query_scalar(
            "SELECT lakekeeper_record_cache_change('cedar_scope', ARRAY['project:p1'], 7)",
        )
        .fetch_one(&mut *tx)
        .await
        .unwrap();
        let current: String = sqlx::query_scalar("SELECT pg_current_xact_id()::text")
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();

        assert_eq!(inserted, 1);
        assert_eq!(
            log_rows(&pool).await,
            vec![(
                "cedar_scope".to_string(),
                vec!["project:p1".to_string()],
                Some(7),
                current
            )]
        );
    }

    #[sqlx::test]
    async fn the_extension_function_records_a_whole_domain_and_rejects_null_keys(pool: PgPool) {
        sqlx::query("SELECT lakekeeper_record_cache_change('cedar_scope', '{}')")
            .execute(&pool)
            .await
            .unwrap();
        let err = sqlx::query("SELECT lakekeeper_record_cache_change('cedar_scope', NULL)")
            .execute(&pool)
            .await
            .unwrap_err();

        assert_eq!(
            err.as_database_error().and_then(|e| e.code()).as_deref(),
            Some("23502")
        );
        let rows = log_rows(&pool).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(
            (rows[0].0.as_str(), rows[0].1.clone(), rows[0].2),
            ("cedar_scope", vec![], None)
        );
    }
}
