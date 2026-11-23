#[cfg(test)]
mod tests {
    use lakekeeper::{WarehouseId, service::TabularId};
    use serde_json::{Value, json};
    use uuid::Uuid;

    use crate::{
        CatalogState,
        tabular::{
            drop_tabular, mark_tabular_as_deleted,
            table::tests::{InitializedTable, initialize_table},
        },
        warehouse::test::initialize_warehouse,
    };

    struct Row {
        status: &'static str,
        max_severity: i16,
        /// Live data, delete and metadata bytes.
        sizes: Option<(i64, i64, i64)>,
        result: Value,
    }

    fn measured() -> Row {
        Row {
            status: "measured",
            max_severity: 2,
            sizes: Some((100, 0, 10)),
            result: json!({"not-measurable": null, "findings": []}),
        }
    }

    fn not_measurable() -> Row {
        Row {
            status: "not-measurable",
            max_severity: 2,
            sizes: None,
            result: json!({"not-measurable": {"reason": "encrypted"}, "findings": []}),
        }
    }

    async fn insert(
        pool: &sqlx::PgPool,
        warehouse_id: WarehouseId,
        table_id: Uuid,
        row: Row,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO health_iceberg_table (warehouse_id, table_id, measurement_version, \
             status, measured_at, next_not_before, max_severity, finding_count, result, \
             live_data_bytes, live_delete_bytes, live_metadata_bytes) \
             VALUES ($1, $2, 1, $3::health_status, now(), now(), $4, 0, $5, $6, $7, $8)",
        )
        .bind(*warehouse_id)
        .bind(table_id)
        .bind(row.status)
        .bind(row.max_severity)
        .bind(row.result)
        .bind(row.sizes.map(|sizes| sizes.0))
        .bind(row.sizes.map(|sizes| sizes.1))
        .bind(row.sizes.map(|sizes| sizes.2))
        .execute(pool)
        .await
        .map(|_| ())
    }

    /// Name of the constraint that `row` violates.
    async fn violated_constraint(
        pool: &sqlx::PgPool,
        warehouse_id: WarehouseId,
        table_id: Uuid,
        row: Row,
    ) -> String {
        let err = insert(pool, warehouse_id, table_id, row)
            .await
            .expect_err("the row must be rejected");
        err.as_database_error()
            .and_then(|e| e.constraint())
            .expect("a constraint violation")
            .to_string()
    }

    async fn row_count(pool: &sqlx::PgPool, table_id: Uuid) -> i64 {
        sqlx::query_scalar("SELECT count(*) FROM health_iceberg_table WHERE table_id = $1")
            .bind(table_id)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    async fn setup(pool: &sqlx::PgPool) -> (CatalogState, WarehouseId, InitializedTable) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());
        let (_, warehouse_id) = initialize_warehouse(state.clone(), None, None, None, true).await;
        let table = initialize_table(warehouse_id, state.clone(), false, None, None, None).await;
        (state, warehouse_id, table)
    }

    #[lakekeeper_storage_postgres::db_test]
    async fn measured_and_not_measurable_rows_are_stored(pool: sqlx::PgPool) {
        let (state, warehouse_id, table) = setup(&pool).await;
        let other = initialize_table(warehouse_id, state, false, None, None, None).await;

        insert(&pool, warehouse_id, *table.table_id, measured())
            .await
            .unwrap();
        insert(&pool, warehouse_id, *other.table_id, not_measurable())
            .await
            .unwrap();

        assert_eq!(row_count(&pool, *table.table_id).await, 1);
        assert_eq!(row_count(&pool, *other.table_id).await, 1);
    }

    #[lakekeeper_storage_postgres::db_test]
    async fn checks_reject_inconsistent_rows(pool: sqlx::PgPool) {
        let (_, warehouse_id, table) = setup(&pool).await;
        let id = *table.table_id;

        let with_size = |sizes| Row {
            sizes,
            ..measured()
        };
        let unknown_severity = Row {
            max_severity: 4,
            ..measured()
        };
        let oversized_result = Row {
            result: json!({"findings": [], "padding": "x".repeat(1_048_576)}),
            ..measured()
        };

        for (row, constraint) in [
            (with_size(None), "health_iceberg_table_sizes_check"),
            (
                with_size(Some((-1, 0, 10))),
                "health_iceberg_table_sizes_check",
            ),
            (
                Row {
                    sizes: Some((0, 0, 10)),
                    ..not_measurable()
                },
                "health_iceberg_table_sizes_check",
            ),
            (unknown_severity, "health_iceberg_table_max_severity_check"),
            (oversized_result, "health_iceberg_table_result_check"),
        ] {
            assert_eq!(
                violated_constraint(&pool, warehouse_id, id, row).await,
                constraint
            );
        }
        assert_eq!(row_count(&pool, id).await, 0);
    }

    /// Only an Iceberg table can have a row: views and generic tables have no row in `"table"`.
    #[lakekeeper_storage_postgres::db_test]
    async fn rows_need_an_iceberg_table(pool: sqlx::PgPool) {
        let (_, warehouse_id, _) = setup(&pool).await;

        assert_eq!(
            violated_constraint(&pool, warehouse_id, Uuid::now_v7(), measured()).await,
            "health_iceberg_table_table_id_fkey"
        );
    }

    /// A soft-deleted table keeps its row. Dropping the table removes it, and the other table
    /// keeps its row.
    #[lakekeeper_storage_postgres::db_test]
    async fn a_hard_drop_removes_the_row(pool: sqlx::PgPool) {
        let (state, warehouse_id, table) = setup(&pool).await;
        let other = initialize_table(warehouse_id, state, false, None, None, None).await;
        for id in [*table.table_id, *other.table_id] {
            insert(&pool, warehouse_id, id, measured()).await.unwrap();
        }
        let tabular_id = TabularId::Table(table.table_id);

        let mut transaction = pool.begin().await.unwrap();
        mark_tabular_as_deleted(warehouse_id, tabular_id, false, None, &mut transaction)
            .await
            .unwrap();
        transaction.commit().await.unwrap();
        assert_eq!(row_count(&pool, *table.table_id).await, 1);

        let mut transaction = pool.begin().await.unwrap();
        drop_tabular(warehouse_id, tabular_id, false, None, &mut transaction)
            .await
            .unwrap();
        transaction.commit().await.unwrap();
        assert_eq!(row_count(&pool, *table.table_id).await, 0);
        assert_eq!(row_count(&pool, *other.table_id).await, 1);
    }
}
