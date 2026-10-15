pub(crate) mod list;
pub(crate) mod sink;

pub use sink::PostgresStatisticsSink;

#[cfg(test)]
mod test {
    use std::{collections::HashMap, sync::Arc};

    use lakekeeper::{
        DEFAULT_PROJECT_ID,
        api::{
            endpoints::{CatalogV1Endpoint, Endpoint},
            management::v1::{
                project::{TimeWindowSelector, WarehouseFilter},
                warehouse::TabularDeleteProfile,
            },
        },
        service::authz::AllowAllAuthorizer,
    };
    use strum::IntoEnumIterator;

    use crate::endpoint_statistics::sink::PostgresStatisticsSink;

    #[sqlx::test]
    async fn test_can_insert_all_variants(pool: sqlx::PgPool) {
        let conn = pool.begin().await.unwrap();
        let (_api, warehouse) = crate::tests::setup(
            pool.clone(),
            crate::tests::memory_io_profile(),
            None,
            AllowAllAuthorizer::default(),
            TabularDeleteProfile::Hard {},
            None,
            1,
            None,
        )
        .await;

        let sink = PostgresStatisticsSink::new(pool.clone(), pool);

        let project = DEFAULT_PROJECT_ID.clone().unwrap();
        let status_code = http::StatusCode::OK;
        let count = 1;
        let ident = None;
        let warehouse_name = Some(warehouse.warehouse_name);
        let mut stats = HashMap::default();
        stats.insert(project.clone(), HashMap::default());
        let s = stats.get_mut(&project).unwrap();
        for uri in Endpoint::iter() {
            s.insert(
                lakekeeper::service::endpoint_statistics::EndpointIdentifier {
                    uri,
                    status_code,
                    warehouse: ident,
                    warehouse_name: warehouse_name.clone(),
                },
                count,
            );
        }
        sink.process_stats(Arc::new(stats)).await.unwrap();
        conn.commit().await.unwrap();
    }

    /// The `api_endpoints` type is frozen and holds every name stored before names were
    /// stored as text. Each of them must still resolve.
    #[sqlx::test]
    async fn test_every_legacy_name_resolves(pool: sqlx::PgPool) {
        let (_api, _warehouse) = crate::tests::setup(
            pool.clone(),
            crate::tests::memory_io_profile(),
            None,
            AllowAllAuthorizer::default(),
            TabularDeleteProfile::Hard {},
            None,
            1,
            None,
        )
        .await;

        let names = sqlx::query_scalar!(
            r#"SELECT unnest(enum_range(NULL::api_endpoints))::text as "name!""#
        )
        .fetch_all(&pool)
        .await
        .unwrap();

        assert_eq!(names.len(), 189);
        for name in names {
            assert!(
                Endpoint::from_name(&name).is_some(),
                "unknown legacy name {name}"
            );
        }
    }

    /// Rows recorded before names were stored as text live in the legacy table. The listing
    /// merges both tables and leaves out names this server does not know.
    #[sqlx::test]
    async fn test_listing_merges_legacy_rows_and_skips_unknown_names(pool: sqlx::PgPool) {
        let (_api, _warehouse) = crate::tests::setup(
            pool.clone(),
            crate::tests::memory_io_profile(),
            None,
            AllowAllAuthorizer::default(),
            TabularDeleteProfile::Hard {},
            None,
            1,
            None,
        )
        .await;
        let project = DEFAULT_PROJECT_ID.clone().unwrap();

        sqlx::query(
            "INSERT INTO endpoint_statistics_legacy (project_id, matched_path, status_code, count)
             VALUES ($1, 'catalog-v1-create-table', 200, 3)",
        )
        .bind(project.as_str())
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO endpoint_statistics (project_id, matched_path, status_code, count)
             VALUES ($1, 'catalog-v1-create-table', 200, 4), ($1, 'ext-removed-endpoint', 200, 1)",
        )
        .bind(project.as_str())
        .execute(&pool)
        .await
        .unwrap();

        let response = crate::endpoint_statistics::list::list_statistics(
            project,
            WarehouseFilter::All,
            None,
            TimeWindowSelector::Window {
                end: chrono::Utc::now(),
                interval: chrono::Duration::days(1),
            },
            &pool,
        )
        .await
        .unwrap();

        let stats = response
            .called_endpoints
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        assert_eq!(stats.len(), 1);
        assert_eq!(
            stats[0].http_route,
            Endpoint::from(CatalogV1Endpoint::CreateTable).as_http_route()
        );
        assert_eq!(stats[0].count, 7);
    }
}
