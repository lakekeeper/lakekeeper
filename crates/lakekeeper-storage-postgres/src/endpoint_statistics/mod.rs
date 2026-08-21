pub(crate) mod list;
pub(crate) mod sink;

pub use sink::PostgresStatisticsSink;

#[cfg(test)]
mod test {
    use std::{collections::HashMap, sync::Arc};

    use lakekeeper::{
        DEFAULT_PROJECT_ID,
        api::{
            endpoints::{Endpoint, EndpointFlat},
            management::v1::warehouse::TabularDeleteProfile,
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

    #[sqlx::test]
    async fn test_can_select_all_variants(pool: sqlx::PgPool) {
        let mut conn = pool.begin().await.unwrap();
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

        // Query all enum values from the database using enum_range
        let rows = sqlx::query!(
            r#"
            SELECT unnest(enum_range(NULL::api_endpoints)) as "api_endpoints!: EndpointFlat"
            "#
        )
        .fetch_all(&mut *conn)
        .await
        .unwrap();

        // Check that the number of rows returned matches the number of enum variants
        assert_eq!(rows.len(), EndpointFlat::iter().count());
    }

    /// A statistics row whose warehouse id now belongs to another project must not
    /// resolve that project's warehouse name.
    ///
    /// `endpoint_statistics` has no foreign key to `warehouse`, so rows outlive the
    /// warehouse they describe, and `warehouse_name` is read from the join rather
    /// than stored. Joining on the id alone therefore reaches across the project
    /// boundary once an id is in use again elsewhere.
    #[sqlx::test]
    async fn test_stats_do_not_resolve_a_warehouse_name_from_another_project(pool: sqlx::PgPool) {
        use lakekeeper::{
            ProjectId,
            api::management::v1::project::{TimeWindowSelector, WarehouseFilter},
            service::{CatalogStore, Transaction},
        };

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

        // A second project, whose statistics will reference the *first* project's
        // warehouse id — the state that re-using a deleted warehouse's id produces.
        let other_project = Arc::new(ProjectId::from(uuid::Uuid::now_v7()));
        let mut t = <crate::PostgresBackend as CatalogStore>::Transaction::begin_write(
            crate::CatalogState::from_pools(pool.clone(), pool.clone()),
        )
        .await
        .unwrap();
        crate::PostgresBackend::create_project(
            &other_project,
            format!("other-{}", uuid::Uuid::now_v7()),
            t.transaction(),
        )
        .await
        .unwrap();
        t.commit().await.unwrap();

        let sink = PostgresStatisticsSink::new(pool.clone(), pool.clone());
        let mut stats = HashMap::default();
        stats.insert(other_project.clone(), HashMap::default());
        stats.get_mut(&other_project).unwrap().insert(
            lakekeeper::service::endpoint_statistics::EndpointIdentifier {
                uri: Endpoint::iter().next().unwrap(),
                status_code: http::StatusCode::OK,
                warehouse: Some(warehouse.warehouse_id),
                warehouse_name: None,
            },
            1,
        );
        sink.process_stats(Arc::new(stats)).await.unwrap();

        let listed = crate::endpoint_statistics::list::list_statistics(
            other_project.clone(),
            WarehouseFilter::All,
            None,
            TimeWindowSelector::Window {
                end: chrono::Utc::now() + chrono::TimeDelta::days(1),
                interval: chrono::TimeDelta::days(2),
            },
            &pool,
        )
        .await
        .unwrap();

        let recorded: Vec<_> = listed
            .called_endpoints
            .iter()
            .flatten()
            .filter(|e| e.warehouse_id == Some(*warehouse.warehouse_id))
            .collect();
        assert_eq!(recorded.len(), 1, "{listed:?}");
        assert!(
            recorded[0].warehouse_name.is_none(),
            "leaked a warehouse name from another project: {:?}",
            recorded[0]
        );
    }
}
