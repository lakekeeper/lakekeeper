pub(crate) mod list;
pub(crate) mod sink;

pub use sink::PostgresStatisticsSink;

#[cfg(test)]
mod test {
    use std::{collections::HashMap, sync::Arc};

    use strum::IntoEnumIterator;

    use crate::{
        api::{
            endpoints::{Endpoint, EndpointFlat},
            management::v1::warehouse::TabularDeleteProfile,
        },
        implementations::postgres::endpoint_statistics::sink::PostgresStatisticsSink,
        service::authz::AllowAllAuthorizer,
        DEFAULT_PROJECT_ID,
    };

    #[sqlx::test]
    async fn test_can_insert_all_variants(pool: sqlx::PgPool) {
        let conn = pool.begin().await.unwrap();
        let (_api, warehouse) = crate::tests::setup(
            pool.clone(),
            crate::tests::test_io_profile(),
            None,
            AllowAllAuthorizer,
            TabularDeleteProfile::Hard {},
            None,
            1,
        )
        .await;

        let sink = PostgresStatisticsSink::new(pool);

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
                crate::service::endpoint_statistics::EndpointIdentifier {
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
            crate::tests::test_io_profile(),
            None,
            AllowAllAuthorizer,
            TabularDeleteProfile::Hard {},
            None,
            1,
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
}
