pub(crate) mod list;
pub(crate) mod sink;

pub use sink::PostgresStatisticsSink;

#[cfg(test)]
mod test {
    use std::{collections::HashMap, sync::Arc};

    use strum::IntoEnumIterator;

    use crate::{
        api::management::v1::warehouse::TabularDeleteProfile,
        implementations::postgres::endpoint_statistics::sink::PostgresStatisticsSink,
        service::authz::AllowAllAuthorizer, DEFAULT_PROJECT_ID,
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
            None,
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
        for uri in crate::api::endpoints::Endpoints::iter() {
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
}
