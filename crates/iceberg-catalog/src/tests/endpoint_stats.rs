use std::{sync::Arc, time::Duration};

use sqlx::PgPool;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;

use crate::{
    api::{management::v1::warehouse::TabularDeleteProfile, ApiContext},
    implementations::postgres::{
        endpoint_statistics::sink::PostgresStatisticsSink, PostgresCatalog, SecretsState,
    },
    service::{
        authz::AllowAllAuthorizer,
        endpoint_statistics::{EndpointStatisticsTracker, FlushMode},
        task_queue::TaskQueueConfig,
        EndpointStatisticsTrackerTx, State, UserId,
    },
    tests::TestWarehouseResponse,
};

mod test {
    use std::{
        collections::{HashMap, HashSet},
        str::FromStr,
        sync::Arc,
        time::Duration,
    };

    use chrono::{SubsecRound, Utc};
    use http::Method;
    use maplit::hashmap;
    use sqlx::PgPool;
    use strum::IntoEnumIterator;

    use crate::{
        api::{
            endpoints::{CatalogV1Endpoint, Endpoint},
            management::v1::{
                project::{
                    GetEndpointStatisticsRequest, Service as OtherService, TimeWindowSelector,
                    WarehouseFilter,
                },
                warehouse::Service,
                ApiServer, DeleteWarehouseQuery,
            },
        },
        request_metadata::RequestMetadata,
        service::{
            endpoint_statistics::{EndpointStatisticsMessage, FlushMode},
            Actor,
        },
        tests::endpoint_stats::StatsSetup,
        ProjectId, DEFAULT_PROJECT_ID,
    };

    #[sqlx::test]
    async fn test_stats_task_produces_correct_values(pool: PgPool) {
        let setup = super::setup_stats_test(pool, FlushMode::Automatic, 1).await;

        // send each endpoint once
        send_all_endpoints(&setup).await;
        tokio::time::sleep(Duration::from_millis(500)).await;

        let stats = ApiServer::get_endpoint_statistics(
            setup.ctx.clone(),
            GetEndpointStatisticsRequest {
                warehouse: WarehouseFilter::All,
                status_codes: None,
                range_specifier: None,
            },
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();
        assert_eq!(stats.timestamps.len(), 1);
        assert_eq!(stats.called_endpoints.len(), 1);
        assert_eq!(stats.called_endpoints[0].len(), Endpoint::iter().count());

        for s in &stats.called_endpoints[0] {
            assert_eq!(s.count, 1, "{s:?}");
        }

        let all = stats.called_endpoints[0]
            .iter()
            .map(|s| s.http_route.clone())
            .collect::<HashSet<_>>();
        let expected = Endpoint::iter()
            .map(|e| e.as_http_route().to_string())
            .collect::<HashSet<_>>();
        assert_eq!(
            all,
            expected,
            "symmetric diff: {:?}",
            all.symmetric_difference(&expected)
        );
        setup
            .tx
            .send(EndpointStatisticsMessage::Shutdown)
            .await
            .unwrap();
        setup.tracker_handle.await.unwrap();
    }

    #[sqlx::test]
    async fn test_pagination_endpoints_statistics(pool: sqlx::PgPool) {
        let setup = super::setup_stats_test(pool, FlushMode::Automatic, 1).await;
        // sync to the nearest second
        let now = Utc::now();
        let tdiff = now - now.round_subsecs(0);
        let sleep_duration = chrono::Duration::seconds(1) - tdiff;
        tracing::info!(
            "Sleeping for {} {tdiff} {now}",
            sleep_duration.num_milliseconds()
        );
        tokio::time::sleep(Duration::from_millis(
            sleep_duration.num_milliseconds().try_into().unwrap(),
        ))
        .await;

        // Send endpoints data for first timestamp (oldest)
        send_all_endpoints(&setup).await;
        tokio::time::sleep(Duration::from_millis(1025)).await;

        // Send endpoints data for second timestamp
        send_all_endpoints(&setup).await;
        tokio::time::sleep(Duration::from_millis(1025)).await;

        // Send endpoints data for third timestamp (newest)
        send_all_endpoints(&setup).await;
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Get all statistics to use for reference
        let all_stats = ApiServer::get_endpoint_statistics(
            setup.ctx.clone(),
            GetEndpointStatisticsRequest {
                warehouse: WarehouseFilter::All,
                status_codes: None,
                range_specifier: Some(TimeWindowSelector::Window {
                    end: Utc::now(),
                    interval: chrono::Duration::seconds(10),
                }),
            },
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        assert_eq!(
            all_stats.timestamps.len(),
            3,
            "Should have 3 distinct timestamps"
        );

        // Test pagination with range - get first page (most recent item)
        let page_1 = ApiServer::get_endpoint_statistics(
            setup.ctx.clone(),
            GetEndpointStatisticsRequest {
                warehouse: WarehouseFilter::All,
                status_codes: None,
                range_specifier: Some(TimeWindowSelector::Window {
                    end: Utc::now(),
                    interval: chrono::Duration::milliseconds(1000),
                }),
            },
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        assert_eq!(page_1.timestamps.len(), 1, "First page should have 1 item");
        assert_eq!(
            page_1.timestamps[0], all_stats.timestamps[0],
            "First page should have the newest timestamp"
        );

        // Use the previous page token to get the second page
        let page_2 = ApiServer::get_endpoint_statistics(
            setup.ctx.clone(),
            GetEndpointStatisticsRequest {
                warehouse: WarehouseFilter::All,
                status_codes: None,
                range_specifier: Some(TimeWindowSelector::PageToken {
                    token: page_1.previous_page_token.clone(),
                }),
            },
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        assert_eq!(page_2.timestamps.len(), 1, "Second page should have 1 item");
        assert_eq!(
            page_2.timestamps[0], all_stats.timestamps[1],
            "Second page should have the middle timestamp"
        );

        // Use the previous page token to get the third page
        let page_3 = ApiServer::get_endpoint_statistics(
            setup.ctx.clone(),
            GetEndpointStatisticsRequest {
                warehouse: WarehouseFilter::All,
                status_codes: None,
                range_specifier: Some(TimeWindowSelector::PageToken {
                    token: page_2.previous_page_token.clone(),
                }),
            },
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        assert_eq!(page_3.timestamps.len(), 1, "Third page should have 1 item");
        assert_eq!(
            page_3.timestamps[0], all_stats.timestamps[2],
            "Third page should have the oldest timestamp"
        );

        // Test using next_page_token to go back to more recent results
        let page_back_2 = ApiServer::get_endpoint_statistics(
            setup.ctx.clone(),
            GetEndpointStatisticsRequest {
                warehouse: WarehouseFilter::All,
                status_codes: None,
                range_specifier: Some(TimeWindowSelector::PageToken {
                    token: page_3.next_page_token.clone(),
                }),
            },
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        assert_eq!(
            page_back_2.timestamps.len(),
            1,
            "Going back should show 1 item"
        );
        assert_eq!(
            page_back_2.timestamps[0], all_stats.timestamps[1],
            "Should match middle timestamp"
        );

        // Test larger interval that captures multiple timestamps
        let multi_page = ApiServer::get_endpoint_statistics(
            setup.ctx.clone(),
            GetEndpointStatisticsRequest {
                warehouse: WarehouseFilter::All,
                status_codes: None,
                range_specifier: Some(TimeWindowSelector::Window {
                    end: Utc::now(),
                    interval: chrono::Duration::milliseconds(2200), // Should capture 2 timestamps
                }),
            },
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        assert_eq!(
            multi_page.timestamps.len(),
            2,
            "Multi-page should have 2 items"
        );
        assert_eq!(
            multi_page.timestamps[0], all_stats.timestamps[0],
            "Should include newest timestamp"
        );
        assert_eq!(
            multi_page.timestamps[1], all_stats.timestamps[1],
            "Should include middle timestamp"
        );

        // Test empty result for future range
        let future_page = ApiServer::get_endpoint_statistics(
            setup.ctx.clone(),
            GetEndpointStatisticsRequest {
                warehouse: WarehouseFilter::All,
                status_codes: None,
                range_specifier: Some(TimeWindowSelector::Window {
                    end: Utc::now() + chrono::Duration::hours(1),
                    interval: chrono::Duration::milliseconds(500),
                }),
            },
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        assert_eq!(
            future_page.timestamps.len(),
            0,
            "Future page should be empty"
        );

        setup
            .tx
            .send(EndpointStatisticsMessage::Shutdown)
            .await
            .unwrap();
        setup.tracker_handle.await.unwrap();
    }

    #[sqlx::test]
    async fn test_endpoint_statistics_filters(pool: sqlx::PgPool) {
        let setup = super::setup_stats_test(pool, FlushMode::Automatic, 1).await;

        // Send endpoints data with OK status
        for ep in Endpoint::iter() {
            let (method, path) = ep.as_http_route().split_once(' ').unwrap();
            let method = Method::from_str(method).unwrap();
            let request_metadata = RequestMetadata::new_test(
                None,
                None,
                Actor::Anonymous,
                DEFAULT_PROJECT_ID.clone(),
                Some(Arc::from(path)),
                method,
            );

            setup
                .tx
                .send(EndpointStatisticsMessage::EndpointCalled {
                    request_metadata,
                    response_status: http::StatusCode::OK,
                    path_params: hashmap! {
                        "warehouse_id".to_string() => setup.warehouse.warehouse_id.to_string(),
                    },
                    query_params: HashMap::default(),
                })
                .await
                .unwrap();
        }

        // Send endpoints data with 404 status
        for ep in Endpoint::iter().take(3) {
            let (method, path) = ep.as_http_route().split_once(' ').unwrap();
            let method = Method::from_str(method).unwrap();
            let request_metadata = RequestMetadata::new_test(
                None,
                None,
                Actor::Anonymous,
                DEFAULT_PROJECT_ID.clone(),
                Some(Arc::from(path)),
                method,
            );

            setup
                .tx
                .send(EndpointStatisticsMessage::EndpointCalled {
                    request_metadata,
                    response_status: http::StatusCode::NOT_FOUND,
                    path_params: hashmap! {
                        "warehouse_id".to_string() => setup.warehouse.warehouse_id.to_string(),
                    },
                    query_params: HashMap::default(),
                })
                .await
                .unwrap();
        }

        tokio::time::sleep(Duration::from_millis(500)).await;

        // Test filtering by status code
        let status_filtered = ApiServer::get_endpoint_statistics(
            setup.ctx.clone(),
            GetEndpointStatisticsRequest {
                warehouse: WarehouseFilter::All,
                status_codes: Some(vec![404]),
                range_specifier: Some(TimeWindowSelector::Window {
                    end: Utc::now(),
                    interval: chrono::Duration::seconds(10),
                }),
            },
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        // Check that we only have 404 status codes in the results
        for stat_group in &status_filtered.called_endpoints {
            for stat in stat_group {
                assert_eq!(stat.status_code, 404);
            }
        }

        // Test filtering by warehouse
        let warehouse_filtered = ApiServer::get_endpoint_statistics(
            setup.ctx.clone(),
            GetEndpointStatisticsRequest {
                warehouse: WarehouseFilter::WarehouseId {
                    id: *setup.warehouse.warehouse_id,
                },
                status_codes: None,
                range_specifier: Some(TimeWindowSelector::Window {
                    end: Utc::now(),
                    interval: chrono::Duration::seconds(10),
                }),
            },
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        // Check warehouse filtering
        for stat_group in &warehouse_filtered.called_endpoints {
            for stat in stat_group {
                assert_eq!(stat.warehouse_id, Some(*setup.warehouse.warehouse_id));
            }
        }

        setup
            .tx
            .send(EndpointStatisticsMessage::Shutdown)
            .await
            .unwrap();
        setup.tracker_handle.await.unwrap();
    }

    #[sqlx::test]
    async fn test_not_existing_project_is_not_recorded(pg_pool: PgPool) {
        let setup = super::setup_stats_test(pg_pool, FlushMode::Manual, 1).await;
        let ep: Endpoint = CatalogV1Endpoint::DropTable.into();
        let (method, path) = ep.as_http_route().split_once(' ').unwrap();
        let method = Method::from_str(method).unwrap();
        let request_metadata = RequestMetadata::new_test(
            None,
            None,
            Actor::Anonymous,
            Some(ProjectId::default()),
            Some(Arc::from(path)),
            method,
        );

        setup
            .tx
            .send(EndpointStatisticsMessage::EndpointCalled {
                request_metadata,
                response_status: http::StatusCode::OK,
                path_params: hashmap! {
                    "warehouse_id".to_string() => setup.warehouse.warehouse_id.to_string(),
                },
                query_params: HashMap::default(),
            })
            .await
            .unwrap();
        setup
            .tx
            .send(EndpointStatisticsMessage::Flush)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        setup
            .tx
            .send(EndpointStatisticsMessage::Shutdown)
            .await
            .unwrap();
        setup.tracker_handle.await.unwrap();

        // Test filtering by warehouse
        let stats = ApiServer::get_endpoint_statistics(
            setup.ctx.clone(),
            GetEndpointStatisticsRequest {
                warehouse: WarehouseFilter::All,
                status_codes: None,
                range_specifier: None,
            },
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        assert_eq!(stats.called_endpoints.len(), 0);
        assert_eq!(stats.timestamps.len(), 0);
    }

    #[sqlx::test]
    async fn test_with_multiple_warehouses(pg_pool: PgPool) {
        let setup = super::setup_stats_test(pg_pool, FlushMode::Manual, 3).await;
        let ep: Endpoint = CatalogV1Endpoint::DropTable.into();

        let request_metadata = RequestMetadata::new_test(
            None,
            None,
            Actor::Anonymous,
            DEFAULT_PROJECT_ID.clone(),
            Some(Arc::from(ep.path())),
            ep.method(),
        );

        setup
            .tx
            .send(EndpointStatisticsMessage::EndpointCalled {
                request_metadata: request_metadata.clone(),
                response_status: http::StatusCode::OK,
                path_params: hashmap! {
                    "warehouse_id".to_string() => setup.warehouse.warehouse_id.to_string(),
                },
                query_params: HashMap::default(),
            })
            .await
            .unwrap();
        // send message for second warehouse
        setup
            .tx
            .send(EndpointStatisticsMessage::EndpointCalled {
                request_metadata: request_metadata.clone(),
                response_status: http::StatusCode::OK,
                path_params: hashmap! {
                    "warehouse_id".to_string() => setup.warehouse.additional_warehouses.first().unwrap().0.to_string(),
                },
                query_params: HashMap::default(),
            })
            .await
            .unwrap();

        setup
            .tx
            .send(EndpointStatisticsMessage::Flush)
            .await
            .unwrap();

        setup
            .tx
            .send(EndpointStatisticsMessage::Shutdown)
            .await
            .unwrap();
        setup.tracker_handle.await.unwrap();
        tokio::time::sleep(Duration::from_millis(75)).await;
        // Test filtering by warehouse
        let stats = ApiServer::get_endpoint_statistics(
            setup.ctx.clone(),
            GetEndpointStatisticsRequest {
                warehouse: WarehouseFilter::All,
                status_codes: None,
                range_specifier: None,
            },
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();
        assert_eq!(stats.called_endpoints.len(), 1, "{stats:?}");
        assert_eq!(stats.called_endpoints[0].len(), 2);
        assert_eq!(stats.called_endpoints[0][0].http_route, ep.as_http_route());
        assert_eq!(stats.called_endpoints[0][1].http_route, ep.as_http_route());
        assert_eq!(stats.called_endpoints[0][0].status_code, 200);
        assert_eq!(stats.called_endpoints[0][1].status_code, 200);
        assert_eq!(stats.called_endpoints[0][0].count, 1);
        assert_eq!(stats.called_endpoints[0][1].count, 1);
        assert!(stats.called_endpoints[0][0].warehouse_name.is_some());

        ApiServer::delete_warehouse(
            stats.called_endpoints[0][0].warehouse_id.unwrap().into(),
            DeleteWarehouseQuery { force: false },
            setup.ctx.clone(),
            request_metadata.clone(),
        )
        .await
        .unwrap();

        let stats = ApiServer::get_endpoint_statistics(
            setup.ctx.clone(),
            GetEndpointStatisticsRequest {
                warehouse: WarehouseFilter::All,
                status_codes: None,
                range_specifier: None,
            },
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        assert_eq!(stats.called_endpoints.len(), 1);
        assert_eq!(stats.called_endpoints[0].len(), 1);
        assert_eq!(stats.called_endpoints[0][0].http_route, ep.as_http_route());
        assert_eq!(stats.called_endpoints[0][0].status_code, 200);
        assert_eq!(stats.called_endpoints[0][0].count, 1);

        assert!(stats.called_endpoints[0][0].warehouse_name.is_some());
    }

    async fn send_all_endpoints(setup: &StatsSetup) {
        // send each endpoint once
        for ep in Endpoint::iter() {
            let (method, path) = ep.as_http_route().split_once(' ').unwrap();
            let method = Method::from_str(method).unwrap();
            let request_metadata = RequestMetadata::new_test(
                None,
                None,
                Actor::Anonymous,
                DEFAULT_PROJECT_ID.clone(),
                Some(Arc::from(path)),
                method,
            );

            setup
                .tx
                .send(EndpointStatisticsMessage::EndpointCalled {
                    request_metadata,
                    response_status: http::StatusCode::OK,
                    path_params: hashmap! {
                        "warehouse_id".to_string() => setup.warehouse.warehouse_id.to_string(),
                    },
                    query_params: HashMap::default(),
                })
                .await
                .unwrap();
        }
    }
}

// TODO: test with multiple warehouses and projects

struct StatsSetup {
    ctx: ApiContext<State<AllowAllAuthorizer, PostgresCatalog, SecretsState>>,
    tracker_handle: tokio::task::JoinHandle<()>,
    warehouse: TestWarehouseResponse,
    tx: EndpointStatisticsTrackerTx,
}

async fn configure_trigger(pool: &PgPool) {
    sqlx::query!(
        r#"CREATE OR REPLACE FUNCTION get_stats_interval_unit() RETURNS text AS
        $$
        BEGIN
            RETURN 'second';
        END;
        $$ LANGUAGE plpgsql;"#
    )
    .execute(pool)
    .await
    .unwrap();
}

async fn setup_stats_test(
    pool: PgPool,
    flush_mode: FlushMode,
    number_of_warehouses: usize,
) -> StatsSetup {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::DEBUG.into())
                .from_env_lossy(),
        )
        .try_init()
        .ok();
    configure_trigger(&pool).await;

    let prof = crate::tests::test_io_profile();
    let (ctx, warehouse) = crate::tests::setup(
        pool.clone(),
        prof,
        None,
        AllowAllAuthorizer,
        TabularDeleteProfile::Hard {},
        Some(UserId::new_unchecked("oidc", "test-user-id")),
        Some(TaskQueueConfig {
            max_retries: 1,
            max_age: chrono::Duration::seconds(60),
            poll_interval: std::time::Duration::from_secs(10),
            num_workers: 2,
        }),
        number_of_warehouses,
    )
    .await;

    let (tx, rx) = tokio::sync::mpsc::channel(100);
    let tx = EndpointStatisticsTrackerTx::new(tx);
    let tracker = EndpointStatisticsTracker::new(
        rx,
        vec![Arc::new(PostgresStatisticsSink::new(pool.clone()))],
        Duration::from_millis(150),
        flush_mode,
    );
    let tracker_handle = tokio::task::spawn(tracker.run());

    StatsSetup {
        ctx,
        tracker_handle,
        warehouse,
        tx,
    }
}
