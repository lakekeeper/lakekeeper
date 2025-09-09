use iceberg_ext::catalog::rest::ErrorModel;

use crate::{
    implementations::postgres::dbutils::DBErrorHandler,
    service::{Result, ServerInfo},
    CONFIG,
};

pub(super) async fn get_validation_data<
    'e,
    'c: 'e,
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
>(
    connection: E,
) -> std::result::Result<ServerInfo, ErrorModel> {
    let server = sqlx::query!(
        r#"
        SELECT
            server_id, open_for_bootstrap, terms_accepted
        FROM server
        LIMIT 2
        "#,
    )
    .fetch_all(connection)
    .await
    .map_err(|e| e.into_error_model("Error fetching bootstrap data".to_string()))?;

    if server.len() > 1 {
        return Err(ErrorModel::internal(
            "Multiple servers found while bootstrapping".to_string(),
            "MultipleServers",
            None,
        ));
    }

    let server = server.into_iter().next();
    if let Some(server) = server {
        Ok(ServerInfo::Bootstrapped {
            server_id: server.server_id,
        })
    } else {
        Ok(ServerInfo::NotBootstrapped)
    }
}

pub(super) async fn bootstrap<'e, 'c: 'e, E: sqlx::Executor<'c, Database = sqlx::Postgres>>(
    connection: E,
) -> Result<bool> {
    let server_id = CONFIG.server_id;

    let result = sqlx::query!(
        r#"
        INSERT INTO server (single_row, server_id, open_for_bootstrap, terms_accepted)
        VALUES (true, $1, false, true)
        ON CONFLICT (single_row)
        DO UPDATE SET open_for_bootstrap = false, terms_accepted = true
        WHERE server.open_for_bootstrap = true
        returning server_id
        "#,
        server_id,
    )
    .fetch_one(connection)
    .await;

    let success = match result {
        Ok(_) => true,
        Err(e) => match &e {
            sqlx::Error::RowNotFound => false,
            _ => {
                return Err(e
                    .into_error_model("Error while bootstrapping Server".to_string())
                    .into())
            }
        },
    };

    Ok(success)
}

#[cfg(test)]
mod test {
    use sqlx::PgPool;

    use super::*;
    use crate::{implementations::postgres::CatalogState, service::ServerInfo};

    #[sqlx::test]
    async fn test_bootstrap(pool: PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let data = get_validation_data(&state.read_pool()).await.unwrap();
        assert_eq!(data, ServerInfo::NotBootstrapped);

        let success = bootstrap(&state.read_write.write_pool).await.unwrap();
        assert!(success);
        let data = get_validation_data(&state.read_pool()).await.unwrap();
        assert_eq!(
            data,
            ServerInfo::Bootstrapped {
                server_id: CONFIG.server_id,
            }
        );

        let success = bootstrap(&state.read_write.write_pool).await.unwrap();
        assert!(!success);
    }
}
