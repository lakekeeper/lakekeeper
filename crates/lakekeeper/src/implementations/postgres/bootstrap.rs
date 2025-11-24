use iceberg_ext::catalog::rest::ErrorModel;

use crate::{
    implementations::postgres::dbutils::DBErrorHandler,
    service::{Result, ServerId, ServerInfo},
};

pub(super) async fn get_or_set_server_id<
    'e,
    'c: 'e,
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
>(
    connection: E,
) -> anyhow::Result<ServerId> {
    let server_id = ServerId::new_random();
    let existing: uuid::Uuid = sqlx::query_scalar!(
        r#"
        WITH inserted AS (
            INSERT INTO server (single_row, server_id, open_for_bootstrap, terms_accepted)
            VALUES (true, $1, true, false)
            ON CONFLICT (single_row) DO NOTHING
            RETURNING server_id
        )
        SELECT server_id as "server_id!" FROM inserted
        UNION ALL
        SELECT server_id as "server_id!" FROM server
        LIMIT 1
        "#,
        *server_id,
    )
    .fetch_one(connection)
    .await
    .map_err(|e| e.into_error_model("Error getting or setting server_id".to_string()))?;

    Ok(ServerId::from(existing))
}

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
        Ok(ServerInfo {
            server_id: server.server_id.into(),
            open_for_bootstrap: server.open_for_bootstrap,
            terms_accepted: server.terms_accepted,
        })
    } else {
        Err(ErrorModel::failed_dependency(
            "No server_id found in database. Please run migration first.",
            "ServerIdMissing",
            None,
        ))
    }
}

pub(super) async fn bootstrap<'e, 'c: 'e, E: sqlx::Executor<'c, Database = sqlx::Postgres>>(
    terms_accepted: bool,
    connection: E,
) -> Result<bool> {
    // The table has a restriction that only one row can exist
    let result = sqlx::query!(
        r#"
        UPDATE server
        SET open_for_bootstrap = false, terms_accepted = $1
        WHERE server.open_for_bootstrap = true
        returning server_id
        "#,
        terms_accepted,
    )
    .fetch_one(connection)
    .await;

    let success = match result {
        Ok(_) => true,
        Err(e) => match &e {
            sqlx::Error::RowNotFound => false,
            _ => {
                return Err(e
                    .into_error_model("Error while bootstrapping Server. No server found. Please run migration first.".to_string())
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
    use crate::implementations::postgres::{CatalogState, migrations::migrate};

    #[sqlx::test]
    async fn test_bootstrap(pool: PgPool) {
        migrate(&pool).await.unwrap();
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let data = get_validation_data(&state.read_pool()).await.unwrap();
        assert!(data.is_open_for_bootstrap());
        assert!(!data.terms_accepted());

        let success = bootstrap(true, &state.read_write.write_pool).await.unwrap();
        assert!(success);
        let data = get_validation_data(&state.read_pool()).await.unwrap();
        assert!(!data.is_open_for_bootstrap());
        assert!(data.terms_accepted());

        let success = bootstrap(true, &state.read_write.write_pool).await.unwrap();
        assert!(!success);
    }

    #[sqlx::test]
    async fn test_get_or_set_server_id(pool: PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        // First call should create a new server with a random ID
        let server_id_1 = get_or_set_server_id(&state.read_write.write_pool)
            .await
            .unwrap();

        // Verify the server was created with correct defaults
        let data = get_validation_data(&state.read_pool()).await.unwrap();
        assert_eq!(data.server_id(), server_id_1);
        assert!(data.is_open_for_bootstrap());
        assert!(!data.terms_accepted());

        // Second call should return the same server ID (no new insert)
        let server_id_2 = get_or_set_server_id(&state.read_write.write_pool)
            .await
            .unwrap();
        assert_eq!(server_id_1, server_id_2);

        // Verify only one server exists in the database
        let server_count = sqlx::query!("SELECT COUNT(*) as count FROM server")
            .fetch_one(&state.read_pool())
            .await
            .unwrap();
        assert_eq!(server_count.count.unwrap(), 1);

        // Verify the server ID is consistent across multiple calls
        let server_id_3 = get_or_set_server_id(&state.read_write.write_pool)
            .await
            .unwrap();
        assert_eq!(server_id_1, server_id_3);
    }
}
