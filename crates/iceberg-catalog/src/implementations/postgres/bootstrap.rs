use crate::implementations::postgres::dbutils::DBErrorHandler;
use crate::service::{Result, StartupValidationData};
use crate::CONFIG;
use iceberg_ext::catalog::rest::ErrorModel;

pub(super) async fn get_validation_data<
    'e,
    'c: 'e,
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
>(
    connection: E,
) -> std::result::Result<StartupValidationData, ErrorModel> {
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
        Ok(StartupValidationData::Bootstrapped {
            server_id: server.server_id,
            terms_accepted: server.terms_accepted,
        })
    } else {
        Ok(StartupValidationData::NotBootstrapped)
    }
}

pub(super) async fn bootstrap<'e, 'c: 'e, E: sqlx::Executor<'c, Database = sqlx::Postgres>>(
    terms_accepted: bool,
    connection: E,
) -> Result<bool> {
    let server_id = CONFIG.server_id;

    let result = sqlx::query!(
        r#"
        INSERT INTO server (single_row, server_id, open_for_bootstrap, terms_accepted)
        VALUES (true, $1, false, $2)
        ON CONFLICT (single_row)
        DO UPDATE SET terms_accepted = $2, open_for_bootstrap = false
        WHERE server.open_for_bootstrap = true
        returning server_id
        "#,
        server_id,
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
                    .into_error_model("Error while bootstrapping Server".to_string())
                    .into())
            }
        },
    };

    Ok(success)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::implementations::postgres::CatalogState;
    use crate::service::StartupValidationData;
    use sqlx::PgPool;

    #[sqlx::test]
    async fn test_bootstrap(pool: PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let data = get_validation_data(&state.read_pool()).await.unwrap();
        assert_eq!(data, StartupValidationData::NotBootstrapped);

        let success = bootstrap(true, &state.read_write.write_pool).await.unwrap();
        assert!(success);
        let data = get_validation_data(&state.read_pool()).await.unwrap();
        assert_eq!(
            data,
            StartupValidationData::Bootstrapped {
                server_id: CONFIG.server_id,
                terms_accepted: true,
            }
        );

        let success = bootstrap(true, &state.read_write.write_pool).await.unwrap();
        assert!(!success);
    }
}
