use futures::{future::BoxFuture, FutureExt};
use iceberg_ext::catalog::rest::ErrorModel;
use sqlx::Postgres;

use crate::{
    api,
    api::iceberg::v1::PaginationQuery,
    implementations::postgres::{
        migrations::MigrationHook,
        tabular::{
            list_tabulars, mark_tabular_as_deleted, table, table::create_table, TabularType,
        },
        warehouse::{list_projects, list_warehouses},
    },
    service::{ListFlags, TableCreation, TabularIdentUuid, WarehouseStatus},
};

pub(super) struct SplitTableMetadataHook;

impl MigrationHook for SplitTableMetadataHook {
    fn apply<'c>(
        &self,
        trx: &'c mut sqlx::Transaction<'_, Postgres>,
    ) -> BoxFuture<'c, api::Result<()>> {
        split_table_metadata(trx).boxed()
    }

    fn version() -> i64
    where
        Self: Sized,
    {
        20_241_106_201_139
    }
}

// FIXME: delete after migration period is done
#[allow(clippy::too_many_lines)]
/// Migrate tables from the old table schema to the new one.
///
/// # Errors
/// This function fails if:
///  - the transaction fails
///  - tables that could be listed cannot be loaded
///  - tables that could be loaded cannot be dropped
///  - tables that could be dropped cannot be re-created
///  - deleted tables that could be re-created cannot be marked as deleted
async fn split_table_metadata(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
) -> api::Result<()> {
    let projects = list_projects(None, &mut **transaction).await?;
    for project in projects {
        tracing::info!("Migrating tables for project {}", project.project_id);
        let warehouses = list_warehouses(
            project.project_id,
            Some(vec![WarehouseStatus::Active, WarehouseStatus::Inactive]),
            &mut **transaction,
        )
        .await?;
        for warehouse in warehouses {
            let warehouse_id = warehouse.id;

            let mut token = None;
            loop {
                let tabs = list_tabulars(
                    warehouse_id,
                    None,
                    None,
                    ListFlags::all(),
                    &mut **transaction,
                    Some(TabularType::Table),
                    PaginationQuery::new(token.into(), Some(100)),
                    true,
                )
                .await?;

                token = tabs.next_token().map(ToString::to_string);
                let tabs = tabs.into_hashmap();
                let ids = tabs
                    .keys()
                    .map(|k| (*k).try_into())
                    .collect::<api::Result<Vec<_>>>()?;

                let tables = table::load_tables_old(warehouse_id, ids, true, transaction).await?;
                let n_tables = tables.len();
                for (idx, (table_id, table)) in tables.into_iter().enumerate() {
                    tracing::info!("Migrating table '{table_id}', {idx}/{n_tables}");
                    let tab = *tabs
                        .get(&TabularIdentUuid::Table(*table_id))
                        .as_ref()
                        .ok_or(ErrorModel::internal(
                            "Table not found in tabulars",
                            "InternalTableNotFound",
                            None,
                        ))?;
                    table::drop_table(table_id, transaction).await?;

                    create_table(
                        TableCreation {
                            namespace_id: table.namespace_id,
                            table_ident: tab.0.clone().as_table()?,
                            metadata_location: table.metadata_location.as_ref(),
                            table_metadata: table.table_metadata,
                        },
                        transaction,
                    )
                    .await?;

                    if let Some(del) = tab.1.as_ref() {
                        mark_tabular_as_deleted(
                            TabularIdentUuid::Table(*table_id),
                            Some(del.deleted_at),
                            transaction,
                        )
                        .await?;
                    }
                }
                if token.is_none() {
                    break;
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod test {

    use iceberg::{spec::TableMetadata, NamespaceIdent, TableIdent};
    use sqlx::PgPool;

    use crate::{
        implementations::postgres::{
            migrations::split_table_metadata::split_table_metadata,
            namespace::tests::initialize_namespace,
            tabular::table::{create_table, load_tables, tests::get_namespace_id},
            warehouse::test::initialize_warehouse,
            CatalogState,
        },
        service::TableCreation,
    };

    #[sqlx::test]
    async fn test_load_is_equal_to_deserialized_jsons(pool: PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let warehouse_id = initialize_warehouse(state.clone(), None, None, None, true).await;
        let namespace = NamespaceIdent::from_vec(vec!["my_namespace".to_string()]).unwrap();
        initialize_namespace(state.clone(), warehouse_id, &namespace, None).await;
        let namespace_id = get_namespace_id(state.clone(), warehouse_id, &namespace).await;

        let jsons = include_str!("../../../../tests/table_metadatas.jsonl")
            .lines()
            .map(serde_json::from_str)
            .collect::<std::result::Result<Vec<TableMetadata>, _>>()
            .unwrap();
        let mut trx = pool.begin().await.unwrap();
        for js in jsons.clone() {
            create_table(
                TableCreation {
                    namespace_id,
                    table_ident: &TableIdent {
                        namespace: namespace.clone(),
                        name: js.uuid().to_string(),
                    },
                    table_metadata: js,
                    metadata_location: None,
                },
                &mut trx,
            )
            .await
            .unwrap();
        }

        for js in jsons {
            let tables = load_tables(warehouse_id, vec![js.uuid().into()], false, &mut trx)
                .await
                .unwrap();
            let table = tables.get(&(js.uuid().into())).unwrap();
            pretty_assertions::assert_eq!(table.table_metadata, js);
        }
        trx.commit().await.unwrap();
    }

    #[sqlx::test]
    async fn test_migrate_tables_with_no_old_tables(pool: PgPool) {
        let state = CatalogState::from_pools(pool.clone(), pool.clone());

        let warehouse_id = initialize_warehouse(state.clone(), None, None, None, true).await;
        let namespace = NamespaceIdent::from_vec(vec!["my_namespace".to_string()]).unwrap();
        initialize_namespace(state.clone(), warehouse_id, &namespace, None).await;
        let namespace_id = get_namespace_id(state.clone(), warehouse_id, &namespace).await;

        let jsons = include_str!("../../../../tests/table_metadatas.jsonl")
            .lines()
            .map(serde_json::from_str)
            .collect::<std::result::Result<Vec<TableMetadata>, _>>()
            .unwrap();
        let mut trx = pool.begin().await.unwrap();
        for js in jsons.clone() {
            create_table(
                TableCreation {
                    namespace_id,
                    table_ident: &TableIdent {
                        namespace: namespace.clone(),
                        name: js.uuid().to_string(),
                    },
                    table_metadata: js,
                    metadata_location: None,
                },
                &mut trx,
            )
            .await
            .unwrap();
        }
        trx.commit().await.unwrap();

        let mut trx = pool.begin().await.unwrap();
        split_table_metadata(&mut trx).await.unwrap();

        for js in jsons {
            let tables = load_tables(warehouse_id, vec![js.uuid().into()], false, &mut trx)
                .await
                .unwrap();
            let table = tables.get(&(js.uuid().into())).unwrap();
            pretty_assertions::assert_eq!(table.table_metadata, js);
        }
        trx.commit().await.unwrap();
    }
}
