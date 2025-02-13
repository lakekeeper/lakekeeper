use openfga_rs::{
    open_fga_service_client::OpenFgaServiceClient,
    tonic::{
        self,
        codegen::{Body, Bytes, StdError},
    },
    ListStoresRequest, ReadRequestTupleKey, Store, Tuple,
};

use super::{OpenFGAError, OpenFGAResult};

const DEFAULT_MAX_PAGES: u32 = 500;
const PAGE_SIZE: i16 = 100;
/// Maximum number of tuples to write in a single request.
/// Limit set by `OpenFGA`
pub(crate) const MAX_TUPLES_PER_WRITE: i32 = 100;

#[async_trait::async_trait]
pub(crate) trait ClientHelper {
    async fn get_store_by_name(&mut self, store_name: &str) -> OpenFGAResult<Option<Store>>;

    async fn get_or_create_store(&mut self, store_name: &str) -> OpenFGAResult<Store>;

    #[cfg(test)]
    #[allow(dead_code)]
    async fn get_auth_model_id(
        &mut self,
        store_id: String,
        model: &super::models::AuthorizationModel,
    ) -> OpenFGAResult<Option<String>>;

    #[cfg(test)]
    #[allow(dead_code)]
    async fn get_all_auth_models(
        &mut self,
        store_id: String,
    ) -> OpenFGAResult<Vec<super::models::AuthorizationModel>>;

    /// Read all pages of a read request
    async fn read_all_pages(
        &mut self,
        store_id: &str,
        tuple: ReadRequestTupleKey,
    ) -> OpenFGAResult<Vec<Tuple>>;
}

#[async_trait::async_trait]
impl<T> ClientHelper for OpenFgaServiceClient<T>
where
    T: Clone + Sync + Send + 'static,
    T: tonic::client::GrpcService<tonic::body::BoxBody>,
    T::Error: Into<StdError>,
    T::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    <T as tonic::client::GrpcService<
        http_body_util::combinators::UnsyncBoxBody<Bytes, tonic::Status>,
    >>::Future: Send,
{
    async fn get_store_by_name(&mut self, store_name: &str) -> OpenFGAResult<Option<Store>> {
        let mut continuation_token = String::new();
        let count = 0;
        let store = loop {
            let stores = self
                .list_stores(ListStoresRequest {
                    page_size: Some(100),
                    continuation_token: continuation_token.clone(),
                })
                .await
                .map_err(OpenFGAError::list_stores)?
                .into_inner();
            let num_stores = stores.stores.len();
            continuation_token.clone_from(&stores.continuation_token);
            let store = stores.stores.into_iter().find(|s| s.name == store_name);
            if let Some(store) = store {
                break store;
            }

            if continuation_token.is_empty() || num_stores < 100 || count > DEFAULT_MAX_PAGES {
                return Ok(None);
            }
        };
        Ok(Some(store))
    }

    async fn get_or_create_store(&mut self, store_name: &str) -> OpenFGAResult<Store> {
        let store = self.get_store_by_name(store_name).await?;
        match store {
            None => {
                tracing::info!("OpenFGA Store {} not found. Creating it.", store_name);
                let store = self
                    .create_store(openfga_rs::CreateStoreRequest {
                        name: store_name.to_owned(),
                    })
                    .await
                    .map_err(OpenFGAError::store_creation)?
                    .into_inner();
                Ok(Store {
                    id: store.id,
                    name: store.name,
                    created_at: store.created_at,
                    updated_at: store.updated_at,
                    deleted_at: None,
                })
            }
            Some(store) => Ok(store),
        }
    }

    #[cfg(test)]
    async fn get_auth_model_id(
        &mut self,
        store_id: String,
        model: &super::models::AuthorizationModel,
    ) -> OpenFGAResult<Option<String>> {
        let mut continuation_token = String::new();
        let count = 0;
        let model_id = loop {
            let models = self
                .read_authorization_models(openfga_rs::ReadAuthorizationModelsRequest {
                    store_id: store_id.clone(),
                    page_size: Some(i32::from(PAGE_SIZE)),
                    continuation_token: continuation_token.clone(),
                })
                .await
                .map_err(OpenFGAError::list_authentication_models)?
                .into_inner();
            let num_models = models.authorization_models.len();
            continuation_token.clone_from(&models.continuation_token);
            let found_model = models
                .authorization_models
                .into_iter()
                .map(|m| (m.id.clone(), super::models::AuthorizationModel::from(m)))
                .find(|m| &m.1 == model)
                .map(|m| m.0);
            if let Some(found_model) = found_model {
                break Some(found_model);
            }

            if continuation_token.is_empty()
                || num_models < { PAGE_SIZE as usize }
                || count > DEFAULT_MAX_PAGES
            {
                if count > DEFAULT_MAX_PAGES {
                    return Err(OpenFGAError::TooManyAuthorizationModels(DEFAULT_MAX_PAGES));
                }
                break None;
            }
        };

        Ok(model_id)
    }

    #[cfg(test)]
    async fn get_all_auth_models(
        &mut self,
        store_id: String,
    ) -> OpenFGAResult<Vec<super::models::AuthorizationModel>> {
        let mut continuation_token = String::new();
        let mut models = Vec::new();
        let count = 0;
        loop {
            let response = self
                .read_authorization_models(openfga_rs::ReadAuthorizationModelsRequest {
                    store_id: store_id.clone(),
                    page_size: Some(i32::from(PAGE_SIZE)),
                    continuation_token: continuation_token.clone(),
                })
                .await
                .map_err(OpenFGAError::list_authentication_models)?
                .into_inner();
            models.extend(
                response
                    .authorization_models
                    .into_iter()
                    .map(super::models::AuthorizationModel::from),
            );
            continuation_token.clone_from(&response.continuation_token);
            if continuation_token.is_empty() || count > DEFAULT_MAX_PAGES {
                break;
            }
        }
        Ok(models)
    }

    async fn read_all_pages(
        &mut self,
        store_id: &str,
        tuple: ReadRequestTupleKey,
    ) -> OpenFGAResult<Vec<Tuple>> {
        let mut continuation_token = String::new();
        let mut tuples = Vec::new();
        let mut count = 0;

        loop {
            let read_request = openfga_rs::ReadRequest {
                store_id: store_id.to_owned(),
                tuple_key: Some(tuple.clone()),
                page_size: Some(i32::from(PAGE_SIZE)),
                continuation_token: continuation_token.clone(),
                consistency: openfga_rs::ConsistencyPreference::MinimizeLatency.into(),
            };
            let response = self
                .read(read_request.clone())
                .await
                .map_err(|e| OpenFGAError::ReadFailed {
                    read_request: Box::new(read_request.clone()),
                    source: e,
                })?
                .into_inner();
            tuples.extend(response.tuples);
            continuation_token.clone_from(&response.continuation_token);
            if continuation_token.is_empty() || count > DEFAULT_MAX_PAGES {
                if count > DEFAULT_MAX_PAGES {
                    return Err(OpenFGAError::TooManyPages {
                        max_pages: DEFAULT_MAX_PAGES,
                        tuple,
                    });
                }
                break;
            }
            count += 1;
        }

        Ok(tuples)
    }
}

#[cfg(test)]
mod test {
    use needs_env_var::needs_env_var;

    #[needs_env_var(TEST_OPENFGA = 1)]
    mod openfga {
        use openfga_rs::{CreateStoreRequest, TupleKey, WriteAuthorizationModelRequest};

        use super::super::*;
        use crate::service::authz::implementations::openfga::{
            client::ClientConnection, new_client_from_config, ModelVersion,
        };

        async fn new_store(client: &mut OpenFgaServiceClient<ClientConnection>) -> String {
            let store_name = uuid::Uuid::now_v7().to_string();
            client
                .create_store(CreateStoreRequest {
                    name: store_name.clone(),
                })
                .await
                .unwrap();
            store_name
        }

        async fn create_model(
            client: &mut OpenFgaServiceClient<ClientConnection>,
            store_id: &str,
        ) -> String {
            let test_model = ModelVersion::active().get_model();

            client
                .write_authorization_model(WriteAuthorizationModelRequest {
                    store_id: store_id.to_string(),
                    type_definitions: test_model.type_definitions.clone(),
                    schema_version: test_model.schema_version.clone(),
                    conditions: test_model.conditions.clone().unwrap_or_default(),
                })
                .await
                .unwrap()
                .into_inner()
                .authorization_model_id
        }

        #[tokio::test]
        async fn test_get_store_by_name() {
            let mut client = new_client_from_config()
                .await
                .expect("Failed to create OpenFGA client");

            let store_name = new_store(&mut client).await;
            let store = client
                .get_store_by_name(&store_name)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(store.name, store_name);

            let retrieved_store = client
                .get_store_by_name("non-existent-store")
                .await
                .unwrap();
            assert!(retrieved_store.is_none());

            let retrieved_store = client
                .get_store_by_name(&store_name)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(retrieved_store.name, store_name);
            assert_eq!(retrieved_store, store);
        }

        #[tokio::test]
        async fn test_get_store_by_name_pagination() {
            // Create 201 stores
            let mut client = new_client_from_config()
                .await
                .expect("Failed to create OpenFGA client");

            let mut store_names = std::collections::HashSet::new();
            for _ in 0..201 {
                store_names.insert(new_store(&mut client).await);
            }

            assert_eq!(store_names.len(), 201);

            // Get all stores
            for store_name in &store_names {
                let store = client.get_store_by_name(store_name).await.unwrap().unwrap();
                assert_eq!(store.name, *store_name);
            }
        }

        #[tokio::test]
        async fn test_get_auth_model_id() {
            let mut client = new_client_from_config()
                .await
                .expect("Failed to create OpenFGA client");

            let store_name = new_store(&mut client).await;
            let store = client
                .get_store_by_name(&store_name)
                .await
                .unwrap()
                .unwrap();

            let test_model = ModelVersion::active().get_model();

            let authorization_model_id = create_model(&mut client, &store.id).await;

            let model_id = client
                .get_auth_model_id(store.id.clone(), &test_model)
                .await
                .unwrap()
                .unwrap();

            assert_eq!(model_id, authorization_model_id);
        }

        #[tokio::test]
        async fn test_get_auth_model_id_not_found() {
            let mut client = new_client_from_config()
                .await
                .expect("Failed to create OpenFGA client");

            let store_name = new_store(&mut client).await;
            let store = client
                .get_store_by_name(&store_name)
                .await
                .unwrap()
                .unwrap();

            let test_model = ModelVersion::active().get_model();

            let model_id = client
                .get_auth_model_id(store.id.clone(), &test_model)
                .await
                .unwrap();

            assert!(model_id.is_none());
        }

        #[tokio::test]
        async fn test_read_all_pages() {
            let mut client = new_client_from_config()
                .await
                .expect("Failed to create OpenFGA client");

            let store_name = new_store(&mut client).await;
            let store = client
                .get_store_by_name(&store_name)
                .await
                .unwrap()
                .unwrap();

            let read_request = ReadRequestTupleKey {
                user: "auth_model_id:*".to_string(),
                relation: "exists".to_string(),
                object: "model_version:".to_string(),
            };

            let tuples = client
                .read_all_pages(&store.id, read_request.clone())
                .await
                .unwrap();

            assert!(tuples.is_empty());

            let authorization_model_id = create_model(&mut client, &store.id).await;

            for i in 0..501 {
                client
                    .write(openfga_rs::WriteRequest {
                        authorization_model_id: authorization_model_id.clone(),
                        store_id: store.id.clone(),
                        writes: Some(openfga_rs::WriteRequestWrites {
                            tuple_keys: vec![TupleKey {
                                user: "auth_model_id:*".to_string(),
                                relation: "exists".to_string(),
                                object: format!("model_version:{i}").to_string(),
                                condition: None,
                            }],
                        }),
                        deletes: None,
                    })
                    .await
                    .unwrap();
            }

            let tuples = client
                .read_all_pages(&store.id, read_request.clone())
                .await
                .unwrap();

            assert_eq!(tuples.len(), 501);
        }

        #[tokio::test]
        async fn test_max_tuples_per_write() {
            let mut client = new_client_from_config()
                .await
                .expect("Failed to create OpenFGA client");

            let store_name = new_store(&mut client).await;
            let store = client
                .get_store_by_name(&store_name)
                .await
                .unwrap()
                .unwrap();

            let authorization_model_id = create_model(&mut client, &store.id).await;

            let tuples = (0..MAX_TUPLES_PER_WRITE)
                .map(|i| TupleKey {
                    user: "auth_model_id:*".to_string(),
                    relation: "exists".to_string(),
                    object: format!("model_version:{i}").to_string(),
                    condition: None,
                })
                .collect::<Vec<_>>();

            client
                .write(openfga_rs::WriteRequest {
                    authorization_model_id: authorization_model_id.clone(),
                    store_id: store.id.clone(),
                    writes: Some(openfga_rs::WriteRequestWrites { tuple_keys: tuples }),
                    deletes: None,
                })
                .await
                .unwrap();

            let tuples = client
                .read_all_pages(
                    &store.id,
                    ReadRequestTupleKey {
                        user: "auth_model_id:*".to_string(),
                        relation: "exists".to_string(),
                        object: "model_version:".to_string(),
                    },
                )
                .await
                .unwrap();

            assert_eq!(i32::try_from(tuples.len()).unwrap(), MAX_TUPLES_PER_WRITE);
        }
    }
}
