use std::{collections::HashMap, fmt::Debug, sync::Arc};

use iceberg::{
    spec::{TableMetadata, ViewMetadata},
    TableIdent,
};
use iceberg_ext::catalog::rest::{
    CommitTransactionRequest, CommitViewRequest, CreateTableRequest, CreateViewRequest,
    RegisterTableRequest, RenameTableRequest,
};

use crate::{
    api::{
        iceberg::{
            types::DropParams,
            v1::{DataAccess, NamespaceParameters, TableParameters, ViewParameters},
        },
        management::v1::warehouse::UndropTabularsRequest,
        RequestMetadata,
    },
    catalog::tables::CommitContext,
    service::{TableId, UndropTabularResponse, ViewId},
    WarehouseId,
};

#[derive(Clone)]
pub struct EndpointHookCollection(Vec<Arc<dyn EndpointHooks>>);

impl core::fmt::Debug for EndpointHookCollection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Hooks").field(&self.0.len()).finish()
    }
}

impl EndpointHookCollection {
    #[must_use]
    pub fn new(hooks: Vec<Arc<dyn EndpointHooks>>) -> Self {
        Self(hooks)
    }
}

#[async_trait::async_trait]
impl EndpointHooks for EndpointHookCollection {
    async fn commit_table(
        &self,
        warehouse_id: WarehouseId,
        request: Arc<CommitTransactionRequest>,
        responses: Arc<Vec<CommitContext>>,
        table_ident_map: Arc<HashMap<TableIdent, TableId>>,
        request_metadata: Arc<RequestMetadata>,
    ) {
        futures::future::join_all(self.0.iter().map(|hook| {
            hook.commit_table(
                warehouse_id,
                request.clone(),
                responses.clone(),
                table_ident_map.clone(),
                request_metadata.clone(),
            )
        }))
        .await;
    }

    async fn drop_table(
        &self,
        warehouse_id: WarehouseId,
        parameters: TableParameters,
        drop_params: DropParams,
        table_id: TableId,
        request_metadata: Arc<RequestMetadata>,
    ) {
        futures::future::join_all(self.0.iter().map(|h| {
            h.drop_table(
                warehouse_id,
                parameters.clone(),
                drop_params.clone(),
                table_id,
                request_metadata.clone(),
            )
        }))
        .await;
    }

    async fn register_table(
        &self,
        warehouse_id: WarehouseId,
        parameters: NamespaceParameters,
        request: Arc<RegisterTableRequest>,
        metadata: Arc<TableMetadata>,
        request_metadata: Arc<RequestMetadata>,
    ) {
        futures::future::join_all(self.0.iter().map(|hook| {
            hook.register_table(
                warehouse_id,
                parameters.clone(),
                request.clone(),
                metadata.clone(),
                request_metadata.clone(),
            )
        }))
        .await;
    }

    async fn create_view(
        &self,
        warehouse_id: WarehouseId,
        parameters: NamespaceParameters,
        request: Arc<CreateViewRequest>,
        metadata: Arc<ViewMetadata>,
        data_access: DataAccess,
        request_metadata: Arc<RequestMetadata>,
    ) {
        futures::future::join_all(self.0.iter().map(|hook| {
            hook.create_view(
                warehouse_id,
                parameters.clone(),
                request.clone(),
                metadata.clone(),
                data_access,
                request_metadata.clone(),
            )
        }))
        .await;
    }

    async fn commit_view(
        &self,
        warehouse_id: WarehouseId,
        parameters: ViewParameters,
        request: Arc<CommitViewRequest>,
        metadata: Arc<ViewMetadata>,
        data_access: DataAccess,
        request_metadata: Arc<RequestMetadata>,
    ) {
        futures::future::join_all(self.0.iter().map(|hook| {
            hook.commit_view(
                warehouse_id,
                parameters.clone(),
                request.clone(),
                metadata.clone(),
                data_access,
                request_metadata.clone(),
            )
        }))
        .await;
    }

    async fn drop_view(
        &self,
        warehouse_id: WarehouseId,
        parameters: ViewParameters,
        drop_params: DropParams,
        view_id: ViewId,
        request_metadata: Arc<RequestMetadata>,
    ) {
        futures::future::join_all(self.0.iter().map(|hook| {
            hook.drop_view(
                warehouse_id,
                parameters.clone(),
                drop_params.clone(),
                view_id,
                request_metadata.clone(),
            )
        }))
        .await;
    }

    async fn rename_view(
        &self,
        warehouse_id: WarehouseId,
        view_id: ViewId,
        request: Arc<RenameTableRequest>,
        request_metadata: Arc<RequestMetadata>,
    ) {
        futures::future::join_all(self.0.iter().map(|hook| {
            hook.rename_view(
                warehouse_id,
                view_id,
                request.clone(),
                request_metadata.clone(),
            )
        }))
        .await;
    }

    async fn rename_table(
        &self,
        warehouse_id: WarehouseId,
        table_id: TableId,
        request: Arc<RenameTableRequest>,
        request_metadata: Arc<RequestMetadata>,
    ) {
        futures::future::join_all(self.0.iter().map(|hook| {
            hook.rename_table(
                warehouse_id,
                table_id,
                request.clone(),
                request_metadata.clone(),
            )
        }))
        .await;
    }

    async fn undrop_tabular(
        &self,
        warehouse_id: WarehouseId,
        request: Arc<UndropTabularsRequest>,
        responses: Arc<Vec<UndropTabularResponse>>,
        request_metadata: Arc<RequestMetadata>,
    ) {
        futures::future::join_all(self.0.iter().map(|hook| {
            hook.undrop_tabular(
                warehouse_id,
                request.clone(),
                responses.clone(),
                request_metadata.clone(),
            )
        }))
        .await;
    }
}

#[async_trait::async_trait]
pub trait EndpointHooks: Send + Sync + Debug {
    async fn commit_table(
        &self,
        _warehouse_id: WarehouseId,
        _request: Arc<CommitTransactionRequest>,
        _responses: Arc<Vec<CommitContext>>,
        _table_ident_map: Arc<HashMap<TableIdent, TableId>>,
        _request_metadata: Arc<RequestMetadata>,
    ) {
        // Default implementation does nothing
    }

    async fn drop_table(
        &self,
        _warehouse_id: WarehouseId,
        _parameters: TableParameters,
        _drop_params: DropParams,
        _table_id: TableId,
        _request_metadata: Arc<RequestMetadata>,
    ) {
    }
    async fn register_table(
        &self,
        _warehouse_id: WarehouseId,
        _parameters: NamespaceParameters,
        _request: Arc<RegisterTableRequest>,
        _metadata: Arc<TableMetadata>,
        _request_metadata: Arc<RequestMetadata>,
    ) {
    }
    async fn create_table(
        &self,
        _warehouse_id: WarehouseId,
        _parameters: NamespaceParameters,
        _request: Arc<CreateTableRequest>,
        _metadata: Arc<TableMetadata>,
        _data_access: DataAccess,
        _request_metadata: Arc<RequestMetadata>,
    ) {
    }

    async fn rename_table(
        &self,
        _warehouse_id: WarehouseId,
        _table_id: TableId,
        _request: Arc<RenameTableRequest>,
        _request_metadata: Arc<RequestMetadata>,
    ) {
    }

    async fn create_view(
        &self,
        _warehouse_id: WarehouseId,
        _parameters: NamespaceParameters,
        _request: Arc<CreateViewRequest>,
        _metadata: Arc<ViewMetadata>,
        _data_access: DataAccess,
        _request_metadata: Arc<RequestMetadata>,
    ) {
    }

    #[allow(clippy::too_many_arguments)]
    async fn commit_view(
        &self,
        _warehouse_id: WarehouseId,
        _parameters: ViewParameters,
        _request: Arc<CommitViewRequest>,
        _metadata: Arc<ViewMetadata>,
        _data_access: DataAccess,
        _request_metadata: Arc<RequestMetadata>,
    ) {
    }

    async fn drop_view(
        &self,
        _warehouse_id: WarehouseId,
        _parameters: ViewParameters,
        _drop_params: DropParams,
        _view_id: ViewId,
        _request_metadata: Arc<RequestMetadata>,
    ) {
    }

    async fn rename_view(
        &self,
        _warehouse_id: WarehouseId,
        _view_id: ViewId,
        _request: Arc<RenameTableRequest>,
        _request_metadata: Arc<RequestMetadata>,
    ) {
    }

    async fn undrop_tabular(
        &self,
        _warehouse_id: WarehouseId,
        _request: Arc<UndropTabularsRequest>,
        _responses: Arc<Vec<UndropTabularResponse>>,
        _request_metadata: Arc<RequestMetadata>,
    ) {
    }
}
