use std::{collections::HashMap, sync::Arc};

use iceberg::TableIdent;
use iceberg_ext::catalog::rest::{
    CommitTransactionRequest, CommitViewRequest, CreateTableRequest, CreateViewRequest,
    RegisterTableRequest, RenameTableRequest,
};

use crate::{
    api::{
        iceberg::{
            types::{DropParams, Prefix},
            v1::{DataAccess, NamespaceParameters, TableParameters, ViewParameters},
        },
        RequestMetadata,
    },
    service::{TableIdentUuid, ViewIdentUuid},
    WarehouseIdent,
};

#[derive(Clone)]
pub struct Hooks(Vec<Arc<dyn EndpointHooks>>);

impl core::fmt::Debug for Hooks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Hooks").field(&self.0.len()).finish()
    }
}

impl Hooks {
    #[must_use]
    pub fn new(hooks: Vec<Arc<dyn EndpointHooks>>) -> Self {
        Self(hooks)
    }
}

#[async_trait::async_trait]
impl EndpointHooks for Hooks {
    async fn commit_table(
        &self,
        _warehouse_id: WarehouseIdent,
        _prefix: Option<Prefix>,
        _request: Arc<CommitTransactionRequest>,
        _table_ident_map: Arc<HashMap<TableIdent, TableIdentUuid>>,
        _request_metadata: Arc<RequestMetadata>,
    ) {
        for hook in &self.0 {
            hook.commit_table(
                _warehouse_id,
                _prefix.clone(),
                _request.clone(),
                _table_ident_map.clone(),
                _request_metadata.clone(),
            )
            .await;
        }
    }

    async fn drop_table(
        &self,
        warehouse_id: WarehouseIdent,
        parameters: TableParameters,
        drop_params: DropParams,
        table_ident_uuid: TableIdentUuid,
        request_metadata: Arc<RequestMetadata>,
    ) {
        for hook in &self.0 {
            hook.drop_table(
                warehouse_id,
                parameters.clone(),
                drop_params.clone(),
                table_ident_uuid,
                request_metadata.clone(),
            )
            .await;
        }
    }

    async fn register_table(
        &self,
        warehouse_id: WarehouseIdent,
        parameters: NamespaceParameters,
        request: Arc<RegisterTableRequest>,
        table_ident_uuid: TableIdentUuid,
        request_metadata: Arc<RequestMetadata>,
    ) {
        for hook in &self.0 {
            hook.register_table(
                warehouse_id,
                parameters.clone(),
                request.clone(),
                table_ident_uuid,
                request_metadata.clone(),
            )
            .await;
        }
    }

    async fn create_view(
        &self,
        warehouse_id: WarehouseIdent,
        view_ident_uuid: ViewIdentUuid,
        parameters: NamespaceParameters,
        request: Arc<CreateViewRequest>,
        data_access: DataAccess,
        request_metadata: Arc<RequestMetadata>,
    ) {
        for hook in &self.0 {
            hook.create_view(
                warehouse_id,
                view_ident_uuid,
                parameters.clone(),
                request.clone(),
                data_access,
                request_metadata.clone(),
            )
            .await;
        }
    }

    async fn commit_view(
        &self,
        warehouse_id: WarehouseIdent,
        view_ident_uuid: ViewIdentUuid,
        parameters: ViewParameters,
        request: Arc<CommitViewRequest>,
        data_access: DataAccess,
        request_metadata: Arc<RequestMetadata>,
    ) {
        for hook in &self.0 {
            hook.commit_view(
                warehouse_id,
                view_ident_uuid,
                parameters.clone(),
                request.clone(),
                data_access,
                request_metadata.clone(),
            )
            .await;
        }
    }

    async fn drop_view(
        &self,
        warehouse_id: WarehouseIdent,
        parameters: ViewParameters,
        drop_params: DropParams,
        view_ident_uuid: ViewIdentUuid,
        request_metadata: Arc<RequestMetadata>,
    ) {
        for hook in &self.0 {
            hook.drop_view(
                warehouse_id,
                parameters.clone(),
                drop_params.clone(),
                view_ident_uuid,
                request_metadata.clone(),
            )
            .await;
        }
    }

    async fn rename_view(
        &self,
        warehouse_id: WarehouseIdent,
        prefix: Option<Prefix>,
        view_ident_uuid: ViewIdentUuid,
        request: Arc<RenameTableRequest>,
        request_metadata: Arc<RequestMetadata>,
    ) {
        for hook in &self.0 {
            hook.rename_view(
                warehouse_id,
                prefix.clone(),
                view_ident_uuid,
                request.clone(),
                request_metadata.clone(),
            )
            .await;
        }
    }
}

#[async_trait::async_trait]
pub trait EndpointHooks: Send + Sync {
    async fn commit_table(
        &self,
        _warehouse_id: WarehouseIdent,
        _prefix: Option<Prefix>,
        _request: Arc<CommitTransactionRequest>,
        _table_ident_map: Arc<HashMap<TableIdent, TableIdentUuid>>,
        _request_metadata: Arc<RequestMetadata>,
    ) {
        // Default implementation does nothing
    }

    async fn drop_table(
        &self,
        _warehouse_id: WarehouseIdent,
        _parameters: TableParameters,
        _drop_params: DropParams,
        _table_ident_uuid: TableIdentUuid,
        _request_metadata: Arc<RequestMetadata>,
    ) {
    }
    async fn register_table(
        &self,
        _warehouse_id: WarehouseIdent,
        _parameters: NamespaceParameters,
        _request: Arc<RegisterTableRequest>,
        _table_ident_uuid: TableIdentUuid,
        _request_metadata: Arc<RequestMetadata>,
    ) {
    }
    async fn create_table(
        &self,
        _warehouse_id: WarehouseIdent,
        _parameters: NamespaceParameters,
        _table_ident_uuid: TableIdentUuid,
        _request: Arc<CreateTableRequest>,
        _data_access: DataAccess,
        _request_metadata: Arc<RequestMetadata>,
    ) {
    }

    async fn rename_table(
        &self,
        _warehouse_id: WarehouseIdent,
        _prefix: Option<Prefix>,
        _table_ident_uuid: TableIdentUuid,
        _request: Arc<RenameTableRequest>,
        _request_metadata: Arc<RequestMetadata>,
    ) {
    }

    async fn create_view(
        &self,
        _warehouse_id: WarehouseIdent,
        _view_ident_uuid: ViewIdentUuid,
        _parameters: NamespaceParameters,
        _request: Arc<CreateViewRequest>,
        _data_access: DataAccess,
        _request_metadata: Arc<RequestMetadata>,
    ) {
        // Default implementation does nothing
    }

    #[allow(clippy::too_many_arguments)]
    async fn commit_view(
        &self,
        _warehouse_id: WarehouseIdent,
        _view_ident_uuid: ViewIdentUuid,
        _parameters: ViewParameters,
        _request: Arc<CommitViewRequest>,
        _data_access: DataAccess,
        _request_metadata: Arc<RequestMetadata>,
    ) {
    }

    async fn drop_view(
        &self,
        _warehouse_id: WarehouseIdent,
        _parameters: ViewParameters,
        _drop_params: DropParams,
        _view_ident_uuid: ViewIdentUuid,
        _request_metadata: Arc<RequestMetadata>,
    ) {
    }

    async fn rename_view(
        &self,
        _warehouse_id: WarehouseIdent,
        _prefix: Option<Prefix>,
        _view_ident_uuid: ViewIdentUuid,
        _request: Arc<RenameTableRequest>,
        _request_metadata: Arc<RequestMetadata>,
    ) {
    }
    // TODO: undrop tabular
}
