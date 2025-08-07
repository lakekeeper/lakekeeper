use std::{
    collections::HashMap,
    fmt::{Debug, Display},
    sync::Arc,
};

use futures::TryFutureExt;
use iceberg::{
    spec::{TableMetadata, ViewMetadata},
    TableIdent,
};
use iceberg_ext::catalog::rest::{
    CommitTransactionRequest, CommitViewRequest, CreateTableRequest, CreateViewRequest,
    RegisterTableRequest, RenameTableRequest,
};
use lakekeeper_io::Location;

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
pub struct EndpointHookCollection(pub(crate) Vec<Arc<dyn EndpointHook>>);

impl core::fmt::Debug for EndpointHookCollection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Hooks").field(&self.0.len()).finish()
    }
}

impl EndpointHookCollection {
    #[must_use]
    pub fn new(hooks: Vec<Arc<dyn EndpointHook>>) -> Self {
        Self(hooks)
    }

    pub fn append(&mut self, hook: Arc<dyn EndpointHook>) -> &mut Self {
        self.0.push(hook);
        self
    }
}

impl Display for EndpointHookCollection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "EndpointHookCollection with [")?;
        for idx in 0..self.0.len() {
            if idx == self.0.len() - 1 {
                write!(f, "{}", self.0[idx])?;
            } else {
                write!(f, "{}, ", self.0[idx])?;
            }
        }
        write!(f, "]")
    }
}

#[derive(Debug, Clone)]
pub struct ViewCommit {
    pub old_metadata: ViewMetadata,
    pub new_metadata: ViewMetadata,
    pub old_metadata_location: Location,
    pub new_metadata_location: Location,
}

impl EndpointHookCollection {
    pub(crate) async fn commit_transaction(
        &self,
        warehouse_id: WarehouseId,
        request: Arc<CommitTransactionRequest>,
        commits: Arc<Vec<CommitContext>>,
        table_ident_map: Arc<HashMap<TableIdent, TableId>>,
        request_metadata: Arc<RequestMetadata>,
    ) {
        futures::future::join_all(self.0.iter().map(|hook| {
            hook.commit_transaction(
                warehouse_id,
                request.clone(),
                commits.clone(),
                table_ident_map.clone(),
                request_metadata.clone(),
            )
            .map_err(|e| {
                tracing::warn!(
                    "Hook '{}' encountered error on commit_transaction: {e:?}",
                    hook.to_string()
                );
            })
        }))
        .await;
    }

    pub(crate) async fn drop_table(
        &self,
        warehouse_id: WarehouseId,
        parameters: TableParameters,
        drop_params: DropParams,
        table_id: TableId,
        request_metadata: Arc<RequestMetadata>,
    ) {
        futures::future::join_all(self.0.iter().map(|hook| {
            hook.drop_table(
                warehouse_id,
                parameters.clone(),
                drop_params.clone(),
                table_id,
                request_metadata.clone(),
            )
            .map_err(|e| {
                tracing::warn!(
                    "Hook '{}' encountered error on drop_table: {e:?}",
                    hook.to_string()
                );
            })
        }))
        .await;
    }

    pub(crate) async fn register_table(
        &self,
        warehouse_id: WarehouseId,
        parameters: NamespaceParameters,
        request: Arc<RegisterTableRequest>,
        metadata: Arc<TableMetadata>,
        metadata_location: Arc<Location>,
        request_metadata: Arc<RequestMetadata>,
    ) {
        futures::future::join_all(self.0.iter().map(|hook| {
            hook.register_table(
                warehouse_id,
                parameters.clone(),
                request.clone(),
                metadata.clone(),
                metadata_location.clone(),
                request_metadata.clone(),
            )
            .map_err(|e| {
                tracing::warn!(
                    "Hook '{}' encountered error on register_table: {e:?}",
                    hook.to_string()
                );
            })
        }))
        .await;
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn create_table(
        &self,
        warehouse_id: WarehouseId,
        parameters: NamespaceParameters,
        request: Arc<CreateTableRequest>,
        metadata: Arc<TableMetadata>,
        metadata_location: Option<Arc<Location>>,
        data_access: DataAccess,
        request_metadata: Arc<RequestMetadata>,
    ) {
        futures::future::join_all(self.0.iter().map(|hook| {
            hook.create_table(
                warehouse_id,
                parameters.clone(),
                request.clone(),
                metadata.clone(),
                metadata_location.clone(),
                data_access,
                request_metadata.clone(),
            )
            .map_err(|e| {
                tracing::warn!(
                    "Hook '{}' encountered error on create_table: {e:?}",
                    hook.to_string()
                );
            })
        }))
        .await;
    }

    pub(crate) async fn rename_table(
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
            .map_err(|e| {
                tracing::warn!(
                    "Hook '{}' encountered error on rename_table: {e:?}",
                    hook.to_string()
                );
            })
        }))
        .await;
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn create_view(
        &self,
        warehouse_id: WarehouseId,
        parameters: NamespaceParameters,
        request: Arc<CreateViewRequest>,
        metadata: Arc<ViewMetadata>,
        metadata_location: Arc<Location>,
        data_access: DataAccess,
        request_metadata: Arc<RequestMetadata>,
    ) {
        futures::future::join_all(self.0.iter().map(|hook| {
            hook.create_view(
                warehouse_id,
                parameters.clone(),
                request.clone(),
                metadata.clone(),
                metadata_location.clone(),
                data_access,
                request_metadata.clone(),
            )
            .map_err(|e| {
                tracing::warn!(
                    "Hook '{}' encountered error on create_view: {e:?}",
                    hook.to_string()
                );
            })
        }))
        .await;
    }

    pub(crate) async fn commit_view(
        &self,
        warehouse_id: WarehouseId,
        parameters: ViewParameters,
        request: Arc<CommitViewRequest>,
        view_commit: Arc<ViewCommit>,
        data_access: DataAccess,
        request_metadata: Arc<RequestMetadata>,
    ) {
        futures::future::join_all(self.0.iter().map(|hook| {
            hook.commit_view(
                warehouse_id,
                parameters.clone(),
                request.clone(),
                view_commit.clone(),
                data_access,
                request_metadata.clone(),
            )
            .map_err(|e| {
                tracing::warn!(
                    "Hook '{}' encountered error on commit_view: {e:?}",
                    hook.to_string()
                );
            })
        }))
        .await;
    }

    pub(crate) async fn drop_view(
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
            .map_err(|e| {
                tracing::warn!(
                    "Hook '{}' encountered error on drop_view: {e:?}",
                    hook.to_string()
                );
            })
        }))
        .await;
    }

    pub(crate) async fn rename_view(
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
            .map_err(|e| {
                tracing::warn!(
                    "Hook '{}' encountered error on rename_view: {e:?}",
                    hook.to_string()
                );
            })
        }))
        .await;
    }

    pub(crate) async fn undrop_tabular(
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
            .map_err(|e| {
                tracing::warn!(
                    "Hook '{}' encountered error on undrop_tabular: {e:?}",
                    hook.to_string()
                );
            })
        }))
        .await;
    }
}

/// `EndpointHook` is a trait that allows for custom hooks to be executed within the context of
/// various endpoints.
///
/// The default implementation of every hook does nothing. Override any function if you want to
/// implement it.
///
/// An implementation should be light-weight, ideally every longer running task is deferred to a
/// background task via a channel or is spawned as a tokio task.
///
/// The `EndpointHook` are passed into the services via the [`EndpointHookCollection`]. If you want
/// to provide your own implementation, you'll have to fork and modify the main function to include
/// your hooks.
///
/// If the hook fails, it will be logged, but the request will continue to process. This is to ensure
/// that the request is not blocked by a hook failure.
#[async_trait::async_trait]
pub trait EndpointHook: Send + Sync + Debug + Display {
    async fn commit_transaction(
        &self,
        _warehouse_id: WarehouseId,
        _request: Arc<CommitTransactionRequest>,
        _commits: Arc<Vec<CommitContext>>,
        _table_ident_map: Arc<HashMap<TableIdent, TableId>>,
        _request_metadata: Arc<RequestMetadata>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn drop_table(
        &self,
        _warehouse_id: WarehouseId,
        _parameters: TableParameters,
        _drop_params: DropParams,
        _table_id: TableId,
        _request_metadata: Arc<RequestMetadata>,
    ) -> anyhow::Result<()> {
        Ok(())
    }
    async fn register_table(
        &self,
        _warehouse_id: WarehouseId,
        _parameters: NamespaceParameters,
        _request: Arc<RegisterTableRequest>,
        _metadata: Arc<TableMetadata>,
        _metadata_location: Arc<Location>,
        _request_metadata: Arc<RequestMetadata>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn create_table(
        &self,
        _warehouse_id: WarehouseId,
        _parameters: NamespaceParameters,
        _request: Arc<CreateTableRequest>,
        _metadata: Arc<TableMetadata>,
        _metadata_location: Option<Arc<Location>>,
        _data_access: DataAccess,
        _request_metadata: Arc<RequestMetadata>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn rename_table(
        &self,
        _warehouse_id: WarehouseId,
        _table_id: TableId,
        _request: Arc<RenameTableRequest>,
        _request_metadata: Arc<RequestMetadata>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn create_view(
        &self,
        _warehouse_id: WarehouseId,
        _parameters: NamespaceParameters,
        _request: Arc<CreateViewRequest>,
        _metadata: Arc<ViewMetadata>,
        _metadata_location: Arc<Location>,
        _data_access: DataAccess,
        _request_metadata: Arc<RequestMetadata>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn commit_view(
        &self,
        _warehouse_id: WarehouseId,
        _parameters: ViewParameters,
        _request: Arc<CommitViewRequest>,
        _view_commit: Arc<ViewCommit>,
        _data_access: DataAccess,
        _request_metadata: Arc<RequestMetadata>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn drop_view(
        &self,
        _warehouse_id: WarehouseId,
        _parameters: ViewParameters,
        _drop_params: DropParams,
        _view_id: ViewId,
        _request_metadata: Arc<RequestMetadata>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn rename_view(
        &self,
        _warehouse_id: WarehouseId,
        _view_id: ViewId,
        _request: Arc<RenameTableRequest>,
        _request_metadata: Arc<RequestMetadata>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn undrop_tabular(
        &self,
        _warehouse_id: WarehouseId,
        _request: Arc<UndropTabularsRequest>,
        _responses: Arc<Vec<UndropTabularResponse>>,
        _request_metadata: Arc<RequestMetadata>,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}
