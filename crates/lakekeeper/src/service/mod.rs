pub mod authn;
pub mod authz;
mod catalog_store;
pub mod contract_verification;
pub mod endpoint_hooks;
pub mod endpoint_statistics;
pub mod event_publisher;
pub mod health;
pub mod secrets;
pub mod storage;
pub mod task_configs;
pub mod tasks;
pub use authn::{Actor, UserId};
pub use catalog_store::*;
pub use endpoint_statistics::EndpointStatisticsTrackerTx;
#[allow(unused_imports)]
pub(crate) use identifier::tabular::TabularIdentBorrowed;
pub use identifier::tabular::{TabularId, TabularIdentOwned};
pub use lakekeeper_io::Location;
pub use secrets::{SecretId, SecretStore};
use tasks::RegisteredTaskQueues;

use self::authz::Authorizer;
pub use crate::api::{ErrorModel, IcebergErrorResponse};
use crate::{
    api::{ThreadSafe as ServiceState, management::v1::server::LicenseStatus},
    service::{contract_verification::ContractVerifiers, endpoint_hooks::EndpointHookCollection},
};

mod identifier;

pub use identifier::{generic::*, project::ProjectId};

// ---------------- State ----------------
#[derive(Clone, Debug)]
pub struct State<A: Authorizer + Clone, C: CatalogStore, S: SecretStore> {
    pub authz: A,
    pub catalog: C::State,
    pub secrets: S,
    pub contract_verifiers: ContractVerifiers,
    pub hooks: EndpointHookCollection,
    pub registered_task_queues: RegisteredTaskQueues,
    pub registered_project_task_queues: RegisteredTaskQueues,
    pub license_status: &'static LicenseStatus,
}

impl<A: Authorizer + Clone, C: CatalogStore, S: SecretStore> ServiceState for State<A, C, S> {}

impl<A: Authorizer + Clone, C: CatalogStore, S: SecretStore> State<A, C, S> {
    pub fn server_id(&self) -> ServerId {
        self.authz.server_id()
    }
}
