use openfga_rs::{open_fga_service_client::OpenFgaServiceClient, Store};

use crate::service::authz::implementations::openfga::client::ClientConnection;

#[allow(clippy::unused_async)]
pub(crate) async fn migrate(
    _client: &mut OpenFgaServiceClient<ClientConnection>,
    _auth_model_id: &str,
    _store: &Store,
) {
}
