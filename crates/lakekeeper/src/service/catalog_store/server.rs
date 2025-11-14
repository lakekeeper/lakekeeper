use crate::service::ServerId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerInfo {
    /// Server ID of the catalog at the time of bootstrapping
    pub(crate) server_id: ServerId,
    /// Whether the terms have been accepted
    pub(crate) terms_accepted: bool,
    /// Whether the catalog is open for re-bootstrap,
    /// i.e. to recover admin access.
    pub(crate) open_for_bootstrap: bool,
}

impl ServerInfo {
    /// Returns the server ID if the catalog is bootstrapped.
    #[must_use]
    pub fn server_id(&self) -> ServerId {
        self.server_id
    }

    /// Returns true if the catalog is bootstrapped.
    #[must_use]
    pub fn is_open_for_bootstrap(&self) -> bool {
        self.open_for_bootstrap
    }

    /// Returns true if the terms have been accepted.
    #[must_use]
    pub fn terms_accepted(&self) -> bool {
        self.terms_accepted
    }
}
