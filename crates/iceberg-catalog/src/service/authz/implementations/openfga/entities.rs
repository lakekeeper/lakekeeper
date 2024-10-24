use crate::service::authz::implementations::openfga::{OpenFGAError, OpenFGAResult};
use crate::service::authz::implementations::FgaType;
use crate::service::token_verification::Actor;
use crate::service::{NamespaceIdentUuid, RoleId, TableIdentUuid, UserId, ViewIdentUuid};
use crate::{ProjectIdent, WarehouseIdent};
use std::str::FromStr;

pub(super) trait ParseOpenFgaEntity: Sized {
    fn parse_from_openfga(s: &str) -> OpenFGAResult<Self> {
        let parts = s.split(':').collect::<Vec<&str>>();

        if parts.len() != 2 {
            return Err(OpenFGAError::InvalidEntity(s.to_string()));
        }

        let r#type =
            FgaType::from_str(parts[0]).map_err(|e| OpenFGAError::UnknownType(e.to_string()))?;

        Self::try_from_openfga_id(r#type, parts[1])
    }

    fn try_from_openfga_id(r#type: FgaType, id: &str) -> OpenFGAResult<Self>;
}

pub(super) trait OpenFgaEntity: Sized {
    fn to_openfga(&self) -> String;

    fn openfga_type(&self) -> FgaType;
}

impl OpenFgaEntity for RoleId {
    fn to_openfga(&self) -> String {
        format!("role:{self}")
    }

    fn openfga_type(&self) -> FgaType {
        FgaType::Role
    }
}

impl ParseOpenFgaEntity for RoleId {
    fn try_from_openfga_id(r#type: FgaType, id: &str) -> OpenFGAResult<Self> {
        if r#type != FgaType::Role {
            return Err(OpenFGAError::unexpected_entity(
                vec![FgaType::Role],
                id.to_string(),
            ));
        }

        id.parse()
            .map_err(|_e| OpenFGAError::unexpected_entity(vec![FgaType::Role], id.to_string()))
    }
}

impl OpenFgaEntity for UserId {
    fn to_openfga(&self) -> String {
        format!("user:{self}")
    }

    fn openfga_type(&self) -> FgaType {
        FgaType::User
    }
}

impl ParseOpenFgaEntity for UserId {
    fn try_from_openfga_id(r#type: FgaType, id: &str) -> OpenFGAResult<Self> {
        if r#type != FgaType::User {
            return Err(OpenFGAError::unexpected_entity(
                vec![FgaType::User],
                id.to_string(),
            ));
        }

        UserId::new(id)
            .map_err(|_e| OpenFGAError::unexpected_entity(vec![FgaType::User], id.to_string()))
    }
}

impl OpenFgaEntity for Actor {
    fn to_openfga(&self) -> String {
        let fga_type = self.openfga_type().to_string();
        match self {
            Actor::Anonymous => format!("{fga_type}:*").to_string(),
            Actor::Principal(principal) => format!("{fga_type}:{principal}"),
            Actor::Role {
                principal: _,
                assumed_role,
            } => format!("{fga_type}:{assumed_role}#assignee"),
        }
    }

    fn openfga_type(&self) -> FgaType {
        match self {
            Actor::Anonymous | Actor::Principal(_) => FgaType::User,
            Actor::Role { .. } => FgaType::Role,
        }
    }
}

impl OpenFgaEntity for ProjectIdent {
    fn to_openfga(&self) -> String {
        format!("{}:{self}", self.openfga_type())
    }

    fn openfga_type(&self) -> FgaType {
        FgaType::Project
    }
}

impl OpenFgaEntity for WarehouseIdent {
    fn to_openfga(&self) -> String {
        format!("{}:{self}", self.openfga_type())
    }

    fn openfga_type(&self) -> FgaType {
        FgaType::Warehouse
    }
}

impl OpenFgaEntity for TableIdentUuid {
    fn to_openfga(&self) -> String {
        format!("{}:{self}", self.openfga_type())
    }

    fn openfga_type(&self) -> FgaType {
        FgaType::Table
    }
}

impl OpenFgaEntity for NamespaceIdentUuid {
    fn to_openfga(&self) -> String {
        format!("{}:{self}", self.openfga_type())
    }

    fn openfga_type(&self) -> FgaType {
        FgaType::Namespace
    }
}

impl OpenFgaEntity for ViewIdentUuid {
    fn to_openfga(&self) -> String {
        format!("{}:{self}", self.openfga_type())
    }

    fn openfga_type(&self) -> FgaType {
        FgaType::View
    }
}
