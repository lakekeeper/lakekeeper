use std::str::FromStr;

use iceberg_ext::catalog::rest::IcebergErrorResponse;

use super::RoleAssignee;
use crate::{
    service::{
        authn::{Actor, UserId},
        authz::implementations::{
            openfga::{OpenFGAError, OpenFGAResult},
            FgaType,
        },
        NamespaceId, RoleId, TableId, ViewId,
    },
    ProjectId, WarehouseId,
};

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

impl OpenFgaEntity for RoleAssignee {
    fn to_openfga(&self) -> String {
        format!("{}#assignee", self.role().to_openfga())
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
                format!("Expected role type, but got {type}"),
            ));
        }

        id.parse().map_err(|e: IcebergErrorResponse| {
            OpenFGAError::unexpected_entity(vec![FgaType::Role], id.to_string(), e.error.message)
        })
    }
}

impl ParseOpenFgaEntity for RoleAssignee {
    fn try_from_openfga_id(r#type: FgaType, id: &str) -> OpenFGAResult<Self> {
        if r#type != FgaType::Role {
            return Err(OpenFGAError::unexpected_entity(
                vec![FgaType::Role],
                id.to_string(),
                format!("Expected role type, but got {type}"),
            ));
        }

        if !id.ends_with("#assignee") {
            return Err(OpenFGAError::unexpected_entity(
                vec![FgaType::Role],
                id.to_string(),
                "Expected role assignee type, but got a role".to_string(),
            ));
        }

        let id = &id[..id.len() - "#assignee".len()];

        Ok(RoleAssignee::from_role(id.parse().map_err(
            |e: IcebergErrorResponse| {
                OpenFGAError::unexpected_entity(
                    vec![FgaType::Role],
                    id.to_string(),
                    e.error.message,
                )
            },
        )?))
    }
}

impl OpenFgaEntity for UserId {
    fn to_openfga(&self) -> String {
        format!("user:{}", urlencoding::encode(&self.to_string()))
    }

    fn openfga_type(&self) -> FgaType {
        FgaType::User
    }
}

impl ParseOpenFgaEntity for UserId {
    fn try_from_openfga_id(r#type: FgaType, id: &str) -> OpenFGAResult<Self> {
        let id = urlencoding::decode(id)
            .map_err(|e| {
                OpenFGAError::unexpected_entity(
                    vec![FgaType::User],
                    id.to_string(),
                    format!("Failed to decode user ID: {e}"),
                )
            })?
            .to_string();
        if r#type != FgaType::User {
            return Err(OpenFGAError::unexpected_entity(
                vec![FgaType::User],
                id.clone(),
                format!("Expected user type, but got {type}"),
            ));
        }

        UserId::try_from(id.as_str())
            .map_err(|e| OpenFGAError::unexpected_entity(vec![FgaType::User], id, e.message))
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

impl OpenFgaEntity for ProjectId {
    fn to_openfga(&self) -> String {
        format!("{}:{self}", self.openfga_type())
    }

    fn openfga_type(&self) -> FgaType {
        FgaType::Project
    }
}

impl OpenFgaEntity for &ProjectId {
    fn to_openfga(&self) -> String {
        format!("{}:{self}", self.openfga_type())
    }

    fn openfga_type(&self) -> FgaType {
        FgaType::Project
    }
}

impl ParseOpenFgaEntity for ProjectId {
    fn try_from_openfga_id(r#type: FgaType, id: &str) -> OpenFGAResult<Self> {
        if r#type != FgaType::Project {
            return Err(OpenFGAError::unexpected_entity(
                vec![FgaType::Project],
                id.to_string(),
                format!("Expected project type, but got {type}"),
            ));
        }

        ProjectId::from_str(id).map_err(|e: IcebergErrorResponse| {
            OpenFGAError::unexpected_entity(vec![FgaType::Project], id.to_string(), e.error.message)
        })
    }
}

impl OpenFgaEntity for WarehouseId {
    fn to_openfga(&self) -> String {
        format!("{}:{self}", self.openfga_type())
    }

    fn openfga_type(&self) -> FgaType {
        FgaType::Warehouse
    }
}

impl OpenFgaEntity for TableId {
    fn to_openfga(&self) -> String {
        format!("{}:{self}", self.openfga_type())
    }

    fn openfga_type(&self) -> FgaType {
        FgaType::Table
    }
}

impl OpenFgaEntity for NamespaceId {
    fn to_openfga(&self) -> String {
        format!("{}:{self}", self.openfga_type())
    }

    fn openfga_type(&self) -> FgaType {
        FgaType::Namespace
    }
}

impl OpenFgaEntity for ViewId {
    fn to_openfga(&self) -> String {
        format!("{}:{self}", self.openfga_type())
    }

    fn openfga_type(&self) -> FgaType {
        FgaType::View
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_user_id_pre_0_9_can_be_parsed() {
        // Previously allowed characters up to 0.8: "-", "_", alphanumeric
        let user_id = "oidc~abc-def_ghi";
        let openfga_id = format!("user:{user_id}",);
        let parsed = UserId::parse_from_openfga(openfga_id.as_str()).unwrap();
        assert_eq!(parsed.to_openfga(), openfga_id);
        assert_eq!(parsed.openfga_type(), FgaType::User);
        assert_eq!(parsed.to_string(), user_id);
    }
}
