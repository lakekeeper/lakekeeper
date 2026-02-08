use std::str::FromStr;

use lakekeeper::service::{
    NamespaceId, ProjectId, RoleId, ServerId, TableId, ViewId, WarehouseId,
    authn::{Actor, UserId},
    authz::RoleAssignee,
};

use crate::{
    FgaType,
    error::{OpenFGAError, OpenFGAResult},
};

/// Parse a table ID from an OpenFGA object string.
/// Format: `lakekeeper_table:{warehouse_id}/{table_id}`
/// Only returns the table ID if it belongs to the specified warehouse.
///
/// # Arguments
/// * `obj` - The OpenFGA object string to parse
/// * `warehouse_id` - The warehouse ID to filter by
///
/// # Returns
/// * `Ok(TableId)` - The parsed table ID if it belongs to the specified warehouse
/// * `Err(OpenFGAError::InvalidEntity)` - If the object string is malformed or belongs to a different warehouse
pub(crate) fn parse_table_from_openfga(obj: &str, warehouse_id: WarehouseId) -> OpenFGAResult<TableId> {
    // Expected format: "lakekeeper_table:{warehouse_uuid}/{table_uuid}"
    let parts: Vec<&str> = obj.split(':').collect();
    if parts.len() != 2 || parts[0] != FgaType::Table.to_string() {
        return Err(OpenFGAError::InvalidEntity(obj.to_string()));
    }

    let id_parts: Vec<&str> = parts[1].split('/').collect();
    if id_parts.len() != 2 {
        return Err(OpenFGAError::InvalidEntity(obj.to_string()));
    }

    let parsed_warehouse_id = WarehouseId::from_str_or_bad_request(id_parts[0])
        .map_err(|e| OpenFGAError::InvalidEntity(format!("{}: {}", obj, e.message)))?;

    // Filter to only include tables from the requested warehouse
    if parsed_warehouse_id != warehouse_id {
        return Err(OpenFGAError::InvalidEntity(format!(
            "Table {} belongs to different warehouse",
            obj
        )));
    }

    TableId::from_str_or_bad_request(id_parts[1])
        .map_err(|e| OpenFGAError::InvalidEntity(format!("{}: {}", obj, e.message)))
}

/// Parse a view ID from an OpenFGA object string.
/// Format: `lakekeeper_view:{warehouse_id}/{view_id}`
/// Only returns the view ID if it belongs to the specified warehouse.
///
/// # Arguments
/// * `obj` - The OpenFGA object string to parse
/// * `warehouse_id` - The warehouse ID to filter by
///
/// # Returns
/// * `Ok(ViewId)` - The parsed view ID if it belongs to the specified warehouse
/// * `Err(OpenFGAError::InvalidEntity)` - If the object string is malformed or belongs to a different warehouse
pub(crate) fn parse_view_from_openfga(obj: &str, warehouse_id: WarehouseId) -> OpenFGAResult<ViewId> {
    // Expected format: "lakekeeper_view:{warehouse_uuid}/{view_uuid}"
    let parts: Vec<&str> = obj.split(':').collect();
    if parts.len() != 2 || parts[0] != FgaType::View.to_string() {
        return Err(OpenFGAError::InvalidEntity(obj.to_string()));
    }

    let id_parts: Vec<&str> = parts[1].split('/').collect();
    if id_parts.len() != 2 {
        return Err(OpenFGAError::InvalidEntity(obj.to_string()));
    }

    let parsed_warehouse_id = WarehouseId::from_str_or_bad_request(id_parts[0])
        .map_err(|e| OpenFGAError::InvalidEntity(format!("{}: {}", obj, e.message)))?;

    // Filter to only include views from the requested warehouse
    if parsed_warehouse_id != warehouse_id {
        return Err(OpenFGAError::InvalidEntity(format!(
            "View {} belongs to different warehouse",
            obj
        )));
    }

    ViewId::from_str_or_bad_request(id_parts[1])
        .map_err(|e| OpenFGAError::InvalidEntity(format!("{}: {}", obj, e.message)))
}

/// Parse a namespace ID from an OpenFGA object string.
/// Format: `lakekeeper_namespace:{namespace_id}`
///
/// # Arguments
/// * `obj` - The OpenFGA object string to parse
///
/// # Returns
/// * `Ok(NamespaceId)` - The parsed namespace ID
/// * `Err(OpenFGAError::InvalidEntity)` - If the object string is malformed
pub(crate) fn parse_namespace_from_openfga(obj: &str) -> OpenFGAResult<NamespaceId> {
    // Expected format: "lakekeeper_namespace:{namespace_uuid}"
    let parts: Vec<&str> = obj.split(':').collect();
    if parts.len() != 2 || parts[0] != FgaType::Namespace.to_string() {
        return Err(OpenFGAError::InvalidEntity(obj.to_string()));
    }

    NamespaceId::from_str_or_bad_request(parts[1])
        .map_err(|e| OpenFGAError::InvalidEntity(format!("{}: {}", obj, e.message)))
}

pub(crate) trait ParseOpenFgaEntity: Sized {
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

pub(crate) trait OpenFgaEntity: Sized {
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

        RoleId::from_str_or_bad_request(id).map_err(|e| {
            OpenFGAError::unexpected_entity(vec![FgaType::Role], id.to_string(), e.message)
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

        Ok(RoleAssignee::from_role(
            RoleId::from_str_or_bad_request(id).map_err(|e| {
                OpenFGAError::unexpected_entity(vec![FgaType::Role], id.to_string(), e.message)
            })?,
        ))
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
            Actor::Principal(principal) => principal.to_openfga(),
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

impl OpenFgaEntity for ServerId {
    fn to_openfga(&self) -> String {
        format!("{}:{self}", self.openfga_type())
    }

    fn openfga_type(&self) -> FgaType {
        FgaType::Server
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

        ProjectId::from_str(id).map_err(|e| {
            OpenFGAError::unexpected_entity(vec![FgaType::Project], id.to_string(), e.message)
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

/// Adds warehouse context to the `OpenFga` entity for `table`.
///
/// Table ids can be reused across warehouses, so this context is required to ensure that `table`
/// entities are unique.
impl OpenFgaEntity for (WarehouseId, TableId) {
    fn to_openfga(&self) -> String {
        format!("{}:{}/{}", self.openfga_type(), self.0, self.1)
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

/// Adds warehouse context to the `OpenFga` entity for `view`.
///
/// View ids can be reused across warehouses, so this context is required to ensure that `view`
/// entities are unique.
impl OpenFgaEntity for (WarehouseId, ViewId) {
    fn to_openfga(&self) -> String {
        format!("{}:{}/{}", self.openfga_type(), self.0, self.1)
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

        let actor = Actor::Principal(parsed.clone());
        assert_eq!(actor.to_openfga(), openfga_id);
        assert_eq!(actor.openfga_type(), FgaType::User);
    }

    /// The `OpenFgaEntity` implementation for `ServerId` was added after `ServerId` itself.
    /// This test verifies that `ServerId::to_openfga` is backwards compatible.
    #[test]
    fn test_server_id_openfga_backwards_compatibility() {
        let id = ServerId::new_random();
        let entity = id.to_openfga();
        let previous_entity = format!("server:{id}");
        assert_eq!(entity, previous_entity);
    }

    #[test]
    fn test_parse_table_from_openfga_valid() {
        let warehouse_id = WarehouseId::new_random();
        let table_id = TableId::new_random();
        let openfga_obj = (warehouse_id, table_id).to_openfga();

        let parsed = parse_table_from_openfga(&openfga_obj, warehouse_id).unwrap();
        assert_eq!(parsed, table_id);
    }

    #[test]
    fn test_parse_table_from_openfga_wrong_warehouse() {
        let warehouse_id = WarehouseId::new_random();
        let other_warehouse_id = WarehouseId::new_random();
        let table_id = TableId::new_random();
        let openfga_obj = (warehouse_id, table_id).to_openfga();

        let result = parse_table_from_openfga(&openfga_obj, other_warehouse_id);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, OpenFGAError::InvalidEntity(_)));
        assert!(err.to_string().contains("belongs to different warehouse"));
    }

    #[test]
    fn test_parse_table_from_openfga_wrong_type() {
        let warehouse_id = WarehouseId::new_random();
        let view_id = ViewId::new_random();
        // Create a view object string instead of table
        let openfga_obj = (warehouse_id, view_id).to_openfga();

        let result = parse_table_from_openfga(&openfga_obj, warehouse_id);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), OpenFGAError::InvalidEntity(_)));
    }

    #[test]
    fn test_parse_table_from_openfga_missing_colon() {
        let warehouse_id = WarehouseId::new_random();
        let malformed = "lakekeeper_table_no_colon";

        let result = parse_table_from_openfga(malformed, warehouse_id);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), OpenFGAError::InvalidEntity(_)));
    }

    #[test]
    fn test_parse_table_from_openfga_missing_slash() {
        let warehouse_id = WarehouseId::new_random();
        let malformed = format!("lakekeeper_table:{}", warehouse_id);

        let result = parse_table_from_openfga(&malformed, warehouse_id);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), OpenFGAError::InvalidEntity(_)));
    }

    #[test]
    fn test_parse_table_from_openfga_invalid_warehouse_uuid() {
        let warehouse_id = WarehouseId::new_random();
        let malformed = "lakekeeper_table:not-a-uuid/550e8400-e29b-41d4-a716-446655440000";

        let result = parse_table_from_openfga(malformed, warehouse_id);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), OpenFGAError::InvalidEntity(_)));
    }

    #[test]
    fn test_parse_table_from_openfga_invalid_table_uuid() {
        let warehouse_id = WarehouseId::new_random();
        let malformed = format!("lakekeeper_table:{}/not-a-uuid", warehouse_id);

        let result = parse_table_from_openfga(&malformed, warehouse_id);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), OpenFGAError::InvalidEntity(_)));
    }

    #[test]
    fn test_parse_table_from_openfga_empty_string() {
        let warehouse_id = WarehouseId::new_random();

        let result = parse_table_from_openfga("", warehouse_id);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), OpenFGAError::InvalidEntity(_)));
    }

    #[test]
    fn test_parse_table_from_openfga_extra_colons() {
        let warehouse_id = WarehouseId::new_random();
        let table_id = TableId::new_random();
        let malformed = format!("lakekeeper_table:{}:{}:{}", warehouse_id, table_id, "extra");

        let result = parse_table_from_openfga(&malformed, warehouse_id);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), OpenFGAError::InvalidEntity(_)));
    }

    #[test]
    fn test_parse_table_from_openfga_round_trip() {
        // Test that to_openfga and parse_table_from_openfga are inverses
        let warehouse_id = WarehouseId::new_random();
        let table_id = TableId::new_random();

        let openfga_str = (warehouse_id, table_id).to_openfga();
        let parsed = parse_table_from_openfga(&openfga_str, warehouse_id).unwrap();

        assert_eq!(parsed, table_id);
    }

    #[test]
    fn test_parse_view_from_openfga_valid() {
        let warehouse_id = WarehouseId::new_random();
        let view_id = ViewId::new_random();
        let openfga_obj = (warehouse_id, view_id).to_openfga();

        let parsed = parse_view_from_openfga(&openfga_obj, warehouse_id).unwrap();
        assert_eq!(parsed, view_id);
    }

    #[test]
    fn test_parse_view_from_openfga_wrong_warehouse() {
        let warehouse_id = WarehouseId::new_random();
        let other_warehouse_id = WarehouseId::new_random();
        let view_id = ViewId::new_random();
        let openfga_obj = (warehouse_id, view_id).to_openfga();

        let result = parse_view_from_openfga(&openfga_obj, other_warehouse_id);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, OpenFGAError::InvalidEntity(_)));
        assert!(err.to_string().contains("belongs to different warehouse"));
    }

    #[test]
    fn test_parse_view_from_openfga_wrong_type() {
        let warehouse_id = WarehouseId::new_random();
        let table_id = TableId::new_random();
        // Create a table object string instead of view
        let openfga_obj = (warehouse_id, table_id).to_openfga();

        let result = parse_view_from_openfga(&openfga_obj, warehouse_id);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), OpenFGAError::InvalidEntity(_)));
    }

    #[test]
    fn test_parse_view_from_openfga_missing_colon() {
        let warehouse_id = WarehouseId::new_random();
        let malformed = "lakekeeper_view_no_colon";

        let result = parse_view_from_openfga(malformed, warehouse_id);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), OpenFGAError::InvalidEntity(_)));
    }

    #[test]
    fn test_parse_view_from_openfga_missing_slash() {
        let warehouse_id = WarehouseId::new_random();
        let malformed = format!("lakekeeper_view:{}", warehouse_id);

        let result = parse_view_from_openfga(&malformed, warehouse_id);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), OpenFGAError::InvalidEntity(_)));
    }

    #[test]
    fn test_parse_view_from_openfga_invalid_warehouse_uuid() {
        let warehouse_id = WarehouseId::new_random();
        let malformed = "lakekeeper_view:not-a-uuid/550e8400-e29b-41d4-a716-446655440000";

        let result = parse_view_from_openfga(malformed, warehouse_id);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), OpenFGAError::InvalidEntity(_)));
    }

    #[test]
    fn test_parse_view_from_openfga_invalid_view_uuid() {
        let warehouse_id = WarehouseId::new_random();
        let malformed = format!("lakekeeper_view:{}/not-a-uuid", warehouse_id);

        let result = parse_view_from_openfga(&malformed, warehouse_id);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), OpenFGAError::InvalidEntity(_)));
    }

    #[test]
    fn test_parse_view_from_openfga_empty_string() {
        let warehouse_id = WarehouseId::new_random();

        let result = parse_view_from_openfga("", warehouse_id);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), OpenFGAError::InvalidEntity(_)));
    }

    #[test]
    fn test_parse_view_from_openfga_extra_colons() {
        let warehouse_id = WarehouseId::new_random();
        let view_id = ViewId::new_random();
        let malformed = format!("lakekeeper_view:{}:{}:{}", warehouse_id, view_id, "extra");

        let result = parse_view_from_openfga(&malformed, warehouse_id);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), OpenFGAError::InvalidEntity(_)));
    }

    #[test]
    fn test_parse_view_from_openfga_round_trip() {
        // Test that to_openfga and parse_view_from_openfga are inverses
        let warehouse_id = WarehouseId::new_random();
        let view_id = ViewId::new_random();

        let openfga_str = (warehouse_id, view_id).to_openfga();
        let parsed = parse_view_from_openfga(&openfga_str, warehouse_id).unwrap();

        assert_eq!(parsed, view_id);
    }

    #[test]
    fn test_parse_namespace_from_openfga_valid() {
        let namespace_id = NamespaceId::new_random();
        let openfga_obj = namespace_id.to_openfga();

        let parsed = parse_namespace_from_openfga(&openfga_obj).unwrap();
        assert_eq!(parsed, namespace_id);
    }

    #[test]
    fn test_parse_namespace_from_openfga_wrong_type() {
        let warehouse_id = WarehouseId::new_random();
        // Create a warehouse object string instead of namespace
        let openfga_obj = warehouse_id.to_openfga();

        let result = parse_namespace_from_openfga(&openfga_obj);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), OpenFGAError::InvalidEntity(_)));
    }

    #[test]
    fn test_parse_namespace_from_openfga_missing_colon() {
        let malformed = "lakekeeper_namespace_no_colon";

        let result = parse_namespace_from_openfga(malformed);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), OpenFGAError::InvalidEntity(_)));
    }

    #[test]
    fn test_parse_namespace_from_openfga_invalid_uuid() {
        let malformed = "lakekeeper_namespace:not-a-uuid";

        let result = parse_namespace_from_openfga(malformed);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), OpenFGAError::InvalidEntity(_)));
    }

    #[test]
    fn test_parse_namespace_from_openfga_empty_string() {
        let result = parse_namespace_from_openfga("");
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), OpenFGAError::InvalidEntity(_)));
    }

    #[test]
    fn test_parse_namespace_from_openfga_extra_colons() {
        let namespace_id = NamespaceId::new_random();
        let malformed = format!("lakekeeper_namespace:{}:extra", namespace_id);

        let result = parse_namespace_from_openfga(&malformed);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), OpenFGAError::InvalidEntity(_)));
    }

    #[test]
    fn test_parse_namespace_from_openfga_round_trip() {
        // Test that to_openfga and parse_namespace_from_openfga are inverses
        let namespace_id = NamespaceId::new_random();

        let openfga_str = namespace_id.to_openfga();
        let parsed = parse_namespace_from_openfga(&openfga_str).unwrap();

        assert_eq!(parsed, namespace_id);
    }
}
