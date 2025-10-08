use std::collections::HashMap;

use iceberg::NamespaceIdent;

use crate::{
    service::{tasks::TaskId, NamespaceId, TableIdent, TabularId},
    WarehouseId,
};

#[derive(Debug)]
pub struct GetNamespaceResponse {
    /// Reference to one or more levels of a namespace
    pub namespace: NamespaceIdent,
    pub namespace_id: NamespaceId,
    pub warehouse_id: WarehouseId,
    pub properties: Option<std::collections::HashMap<String, String>>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ListNamespacesResponse {
    pub next_page_tokens: Vec<(NamespaceId, String)>,
    pub namespaces: HashMap<NamespaceId, NamespaceIdent>,
}

#[derive(Debug, PartialEq)]
pub struct NamespaceInfo {
    pub namespace_ident: NamespaceIdent,
    pub protected: bool,
}

#[derive(Debug)]
pub struct NamespaceDropInfo {
    pub child_namespaces: Vec<NamespaceId>,
    // table-id, location, table-ident
    pub child_tables: Vec<(TabularId, String, TableIdent)>,
    pub open_tasks: Vec<TaskId>,
}
