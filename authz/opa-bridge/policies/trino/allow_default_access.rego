package trino

import future.keywords.in
import future.keywords.if

import data.configuration

# Tables allowed in information_schema across all catalogs.
# Re-used in allow_schema.rego for Lakekeeper catalogs.
allowed_information_schema_tables := ["columns", "schemata", "tables", "views"]

# System schemas in Lakekeeper catalogs (not user-created, handled separately)
lakekeeper_system_schemas := ["information_schema", "schema_discovery", "system"]

# Tables allowed in Lakekeeper catalog system schemas
allowed_schema_discovery_tables := ["discovery", "shallow_discovery"]
allowed_system_schema_tables := ["iceberg_tables"]

# Tables allowed in system.metadata - excludes *_authorization and broken_catalog_definitions
allowed_metadata_tables := [
    "analyze_properties",
    "catalogs",
    "column_properties",
    "schema_properties",
    "table_comments",
    "table_properties",
]

# Admin check - reusable across policies
is_admin if {
    trino_user_id in configuration.trino_admin_users
}

allow_default_access if {
    allow_admin_system_access
}

allow_default_access if {
    allow_execute_query
}

allow_default_access if {
    allow_access_catalog_on_system_catalog
}

allow_default_access if {
    allow_jdbc_schema
}

allow_default_access if {
    allow_filter_catalogs_for_system_catalog
}

allow_default_access if {
    allow_show_schemas_on_system_catalog
}

allow_default_access if {
    allow_metadata_schema
}

allow_default_access if {
    allow_information_schema
}

allow_default_access if {
    allow_runtime_schema
}

# Every authenticated user can execute queries.
allow_execute_query if {
	input.action.operation == "ExecuteQuery"
}

allow_access_catalog_on_system_catalog if {
	input.action.operation == "AccessCatalog"
	input.action.resource.catalog.name == "system"
}

allow_show_schemas_on_system_catalog if {
	input.action.operation == "ShowSchemas"
	input.action.resource.catalog.name == "system"
}

# Allow access for JDBC Clients, which require access to the "system.jdbc" schema
allow_jdbc_schema if {
	input.action.operation == "FilterSchemas"
	input.action.resource.schema.catalogName == "system"
    input.action.resource.schema.schemaName == "jdbc"
}

allow_jdbc_schema if {
	input.action.operation in ["SelectFromColumns", "FilterTables", "FilterColumns"]
	input.action.resource.table.catalogName == "system"
    input.action.resource.table.schemaName == "jdbc"
}

# Metadata schema used by starburst UI
allow_metadata_schema if {
	input.action.operation == "FilterSchemas"
	input.action.resource.schema.catalogName == "system"
    input.action.resource.schema.schemaName == "metadata"
}

allow_metadata_schema if {
	input.action.operation in ["SelectFromColumns", "FilterTables", "FilterColumns"]
	input.action.resource.table.catalogName == "system"
    input.action.resource.table.schemaName == "metadata"
    input.action.resource.table.tableName in allowed_metadata_tables
}

allow_information_schema if {
	input.action.operation == "FilterSchemas"
	input.action.resource.schema.catalogName == "system"
    input.action.resource.schema.schemaName == "information_schema"
}

allow_information_schema if {
	input.action.operation in ["SelectFromColumns", "FilterTables", "FilterColumns"]
	input.action.resource.table.catalogName == "system"
    input.action.resource.table.schemaName == "information_schema"
    input.action.resource.table.tableName in allowed_information_schema_tables
}

# Public access to system.runtime - only the "queries" table
allow_runtime_schema if {
	input.action.operation == "FilterSchemas"
	input.action.resource.schema.catalogName == "system"
    input.action.resource.schema.schemaName == "runtime"
}

allow_runtime_schema if {
	input.action.operation in ["SelectFromColumns", "FilterTables", "FilterColumns"]
	input.action.resource.table.catalogName == "system"
    input.action.resource.table.schemaName == "runtime"
    input.action.resource.table.tableName == "queries"
}

allow_filter_catalogs_for_system_catalog if {
    input.action.operation == "FilterCatalogs"
    input.action.resource.catalog.name == "system"
}

# ------------- Admin Access -------------
# Admins get full access to all schemas and tables in the system catalog
# (no table filtering on metadata, information_schema, etc.)
allow_admin_system_access if {
    is_admin
    input.action.operation in ["FilterSchemas", "ShowSchemas"]
    input.action.resource.schema.catalogName == "system"
}

allow_admin_system_access if {
    is_admin
    input.action.operation in ["SelectFromColumns", "FilterTables", "FilterColumns"]
    input.action.resource.table.catalogName == "system"
}

# Admins can view queries owned by any user
allow_admin_system_access if {
    is_admin
    input.action.operation in ["FilterViewQueryOwnedBy", "ViewQueryOwnedBy"]
}

# Non-admins can only view their own queries
allow_default_access if {
    input.action.operation == "FilterViewQueryOwnedBy"
    input.action.resource.user.user == input.context.identity.user
}

allow_default_access if {
    input.action.operation == "ViewQueryOwnedBy"
    input.action.resource.user.user == input.context.identity.user
}
