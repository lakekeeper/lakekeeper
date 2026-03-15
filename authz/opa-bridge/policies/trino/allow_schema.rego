package trino

import data.trino
import data.configuration

allow_schema if {
    allow_schema_create
}

allow_schema if {
    allow_schema_drop
}

allow_schema if {
    allow_schema_rename
}

allow_schema if {
    allow_show_schemas
}

allow_schema if {
    allow_tables_in_system_schemas
}

allow_schema if {
    allow_filter_schemas
}

allow_schema if {
    allow_show_create_schemas
}

allow_schema if {
    allow_show_tables_in_schema
}

allow_schema if {
    allow_filter_system_schemas
}

allow_schema if {
    allow_admin_system_schemas
}

allow_schema_create if {
    input.action.operation in ["CreateSchema"]
    catalog := input.action.resource.schema.catalogName
    schema := input.action.resource.schema.schemaName
    properties := object.get(input.action.resource.schema, "properties", {})
    flattended_properties := flatten_properties(properties)
    is_nested_schema(schema) == false
    trino.require_catalog_create_namespace_access(catalog, flattended_properties, schema)
}

allow_schema_create if {
    input.action.operation in ["CreateSchema"]
    catalog := input.action.resource.schema.catalogName
    schema := input.action.resource.schema.schemaName
    properties := object.get(input.action.resource.schema, "properties", {})
    flattended_properties := flatten_properties(properties)
    is_nested_schema(schema) == true
    trino.require_schema_access_create(catalog, parent_schema(schema), "create_namespace", flattended_properties, trino.child_schema_name(schema))
}

allow_schema_drop if {
    input.action.operation in ["DropSchema"]
    catalog := input.action.resource.schema.catalogName
    schema := input.action.resource.schema.schemaName
    trino.require_schema_access_simple(catalog, schema, "delete")
}

# renameNamespace is not supported for Iceberg REST catalog in trino.
# Lakekeeper supports renaming schemas, please use the UI or management API
# to rename schemas. (namespaces)
allow_schema_rename if {
    input.action.operation in ["RenameSchema"]
    false
}

allow_show_schemas if {
    input.action.operation in ["ShowSchemas"]
    catalog := input.action.resource.catalog.name
    trino.require_catalog_access_simple(catalog, "list_namespaces")
}

allow_filter_system_schemas if {
    input.action.operation == "FilterSchemas"
    catalog := input.action.resource.schema.catalogName
    schema := input.action.resource.schema.schemaName
    schema in trino.lakekeeper_system_schemas
    trino.require_catalog_access_simple(catalog, "get_config")
}

# Table-level access for Lakekeeper system schemas.
# Each schema has its own allowed table list defined in allow_default_access.rego.
allow_tables_in_system_schemas if {
    input.action.operation in ["SelectFromColumns", "FilterTables", "FilterColumns"]
    catalog := input.action.resource.table.catalogName
    input.action.resource.table.schemaName == "information_schema"
    input.action.resource.table.tableName in trino.allowed_information_schema_tables
    trino.require_catalog_access_simple(catalog, "get_config")
}

allow_tables_in_system_schemas if {
    input.action.operation in ["SelectFromColumns", "FilterTables", "FilterColumns"]
    catalog := input.action.resource.table.catalogName
    input.action.resource.table.schemaName == "schema_discovery"
    input.action.resource.table.tableName in trino.allowed_schema_discovery_tables
    trino.require_catalog_access_simple(catalog, "get_config")
}

allow_tables_in_system_schemas if {
    input.action.operation in ["SelectFromColumns", "FilterTables", "FilterColumns"]
    catalog := input.action.resource.table.catalogName
    input.action.resource.table.schemaName == "system"
    input.action.resource.table.tableName in trino.allowed_system_schema_tables
    trino.require_catalog_access_simple(catalog, "get_config")
}

allow_filter_schemas if {
    input.action.operation == "FilterSchemas"
    catalog := input.action.resource.schema.catalogName
    schema := input.action.resource.schema.schemaName
    not schema in trino.lakekeeper_system_schemas
    trino.require_schema_access_simple(catalog, schema, "get_metadata")
}

allow_show_create_schemas if {
    input.action.operation == "ShowCreateSchema"
    catalog := input.action.resource.schema.catalogName
    schema := input.action.resource.schema.schemaName
    not schema in trino.lakekeeper_system_schemas
    trino.require_schema_access_simple(catalog, schema, "get_metadata")
}

allow_show_tables_in_schema if {
    input.action.operation in ["ShowTables"]
    catalog := input.action.resource.schema.catalogName
    schema := input.action.resource.schema.schemaName
    trino.require_schema_access_simple(catalog, schema, "get_metadata")
}

# ------------- Admin Access -------------
# Admins get full access to all tables in Lakekeeper system schemas
# (no table filtering on information_schema, schema_discovery, system)
allow_admin_system_schemas if {
    trino.is_admin
    input.action.operation == "FilterSchemas"
    input.action.resource.schema.schemaName in trino.lakekeeper_system_schemas
}

allow_admin_system_schemas if {
    trino.is_admin
    input.action.operation in ["SelectFromColumns", "FilterTables", "FilterColumns"]
    input.action.resource.table.schemaName in trino.lakekeeper_system_schemas
}
