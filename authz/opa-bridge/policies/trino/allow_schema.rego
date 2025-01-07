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
    allow_select_from_columns_schemata
}

allow_schema if {
    allow_filter_schemas
}

allow_schema if {
    allow_show_create_schemas
}

allow_schema_create if {
    input.action.operation in ["CreateSchema"]
    catalog := input.action.resource.schema.catalogName
    schema := input.action.resource.schema.schemaName
    is_nested_schema(schema) == false
    trino.require_catalog_access(catalog, "create_namespace")
}

allow_schema_create if {
    input.action.operation in ["CreateSchema"]
    catalog := input.action.resource.schema.catalogName
    schema := input.action.resource.schema.schemaName
    is_nested_schema(schema) == true
    trino.require_schema_access(catalog, parent_schema(schema), "create_namespace")
}

allow_schema_drop if {
    input.action.operation in ["DropSchema"]
    catalog := input.action.resource.schema.catalogName
    schema := input.action.resource.schema.schemaName
    trino.require_schema_access(catalog, schema, "delete")
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
    trino.require_catalog_access(catalog, "list_namespaces")
}

allow_select_from_columns_schemata if {
    input.action.operation == "SelectFromColumns"
    input.action.resource.table.schemaName == "information_schema"
    input.action.resource.table.tableName in ["schemata", "tables", "columns"]
}

# always allow "information_schema" schema
allow_filter_schemas if {
    input.action.operation == "FilterSchemas"
    catalog := input.action.resource.schema.catalogName
    schema := input.action.resource.schema.schemaName
    schema == "information_schema"
}

allow_filter_schemas if {
    input.action.operation == "FilterSchemas"
    catalog := input.action.resource.schema.catalogName
    schema := input.action.resource.schema.schemaName
    schema != "information_schema"
    trino.require_schema_access(catalog, schema, "get_metadata")
}

allow_show_create_schemas if {
    input.action.operation == "ShowCreateSchema"
    catalog := input.action.resource.schema.catalogName
    schema := input.action.resource.schema.schemaName
    schema != "information_schema"
    trino.require_schema_access(catalog, schema, "get_metadata")
}
