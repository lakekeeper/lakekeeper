package trino

import data.trino
import data.configuration

allow_view if {
    allow_view_modify
}

allow_view if {
    allow_view_create
}

allow_view if {
    allow_view_rename
}

allow_view if {
    allow_view_drop
}

allow_view_create if {
    input.action.operation in ["CreateView", "CreateMaterializedView"]
    catalog := input.action.resource.table.catalogName
    schema := input.action.resource.table.schemaName
    trino.require_schema_access(catalog, schema, "create_view")
}

allow_view_modify if {
    input.action.operation in ["SetViewComment"]
    catalog := input.action.resource.table.catalogName
    schema := input.action.resource.table.schemaName
    table := input.action.resource.table.tableName
    trino.require_view_access(catalog, schema, table, "write_data")
}

allow_view_rename if {
    input.action.operation in ["RenameView"]
    source_catalog := input.action.resource.table.catalogName
    source_schema := input.action.resource.table.schemaName
    source_table := input.action.resource.table.tableName
    target_catalog := input.action.targetResource.table.catalogName
    target_schema := input.action.targetResource.table.schemaName
    trino.require_view_access(source_catalog, source_schema, source_table, "rename")
    trino.require_schema_access(target_catalog, target_schema, "create_view")
}

allow_view_drop if {
    input.action.operation in ["DropView"]
    catalog := input.action.resource.table.catalogName
    schema := input.action.resource.table.schemaName
    table := input.action.resource.table.tableName
    trino.require_view_access(catalog, schema, table, "drop")
}
