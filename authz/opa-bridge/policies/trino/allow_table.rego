package trino

import data.trino
import data.configuration

allow_table if {
    allow_table_create
}

allow_table if {
    allow_table_drop
}

allow_table if {
    allow_table_rename
}

allow_table if {
    allow_table_modify
}

allow_table if {
    allow_show_tables
}

allow_table if {
    allow_table_metadata
}

allow_table if {
    allow_table_read
}

allow_table_create if {
    input.action.operation in ["CreateTable"]
    catalog := input.action.resource.table.catalogName
    schema := input.action.resource.table.schemaName
    table := input.action.resource.table.tableName
    trino.require_schema_access(catalog, schema, "create_table")
}

allow_table_drop if {
    input.action.operation in ["DropTable"]
    catalog := input.action.resource.table.catalogName
    schema := input.action.resource.table.schemaName
    table := input.action.resource.table.tableName
    trino.require_table_access(catalog, schema, table, "drop")
}

allow_table_rename if {
    input.action.operation in ["RenameTable"]
    source_catalog := input.action.resource.table.catalogName
    source_schema := input.action.resource.table.schemaName
    source_table := input.action.resource.table.tableName
    target_catalog := input.action.targetResource.table.catalogName
    target_schema := input.action.targetResource.table.schemaName
    trino.require_table_access(source_catalog, source_schema, source_table, "rename")
    trino.require_schema_access(target_catalog, target_schema, "create_table")
}

allow_table_modify if {
    input.action.operation in [
        "SetTableProperties", "SetTableComment", "SetColumnComment", 
        "AddColumn", "AlterColumn", "DropColumn", "RenameColumn", 
        "InsertIntoTable", "DeleteFromTable", "TruncateTable", 
        "UpdateTableColumns"]
    catalog := input.action.resource.table.catalogName
    schema := input.action.resource.table.schemaName
    table := input.action.resource.table.tableName
    trino.require_table_access(catalog, schema, table, "write_data")
}

allow_show_tables if {
    input.action.operation in ["ShowTables"]
    catalog := input.action.resource.schema.catalogName
    schema := input.action.resource.schema.schemaName
    trino.require_schema_access(catalog, schema, "get_metadata")
}

allow_table_metadata if {
    input.action.operation in ["FilterTables", "ShowColumns", "FilterColumns"]
    catalog := input.action.resource.table.catalogName
    schema := input.action.resource.table.schemaName
    table := input.action.resource.table.tableName
    trino.require_table_access(catalog, schema, table, "get_metadata")
}

allow_table_read if {
    input.action.operation in ["SelectFromColumns"]
    catalog := input.action.resource.table.catalogName
    schema := input.action.resource.table.schemaName
    table := input.action.resource.table.tableName
    trino.require_table_access(catalog, schema, table, "read_data")
}
