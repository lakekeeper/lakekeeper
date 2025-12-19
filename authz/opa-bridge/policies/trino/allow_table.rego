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
    allow_table_metadata
}

allow_table if {
    allow_table_read
}

allow_table if {
    allow_table_procedure
}

allow_table if {
    allow_table_metadata_read
}

allow_table_create if {
    input.action.operation in ["CreateTable"]
    catalog := input.action.resource.table.catalogName
    schema := input.action.resource.table.schemaName
    table := input.action.resource.table.tableName
    properties := object.get(input.action.resource.table, "properties", {})
    flattened_properties := flatten_properties(properties)
    trino.is_metadata_table(table) == false
    trino.require_schema_access_create(catalog, schema, "create_table", flattened_properties)
}

allow_table_drop if {
    input.action.operation in ["DropTable"]
    catalog := input.action.resource.table.catalogName
    schema := input.action.resource.table.schemaName
    table := input.action.resource.table.tableName
    trino.is_metadata_table(table) == false
    trino.require_table_access_simple(catalog, schema, table, "drop")
}

allow_table_rename if {
    input.action.operation in ["RenameTable"]
    source_catalog := input.action.resource.table.catalogName
    source_schema := input.action.resource.table.schemaName
    source_table := input.action.resource.table.tableName
    target_catalog := input.action.targetResource.table.catalogName
    target_schema := input.action.targetResource.table.schemaName
    trino.is_metadata_table(source_table) == false
    trino.require_table_access_simple(source_catalog, source_schema, source_table, "rename")
    trino.require_schema_access_simple(target_catalog, target_schema, "create_table")
}

allow_table_modify if {
    input.action.operation in [
        "SetTableComment", "SetColumnComment", 
        "AddColumn", "AlterColumn", "DropColumn", "RenameColumn", 
        "InsertIntoTable", "DeleteFromTable", "TruncateTable", 
        "UpdateTableColumns"]
    catalog := input.action.resource.table.catalogName
    schema := input.action.resource.table.schemaName
    table := input.action.resource.table.tableName
    trino.is_metadata_table(table) == false
    trino.require_table_access_simple(catalog, schema, table, "write_data")
}

allow_table_modify if {
    input.action.operation in ["SetTableProperties"]
    catalog := input.action.resource.table.catalogName
    schema := input.action.resource.table.schemaName
    table := input.action.resource.table.tableName
    properties := object.get(input.action.resource.table, "properties", {})
    flattened_properties := flatten_properties(properties)
    trino.is_metadata_table(table) == false
    trino.require_table_access_commit(catalog, schema, table, flattened_properties, [])
}

allow_table_metadata if {
    input.action.operation in ["FilterTables", "ShowColumns", "FilterColumns"]
    catalog := input.action.resource.table.catalogName
    schema := input.action.resource.table.schemaName
    table := input.action.resource.table.tableName
    trino.is_metadata_table(table) == false
    trino.require_table_access_simple(catalog, schema, table, "get_metadata")
}

allow_table_read if {
    input.action.operation in ["SelectFromColumns"]
    catalog := input.action.resource.table.catalogName
    schema := input.action.resource.table.schemaName
    table := input.action.resource.table.tableName
    trino.is_metadata_table(table) == false
    trino.require_table_access_simple(catalog, schema, table, "read_data")
}

allow_table_metadata_read if {
    input.action.operation in ["SelectFromColumns"]
    catalog := input.action.resource.table.catalogName
    schema := input.action.resource.table.schemaName
    table := trino.split_metadata_table_name(input.action.resource.table.tableName)
    trino.require_table_access_simple(catalog, schema, table, "get_metadata")
}

allow_table_procedure if {
    input.action.operation in ["ExecuteTableProcedure"]
    input.action.resource.function.functionName in ["OPTIMIZE", "OPTIMIZE_MANIFESTS", "EXPIRE_SNAPSHOTS", "DROP_EXTENDED_STATS", "REMOVE_ORPHAN_FILES"]
    catalog := input.action.resource.table.catalogName
    schema := input.action.resource.table.schemaName
    table := input.action.resource.table.tableName
    trino.is_metadata_table(table) == false
    trino.require_table_access_simple(catalog, schema, table, "write_data")
}
