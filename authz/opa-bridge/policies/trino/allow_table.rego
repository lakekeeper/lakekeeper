package trino

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
	input.action.operation == "CreateTable"
	table := input.action.resource.table.tableName
	is_metadata_table(table) == false
	catalog := input.action.resource.table.catalogName
	schema := input.action.resource.table.schemaName
	properties := object.get(input.action.resource.table, "properties", {})
	flattened_properties := flatten_properties(properties)
	require_schema_access_create(catalog, schema, "create_table", flattened_properties, table)
}

allow_table_drop if {
	input.action.operation == "DropTable"
	table := input.action.resource.table.tableName
	is_metadata_table(table) == false
	catalog := input.action.resource.table.catalogName
	schema := input.action.resource.table.schemaName
	require_table_access_simple(catalog, schema, table, "drop")
}

allow_table_rename if {
	input.action.operation == "RenameTable"
	source_table := input.action.resource.table.tableName
	is_metadata_table(source_table) == false
	source_catalog := input.action.resource.table.catalogName
	source_schema := input.action.resource.table.schemaName
	require_table_access_simple(source_catalog, source_schema, source_table, "rename")
	target_catalog := input.action.targetResource.table.catalogName
	target_schema := input.action.targetResource.table.schemaName
	require_schema_access_simple(target_catalog, target_schema, "create_table")
	require_rename_target_access("table", source_catalog, source_schema, source_table, target_catalog, target_schema)
}

allow_table_modify if {
	input.action.operation in [
		"InsertIntoTable", "DeleteFromTable", "TruncateTable",
		"UpdateTableColumns",
	]
	table := input.action.resource.table.tableName
	is_metadata_table(table) == false
	catalog := input.action.resource.table.catalogName
	schema := input.action.resource.table.schemaName
	require_table_access_simple(catalog, schema, table, "write_data")
}

# Column changes and column comments add a schema; a table comment is a property.
allow_table_modify if {
	input.action.operation in ["SetColumnComment", "AddColumn", "AlterColumn", "DropColumn", "RenameColumn"]
	table := input.action.resource.table.tableName
	is_metadata_table(table) == false
	catalog := input.action.resource.table.catalogName
	schema := input.action.resource.table.schemaName
	require_table_access_commit(catalog, schema, table, {}, [], ["add-schema", "set-current-schema"])
}

allow_table_modify if {
	input.action.operation == "SetTableComment"
	table := input.action.resource.table.tableName
	is_metadata_table(table) == false
	catalog := input.action.resource.table.catalogName
	schema := input.action.resource.table.schemaName
	require_table_access_commit(catalog, schema, table, {}, [], ["set-properties"])
}

allow_table_modify if {
	input.action.operation == "SetTableProperties"
	table := input.action.resource.table.tableName
	is_metadata_table(table) == false
	catalog := input.action.resource.table.catalogName
	schema := input.action.resource.table.schemaName
	properties := object.get(input.action.resource.table, "properties", {})
	flattened_properties := flatten_properties(properties)
	require_table_access_commit(catalog, schema, table, flattened_properties, [], ["set-properties"])
}

allow_table_metadata if {
	input.action.operation in ["FilterTables", "ShowColumns", "FilterColumns", "ShowCreateTable"]
	table := input.action.resource.table.tableName
	is_metadata_table(table) == false
	catalog := input.action.resource.table.catalogName
	schema := input.action.resource.table.schemaName
	require_table_access_simple(catalog, schema, table, "get_metadata")
}

allow_table_read if {
	input.action.operation in ["SelectFromColumns", "CreateViewWithSelectFromColumns"]
	table := input.action.resource.table.tableName
	is_metadata_table(table) == false
	catalog := input.action.resource.table.catalogName
	schema := input.action.resource.table.schemaName
	require_table_access_simple(catalog, schema, table, "read_data")
}

allow_table_metadata_read if {
	input.action.operation == "SelectFromColumns"
	catalog := input.action.resource.table.catalogName
	schema := input.action.resource.table.schemaName
	table := split_metadata_table_name(input.action.resource.table.tableName)
	require_table_access_simple(catalog, schema, table, "get_metadata")
}

allow_table_procedure if {
	input.action.operation == "ExecuteTableProcedure"
	input.action.resource.function.functionName in [
		"OPTIMIZE",
		"OPTIMIZE_MANIFESTS",
		"DROP_EXTENDED_STATS",
	]
	table := input.action.resource.table.tableName
	is_metadata_table(table) == false
	catalog := input.action.resource.table.catalogName
	schema := input.action.resource.table.schemaName
	require_table_access_simple(catalog, schema, table, "write_data")
}

# Adding files also sets the table's name mapping if it has none.
allow_table_procedure if {
	input.action.operation == "ExecuteTableProcedure"
	input.action.resource.function.functionName == "ADD_FILES"
	table := input.action.resource.table.tableName
	is_metadata_table(table) == false
	catalog := input.action.resource.table.catalogName
	schema := input.action.resource.table.schemaName
	require_table_access_commit(catalog, schema, table, {}, [], ["add-snapshot", "set-snapshot-ref", "set-properties"])
}

# Expiring snapshots and removing orphan files delete history and files for good.
allow_table_procedure if {
	input.action.operation == "ExecuteTableProcedure"
	input.action.resource.function.functionName in ["EXPIRE_SNAPSHOTS", "REMOVE_ORPHAN_FILES"]
	table := input.action.resource.table.tableName
	is_metadata_table(table) == false
	catalog := input.action.resource.table.catalogName
	schema := input.action.resource.table.schemaName
	require_table_access_commit(catalog, schema, table, {}, [], ["remove-snapshots"])
}
