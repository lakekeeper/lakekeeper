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
	is_canonical_tabular_name(table)
	catalog := input.action.resource.table.catalogName
	schema := input.action.resource.table.schemaName
	properties := object.get(input.action.resource.table, "properties", {})
	flattened_properties := flatten_properties(properties)
	require_schema_access_create(catalog, schema, "create_table", flattened_properties, table)
	require_table_replace_access(catalog, schema, table, flattened_properties)
}

allow_table_drop if {
	input.action.operation == "DropTable"
	table := input.action.resource.table.tableName
	catalog := input.action.resource.table.catalogName
	schema := input.action.resource.table.schemaName
	require_table_access_drop(catalog, schema, table)
}

allow_table_rename if {
	input.action.operation == "RenameTable"
	source_table := input.action.resource.table.tableName
	source_catalog := input.action.resource.table.catalogName
	source_schema := input.action.resource.table.schemaName
	require_table_access_simple(source_catalog, source_schema, source_table, "rename")
	is_canonical_tabular_name(input.action.targetResource.table.tableName)
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
	catalog := input.action.resource.table.catalogName
	schema := input.action.resource.table.schemaName
	require_table_access_simple(catalog, schema, table, "write_data")
}

# Column changes add a schema, and comments and column changes can also change
# properties, such as the name mapping. The bridge cannot see which updates Trino
# will send, so it asks about a commit of any kind.
allow_table_modify if {
	input.action.operation in [
		"SetColumnComment", "AddColumn", "AlterColumn", "DropColumn", "RenameColumn",
		"SetTableComment",
	]
	table := input.action.resource.table.tableName
	catalog := input.action.resource.table.catalogName
	schema := input.action.resource.table.schemaName
	require_table_access_commit(catalog, schema, table, {}, [])
}

allow_table_modify if {
	input.action.operation == "SetTableProperties"
	table := input.action.resource.table.tableName
	catalog := input.action.resource.table.catalogName
	schema := input.action.resource.table.schemaName
	properties := object.get(input.action.resource.table, "properties", {})
	flattened_properties := flatten_properties(properties)
	require_table_access_commit(catalog, schema, table, flattened_properties, [])
}

allow_table_metadata if {
	input.action.operation in ["FilterTables", "ShowColumns", "FilterColumns", "ShowCreateTable"]
	table := input.action.resource.table.tableName
	catalog := input.action.resource.table.catalogName
	schema := input.action.resource.table.schemaName
	require_table_access_simple(catalog, schema, table, "get_metadata")
}

allow_table_read if {
	input.action.operation in ["SelectFromColumns", "CreateViewWithSelectFromColumns"]
	table := input.action.resource.table.tableName
	catalog := input.action.resource.table.catalogName
	schema := input.action.resource.table.schemaName
	require_table_access_simple(catalog, schema, table, "read_data")
}

# Metadata tables that list data files, manifests, partitions or snapshot
# summaries show values of the data, such as column bounds, partition values and
# custom snapshot properties, so reading them needs `read_data` on the base table.
# The others show history, refs and properties and need `get_metadata`. A type
# without an entry here cannot be read.
metadata_table_read_action := {
	"files": "read_data",
	"entries": "read_data",
	"all_entries": "read_data",
	"manifests": "read_data",
	"all_manifests": "read_data",
	"partitions": "read_data",
	"snapshots": "read_data",
	"history": "get_metadata",
	"refs": "get_metadata",
	"properties": "get_metadata",
	"metadata_log_entries": "get_metadata",
}

allow_table_metadata_read if {
	input.action.operation == "SelectFromColumns"
	catalog := input.action.resource.table.catalogName
	schema := input.action.resource.table.schemaName
	name := input.action.resource.table.tableName
	table := split_metadata_table_name(name)
	action := metadata_table_read_action[metadata_table_type(name)]
	require_table_access_simple(catalog, schema, table, action)
	require_metadata_table_view_access(catalog, schema, name)
}

allow_table_procedure if {
	input.action.operation == "ExecuteTableProcedure"
	input.action.resource.function.functionName in [
		"OPTIMIZE",
		"OPTIMIZE_MANIFESTS",
		"DROP_EXTENDED_STATS",
	]
	table := input.action.resource.table.tableName
	catalog := input.action.resource.table.catalogName
	schema := input.action.resource.table.schemaName
	require_table_access_simple(catalog, schema, table, "write_data")
}

# Adding files also sets the table's name mapping if it has none. Expiring snapshots
# removes history, and removing orphan files deletes files without a commit. All of
# them ask about a commit of any kind.
allow_table_procedure if {
	input.action.operation == "ExecuteTableProcedure"
	input.action.resource.function.functionName in ["ADD_FILES", "EXPIRE_SNAPSHOTS", "REMOVE_ORPHAN_FILES"]
	table := input.action.resource.table.tableName
	catalog := input.action.resource.table.catalogName
	schema := input.action.resource.table.schemaName
	require_table_access_commit(catalog, schema, table, {}, [])
}
