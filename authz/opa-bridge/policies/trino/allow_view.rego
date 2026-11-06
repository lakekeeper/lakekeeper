package trino

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

allow_view if {
	allow_view_metadata
}

allow_view if {
	allow_view_read
}

allow_view if {
	allow_view_read_in_definer_view
}

# Trino's Iceberg REST catalog cannot create materialized views, so a
# `CreateMaterializedView` that passes here fails in Trino. It is checked like
# `CreateView`.
allow_view_create if {
	input.action.operation in ["CreateView", "CreateMaterializedView"]
	catalog := input.action.resource.table.catalogName
	schema := input.action.resource.table.schemaName
	view := input.action.resource.table.tableName
	is_canonical_tabular_name(view)
	properties := object.get(input.action.resource.table, "properties", {})
	flattened_properties := flatten_properties(properties)
	require_schema_access_create(catalog, schema, "create_view", flattened_properties, view)
	require_view_replace_access(catalog, schema, view)
}

# Trino stores a view comment as a view property and a view column comment in a
# new view version. The bridge cannot see what Trino changes, so it asks about a
# commit of any kind. For a table, `SetColumnComment` is decided by the table rules.
allow_view_modify if {
	input.action.operation in ["SetViewComment", "SetColumnComment"]
	catalog := input.action.resource.table.catalogName
	schema := input.action.resource.table.schemaName
	view := input.action.resource.table.tableName
	require_view_access_simple(catalog, schema, view, "commit")
}

allow_view_rename if {
	input.action.operation == "RenameView"
	source_catalog := input.action.resource.table.catalogName
	source_schema := input.action.resource.table.schemaName
	source_table := input.action.resource.table.tableName
	require_view_access_simple(source_catalog, source_schema, source_table, "rename")
	is_canonical_tabular_name(input.action.targetResource.table.tableName)
	target_catalog := input.action.targetResource.table.catalogName
	target_schema := input.action.targetResource.table.schemaName
	require_schema_access_simple(target_catalog, target_schema, "create_view")
	require_rename_target_access("view", source_catalog, source_schema, source_table, target_catalog, target_schema)
}

allow_view_drop if {
	input.action.operation == "DropView"
	catalog := input.action.resource.table.catalogName
	schema := input.action.resource.table.schemaName
	table := input.action.resource.table.tableName
	require_view_access_drop(catalog, schema, table)
}

allow_view_metadata if {
	input.action.operation in ["FilterTables", "ShowColumns", "FilterColumns"]
	catalog := input.action.resource.table.catalogName
	schema := input.action.resource.table.schemaName
	table := input.action.resource.table.tableName
	require_view_access_simple(catalog, schema, table, "get_metadata")
}

allow_view_read if {
	input.action.operation == "SelectFromColumns"
	catalog := input.action.resource.table.catalogName
	schema := input.action.resource.table.schemaName
	table := input.action.resource.table.tableName
	require_view_access_simple(catalog, schema, table, "select")
}

# When a user queries a DEFINER view of another user, Trino asks
# `CreateViewWithSelectFromColumns` as the view's owner for each table and view
# that the view reads. Trino runs a view without an owner of its own as that
# owner too, so whoever may change such a view could read with the owner's
# rights. Lakekeeper allows the read of a view only if the view has an owner.
allow_view_read_in_definer_view if {
	input.action.operation == "CreateViewWithSelectFromColumns"
	catalog := input.action.resource.table.catalogName
	schema := input.action.resource.table.schemaName
	view := input.action.resource.table.tableName
	require_view_in_definer_view_access(catalog, schema, view)
}
