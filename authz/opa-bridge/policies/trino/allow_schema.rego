package trino

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
	input.action.operation == "CreateSchema"
	schema := input.action.resource.schema.schemaName
	is_nested_schema(schema) == false
	catalog := input.action.resource.schema.catalogName
	properties := object.get(input.action.resource.schema, "properties", {})
	flattended_properties := flatten_properties(properties)
	require_catalog_create_namespace_access(catalog, flattended_properties, schema)
}

allow_schema_create if {
	input.action.operation == "CreateSchema"
	schema := input.action.resource.schema.schemaName
	is_nested_schema(schema) == true
	catalog := input.action.resource.schema.catalogName
	properties := object.get(input.action.resource.schema, "properties", {})
	flattended_properties := flatten_properties(properties)
	require_schema_access_create(
		catalog,
		parent_schema(schema),
		"create_namespace",
		flattended_properties,
		child_schema_name(schema),
	)
}

allow_schema_drop if {
	input.action.operation == "DropSchema"
	catalog := input.action.resource.schema.catalogName
	schema := input.action.resource.schema.schemaName
	require_schema_access_simple(catalog, schema, "delete")
}

# renameNamespace is not supported for Iceberg REST catalog in trino.
# Lakekeeper supports renaming schemas, please use the UI or management API
# to rename schemas. (namespaces)
default allow_schema_rename := false

allow_show_schemas if {
	input.action.operation == "ShowSchemas"
	catalog := input.action.resource.catalog.name
	require_catalog_access_simple(catalog, "list_namespaces")
}

allow_filter_system_schemas if {
	input.action.operation == "FilterSchemas"
	schema := input.action.resource.schema.schemaName
	schema in lakekeeper_system_schemas
	catalog := input.action.resource.schema.catalogName
	require_catalog_access_simple(catalog, "get_config")
}

# Table-level access for Lakekeeper system schemas.
# Each schema has its own allowed table list defined in allow_default_access.rego.
allow_tables_in_system_schemas if {
	input.action.operation in ["SelectFromColumns", "FilterTables", "FilterColumns"]
	input.action.resource.table.schemaName == "information_schema"
	input.action.resource.table.tableName in allowed_information_schema_tables
	catalog := input.action.resource.table.catalogName
	require_catalog_access_simple(catalog, "get_config")
}

allow_tables_in_system_schemas if {
	input.action.operation in ["SelectFromColumns", "FilterTables", "FilterColumns"]
	input.action.resource.table.schemaName == "schema_discovery"
	input.action.resource.table.tableName in allowed_schema_discovery_tables
	catalog := input.action.resource.table.catalogName
	require_catalog_access_simple(catalog, "get_config")
}

allow_tables_in_system_schemas if {
	input.action.operation in ["SelectFromColumns", "FilterTables", "FilterColumns"]
	input.action.resource.table.schemaName == "system"
	input.action.resource.table.tableName in allowed_system_schema_tables
	catalog := input.action.resource.table.catalogName
	require_catalog_access_simple(catalog, "get_config")
}

allow_filter_schemas if {
	input.action.operation == "FilterSchemas"
	schema := input.action.resource.schema.schemaName
	not schema in lakekeeper_system_schemas
	catalog := input.action.resource.schema.catalogName
	require_schema_access_simple(catalog, schema, "get_metadata")
}

allow_show_create_schemas if {
	input.action.operation == "ShowCreateSchema"
	schema := input.action.resource.schema.schemaName
	not schema in lakekeeper_system_schemas
	catalog := input.action.resource.schema.catalogName
	require_schema_access_simple(catalog, schema, "get_metadata")
}

allow_show_tables_in_schema if {
	input.action.operation == "ShowTables"
	catalog := input.action.resource.schema.catalogName
	schema := input.action.resource.schema.schemaName
	require_schema_access_simple(catalog, schema, "get_metadata")
}

# ------------- Admin Access -------------
# Admins get full access to all tables in Lakekeeper system schemas
# (no table filtering on information_schema, schema_discovery, system)
allow_admin_system_schemas if {
	is_admin
	input.action.operation == "FilterSchemas"
	input.action.resource.schema.schemaName in lakekeeper_system_schemas
}

allow_admin_system_schemas if {
	is_admin
	input.action.operation in ["SelectFromColumns", "FilterTables", "FilterColumns"]
	input.action.resource.table.schemaName in lakekeeper_system_schemas
}
