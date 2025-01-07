package trino

import future.keywords.in
import future.keywords.if

allow_default_access if {
    allow_execute_query
}

allow_default_access if {
    allow_access_catalog_on_system_catalog
}

allow_default_access if {
    allow_sfc_on_system_catalog
}

allow_default_access if {
    allow_system_catalog_jdbc_schema_tables_table
}

allow_default_access if {
    allow_sfc_on_table_schemas_in_system_catalog
}

allow_default_access if {
    allow_sfc_on_table_columns_in_system_catalog
}

allow_default_access if {
    allow_filter_catalogs_for_system_catalog
}

# Every authenticated user can execute queries.
# Authentication is checked by trino.
allow_execute_query if {
	input.action.operation == "ExecuteQuery"
}

# Will run if you'll try to use the dbeaver's GUI
allow_access_catalog_on_system_catalog if {
	input.action.operation == "AccessCatalog"
	input.action.resource.catalog.name == "system"
}
# Will run if you'll try to use the dbeaver's GUI
allow_sfc_on_system_catalog if {
	input.action.operation == "SelectFromColumns"
	input.action.resource.table.catalogName == "system"
    input.action.resource.table.schemaName == "jdbc"
	input.action.resource.table.tableName in ["catalogs", "types"]
}

allow_system_catalog_jdbc_schema_tables_table if {
    input.action.operation == "SelectFromColumns"
    input.action.resource.table.catalogName == "system"
    input.action.resource.table.schemaName == "jdbc"
    input.action.resource.table.tableName == "tables"
}

# Will run if you'll try to use the dbeaver's GUI
allow_sfc_on_table_schemas_in_system_catalog if {
	input.action.operation == "SelectFromColumns"
	input.action.resource.table.catalogName == "system"
	input.action.resource.table.schemaName == "jdbc"
	input.action.resource.table.tableName == "schemas"
}

allow_sfc_on_table_columns_in_system_catalog if {
    input.action.operation == "SelectFromColumns"
    input.action.resource.table.catalogName == "system"
    input.action.resource.table.schemaName == "jdbc"
    input.action.resource.table.tableName == "columns"
}

allow_filter_catalogs_for_system_catalog if {
    input.action.operation == "FilterCatalogs"
    input.action.resource.catalog.name == "system"
}