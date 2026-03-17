package trino

import data.configuration

# METADATA
# entrypoint: true
default allow := false

# Allow if catalog is not present
# in configuration.trino_catalog array (name field).
# These are catalogs not managed by Lakekeeper.
allow if {
	configuration.trino_allow_unmanaged_catalogs == true
	catalog_name := input.action.resource.catalog.name
	managed_catalogs := {cat.name | some cat in configuration.trino_catalog}
	not catalog_name in managed_catalogs
}

allow if {
	configuration.trino_allow_unmanaged_catalogs == true
	catalog_name := input.action.resource.table.catalogName
	managed_catalogs := {cat.name | some cat in configuration.trino_catalog}
	not catalog_name in managed_catalogs
}

allow if {
	configuration.trino_allow_unmanaged_catalogs == true
	catalog_name := input.action.resource.schema.catalogName
	managed_catalogs := {cat.name | some cat in configuration.trino_catalog}
	not catalog_name in managed_catalogs
}

allow if {
	allow_default_access
}

allow if {
	allow_catalog
}

allow if {
	allow_schema
}

allow if {
	allow_table
}

allow if {
	allow_view
}

allow if {
	allow_custom
}

default allow_custom := false

batch contains i if {
	some i, raw_resource in input.action.filterResources

	# regal ignore:with-outside-test-context
	allow with input.action.resource as raw_resource
}

# Corner case: filtering columns is done with a single table item, and many columns inside
# We cannot use our normal logic in other parts of the policy as they are based on sets
# and we need to retain order
batch contains i if {
	input.action.operation == "FilterColumns"
	count(input.action.filterResources) == 1
	raw_resource := input.action.filterResources[0]
	count(raw_resource.table.columns) > 0
	new_resources := [
	object.union(raw_resource, {"table": object.union(raw_resource.table, {"column": column_name})}) |
		some column_name in raw_resource.table.columns
	]
	some i, resource in new_resources

	# regal ignore:with-outside-test-context
	allow with input.action.resource as resource
}
