package trino

import data.configuration

# METADATA
# entrypoint: true
default allow := false

default allow_managed := false

default allow_unmanaged := false

# Blanket allow for unmanaged catalogs when enabled via env var or configuration.
# This is useful when Trino has multiple authorizers and this OPA bridge
# should not block access to catalogs managed by other authorizers.
allow_unmanaged if {
	configuration.trino_allow_unmanaged_catalogs == true
	_resource_catalog_name
	not _resource_catalog_name in _managed_catalog_names
}

# Extract catalog name from the current resource (works for all resource types)
_resource_catalog_name := input.action.resource.catalog.name
_resource_catalog_name := input.action.resource.table.catalogName
_resource_catalog_name := input.action.resource.schema.catalogName
_resource_catalog_name := input.action.resource.function.catalogName

# Pre-compute managed catalog names (evaluated once)
_managed_catalog_names contains cat.name if {
	some cat in configuration.trino_catalog
}

# --- allow rules ---

# Default access (system catalog, ExecuteQuery, etc.) - always applies
# regal ignore:messy-rule
allow if {
	allow_default_access
}

# Managed catalog rules (Lakekeeper)
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

# Extension point for managed catalogs.
# Create policies/trino/allow_managed.rego with rules that set allow_managed to true.
allow if {
	allow_managed
}

# Extension point for catalogs not listed in configuration.trino_catalog.
# Create policies/trino/allow_unmanaged.rego with rules that set allow_unmanaged to true.
# When TRINO_ALLOW_UNMANAGED_CATALOGS=true, all access to unmanaged catalogs is permitted.
allow if {
	allow_unmanaged
}

# --- batch rules ---

# Operations with dedicated batch handling via check_batch.rego
_batch_operations := {"FilterTables", "FilterColumns", "SelectFromColumns", "FilterSchemas"}

# Extract catalog name from a batch resource
_batch_resource_catalog(raw_resource) := raw_resource.table.catalogName
_batch_resource_catalog(raw_resource) := raw_resource.schema.catalogName
_batch_resource_catalog(raw_resource) := raw_resource.catalog.name

# Extract schema name from a batch resource
_batch_resource_schema(raw_resource) := raw_resource.table.schemaName
_batch_resource_schema(raw_resource) := raw_resource.schema.schemaName

# Fast path for unmanaged catalogs: only default_access + allow_unmanaged
_allow_unmanaged if allow_default_access

_allow_unmanaged if allow_unmanaged

# Unmanaged catalogs in batch operations: fast path (no Lakekeeper evaluation)
batch contains i if {
	input.action.operation in _batch_operations
	some i, raw_resource in input.action.filterResources
	not _batch_resource_catalog(raw_resource) in _managed_catalog_names

	# regal ignore:with-outside-test-context
	_allow_unmanaged with input.action.resource as raw_resource
}

# System schema resources in managed catalogs still need per-resource evaluation
# (information_schema, schema_discovery, system are excluded from Lakekeeper batch)
batch contains i if {
	input.action.operation in _batch_operations
	some i, raw_resource in input.action.filterResources
	_batch_resource_catalog(raw_resource) in _managed_catalog_names
	_batch_resource_schema(raw_resource) in lakekeeper_system_schemas

	# regal ignore:with-outside-test-context
	allow with input.action.resource as raw_resource
}

# Metadata tables in managed catalogs need per-resource evaluation
# (excluded from Lakekeeper batch since they resolve to base table permissions)
batch contains i if {
	input.action.operation in {"FilterTables", "FilterColumns", "SelectFromColumns"}
	some i, raw_resource in input.action.filterResources
	_batch_resource_catalog(raw_resource) in _managed_catalog_names
	not _batch_resource_schema(raw_resource) in lakekeeper_system_schemas
	is_metadata_table(raw_resource.table.tableName)

	# regal ignore:with-outside-test-context
	allow with input.action.resource as raw_resource
}

# Non-batch operations: per-resource allow evaluation as before
batch contains i if {
	not input.action.operation in _batch_operations
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
