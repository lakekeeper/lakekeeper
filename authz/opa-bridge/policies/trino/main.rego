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

# Unique unmanaged catalog names in this batch request.
_batch_unmanaged_catalogs contains name if {
	some raw_resource in input.action.filterResources
	name := _batch_resource_catalog(raw_resource)
	not name in _managed_catalog_names
}

# Catalogs blanket-allowed via allow_unmanaged, evaluated once per unique
# catalog (not per resource). allow_unmanaged's decision is catalog-level
# (depends on configuration.trino_allow_unmanaged_catalogs + catalog name),
# so one evaluation per catalog suffices — avoids re-running the rule for
# every filterResource in a large batch.
_allowed_unmanaged_batch_catalogs contains catalog_name if {
	some catalog_name in _batch_unmanaged_catalogs
	representative := [r |
		some r in input.action.filterResources
		_batch_resource_catalog(r) == catalog_name
	][0]

	# regal ignore:with-outside-test-context
	allow_unmanaged with input.action.resource as representative
}

# Unmanaged catalogs allowed by allow_unmanaged: one membership check per resource.
batch contains i if {
	input.action.operation in _batch_operations
	some i, raw_resource in input.action.filterResources
	_batch_resource_catalog(raw_resource) in _allowed_unmanaged_batch_catalogs
}

# Unmanaged catalogs not blanket-allowed: fall back to per-resource
# allow_default_access (depends on operation/schema specifics like
# ExecuteQuery or information_schema, so can't be collapsed per-catalog).
batch contains i if {
	input.action.operation in _batch_operations
	some i, raw_resource in input.action.filterResources
	not _batch_resource_catalog(raw_resource) in _managed_catalog_names
	not _batch_resource_catalog(raw_resource) in _allowed_unmanaged_batch_catalogs

	# regal ignore:with-outside-test-context
	allow_default_access with input.action.resource as raw_resource
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

	# Skip when check_batch.rego already handles this case (managed catalog,
	# non-system schema, non-metadata table). That rule short-circuits via
	# warehouse/namespace broad access or a single per-table batch-check —
	# running per-column `allow` here would duplicate Lakekeeper calls.
	not _single_filter_columns_handled_by_batch
	new_resources := [
	object.union(raw_resource, {"table": object.union(raw_resource.table, {"column": column_name})}) |
		some column_name in raw_resource.table.columns
	]
	some i, resource in new_resources

	# regal ignore:with-outside-test-context
	allow with input.action.resource as resource
}

# True when single-resource FilterColumns is fully handled by check_batch.rego,
# i.e. catalog is managed, schema is not a Lakekeeper system schema, and the
# table is not an iceberg metadata-table ($history, $snapshots, ...).
_single_filter_columns_handled_by_batch if {
	raw_resource := input.action.filterResources[0]
	raw_resource.table.catalogName in _managed_catalog_names
	not raw_resource.table.schemaName in lakekeeper_system_schemas
	not is_metadata_table(raw_resource.table.tableName)
}
