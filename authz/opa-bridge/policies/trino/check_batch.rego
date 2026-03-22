# Optimized batch filtering for Lakekeeper-managed catalogs.
# Instead of calling `allow` per resource (which makes 1+ HTTP calls each),
# this collects all checks into a single Lakekeeper batch-check HTTP request.
package trino

import data.lakekeeper

# Lakekeeper action for each Trino batch operation
_batch_lakekeeper_actions := {
	"FilterTables": "get_metadata",
	"FilterColumns": "get_metadata",
	"SelectFromColumns": "read_data",
}

# _managed_catalog_names is defined in main.rego

# --- Table/View batch ---

# Collect non-system-schema table resource indices for managed catalogs.
_lakekeeper_batch_table_indices contains i if {
	input.action.operation in ["FilterTables", "FilterColumns", "SelectFromColumns"]
	some i, raw_resource in input.action.filterResources
	raw_resource.table.catalogName in _managed_catalog_names
	not raw_resource.table.schemaName in lakekeeper_system_schemas
	not is_metadata_table(raw_resource.table.tableName)
}

# Build checks, execute batch-check, and return allowed indices per catalog.
# Each resource generates two checks (table + view) since Trino doesn't distinguish.
# Checks are automatically chunked to stay within Lakekeeper's batch-check limit.
# regal ignore:rule-length
_lakekeeper_batch_allowed[catalog_name] := allowed_indices if {
	input.action.operation in ["FilterTables", "FilterColumns", "SelectFromColumns"]
	some catalog_name in _managed_catalog_names
	action := _batch_lakekeeper_actions[input.action.operation]
	trino_catalog := catalog_config_by_name[catalog_name]
	warehouse_id := lakekeeper.warehouse_id_for_name(trino_catalog.lakekeeper_id, trino_catalog.lakekeeper_warehouse)

	# Build an ordered list of (index, resource) from the input array (deterministic order).
	# Sets are unordered in Rego, so we iterate the array directly to guarantee
	# that checks and ordered_indices are aligned.
	catalog_resources := [{"idx": i, "res": raw_resource} |
		some i, raw_resource in input.action.filterResources
		i in _lakekeeper_batch_table_indices
		raw_resource.table.catalogName == catalog_name
	]

	checks := [check |
		some entry in catalog_resources
		namespace := namespace_for_schema(entry.res.table.schemaName)
		some check in [
			lakekeeper.build_table_check(warehouse_id, namespace, entry.res.table.tableName, lakekeeper_user_id, action),
			lakekeeper.build_view_check(warehouse_id, namespace, entry.res.table.tableName, lakekeeper_user_id, action),
		]
	]

	count(checks) > 0

	ordered_indices := [entry.idx |
		some entry in catalog_resources
		some _ in [0, 1] # two checks per resource (table + view)
	]

	results := lakekeeper.batch_check_results(trino_catalog.lakekeeper_id, checks)

	# A resource is allowed if ANY of its checks (table or view) returned true.
	# Each resource has two consecutive checks (table at j, view at j+1).
	allowed_indices := {idx |
		some j, idx in ordered_indices
		j % 2 == 0 # only process even positions (first of each pair)
		some type_offset in [0, 1]
		results[j + type_offset].allowed == true
	}
}

# --- Schema batch ---

_lakekeeper_batch_schema_indices contains i if {
	input.action.operation == "FilterSchemas"
	some i, raw_resource in input.action.filterResources
	raw_resource.schema.catalogName in _managed_catalog_names
	not raw_resource.schema.schemaName in lakekeeper_system_schemas
}

# regal ignore:rule-length
_lakekeeper_batch_schema_allowed[catalog_name] := allowed_indices if {
	input.action.operation == "FilterSchemas"
	some catalog_name in _managed_catalog_names
	trino_catalog := catalog_config_by_name[catalog_name]
	warehouse_id := lakekeeper.warehouse_id_for_name(trino_catalog.lakekeeper_id, trino_catalog.lakekeeper_warehouse)

	catalog_resources := [{"idx": i, "res": raw_resource} |
		some i, raw_resource in input.action.filterResources
		i in _lakekeeper_batch_schema_indices
		raw_resource.schema.catalogName == catalog_name
	]

	checks := [check |
		some entry in catalog_resources
		namespace := namespace_for_schema(entry.res.schema.schemaName)
		check := lakekeeper.build_namespace_check(warehouse_id, namespace, lakekeeper_user_id, "get_metadata")
	]

	count(checks) > 0

	ordered_indices := [entry.idx |
		some entry in catalog_resources
	]

	results := lakekeeper.batch_check_results(trino_catalog.lakekeeper_id, checks)

	allowed_indices := {idx |
		some j, idx in ordered_indices
		results[j].allowed == true
	}
}

# --- Batch rules (grouped together to avoid messy-rule) ---

batch contains i if {
	some catalog_name in _managed_catalog_names
	some i in _lakekeeper_batch_allowed[catalog_name]
}

batch contains i if {
	some catalog_name in _managed_catalog_names
	some i in _lakekeeper_batch_schema_allowed[catalog_name]
}

# FilterColumns with a single table + columns array on a managed catalog:
# Lakekeeper authorizes at table level, so we check the table once via the
# batch path and return all column indices if allowed — no per-column evaluation.
batch contains i if {
	input.action.operation == "FilterColumns"
	count(input.action.filterResources) == 1
	raw_resource := input.action.filterResources[0]
	count(raw_resource.table.columns) > 0
	raw_resource.table.catalogName in _managed_catalog_names
	not raw_resource.table.schemaName in lakekeeper_system_schemas
	not is_metadata_table(raw_resource.table.tableName)
	0 in _lakekeeper_batch_allowed[raw_resource.table.catalogName]
	some i in numbers.range(0, count(raw_resource.table.columns) - 1)
}
