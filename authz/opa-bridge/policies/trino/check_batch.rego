# Optimized batch filtering for Lakekeeper-managed catalogs.
# Instead of calling `allow` per resource (which makes 1+ HTTP calls each),
# this collects all checks into a single Lakekeeper batch-check HTTP request.
#
# Fast path: when the principal has `list_everything` on the warehouse or on
# individual namespaces, the per-table batch-check is skipped entirely for the
# covered resources. The coarse check is cached per-query (via the queryId
# salt on the cached batch-check helper), so queries that fan out across many
# tables (information_schema, SHOW TABLES, etc.) only pay the cost once per
# query.
package trino

import data.lakekeeper

# Lakekeeper action for each Trino batch operation
_batch_lakekeeper_actions := {
	"FilterTables": "get_metadata",
	"FilterColumns": "get_metadata",
	"SelectFromColumns": "read_data",
}

# Cache TTL for broad-access (list_everything) checks. Short enough to respect
# permission revocations quickly; the cache is also keyed per queryId so the
# practical staleness window is bounded by the duration of one query.
_broad_access_cache_secs := 30

# Request ID forwarded to Lakekeeper as X-Request-ID on broad-access probes.
# Uses the Trino queryId so each new query re-probes once (all waves share
# the result), and Lakekeeper's audit/trace logs correlate to the query.
# Falls back to empty string if queryId is absent — broad-access probes
# then share a cache key across queries (acceptable fallback, not expected
# from Trino).
_broad_access_request_id := object.get(input.context, "queryId", "")

# --- Fast path: warehouse-level `list_everything` ---
#
# `can_list_everything` on a warehouse is defined as `describe` in the FGA
# model, which propagates `describe from parent` to every namespace/table and
# implies `can_get_metadata` on each. So it's semantically safe to short-circuit
# FilterTables/FilterColumns/FilterSchemas. SelectFromColumns needs `read_data`
# (= `select`, NOT implied by `describe`) and must stay on the slow path.
_warehouse_broad contains catalog_name if {
	some catalog_name in _managed_catalog_names
	trino_catalog := catalog_config_by_name[catalog_name]
	warehouse_id := lakekeeper.warehouse_id_for_name(trino_catalog.lakekeeper_id, trino_catalog.lakekeeper_warehouse)
	check := lakekeeper.build_warehouse_check(warehouse_id, lakekeeper_user_id, "list_everything")
	results := lakekeeper.batch_check_results_cached(
		trino_catalog.lakekeeper_id,
		[check],
		_broad_access_request_id,
		_broad_access_cache_secs,
	)
	results[0].allowed == true
}

# --- Fast path: namespace-level `list_everything` ---
#
# Only evaluated when the warehouse-level path doesn't apply. One cached
# batch-check per catalog containing all distinct namespaces in the batch.
# regal ignore:rule-length
_namespace_broad[catalog_name] := allowed_names if {
	some catalog_name in _managed_catalog_names
	not catalog_name in _warehouse_broad
	trino_catalog := catalog_config_by_name[catalog_name]
	warehouse_id := lakekeeper.warehouse_id_for_name(trino_catalog.lakekeeper_id, trino_catalog.lakekeeper_warehouse)
	schema_set := _distinct_schemas_for(catalog_name)
	count(schema_set) > 0

	# Sort the set into a deterministic list so `checks[i]` aligns with
	# `schema_names[i]` when mapping results back to allowed names.
	schema_names := sort(schema_set)
	checks := [
	lakekeeper.build_namespace_check(warehouse_id, namespace_for_schema(name), lakekeeper_user_id, "list_everything") |
		some name in schema_names
	]
	results := lakekeeper.batch_check_results_cached(
		trino_catalog.lakekeeper_id,
		checks,
		_broad_access_request_id,
		_broad_access_cache_secs,
	)
	allowed_names := {name |
		some i, name in schema_names
		results[i].allowed == true
	}
}

# Helper: distinct schema names appearing in the batch for one catalog.
# Set comprehension dedups — a single FilterTables batch can contain thousands
# of rows sharing the same schema, and we want one namespace check per schema.
_distinct_schemas_for(catalog_name) := names if {
	input.action.operation in ["FilterTables", "FilterColumns", "SelectFromColumns"]
	names := {r.table.schemaName |
		some r in input.action.filterResources
		r.table.catalogName == catalog_name
		not r.table.schemaName in lakekeeper_system_schemas
	}
} else := names if {
	input.action.operation == "FilterSchemas"
	names := {r.schema.schemaName |
		some r in input.action.filterResources
		r.schema.catalogName == catalog_name
		not r.schema.schemaName in lakekeeper_system_schemas
	}
} else := set()

# Helper: does the fast path cover this table resource? Used to exclude
# such resources from the per-table slow path. Applies only to the describe-
# level operations — SelectFromColumns intentionally never hits this.
_fast_path_covers_table(raw_resource) if {
	input.action.operation in ["FilterTables", "FilterColumns"]
	raw_resource.table.catalogName in _warehouse_broad
}

_fast_path_covers_table(raw_resource) if {
	input.action.operation in ["FilterTables", "FilterColumns"]
	catalog := raw_resource.table.catalogName
	not catalog in _warehouse_broad
	raw_resource.table.schemaName in object.get(_namespace_broad, catalog, set())
}

# Helper: does the fast path cover this schema resource?
_fast_path_covers_schema(raw_resource) if {
	input.action.operation == "FilterSchemas"
	raw_resource.schema.catalogName in _warehouse_broad
}

_fast_path_covers_schema(raw_resource) if {
	input.action.operation == "FilterSchemas"
	catalog := raw_resource.schema.catalogName
	not catalog in _warehouse_broad
	raw_resource.schema.schemaName in object.get(_namespace_broad, catalog, set())
}

# _managed_catalog_names is defined in main.rego

# --- Table/View batch (slow path) ---

# Collect non-system-schema table resource indices for managed catalogs,
# excluding anything already covered by the fast path.
_lakekeeper_batch_table_indices contains i if {
	input.action.operation in ["FilterTables", "FilterColumns", "SelectFromColumns"]
	some i, raw_resource in input.action.filterResources
	raw_resource.table.catalogName in _managed_catalog_names
	not raw_resource.table.schemaName in lakekeeper_system_schemas
	not is_metadata_table(raw_resource.table.tableName)
	not _fast_path_covers_table(raw_resource)
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

# --- Schema batch (slow path) ---

_lakekeeper_batch_schema_indices contains i if {
	input.action.operation == "FilterSchemas"
	some i, raw_resource in input.action.filterResources
	raw_resource.schema.catalogName in _managed_catalog_names
	not raw_resource.schema.schemaName in lakekeeper_system_schemas
	not _fast_path_covers_schema(raw_resource)
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

# --- Batch rules ---

# Slow-path results.
batch contains i if {
	some catalog_name in _managed_catalog_names
	some i in _lakekeeper_batch_allowed[catalog_name]
}

batch contains i if {
	some catalog_name in _managed_catalog_names
	some i in _lakekeeper_batch_schema_allowed[catalog_name]
}

# Fast-path: warehouse-level broad access → allow all qualifying table indices.
batch contains i if {
	input.action.operation in ["FilterTables", "FilterColumns"]
	some i, raw_resource in input.action.filterResources
	raw_resource.table.catalogName in _warehouse_broad
	not raw_resource.table.schemaName in lakekeeper_system_schemas
	not is_metadata_table(raw_resource.table.tableName)
}

# Fast-path: namespace-level broad access → allow table indices in those namespaces.
batch contains i if {
	input.action.operation in ["FilterTables", "FilterColumns"]
	some i, raw_resource in input.action.filterResources
	catalog := raw_resource.table.catalogName
	not catalog in _warehouse_broad
	not raw_resource.table.schemaName in lakekeeper_system_schemas
	not is_metadata_table(raw_resource.table.tableName)
	raw_resource.table.schemaName in object.get(_namespace_broad, catalog, set())
}

# Fast-path for FilterSchemas: warehouse broad → all schema indices.
batch contains i if {
	input.action.operation == "FilterSchemas"
	some i, raw_resource in input.action.filterResources
	raw_resource.schema.catalogName in _warehouse_broad
	not raw_resource.schema.schemaName in lakekeeper_system_schemas
}

# Fast-path for FilterSchemas: namespace broad → matching schema indices.
batch contains i if {
	input.action.operation == "FilterSchemas"
	some i, raw_resource in input.action.filterResources
	catalog := raw_resource.schema.catalogName
	not catalog in _warehouse_broad
	not raw_resource.schema.schemaName in lakekeeper_system_schemas
	raw_resource.schema.schemaName in object.get(_namespace_broad, catalog, set())
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
	_single_table_filter_columns_allowed(raw_resource)
	some i in numbers.range(0, count(raw_resource.table.columns) - 1)
}

# Single-resource FilterColumns is allowed if any of the paths say so.
_single_table_filter_columns_allowed(raw_resource) if {
	raw_resource.table.catalogName in _warehouse_broad
}

_single_table_filter_columns_allowed(raw_resource) if {
	catalog := raw_resource.table.catalogName
	raw_resource.table.schemaName in object.get(_namespace_broad, catalog, set())
}

_single_table_filter_columns_allowed(raw_resource) if {
	0 in _lakekeeper_batch_allowed[raw_resource.table.catalogName]
}
