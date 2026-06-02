# Tests for the broad-access fast path in check_batch.rego.
# Verifies that `list_everything` on warehouse or namespace short-circuits the
# per-resource batch-check and that the slow path is correctly bypassed.
package trino_test

import data.trino

# ===================================================================
# Warehouse-level list_everything: all qualifying resources allowed
# without per-resource Lakekeeper calls.
# ===================================================================

test_fast_path_warehouse_broad_allows_all_tables if {
	result := trino.batch with input as {
		"context": mock_context,
		"action": {
			"operation": "FilterTables",
			"filterResources": [
				{"table": {"catalogName": "managed", "schemaName": "schema_a", "tableName": "table_a"}},
				{"table": {"catalogName": "managed", "schemaName": "schema_b", "tableName": "table_b"}},
				{"table": {"catalogName": "managed", "schemaName": "schema_c", "tableName": "table_c"}},
			],
		},
	}
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_warehouse_list_everything

	# All three allowed via fast path (mock denies per-resource checks, so if
	# the slow path ran they'd fail).
	0 in result
	1 in result
	2 in result
}

test_fast_path_warehouse_broad_allows_all_schemas if {
	result := trino.batch with input as {
		"context": mock_context,
		"action": {
			"operation": "FilterSchemas",
			"filterResources": [
				{"schema": {"catalogName": "managed", "schemaName": "schema_a"}},
				{"schema": {"catalogName": "managed", "schemaName": "schema_b"}},
			],
		},
	}
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_warehouse_list_everything

	0 in result
	1 in result
}

test_fast_path_warehouse_broad_single_table_filter_columns if {
	result := trino.batch with input as {
		"context": mock_context,
		"action": {
			"operation": "FilterColumns",
			"filterResources": [{"table": {
				"catalogName": "managed",
				"schemaName": "schema_a",
				"tableName": "table_a",
				"columns": ["c1", "c2", "c3"],
			}}],
		},
	}
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_warehouse_list_everything

	# All column indices allowed.
	0 in result
	1 in result
	2 in result
}

# Fast-path must NOT cover SelectFromColumns, because `list_everything` only
# implies describe-level access, not read_data. Slow path runs and denies.
test_fast_path_does_not_cover_select_from_columns if {
	result := trino.batch with input as {
		"context": mock_context,
		"action": {
			"operation": "SelectFromColumns",
			"filterResources": [{"table": {"catalogName": "managed", "schemaName": "schema_a", "tableName": "table_a"}}],
		},
	}
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_warehouse_list_everything

	# Mock denies per-resource checks — slow path kicks in, table denied.
	not 0 in result
}

# ===================================================================
# Namespace-level list_everything: only resources in allowed namespaces
# short-circuit; others fall through to slow path.
# ===================================================================

test_fast_path_namespace_broad_allows_matching_namespace if {
	result := trino.batch with input as {
		"context": mock_context,
		"action": {
			"operation": "FilterTables",
			"filterResources": [
				{"table": {"catalogName": "managed", "schemaName": "schema_a", "tableName": "table_a"}},
				{"table": {"catalogName": "managed", "schemaName": "schema_b", "tableName": "table_b"}},
			],
		},
	}
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_namespace_list_everything_schema_a

	# schema_a allowed via namespace fast path; schema_b denied (slow path
	# runs, mock returns false for per-resource checks).
	0 in result
	not 1 in result
}

test_fast_path_namespace_broad_allows_matching_schema if {
	result := trino.batch with input as {
		"context": mock_context,
		"action": {
			"operation": "FilterSchemas",
			"filterResources": [
				{"schema": {"catalogName": "managed", "schemaName": "schema_a"}},
				{"schema": {"catalogName": "managed", "schemaName": "schema_b"}},
			],
		},
	}
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_namespace_list_everything_schema_a

	0 in result
	not 1 in result
}
