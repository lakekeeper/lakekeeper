# Tests for batch routing edge cases: system schema fallback, metadata tables,
# mixed resources, chunking, FilterColumns optimization, and correctness.
package trino_test

import data.trino

# ===================================================================
# System schema fallback in managed catalogs
# ===================================================================

# System schema tables go through per-resource allow (not Lakekeeper batch).
# With warehouse-only mock: allow_tables_in_system_schemas passes for known tables
# (requires catalog get_config), but allow_table_metadata fails for unknown tables
# (requires table-level get_metadata which the mock denies).
test_batch_filter_tables_managed_information_schema if {
	result := trino.batch with input as {
		"context": mock_context,
		"action": {
			"operation": "FilterTables",
			"filterResources": [
				{"table": {"catalogName": "managed", "schemaName": "information_schema", "tableName": "columns"}},
				{"table": {"catalogName": "managed", "schemaName": "information_schema", "tableName": "secret_table"}},
			],
		},
	}
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_allow_warehouse_only

	# "columns" is in allowed_information_schema_tables → allowed via system schema path
	0 in result

	# "secret_table" is not in the list, and Lakekeeper denies table-level access
	not 1 in result
}

test_batch_filter_schemas_managed_system_schema if {
	result := trino.batch with input as {
		"context": mock_context,
		"action": {
			"operation": "FilterSchemas",
			"filterResources": [
				{"schema": {"catalogName": "managed", "schemaName": "information_schema"}},
				{"schema": {"catalogName": "managed", "schemaName": "schema_discovery"}},
			],
		},
	}
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_allow_all

	0 in result
	1 in result
}

# ===================================================================
# Metadata tables in managed catalogs
# ===================================================================

# Metadata tables (e.g., foo$snapshots) are excluded from Lakekeeper batch
# and evaluated per-resource via allow_table_metadata_read
test_batch_filter_tables_managed_metadata_table if {
	result := trino.batch with input as {
		"context": mock_context,
		"action": {
			"operation": "FilterTables",
			"filterResources": [
				{"table": {"catalogName": "managed", "schemaName": "my_schema", "tableName": "my_table$snapshots"}},
				{"table": {"catalogName": "managed", "schemaName": "my_schema", "tableName": "my_table$history"}},
			],
		},
	}
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_allow_all

	0 in result
	1 in result
}

# ===================================================================
# Mixed resources: managed + system catalog in same request
# ===================================================================

test_batch_filter_tables_mixed_managed_and_system if {
	result := trino.batch with input as {
		"context": mock_context,
		"action": {
			"operation": "FilterTables",
			"filterResources": [
				{"table": {"catalogName": "managed", "schemaName": "my_schema", "tableName": "table_a"}},
				{"table": {"catalogName": "system", "schemaName": "jdbc", "tableName": "types"}},
				{"table": {"catalogName": "managed", "schemaName": "my_schema", "tableName": "table_b"}},
			],
		},
	}
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_allow_table_a

	# table_a: allowed by Lakekeeper
	0 in result

	# system.jdbc.types: allowed by allow_default_access (no Lakekeeper call)
	1 in result

	# table_b: denied by Lakekeeper
	not 2 in result
}

test_batch_filter_schemas_mixed_user_and_system if {
	result := trino.batch with input as {
		"context": mock_context,
		"action": {
			"operation": "FilterSchemas",
			"filterResources": [
				{"schema": {"catalogName": "managed", "schemaName": "schema_a"}},
				{"schema": {"catalogName": "managed", "schemaName": "information_schema"}},
				{"schema": {"catalogName": "managed", "schemaName": "schema_b"}},
			],
		},
	}
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_allow_schema_a

	# schema_a: allowed by Lakekeeper batch
	0 in result

	# information_schema: allowed by allow_filter_system_schemas (system schema fallback)
	1 in result

	# schema_b: denied by Lakekeeper batch
	not 2 in result
}

# ===================================================================
# Correctness: selective permissions with multiple tables
# ===================================================================

test_batch_filter_tables_products_allowed_revenue_denied if {
	result := trino.batch with input as {
		"context": mock_context,
		"action": {
			"operation": "FilterTables",
			"filterResources": [
				{"table": {"catalogName": "managed", "schemaName": "finance", "tableName": "products"}},
				{"table": {"catalogName": "managed", "schemaName": "finance", "tableName": "revenue"}},
			],
		},
	}
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_allow_products_only

	0 in result
	not 1 in result
}

# Exercises chunking with small batch size: 32 tables × 2 checks = 64 checks / 10 = 7 chunks
test_batch_filter_tables_selective_with_many_tables if {
	resources := [{"table": {"catalogName": "managed", "schemaName": "finance", "tableName": sprintf("table_%d", [n])}} |
		some n in numbers.range(0, 29)
	]

	all_resources := array.flatten([
		resources,
		[{"table": {"catalogName": "managed", "schemaName": "finance", "tableName": "products"}}],
		[{"table": {"catalogName": "managed", "schemaName": "finance", "tableName": "revenue"}}],
	])

	# Use small max_batch_check_size to force multiple chunks
	mock_lakekeeper_small_batch := [{
		"id": "default",
		"url": "http://mock-lakekeeper",
		"openid_token_endpoint": "http://mock-idp/token",
		"client_id": "test-client",
		"client_secret": "test-secret",
		"scope": "lakekeeper",
		"max_batch_check_size": 10,
	}]

	result := trino.batch with input as {
		"context": mock_context,
		"action": {
			"operation": "FilterTables",
			"filterResources": all_resources,
		},
	}
		with data.configuration.lakekeeper as mock_lakekeeper_small_batch
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_allow_products_only

	# products at index 30: allowed
	30 in result

	# revenue at index 31: denied
	not 31 in result

	# table_0..table_29: all denied
	not 0 in result
	not 15 in result
	not 29 in result
}

# ===================================================================
# FilterColumns: managed catalog column-level optimization
# ===================================================================

# Allowed table: all columns returned without per-column evaluation
test_batch_filter_columns_managed_allowed if {
	result := trino.batch with input as {
		"context": mock_context,
		"action": {
			"operation": "FilterColumns",
			"filterResources": [{"table": {
				"catalogName": "managed",
				"schemaName": "my_schema",
				"tableName": "table_a",
				"columns": ["col1", "col2", "col3"],
			}}],
		},
	}
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_allow_all

	result == {0, 1, 2}
}

# Denied table: no columns returned
test_batch_filter_columns_managed_denied if {
	result := trino.batch with input as {
		"context": mock_context,
		"action": {
			"operation": "FilterColumns",
			"filterResources": [{"table": {
				"catalogName": "managed",
				"schemaName": "my_schema",
				"tableName": "table_a",
				"columns": ["col1", "col2", "col3"],
			}}],
		},
	}
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_deny_all

	count(result) == 0
}

# Selective: only allowed table's columns returned
test_batch_filter_columns_managed_selective if {
	result := trino.batch with input as {
		"context": mock_context,
		"action": {
			"operation": "FilterColumns",
			"filterResources": [{"table": {
				"catalogName": "managed",
				"schemaName": "my_schema",
				"tableName": "table_b",
				"columns": ["col1", "col2"],
			}}],
		},
	}
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_allow_table_a

	# table_b is denied, so no columns
	count(result) == 0
}
