# Tests for Lakekeeper batch authorization (check_batch.rego).
# Verifies that allow/deny responses from Lakekeeper are correctly
# mapped to batch result indices.
package trino_test

import data.trino

# ===================================================================
# Lakekeeper batch: allow/deny based on responses
# ===================================================================

test_batch_filter_tables_managed_allow_all if {
	result := trino.batch with input as {
		"context": mock_context,
		"action": {
			"operation": "FilterTables",
			"filterResources": [
				{"table": {"catalogName": "managed", "schemaName": "my_schema", "tableName": "table_a"}},
				{"table": {"catalogName": "managed", "schemaName": "my_schema", "tableName": "table_b"}},
			],
		},
	}
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_allow_all

	0 in result
	1 in result
}

test_batch_filter_tables_managed_selective if {
	result := trino.batch with input as {
		"context": mock_context,
		"action": {
			"operation": "FilterTables",
			"filterResources": [
				{"table": {"catalogName": "managed", "schemaName": "my_schema", "tableName": "table_a"}},
				{"table": {"catalogName": "managed", "schemaName": "my_schema", "tableName": "table_b"}},
			],
		},
	}
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_allow_table_a

	0 in result
	not 1 in result
}

test_batch_filter_tables_managed_deny_all if {
	result := trino.batch with input as {
		"context": mock_context,
		"action": {
			"operation": "FilterTables",
			"filterResources": [
				{"table": {"catalogName": "managed", "schemaName": "my_schema", "tableName": "table_a"}},
				{"table": {"catalogName": "managed", "schemaName": "my_schema", "tableName": "table_b"}},
			],
		},
	}
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_deny_all

	not 0 in result
	not 1 in result
}

# View-only allow tests OR logic (table denied, view allowed → resource allowed)
test_batch_filter_tables_managed_view_check_sufficient if {
	result := trino.batch with input as {
		"context": mock_context,
		"action": {
			"operation": "FilterTables",
			"filterResources": [{"table": {"catalogName": "managed", "schemaName": "my_schema", "tableName": "table_a"}}],
		},
	}
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_allow_views_only

	0 in result
}

test_batch_select_from_columns_managed if {
	result := trino.batch with input as {
		"context": mock_context,
		"action": {
			"operation": "SelectFromColumns",
			"filterResources": [
				{"table": {"catalogName": "managed", "schemaName": "my_schema", "tableName": "table_a"}},
				{"table": {"catalogName": "managed", "schemaName": "my_schema", "tableName": "table_b"}},
			],
		},
	}
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_allow_table_a

	0 in result
	not 1 in result
}

test_batch_filter_schemas_managed_selective if {
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
		with http.send as mock_http_allow_schema_a

	0 in result
	not 1 in result
}
