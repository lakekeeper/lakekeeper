package trino_test

import data.trino

# --- Helpers ---

mock_context := {"identity": {"user": "test-user", "groups": []}, "softwareStack": {"trinoVersion": "467"}}

# --- batch: FilterCatalogs ---

test_batch_filter_catalogs_includes_system if {
	result := trino.batch with input as {
		"context": mock_context,
		"action": {
			"operation": "FilterCatalogs",
			"filterResources": [
				{"catalog": {"name": "system"}},
				{"catalog": {"name": "unknown"}},
			],
		},
	}

	# Index 0 (system) should be in the result
	0 in result

	# Index 1 (unknown) should not — unmanaged catalogs are off by default
	not 1 in result
}

# --- batch: FilterColumns on system.jdbc ---

test_batch_filter_columns_system_jdbc if {
	result := trino.batch with input as {
		"context": mock_context,
		"action": {
			"operation": "FilterColumns",
			"filterResources": [{"table": {
				"catalogName": "system",
				"schemaName": "jdbc",
				"tableName": "types",
				"columns": ["type_name", "data_type", "precision"],
			}}],
		},
	}

	# All three columns should be allowed
	result == {0, 1, 2}
}

# --- batch: FilterColumns preserves table properties (deep merge test) ---

test_batch_filter_columns_preserves_table_fields if {
	result := trino.batch with input as {
		"context": mock_context,
		"action": {
			"operation": "FilterColumns",
			"filterResources": [{"table": {
				"catalogName": "system",
				"schemaName": "information_schema",
				"tableName": "columns",
				"columns": ["column_name"],
			}}],
		},
	}

	# The column should be allowed — this only works if catalogName/schemaName/tableName
	# survive the object.union (deep merge)
	0 in result
}

# --- batch: FilterColumns denied for unknown table ---

test_batch_filter_columns_denied_for_unknown_table if {
	result := trino.batch with input as {
		"context": mock_context,
		"action": {
			"operation": "FilterColumns",
			"filterResources": [{"table": {
				"catalogName": "system",
				"schemaName": "information_schema",
				"tableName": "not_a_real_table",
				"columns": ["col1"],
			}}],
		},
	}
	count(result) == 0
}

# --- batch: regular filterResources (non-FilterColumns) ---

test_batch_filter_schemas_system if {
	result := trino.batch with input as {
		"context": mock_context,
		"action": {
			"operation": "FilterSchemas",
			"filterResources": [
				{"schema": {"catalogName": "system", "schemaName": "jdbc"}},
				{"schema": {"catalogName": "system", "schemaName": "runtime"}},
				{"schema": {"catalogName": "system", "schemaName": "secret_schema"}},
			],
		},
	}
	0 in result
	1 in result
	not 2 in result
}
