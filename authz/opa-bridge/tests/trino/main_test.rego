package trino_test

import data.trino

# --- Helpers ---

mock_context := {"identity": {"user": "test-user", "groups": []}, "softwareStack": {"trinoVersion": "467"}}

mock_trino_catalog := [{
	"name": "managed",
	"lakekeeper_id": "default",
	"lakekeeper_warehouse": "test-warehouse",
}]

# ===================================================================
# System catalog batch routing (no Lakekeeper calls)
# ===================================================================

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

	0 in result
	not 1 in result
}

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

	result == {0, 1, 2}
}

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

	0 in result
}

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

# ===================================================================
# Unmanaged catalog extension point
# ===================================================================

test_batch_filter_tables_unmanaged_denied_by_default if {
	result := trino.batch with input as {
		"context": mock_context,
		"action": {
			"operation": "FilterTables",
			"filterResources": [{"table": {"catalogName": "external_db", "schemaName": "public", "tableName": "users"}}],
		},
	}

	not 0 in result
}

test_batch_filter_tables_unmanaged_blanket_allow if {
	result := trino.batch with input as {
		"context": mock_context,
		"action": {
			"operation": "FilterTables",
			"filterResources": [{"table": {"catalogName": "external_db", "schemaName": "public", "tableName": "users"}}],
		},
	}
		with data.configuration.trino_allow_unmanaged_catalogs as true

	0 in result
}

test_unmanaged_flag_does_not_allow_managed if {
	not trino.allow with input as {
		"context": mock_context,
		"action": {
			"operation": "AccessCatalog",
			"resource": {"catalog": {"name": "managed"}},
		},
	}
		with data.configuration.trino_allow_unmanaged_catalogs as true
		with data.configuration.trino_catalog as mock_trino_catalog
}

test_batch_unmanaged_no_lakekeeper_calls if {
	result := trino.batch with input as {
		"context": mock_context,
		"action": {
			"operation": "FilterTables",
			"filterResources": [
				{"table": {"catalogName": "external_db", "schemaName": "public", "tableName": "users"}},
				{"table": {"catalogName": "system", "schemaName": "jdbc", "tableName": "types"}},
			],
		},
	}
		with data.configuration.trino_allow_unmanaged_catalogs as true

	0 in result
	1 in result
}
