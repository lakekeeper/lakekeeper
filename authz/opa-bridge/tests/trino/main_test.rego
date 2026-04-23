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

# Large batch on a single unmanaged catalog: verifies the per-catalog optimization
# (allow_unmanaged evaluated once per unique catalog, not once per resource) still
# returns every allowed index. Regression test for _allowed_unmanaged_batch_catalogs
# membership check — if the collapse dropped indices, result would be a strict subset.
test_batch_unmanaged_many_rows_same_catalog if {
	rows := [{"table": {"catalogName": "external_db", "schemaName": "public", "tableName": sprintf("t%d", [i])}} |
		some i in numbers.range(0, 199)
	]
	result := trino.batch with input as {
		"context": mock_context,
		"action": {
			"operation": "FilterTables",
			"filterResources": rows,
		},
	}
		with data.configuration.trino_allow_unmanaged_catalogs as true

	# All 200 indices must be present.
	count(result) == 200
	count({i | some i in numbers.range(0, 199); i in result}) == 200
}

# Multiple distinct unmanaged catalogs in one batch: each catalog must be
# evaluated independently. Regression test for _batch_unmanaged_catalogs
# dedup — if dedup collapsed catalogs incorrectly, some would be missed.
test_batch_unmanaged_mixed_catalogs if {
	result := trino.batch with input as {
		"context": mock_context,
		"action": {
			"operation": "FilterTables",
			"filterResources": [
				{"table": {"catalogName": "external_a", "schemaName": "public", "tableName": "t1"}},
				{"table": {"catalogName": "external_b", "schemaName": "public", "tableName": "t2"}},
				{"table": {"catalogName": "external_a", "schemaName": "public", "tableName": "t3"}},
				{"table": {"catalogName": "external_c", "schemaName": "public", "tableName": "t4"}},
			],
		},
	}
		with data.configuration.trino_allow_unmanaged_catalogs as true

	result == {0, 1, 2, 3}
}

# Without blanket-allow, unmanaged catalogs fall back to per-resource
# allow_default_access. Verifies the fallback rule still admits resources
# that default_access would pass (system catalog / jdbc.types).
test_batch_unmanaged_fallback_via_default_access if {
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

	# external_db is unmanaged and the flag is off → denied.
	not 0 in result

	# system.jdbc.types is admitted by allow_default_access.
	1 in result
}
