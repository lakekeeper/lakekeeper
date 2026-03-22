package trino_test

import data.trino

# --- Helpers ---

mock_context := {"identity": {"user": "test-user", "groups": []}, "softwareStack": {"trinoVersion": "467"}}

mock_lakekeeper := [{
	"id": "default",
	"url": "http://mock-lakekeeper",
	"openid_token_endpoint": "http://mock-idp/token",
	"client_id": "test-client",
	"client_secret": "test-secret",
	"scope": "lakekeeper",
	"max_batch_check_size": 1000,
}]

mock_trino_catalog := [{
	"name": "managed",
	"lakekeeper_id": "default",
	"lakekeeper_warehouse": "test-warehouse",
}]

# --- Mock HTTP functions ---
# Each mock handles 3 HTTP call types: token, warehouse-id lookup, batch-check.

# Extract resource name from a batch-check check object
_mock_check_name(check) := check.operation.table.table
_mock_check_name(check) := check.operation.view.table

# Allow all batch-check results
mock_http_allow_all(request) := {"status_code": 200, "body": {"access_token": "mock-token"}} if {
	endswith(request.url, "/token")
} else := {"status_code": 200, "body": {"defaults": {"prefix": "mock-wh-id"}}} if {
	contains(request.url, "catalog/v1/config")
} else := {"status_code": 200, "body": {"results": results}} if {
	contains(request.url, "batch-check")
	results := [{"allowed": true} | some _check in request.body.checks]
}

# Deny all batch-check results
mock_http_deny_all(request) := {"status_code": 200, "body": {"access_token": "mock-token"}} if {
	endswith(request.url, "/token")
} else := {"status_code": 200, "body": {"defaults": {"prefix": "mock-wh-id"}}} if {
	contains(request.url, "catalog/v1/config")
} else := {"status_code": 200, "body": {"results": results}} if {
	contains(request.url, "batch-check")
	results := [{"allowed": false} | some _check in request.body.checks]
}

# Allow only resources named "table_a" (both table and view checks)
_mock_table_a_result(check) := {"allowed": true} if {
	_mock_check_name(check) == "table_a"
} else := {"allowed": false}

mock_http_allow_table_a(request) := {"status_code": 200, "body": {"access_token": "mock-token"}} if {
	endswith(request.url, "/token")
} else := {"status_code": 200, "body": {"defaults": {"prefix": "mock-wh-id"}}} if {
	contains(request.url, "catalog/v1/config")
} else := {"status_code": 200, "body": {"results": results}} if {
	contains(request.url, "batch-check")
	results := [_mock_table_a_result(check) | some check in request.body.checks]
}

# Allow only view checks (deny table checks) - tests OR logic in check_batch.rego
_mock_view_only_result(check) := {"allowed": true} if {
	check.operation.view
} else := {"allowed": false}

mock_http_allow_views_only(request) := {"status_code": 200, "body": {"access_token": "mock-token"}} if {
	endswith(request.url, "/token")
} else := {"status_code": 200, "body": {"defaults": {"prefix": "mock-wh-id"}}} if {
	contains(request.url, "catalog/v1/config")
} else := {"status_code": 200, "body": {"results": results}} if {
	contains(request.url, "batch-check")
	results := [_mock_view_only_result(check) | some check in request.body.checks]
}

# Allow only namespace ["schema_a"] + warehouse checks (needed for system schema fallback)
_mock_schema_a_result(check) := {"allowed": true} if {
	check.operation.namespace.namespace == ["schema_a"]
} else := {"allowed": true} if {
	check.operation.warehouse
} else := {"allowed": false}

mock_http_allow_schema_a(request) := {"status_code": 200, "body": {"access_token": "mock-token"}} if {
	endswith(request.url, "/token")
} else := {"status_code": 200, "body": {"defaults": {"prefix": "mock-wh-id"}}} if {
	contains(request.url, "catalog/v1/config")
} else := {"status_code": 200, "body": {"results": results}} if {
	contains(request.url, "batch-check")
	results := [_mock_schema_a_result(check) | some check in request.body.checks]
}

# Allow only warehouse-level checks (get_config), deny table/view-level checks.
# Simulates: user has catalog access but Lakekeeper doesn't know about system schema tables.
_mock_warehouse_only_result(check) := {"allowed": true} if {
	check.operation.warehouse
} else := {"allowed": false}

mock_http_allow_warehouse_only(request) := {"status_code": 200, "body": {"access_token": "mock-token"}} if {
	endswith(request.url, "/token")
} else := {"status_code": 200, "body": {"defaults": {"prefix": "mock-wh-id"}}} if {
	contains(request.url, "catalog/v1/config")
} else := {"status_code": 200, "body": {"results": results}} if {
	contains(request.url, "batch-check")
	results := [_mock_warehouse_only_result(check) | some check in request.body.checks]
}

# Allow only "products" table/view checks
_mock_products_only_result(check) := {"allowed": true} if {
	_mock_check_name(check) == "products"
} else := {"allowed": true} if {
	check.operation.warehouse
} else := {"allowed": false}

mock_http_allow_products_only(request) := {"status_code": 200, "body": {"access_token": "mock-token"}} if {
	endswith(request.url, "/token")
} else := {"status_code": 200, "body": {"defaults": {"prefix": "mock-wh-id"}}} if {
	contains(request.url, "catalog/v1/config")
} else := {"status_code": 200, "body": {"results": results}} if {
	contains(request.url, "batch-check")
	results := [_mock_products_only_result(check) | some check in request.body.checks]
}

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

# Exercises chunking: 30 denied tables + products (allowed) + revenue (denied)
test_batch_filter_tables_selective_with_many_tables if {
	resources := [{"table": {"catalogName": "managed", "schemaName": "finance", "tableName": sprintf("table_%d", [n])}} |
		some n in numbers.range(0, 29)
	]

	all_resources := array.flatten([
		resources,
		[{"table": {"catalogName": "managed", "schemaName": "finance", "tableName": "products"}}],
		[{"table": {"catalogName": "managed", "schemaName": "finance", "tableName": "revenue"}}],
	])

	result := trino.batch with input as {
		"context": mock_context,
		"action": {
			"operation": "FilterTables",
			"filterResources": all_resources,
		},
	}
		with data.configuration.lakekeeper as mock_lakekeeper
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
