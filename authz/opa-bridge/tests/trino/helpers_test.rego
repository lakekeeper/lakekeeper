package trino_test

# --- Shared test helpers ---

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

# Allow only namespace ["schema_a"] + warehouse checks (needed for system schema fallback).
# `list_everything` is denied so check_batch.rego's broad-access fast path stays
# disabled; these tests exercise the per-resource slow path only.
_mock_schema_a_result(check) := {"allowed": true} if {
	check.operation.namespace.namespace == ["schema_a"]
	check.operation.namespace.action.action != "list_everything"
} else := {"allowed": true} if {
	check.operation.warehouse
	check.operation.warehouse.action.action != "list_everything"
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
# `list_everything` is denied so the broad-access fast path stays disabled.
_mock_warehouse_only_result(check) := {"allowed": true} if {
	check.operation.warehouse
	check.operation.warehouse.action.action != "list_everything"
} else := {"allowed": false}

mock_http_allow_warehouse_only(request) := {"status_code": 200, "body": {"access_token": "mock-token"}} if {
	endswith(request.url, "/token")
} else := {"status_code": 200, "body": {"defaults": {"prefix": "mock-wh-id"}}} if {
	contains(request.url, "catalog/v1/config")
} else := {"status_code": 200, "body": {"results": results}} if {
	contains(request.url, "batch-check")
	results := [_mock_warehouse_only_result(check) | some check in request.body.checks]
}

# Allow only "products" table/view checks.
# `list_everything` on warehouse is denied so the broad-access fast path stays
# disabled — this mock exercises the selective per-resource slow path.
_mock_products_only_result(check) := {"allowed": true} if {
	_mock_check_name(check) == "products"
} else := {"allowed": true} if {
	check.operation.warehouse
	check.operation.warehouse.action.action != "list_everything"
} else := {"allowed": false}

mock_http_allow_products_only(request) := {"status_code": 200, "body": {"access_token": "mock-token"}} if {
	endswith(request.url, "/token")
} else := {"status_code": 200, "body": {"defaults": {"prefix": "mock-wh-id"}}} if {
	contains(request.url, "catalog/v1/config")
} else := {"status_code": 200, "body": {"results": results}} if {
	contains(request.url, "batch-check")
	results := [_mock_products_only_result(check) | some check in request.body.checks]
}

# Allow ONLY warehouse-level list_everything — deny everything else.
# Used to test the fast-path short-circuit: if per-resource checks are still
# being made (i.e. slow path ran), they'd return false and the test would fail.
_mock_warehouse_list_everything_result(check) := {"allowed": true} if {
	check.operation.warehouse.action.action == "list_everything"
} else := {"allowed": false}

mock_http_warehouse_list_everything(request) := {"status_code": 200, "body": {"access_token": "mock-token"}} if {
	endswith(request.url, "/token")
} else := {"status_code": 200, "body": {"defaults": {"prefix": "mock-wh-id"}}} if {
	contains(request.url, "catalog/v1/config")
} else := {"status_code": 200, "body": {"results": results}} if {
	contains(request.url, "batch-check")
	results := [_mock_warehouse_list_everything_result(check) | some check in request.body.checks]
}

# Allow ONLY namespace-level list_everything on namespace ["schema_a"] — deny
# warehouse-level list_everything and all per-resource checks. Used to test the
# namespace-level fast path in isolation.
_mock_namespace_list_everything_schema_a_result(check) := {"allowed": true} if {
	check.operation.namespace.action.action == "list_everything"
	check.operation.namespace.namespace == ["schema_a"]
} else := {"allowed": false}

mock_http_namespace_list_everything_schema_a(request) := {"status_code": 200, "body": {"access_token": "mock-token"}} if {
	endswith(request.url, "/token")
} else := {"status_code": 200, "body": {"defaults": {"prefix": "mock-wh-id"}}} if {
	contains(request.url, "catalog/v1/config")
} else := {"status_code": 200, "body": {"results": results}} if {
	contains(request.url, "batch-check")
	results := [_mock_namespace_list_everything_schema_a_result(check) | some check in request.body.checks]
}
