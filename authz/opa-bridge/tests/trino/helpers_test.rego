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
