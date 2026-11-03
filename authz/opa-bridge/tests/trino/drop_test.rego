# Tests the exact batch-check requests of drops. Lakekeeper drops the tables and
# views that Trino drops with purge, and the `DropSchema` request does not say
# whether the statement has `CASCADE`, so the schema check describes a recursive
# drop with purge.
package trino_test

import data.trino

# Allow only a batch-check whose body is exactly `input.expected_body`.
mock_http_expect_body(request) := {"status_code": 200, "body": {"access_token": "mock-token"}} if {
	endswith(request.url, "/token")
} else := {"status_code": 200, "body": {"defaults": {"prefix": "mock-wh-id"}}} if {
	contains(request.url, "catalog/v1/config")
} else := {"status_code": 200, "body": {"results": [{"allowed": true}]}} if {
	contains(request.url, "batch-check")
	request.body == input.expected_body
} else := {"status_code": 200, "body": {"results": [{"allowed": false} | some _ in request.body.checks]}}

_expected_check_body(operation) := {
	"error-on-not-found": false,
	"checks": [{"operation": operation, "identity": {"user": "oidc~test-user"}}],
}

_allowed_with_body(request) if {
	trino.allow with input as request
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_expect_body
}

_drop_input(operation, resource, body) := {
	"context": mock_context,
	"action": {"operation": operation, "resource": resource},
	"expected_body": body,
}

_table_resource_in(schema, name) := {"table": {"catalogName": "managed", "schemaName": schema, "tableName": name}}

test_drop_table_checks_drop_with_purge if {
	_allowed_with_body(_drop_input(
		"DropTable",
		_table_resource_in("sales", "orders"),
		_expected_check_body({"table": {
			"action": {"action": "drop", "purge": true},
			"warehouse-id": "mock-wh-id",
			"namespace": ["sales"],
			"table": "orders",
		}}),
	))
}

test_drop_schema_checks_recursive_delete_with_purge if {
	_allowed_with_body(_drop_input(
		"DropSchema",
		{"schema": {"catalogName": "managed", "schemaName": "sales.eu"}},
		_expected_check_body({"namespace": {
			"action": {"action": "delete", "recursive": true, "purge": true},
			"warehouse-id": "mock-wh-id",
			"namespace": ["sales", "eu"],
		}}),
	))
}

test_drop_view_checks_drop_with_purge if {
	_allowed_with_body(_drop_input(
		"DropView",
		_table_resource_in("sales", "orders_by_day"),
		_expected_check_body({"view": {
			"action": {"action": "drop", "purge": true},
			"warehouse-id": "mock-wh-id",
			"namespace": ["sales"],
			"table": "orders_by_day",
		}}),
	))
}

# A body without the flags is not what the bridge sends.
test_drop_table_without_purge_is_not_sent if {
	not _allowed_with_body(_drop_input(
		"DropTable",
		_table_resource_in("sales", "orders"),
		_expected_check_body({"table": {
			"action": {"action": "drop"},
			"warehouse-id": "mock-wh-id",
			"namespace": ["sales"],
			"table": "orders",
		}}),
	))
}
