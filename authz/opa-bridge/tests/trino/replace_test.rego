# Tests for CreateTable / CreateView under a name that is already taken. Trino
# asks the same operation for `CREATE OR REPLACE`, so a taken name also needs
# `commit` on the table or view. A free name needs only the create action.
# The bridge cannot see which updates Trino sends, so it asks `commit` without
# update kinds.
package trino_test

import data.trino

_create_input(operation, schema, name) := {
	"context": mock_context,
	"action": {
		"operation": operation,
		"resource": {"table": {"catalogName": "managed", "schemaName": schema, "tableName": name}},
	},
}

_create_table_input := _create_input("CreateTable", "schema_a", "t")

_create_view_input := _create_input("CreateView", "schema_a", "t")

_not_found(error_type) := {"status_code": 404, "body": {"error": {
	"code": 404,
	"type": error_type,
	"message": "Tabular not found",
}}}

_replace_allowed := {"status_code": 200, "body": {"results": [{"allowed": true}]}}

_replace_denied := {"status_code": 200, "body": {"results": [{"allowed": false}]}}

# Answer the create check with `create_allowed` and the replace check, the only
# one sent with `error-on-not-found`, with `replace_response`.
_mock_http_replace(request, create_allowed, replace_response) := {"status_code": 200, "body": {"access_token": "mock-token"}} if {
	endswith(request.url, "/token")
} else := {"status_code": 200, "body": {"defaults": {"prefix": "mock-wh-id"}}} if {
	contains(request.url, "catalog/v1/config")
} else := replace_response if {
	contains(request.url, "batch-check")
	request.body["error-on-not-found"] == true
} else := {"status_code": 200, "body": {"results": [{"allowed": create_allowed}]}} if {
	contains(request.url, "batch-check")
}

mock_http_table_name_free(request) := _mock_http_replace(request, true, _not_found("NoSuchTableException"))

mock_http_view_name_free(request) := _mock_http_replace(request, true, _not_found("NoSuchViewException"))

mock_http_name_free_create_denied(request) := _mock_http_replace(request, false, _not_found("NoSuchTableException"))

mock_http_name_taken_replace_denied(request) := _mock_http_replace(request, true, _replace_denied)

mock_http_name_taken_replace_allowed(request) := _mock_http_replace(request, true, _replace_allowed)

mock_http_name_taken_create_denied(request) := _mock_http_replace(request, false, _replace_allowed)

mock_http_replace_warehouse_not_found(request) := _mock_http_replace(request, true, _not_found("NoSuchWarehouseException"))

mock_http_replace_not_found_without_type(request) := _mock_http_replace(request, true, {"status_code": 404, "body": {}})

mock_http_replace_forbidden(request) := _mock_http_replace(request, true, {"status_code": 403, "body": {"error": {
	"code": 403,
	"type": "CannotInspectPermissions",
	"message": "Forbidden",
}}})

mock_http_replace_server_error(request) := _mock_http_replace(request, true, {"status_code": 500, "body": {"error": {
	"code": 500,
	"type": "InternalServerError",
	"message": "Internal error",
}}})

mock_http_replace_no_results(request) := _mock_http_replace(request, true, {"status_code": 200, "body": {"results": []}})

mock_http_replace_two_results(request) := _mock_http_replace(
	request, true,
	{"status_code": 200, "body": {"results": [{"allowed": true}, {"allowed": true}]}},
)

# Allow the replace check only if it carries exactly `expected_check`.
_mock_http_replace_expecting(request, expected_check) := response if {
	contains(request.url, "batch-check")
	request.body["error-on-not-found"] == true
	response := _replace_response_if(request.body.checks == [expected_check])
} else := _mock_http_replace(request, true, _replace_denied)

_replace_response_if(true) := _replace_allowed

_replace_response_if(false) := _replace_denied

_expected_table_replace_check(namespace, updated_properties) := {
	"operation": {"table": {
		"action": {
			"action": "commit",
			"target_refs": ["main"],
			"update_kinds": ["set-current-schema", "set-default-sort-order", "set-default-spec"],
			"updated_properties": updated_properties,
		},
		"warehouse-id": "mock-wh-id",
		"namespace": namespace,
		"table": "t",
	}},
	"identity": {"user": "oidc~test-user"},
}

_expected_view_replace_check := {
	"operation": {"view": {
		"action": {"action": "commit"},
		"warehouse-id": "mock-wh-id",
		"namespace": ["schema_a"],
		"table": "t",
	}},
	"identity": {"user": "oidc~test-user"},
}

mock_http_expect_table_replace_check(request) := _mock_http_replace_expecting(
	request,
	_expected_table_replace_check(["schema_a"], {}),
)

mock_http_expect_nested_table_replace_check(request) := _mock_http_replace_expecting(
	request,
	_expected_table_replace_check(["a", "b"], {}),
)

mock_http_expect_table_replace_check_with_properties(request) := _mock_http_replace_expecting(
	request,
	_expected_table_replace_check(["schema_a"], {"format": "PARQUET", "write.parquet.compression-codec": "zstd"}),
)

mock_http_expect_view_replace_check(request) := _mock_http_replace_expecting(request, _expected_view_replace_check)

# --- CreateTable ---

test_create_table_free_name_needs_only_create_table if {
	trino.allow with input as _create_table_input
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_table_name_free
}

test_create_table_free_name_still_needs_create_table if {
	not trino.allow with input as _create_table_input
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_name_free_create_denied
}

test_create_table_taken_name_without_commit_denied if {
	not trino.allow with input as _create_table_input
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_name_taken_replace_denied
}

test_create_table_taken_name_with_commit_allowed if {
	trino.allow with input as _create_table_input
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_name_taken_replace_allowed
}

test_create_table_taken_name_still_needs_create_table if {
	not trino.allow with input as _create_table_input
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_name_taken_create_denied
}

test_create_table_asks_commit_of_a_replacement if {
	trino.allow with input as _create_table_input
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_expect_table_replace_check
}

# The replace check carries the statement's properties, flattened as for
# `SetTableProperties`.
test_create_table_forwards_properties_to_the_replace_check if {
	request := {
		"context": mock_context,
		"action": {
			"operation": "CreateTable",
			"resource": {"table": {
				"catalogName": "managed",
				"schemaName": "schema_a",
				"tableName": "t",
				"properties": {
					"format": "PARQUET",
					"extra_properties": {"write.parquet.compression-codec": "zstd"},
				},
			}},
		},
	}
	trino.allow with input as request
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_expect_table_replace_check_with_properties
}

test_create_table_in_nested_schema_asks_commit_on_nested_namespace if {
	trino.allow with input as _create_input("CreateTable", "a.b", "t")
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_expect_nested_table_replace_check
}

test_create_table_view_not_found_is_no_free_table_name if {
	not trino.allow with input as _create_table_input
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_view_name_free
}

test_create_table_warehouse_not_found_denied if {
	not trino.allow with input as _create_table_input
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_replace_warehouse_not_found
}

test_create_table_not_found_without_error_type_denied if {
	not trino.allow with input as _create_table_input
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_replace_not_found_without_type
}

test_create_table_replace_check_forbidden_denied if {
	not trino.allow with input as _create_table_input
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_replace_forbidden
}

test_create_table_replace_check_server_error_denied if {
	not trino.allow with input as _create_table_input
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_replace_server_error
}

test_create_table_replace_check_without_results_denied if {
	not trino.allow with input as _create_table_input
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_replace_no_results
}

test_create_table_replace_check_with_extra_results_denied if {
	not trino.allow with input as _create_table_input
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_replace_two_results
}

test_create_metadata_table_denied if {
	not trino.allow with input as _create_input("CreateTable", "schema_a", "t$snapshots")
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_allow_all
}

# --- CreateView ---

test_create_view_free_name_needs_only_create_view if {
	trino.allow with input as _create_view_input
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_view_name_free
}

test_create_view_taken_name_without_commit_denied if {
	not trino.allow with input as _create_view_input
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_name_taken_replace_denied
}

test_create_view_taken_name_with_commit_allowed if {
	trino.allow with input as _create_view_input
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_name_taken_replace_allowed
}

test_create_view_asks_commit_on_the_view if {
	trino.allow with input as _create_view_input
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_expect_view_replace_check
}

test_create_view_table_not_found_is_no_free_view_name if {
	not trino.allow with input as _create_view_input
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_table_name_free
}

test_create_view_replace_check_server_error_denied if {
	not trino.allow with input as _create_view_input
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_replace_server_error
}

test_create_materialized_view_taken_name_without_commit_denied if {
	not trino.allow with input as _create_input("CreateMaterializedView", "schema_a", "t")
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_name_taken_replace_denied
}

test_create_materialized_view_free_name_allowed if {
	trino.allow with input as _create_input("CreateMaterializedView", "schema_a", "t")
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_view_name_free
}
