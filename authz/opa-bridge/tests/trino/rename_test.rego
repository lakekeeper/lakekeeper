# Tests for RenameTable / RenameView: a rename into another schema is a move
# and needs `move` on the source and `accept_moved_tabular` on the target.
package trino_test

import data.trino

_rename_input(operation, target_schema) := _rename_input_between(operation, "schema_a", "managed", target_schema)

_rename_input_between(operation, source_schema, target_catalog, target_schema) := {
	"context": mock_context,
	"action": {
		"operation": operation,
		"resource": {"table": {"catalogName": "managed", "schemaName": source_schema, "tableName": "t"}},
		"targetResource": {"table": {"catalogName": target_catalog, "schemaName": target_schema, "tableName": "t2"}},
	},
}

# Two Trino catalogs on the same Lakekeeper warehouse.
_mock_two_trino_catalogs := [
	{
		"name": "managed",
		"lakekeeper_id": "default",
		"lakekeeper_warehouse": "test-warehouse",
	},
	{
		"name": "managed_b",
		"lakekeeper_id": "default",
		"lakekeeper_warehouse": "test-warehouse",
	},
]

# Allow every check except those whose action is `denied_action`.
_mock_deny_action_result(check, denied_action) := {"allowed": false} if {
	some kind
	check.operation[kind].action.action == denied_action
} else := {"allowed": true}

_mock_http_deny_action(request, denied_action) := {"status_code": 200, "body": {"access_token": "mock-token"}} if {
	endswith(request.url, "/token")
} else := {"status_code": 200, "body": {"defaults": {"prefix": "mock-wh-id"}}} if {
	contains(request.url, "catalog/v1/config")
} else := {"status_code": 200, "body": {"results": results}} if {
	contains(request.url, "batch-check")
	results := [_mock_deny_action_result(check, denied_action) | some check in request.body.checks]
}

mock_http_deny_move(request) := _mock_http_deny_action(request, "move")

mock_http_deny_accept_moved_tabular(request) := _mock_http_deny_action(request, "accept_moved_tabular")

# Allow `move` only from `source` towards `destination` and
# `accept_moved_tabular` only on `destination` from `source`; allow every other
# action.
_mock_move_paths_result(check, source, destination) := {"allowed": true} if {
	some kind
	check.operation[kind].action.action == "move"
	check.operation[kind].namespace == source
	check.operation[kind].table == "t"
	check.operation[kind].action.destination == destination
} else := {"allowed": true} if {
	check.operation.namespace.action.action == "accept_moved_tabular"
	check.operation.namespace.namespace == destination
	check.operation.namespace.action.source == source
} else := {"allowed": false} if {
	some kind
	check.operation[kind].action.action in {"move", "accept_moved_tabular"}
} else := {"allowed": true}

_mock_http_move_paths(request, source, destination) := {"status_code": 200, "body": {"access_token": "mock-token"}} if {
	endswith(request.url, "/token")
} else := {"status_code": 200, "body": {"defaults": {"prefix": "mock-wh-id"}}} if {
	contains(request.url, "catalog/v1/config")
} else := {"status_code": 200, "body": {"results": results}} if {
	contains(request.url, "batch-check")
	results := [_mock_move_paths_result(check, source, destination) | some check in request.body.checks]
}

mock_http_move_paths(request) := _mock_http_move_paths(request, ["schema_a"], ["schema_b"])

mock_http_move_nested_paths(request) := _mock_http_move_paths(request, ["a", "b"], ["a", "c"])

test_rename_table_into_another_schema_allowed if {
	trino.allow with input as _rename_input("RenameTable", "schema_b")
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_allow_all
}

test_rename_table_into_another_schema_checks_both_paths if {
	trino.allow with input as _rename_input("RenameTable", "schema_b")
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_move_paths
}

test_rename_table_into_another_schema_needs_move if {
	not trino.allow with input as _rename_input("RenameTable", "schema_b")
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_deny_move
}

test_rename_table_into_another_schema_needs_accept_moved_tabular if {
	not trino.allow with input as _rename_input("RenameTable", "schema_b")
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_deny_accept_moved_tabular
}

test_rename_table_within_schema_asks_no_move if {
	trino.allow with input as _rename_input("RenameTable", "schema_a")
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_deny_move
}

test_rename_table_within_schema_asks_no_accept_moved_tabular if {
	trino.allow with input as _rename_input("RenameTable", "schema_a")
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_deny_accept_moved_tabular
}

test_rename_view_into_another_schema_allowed if {
	trino.allow with input as _rename_input("RenameView", "schema_b")
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_allow_all
}

test_rename_view_into_another_schema_checks_both_paths if {
	trino.allow with input as _rename_input("RenameView", "schema_b")
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_move_paths
}

test_rename_view_into_another_schema_needs_move if {
	not trino.allow with input as _rename_input("RenameView", "schema_b")
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_deny_move
}

test_rename_view_into_another_schema_needs_accept_moved_tabular if {
	not trino.allow with input as _rename_input("RenameView", "schema_b")
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_deny_accept_moved_tabular
}

test_rename_view_within_schema_asks_no_move if {
	trino.allow with input as _rename_input("RenameView", "schema_a")
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_deny_move
}

test_rename_view_within_schema_asks_no_accept_moved_tabular if {
	trino.allow with input as _rename_input("RenameView", "schema_a")
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_deny_accept_moved_tabular
}

test_rename_table_into_nested_schema_checks_both_paths if {
	trino.allow with input as _rename_input_between("RenameTable", "a.b", "managed", "a.c")
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_move_nested_paths
}

test_rename_view_into_nested_schema_checks_both_paths if {
	trino.allow with input as _rename_input_between("RenameView", "a.b", "managed", "a.c")
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_move_nested_paths
}

test_rename_table_into_another_catalog_refused if {
	not trino.allow with input as _rename_input_between("RenameTable", "schema_a", "managed_b", "schema_b")
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as _mock_two_trino_catalogs
		with http.send as mock_http_allow_all
}

test_rename_table_into_another_catalog_same_schema_refused if {
	not trino.allow with input as _rename_input_between("RenameTable", "schema_a", "managed_b", "schema_a")
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as _mock_two_trino_catalogs
		with http.send as mock_http_allow_all
}

test_rename_view_into_another_catalog_refused if {
	not trino.allow with input as _rename_input_between("RenameView", "schema_a", "managed_b", "schema_b")
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as _mock_two_trino_catalogs
		with http.send as mock_http_allow_all
}

test_rename_table_within_catalog_with_two_catalogs_allowed if {
	trino.allow with input as _rename_input_between("RenameTable", "schema_a", "managed", "schema_b")
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as _mock_two_trino_catalogs
		with http.send as mock_http_allow_all
}
