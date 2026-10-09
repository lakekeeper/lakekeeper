# Tests for which Lakekeeper check each table operation asks: data changes ask
# `write_data`, structural changes and history removal ask `commit` with the
# kinds of update they make, so a principal that may only write data is refused.
package trino_test

import data.trino

_table_input(operation) := {
	"context": mock_context,
	"action": {
		"operation": operation,
		"resource": {"table": {"catalogName": "managed", "schemaName": "schema_a", "tableName": "t"}},
	},
}

_procedure_input(procedure) := {
	"context": mock_context,
	"action": {
		"operation": "ExecuteTableProcedure",
		"resource": {
			"table": {"catalogName": "managed", "schemaName": "schema_a", "tableName": "t"},
			"function": {"catalogName": "managed", "schemaName": "schema_a", "functionName": procedure},
		},
	},
}

_data_kinds := {
	"add-snapshot", "set-snapshot-ref", "set-statistics", "remove-statistics",
	"set-partition-statistics", "remove-partition-statistics",
}

# Answers as Lakekeeper does for a principal that may write data but not change a
# table's structure: a commit is allowed only if it names kinds and every one of them
# is a data kind.
_mock_writer_result(check) := {"allowed": true} if {
	check.operation.table.action.action == "commit"
	kinds := object.get(check.operation.table.action, "update_kinds", [])
	count(kinds) > 0
	every kind in kinds {
		kind in _data_kinds
	}
} else := {"allowed": false} if {
	check.operation.table.action.action == "commit"
} else := {"allowed": true}

mock_http_writer(request) := {"status_code": 200, "body": {"access_token": "mock-token"}} if {
	endswith(request.url, "/token")
} else := {"status_code": 200, "body": {"defaults": {"prefix": "mock-wh-id"}}} if {
	contains(request.url, "catalog/v1/config")
} else := {"status_code": 200, "body": {"results": results}} if {
	contains(request.url, "batch-check")
	results := [_mock_writer_result(check) | some check in request.body.checks]
}

test_writer_may_change_data if {
	every operation in ["InsertIntoTable", "DeleteFromTable", "TruncateTable", "UpdateTableColumns"] {
		trino.allow with input as _table_input(operation)
			with data.configuration.lakekeeper as mock_lakekeeper
			with data.configuration.trino_catalog as mock_trino_catalog
			with http.send as mock_http_writer
	}
}

test_writer_may_not_change_structure if {
	every operation in [
		"AddColumn", "AlterColumn", "DropColumn", "RenameColumn",
		"SetColumnComment", "SetTableComment",
	] {
		not trino.allow with input as _table_input(operation)
			with data.configuration.lakekeeper as mock_lakekeeper
			with data.configuration.trino_catalog as mock_trino_catalog
			with http.send as mock_http_writer
	}
}

test_writer_may_not_set_properties if {
	not trino.allow with input as object.union(
		_table_input("SetTableProperties"),
		{"action": {"resource": {"table": {"properties": {"format_version": 3}}}}},
	)
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_writer
}

test_writer_may_compact_but_not_add_files_or_remove_history if {
	every procedure in ["OPTIMIZE", "OPTIMIZE_MANIFESTS", "DROP_EXTENDED_STATS"] {
		trino.allow with input as _procedure_input(procedure)
			with data.configuration.lakekeeper as mock_lakekeeper
			with data.configuration.trino_catalog as mock_trino_catalog
			with http.send as mock_http_writer
	}
	every procedure in ["ADD_FILES", "EXPIRE_SNAPSHOTS", "REMOVE_ORPHAN_FILES"] {
		not trino.allow with input as _procedure_input(procedure)
			with data.configuration.lakekeeper as mock_lakekeeper
			with data.configuration.trino_catalog as mock_trino_catalog
			with http.send as mock_http_writer
	}
}

test_structural_changes_allowed_with_full_access if {
	every operation in ["AddColumn", "SetTableComment"] {
		trino.allow with input as _table_input(operation)
			with data.configuration.lakekeeper as mock_lakekeeper
			with data.configuration.trino_catalog as mock_trino_catalog
			with http.send as mock_http_allow_all
	}
	every procedure in ["ADD_FILES", "EXPIRE_SNAPSHOTS"] {
		trino.allow with input as _procedure_input(procedure)
			with data.configuration.lakekeeper as mock_lakekeeper
			with data.configuration.trino_catalog as mock_trino_catalog
			with http.send as mock_http_allow_all
	}
}
