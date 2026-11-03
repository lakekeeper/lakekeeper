# Tests for which Lakekeeper check each table operation asks: data changes ask
# `write_data`, structural changes and history removal ask `commit` without update
# kinds, so a principal that may only write data is refused.
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

# Answers as Lakekeeper does for a principal holding only `write` on the namespace: it
# reads and writes the data of its tables and lists the namespace and warehouse. A commit
# is allowed only if it names update kinds, every one of them a data kind, and changes no
# property. Everything else is refused.
default _writer_allows(_) := false

_writer_allows(check) if {
	action := check.operation.table.action
	action.action == "commit"
	count(object.get(action, "updated_properties", {})) == 0
	count(object.get(action, "removed_properties", [])) == 0
	kinds := object.get(action, "update_kinds", [])
	count(kinds) > 0
	every kind in kinds {
		kind in _data_kinds
	}
}

_writer_allows(check) if {
	check.operation.table.action.action in {"write_data", "read_data", "get_metadata", "include_in_list"}
	not contains(check.operation.table.table, "$")
}

_writer_allows(check) if {
	check.operation.namespace.action.action in {"get_metadata", "list_tables", "list_views", "list_namespaces", "list_everything", "include_in_list"}
}

_writer_allows(check) if {
	check.operation.warehouse.action.action in {"get_config", "get_metadata", "list_namespaces", "include_in_list"}
}

mock_http_writer(request) := {"status_code": 200, "body": {"access_token": "mock-token"}} if {
	endswith(request.url, "/token")
} else := {"status_code": 200, "body": {"defaults": {"prefix": "mock-wh-id"}}} if {
	contains(request.url, "catalog/v1/config")
} else := {"status_code": 200, "body": {"results": results}} if {
	contains(request.url, "batch-check")
	results := [{"allowed": _writer_allows(check)} | some check in request.body.checks]
}

_writer_may(input_document) if {
	trino.allow with input as input_document
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_writer
}

_writer_table_operations := [
	"CreateTable", "DropTable", "SetTableProperties", "SetTableComment", "SetViewComment",
	"SetColumnComment", "ShowColumns", "AddColumn", "AlterColumn", "DropColumn",
	"RenameColumn", "SelectFromColumns", "InsertIntoTable", "DeleteFromTable",
	"TruncateTable", "UpdateTableColumns", "CreateView", "DropView",
	"CreateViewWithSelectFromColumns", "CreateMaterializedView", "RefreshMaterializedView",
	"SetMaterializedViewProperties", "SetMaterializedViewComment", "DropMaterializedView",
	"ShowCreateTable", "FilterTables", "FilterColumns", "SetTableAuthorization",
	"SetViewAuthorization",
]

_writer_procedures := [
	"OPTIMIZE", "OPTIMIZE_MANIFESTS", "DROP_EXTENDED_STATS", "ROLLBACK_TO_SNAPSHOT",
	"EXPIRE_SNAPSHOTS", "REMOVE_ORPHAN_FILES", "ADD_FILES", "ADD_FILES_FROM_TABLE",
]

_with_table(input_document, table) := object.union(
	input_document,
	{"action": {"resource": {"table": {"tableName": table}}}},
)

writer_allowed contains operation if {
	some operation in _writer_table_operations
	_writer_may(_table_input(operation))
}

writer_allowed contains operation if {
	some operation in ["RenameTable", "RenameView", "RenameMaterializedView"]
	_writer_may(object.union(
		_table_input(operation),
		{"action": {"targetResource": {"table": {"catalogName": "managed", "schemaName": "schema_a", "tableName": "t2"}}}},
	))
}

writer_allowed contains procedure if {
	some procedure in _writer_procedures
	_writer_may(_procedure_input(procedure))
}

writer_allowed contains concat("", ["CALL ", procedure]) if {
	some procedure in ["rollback_to_snapshot", "register_table", "unregister_table", "migrate", "add_files"]
	_writer_may({"context": mock_context, "action": {
		"operation": "ExecuteProcedure",
		"resource": {"function": {"catalogName": "managed", "schemaName": "system", "functionName": procedure}},
	}})
}

writer_allowed contains concat("", [operation, " on t$snapshots"]) if {
	some operation in _writer_table_operations
	operation != "SelectFromColumns"
	_writer_may(_with_table(_table_input(operation), "t$snapshots"))
}

writer_allowed contains concat("", [procedure, " on t$snapshots"]) if {
	some procedure in _writer_procedures
	_writer_may(_with_table(_procedure_input(procedure), "t$snapshots"))
}

# Pins every Trino operation a principal holding only `write` may run: reading, data
# changes, compaction and statistics. Structural changes, history removal, drops,
# renames, replacing and procedures stay refused.
test_writer_may_run_exactly_these_operations if {
	writer_allowed == {
		"ShowColumns", "SelectFromColumns", "InsertIntoTable", "DeleteFromTable", "TruncateTable",
		"UpdateTableColumns", "CreateViewWithSelectFromColumns", "ShowCreateTable", "FilterTables",
		"FilterColumns", "OPTIMIZE", "OPTIMIZE_MANIFESTS", "DROP_EXTENDED_STATS",
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

# Allows a commit only if it names no update kinds, and refuses `write_data`.
_mock_any_commit_result(check) := {"allowed": true} if {
	check.operation.table.action.action == "commit"
	object.get(check.operation.table.action, "update_kinds", []) == []
} else := {"allowed": false} if {
	check.operation.table.action.action in ["commit", "write_data"]
} else := {"allowed": true}

mock_http_any_commit(request) := {"status_code": 200, "body": {"access_token": "mock-token"}} if {
	endswith(request.url, "/token")
} else := {"status_code": 200, "body": {"defaults": {"prefix": "mock-wh-id"}}} if {
	contains(request.url, "catalog/v1/config")
} else := {"status_code": 200, "body": {"results": results}} if {
	contains(request.url, "batch-check")
	results := [_mock_any_commit_result(check) | some check in request.body.checks]
}

# The bridge cannot see which updates Trino sends for these, so it asks about a
# commit of any kind.
test_structural_changes_ask_a_commit_of_any_kind if {
	every operation in [
		"AddColumn", "AlterColumn", "DropColumn", "RenameColumn",
		"SetColumnComment", "SetTableComment", "SetTableProperties",
	] {
		trino.allow with input as _table_input(operation)
			with data.configuration.lakekeeper as mock_lakekeeper
			with data.configuration.trino_catalog as mock_trino_catalog
			with http.send as mock_http_any_commit
	}
	every procedure in ["ADD_FILES", "EXPIRE_SNAPSHOTS", "REMOVE_ORPHAN_FILES"] {
		trino.allow with input as _procedure_input(procedure)
			with data.configuration.lakekeeper as mock_lakekeeper
			with data.configuration.trino_catalog as mock_trino_catalog
			with http.send as mock_http_any_commit
	}
}

test_data_changes_do_not_ask_a_commit if {
	every operation in ["InsertIntoTable", "DeleteFromTable", "TruncateTable", "UpdateTableColumns"] {
		not trino.allow with input as _table_input(operation)
			with data.configuration.lakekeeper as mock_lakekeeper
			with data.configuration.trino_catalog as mock_trino_catalog
			with http.send as mock_http_any_commit
	}
}
