# Tests for table and view names that Trino reads as metadata tables, and for
# other names with `$` or `@`. Trino reads `marketing."orders$files"` as the
# `$files` metadata table of the table `orders`. Table and view rules refuse
# such names, and reading one is decided on the base table (and on a view with
# this name, see metadata_table_view_test.rego). Every other name with `$` or
# `@` is refused.
package trino_test

import data.trino

# Allow every check except checks on tables: the user owns a view with the
# name of a metadata table and its namespace, but may not read the table
# `orders`.
_mock_all_but_tables_result(check) := {"allowed": false} if {
	check.operation.table
} else := {"allowed": true}

mock_http_allow_all_but_tables(request) := {"status_code": 200, "body": {"access_token": "mock-token"}} if {
	endswith(request.url, "/token")
} else := {"status_code": 200, "body": {"defaults": {"prefix": "mock-wh-id"}}} if {
	contains(request.url, "catalog/v1/config")
} else := {"status_code": 200, "body": {"results": results}} if {
	contains(request.url, "batch-check")
	results := [_mock_all_but_tables_result(check) | some check in request.body.checks]
}

_view_input(operation, name) := {
	"context": mock_context,
	"action": {
		"operation": operation,
		"resource": {"table": {"catalogName": "managed", "schemaName": "marketing", "tableName": name}},
	},
}

_view_rename_input(source, target) := {
	"context": mock_context,
	"action": {
		"operation": "RenameView",
		"resource": {"table": {"catalogName": "managed", "schemaName": "marketing", "tableName": source}},
		"targetResource": {"table": {"catalogName": "managed", "schemaName": "marketing", "tableName": target}},
	},
}

_view_operations := [
	"CreateView", "CreateMaterializedView", "SetViewComment", "SetColumnComment", "DropView",
	"FilterTables", "ShowColumns", "FilterColumns",
	"SelectFromColumns", "CreateViewWithSelectFromColumns",
]

_view_inputs(name) := array.concat(
	[_view_input(operation, name) | some operation in _view_operations],
	[_view_rename_input(name, "renamed")],
)

_allowed_without_table_access(request) if {
	trino.allow with input as request
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_allow_all_but_tables
}

_batch_without_table_access(operation, names) := result if {
	result := trino.batch with input as {
		"context": mock_context,
		"action": {
			"operation": operation,
			"filterResources": [{"table": {"catalogName": "managed", "schemaName": "marketing", "tableName": name}} |
				some name in names
			],
		},
	}
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_allow_all_but_tables
}

test_view_with_a_metadata_table_name_does_not_open_the_metadata_table if {
	not _allowed_without_table_access(_view_input("SelectFromColumns", "orders$files"))
}

test_every_view_operation_refuses_a_metadata_table_name if {
	allowed := {[request.action.operation, name] |
		some name in [
			"orders$files", "orders$snapshots", "orders$entries", "orders$properties",
			"orders$FILES",
		]
		some request in _view_inputs(name)
		_allowed_without_table_access(request)
	}
	allowed == set()
}

test_metadata_table_is_decided_on_the_base_table if {
	trino.allow with input as _view_input("SelectFromColumns", "orders$files")
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_allow_all
}

test_ordinary_views_are_unaffected if {
	refused := {request.action.operation |
		some request in _view_inputs("orders_by_day")
		not _allowed_without_table_access(request)
	}
	refused == set()
}

test_batch_hides_a_view_with_a_metadata_table_name if {
	_batch_without_table_access("SelectFromColumns", ["orders$entries", "orders_by_day"]) == {1}
	_batch_without_table_access("FilterTables", ["orders$files", "orders_by_day"]) == {1}
}

_allowed_with_full_access(request) if {
	trino.allow with input as request
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_allow_all
}

_batch_with_full_access(operation, names) := result if {
	result := trino.batch with input as {
		"context": mock_context,
		"action": {
			"operation": operation,
			"filterResources": [{"table": {"catalogName": "managed", "schemaName": "marketing", "tableName": name}} |
				some name in names
			],
		},
	}
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_allow_all
}

# Trino 484 matches the base name `[^$@]+` and a known type. Any other name with
# `$` or `@` is refused for every table and view operation, also with full
# access: Trino might load a view with this name, while the bridge cannot tell
# which object Trino acts on. `$materialized_view_storage` is refused too,
# because Trino cannot read it from Lakekeeper.
_other_names_with_dollar_or_at := [
	"a$b$files", "x@y$files", "orders$foo", "orders$", "$files", "x@y", "a$b",
	"orders$materialized_view_storage",
]

test_other_names_with_dollar_or_at_are_refused if {
	allowed := {[request.action.operation, name] |
		some name in _other_names_with_dollar_or_at
		some request in array.concat(
			_view_inputs(name),
			[_named_table_input(operation, "marketing", name) | some operation in _table_operations],
		)
		_allowed_with_full_access(request)
	}
	allowed == set()
}

test_metadata_table_names_follow_trino if {
	trino.is_metadata_table("orders$files")
	trino.is_metadata_table("orders$FILES")
	trino.is_metadata_table("orders$Snapshots")
	trino.is_metadata_table("daily sales$history")
	trino.split_metadata_table_name("orders$FILES") == "orders"
	not trino.is_metadata_table("a$b$files")
	not trino.is_metadata_table("x@y$files")
	not trino.is_metadata_table("orders$foo")
	not trino.is_metadata_table("orders$data")
	not trino.is_metadata_table("orders$materialized_view_storage")
	not trino.is_metadata_table("orders$files$files")
	not trino.is_metadata_table("$files")
	not trino.is_metadata_table("orders")

	# Trino ignores only ASCII case: U+017F (long s) and U+212A (Kelvin sign) do
	# not match `s` and `k`.
	not trino.is_metadata_table("orders$file\u017f")
	not trino.is_metadata_table("orders$\u017fnapshots")
	not trino.is_metadata_table("orders$\u212a")
}

test_metadata_table_name_with_non_ascii_type_is_refused if {
	not _allowed_with_full_access(_view_input("SelectFromColumns", "orders$file\u017f"))
}

# `$` and `@` are allowed in schema names, also for a nested schema.
test_schema_names_with_dollar_or_at_are_allowed if {
	refused := {schema |
		some schema in ["a$b", "x@y", "p.a$b", "p.x@y", "p.orders$files"]
		not _allowed_with_full_access(_schema_input("CreateSchema", schema))
	}
	refused == set()
}

test_metadata_table_with_upper_case_type_is_decided_on_the_base_table if {
	_allowed_with_full_access(_view_input("SelectFromColumns", "orders$FILES"))
	not _allowed_without_table_access(_view_input("SelectFromColumns", "orders$FILES"))
}

test_rename_target_with_dollar_or_at_is_refused if {
	allowed := {[operation, target] |
		some operation in ["RenameTable", "RenameView"]
		some target in ["orders$files", "a$b", "x@y"]
		_allowed_with_full_access({
			"context": mock_context,
			"action": {
				"operation": operation,
				"resource": {"table": {"catalogName": "managed", "schemaName": "marketing", "tableName": "orders_by_day"}},
				"targetResource": {"table": {"catalogName": "managed", "schemaName": "marketing", "tableName": target}},
			},
		})
	}
	allowed == set()
}

test_rename_to_an_ordinary_name_is_allowed if {
	some operation in ["RenameTable", "RenameView"]
	_allowed_with_full_access({
		"context": mock_context,
		"action": {
			"operation": operation,
			"resource": {"table": {"catalogName": "managed", "schemaName": "marketing", "tableName": "orders_by_day"}},
			"targetResource": {"table": {"catalogName": "managed", "schemaName": "marketing", "tableName": "orders_per_day"}},
		},
	})
}

test_batch_hides_other_names_with_dollar_or_at if {
	names := ["a$b$files", "x@y$files", "orders$foo", "orders$FILES", "orders_by_day"]
	_batch_with_full_access("SelectFromColumns", names) == {3, 4}
	_batch_with_full_access("FilterTables", names) == {4}
	_batch_with_full_access("FilterColumns", names) == {4}
}
