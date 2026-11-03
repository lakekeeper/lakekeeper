# Tests that every Lakekeeper check names the namespace and table that Trino's
# Iceberg REST catalog acts on, and that names Trino or Lakekeeper rewrite are refused.
#
# Trino builds a namespace with `Splitter.on(".").omitEmptyStrings().trimResults()`,
# the Iceberg REST client joins namespace parts with U+001F, and Lakekeeper reads a
# '+' in a request path as a space. For a name that one of them rewrites, a check
# would name another object than the one Trino acts on, so such names are refused.
package trino_test

import data.trino

# --- Inputs ---

_named_table_input(operation, schema, table) := {
	"context": mock_context,
	"action": {
		"operation": operation,
		"resource": {"table": {"catalogName": "managed", "schemaName": schema, "tableName": table}},
	},
}

_schema_input(operation, schema) := {
	"context": mock_context,
	"action": {
		"operation": operation,
		"resource": {"schema": {"catalogName": "managed", "schemaName": schema}},
	},
}

_named_procedure_input(schema, table) := {
	"context": mock_context,
	"action": {
		"operation": "ExecuteTableProcedure",
		"resource": {
			"table": {"catalogName": "managed", "schemaName": schema, "tableName": table},
			"function": {"functionName": "OPTIMIZE"},
		},
	},
}

_rename_identifier_input(operation, source_schema, target_schema, target_table) := {
	"context": mock_context,
	"action": {
		"operation": operation,
		"resource": {"table": {"catalogName": "managed", "schemaName": source_schema, "tableName": "orders"}},
		"targetResource": {"table": {"catalogName": "managed", "schemaName": target_schema, "tableName": target_table}},
	},
}

_batch_input(operation, resources) := {
	"context": mock_context,
	"action": {"operation": operation, "filterResources": resources},
}

_table_resource(schema, table) := {"table": {"catalogName": "managed", "schemaName": schema, "tableName": table}}

_schema_resource(schema) := {"schema": {"catalogName": "managed", "schemaName": schema}}

# --- Mock: every check is allowed except checks on the objects Trino acts on ---

# Namespaces Trino acts on for the rewritten names below.
_real_namespaces := {["sales"], ["a", "b"], ["sa les"]}

# Table names and new namespace names Trino acts on for the rewritten names below.
_real_names := {"sales", "b", "daily sales"}

_names_real_object(check) if {
	some kind
	check.operation[kind].namespace in _real_namespaces
}

_names_real_object(check) if {
	some kind
	check.operation[kind].table in _real_names
}

_names_real_object(check) if {
	some kind
	check.operation[kind].action.name in _real_names
}

_names_real_object(check) if {
	some kind
	check.operation[kind].action.destination in _real_namespaces
}

# Warehouse-level `list_everything` is denied, so the warehouse fast path stays off.
# Namespace-level `list_everything` follows the same rule, so the namespace fast path
# is exercised.
_deny_real_result(check) := {"allowed": false} if {
	_names_real_object(check)
} else := {"allowed": false} if {
	check.operation.warehouse.action.action == "list_everything"
} else := {"allowed": true}

mock_http_deny_real(request) := {"status_code": 200, "body": {"access_token": "mock-token"}} if {
	endswith(request.url, "/token")
} else := {"status_code": 200, "body": {"defaults": {"prefix": "mock-wh-id"}}} if {
	contains(request.url, "catalog/v1/config")
} else := {"status_code": 200, "body": {"results": results}} if {
	contains(request.url, "batch-check")
	results := [_deny_real_result(check) | some check in request.body.checks]
}

# --- Mock: allow only checks naming exactly `input.expected` ---

_names_expected_object(check) if {
	check.operation.warehouse.action.action != "list_everything"
	object.get(check.operation.warehouse.action, "name", input.expected.namespace[0]) == input.expected.namespace[0]
}

_names_expected_object(check) if {
	check.operation.namespace.namespace == input.expected.namespace
	check.operation.namespace.action.action != "list_everything"
}

_names_expected_object(check) if {
	some kind in ["table", "view"]
	check.operation[kind].namespace == input.expected.namespace
	check.operation[kind].table == input.expected.table
}

_expected_result(check) := {"allowed": true} if {
	_names_expected_object(check)
} else := {"allowed": false}

mock_http_expect_names(request) := {"status_code": 200, "body": {"access_token": "mock-token"}} if {
	endswith(request.url, "/token")
} else := {"status_code": 200, "body": {"defaults": {"prefix": "mock-wh-id"}}} if {
	contains(request.url, "catalog/v1/config")
} else := {"status_code": 200, "body": {"results": results}} if {
	contains(request.url, "batch-check")
	results := [_expected_result(check) | some check in request.body.checks]
}

# --- Names ---

# Schemas Trino sends to Lakekeeper as one of `_real_namespaces`.
_rewritten_schemas := [
	" sales", # Trino trims: ["sales"]
	"sales ",
	"sales\t",
	"sales\n",
	"\u3000sales", # ideographic space, trimmed by Guava's CharMatcher.whitespace()
	"sales\u00a0", # no-break space, trimmed by Guava's CharMatcher.whitespace()
	"a\u001fb", # the REST client sends parts separated by U+001F: ["a", "b"]
	"a..b", # Trino omits empty parts: ["a", "b"]
	".sales",
	"sales.",
	"a. b",
	"sa+les", # Lakekeeper reads '+' in a path as a space: ["sa les"]
]

_schema_operations := ["CreateSchema", "DropSchema", "ShowCreateSchema", "ShowTables", "FilterSchemas"]

_table_operations := [
	"CreateTable", "DropTable",
	"SetTableComment", "SetColumnComment",
	"AddColumn", "AlterColumn", "DropColumn", "RenameColumn",
	"InsertIntoTable", "DeleteFromTable", "TruncateTable", "UpdateTableColumns",
	"SetTableProperties",
	"FilterTables", "ShowColumns", "FilterColumns", "ShowCreateTable",
	"SelectFromColumns", "CreateViewWithSelectFromColumns",
	"CreateView", "CreateMaterializedView", "SetViewComment", "DropView",
]

# Every single-resource input naming `schema` and `table`.
_single_inputs(schema, table) := array.flatten([
	[_schema_input(operation, schema) | some operation in _schema_operations],
	[_named_table_input(operation, schema, table) | some operation in _table_operations],
	[
		_named_procedure_input(schema, table),
		_named_table_input("SelectFromColumns", schema, concat("", [table, "$snapshots"])),
		_rename_identifier_input("RenameTable", schema, schema, "orders_new"),
		_rename_identifier_input("RenameView", schema, schema, "orders_new"),
		_rename_identifier_input("RenameTable", "marketing", schema, "orders_new"),
		_rename_identifier_input("RenameView", "marketing", schema, "orders_new"),
	],
])

_allowed(request) if {
	trino.allow with input as request
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_deny_real
}

_allowed_by_all(request) if {
	trino.allow with input as request
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_allow_all
}

_allowed_by_expected(request) if {
	trino.allow with input as request
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_expect_names
}

# ===================================================================
# One rewritten name for each rewrite
# ===================================================================

test_leading_space_schema_cannot_select_from_the_trimmed_schema if {
	not _allowed(_named_table_input("SelectFromColumns", " sales", "orders"))
}

test_trailing_space_schema_cannot_insert_into_the_trimmed_schema if {
	not _allowed(_named_table_input("InsertIntoTable", "sales ", "orders"))
}

test_ideographic_space_schema_cannot_drop_in_the_trimmed_schema if {
	not _allowed(_named_table_input("DropTable", "\u3000sales", "orders"))
}

test_leading_space_schema_cannot_drop_the_trimmed_schema if {
	not _allowed(_schema_input("DropSchema", " sales"))
}

test_leading_space_schema_cannot_create_the_trimmed_schema if {
	not _allowed(_schema_input("CreateSchema", " sales"))
}

test_space_after_dot_cannot_create_the_trimmed_child_schema if {
	not _allowed(_schema_input("CreateSchema", "a. b"))
}

test_unit_separator_schema_cannot_select_from_the_nested_schema if {
	not _allowed(_named_table_input("SelectFromColumns", "a\u001fb", "orders"))
}

test_empty_part_schema_cannot_select_from_the_schema_without_it if {
	not _allowed(_named_table_input("SelectFromColumns", "a..b", "orders"))
}

test_plus_in_schema_cannot_select_from_the_schema_with_a_space if {
	not _allowed(_named_table_input("SelectFromColumns", "sa+les", "orders"))
}

test_plus_in_table_cannot_select_from_the_table_with_a_space if {
	not _allowed(_named_table_input("SelectFromColumns", "marketing", "daily+sales"))
}

test_plus_in_view_cannot_drop_the_view_with_a_space if {
	not _allowed(_named_table_input("DropView", "marketing", "daily+sales"))
}

test_rename_into_leading_space_schema_cannot_move_into_the_trimmed_schema if {
	not _allowed(_rename_identifier_input("RenameTable", "marketing", " sales", "orders"))
}

test_rename_to_a_table_name_with_plus_is_refused if {
	not _allowed(_rename_identifier_input("RenameTable", "marketing", "marketing", "daily+sales"))
}

test_rename_to_a_view_name_with_plus_is_refused if {
	not _allowed(_rename_identifier_input("RenameView", "marketing", "marketing", "daily+sales"))
}

# ===================================================================
# Every operation refuses every rewritten name
# ===================================================================

test_every_single_resource_operation_refuses_rewritten_schemas if {
	allowed := {[request.action.operation, schema] |
		some schema in _rewritten_schemas
		some request in _single_inputs(schema, "orders")
		_allowed(request)
	}
	allowed == set()
}

test_every_table_operation_refuses_a_table_name_with_plus if {
	allowed := {request.action.operation |
		some operation in _table_operations
		request := _named_table_input(operation, "marketing", "daily+sales")
		_allowed(request)
	}
	allowed == set()
}

test_procedure_refuses_a_table_name_with_plus if {
	not _allowed(_named_procedure_input("marketing", "daily+sales"))
}

# Rewritten names are refused as such: they fail even with every check allowed.
test_every_single_resource_operation_refuses_rewritten_schemas_even_when_lakekeeper_allows if {
	allowed := {[request.action.operation, schema] |
		some schema in _rewritten_schemas
		some request in _single_inputs(schema, "orders")
		_allowed_by_all(request)
	}
	allowed == set()
}

# ===================================================================
# Batch operations hide rewritten names
# ===================================================================

test_filter_tables_hides_rewritten_schemas if {
	resources := [_table_resource(schema, "orders") | some schema in _rewritten_schemas]
	result := trino.batch with input as _batch_input("FilterTables", resources)
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_deny_real
	result == set()
}

test_filter_tables_hides_table_names_with_plus if {
	result := trino.batch with input as _batch_input("FilterTables", [_table_resource("marketing", "daily+sales")])
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_deny_real
	result == set()
}

test_select_from_columns_batch_hides_rewritten_names if {
	resources := array.concat(
		[_table_resource(schema, "orders") | some schema in _rewritten_schemas],
		[_table_resource("marketing", "daily+sales")],
	)
	result := trino.batch with input as _batch_input("SelectFromColumns", resources)
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_deny_real
	result == set()
}

test_filter_schemas_hides_rewritten_schemas if {
	resources := [_schema_resource(schema) | some schema in _rewritten_schemas]
	result := trino.batch with input as _batch_input("FilterSchemas", resources)
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_deny_real
	result == set()
}

test_filter_columns_single_table_hides_rewritten_schema if {
	resource := {"table": {
		"catalogName": "managed",
		"schemaName": " sales",
		"tableName": "orders",
		"columns": ["id", "amount"],
	}}
	result := trino.batch with input as _batch_input("FilterColumns", [resource])
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_deny_real
	result == set()
}

# Warehouse-level `list_everything` lists every object; rewritten names stay hidden.
test_warehouse_fast_path_hides_rewritten_names if {
	tables := trino.batch with input as _batch_input("FilterTables", [
		_table_resource(" sales", "orders"),
		_table_resource("marketing", "daily+sales"),
		_table_resource("marketing", "orders"),
	])
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_warehouse_list_everything
	tables == {2}

	schemas := trino.batch with input as _batch_input("FilterSchemas", [
		_schema_resource(" sales"),
		_schema_resource("marketing"),
	])
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_warehouse_list_everything
	schemas == {1}
}

# Resources around a hidden rewritten name keep their own results.
test_batch_results_stay_aligned_around_a_hidden_rewritten_name if {
	result := trino.batch with input as _batch_input("FilterTables", [
		_table_resource("schema_a", "table_b"),
		_table_resource(" schema_a", "table_a"),
		_table_resource("schema_a", "table_a"),
		_table_resource("schema_a", "table+a"),
		_table_resource("schema_b", "table_a"),
	])
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_allow_table_a
	result == {2, 4}
}

# ===================================================================
# Ordinary names reach Lakekeeper unchanged
# ===================================================================

_ordinary_names := [
	{"schema": "sales", "table": "orders", "namespace": ["sales"]},
	{"schema": "daily sales", "table": "weekly report", "namespace": ["daily sales"]},
	{"schema": "a b.c d", "table": "e f", "namespace": ["a b", "c d"]},
	{"schema": "a.b.c", "table": "orders", "namespace": ["a", "b", "c"]},
	{"schema": "Sales", "table": "Orders", "namespace": ["Sales"]},
	{"schema": "café.ümlaut", "table": "日本語", "namespace": ["café", "ümlaut"]},
	{"schema": "daily\u3000sales", "table": "daily\tsales", "namespace": ["daily\u3000sales"]},
	{"schema": "sales-2026_q1", "table": "orders~x", "namespace": ["sales-2026_q1"]},
]

test_namespace_for_schema_keeps_ordinary_names if {
	every name in _ordinary_names {
		trino.namespace_for_schema(name.schema) == name.namespace
	}
}

test_namespace_for_schema_refuses_rewritten_names if {
	every schema in _rewritten_schemas {
		not trino.namespace_for_schema(schema)
	}
}

# Each character Guava's CharMatcher.whitespace() trims, and U+180E, which Guava
# versions before 19 trimmed.
_trimmed_characters := [
	"\u0009", "\u000a", "\u000b", "\u000c", "\u000d", " ", "\u0085", "\u00a0",
	"\u1680", "\u180e", "\u2000", "\u2001", "\u2002", "\u2003", "\u2004", "\u2005",
	"\u2006", "\u2007", "\u2008", "\u2009", "\u200a", "\u2028", "\u2029", "\u202f",
	"\u205f", "\u3000",
]

_refused_at_either_end(character) if {
	not trino.namespace_for_schema(concat("", [character, "sales"]))
	not trino.namespace_for_schema(concat("", ["sales", character]))
	not trino.namespace_for_schema(concat("", ["a.", character, "b"]))
	not trino.namespace_for_schema(concat("", ["a", character, ".b"]))
}

test_namespace_for_schema_refuses_every_trimmed_character_at_either_end if {
	every character in _trimmed_characters {
		_refused_at_either_end(character)
	}
}

# Trino trims only the ends of a part.
test_namespace_for_schema_keeps_trimmed_characters_inside_a_part if {
	every character in _trimmed_characters {
		name := concat("", ["sa", character, "les"])
		trino.namespace_for_schema(name) == [name]
	}
}

test_namespace_for_schema_refuses_control_characters_at_either_end if {
	every character in ["\u0000", "\u0001", "\u001b", "\u007f", "\u009f"] {
		_refused_at_either_end(character)
	}
}

test_every_single_resource_operation_allows_ordinary_names if {
	refused := {[request.action.operation, name.schema] |
		some name in _ordinary_names
		some request in _single_inputs(name.schema, name.table)
		not _allowed_by_all(request)
	}
	refused == set()
}

test_table_operations_send_ordinary_names_unchanged if {
	refused := {[operation, name.schema] |
		some name in _ordinary_names
		some operation in ["DropTable", "InsertIntoTable", "SelectFromColumns", "FilterTables", "DropView", "SetViewComment"]
		request := object.union(_named_table_input(operation, name.schema, name.table), {"expected": name})
		not _allowed_by_expected(request)
	}
	refused == set()
}

test_schema_operations_send_ordinary_names_unchanged if {
	refused := {[operation, name.schema] |
		some name in _ordinary_names
		some operation in ["DropSchema", "ShowTables", "ShowCreateSchema", "FilterSchemas"]
		request := object.union(_schema_input(operation, name.schema), {"expected": name})
		not _allowed_by_expected(request)
	}
	refused == set()
}

test_create_schema_sends_the_ordinary_name_unchanged if {
	_allowed_by_expected(object.union(
		_schema_input("CreateSchema", "daily sales"),
		{"expected": {"namespace": ["daily sales"]}},
	))
	not _allowed_by_expected(object.union(
		_schema_input("CreateSchema", "daily sales"),
		{"expected": {"namespace": ["other"]}},
	))
}

test_batch_sends_ordinary_names_unchanged if {
	result := trino.batch with input as object.union(
		_batch_input("SelectFromColumns", [_table_resource("daily sales.café", "weekly report")]),
		{"expected": {"namespace": ["daily sales", "café"], "table": "weekly report"}},
	)
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_expect_names
	result == {0}
}
