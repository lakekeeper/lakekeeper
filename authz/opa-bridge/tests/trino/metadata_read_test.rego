# Tests which permission reading a metadata table needs. Metadata tables that
# show values of the data need `read_data` on the base table; the others need
# `get_metadata`.
package trino_test

import data.trino

# Allow every check of `allowed_action`, deny everything else, including
# `list_everything`.
_action_result(check, allowed_action) := {"allowed": true} if {
	some kind
	check.operation[kind].action.action == allowed_action
} else := {"allowed": false}

# No view has the name of a metadata table.
_no_view_of_that_name := {"status_code": 404, "body": {"error": {"code": 404, "type": "NoSuchViewException"}}}

mock_http_get_metadata_only(request) := {"status_code": 200, "body": {"access_token": "mock-token"}} if {
	endswith(request.url, "/token")
} else := {"status_code": 200, "body": {"defaults": {"prefix": "mock-wh-id"}}} if {
	contains(request.url, "catalog/v1/config")
} else := _no_view_of_that_name if {
	request.body["error-on-not-found"] == true
} else := {"status_code": 200, "body": {"results": results}} if {
	contains(request.url, "batch-check")
	results := [_action_result(check, "get_metadata") | some check in request.body.checks]
}

# Allow only `read_data` on the table `orders`.
_read_orders_result(check) := {"allowed": true} if {
	check.operation.table.action == {"action": "read_data"}
	check.operation.table.table == "orders"
} else := {"allowed": false}

mock_http_read_orders_only(request) := {"status_code": 200, "body": {"access_token": "mock-token"}} if {
	endswith(request.url, "/token")
} else := {"status_code": 200, "body": {"defaults": {"prefix": "mock-wh-id"}}} if {
	contains(request.url, "catalog/v1/config")
} else := _no_view_of_that_name if {
	request.body["error-on-not-found"] == true
} else := {"status_code": 200, "body": {"results": results}} if {
	contains(request.url, "batch-check")
	results := [_read_orders_result(check) | some check in request.body.checks]
}

_metadata_input(operation, name) := {
	"context": mock_context,
	"action": {
		"operation": operation,
		"resource": {"table": {"catalogName": "managed", "schemaName": "sales", "tableName": name, "columns": ["a", "b"]}},
	},
}

_allowed_with_get_metadata(request) if {
	trino.allow with input as request
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_get_metadata_only
}

_allowed_with_read_orders(request) if {
	trino.allow with input as request
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_read_orders_only
}

_data_value_types := ["files", "entries", "all_entries", "manifests", "all_manifests", "partitions"]

_metadata_only_types := ["snapshots", "history", "refs", "properties", "metadata_log_entries"]

test_metadata_table_read_actions_are_exact if {
	trino.metadata_table_read_action == {
		"files": "read_data",
		"entries": "read_data",
		"all_entries": "read_data",
		"manifests": "read_data",
		"all_manifests": "read_data",
		"partitions": "read_data",
		"snapshots": "get_metadata",
		"history": "get_metadata",
		"refs": "get_metadata",
		"properties": "get_metadata",
		"metadata_log_entries": "get_metadata",
	}

	# Every metadata table type Trino knows has an action.
	{type | some type, _ in trino.metadata_table_read_action} == trino._metadata_table_types
}

test_metadata_tables_with_data_values_need_read_data if {
	allowed := {type |
		some type in _data_value_types
		_allowed_with_get_metadata(_metadata_input("SelectFromColumns", concat("$", ["orders", type])))
	}
	allowed == set()

	refused := {type |
		some type in _data_value_types
		not _allowed_with_read_orders(_metadata_input("SelectFromColumns", concat("$", ["orders", type])))
	}
	refused == set()
}

test_metadata_tables_without_data_values_need_get_metadata if {
	refused := {type |
		some type in _metadata_only_types
		not _allowed_with_get_metadata(_metadata_input("SelectFromColumns", concat("$", ["orders", type])))
	}
	refused == set()

	allowed := {type |
		some type in _metadata_only_types
		_allowed_with_read_orders(_metadata_input("SelectFromColumns", concat("$", ["orders", type])))
	}
	allowed == set()
}

# Trino cannot read `$materialized_view_storage` from Lakekeeper, so the bridge
# denies it like other names with `$`, also with full access.
test_materialized_view_storage_is_denied if {
	name := "orders$materialized_view_storage"
	not _allowed_with_read_orders(_metadata_input("SelectFromColumns", name))
	not _allowed_with_full_access(_metadata_input("SelectFromColumns", name))
}

test_metadata_table_type_in_upper_case_needs_the_same_action if {
	not _allowed_with_get_metadata(_metadata_input("SelectFromColumns", "orders$FILES"))
	_allowed_with_read_orders(_metadata_input("SelectFromColumns", "orders$FILES"))
	_allowed_with_get_metadata(_metadata_input("SelectFromColumns", "orders$SNAPSHOTS"))
}

test_batch_metadata_tables_follow_the_same_actions if {
	names := ["orders$files", "orders$snapshots", "orders$partitions", "orders$history", "orders"]
	resources := [{"table": {"catalogName": "managed", "schemaName": "sales", "tableName": name}} | some name in names]
	describe_only := trino.batch with input as {
		"context": mock_context,
		"action": {"operation": "SelectFromColumns", "filterResources": resources},
	}
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_get_metadata_only
	describe_only == {1, 3}

	read_orders := trino.batch with input as {
		"context": mock_context,
		"action": {"operation": "SelectFromColumns", "filterResources": resources},
	}
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_read_orders_only
	read_orders == {0, 2, 4}
}

# A user who may only see a table can list it, list its columns and show
# its definition.
test_describe_only_user_can_list_and_describe if {
	refused := {operation |
		some operation in ["ShowColumns", "ShowCreateTable", "FilterColumns", "FilterTables"]
		not _allowed_with_get_metadata(_metadata_input(operation, "orders"))
	}
	refused == set()
	not _allowed_with_get_metadata(_metadata_input("SelectFromColumns", "orders"))

	tables := trino.batch with input as {
		"context": mock_context,
		"action": {"operation": "FilterTables", "filterResources": [{"table": {"catalogName": "managed", "schemaName": "sales", "tableName": "orders"}}]},
	}
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_get_metadata_only
	tables == {0}

	columns := trino.batch with input as {
		"context": mock_context,
		"action": {"operation": "FilterColumns", "filterResources": [{"table": {"catalogName": "managed", "schemaName": "sales", "tableName": "orders", "columns": ["a", "b"]}}]},
	}
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_get_metadata_only
	columns == {0, 1}
}
