# Tests that a batch check whose results are incomplete denies the whole batch.
# Results are matched to resources by position, so every chunk must answer with
# one result per check.
package trino_test

import data.lakekeeper
import data.trino

_two_checks_per_request := [object.union(mock_lakekeeper[0], {"max_batch_check_size": 2})]

_names_secret(request) if {
	some check in request.body.checks
	_mock_check_name(check) == "secret"
}

_mine_result(check) := {"allowed": true} if {
	_mock_check_name(check) == "mine"
} else := {"allowed": false}

_answer(request) := {"status_code": 200, "body": {"access_token": "mock-token"}} if {
	endswith(request.url, "/token")
} else := {"status_code": 200, "body": {"defaults": {"prefix": "mock-wh-id"}}} if {
	contains(request.url, "catalog/v1/config")
}

# The request that checks `secret` fails; every other request is answered.
mock_http_secret_chunk_fails(request) := _answer(request) if {
	not contains(request.url, "batch-check")
} else := {"status_code": 500, "body": {"error": {"code": 500}}} if {
	_names_secret(request)
} else := {"status_code": 200, "body": {"results": [_mine_result(check) | some check in request.body.checks]}}

# The request that checks `secret` answers with one result too few.
mock_http_secret_chunk_short(request) := _answer(request) if {
	not contains(request.url, "batch-check")
} else := {"status_code": 200, "body": {"results": [{"allowed": true}]}} if {
	_names_secret(request)
} else := {"status_code": 200, "body": {"results": [_mine_result(check) | some check in request.body.checks]}}

# Each table yields two checks, so with two checks per request the tables
# `secret` and `mine` are checked in separate requests.
_filter_tables_input := {
	"context": mock_context,
	"action": {
		"operation": "FilterTables",
		"filterResources": [
			{"table": {"catalogName": "managed", "schemaName": "finance", "tableName": "secret"}},
			{"table": {"catalogName": "managed", "schemaName": "finance", "tableName": "mine"}},
		],
	},
}

test_batch_with_a_failed_request_allows_nothing if {
	result := trino.batch with input as _filter_tables_input
		with data.configuration.lakekeeper as _two_checks_per_request
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_secret_chunk_fails
	result == set()
}

test_batch_with_a_short_answer_allows_nothing if {
	result := trino.batch with input as _filter_tables_input
		with data.configuration.lakekeeper as _two_checks_per_request
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_secret_chunk_short
	result == set()
}

test_batch_with_complete_answers_keeps_positions if {
	result := trino.batch with input as _filter_tables_input
		with data.configuration.lakekeeper as _two_checks_per_request
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_allow_table_mine
	result == {1}
}

mock_http_allow_table_mine(request) := _answer(request) if {
	not contains(request.url, "batch-check")
} else := {"status_code": 200, "body": {"results": [_mine_result(check) | some check in request.body.checks]}}

_table_check(name) := {"operation": {"table": {"action": "get_metadata", "table": name}}}

test_batch_check_results_is_undefined_if_a_request_fails if {
	not lakekeeper.batch_check_results("default", [_table_check("mine"), _table_check("secret"), _table_check("mine")]) with data.configuration.lakekeeper as _two_checks_per_request
		with http.send as mock_http_secret_chunk_fails
}

test_batch_check_results_is_undefined_if_an_answer_is_short if {
	not lakekeeper.batch_check_results_cached("default", [_table_check("secret"), _table_check("mine")], "q", 10) with data.configuration.lakekeeper as _two_checks_per_request
		with http.send as mock_http_secret_chunk_short
}

test_batch_check_results_keeps_order if {
	results := lakekeeper.batch_check_results("default", [_table_check("mine"), _table_check("other"), _table_check("mine")]) with data.configuration.lakekeeper as _two_checks_per_request
		with http.send as mock_http_allow_table_mine
	results == [{"allowed": true}, {"allowed": false}, {"allowed": true}]
}

# The request answers with `results` as an object with one entry per check.
mock_http_results_as_object(request) := _answer(request) if {
	not contains(request.url, "batch-check")
} else := {"status_code": 200, "body": {"results": {sprintf("%d", [i]): {"allowed": true} | some i, _ in request.body.checks}}}

test_batch_check_results_is_undefined_if_results_are_not_an_array if {
	not lakekeeper.batch_check_results("default", [_table_check("mine"), _table_check("other")]) with data.configuration.lakekeeper as _two_checks_per_request
		with http.send as mock_http_results_as_object
}

test_batch_with_results_as_object_allows_nothing if {
	result := trino.batch with input as _filter_tables_input
		with data.configuration.lakekeeper as _two_checks_per_request
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_results_as_object
	result == set()
}

# Every check is allowed, but with a status other than 200.
mock_http_allow_with_status_201(request) := _answer(request) if {
	not contains(request.url, "batch-check")
} else := {"status_code": 201, "body": {"results": [{"allowed": true} | some _ in request.body.checks]}}

test_single_check_needs_status_200 if {
	refused := {operation |
		some operation in ["SelectFromColumns", "DropTable", "ShowCreateTable", "DropSchema", "CreateTable"]
		trino.allow with input as {
			"context": mock_context,
			"action": {
				"operation": operation,
				"resource": {
					"table": {"catalogName": "managed", "schemaName": "finance", "tableName": "mine"},
					"schema": {"catalogName": "managed", "schemaName": "finance"},
				},
			},
		}
			with data.configuration.lakekeeper as mock_lakekeeper
			with data.configuration.trino_catalog as mock_trino_catalog
			with http.send as mock_http_allow_with_status_201
	}
	refused == set()
}

test_single_check_with_status_200_is_allowed if {
	trino.allow with input as {
		"context": mock_context,
		"action": {
			"operation": "ShowCreateTable",
			"resource": {"table": {"catalogName": "managed", "schemaName": "finance", "tableName": "mine"}},
		},
	}
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_allow_all
}
