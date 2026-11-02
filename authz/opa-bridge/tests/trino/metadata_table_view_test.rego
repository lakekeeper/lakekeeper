# Tests that reading a metadata table also checks a view with the same name.
# Trino before 484 can read a view named `orders$files`, for example if the base
# table is created after Trino planned the query, so the user also needs
# `select` on such a view if it exists.
package trino_test

import data.trino

_view_answers := {
	"none": {"status_code": 404, "body": {"error": {"code": 404, "type": "NoSuchViewException"}}},
	"denied": {"status_code": 200, "body": {"results": [{"allowed": false}]}},
	"allowed": {"status_code": 200, "body": {"results": [{"allowed": true}]}},
	"error": {"status_code": 500, "body": {"error": {"code": 500, "type": "InternalServerError"}}},
	"other_not_found": {"status_code": 404, "body": {"error": {"code": 404, "type": "NoSuchNamespaceException"}}},
}

_expected_view_check_body := {
	"error-on-not-found": true,
	"checks": [{
		"operation": {"view": {
			"action": {"action": "select"},
			"warehouse-id": "mock-wh-id",
			"namespace": ["sales"],
			"table": "orders$files",
		}},
		"identity": {"user": "oidc~test-user"},
	}],
}

# Answers the view check with `input.view_answer`, and allows every other check.
mock_http_view_answer(request) := {"status_code": 200, "body": {"access_token": "mock-token"}} if {
	endswith(request.url, "/token")
} else := {"status_code": 200, "body": {"defaults": {"prefix": "mock-wh-id"}}} if {
	contains(request.url, "catalog/v1/config")
} else := _view_answers[input.view_answer] if {
	request.body == _expected_view_check_body
} else := {"status_code": 400, "body": {"error": {"code": 400, "type": "UnexpectedCheck"}}} if {
	request.body["error-on-not-found"] == true
} else := {"status_code": 200, "body": {"results": [{"allowed": true} | some _ in request.body.checks]}}

_read_metadata_table(view_answer) if {
	trino.allow with input as {
		"context": mock_context,
		"action": {
			"operation": "SelectFromColumns",
			"resource": {"table": {"catalogName": "managed", "schemaName": "sales", "tableName": "orders$files"}},
		},
		"view_answer": view_answer,
	}
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_view_answer
}

_batch_read_metadata_table(view_answer) := result if {
	result := trino.batch with input as {
		"context": mock_context,
		"action": {
			"operation": "SelectFromColumns",
			"filterResources": [
				{"table": {"catalogName": "managed", "schemaName": "sales", "tableName": "orders$files"}},
				{"table": {"catalogName": "managed", "schemaName": "sales", "tableName": "orders"}},
			],
		},
		"view_answer": view_answer,
	}
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_view_answer
}

test_metadata_table_without_a_view_of_that_name_is_allowed if {
	_read_metadata_table("none")
	_batch_read_metadata_table("none") == {0, 1}
}

test_metadata_table_with_a_view_the_user_may_not_select_is_denied if {
	not _read_metadata_table("denied")
	_batch_read_metadata_table("denied") == {1}
}

test_metadata_table_with_a_view_the_user_may_select_is_allowed if {
	_read_metadata_table("allowed")
	_batch_read_metadata_table("allowed") == {0, 1}
}

test_metadata_table_view_check_that_fails_denies if {
	not _read_metadata_table("error")
	not _read_metadata_table("other_not_found")
	_batch_read_metadata_table("error") == {1}
}

# Answers every view check with an error, so a read that sends one is denied.
mock_http_view_check_fails(request) := {"status_code": 200, "body": {"access_token": "mock-token"}} if {
	endswith(request.url, "/token")
} else := {"status_code": 200, "body": {"defaults": {"prefix": "mock-wh-id"}}} if {
	contains(request.url, "catalog/v1/config")
} else := {"status_code": 500, "body": {"error": {"code": 500, "type": "InternalServerError"}}} if {
	request.body["error-on-not-found"] == true
} else := {"status_code": 200, "body": {"results": [{"allowed": true} | some _ in request.body.checks]}}

_metadata_table_read_input(software_stack, view_answer) := {
	"context": {"identity": {"user": "test-user", "groups": []}, "softwareStack": software_stack},
	"action": {
		"operation": "SelectFromColumns",
		"resource": {"table": {"catalogName": "managed", "schemaName": "sales", "tableName": "orders$files"}},
	},
	"view_answer": view_answer,
}

_metadata_table_batch_input(software_stack, view_answer) := {
	"context": {"identity": {"user": "test-user", "groups": []}, "softwareStack": software_stack},
	"action": {
		"operation": "SelectFromColumns",
		"filterResources": [
			{"table": {"catalogName": "managed", "schemaName": "sales", "tableName": "orders$files"}},
			{"table": {"catalogName": "managed", "schemaName": "sales", "tableName": "orders"}},
		],
	},
	"view_answer": view_answer,
}

_read_metadata_table_on(software_stack) if {
	trino.allow with input as _metadata_table_read_input(software_stack, "none")
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_view_check_fails
}

_batch_read_metadata_table_on(software_stack) := result if {
	result := trino.batch with input as _metadata_table_batch_input(software_stack, "none")
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_view_check_fails
}

# Answers the view check with "no such view" (`input.view_answer` is "none").
_read_metadata_table_without_view_on(software_stack) if {
	trino.allow with input as _metadata_table_read_input(software_stack, "none")
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_view_answer
}

_batch_read_metadata_table_without_view_on(software_stack) := result if {
	result := trino.batch with input as _metadata_table_batch_input(software_stack, "none")
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_view_answer
}

_older_or_unknown_software_stacks := [
	{"trinoVersion": "483"}, {"trinoVersion": "476"}, {"trinoVersion": "476-e.1"},
	{"trinoVersion": "UNKNOWN"}, {"trinoVersion": "garbage"}, {"trinoVersion": ""},
	{"trinoVersion": 484}, {},
]

# Trino 484 and later read the metadata table, so no view check is sent.
test_trino_484_and_later_send_no_view_check if {
	every version in ["484", "485", "484-SNAPSHOT", "500-e.1"] {
		_read_metadata_table_on({"trinoVersion": version})
		_batch_read_metadata_table_on({"trinoVersion": version}) == {0, 1}
	}
}

# Older and unknown versions send the view check: a failing check denies.
test_older_or_unknown_trino_versions_send_the_view_check if {
	every software_stack in _older_or_unknown_software_stacks {
		not _read_metadata_table_on(software_stack)
		_batch_read_metadata_table_on(software_stack) == {1}
	}
}

# Older and unknown versions are allowed when no view has the name.
test_older_or_unknown_trino_versions_pass_the_view_check_without_a_view if {
	every software_stack in _older_or_unknown_software_stacks {
		_read_metadata_table_without_view_on(software_stack)
		_batch_read_metadata_table_without_view_on(software_stack) == {0, 1}
	}
}

# On Trino 484 the base table decides alone: denied there, the read is denied.
test_trino_484_denies_a_metadata_table_of_a_denied_base_table if {
	not trino.allow with input as _metadata_table_read_input({"trinoVersion": "484"}, "none")
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_allow_all_but_tables
	result := trino.batch with input as _metadata_table_batch_input({"trinoVersion": "484"}, "none")
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_allow_all_but_tables
	not 0 in result
}
