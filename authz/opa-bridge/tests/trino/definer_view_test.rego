# Tests for reads inside a DEFINER view. When a user queries a DEFINER view of
# another user, Trino asks `CreateViewWithSelectFromColumns` as the view's owner
# for each table and view that the view reads. A view read this way is allowed
# only if Lakekeeper confirms that it has an owner of its own.
package trino_test

import data.trino

_read_input(operation, catalog, name) := {
	"context": mock_context,
	"action": {
		"operation": operation,
		"resource": {"table": {"catalogName": catalog, "schemaName": "schema_a", "tableName": name}},
	},
}

_in_definer_view_check := {
	"operation": {"view-in-definer-view": {
		"warehouse-id": "mock-wh-id",
		"namespace": ["schema_a"],
		"table": "v",
	}},
	"identity": {"user": "oidc~test-user"},
}

_view_select_check := {
	"operation": {"view": {
		"action": {"action": "select"},
		"warehouse-id": "mock-wh-id",
		"namespace": ["schema_a"],
		"table": "v",
	}},
	"identity": {"user": "oidc~test-user"},
}

_table_read_check := {
	"operation": {"table": {
		"action": {"action": "read_data"},
		"warehouse-id": "mock-wh-id",
		"namespace": ["schema_a"],
		"table": "v",
	}},
	"identity": {"user": "oidc~test-user"},
}

# Allow a single check only if it is one of `allowed_checks`.
_mock_http_single(request, allowed_checks) := {"status_code": 200, "body": {"access_token": "mock-token"}} if {
	endswith(request.url, "/token")
} else := {"status_code": 200, "body": {"defaults": {"prefix": "mock-wh-id"}}} if {
	contains(request.url, "catalog/v1/config")
} else := {"status_code": 200, "body": {"results": [{"allowed": request.body.checks[0] in allowed_checks}]}} if {
	contains(request.url, "batch-check")
	count(request.body.checks) == 1
}

# Lakekeeper allows the read inside a DEFINER view: the view has an owner.
mock_http_view_has_owner(request) := _mock_http_single(request, [_in_definer_view_check])

# Lakekeeper refuses the read inside a DEFINER view, although the owner may
# select the view: the view has no owner.
mock_http_view_has_no_owner(request) := _mock_http_single(request, [_view_select_check])

mock_http_table_readable(request) := _mock_http_single(request, [_table_read_check])

_unknown_check_response := {
	"status_code": 422,
	"body": {"error": {"code": 422, "type": "UnprocessableEntity", "message": "unknown variant"}},
}

_is_in_definer_view_check(request) if {
	contains(request.url, "batch-check")
	request.body.checks[0].operation["view-in-definer-view"]
}

# A server that does not know the check rejects the request. It allows every
# check on a view and none on a table.
mock_http_server_without_check(request) := _unknown_check_response if {
	_is_in_definer_view_check(request)
} else := mock_http_allow_views_only(request)

# A server that does not know the check, on which the owner may read the table.
mock_http_server_without_check_table_readable(request) := _unknown_check_response if {
	_is_in_definer_view_check(request)
} else := _mock_http_single(request, [_table_read_check])

_read_on(operation) := _read_input(operation, "managed", "v")

test_read_inside_definer_view_of_a_view_with_owner_is_allowed if {
	trino.allow with input as _read_on("CreateViewWithSelectFromColumns")
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_view_has_owner
}

test_read_inside_definer_view_of_a_view_without_owner_is_denied if {
	not trino.allow with input as _read_on("CreateViewWithSelectFromColumns")
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_view_has_no_owner
}

test_select_of_a_view_asks_select if {
	trino.allow with input as _read_on("SelectFromColumns")
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_view_has_no_owner
	not trino.allow with input as _read_on("SelectFromColumns")
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_view_has_owner
}

test_read_inside_definer_view_of_a_table_asks_read_data if {
	trino.allow with input as _read_on("CreateViewWithSelectFromColumns")
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_table_readable
}

test_read_inside_definer_view_is_denied_by_a_server_without_the_check if {
	not trino.allow with input as _read_on("CreateViewWithSelectFromColumns")
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_server_without_check
	trino.allow with input as _read_on("SelectFromColumns")
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_server_without_check
}

test_table_inside_definer_view_is_allowed_by_a_server_without_the_check if {
	trino.allow with input as _read_on("CreateViewWithSelectFromColumns")
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with http.send as mock_http_server_without_check_table_readable
}

_read_allowed_in_unmanaged_catalog(operation) if {
	trino.allow with input as _read_input(operation, "external", "v")
		with data.configuration.lakekeeper as mock_lakekeeper
		with data.configuration.trino_catalog as mock_trino_catalog
		with data.configuration.trino_allow_unmanaged_catalogs as true
		with http.send as mock_http_allow_all
}

# The bridge cannot look up whether an object of an unmanaged catalog is a view
# without an owner, so the blanket allow leaves out reads inside a DEFINER view.
test_blanket_unmanaged_allow_denies_reads_inside_definer_views if {
	not _read_allowed_in_unmanaged_catalog("CreateViewWithSelectFromColumns")
	_read_allowed_in_unmanaged_catalog("SelectFromColumns")
}
