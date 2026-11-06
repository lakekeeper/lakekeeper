package lakekeeper

import data.configuration

# Get a lakekeeper project by its name
config_by_id[lakekeeper_id] := lakekeeper if {
	some lakekeeper in configuration.lakekeeper
	lakekeeper_id := lakekeeper.id
}

# Send one check. Holds if Lakekeeper answers with status 200 and allows it.
_single_check_allowed(lakekeeper_id, check) if {
	response := authenticated_http_send(
		lakekeeper_id,
		"POST", "/management/v1/action/batch-check",
		{
			"error-on-not-found": false,
			"checks": [check],
		},
	)
	response.status_code == 200
	count(response.body.results) == 1
	response.body.results[0].allowed == true
}

# Check access to a warehouse
require_warehouse_access_simple(lakekeeper_id, warehouse_name, user, action) if {
	_single_check_allowed(lakekeeper_id, {
		"operation": {"warehouse": {
			"action": {"action": action},
			"warehouse-id": warehouse_id_for_name(lakekeeper_id, warehouse_name),
		}},
		"identity": {"user": user},
	})
}

# Check access to a warehouse
require_warehouse_create_namespace_access(lakekeeper_id, warehouse_name, user, properties, name) if {
	_single_check_allowed(lakekeeper_id, {
		"operation": {"warehouse": {
			"action": {"action": "create_namespace", "properties": properties, "name": name},
			"warehouse-id": warehouse_id_for_name(lakekeeper_id, warehouse_name),
		}},
		"identity": {"user": user},
	})
}

# Check access to a namespace
require_namespace_access_simple(lakekeeper_id, warehouse_name, namespace_name, user, action) if {
	require_namespace_access(lakekeeper_id, warehouse_name, namespace_name, user, {"action": action})
}

# Check an action on a namespace. `action` is the action object, for example
# `{"action": "delete", "recursive": true, "purge": true}`.
require_namespace_access(lakekeeper_id, warehouse_name, namespace_name, user, action) if {
	_single_check_allowed(lakekeeper_id, {
		"operation": {"namespace": {
			"action": action,
			"warehouse-id": warehouse_id_for_name(lakekeeper_id, warehouse_name),
			"namespace": namespace_name,
		}},
		"identity": {"user": user},
	})
}

require_namespace_access_create(lakekeeper_id, warehouse_name, namespace_name, user, action, properties, name) if {
	_single_check_allowed(lakekeeper_id, {
		"operation": {"namespace": {
			"action": {"action": action, "properties": properties, "name": name},
			"warehouse-id": warehouse_id_for_name(lakekeeper_id, warehouse_name),
			"namespace": namespace_name,
		}},
		"identity": {"user": user},
	})
}

require_namespace_access_update_properties(
	lakekeeper_id,
	warehouse_name,
	namespace_name,
	user,
	removed_properties,
	updated_properties,
) if {
	_single_check_allowed(lakekeeper_id, {
		"operation": {"namespace": {
			"action": {
				"action": "update_properties",
				"removed_properties": removed_properties,
				"updated_properties": updated_properties,
			},
			"warehouse-id": warehouse_id_for_name(lakekeeper_id, warehouse_name),
			"namespace": namespace_name,
		}},
		"identity": {"user": user},
	})
}

# Check access to a table
require_table_access_simple(lakekeeper_id, warehouse_name, namespace_name, table_name, user, action) if {
	require_table_access(lakekeeper_id, warehouse_name, namespace_name, table_name, user, {"action": action})
}

# Check an action on a table. `action` is the action object, for example
# `{"action": "drop", "purge": true}`.
require_table_access(lakekeeper_id, warehouse_name, namespace_name, table_name, user, action) if {
	_single_check_allowed(lakekeeper_id, {
		"operation": {"table": {
			"action": action,
			"warehouse-id": warehouse_id_for_name(lakekeeper_id, warehouse_name),
			"namespace": namespace_name,
			"table": table_name,
		}},
		"identity": {"user": user},
	})
}

# Check access to a table
require_table_access_commit(
	lakekeeper_id, warehouse_name, namespace_name,
	table_name, user, updated_properties, removed_properties,
) if {
	_single_check_allowed(lakekeeper_id, {
		"operation": {"table": {
			"action": {
				"action": "commit",
				"updated_properties": updated_properties,
				"removed_properties": removed_properties,
			},
			"warehouse-id": warehouse_id_for_name(lakekeeper_id, warehouse_name),
			"namespace": namespace_name,
			"table": table_name,
		}},
		"identity": {"user": user},
	})
}

# Check that `user` may replace the table called `table_name`, if one exists.
# Holds if no live table has this name, or if the user may commit to it.
# Replacing a table commits a new definition and new data to it in one commit
# and moves `main` to the new snapshot. Which other updates the commit holds
# depends on the table and the Trino version, so the check asks about a commit
# of any kind. `updated_properties` are the properties of the statement.
require_table_replace_access(
	lakekeeper_id, warehouse_name, namespace_name,
	table_name, user, updated_properties,
) if {
	_require_tabular_access_if_exists(
		lakekeeper_id, warehouse_name, namespace_name, table_name, user, "table",
		{
			"action": "commit",
			"target_refs": ["main"],
			"updated_properties": updated_properties,
		},
	)
}

# Check that `user` may replace the view called `view_name`, if one exists.
# Holds if no live view has this name, or if Lakekeeper allows the replace.
# Replacing a view commits a new definition to it. Trino does not say whether
# the new definition keeps the security mode, so if the view runs as another
# user, Lakekeeper also asks whether `user` may switch its security mode. A
# server that does not know this check answers with an error, and the check
# denies.
require_view_replace_access(lakekeeper_id, warehouse_name, namespace_name, view_name, user) if {
	_require_check_if_exists(lakekeeper_id, user, "view", {"view-replace": {
		"warehouse-id": warehouse_id_for_name(lakekeeper_id, warehouse_name),
		"namespace": namespace_name,
		"table": view_name,
	}})
}

# Check that `user` may select from the view called `view_name`, if one exists.
# Holds if no live view has this name, or if the user may select from it.
require_view_select_access_if_exists(lakekeeper_id, warehouse_name, namespace_name, view_name, user) if {
	_require_tabular_access_if_exists(
		lakekeeper_id, warehouse_name, namespace_name, view_name, user, "view",
		{"action": "select"},
	)
}

# Holds if `user` may do `action` on the `kind` (table or view) called `name`,
# or if no live `kind` has this name. The check asks Lakekeeper to answer `404`
# for a name that is free, which tells a free name apart from a denied action.
# Every other answer denies. Trino asks for a create check before it creates or
# replaces the table or view, so the check sees the tables and views that exist
# at that time. For a read, Trino loads a view while it analyzes the query and
# asks for the check afterwards; a view dropped in between counts as absent.
_require_tabular_access_if_exists(lakekeeper_id, warehouse_name, namespace_name, name, user, kind, action) if {
	_require_check_if_exists(lakekeeper_id, user, kind, {kind: {
		"action": action,
		"warehouse-id": warehouse_id_for_name(lakekeeper_id, warehouse_name),
		"namespace": namespace_name,
		"table": name,
	}})
}

# Holds if Lakekeeper allows `operation` for `user`, or if no live `kind`
# (table or view) has the name that `operation` names.
_require_check_if_exists(lakekeeper_id, user, kind, operation) if {
	response := authenticated_http_send(
		lakekeeper_id,
		"POST", "/management/v1/action/batch-check",
		{
			"error-on-not-found": true,
			"checks": [{
				"operation": operation,
				"identity": {"user": user},
			}],
		},
	)
	_allowed_or_not_found(response, kind)
}

_tabular_not_found_error_type := {"table": "NoSuchTableException", "view": "NoSuchViewException"}

_allowed_or_not_found(response, _) if {
	response.status_code == 200
	count(response.body.results) == 1
	response.body.results[0].allowed == true
}

_allowed_or_not_found(response, kind) if {
	response.status_code == 404
	response.body.error.type == _tabular_not_found_error_type[kind]
}

# Execute a batch-check, automatically chunking into multiple HTTP requests
# if the number of checks exceeds the instance's max_batch_check_size.
# Returns a flat array of results in the same order as the input checks.
# Undefined unless every request answers with one result per check.
batch_check_results(lakekeeper_id, checks) := all_results if {
	count(checks) > 0
	chunks := _batch_chunks(lakekeeper_id, checks)
	chunk_results := [_batch_check_http(lakekeeper_id, chunk) | some chunk in chunks]
	all_results := _complete_results(checks, chunks, chunk_results)
}

# Like `batch_check_results` but uses OPA force_cache, keyed via the
# `X-Request-ID` header set to `request_id`. Prefer this for probes that
# repeat many times within one query's fan-out (e.g. warehouse/namespace
# list_everything checks) — pass queryId so each new query re-probes once,
# then all its waves hit the cache, and Lakekeeper's audit logs carry the
# originating Trino queryId.
batch_check_results_cached(lakekeeper_id, checks, request_id, cache_secs) := all_results if {
	count(checks) > 0
	chunks := _batch_chunks(lakekeeper_id, checks)
	chunk_results := [_batch_check_http_cached(lakekeeper_id, chunk, request_id, cache_secs) | some chunk in chunks]
	all_results := _complete_results(checks, chunks, chunk_results)
}

_batch_chunks(lakekeeper_id, checks) := chunks if {
	_raw_max := object.get(config_by_id[lakekeeper_id], "max_batch_check_size", 1000)
	max_size := max([_raw_max, 1])
	num_batches := ceil(count(checks) / max_size)
	chunks := [array.slice(checks, start, start + max_size) |
		some batch_idx in numbers.range(0, num_batches - 1)
		start := batch_idx * max_size
	]
}

# Callers match results to checks by position. A request that failed is
# missing from `chunk_results`, so the counts tell a complete answer apart.
_complete_results(checks, chunks, chunk_results) := all_results if {
	count(chunk_results) == count(chunks)
	every i, chunk in chunks {
		is_array(chunk_results[i])
		count(chunk_results[i]) == count(chunk)
	}
	all_results := [result | some results in chunk_results; some result in results]
	count(all_results) == count(checks)
}

# Send a single batch-check HTTP request.
_batch_check_http(lakekeeper_id, checks) := response.body.results if {
	response := authenticated_http_send(
		lakekeeper_id,
		"POST", "/management/v1/action/batch-check",
		{
			"error-on-not-found": false,
			"checks": checks,
		},
	)
	response.status_code == 200
}

# Cached variant of `_batch_check_http`.
_batch_check_http_cached(lakekeeper_id, checks, request_id, cache_secs) := response.body.results if {
	response := authenticated_http_send_cached(
		lakekeeper_id,
		"POST", "/management/v1/action/batch-check",
		{
			"error-on-not-found": false,
			"checks": checks,
		},
		request_id,
		cache_secs,
	)
	response.status_code == 200
}

# Build a single check object for a warehouse action
build_warehouse_check(warehouse_id, user, action) := {
	"operation": {"warehouse": {
		"action": {"action": action},
		"warehouse-id": warehouse_id,
	}},
	"identity": {"user": user},
}

# Build a single check object for a table action
build_table_check(warehouse_id, namespace_name, table_name, user, action) := {
	"operation": {"table": {
		"action": {"action": action},
		"warehouse-id": warehouse_id,
		"namespace": namespace_name,
		"table": table_name,
	}},
	"identity": {"user": user},
}

# Build a single check object for a view action
build_view_check(warehouse_id, namespace_name, view_name, user, action) := {
	"operation": {"view": {
		"action": {"action": action},
		"warehouse-id": warehouse_id,
		"namespace": namespace_name,
		"table": view_name,
	}},
	"identity": {"user": user},
}

# Build a single check object for a namespace action
build_namespace_check(warehouse_id, namespace_name, user, action) := {
	"operation": {"namespace": {
		"action": {"action": action},
		"warehouse-id": warehouse_id,
		"namespace": namespace_name,
	}},
	"identity": {"user": user},
}

# Check access to a view
require_view_access_simple(lakekeeper_id, warehouse_name, namespace_name, view_name, user, action) if {
	require_view_access(lakekeeper_id, warehouse_name, namespace_name, view_name, user, {"action": action})
}

# Check an action on a view. `action` is the action object, for example
# `{"action": "drop", "purge": true}`.
require_view_access(lakekeeper_id, warehouse_name, namespace_name, view_name, user, action) if {
	_single_check_allowed(lakekeeper_id, {
		"operation": {"view": {
			"action": action,
			"warehouse-id": warehouse_id_for_name(lakekeeper_id, warehouse_name),
			"namespace": namespace_name,
			"table": view_name,
		}},
		"identity": {"user": user},
	})
}

# Check that `user`, as the owner of a DEFINER view, may read the view called
# `view_name` inside the DEFINER view's query. Lakekeeper allows it only if
# `view_name` has an owner property of its own and `user` may select it. A server
# that does not know this check answers with an error, and the check denies.
require_view_in_definer_view_access(lakekeeper_id, warehouse_name, namespace_name, view_name, user) if {
	_single_check_allowed(lakekeeper_id, {
		"operation": {"view-in-definer-view": {
			"warehouse-id": warehouse_id_for_name(lakekeeper_id, warehouse_name),
			"namespace": namespace_name,
			"table": view_name,
		}},
		"identity": {"user": user},
	})
}

# Check a table or view move into another namespace: `move` on the tabular and
# `accept_moved_tabular` on the destination namespace. `kind` is "table" or "view".
require_tabular_move_access(
	lakekeeper_id, warehouse_name, kind, namespace_name,
	tabular_name, destination_namespace_name, user,
) if {
	warehouse_id := warehouse_id_for_name(lakekeeper_id, warehouse_name)
	results := batch_check_results(lakekeeper_id, [
		build_tabular_move_check(warehouse_id, kind, namespace_name, tabular_name, destination_namespace_name, user),
		build_accept_moved_tabular_check(warehouse_id, destination_namespace_name, namespace_name, user),
	])
	count(results) == 2
	every result in results {
		result.allowed == true
	}
}

build_tabular_move_check(warehouse_id, kind, namespace_name, tabular_name, destination_namespace_name, user) := {
	"operation": {kind: {
		"action": {"action": "move", "destination": destination_namespace_name},
		"warehouse-id": warehouse_id,
		"namespace": namespace_name,
		"table": tabular_name,
	}},
	"identity": {"user": user},
}

build_accept_moved_tabular_check(warehouse_id, namespace_name, source_namespace_name, user) := {
	"operation": {"namespace": {
		"action": {"action": "accept_moved_tabular", "source": source_namespace_name},
		"warehouse-id": warehouse_id,
		"namespace": namespace_name,
	}},
	"identity": {"user": user},
}
