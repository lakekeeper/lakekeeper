package lakekeeper

import data.configuration

# Get a lakekeeper project by its name
config_by_id[lakekeeper_id] := lakekeeper if {
	some lakekeeper in configuration.lakekeeper
	lakekeeper_id := lakekeeper.id
}

# Check access to a warehouse
require_warehouse_access_simple(lakekeeper_id, warehouse_name, user, action) if {
	value := authenticated_http_send(
		lakekeeper_id,
		"POST", "/management/v1/action/batch-check",
		{
			"error-on-not-found": false,
			"checks": [{
				"operation": {"warehouse": {
					"action": {"action": action},
					"warehouse-id": warehouse_id_for_name(lakekeeper_id, warehouse_name),
				}},
				"identity": {"user": user},
			}],
		},
	).body
	value.results[0].allowed == true
	count(value.results) == 1
}

# Check access to a warehouse
require_warehouse_create_namespace_access(lakekeeper_id, warehouse_name, user, properties, name) if {
	value := authenticated_http_send(
		lakekeeper_id,
		"POST", "/management/v1/action/batch-check",
		{
			"error-on-not-found": false,
			"checks": [{
				"operation": {"warehouse": {
					"action": {"action": "create_namespace", "properties": properties, "name": name},
					"warehouse-id": warehouse_id_for_name(lakekeeper_id, warehouse_name),
				}},
				"identity": {"user": user},
			}],
		},
	).body
	value.results[0].allowed == true
	count(value.results) == 1
}

# Check access to a namespace
require_namespace_access_simple(lakekeeper_id, warehouse_name, namespace_name, user, action) if {
	value := authenticated_http_send(
		lakekeeper_id,
		"POST", "/management/v1/action/batch-check",
		{
			"error-on-not-found": false,
			"checks": [{
				"operation": {"namespace": {
					"action": {"action": action},
					"warehouse-id": warehouse_id_for_name(lakekeeper_id, warehouse_name),
					"namespace": namespace_name,
				}},
				"identity": {"user": user},
			}],
		},
	).body
	value.results[0].allowed == true
	count(value.results) == 1
}

require_namespace_access_create(lakekeeper_id, warehouse_name, namespace_name, user, action, properties, name) if {
	value := authenticated_http_send(
		lakekeeper_id,
		"POST", "/management/v1/action/batch-check",
		{
			"error-on-not-found": false,
			"checks": [{
				"operation": {"namespace": {
					"action": {"action": action, "properties": properties, "name": name},
					"warehouse-id": warehouse_id_for_name(lakekeeper_id, warehouse_name),
					"namespace": namespace_name,
				}},
				"identity": {"user": user},
			}],
		},
	).body
	value.results[0].allowed == true
	count(value.results) == 1
}

require_namespace_access_update_properties(
	lakekeeper_id,
	warehouse_name,
	namespace_name,
	user,
	removed_properties,
	updated_properties,
) if {
	value := authenticated_http_send(
		lakekeeper_id,
		"POST", "/management/v1/action/batch-check",
		{
			"error-on-not-found": false,
			"checks": [{
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
			}],
		},
	).body
	value.results[0].allowed == true
	count(value.results) == 1
}

# Check access to a table
require_table_access_simple(lakekeeper_id, warehouse_name, namespace_name, table_name, user, action) if {
	value := authenticated_http_send(
		lakekeeper_id,
		"POST", "/management/v1/action/batch-check",
		{
			"error-on-not-found": false,
			"checks": [{
				"operation": {"table": {
					"action": {"action": action},
					"warehouse-id": warehouse_id_for_name(lakekeeper_id, warehouse_name),
					"namespace": namespace_name,
					"table": table_name,
				}},
				"identity": {"user": user},
			}],
		},
	).body
	value.results[0].allowed == true
	count(value.results) == 1
}

# Check access to a table
require_table_access_commit(
	lakekeeper_id, warehouse_name, namespace_name,
	table_name, user, updated_properties, removed_properties,
) if {
	value := authenticated_http_send(
		lakekeeper_id,
		"POST", "/management/v1/action/batch-check",
		{
			"error-on-not-found": false,
			"checks": [{
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
			}],
		},
	).body
	value.results[0].allowed == true
	count(value.results) == 1
}

# Execute a batch-check, automatically chunking into multiple HTTP requests
# if the number of checks exceeds the instance's max_batch_check_size.
# Returns a flat array of results in the same order as the input checks.
batch_check_results(lakekeeper_id, checks) := all_results if {
	count(checks) > 0
	_raw_max := object.get(config_by_id[lakekeeper_id], "max_batch_check_size", 1000)
	max_size := max([_raw_max, 1])
	num_batches := ceil(count(checks) / max_size)
	all_results := [result |
		some batch_idx in numbers.range(0, num_batches - 1)
		start := batch_idx * max_size
		batch := array.slice(checks, start, start + max_size)
		chunk_results := _batch_check_http(lakekeeper_id, batch)
		some result in chunk_results
	]
}

# Send a single batch-check HTTP request.
_batch_check_http(lakekeeper_id, checks) := value.results if {
	value := authenticated_http_send(
		lakekeeper_id,
		"POST", "/management/v1/action/batch-check",
		{
			"error-on-not-found": false,
			"checks": checks,
		},
	).body
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
	value := authenticated_http_send(
		lakekeeper_id,
		"POST", "/management/v1/action/batch-check",
		{
			"error-on-not-found": false,
			"checks": [{
				"operation": {"view": {
					"action": {"action": action},
					"warehouse-id": warehouse_id_for_name(lakekeeper_id, warehouse_name),
					"namespace": namespace_name,
					"table": view_name,
				}},
				"identity": {"user": user},
			}],
		},
	).body
	value.results[0].allowed == true
	count(value.results) == 1
}
