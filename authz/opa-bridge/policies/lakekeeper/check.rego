package lakekeeper

import data.configuration

# Get a lakekeeper project by its name
lakekeeper_by_id[lakekeeper_id] := lakekeeper if {
    lakekeeper := configuration.lakekeeper[_]
    lakekeeper_id := lakekeeper.id
}

# Check access to a warehouse
require_warehouse_access_simple(lakekeeper_id, warehouse_name, user, action) := true if {
    value := authenticated_http_send(
        lakekeeper_id,
        "POST", "/management/v1/action/batch-check",
        {
            "error-on-not-found": false,
            "checks" :  [
                {
                    "operation": {
                        "warehouse": {
                            "action": {"action": action},
                            "warehouse-id": warehouse_id_for_name(lakekeeper_id, warehouse_name)
                        }
                    },
                    "identity": {
                        "user": user
                    }
                }
            ]
        }
    ).body
    value.results[0].allowed == true
    count(value.results) == 1
}

# Check access to a warehouse
require_warehouse_create_namespace_access(lakekeeper_id, warehouse_name, user, properties) := true if {
    value := authenticated_http_send(
        lakekeeper_id,
        "POST", "/management/v1/action/batch-check",
        {
            "error-on-not-found": false,
            "checks" :  [
                {
                    "operation": {
                        "warehouse": {
                            "action": {"action": "create_namespace", "properties": properties},
                            "warehouse-id": warehouse_id_for_name(lakekeeper_id, warehouse_name)
                        }
                    },
                    "identity": {
                        "user": user
                    }
                }
            ]
        }
    ).body
    value.results[0].allowed == true
    count(value.results) == 1
}

# Check access to a namespace
require_namespace_access_simple(lakekeeper_id, warehouse_name, namespace_name, user, action) := true if {
    value := authenticated_http_send(
        lakekeeper_id,
        "POST", "/management/v1/action/batch-check", 
        {
            "error-on-not-found": false,
            "checks": [
                {
                    "operation": {
                        "namespace" : {
                            "action": {"action": action},
                            "warehouse-id": warehouse_id_for_name(lakekeeper_id, warehouse_name),
                            "namespace": namespace_name
                        }
                    },
                    "identity": {
                        "user": user
                    }
                }
            ]
        }
    ).body
    value.results[0].allowed == true
    count(value.results) == 1
}

require_namespace_access_create(lakekeeper_id, warehouse_name, namespace_name, user, action, properties) := true if {
    value := authenticated_http_send(
        lakekeeper_id,
        "POST", "/management/v1/action/batch-check", 
        {
            "error-on-not-found": false,
            "checks": [
                {
                    "operation": {
                        "namespace" : {
                            "action": {"action": action, "properties": properties},
                            "warehouse-id": warehouse_id_for_name(lakekeeper_id, warehouse_name),
                            "namespace": namespace_name
                        }
                    },
                    "identity": {
                        "user": user
                    }
                }
            ]
        }
    ).body
    value.results[0].allowed == true
    count(value.results) == 1
}

require_namespace_access_update_properties(lakekeeper_id, warehouse_name, namespace_name, user, removed_properties, updated_properties) := true if {
    value := authenticated_http_send(
        lakekeeper_id,
        "POST", "/management/v1/action/batch-check", 
        {
            "error-on-not-found": false,
            "checks": [
                {
                    "operation": {
                        "namespace" : {
                            "action": {"action": "update_properties", "removed_properties": removed_properties, "updated_properties": updated_properties},
                            "warehouse-id": warehouse_id_for_name(lakekeeper_id, warehouse_name),
                            "namespace": namespace_name
                        }
                    },
                    "identity": {
                        "user": user
                    }
                }
            ]
        }
    ).body
    value.results[0].allowed == true
    count(value.results) == 1
}

# Check access to a table
require_table_access_simple(lakekeeper_id, warehouse_name, namespace_name, table_name, user, action) := true if {
    value := authenticated_http_send(
        lakekeeper_id,
        "POST", "/management/v1/action/batch-check", 
        {
            "error-on-not-found": false,
            "checks": [
                {
                    "operation": {
                        "table": {
                            "action": {"action": action},
                            "warehouse-id": warehouse_id_for_name(lakekeeper_id, warehouse_name),
                            "namespace": namespace_name,
                            "table": table_name
                        }
                    },
                    "identity": {
                        "user": user
                    }
                }
            ]
        }
    ).body
    value.results[0].allowed == true
    count(value.results) == 1
}
# Check access to a table
require_table_access_commit(lakekeeper_id, warehouse_name, namespace_name, table_name, user, updated_properties, removed_properties) := true if {
    value := authenticated_http_send(
        lakekeeper_id,
        "POST", "/management/v1/action/batch-check", 
        {
            "error-on-not-found": false,
            "checks": [
                {
                    "operation": {
                        "table": {
                            "action": {
                                "action": "commit",
                                "updated_properties": updated_properties,
                                "removed_properties": removed_properties
                            },
                            "warehouse-id": warehouse_id_for_name(lakekeeper_id, warehouse_name),
                            "namespace": namespace_name,
                            "table": table_name
                        }
                    },
                    "identity": {
                        "user": user
                    }
                }
            ]
        }
    ).body
    value.results[0].allowed == true
    count(value.results) == 1
}

# Check access to a view
require_view_access_simple(lakekeeper_id, warehouse_name, namespace_name, view_name, user, action) := true if {
    value := authenticated_http_send(
        lakekeeper_id,
        "POST", "/management/v1/action/batch-check", 
        {
            "error-on-not-found": false,
            "checks": [
                {
                    "operation": {
                        "view": {
                            "action": {"action": action},
                            "warehouse-id": warehouse_id_for_name(lakekeeper_id, warehouse_name),
                            "namespace": namespace_name,
                            "table": view_name
                        }
                    },
                    "identity": {
                        "user": user
                    }
                }
            ]
        }
    ).body
    value.results[0].allowed == true
    count(value.results) == 1
}
