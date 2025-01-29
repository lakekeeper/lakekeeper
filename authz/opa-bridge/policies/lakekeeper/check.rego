package lakekeeper

import data.configuration

# Get a lakekeeper project by its name
lakekeeper_by_id[lakekeeper_id] := lakekeeper if {
    lakekeeper := configuration.lakekeeper[_]
    lakekeeper_id := lakekeeper.id
}

# Check access to a warehouse
require_warehouse_access(lakekeeper_id, warehouse_name, user, action) := true if {
    value := authenticated_http_send(
        lakekeeper_id,
        "POST", "/management/v1/permissions/check", 
        {
            "operation": {
                "warehouse": {
                    "action": action,
                    "warehouse-id": warehouse_id_for_name(lakekeeper_id, warehouse_name)
                }
            },
            "identity": {
                "user": user
            }
        }
    ).body
    value.allowed == true
}

# Check access to a namespace
require_namespace_access(lakekeeper_id, warehouse_name, namespace_name, user, action) := true if {
    value := authenticated_http_send(
        lakekeeper_id,
        "POST", "/management/v1/permissions/check", 
        {
            "operation": {
                "namespace" : {
                    "action": action,
                    "warehouse-id": warehouse_id_for_name(lakekeeper_id, warehouse_name),
                    "namespace": namespace_name
                }
            },
            "identity": {
                "user": user
            }
        }
    ).body
    value.allowed == true
}

# Check access to a table
require_table_access(lakekeeper_id, warehouse_name, namespace_name, table_name, user, action) := true if {
    value := authenticated_http_send(
        lakekeeper_id,
        "POST", "/management/v1/permissions/check", 
        {
            "operation": {
                "table": {
                    "action": action,
                    "warehouse-id": warehouse_id_for_name(lakekeeper_id, warehouse_name),
                    "namespace": namespace_name,
                    "table": table_name
                }
            },
            "identity": {
                "user": user
            }
        }
    ).body
    value.allowed == true
}

# Check access to a view
require_view_access(lakekeeper_id, warehouse_name, namespace_name, view_name, user, action) := true if {
    value := authenticated_http_send(
        lakekeeper_id,
        "POST", "/management/v1/permissions/check", 
        {
            "operation": {
                "view": {
                    "action": action,
                    "warehouse-id": warehouse_id_for_name(lakekeeper_id, warehouse_name),
                    "namespace": namespace_name,
                    "table": view_name
                }
            },
            "identity": {
                "user": user
            }
        }
    ).body
    value.allowed == true
}

