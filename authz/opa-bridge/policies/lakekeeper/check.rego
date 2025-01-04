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
            "action": {
                "action": action,
                "type": "warehouse",
                "id": warehouse_id_for_name(lakekeeper_id, warehouse_name)
            },
            "for-principal": {
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
            "action": {
                "action": action,
                "type": "namespace",
                "identifier-type": "name",
                "warehouse-id": warehouse_id_for_name(lakekeeper_id, warehouse_name),
                "name": namespace_name
            },
            "for-principal": {
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
            "action": {
                "action": action,
                "type": "table",
                "identifier-type": "name",
                "warehouse-id": warehouse_id_for_name(lakekeeper_id, warehouse_name),
                "namespace": namespace_name,
                "name": table_name
            },
            "for-principal": {
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
            "action": {
                "action": action,
                "type": "view",
                "identifier-type": "name",
                "warehouse-id": warehouse_id_for_name(lakekeeper_id, warehouse_name),
                "namespace": namespace_name,
                "name": view_name
            },
            "for-principal": {
                "user": user
            }
        }
    ).body
    value.allowed == true
}

