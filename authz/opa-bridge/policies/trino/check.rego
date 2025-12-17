# Contains convenience wrappers around Lakekeeper functions and rules
# to reduce the number inputs required to call them
package trino

import data.lakekeeper
import data.trino
import data.configuration

trino_catalog_by_name[catalog_name] = trino_catalog if {
    trino_catalog := configuration.trino_catalog[_]
    catalog_name := trino_catalog.name
}

# Iceberg REST Namespaces are multi part identifiers (arrays).
# Trino schemas are strings separated by dots.
namespace_for_schema(schema_name) = namespace_name if {
    namespace_name := split(schema_name, ".")
    count(namespace_name) > 0
}

is_nested_schema(schema_name) := is_nested if {
    namespace_name := trino.namespace_for_schema(schema_name)
    is_nested := count(namespace_name) > 1
}

parent_schema(schema_name) = parent_schema if {
    namespace_name := trino.namespace_for_schema(schema_name)
    parent_namespace := array.slice(namespace_name, 0, count(namespace_name) - 1)
    parent_schema := concat(".", parent_namespace)
}

require_catalog_access(catalog_name, action) := true if {
    trino_catalog := trino.trino_catalog_by_name[catalog_name]
    lakekeeper.require_warehouse_access(
        trino_catalog.lakekeeper_id,
        trino_catalog.lakekeeper_warehouse, 
        trino.lakekeeper_user_id,
        action
    )
}

require_schema_access(catalog_name, schema_name, action) := true if {
    trino_catalog := trino.trino_catalog_by_name[catalog_name]
    namespace_name := trino.namespace_for_schema(schema_name)
    lakekeeper.require_namespace_access(
        trino_catalog.lakekeeper_id,
        trino_catalog.lakekeeper_warehouse, 
        namespace_name,
        trino.lakekeeper_user_id,
        action
    )
}

require_table_access(catalog_name, schema_name, table_name, action) := true if {
    trino_catalog := trino.trino_catalog_by_name[catalog_name]
    namespace_name := trino.namespace_for_schema(schema_name)
    lakekeeper.require_table_access(
        trino_catalog.lakekeeper_id,
        trino_catalog.lakekeeper_warehouse, 
        namespace_name,
        table_name,
        trino.lakekeeper_user_id,
        action
    )
}

require_view_access(catalog_name, schema_name, view_name, action) := true if {
    trino_catalog := trino.trino_catalog_by_name[catalog_name]
    namespace_name := trino.namespace_for_schema(schema_name)
    lakekeeper.require_view_access(
        trino_catalog.lakekeeper_id,
        trino_catalog.lakekeeper_warehouse, 
        namespace_name,
        view_name,
        trino.lakekeeper_user_id,
        action
    )
}

is_metadata_table(table_name) := true if {
    table_name_suffixes := [
        "$properties", "$history", "$metadata_log_entries", 
        "$snapshots", "$manifests", "$all_manifests", 
        "$partitions", "$files", "$entries", "$all_entries", "$refs"]
    some suffix in table_name_suffixes
    endswith(table_name, suffix)
} else = false

split_metadata_table_name(table_name) = base_table_name if {
    table_name_suffixes := [
        "$properties", "$history", "$metadata_log_entries", 
        "$snapshots", "$manifests", "$all_manifests", 
        "$partitions", "$files", "$entries", "$all_entries", "$refs"]
    some suffix in table_name_suffixes
    endswith(table_name, suffix)
    base_table_name := substring(table_name, 0, count(table_name) - count(suffix))
}
