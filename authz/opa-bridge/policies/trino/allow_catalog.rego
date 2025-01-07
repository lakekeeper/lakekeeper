package trino

import data.trino
import data.configuration

allow_catalog if {
    allow_catalog_management
}

allow_catalog if {
    allow_catalog_access
}

allow_catalog_management if {
    input.action.operation in ["CreateCatalog", "DropCatalog"]
    catalog := input.action.resource.catalog.name
    trino.require_catalog_access(catalog, "delete")
}

allow_catalog_access if {
    input.action.operation in ["AccessCatalog", "FilterCatalogs"]
    catalog := input.action.resource.catalog.name
    trino.require_catalog_access(catalog, "get_config")
}

