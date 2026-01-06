package lakekeeper

import data.configuration
import data.lakekeeper

# Translate a warehouse name to a warehouse ID.
# Cache the result for 1 hour to avoid repeated calls to the Lakekeeper API.
warehouse_id_for_name(lakekeeper_id, warehouse_name) := warehouse_id if {
    this := lakekeeper.lakekeeper_by_id[lakekeeper_id]
    url := concat("/", [this.url, sprintf("catalog/v1/config?warehouse=%s", [urlquery.encode(warehouse_name)])])
    warehouse_id := http.send({
        "method": "GET",  
        "url": url,
        "headers": {
            "Authorization": sprintf("Bearer %v", [access_token[lakekeeper_id]])
        },
        "force_cache": true,
        "force_cache_duration_seconds": 3600,
        "caching_mode": "deserialized",
    }).body.defaults.prefix
}
