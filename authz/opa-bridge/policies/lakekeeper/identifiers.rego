package lakekeeper

# Lifetime of a resolved warehouse name -> ID mapping.
warehouse_id_cache_seconds := 3600

# Lifetime of the retry lookup used while the long-lived entry holds an unusable response.
# Must differ from `warehouse_id_cache_seconds`: the TTL is part of OPA's cache key.
warehouse_id_retry_cache_seconds := 5

# Status codes OPA's http.send stores in the inter-query cache (topdown/http.go,
# `cacheableHTTPStatusCodes`). Any other status was fetched fresh, so retrying adds nothing.
_http_send_cacheable_status_codes := {200, 203, 204, 206, 300, 301, 404, 405, 410, 414, 501}

# Translate a warehouse name to a warehouse ID.
# Resolves only from a 200 response carrying a non-empty `defaults.prefix`.
#
# Two-tier cache: http.send force-caches cacheable responses without inspecting them, so an
# unusable one (e.g. a 404 while the warehouse is not yet created or not yet visible to this
# client) would deny access for the full `warehouse_id_cache_seconds`. When the long-lived
# response is unusable and may have come from the cache, the same lookup is repeated with
# `warehouse_id_retry_cache_seconds` (a separate cache entry), which recovers within seconds
# while limiting Lakekeeper to one lookup per warehouse per retry interval.
warehouse_id_for_name(lakekeeper_id, warehouse_name) := warehouse_id if {
	warehouse_id := _warehouse_id(_warehouse_config(lakekeeper_id, warehouse_name, warehouse_id_cache_seconds))
} else := warehouse_id if {
	_possibly_cached(_warehouse_config(lakekeeper_id, warehouse_name, warehouse_id_cache_seconds))
	warehouse_id := _warehouse_id(_warehouse_config(lakekeeper_id, warehouse_name, warehouse_id_retry_cache_seconds))
}

_possibly_cached(response) if response.status_code in _http_send_cacheable_status_codes

_warehouse_id(response) := prefix if {
	response.status_code == 200
	prefix := response.body.defaults.prefix
	is_string(prefix)
	count(prefix) > 0
}

_warehouse_config(lakekeeper_id, warehouse_name, cache_seconds) := response if {
	this := config_by_id[lakekeeper_id]
	url := concat("/", [this.url, sprintf("catalog/v1/config?warehouse=%s", [urlquery.encode(warehouse_name)])])
	response := http.send({
		"method": "GET",
		"url": url,
		"headers": {"Authorization": sprintf("Bearer %v", [access_token[lakekeeper_id]])},
		"force_cache": true,
		"force_cache_duration_seconds": cache_seconds,
		"caching_mode": "deserialized",
		"raise_error": false,
	})
}
