package lakekeeper

# TTL of a warehouse config lookup. The bearer token is part of OPA's cache key and rotates
# every 150s (see `access_token`), so an entry is effectively reused for at most ~150s.
warehouse_id_cache_seconds := 3600

# TTL of the retry lookup used while the primary entry holds an unusable response.
# OPA's cache key is the whole request, TTL included, so the retry gets its own entry.
warehouse_id_retry_cache_seconds := 30

# Status codes OPA's http.send stores in the inter-query cache (topdown/http.go,
# `cacheableHTTPStatusCodes`). Any other status was fetched fresh, so retrying adds nothing.
_http_send_cacheable_status_codes := {200, 203, 204, 206, 300, 301, 404, 405, 410, 414, 501}

# Translate a warehouse name to a warehouse ID.
# Resolves only from a 200 response carrying a non-empty `defaults.prefix`.
#
# http.send caches responses without inspecting them. When the primary response is cacheable
# but unusable (e.g. 200 without a prefix, or 404), the lookup is retried with
# `warehouse_id_retry_cache_seconds`, so recovery takes at most the retry TTL.
# 5xx responses and network errors are never cached: there is no retry, and every query
# calls Lakekeeper. The unusable primary entry is not replaced after recovery, so each
# replica sends 1 request per warehouse per retry TTL until the token rotates.
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
	})
}
