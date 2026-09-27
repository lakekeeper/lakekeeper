package lakekeeper_test

import data.lakekeeper

_lakekeeper_config := [{
	"id": "default",
	"url": "http://lakekeeper:8181",
	"openid_token_endpoint": "http://idp/token",
	"client_id": "c",
	"client_secret": "s",
	"scope": "lakekeeper",
}]

_ok(prefix) := {"status_code": 200, "body": {"defaults": {"prefix": prefix}}}

_status(code) := {"status_code": code, "body": {"error": {"code": code}}}

_network_error := {"status_code": 0, "error": {"code": "eval_http_send_network_error", "message": "x"}}

# Answers the token request; routes config lookups by TTL to `input.long` / `input.short`.
_mock(request) := {"status_code": 200, "body": {"access_token": "t"}} if {
	request.url == "http://idp/token"
} else := input.long if {
	request.force_cache_duration_seconds == lakekeeper.warehouse_id_cache_seconds
} else := input.short if {
	request.force_cache_duration_seconds == lakekeeper.warehouse_id_retry_cache_seconds
}

_resolve(long, short) := warehouse_id if {
	warehouse_id := lakekeeper.warehouse_id_for_name("default", "wh") with data.configuration.lakekeeper as _lakekeeper_config
		with http.send as _mock
		with input as {"long": long, "short": short}
}

_resolve_undefined(long, short) if {
	not _resolve(long, short)
}

# 1. Healthy 200 with prefix -> returns the warehouse ID.
test_healthy_response_returns_warehouse_id if {
	_resolve(_ok("wh-long"), _ok("wh-short")) == "wh-long"
}

# 2. Primary 200 without defaults.prefix, fallback healthy -> returns the fallback's ID.
test_primary_200_without_prefix_falls_back_to_healthy_id if {
	_resolve({"status_code": 200, "body": {"defaults": {}}}, _ok("wh-short")) == "wh-short"
}

test_cached_404_falls_back_to_healthy_id if {
	_resolve(_status(404), _ok("wh-short")) == "wh-short"
}

test_200_with_non_string_prefix_falls_back_to_healthy_id if {
	_resolve(_ok(42), _ok("wh-short")) == "wh-short"
}

test_200_with_empty_prefix_falls_back_to_healthy_id if {
	_resolve(_ok(""), _ok("wh-short")) == "wh-short"
}

# 3. Primary 503/500 are never force-cached by OPA (topdown/http.go's cacheable status list
# excludes them), so the primary call already reached Lakekeeper live and failed for real.
# Retrying immediately would double Lakekeeper's load during a genuine outage for no benefit,
# so the fallback is skipped and the function stays undefined - it does not paper over a live
# 5xx with a stale/short-lived guess.
test_uncached_503_skips_fallback_and_stays_undefined if {
	_resolve_undefined(_status(503), _ok("wh-short"))
}

test_uncached_500_skips_fallback_and_stays_undefined if {
	_resolve_undefined(_status(500), _ok("wh-short"))
}

# 4. Both primary and fallback unusable -> undefined, never an empty/invalid ID.
test_both_unusable_empty_prefix_is_undefined if {
	_resolve_undefined(_status(404), _ok(""))
}

test_both_unusable_404_is_undefined if {
	_resolve_undefined(_status(404), _status(404))
}

test_both_unusable_network_error_is_undefined if {
	_resolve_undefined(_network_error, _network_error)
}

test_network_error_skips_fallback_and_stays_undefined if {
	_resolve_undefined(_network_error, _ok("wh-short"))
}

# 5. Assert the actual request objects built by warehouse_id_for_name carry the right TTLs.
# Each mock below echoes the `force_cache_duration_seconds` it received back into the
# resolved warehouse ID, so the assertion only passes if that exact value reached the
# mock on the request object - not just that the two constants differ from each other.

# Primary is always answered as valid, so no fallback attempt should occur: the resolved
# ID must reflect the TTL the primary request carried.
_ttl_probe_primary(request) := {"status_code": 200, "body": {"access_token": "t"}} if {
	request.url == "http://idp/token"
} else := {"status_code": 200, "body": {"defaults": {"prefix": sprintf("ttl-%d", [request.force_cache_duration_seconds])}}}

test_primary_request_uses_long_lived_ttl if {
	result := lakekeeper.warehouse_id_for_name("default", "wh") with data.configuration.lakekeeper as _lakekeeper_config
		with http.send as _ttl_probe_primary
	result == sprintf("ttl-%d", [lakekeeper.warehouse_id_cache_seconds])
}

# Primary is always answered as unusable (forcing the fallback attempt); the resolved ID must
# reflect the TTL the *fallback* request carried.
_ttl_probe_fallback(request) := {"status_code": 200, "body": {"access_token": "t"}} if {
	request.url == "http://idp/token"
} else := {"status_code": 200, "body": {"defaults": {}}} if {
	request.force_cache_duration_seconds == lakekeeper.warehouse_id_cache_seconds
} else := {"status_code": 200, "body": {"defaults": {"prefix": sprintf("ttl-%d", [request.force_cache_duration_seconds])}}}

test_fallback_request_uses_short_retry_ttl if {
	result := lakekeeper.warehouse_id_for_name("default", "wh") with data.configuration.lakekeeper as _lakekeeper_config
		with http.send as _ttl_probe_fallback
	result == sprintf("ttl-%d", [lakekeeper.warehouse_id_retry_cache_seconds])
}

# 6. Primary and fallback requests must carry different `X-Lakekeeper-OPA-Cache-Tier` header
# values, so the two calls land in distinct cache entries independent of the TTL field.
_header_probe_primary(request) := {"status_code": 200, "body": {"access_token": "t"}} if {
	request.url == "http://idp/token"
} else := {"status_code": 200, "body": {"defaults": {"prefix": request.headers["X-Lakekeeper-OPA-Cache-Tier"]}}}

# Force the fallback branch by making the "long" TTL unusable, then read its header value.
_header_probe_fallback(request) := {"status_code": 200, "body": {"access_token": "t"}} if {
	request.url == "http://idp/token"
} else := {"status_code": 200, "body": {"defaults": {}}} if {
	request.force_cache_duration_seconds == lakekeeper.warehouse_id_cache_seconds
} else := {"status_code": 200, "body": {"defaults": {"prefix": request.headers["X-Lakekeeper-OPA-Cache-Tier"]}}}

test_primary_and_fallback_use_different_cache_tier_headers if {
	primary_tier := lakekeeper.warehouse_id_for_name("default", "wh") with data.configuration.lakekeeper as _lakekeeper_config
		with http.send as _header_probe_primary

	primary_tier == sprintf("%d", [lakekeeper.warehouse_id_cache_seconds])

	fallback_tier := lakekeeper.warehouse_id_for_name("default", "wh") with data.configuration.lakekeeper as _lakekeeper_config
		with http.send as _header_probe_fallback

	fallback_tier == sprintf("%d", [lakekeeper.warehouse_id_retry_cache_seconds])

	primary_tier != fallback_tier
}
