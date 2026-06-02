package lakekeeper

# Retrieve an access token from the identity provider.
# Caches the token for 150 seconds.
# Token is specific to a `lakekeeper_id`.
access_token[lakekeeper_id] := access_token if {
	some lakekeeper_id, this in config_by_id
	value := http.send({
		"method": "POST",
		"headers": {"Content-type": "application/x-www-form-urlencoded"},
		"url": this.openid_token_endpoint,
		"force_cache": true,
		"force_cache_duration_seconds": 150,
		"caching_mode": "deserialized",
		"raw_body": sprintf(
			"grant_type=client_credentials&client_id=%v&client_secret=%v&scope=%v",
			[this.client_id, this.client_secret, this.scope],
		),
	}).body
	access_token := value.access_token
}

# Send an authenticated HTTP request to the lakekeeper service
authenticated_http_send(lakekeeper_id, method, path, body) := response if {
	this := config_by_id[lakekeeper_id]
	url := concat("/", [this.url, trim_left(path, "/")])
	response := http.send({
		"method": method,
		"url": url,
		"headers": {
			"Authorization": sprintf("Bearer %v", [access_token[lakekeeper_id]]),
			"Content-Type": "application/json",
		},
		"body": body,
	})
}

# Send an authenticated HTTP request with OPA force_cache enabled.
#
# `request_id` is forwarded to Lakekeeper as `X-Request-ID` — Lakekeeper's
# tracing middleware preserves the client-provided value, so Trino's queryId
# flows into Lakekeeper's audit/trace logs. It also varies OPA's http.send
# cache key (which includes headers), giving per-query consistency: each
# Trino query re-probes once, then all its fan-out waves hit the cache.
authenticated_http_send_cached(lakekeeper_id, method, path, body, request_id, cache_secs) := response if {
	this := config_by_id[lakekeeper_id]
	url := concat("/", [this.url, trim_left(path, "/")])
	response := http.send({
		"method": method,
		"url": url,
		"headers": {
			"Authorization": sprintf("Bearer %v", [access_token[lakekeeper_id]]),
			"Content-Type": "application/json",
			"X-Request-ID": request_id,
		},
		"body": body,
		"force_cache": true,
		"force_cache_duration_seconds": cache_secs,
		"caching_mode": "deserialized",
	})
}
