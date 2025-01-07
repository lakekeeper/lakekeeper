package lakekeeper

import data.lakekeeper

# Retrieve an access token from the identity provider.
# Caches the token for 150 seconds.
# Token is warehouse specific.
access_token[lakekeeper_id] := access_token if {
    this := lakekeeper.lakekeeper_by_id[lakekeeper_id]
    value := http.send({
        "method": "POST", 
        "headers": {"Content-type": "application/x-www-form-urlencoded"}, 
        "url": this.openid_token_endpoint,
        "force_cache": true,
        "force_cache_duration_seconds": 150,
        "caching_mode": "deserialized",
        "raw_body": sprintf(
            "grant_type=client_credentials&client_id=%v&client_secret=%v&scope=%v", 
            [this.client_id, this.client_secret, this.scope])
    }).body
    access_token := value.access_token
}

# Send an authenticated HTTP request to the lakekeeper service
authenticated_http_send(lakekeeper_id, method, path, body) := response if {
    this := lakekeeper.lakekeeper_by_id[lakekeeper_id]
    url := concat("/", [this.url, trim_left(path, "/")])
    response := http.send({
        "method": method,
        "url": url,
        "headers": {
            "Authorization": sprintf("Bearer %v", [access_token[lakekeeper_id]]),
            "Content-Type": "application/json"
        },
        "body": body
    })
    # print(body)
    # print(response)
}
