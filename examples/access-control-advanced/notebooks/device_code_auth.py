def authenticate_device_flow_with_pkce(
    token_endpoint: str,
    device_endpoint: str,
    client_id: str,
    scope: str = None,
    host_header: str = None,
    polling_interval: int = 5,
    # Only required because we are in docker and the users's browser
    # reaches keycloak under a different hostname than the server
    endpoint_rewrite: dict = {"keycloak:8080": "localhost:30080"},
):
    """
    Authenticate using OAuth 2.0 Device Authorization Grant with PKCE.

    Args:
        token_endpoint: OAuth token endpoint URL
        device_endpoint: Device authorization endpoint URL
        client_id: OAuth client ID
        scope: OAuth scope (defaults to client_id if None)
        host_header: Optional Host header value for special network setups
        polling_interval: Seconds to wait between token polling attempts
        endpoint_rewrite: Optional dict of {original: replacement} for URL rewrites

    Returns:
        dict: Contains 'access_token', 'refresh_token', and other OAuth response fields

    Raises:
        requests.HTTPError: If device code request or token request fails
    """
    import hashlib
    import base64
    import time
    import requests
    import secrets

    # Use default scope if not provided
    if scope is None:
        scope = client_id

    # Generate PKCE code verifier and challenge
    pkce_code_verifier = "".join(
        secrets.choice("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
        for _ in range(128)
    )
    pkce_code_challenge = hashlib.sha256(pkce_code_verifier.encode("utf-8")).digest()
    pkce_code_challenge = (
        base64.urlsafe_b64encode(pkce_code_challenge).decode("utf-8").replace("=", "")
    )

    # Prepare headers and params
    headers = {"Content-type": "application/x-www-form-urlencoded"}
    if host_header:
        headers["Host"] = host_header

    data = {
        "client_id": client_id,
        "code_challenge_method": "S256",
        "code_challenge": pkce_code_challenge,
    }

    # Add scope if specified
    if scope:
        data["scope"] = scope

    # Get device code
    response = requests.post(
        url=device_endpoint, data=data, headers=headers, allow_redirects=False
    )
    response.raise_for_status()
    device_response = response.json()

    verification_uri_complete = device_response["verification_uri_complete"]
    device_code = device_response["device_code"]

    # Apply endpoint rewrites if needed
    if endpoint_rewrite:
        for original, replacement in endpoint_rewrite.items():
            verification_uri_complete = verification_uri_complete.replace(
                original, replacement
            )

    # Display authentication URL to user
    print(f"Please open this URL to authenticate: {verification_uri_complete}")

    # Poll for token
    while True:
        response = requests.post(
            url=token_endpoint,
            data={
                "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
                "client_id": client_id,
                "device_code": device_code,
                "code_verifier": pkce_code_verifier,
            },
            headers=headers,
            allow_redirects=False,
        )

        if response.status_code == 200:
            print("Authentication successful")
            return response.json()

        # Handle polling errors
        try:
            error_data = response.json()
            if "error" in error_data and error_data["error"] == "authorization_pending":
                print("Waiting for authentication to complete...")
                time.sleep(polling_interval)
                continue
            else:
                # Some other error occurred
                response.raise_for_status()
        except (ValueError, requests.JSONDecodeError):
            # Couldn't parse JSON response
            response.raise_for_status()


if __name__ == "__main__":
    authenticate_device_flow_with_pkce(
        token_endpoint="http://keycloak:8080/realms/iceberg/protocol/openid-connect/token",
        device_endpoint="http://keycloak:8080/realms/iceberg/protocol/openid-connect/auth/device",
        client_id="lakekeeper",
        scope="lakekeeper",
    )
