# Client Authentication

The Lakekeeper clients — [`pylakekeeper`](generic-tables-pylakekeeper.md) (Python) and the [`lakekeeper-client`](generic-tables-flink.md) (Java, also used from [Spark](generic-tables-spark.md) via py4j) — authenticate to Lakekeeper by sending an OAuth2 **bearer token** on every request. This page covers the **client side**: how to obtain and pass that token. For the **server side** — configuring the OpenID provider, how Lakekeeper validates tokens, and mapping tokens to identities — see [Authentication](authentication.md).

All clients expose the same four strategies behind a common `Auth` interface, so you pick one and hand it to the client; the client attaches the `Authorization` header and refreshes the token as needed.

## Choosing a strategy

| Strategy | Class | Use when |
|---|---|---|
| **Static token** | `StaticToken` | You already have a bearer token, or the target is a no-auth dev stack. |
| **Client credentials** | `ClientCredentials` | A **service** / machine-to-machine job — no human in the loop. The client fetches and refreshes the token. |
| **Device code** | `DeviceCodeFlow` | An interactive **human** login from a CLI, container, or remote kernel — approve a code in a browser on any device (RFC 8628). |
| **Authorization code + PKCE** | `AuthorizationCodeFlow` | An interactive **human** login on your own machine — opens a browser and captures the redirect on a loopback port (RFC 7636). |

!!! tip "Which one?"
    - **Unattended job** (Spark, Flink, cron, CI) → `ClientCredentials`
    - **A person on a headless / remote host** (CLI, container, notebook kernel) → `DeviceCodeFlow`
    - **A person on their own machine** (a browser can reach it) → `AuthorizationCodeFlow`
    - **A pre-issued token or a no-auth dev stack** → `StaticToken`

## Static token

A fixed bearer token — no refresh.

=== "Python"

    ```python
    from pylakekeeper import StaticToken
    auth = StaticToken("my-token")
    ```

=== "Java"

    ```java
    import io.lakekeeper.client.auth.StaticToken;
    var auth = new StaticToken("my-token");
    ```

## Client credentials (service accounts)

The OAuth2 `client_credentials` grant — the client fetches a token and refreshes it before expiry, single-flight under concurrency.

=== "Python"

    ```python
    from pylakekeeper import ClientCredentials

    auth = ClientCredentials(
        token_url="http://keycloak/realms/iceberg/protocol/openid-connect/token",
        client_id="my-service",
        client_secret="...",
        scope="lakekeeper",          # audience-granting scope — see the warning below
    )
    ```

=== "Java"

    ```java
    import io.lakekeeper.client.auth.ClientCredentials;

    var auth = new ClientCredentials(
        "http://keycloak/realms/iceberg/protocol/openid-connect/token",
        "my-service",
        "...",
        "lakekeeper",   // scope (nullable)
        60,             // refreshMarginSeconds
        30);            // timeoutSeconds
    ```

!!! warning "Service accounts need the right audience"
    Lakekeeper rejects a token with `401` unless its `aud` claim matches the configured audience. Many IdPs (e.g. Keycloak) omit that audience for a bare service-account token unless you request the audience-granting **scope** — commonly `scope="lakekeeper"`. If a `client_credentials` token 401s, this is almost always why. See the server-side [Authentication](authentication.md) guide for configuring the audience.

## Device code — CLI / headless / remote kernels

The OAuth2 Device Authorization Grant (RFC 8628). The client shows a URL + code; the user approves it in a browser on any device, and the client polls until they do. No redirect listener needed, so it works on headless and remote hosts.

=== "Python"

    ```python
    from pylakekeeper import DeviceCodeFlow

    auth = DeviceCodeFlow(
        device_authorization_url="http://keycloak/realms/iceberg/protocol/openid-connect/auth/device",
        token_url="http://keycloak/realms/iceberg/protocol/openid-connect/token",
        client_id="lakekeeper",           # a public client — no secret
        scope="openid offline_access",    # offline_access → a refresh token
    )
    # The first request prints a URL + code to approve in a browser, then blocks until you do.
    ```

=== "Java"

    ```java
    import io.lakekeeper.client.auth.DeviceCodeFlow;

    var auth = new DeviceCodeFlow(
        "http://keycloak/realms/iceberg/protocol/openid-connect/auth/device",
        "http://keycloak/realms/iceberg/protocol/openid-connect/token",
        "lakekeeper");   // a public client — no secret
    ```

## Authorization code + PKCE — desktop / notebook

The OAuth2 Authorization Code grant with PKCE (RFC 7636). Opens a browser and captures the redirect on a short-lived loopback HTTP server — best when the code runs on your own machine. PKCE means a public client works with no secret on the machine.

=== "Python"

    ```python
    from pylakekeeper import AuthorizationCodeFlow

    auth = AuthorizationCodeFlow(
        authorization_url="http://keycloak/realms/iceberg/protocol/openid-connect/auth",
        token_url="http://keycloak/realms/iceberg/protocol/openid-connect/token",
        client_id="lakekeeper",           # public client + PKCE — no secret
        scope="openid offline_access",
    )
    # The first request opens your browser and captures the redirect on a loopback port.
    ```

=== "Java"

    ```java
    import io.lakekeeper.client.auth.AuthorizationCodeFlow;

    var auth = new AuthorizationCodeFlow(
        "http://keycloak/realms/iceberg/protocol/openid-connect/auth",
        "http://keycloak/realms/iceberg/protocol/openid-connect/token",
        "lakekeeper");   // public client + PKCE — no secret
    ```

## Refresh & session lifetime

Access tokens are short-lived (often ~1 h). The interactive flows (**device code**, **authorization code**) log the user in **once** and then renew the access token silently with the `refresh_token`, so the session outlives a single access token — a fresh interactive login only happens if the refresh token itself expires or is revoked. `ClientCredentials` simply re-fetches a new token before expiry (no refresh token needed).

Include `offline_access` in the scope if your IdP only issues a refresh token when asked (Keycloak does).

## Passing it to the client

Once you've built an `auth`, hand it to the client:

=== "Python"

    ```python
    from pylakekeeper import Client
    client = Client(base_url="http://localhost:8181", warehouse="my-warehouse-uuid", auth=auth)
    ```

=== "Java"

    ```java
    var client = io.lakekeeper.client.LakekeeperClient.builder()
        .baseUrl("http://localhost:8181")
        .warehouse("my-warehouse-uuid")
        .auth(auth)
        .build();
    ```

From **PySpark**, reach the same Java classes through `spark._jvm.io.lakekeeper.client.auth.*` — see [Apache Spark](generic-tables-spark.md).

## Related

- [Authentication](authentication.md) — server-side: configuring the IdP and how Lakekeeper validates tokens
- [Python Client](generic-tables-pylakekeeper.md) · [Apache Spark](generic-tables-spark.md) · [Apache Flink](generic-tables-flink.md)
- Source & examples: [`lakekeeper-clients` on GitHub](https://github.com/lakekeeper/lakekeeper-clients)
