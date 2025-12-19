# This file contains all configurations for the Lakekeeper OPA bridge.

package configuration

env := opa.runtime().env

# ------------- Lakekeeper Configuration -------------
# Define lakekeeper instances.
#
# The first instance can be configured via environment variables.
# Each configuration must contain the following fields:
# 
# - id: Id of this lakekeeper instance. 
#       Has no relevance except as an internal OPA identifier. 
#       Used to reference this instance in the `trino_catalog` configuration below.
# - lakekeeper_url: The URL where OPA can reach the Lakekeeper instance.
# - idp_token_endpoint: The URL of the token endpoint of the identity provider. 
#       Example: http://localhost:30080/realms/iceberg/protocol/openid-connect/token
# - client_id: The client ID used for authentication with the IdP (Client Credentials Flow)
# - client_secret: The client secret used for authentication with the IdP (Client Credentials Flow)
# - scope: The scope specified in the client credentials flow
#
# A default project is pre-defined and can be configured via environment variables.
# Additional projects can be added below.
lakekeeper := [
    {
        "id": "default",
        "url": trim_right(object.get(env, "LAKEKEEPER_URL", "http://localhost:8181"), "/"),
        "openid_token_endpoint": env["LAKEKEEPER_TOKEN_ENDPOINT"],
        "client_id": env["LAKEKEEPER_CLIENT_ID"],
        "client_secret": env["LAKEKEEPER_CLIENT_SECRET"],
        "scope": object.get(env, "LAKEKEEPER_SCOPE", "lakekeeper")
    }
]

# ------------- Trino Configuration -------------
# Mapping of trino catalogs to Lakekeeper warehouses.
# Add additional entries for additional trino catalogs.
# Each configuration must contain the following fields:
#
# - name: The name of the catalog in Trino.
# - lakekeeper_id: The name of the Lakekeeper project that manages the warehouse. (Reference to "id" field in the "lakekeeper" array above)
# - lakekeeper_warehouse: The name of the warehouse in Lakekeeper.

# Allow access to unmanaged catalogs (catalogs not in the trino_catalog array).
# When Trino has multiple authorizers configured, ALL authorizers must allow an action for it to succeed.
# If Trino uses catalogs managed by other authorizers (not Lakekeeper), such as a connected PostgreSQL catalog,
# set this to true to allow this OPA bridge to permit access to those catalogs.
# Default: false
trino_allow_unmanaged_catalogs := object.get(env, "TRINO_ALLOW_UNMANAGED_CATALOGS", "false") == "true"

# A handful commonly used catalogs are pre-defined and can be configured via environment variables.
# All pre-defined catalogs use the "default" lakekeeper instance defined above.
trino_catalog := [
    {
        "name": object.get(env, "TRINO_DEV_CATALOG_NAME", "dev"),
        "lakekeeper_id": "default",
        "lakekeeper_warehouse": object.get(env, "LAKEKEEPER_DEV_WAREHOUSE", "development")
    },
    {
        "name": object.get(env, "TRINO_PROD_CATALOG_NAME", "prod"),
        "lakekeeper_id": "default",
        "lakekeeper_warehouse": object.get(env, "LAKEKEEPER_PROD_WAREHOUSE", "production")
    },
    {
        "name": object.get(env, "TRINO_DEMO_CATALOG_NAME", "demo"),
        "lakekeeper_id": "default",
        "lakekeeper_warehouse": object.get(env, "LAKEKEEPER_DEMO_WAREHOUSE", "demo")
    },
    {
        "name": object.get(env, "TRINO_LAKEKEEPER_CATALOG_NAME", "lakekeeper"),
        "lakekeeper_id": "default",
        "lakekeeper_warehouse": object.get(env, "LAKEKEEPER_LAKEKEEPER_WAREHOUSE", "lakekeeper")
    }
]
