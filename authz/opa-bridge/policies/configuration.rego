# This file contains all configurations for the Lakekeeper OPA bridge.

# regal ignore:directory-package-mismatch
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
# - url: The URL where OPA can reach the Lakekeeper instance.
# - openid_token_endpoint: The URL of the token endpoint of the identity provider.
#       Example: http://localhost:30080/realms/iceberg/protocol/openid-connect/token
# - client_id: The client ID used for authentication with the IdP (Client Credentials Flow)
# - client_secret: The client secret used for authentication with the IdP (Client Credentials Flow)
# - scope: The scope specified in the client credentials flow
# - max_batch_check_size: Maximum number of checks per batch-check request.
#       Lakekeeper server limit is 1000. Default: 1000
#
# A default project is pre-defined and can be configured via environment variables.
# Additional projects can be added below.
lakekeeper := [{
	"id": "default",
	"url": trim_right(object.get(env, "LAKEKEEPER_URL", "http://localhost:8181"), "/"),
	"openid_token_endpoint": env.LAKEKEEPER_TOKEN_ENDPOINT,
	"client_id": env.LAKEKEEPER_CLIENT_ID,
	"client_secret": env.LAKEKEEPER_CLIENT_SECRET,
	"scope": object.get(env, "LAKEKEEPER_SCOPE", "lakekeeper"),
	"max_batch_check_size": to_number(object.get(env, "LAKEKEEPER_MAX_BATCH_CHECK_SIZE", "1000")),
}]

# ------------- Trino Configuration -------------
# Mapping of trino catalogs to Lakekeeper warehouses.
# Add additional entries for additional trino catalogs.
# Each configuration must contain the following fields:
#
# - name: The name of the catalog in Trino.
# - lakekeeper_id: Id of the Lakekeeper configuration. (Reference to "id" in the "lakekeeper" array)
# - lakekeeper_warehouse: The name of the warehouse in Lakekeeper.

# Allow access to unmanaged catalogs (catalogs not in the trino_catalog array).
# When Trino has multiple authorizers configured, ALL authorizers must allow an action for it to succeed.
# If Trino uses catalogs managed by other authorizers (not Lakekeeper), such as a connected PostgreSQL catalog,
# set this to true to allow this OPA bridge to permit access to those catalogs.
# Default: false
default trino_allow_unmanaged_catalogs := false

trino_allow_unmanaged_catalogs if {
	object.get(env, "TRINO_ALLOW_UNMANAGED_CATALOGS", "false") == "true"
}

# ------------- Trino Admin Users -------------
# Users with full access to all system schemas/tables across all catalogs,
# including FilterViewQueryOwnedBy for all users.
# Specify Trino user IDs (typically OIDC subject).
# Can be set via the TRINO_ADMIN_USERS environment variable as a comma-separated list.
default trino_admin_users := []

trino_admin_users := admin_users if {
	admin_csv := object.get(env, "TRINO_ADMIN_USERS", "")
	admin_csv != ""
	admin_users := [trimmed |
		some entry in split(admin_csv, ",")
		trimmed := trim_space(entry)
		trimmed != ""
	]
}

# A handful commonly used catalogs are pre-defined and can be configured via environment variables.
# All pre-defined catalogs use the "default" lakekeeper instance defined above.
trino_catalog := [
	{
		"name": object.get(env, "TRINO_DEV_CATALOG_NAME", "dev"),
		"lakekeeper_id": "default",
		"lakekeeper_warehouse": object.get(env, "LAKEKEEPER_DEV_WAREHOUSE", "development"),
	},
	{
		"name": object.get(env, "TRINO_PROD_CATALOG_NAME", "prod"),
		"lakekeeper_id": "default",
		"lakekeeper_warehouse": object.get(env, "LAKEKEEPER_PROD_WAREHOUSE", "production"),
	},
	{
		"name": object.get(env, "TRINO_DEMO_CATALOG_NAME", "demo"),
		"lakekeeper_id": "default",
		"lakekeeper_warehouse": object.get(env, "LAKEKEEPER_DEMO_WAREHOUSE", "demo"),
	},
	{
		"name": object.get(env, "TRINO_LAKEKEEPER_CATALOG_NAME", "lakekeeper"),
		"lakekeeper_id": "default",
		"lakekeeper_warehouse": object.get(env, "LAKEKEEPER_LAKEKEEPER_WAREHOUSE", "lakekeeper"),
	},
]
