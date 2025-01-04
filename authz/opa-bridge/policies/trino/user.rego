package trino

trino_user_id := input.context.identity.user
lakekeeper_user_id := concat("", ["oidc~", trino_user_id])
