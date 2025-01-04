# Lakekeeper Open Policy Agent (OPA) Bridge
The Lakekeeper OPA bridge is a gateway that allows trusted query engines to use Lakekeeper permissions via OPA. This allows query engine such as trino, which is shared by multiple users, to enforce permissions for individual users on objects governed by Lakekeeper. Currently trino is the only supported query engine, but we hope to see broader adoption in the future.

For configuration options please check `policies/configuration.rego`