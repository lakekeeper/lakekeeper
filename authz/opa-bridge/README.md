# Lakekeeper Open Policy Agent (OPA) Bridge
The Lakekeeper OPA bridge is a gateway that allows trusted query engines to use Lakekeeper permissions via OPA. This allows query engine such as trino, which is shared by multiple users, to enforce permissions for individual users on objects governed by Lakekeeper. Currently trino is the only supported query engine, but we hope to see broader adoption in the future.

You can start OPA with:
```bash
cd policies
opa run --server --v1-compatible --log-format text --log-level debug --addr 0.0.0.0:38181 .
```
OPA is know listening on port `38181`.

The following files / folders are part of the bridge:
* `inputs`: Contains a example requests from trino that enable UI autocompletion when developing the policies
* `policies`: The main policy folder
* `policies/configuration.rego`: Contains all configurations for this OPA setup. Configurations can be changed in the file itself or via environment variables. Please check the file itself and our [Documentation](https://docs.lakekeeper.io/docs/nightly/opa) for more information.
* `policies/lakekeeper`: Contains means to authenticate to Lakekeeper via Client Credentials as well as functions to easily query Lakekeeper's `/management/v1/permissions/check` for permissions. This folder contains no query engine specific assumptions or rules.
* `policies/trino`: Trino specific configurations, mainly mapping trino to Lakekeeper permissions as well as converting dot-separated trino schemas to Iceberg REST namespace arrays.

For configuration options please check `policies/configuration.rego`.
Further Readings:
* [Documentation](https://docs.lakekeeper.io/docs/nightly/opa)
* [Docker Compose Example](https://github.com/lakekeeper/lakekeeper/tree/main/examples/access-control-advanced)
