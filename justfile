set shell := ["bash", "-c"]
set export

RUST_LOG := "debug"

check-format:
	cargo +nightly fmt --all -- --check

check-clippy:
    cargo clippy --no-default-features --all-targets --workspace -- -D warnings
    cargo clippy --all-targets --all-features --workspace -- -D warnings
    cargo clippy -p lakekeeper --no-default-features --workspace -- -D warnings
    cargo clippy -p lakekeeper-io --no-default-features --workspace -- -D warnings
    cargo clippy -p lakekeeper-io --all-features --workspace -- -D warnings

check-cargo-sort:
	cargo sort -c -w

check: check-clippy check-format check-cargo-sort

fix-format:
    cargo +nightly fmt --all
    cargo sort -w

fix:
    cargo clippy --all-targets --all-features --workspace --fix --allow-staged
    cargo +nightly fmt --all
    cargo sort -w

sqlx-prepare:
    cargo sqlx prepare --workspace -- --tests

doc-test:
	cargo test --no-fail-fast --doc --all-features --workspace

unit-test: doc-test
	cargo test --profile ci --lib --all-features --workspace

test: doc-test
	cargo test --all-targets --all-features --workspace

update-rest-openapi:
    # Download from https://raw.githubusercontent.com/apache/iceberg/main/open-api/rest-catalog-open-api.yaml and put into api folder
    curl -o docs/docs/api/rest-catalog-open-api.yaml https://raw.githubusercontent.com/apache/iceberg/main/open-api/rest-catalog-open-api.yaml
    just add-return-uuid-to-rest-openapi
    just add-return-protection-status-to-rest-openapi
    just add-namespace-delete-extension-to-rest-openapi
    # Remove multiple empty lines due to https://github.com/mikefarah/yq/issues/2074
    if [[ "$(uname)" == "Darwin" ]]; then \
      sed -i '' '/^$/N;/^\n$/D' docs/docs/api/rest-catalog-open-api.yaml; \
    else \
      sed -i '/^$/N;/^\n$/D' docs/docs/api/rest-catalog-open-api.yaml; \
    fi

update-openfga:
    bash -c 'BASE_PATH=authz/openfga; \
    LAST_VERSION=$(ls $BASE_PATH | sort -r | head -n 1); \
    fga model transform --file $BASE_PATH/$LAST_VERSION/fga.mod > $BASE_PATH/$LAST_VERSION/schema.json'

test-openfga:
    bash -c 'BASE_PATH=authz/openfga; \
    LAST_VERSION=$(ls $BASE_PATH | sort -r | head -n 1); \
    fga model test --tests $BASE_PATH/$LAST_VERSION/store.fga.yaml'

update-management-openapi:
    LAKEKEEPER__AUTHZ_BACKEND=openfga RUST_LOG=error cargo run management-openapi > docs/docs/api/management-open-api.yaml
    yq -i '.info.version = "0.0.0"' docs/docs/api/management-open-api.yaml

add-return-uuid-to-rest-openapi:
    yq eval '.paths."/v1/{prefix}/namespaces".get.parameters += [{"name": "returnUuids", "in": "query", "description": "If true, include the `namespace-uuids` field in the response", "required": false, "schema": {"type": "boolean", "default": false}}]' -i docs/docs/api/rest-catalog-open-api.yaml
    yq eval '.paths."/v1/{prefix}/namespaces/{namespace}/tables".get.parameters += [{"name": "returnUuids", "in": "query", "description": "If true, include the `table-uuids` field in the response", "required": false, "schema": {"type": "boolean", "default": false}}]' -i docs/docs/api/rest-catalog-open-api.yaml
    yq eval '.paths."/v1/{prefix}/namespaces/{namespace}/views".get.parameters += [{"name": "returnUuids", "in": "query", "description": "If true, include the `table-uuids` field in the response", "required": false, "schema": {"type": "boolean", "default": false}}]' -i docs/docs/api/rest-catalog-open-api.yaml
    yq eval '.paths."/v1/{prefix}/namespaces/{namespace}".get.parameters += [{"name": "returnUuid", "in": "query", "description": "If true, include the `namespace-uuid` field in the response", "required": false, "schema": {"type": "boolean", "default": false}}]' -i docs/docs/api/rest-catalog-open-api.yaml
    yq eval '.components.schemas.ListNamespacesResponse.properties["namespace-uuids"] = {"type": "array", "uniqueItems": true, "nullable": true, "items": {"type": "string"}}' -i docs/docs/api/rest-catalog-open-api.yaml
    yq eval '.components.schemas.GetNamespaceResponse.properties["namespace-uuid"] = {"type": "string", "nullable": true, "type": "string"}' -i docs/docs/api/rest-catalog-open-api.yaml
    yq eval '.components.schemas.ListTablesResponse.properties["table-uuids"] = {"type": "array", "uniqueItems": true, "nullable": true, "items": {"type": "string"}}' -i docs/docs/api/rest-catalog-open-api.yaml

add-return-protection-status-to-rest-openapi:
    yq eval '.paths."/v1/{prefix}/namespaces".get.parameters += [{"name": "returnProtectionStatus", "in": "query", "description": "If true, include the `protection-status` field in the response", "required": false, "schema": {"type": "boolean", "default": false}}]' -i docs/docs/api/rest-catalog-open-api.yaml
    yq eval '.paths."/v1/{prefix}/namespaces/{namespace}/tables".get.parameters += [{"name": "returnProtectionStatus", "in": "query", "description": "If true, include the `protection-status` field in the response", "required": false, "schema": {"type": "boolean", "default": false}}]' -i docs/docs/api/rest-catalog-open-api.yaml
    yq eval '.paths."/v1/{prefix}/namespaces/{namespace}/views".get.parameters += [{"name": "returnProtectionStatus", "in": "query", "description": "If true, include the `protection-status` field in the response", "required": false, "schema": {"type": "boolean", "default": false}}]' -i docs/docs/api/rest-catalog-open-api.yaml
    yq eval '.components.schemas.ListNamespacesResponse.properties["protection-status"] = {"type": "array", "nullable": true, "items": {"type": "boolean"}}' -i docs/docs/api/rest-catalog-open-api.yaml
    yq eval '.components.schemas.ListTablesResponse.properties["protection-status"] = {"type": "array", "nullable": true, "items": {"type": "boolean"}}' -i docs/docs/api/rest-catalog-open-api.yaml


add-namespace-delete-extension-to-rest-openapi:
    yq eval '.paths."/v1/{prefix}/namespaces/{namespace}/tables/{table}".delete.parameters += [{"name": "force", "in": "query", "description": "If true, ignore `protection-status` when dropping.", "required": false, "schema": {"type": "boolean", "default": false}}]' -i docs/docs/api/rest-catalog-open-api.yaml
    yq eval '.paths."/v1/{prefix}/namespaces/{namespace}/views/{view}".delete.parameters += [{"name": "force", "in": "query", "description": "If true, ignore `protection-status` when dropping.", "required": false, "schema": {"type": "boolean", "default": false}}]' -i docs/docs/api/rest-catalog-open-api.yaml
    yq eval '.paths."/v1/{prefix}/namespaces/{namespace}".delete.parameters += [{"name": "force", "in": "query", "description": "If force and recursive are set to true, immediately delete all contents of the namespace without considering soft-delete policies. Force has no effect without recursive=true.", "required": false, "schema": {"type": "boolean", "default": false}}, {"name": "recursive", "in": "query", "description": "Delete a namespace and its contents. This means all tables, views, and namespaces under this namespace will be deleted. The namespace itself will also be deleted. If the warehouse containing the namespace is configured with a soft-deletion profile, the `force` flag has to be provided. The deletion will not be a soft-deletion. Every table, view and namespace will be gone as soon as this call returns. Depending on whether the `purge` flag was set to true, the data will be queued for deletion too. Any pending `tabular_expiration` will be cancelled. If there is a running `tabular_expiration`, this call will fail with a `409 Conflict` error.", "required": false, "schema": {"type": "boolean", "default": false}},{"name": "purge", "in": "query", "description": "If recursive is true, also deletes table and view data. If false, only metadata is dropped from the catalog, table location remains untouched. Defaults to true for all tables managed by Lakekeeper.", "required": false, "schema": {"type": "boolean", "default": true}}]' -i docs/docs/api/rest-catalog-open-api.yaml
    yq eval '.paths."/v1/{prefix}/namespaces/{namespace}".delete.summary = "Drop a namespace from the catalog."' -i docs/docs/api/rest-catalog-open-api.yaml
    yq eval '.paths."/v1/{prefix}/namespaces/{namespace}".delete.description = "Drop a namespace from the catalog. By default, the namespace needs to be empty. You can however set `recursive=true` which will delete all tables, views and namespaces under this namespace. The namespace itself will also be deleted. If the warehouse containing the namespace is configured with a soft-deletion profile, the `force` flag has to be provided. The deletion will not be a soft-deletion. Every table, view and namespace will be gone as soon as this call returns. Depending on whether the `purge` flag was set to true, the data will be queued for deletion too. Any pending `tabular_expiration` will be cancelled. If there is a running `tabular_expiration`, this call will fail with a `409 Conflict` error."' -i docs/docs/api/rest-catalog-open-api.yaml
