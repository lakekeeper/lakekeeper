set shell := ["bash", "-c"]
set export

RUST_LOG := "debug"

check-format:
	cargo +nightly fmt --all -- --check

check-clippy:
	cargo clippy --all-targets --all-features --workspace -- -D warnings

check-cargo-sort:
	cargo sort -c -w

check: check-format check-clippy check-cargo-sort

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
    yq eval '.paths."/v1/{prefix}/namespaces/{namespace}".get.parameters += [{"name": "returnUuid", "in": "query", "description": "If true, include the `namespace-uuid` field in the response", "required": false, "schema": {"type": "boolean", "default": false}}]' -i docs/docs/api/rest-catalog-open-api.yaml
    yq eval '.components.schemas.ListNamespacesResponse.properties["namespace-uuids"] = {"type": "array", "uniqueItems": true, "nullable": true, "items": {"type": "string"}}' -i docs/docs/api/rest-catalog-open-api.yaml
    yq eval '.components.schemas.GetNamespaceResponse.properties["namespace-uuid"] = {"type": "string", "nullable": true, "type": "string"}' -i docs/docs/api/rest-catalog-open-api.yaml
    yq eval '.components.schemas.ListTablesResponse.properties["table-uuids"] = {"type": "array", "uniqueItems": true, "nullable": true, "items": {"type": "string"}}' -i docs/docs/api/rest-catalog-open-api.yaml
