set shell := ["bash", "-c"]
set export

RUST_LOG := "debug"

check-format:
	cargo fmt --all -- --check

check-clippy:
	cargo clippy --all-targets --all-features --workspace -- -D warnings

check-cargo-sort:
	cargo install cargo-sort
	cargo sort -c -w

check: check-format check-clippy check-cargo-sort

fix:
    cargo clippy --all-targets --all-features --workspace --fix --allow-staged
    cargo fmt --all
    cargo sort -w

sqlx-prepare:
    cargo sqlx prepare --workspace -- --tests

doc-test:
	cargo test --no-fail-fast --doc --all-features --workspace

unit-test: doc-test
	cargo test --no-fail-fast --lib --all-features --workspace

test: doc-test
	cargo test --no-fail-fast --all-targets --all-features --workspace

update-rest-openapi:
    # Download from https://raw.githubusercontent.com/apache/iceberg/main/open-api/rest-catalog-open-api.yaml and put into openapi folder
    curl -o openapi/rest-catalog-open-api.yaml https://raw.githubusercontent.com/apache/iceberg/main/open-api/rest-catalog-open-api.yaml

update-openfga:
    fga model transform --file authz/openfga/v1/schema.fga > authz/openfga/v1/schema.json

update-management-openapi:
    LAKEKEEPER__AUTHZ_BACKEND=openfga RUST_LOG=error cargo run management-openapi > openapi/management-open-api.yaml
