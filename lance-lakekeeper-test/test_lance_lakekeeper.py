#!/usr/bin/env python3
"""Lance + Lakekeeper integration tests.

Covers five patterns for registering and reading Lance data through Lakekeeper:
  A) Existing data with lance_location property
  B) Catalog-managed locations
  C) Register via fake metadata.json
  D) STS credential vending via Iceberg loadTable
  E) Generic Table API (Polaris proposal)
"""

import json
import sys
import time
import uuid

import lance
import numpy as np
import pyarrow as pa
import pytest
import requests
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType

LAKEKEEPER_URI = "http://localhost:8181"
CATALOG_URI = f"{LAKEKEEPER_URI}/catalog"
WAREHOUSE = "lance-test"
NAMESPACE = "test_ns"

S3_ENDPOINT = "http://localhost:9000"
S3_BUCKET = "lance-test"
S3_ACCESS_KEY = "minioadmin"
S3_SECRET_KEY = "minioadmin"
S3_REGION = "us-east-1"

LANCE_STORAGE_OPTIONS = {
    "aws_access_key_id": S3_ACCESS_KEY,
    "aws_secret_access_key": S3_SECRET_KEY,
    "aws_endpoint": S3_ENDPOINT,
    "aws_region": S3_REGION,
    "allow_http": "true",
}

EXISTING_USERS_PATH = f"s3://{S3_BUCKET}/existing-data/users.lance"
EXISTING_PRODUCTS_PATH = f"s3://{S3_BUCKET}/existing-data/products.lance"

DUMMY_SCHEMA = Schema(NestedField(1, "dummy", StringType(), required=False))

ICEBERG_TO_LANCE_KEY_MAP = {
    "s3.access-key-id": "aws_access_key_id",
    "s3.secret-access-key": "aws_secret_access_key",
    "s3.session-token": "aws_session_token",
    "s3.region": "aws_region",
    "s3.endpoint": "aws_endpoint",
}


def _make_users_table():
    return pa.table({
        "user_id": pa.array([1, 2, 3, 4, 5], type=pa.int64()),
        "name": pa.array(["Alice", "Bob", "Charlie", "Diana", "Eve"], type=pa.utf8()),
        "email": pa.array([
            "alice@example.com", "bob@example.com", "charlie@example.com",
            "diana@example.com", "eve@example.com",
        ], type=pa.utf8()),
        "score": pa.array([95.5, 87.3, 92.1, 78.9, 99.0], type=pa.float64()),
    })


def _make_products_table():
    rng = np.random.default_rng
    embeddings = [rng(i).standard_normal(8).astype(np.float32).tolist() for i in (42, 43, 44)]
    return pa.table({
        "product_id": pa.array([101, 102, 103], type=pa.int64()),
        "product_name": pa.array(["Widget", "Gadget", "Doohickey"], type=pa.utf8()),
        "price": pa.array([9.99, 24.50, 3.75], type=pa.float64()),
        "embedding": pa.array(embeddings, type=pa.list_(pa.float32())),
    })


def _upload_to_s3(path: str, data: bytes, content_type: str = "application/json"):
    import boto3
    s3 = boto3.client(
        "s3", endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY, aws_secret_access_key=S3_SECRET_KEY,
        region_name=S3_REGION,
    )
    assert path.startswith(f"s3://{S3_BUCKET}/")
    key = path[len(f"s3://{S3_BUCKET}/"):]
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=data, ContentType=content_type)


def _make_fake_metadata(table_uuid: str, location: str, properties: dict) -> dict:
    return {
        "format-version": 2,
        "table-uuid": table_uuid,
        "location": location,
        "last-sequence-number": 0,
        "last-updated-ms": int(time.time() * 1000),
        "last-column-id": 1,
        "current-schema-id": 0,
        "schemas": [{
            "schema-id": 0, "type": "struct",
            "fields": [{"id": 1, "name": "dummy", "type": "string", "required": False}],
        }],
        "default-spec-id": 0,
        "partition-specs": [{"spec-id": 0, "fields": []}],
        "last-partition-id": 999,
        "default-sort-order-id": 0,
        "sort-orders": [{"order-id": 0, "fields": []}],
        "properties": properties,
        "current-snapshot-id": -1,
        "snapshots": [],
    }


def _iceberg_creds_to_lance_storage_options(storage_credentials, config):
    opts = {"allow_http": "true"}
    for cred in (storage_credentials or []):
        for k, v in cred.get("config", {}).items():
            if k in ICEBERG_TO_LANCE_KEY_MAP:
                opts[ICEBERG_TO_LANCE_KEY_MAP[k]] = v
    for k, v in (config or {}).items():
        if k in ICEBERG_TO_LANCE_KEY_MAP:
            opts[ICEBERG_TO_LANCE_KEY_MAP[k]] = v
    return opts


def _get_warehouse_prefix(warehouse_name):
    r = requests.get(f"{CATALOG_URI}/v1/config", params={"warehouse": warehouse_name}, timeout=10)
    r.raise_for_status()
    data = r.json()
    overrides = data.get("overrides", {})
    defaults = data.get("defaults", {})
    return overrides.get("prefix", defaults.get("prefix", warehouse_name))


def _create_sts_warehouse(name, key_prefix):
    config = {
        "warehouse-name": name,
        "project-id": "00000000-0000-0000-0000-000000000000",
        "storage-profile": {
            "type": "s3", "bucket": S3_BUCKET, "region": S3_REGION,
            "path-style-access": True, "endpoint": "http://minio:9000",
            "sts-enabled": True, "flavor": "minio", "key-prefix": key_prefix,
        },
        "storage-credential": {
            "type": "s3", "credential-type": "access-key",
            "aws-access-key-id": S3_ACCESS_KEY, "aws-secret-access-key": S3_SECRET_KEY,
        },
    }
    r = requests.post(f"{LAKEKEEPER_URI}/management/v1/warehouse", json=config, timeout=10)
    assert r.status_code in (200, 201, 409), f"Warehouse creation failed: {r.status_code} {r.text}"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session", autouse=True)
def bootstrap_lakekeeper():
    deadline = time.time() + 120
    while time.time() < deadline:
        try:
            r = requests.get(f"{LAKEKEEPER_URI}/health", timeout=5)
            if r.status_code == 200:
                break
        except requests.ConnectionError:
            pass
        time.sleep(2)
    else:
        pytest.fail("Lakekeeper did not become healthy within timeout")

    r = requests.post(
        f"{LAKEKEEPER_URI}/management/v1/bootstrap",
        json={"accept-terms-of-use": True}, timeout=10,
    )
    assert r.status_code in (200, 204, 409)

    warehouse_config = {
        "warehouse-name": WAREHOUSE,
        "project-id": "00000000-0000-0000-0000-000000000000",
        "storage-profile": {
            "type": "s3", "bucket": S3_BUCKET, "region": S3_REGION,
            "path-style-access": True, "endpoint": "http://minio:9000",
            "sts-enabled": False, "flavor": "minio", "key-prefix": "warehouse",
        },
        "storage-credential": {
            "type": "s3", "credential-type": "access-key",
            "aws-access-key-id": S3_ACCESS_KEY, "aws-secret-access-key": S3_SECRET_KEY,
        },
    }
    r = requests.post(f"{LAKEKEEPER_URI}/management/v1/warehouse", json=warehouse_config, timeout=10)
    assert r.status_code in (200, 201, 409), f"Warehouse creation failed: {r.status_code} {r.text}"


@pytest.fixture(scope="session")
def catalog():
    return load_catalog("lakekeeper", **{
        "type": "rest", "uri": CATALOG_URI, "warehouse": WAREHOUSE,
        "s3.endpoint": S3_ENDPOINT, "s3.access-key-id": S3_ACCESS_KEY,
        "s3.secret-access-key": S3_SECRET_KEY, "s3.region": S3_REGION,
        "s3.path-style-access": "true",
    })


@pytest.fixture()
def ns(catalog, request):
    """Create a namespace and drop it after the test."""
    name = f"test_{request.node.name}_{uuid.uuid4().hex[:8]}"
    catalog.create_namespace(name)
    yield name
    for t in catalog.list_tables(name):
        try:
            catalog.drop_table(t)
        except Exception:
            pass
    try:
        catalog.drop_namespace(name)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Pattern A — Register existing Lance data via lance_location property
# ---------------------------------------------------------------------------

class TestPatternA:
    def test_write_and_register(self, catalog, ns):
        lance.write_dataset(_make_users_table(), EXISTING_USERS_PATH, mode="overwrite", storage_options=LANCE_STORAGE_OPTIONS)
        lance.write_dataset(_make_products_table(), EXISTING_PRODUCTS_PATH, mode="overwrite", storage_options=LANCE_STORAGE_OPTIONS)

        for name, path in [("users", EXISTING_USERS_PATH), ("products", EXISTING_PRODUCTS_PATH)]:
            tbl = catalog.create_table(
                f"{ns}.{name}", schema=DUMMY_SCHEMA,
                properties={"table_type": "lance", "lance_location": path},
            )
            # Lakekeeper assigns its own location, different from real path
            assert tbl.location().rstrip("/") != path.rstrip("/")

    def test_discover_via_catalog(self, catalog, ns):
        lance.write_dataset(_make_users_table(), EXISTING_USERS_PATH, mode="overwrite", storage_options=LANCE_STORAGE_OPTIONS)
        lance.write_dataset(_make_products_table(), EXISTING_PRODUCTS_PATH, mode="overwrite", storage_options=LANCE_STORAGE_OPTIONS)
        for name, path in [("users", EXISTING_USERS_PATH), ("products", EXISTING_PRODUCTS_PATH)]:
            catalog.create_table(
                f"{ns}.{name}", schema=DUMMY_SCHEMA,
                properties={"table_type": "lance", "lance_location": path},
            )

        tables = catalog.list_tables(ns)
        assert {t[-1] for t in tables} == {"users", "products"}

        for table_id in tables:
            tbl = catalog.load_table(table_id)
            assert tbl.properties.get("table_type") == "lance"
            assert "lance_location" in tbl.properties

    def test_query_via_lance_location(self, catalog, ns):
        lance.write_dataset(_make_users_table(), EXISTING_USERS_PATH, mode="overwrite", storage_options=LANCE_STORAGE_OPTIONS)
        lance.write_dataset(_make_products_table(), EXISTING_PRODUCTS_PATH, mode="overwrite", storage_options=LANCE_STORAGE_OPTIONS)
        for name, path in [("users", EXISTING_USERS_PATH), ("products", EXISTING_PRODUCTS_PATH)]:
            catalog.create_table(
                f"{ns}.{name}", schema=DUMMY_SCHEMA,
                properties={"table_type": "lance", "lance_location": path},
            )

        for table_id in catalog.list_tables(ns):
            tbl = catalog.load_table(table_id)
            ds = lance.dataset(tbl.properties["lance_location"], storage_options=LANCE_STORAGE_OPTIONS)
            df = ds.to_table().to_pandas()
            if table_id[-1] == "users":
                assert len(df) == 5
                assert "Alice" in df["name"].values
            elif table_id[-1] == "products":
                assert len(df) == 3
                assert "Widget" in df["product_name"].values


# ---------------------------------------------------------------------------
# Pattern B — Catalog-managed locations
# ---------------------------------------------------------------------------

class TestPatternB:
    def test_write_to_catalog_location(self, catalog, ns):
        registered = {}
        for name in ["users", "products"]:
            tbl = catalog.create_table(
                f"{ns}.{name}", schema=DUMMY_SCHEMA,
                properties={"table_type": "lance"},
            )
            registered[name] = tbl.location()

        lance.write_dataset(_make_users_table(), registered["users"], storage_options=LANCE_STORAGE_OPTIONS)
        lance.write_dataset(_make_products_table(), registered["products"], storage_options=LANCE_STORAGE_OPTIONS)

        for name, loc in registered.items():
            ds = lance.dataset(loc, storage_options=LANCE_STORAGE_OPTIONS)
            df = ds.to_table().to_pandas()
            if name == "users":
                assert len(df) == 5
                assert "Alice" in df["name"].values
                assert abs(df[df["name"] == "Alice"]["score"].iloc[0] - 95.5) < 0.01
            elif name == "products":
                assert len(df) == 3
                emb = df["embedding"].iloc[0]
                assert isinstance(emb, (list, np.ndarray)) and len(emb) == 8

    def test_discover_via_catalog(self, catalog, ns):
        for name in ["users", "products"]:
            tbl = catalog.create_table(
                f"{ns}.{name}", schema=DUMMY_SCHEMA,
                properties={"table_type": "lance"},
            )
            lance.write_dataset(
                _make_users_table() if name == "users" else _make_products_table(),
                tbl.location(), storage_options=LANCE_STORAGE_OPTIONS,
            )

        tables = catalog.list_tables(ns)
        assert {t[-1] for t in tables} == {"users", "products"}
        for table_id in tables:
            tbl = catalog.load_table(table_id)
            assert tbl.properties.get("table_type") == "lance"


# ---------------------------------------------------------------------------
# Pattern C — Register via fake metadata.json
# ---------------------------------------------------------------------------

class TestPatternC:
    def _register_via_metadata(self, catalog, ns, name, lance_path):
        wh_prefix = f"s3://{S3_BUCKET}/warehouse"
        table_uuid = str(uuid.uuid4())
        metadata = _make_fake_metadata(table_uuid, lance_path, {"table_type": "lance"})
        metadata_loc = f"{wh_prefix}/register-test/{name}-{uuid.uuid4().hex[:8]}/v1.metadata.json"
        _upload_to_s3(metadata_loc, json.dumps(metadata, indent=2).encode("utf-8"))
        return catalog.register_table(f"{ns}.{name}", metadata_loc)

    def test_register_preserves_location(self, catalog, ns):
        wh_prefix = f"s3://{S3_BUCKET}/warehouse"
        users_path = f"{wh_prefix}/register-test/users-{uuid.uuid4().hex[:8]}.lance"
        products_path = f"{wh_prefix}/register-test/products-{uuid.uuid4().hex[:8]}.lance"

        lance.write_dataset(_make_users_table(), users_path, storage_options=LANCE_STORAGE_OPTIONS)
        lance.write_dataset(_make_products_table(), products_path, storage_options=LANCE_STORAGE_OPTIONS)

        for name, path in [("users", users_path), ("products", products_path)]:
            tbl = self._register_via_metadata(catalog, ns, name, path)
            assert tbl.location().rstrip("/") == path.rstrip("/")

    def test_query_through_registered_location(self, catalog, ns):
        wh_prefix = f"s3://{S3_BUCKET}/warehouse"
        users_path = f"{wh_prefix}/register-test/users-{uuid.uuid4().hex[:8]}.lance"
        lance.write_dataset(_make_users_table(), users_path, storage_options=LANCE_STORAGE_OPTIONS)
        self._register_via_metadata(catalog, ns, "users", users_path)

        tbl = catalog.load_table(f"{ns}.users")
        assert tbl.properties.get("table_type") == "lance"
        ds = lance.dataset(tbl.location(), storage_options=LANCE_STORAGE_OPTIONS)
        df = ds.to_table().to_pandas()
        assert len(df) == 5
        assert "Alice" in df["name"].values


# ---------------------------------------------------------------------------
# Pattern D — STS credential vending via Iceberg loadTable
# ---------------------------------------------------------------------------

class TestPatternD:
    STS_WAREHOUSE = "lance-sts-test"
    WH_PREFIX = f"s3://{S3_BUCKET}/sts-warehouse"

    @pytest.fixture(autouse=True, scope="class")
    def setup_sts_warehouse(self):
        _create_sts_warehouse(self.STS_WAREHOUSE, "sts-warehouse")
        TestPatternD._prefix = _get_warehouse_prefix(self.STS_WAREHOUSE)
        TestPatternD._sts_catalog = load_catalog("lakekeeper-sts", **{
            "type": "rest", "uri": CATALOG_URI, "warehouse": self.STS_WAREHOUSE,
            "s3.endpoint": S3_ENDPOINT, "s3.access-key-id": S3_ACCESS_KEY,
            "s3.secret-access-key": S3_SECRET_KEY, "s3.region": S3_REGION,
            "s3.path-style-access": "true",
        })

    def _register_lance_table(self, ns_name, table_name, lance_path):
        table_uuid = str(uuid.uuid4())
        metadata = _make_fake_metadata(table_uuid, lance_path, {"table_type": "lance"})
        metadata_loc = f"{self.WH_PREFIX}/lance-data/{table_name}-{uuid.uuid4().hex[:8]}/v1.metadata.json"
        _upload_to_s3(metadata_loc, json.dumps(metadata, indent=2).encode("utf-8"))
        self._sts_catalog.create_namespace(ns_name)
        return self._sts_catalog.register_table(f"{ns_name}.{table_name}", metadata_loc)

    def test_vended_credentials_present(self):
        ns_name = f"sts_d_{uuid.uuid4().hex[:8]}"
        lance_path = f"{self.WH_PREFIX}/lance-data/users-{uuid.uuid4().hex[:8]}.lance"
        lance.write_dataset(_make_users_table(), lance_path, storage_options=LANCE_STORAGE_OPTIONS)
        self._register_lance_table(ns_name, "users", lance_path)

        load_url = f"{CATALOG_URI}/v1/{self._prefix}/namespaces/{ns_name}/tables/users"
        r = requests.get(load_url, headers={"X-Iceberg-Access-Delegation": "vended-credentials"}, timeout=10)
        assert r.status_code == 200

        resp = r.json()
        storage_creds = resp.get("storage-credentials", [])
        assert len(storage_creds) > 0
        cred_config = storage_creds[0].get("config", {})
        assert "s3.access-key-id" in cred_config
        assert "s3.secret-access-key" in cred_config
        assert "s3.session-token" in cred_config

        # cleanup
        self._sts_catalog.drop_table(f"{ns_name}.users")
        self._sts_catalog.drop_namespace(ns_name)

    def test_read_with_vended_credentials(self):
        ns_name = f"sts_d_{uuid.uuid4().hex[:8]}"
        lance_path = f"{self.WH_PREFIX}/lance-data/users-{uuid.uuid4().hex[:8]}.lance"
        lance.write_dataset(_make_users_table(), lance_path, storage_options=LANCE_STORAGE_OPTIONS)
        self._register_lance_table(ns_name, "users", lance_path)

        load_url = f"{CATALOG_URI}/v1/{self._prefix}/namespaces/{ns_name}/tables/users"
        r = requests.get(load_url, headers={"X-Iceberg-Access-Delegation": "vended-credentials"}, timeout=10)
        resp = r.json()

        cred_config = resp["storage-credentials"][0]["config"]
        temp_opts = {
            "aws_access_key_id": cred_config["s3.access-key-id"],
            "aws_secret_access_key": cred_config["s3.secret-access-key"],
            "aws_session_token": cred_config["s3.session-token"],
            "aws_endpoint": S3_ENDPOINT,
            "aws_region": S3_REGION,
            "allow_http": "true",
        }

        ds = lance.dataset(lance_path, storage_options=temp_opts)
        df = ds.to_table().to_pandas()
        assert len(df) == 5
        assert "Alice" in df["name"].values

        self._sts_catalog.drop_table(f"{ns_name}.users")
        self._sts_catalog.drop_namespace(ns_name)


# ---------------------------------------------------------------------------
# Pattern E — Generic Table API
# ---------------------------------------------------------------------------

class TestPatternE:
    STS_WAREHOUSE = "lance-generic-test"
    WH_PREFIX = f"s3://{S3_BUCKET}/generic-warehouse"

    @pytest.fixture(autouse=True, scope="class")
    def setup_generic_warehouse(self):
        _create_sts_warehouse(self.STS_WAREHOUSE, "generic-warehouse")
        TestPatternE._prefix = _get_warehouse_prefix(self.STS_WAREHOUSE)

    def _create_namespace(self, ns_name):
        r = requests.post(
            f"{CATALOG_URI}/v1/{self._prefix}/namespaces",
            json={"namespace": [ns_name]}, timeout=10,
        )
        assert r.status_code in (200, 409)

    def _generic_table_url(self, ns_name, table_name=None):
        base = f"{LAKEKEEPER_URI}/v1/{self._prefix}/namespaces/{ns_name}/generic-tables"
        return f"{base}/{table_name}" if table_name else base

    def test_create_returns_table_data(self):
        ns_name = f"gen_{uuid.uuid4().hex[:8]}"
        lance_path = f"{self.WH_PREFIX}/lance-data/users-{uuid.uuid4().hex[:8]}.lance"
        lance.write_dataset(_make_users_table(), lance_path, storage_options=LANCE_STORAGE_OPTIONS)

        self._create_namespace(ns_name)
        r = requests.post(self._generic_table_url(ns_name), json={
            "name": "users", "format": "lance", "base-location": lance_path,
            "doc": "test table", "properties": {"lance.version": "0.20"},
        }, timeout=10)
        assert r.status_code == 200

        resp = r.json()
        assert resp["table"]["name"] == "users"
        assert resp["table"]["format"] == "lance"
        assert resp["table"]["base-location"].rstrip("/") == lance_path.rstrip("/")

        requests.delete(self._generic_table_url(ns_name, "users"), timeout=10)
        requests.delete(f"{CATALOG_URI}/v1/{self._prefix}/namespaces/{ns_name}", timeout=10)

    def test_list_returns_identifiers(self):
        ns_name = f"gen_{uuid.uuid4().hex[:8]}"
        lance_path = f"{self.WH_PREFIX}/lance-data/users-{uuid.uuid4().hex[:8]}.lance"
        lance.write_dataset(_make_users_table(), lance_path, storage_options=LANCE_STORAGE_OPTIONS)

        self._create_namespace(ns_name)
        requests.post(self._generic_table_url(ns_name), json={
            "name": "users", "format": "lance", "base-location": lance_path,
        }, timeout=10)

        r = requests.get(self._generic_table_url(ns_name), timeout=10)
        assert r.status_code == 200
        resp = r.json()
        assert len(resp["identifiers"]) == 1
        # Pagination always returns a cursor token for the last row (keyset pagination).
        # An empty next page simply returns no results.
        assert "next-page-token" in resp

        entry = resp["identifiers"][0]
        assert entry["namespace"] == [ns_name]
        assert entry["name"] == "users"
        assert entry["format"] == "lance"
        assert "id" in entry

        requests.delete(self._generic_table_url(ns_name, "users"), timeout=10)
        requests.delete(f"{CATALOG_URI}/v1/{self._prefix}/namespaces/{ns_name}", timeout=10)

    def test_load_with_credential_vending(self):
        ns_name = f"gen_{uuid.uuid4().hex[:8]}"
        lance_path = f"{self.WH_PREFIX}/lance-data/users-{uuid.uuid4().hex[:8]}.lance"
        lance.write_dataset(_make_users_table(), lance_path, storage_options=LANCE_STORAGE_OPTIONS)

        self._create_namespace(ns_name)
        requests.post(self._generic_table_url(ns_name), json={
            "name": "users", "format": "lance", "base-location": lance_path,
        }, timeout=10)

        r = requests.get(
            self._generic_table_url(ns_name, "users"),
            headers={"X-Iceberg-Access-Delegation": "vended-credentials"}, timeout=10,
        )
        assert r.status_code == 200
        resp = r.json()

        assert resp["table"]["name"] == "users"
        assert resp["table"]["format"] == "lance"

        storage_creds = resp.get("storage-credentials", [])
        assert len(storage_creds) > 0
        cred_config = storage_creds[0]["config"]
        assert "s3.access-key-id" in cred_config
        assert "s3.secret-access-key" in cred_config
        assert "s3.session-token" in cred_config

        temp_opts = _iceberg_creds_to_lance_storage_options(storage_creds, resp.get("config"))
        temp_opts["aws_endpoint"] = S3_ENDPOINT
        temp_opts["aws_region"] = S3_REGION
        temp_opts["allow_http"] = "true"

        ds = lance.dataset(resp["table"]["base-location"], storage_options=temp_opts)
        df = ds.to_table().to_pandas()
        assert len(df) == 5
        assert "Alice" in df["name"].values

        requests.delete(self._generic_table_url(ns_name, "users"), timeout=10)
        requests.delete(f"{CATALOG_URI}/v1/{self._prefix}/namespaces/{ns_name}", timeout=10)

    def test_drop_removes_from_listing(self):
        ns_name = f"gen_{uuid.uuid4().hex[:8]}"
        lance_path = f"{self.WH_PREFIX}/lance-data/users-{uuid.uuid4().hex[:8]}.lance"
        lance.write_dataset(_make_users_table(), lance_path, storage_options=LANCE_STORAGE_OPTIONS)

        self._create_namespace(ns_name)
        requests.post(self._generic_table_url(ns_name), json={
            "name": "users", "format": "lance", "base-location": lance_path,
        }, timeout=10)

        r = requests.delete(self._generic_table_url(ns_name, "users"), timeout=10)
        assert r.status_code == 204

        # data should survive (no purge)
        ds = lance.dataset(lance_path, storage_options=LANCE_STORAGE_OPTIONS)
        assert ds.to_table().to_pandas().shape[0] == 5

        # table should be gone from listing
        r = requests.get(self._generic_table_url(ns_name), timeout=10)
        assert r.status_code == 200
        assert len(r.json()["identifiers"]) == 0

        requests.delete(f"{CATALOG_URI}/v1/{self._prefix}/namespaces/{ns_name}", timeout=10)
