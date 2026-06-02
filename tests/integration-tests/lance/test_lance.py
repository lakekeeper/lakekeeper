#!/usr/bin/env python3
"""Lance + Lakekeeper integration tests via the Generic Table API."""

import os
import time
import uuid

import lance
import pyarrow as pa
import pytest
import requests

LAKEKEEPER_URI = os.environ.get("LAKEKEEPER_URI", "http://localhost:8181")
CATALOG_URI = f"{LAKEKEEPER_URI}/catalog"

S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://localhost:9000")
S3_BUCKET = os.environ.get("S3_BUCKET", "lance-test")
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY", "minioadmin")
S3_REGION = os.environ.get("S3_REGION", "us-east-1")
S3_INTERNAL_ENDPOINT = os.environ.get("S3_INTERNAL_ENDPOINT", "http://minio:9000")

LANCE_STORAGE_OPTIONS = {
    "aws_access_key_id": S3_ACCESS_KEY,
    "aws_secret_access_key": S3_SECRET_KEY,
    "aws_endpoint": S3_ENDPOINT,
    "aws_region": S3_REGION,
    "allow_http": "true",
}

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
        "score": pa.array([95.5, 87.3, 92.1, 78.9, 99.0], type=pa.float64()),
    })


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
            "path-style-access": True, "endpoint": S3_INTERNAL_ENDPOINT,
            "sts-enabled": True, "flavor": "minio", "key-prefix": key_prefix,
        },
        "storage-credential": {
            "type": "s3", "credential-type": "access-key",
            "aws-access-key-id": S3_ACCESS_KEY, "aws-secret-access-key": S3_SECRET_KEY,
        },
    }
    r = requests.post(f"{LAKEKEEPER_URI}/management/v1/warehouse", json=config, timeout=10)
    assert r.status_code in (200, 201, 409), f"Warehouse creation failed: {r.status_code} {r.text}"


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


class TestGenericTableApi:
    WAREHOUSE = "lance-generic-test"
    WH_PREFIX = f"s3://{S3_BUCKET}/generic-warehouse"

    @pytest.fixture(autouse=True, scope="class")
    def setup_warehouse(self):
        _create_sts_warehouse(self.WAREHOUSE, "generic-warehouse")
        TestGenericTableApi._prefix = _get_warehouse_prefix(self.WAREHOUSE)

    def _create_namespace(self, ns_name):
        r = requests.post(
            f"{CATALOG_URI}/v1/{self._prefix}/namespaces",
            json={"namespace": [ns_name]}, timeout=10,
        )
        assert r.status_code in (200, 409)

    def _generic_table_url(self, ns_name, table_name=None):
        base = f"{LAKEKEEPER_URI}/lakekeeper/v1/{self._prefix}/namespaces/{ns_name}/generic-tables"
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

        # Underlying data survives the catalog drop (no purge requested).
        ds = lance.dataset(lance_path, storage_options=LANCE_STORAGE_OPTIONS)
        assert ds.to_table().to_pandas().shape[0] == 5

        r = requests.get(self._generic_table_url(ns_name), timeout=10)
        assert r.status_code == 200
        assert all(e["name"] != "users" for e in r.json()["identifiers"])

        requests.delete(f"{CATALOG_URI}/v1/{self._prefix}/namespaces/{ns_name}", timeout=10)

    def test_rename_moves_table(self):
        ns_name = f"gen_{uuid.uuid4().hex[:8]}"
        lance_path = f"{self.WH_PREFIX}/lance-data/users-{uuid.uuid4().hex[:8]}.lance"
        lance.write_dataset(_make_users_table(), lance_path, storage_options=LANCE_STORAGE_OPTIONS)

        self._create_namespace(ns_name)
        requests.post(self._generic_table_url(ns_name), json={
            "name": "users", "format": "lance", "base-location": lance_path,
        }, timeout=10)

        r = requests.post(
            f"{LAKEKEEPER_URI}/lakekeeper/v1/{self._prefix}/generic-tables/rename",
            json={
                "source": {"namespace": [ns_name], "name": "users"},
                "destination": {"namespace": [ns_name], "name": "users_renamed"},
            },
            timeout=10,
        )
        assert r.status_code == 204

        r = requests.get(self._generic_table_url(ns_name, "users"), timeout=10)
        assert r.status_code == 404

        r = requests.get(self._generic_table_url(ns_name, "users_renamed"), timeout=10)
        assert r.status_code == 200
        assert r.json()["table"]["base-location"].rstrip("/") == lance_path.rstrip("/")

        requests.delete(self._generic_table_url(ns_name, "users_renamed"), timeout=10)
        requests.delete(f"{CATALOG_URI}/v1/{self._prefix}/namespaces/{ns_name}", timeout=10)

    def test_credentials_endpoint_returns_vended_creds(self):
        ns_name = f"gen_{uuid.uuid4().hex[:8]}"
        lance_path = f"{self.WH_PREFIX}/lance-data/users-{uuid.uuid4().hex[:8]}.lance"
        lance.write_dataset(_make_users_table(), lance_path, storage_options=LANCE_STORAGE_OPTIONS)

        self._create_namespace(ns_name)
        requests.post(self._generic_table_url(ns_name), json={
            "name": "users", "format": "lance", "base-location": lance_path,
        }, timeout=10)

        r = requests.get(
            f"{self._generic_table_url(ns_name, 'users')}/credentials",
            timeout=10,
        )
        assert r.status_code == 200
        resp = r.json()

        storage_creds = resp.get("storage-credentials", [])
        assert len(storage_creds) > 0
        cred_config = storage_creds[0]["config"]
        assert "s3.access-key-id" in cred_config
        assert "s3.secret-access-key" in cred_config
        assert "s3.session-token" in cred_config

        temp_opts = _iceberg_creds_to_lance_storage_options(storage_creds, None)
        temp_opts["aws_endpoint"] = S3_ENDPOINT
        temp_opts["aws_region"] = S3_REGION
        temp_opts["allow_http"] = "true"

        ds = lance.dataset(lance_path, storage_options=temp_opts)
        assert ds.to_table().to_pandas().shape[0] == 5

        requests.delete(self._generic_table_url(ns_name, "users"), timeout=10)
        requests.delete(f"{CATALOG_URI}/v1/{self._prefix}/namespaces/{ns_name}", timeout=10)
