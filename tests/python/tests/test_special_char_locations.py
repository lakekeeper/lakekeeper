"""End-to-end tests for special characters in table locations.

For each char, create a table at a location containing it (via REST, no
Spark), then use vended creds via the per-cloud SDK to:
  - write at the table prefix → must succeed (proves encoding consistency)
  - write at a sibling path → must 403 (proves scope tightness)

`expect_deny` flips the positive write to "must 403" for cloud-side
limitations (e.g. MinIO doesn't resolve IAM `${*}` → safe over-restrict).
This pins the safe failure mode and catches a regression to over-permit.
"""

import dataclasses
import uuid
from typing import Optional
from urllib.parse import quote, urlparse

import conftest
import pytest
import requests


@dataclasses.dataclass(frozen=True)
class SpecialChar:
    """`url_segment` is the form that goes into the URL path — literal for
    sub-delims (`*`, `+`, `'`, `$`), percent-encoded otherwise."""

    id: str
    url_segment: str
    # (provider, flavor) → reason. Flavor `"*"` matches any.
    # `expect_deny`: cloud-side limitation, asserts positive deny (safe).
    expect_deny: dict = dataclasses.field(default_factory=dict)
    # `expect_create_reject`: Lakekeeper rejects createTable up-front
    # (e.g. ADLS rejects whitespace-only segments since Azure does).
    expect_create_reject: dict = dataclasses.field(default_factory=dict)


# MinIO does not resolve AWS IAM policy variables, so our `${*}`/`${$}`/`${?}`
# glob-escape is treated as a literal 4-char string. Deny is the safe outcome.
_MINIO_NO_IAM_VARS = "MinIO does not resolve IAM policy variables (${*}/${?}/${$})"

# Note: `..` and `.` are removed by RFC 3986 path normalisation in
# `url::Url`, so they can never round-trip in a URL-based location and are
# not testable here.
SPECIAL_CHARS = [
    SpecialChar("star", "*", expect_deny={("s3", "s3-compat"): _MINIO_NO_IAM_VARS}),
    SpecialChar("question", "%3F"),
    SpecialChar("dollar", "$", expect_deny={("s3", "s3-compat"): _MINIO_NO_IAM_VARS}),
    SpecialChar("squote", "'"),
    SpecialChar("plus", "+"),
    SpecialChar("dquote", "%22"),
    SpecialChar(
        "space",
        "%20",
        expect_create_reject={
            ("adls", "*"): "Azure ADLS rejects whitespace-only path segments"
        },
    ),
]


def _lookup(table: dict, provider: str, flavor: Optional[str]) -> Optional[str]:
    return table.get((provider, flavor or "")) or table.get((provider, "*"))

# Per-request timeout (seconds). Prevents CI hangs on a stalled cloud
# endpoint or slow Lakekeeper. Generous enough for cold cloud calls.
HTTP_TIMEOUT = 30

# Minimal Iceberg schema used for every funky-location table.
SCHEMA = {
    "type": "struct",
    "schema-id": 0,
    "fields": [{"id": 1, "name": "v", "type": "int", "required": False}],
}


def _provider(storage_config: dict) -> str:
    t = storage_config["storage-profile"]["type"]
    return "s3" if t in ("s3", "aws") else t


def _vending_enabled(storage_config: dict) -> bool:
    profile = storage_config["storage-profile"]
    if profile["type"] in ("s3", "aws"):
        return bool(profile.get("sts-enabled"))
    return True  # ADLS and GCS always vend


def _wh_url(warehouse: conftest.Warehouse) -> str:
    return (
        warehouse.server.catalog_url.rstrip("/") + f"/v1/{warehouse.warehouse_id}"
    )


def _auth(warehouse: conftest.Warehouse) -> dict:
    return {"Authorization": f"Bearer {warehouse.access_token}"}


def _create_namespace(warehouse: conftest.Warehouse, namespace: str) -> None:
    r = requests.post(
        f"{_wh_url(warehouse)}/namespaces",
        headers=_auth(warehouse),
        json={"namespace": [namespace], "properties": {}},
        timeout=HTTP_TIMEOUT,
    )
    r.raise_for_status()


def _load_namespace_location(
    warehouse: conftest.Warehouse, namespace: str
) -> str:
    r = requests.get(
        f"{_wh_url(warehouse)}/namespaces/{quote(namespace, safe='')}",
        headers=_auth(warehouse),
        timeout=HTTP_TIMEOUT,
    )
    r.raise_for_status()
    return r.json()["properties"]["location"].rstrip("/")


def _create_table(
    warehouse: conftest.Warehouse,
    namespace: str,
    table: str,
    location: str,
) -> dict:
    r = requests.post(
        f"{_wh_url(warehouse)}/namespaces/{quote(namespace, safe='')}/tables",
        headers=_auth(warehouse),
        json={"name": table, "location": location, "schema": SCHEMA},
        timeout=HTTP_TIMEOUT,
    )
    r.raise_for_status()
    return r.json()


def _load_table_with_creds(
    warehouse: conftest.Warehouse, namespace: str, table: str
) -> dict:
    """REST `loadTable` with vended-credentials header. Returns the parsed
    response (`metadata`, `config`, …)."""
    r = requests.get(
        f"{_wh_url(warehouse)}/namespaces/{quote(namespace, safe='')}/tables/{quote(table, safe='')}",
        headers={
            **_auth(warehouse),
            "X-Iceberg-Access-Delegation": "vended-credentials",
        },
        timeout=HTTP_TIMEOUT,
    )
    r.raise_for_status()
    return r.json()


def _try_write(provider: str, config: dict, target_url: str) -> Optional[str]:
    """Attempt a small object PUT at `target_url` using vended creds.
    Returns None on SUCCESS, or a short description of the failure."""
    if provider == "s3":
        import boto3
        import botocore.exceptions

        parsed = urlparse(target_url)
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")
        s3 = boto3.client(
            "s3",
            aws_access_key_id=config.get("s3.access-key-id"),
            aws_secret_access_key=config.get("s3.secret-access-key"),
            aws_session_token=config.get("s3.session-token"),
            endpoint_url=config.get("s3.endpoint") or None,
            region_name=config.get("s3.region") or None,
        )
        try:
            s3.put_object(Bucket=bucket, Key=key, Body=b"canary")
        except botocore.exceptions.ClientError as e:
            return f"s3 ClientError: {e.response.get('Error', {}).get('Code', '?')}"
        return None
    if provider == "adls":
        sas_key = next((k for k in config if k.startswith("adls.sas-token.")), None)
        assert sas_key, f"no adls.sas-token.* in config: {list(config)}"
        sas = config[sas_key].lstrip("?")
        parsed = urlparse(target_url)
        fs = parsed.username
        # Vended SAS is a Blob Service SAS — use the blob endpoint (single
        # PUT) rather than DFS (which would need 3-step create/append/flush).
        host = (parsed.hostname or "").replace(".dfs.", ".blob.")
        path = parsed.path
        https_url = f"https://{host}/{fs}{path}?{sas}"
        r = requests.put(
            https_url,
            headers={"x-ms-blob-type": "BlockBlob"},
            data=b"canary",
            timeout=HTTP_TIMEOUT,
        )
        if 200 <= r.status_code < 300:
            return None
        return f"adls HTTP {r.status_code}: {r.text[:200]}"
    if provider == "gcs":
        token = config.get("gcs.oauth2.token")
        assert token, f"no gcs.oauth2.token in config: {list(config)}"
        parsed = urlparse(target_url)
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")
        https_url = f"https://storage.googleapis.com/{bucket}/{quote(key, safe='/')}"
        r = requests.put(
            https_url,
            headers={"Authorization": f"Bearer {token}"},
            data=b"canary",
            timeout=HTTP_TIMEOUT,
        )
        if 200 <= r.status_code < 300:
            return None
        return f"gcs HTTP {r.status_code}: {r.text[:200]}"
    raise AssertionError(f"unknown provider: {provider}")


@pytest.mark.parametrize("char", SPECIAL_CHARS, ids=lambda c: c.id)
def test_special_char_in_location(
    warehouse: conftest.Warehouse, storage_config, char: SpecialChar
):
    if not _vending_enabled(storage_config):
        pytest.skip("requires vended credentials")
    provider = _provider(storage_config)
    flavor: Optional[str] = storage_config["storage-profile"].get("flavor")
    expect_deny = _lookup(char.expect_deny, provider, flavor)
    expect_create_reject = _lookup(char.expect_create_reject, provider, flavor)

    ns_name = f"sc_{char.id}_{uuid.uuid4().hex[:8]}"
    table_name = "data"
    table_dir = f"sc_{char.id}"

    _create_namespace(warehouse, ns_name)
    ns_location = _load_namespace_location(warehouse, ns_name)

    table_location = f"{ns_location}/{table_dir}/{char.url_segment}/data/"
    sibling_location = f"{ns_location}/{table_dir}_evil/canary"

    if expect_create_reject:
        with pytest.raises(requests.HTTPError) as excinfo:
            _create_table(warehouse, ns_name, table_name, table_location)
        assert excinfo.value.response.status_code == 400, (
            f"expected 400 from createTable for {char.id} on {provider}/{flavor} "
            f"({expect_create_reject}); got {excinfo.value.response.status_code}"
        )
        return

    _create_table(warehouse, ns_name, table_name, table_location)

    loaded = _load_table_with_creds(warehouse, ns_name, table_name)
    config = loaded.get("config", {})
    stored_location = loaded["metadata"]["location"].rstrip("/")
    if char.url_segment not in stored_location:
        pytest.skip(
            f"Lakekeeper normalised `{char.url_segment}` out of the location "
            f"(requested {table_location.rstrip('/')}, stored {stored_location})"
        )

    # Positive: write at the table prefix.
    target = f"{stored_location}/canary.txt"
    err = _try_write(provider, config, target)
    if expect_deny:
        # Cloud-side limitation (not a Lakekeeper bug). Pin the *safe*
        # outcome: must deny, not allow. A regression to over-permit would
        # be a security issue.
        assert err is not None, (
            f"expected deny (cloud-side limitation: {expect_deny}) but write "
            f"to {target} SUCCEEDED — credential scope is over-permissive on "
            f"{provider}/{flavor} for char {char.id}"
        )
        return  # No point testing the negative — creds are already too tight.

    assert err is None, (
        f"vended credentials failed to write at the table prefix\n"
        f"  char         = {char.id}\n"
        f"  table_loc    = {table_location}\n"
        f"  stored_loc   = {stored_location}\n"
        f"  target       = {target}\n"
        f"  config_keys  = {sorted(config)}\n"
        f"  error        = {err}"
    )

    # Negative: vended creds for the table must NOT allow a sibling write.
    err = _try_write(provider, config, sibling_location)
    assert err is not None, (
        f"vended credentials for {table_location} unexpectedly allowed write "
        f"to {sibling_location} (provider={provider}, char={char.id}). "
        f"Credential scope is too broad."
    )
