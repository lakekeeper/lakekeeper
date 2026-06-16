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
_ONELAKE_PERCENT_COLLAPSE = (
    "OneLake collapses any `%XX` in the blob path to its decoded character, "
    "breaking the byte-literal model — Lakekeeper rejects such paths at create time."
)

SPECIAL_CHARS = [
    SpecialChar("star", "*", expect_deny={("s3", "minio"): _MINIO_NO_IAM_VARS}),
    SpecialChar(
        "question",
        "%3F",
        expect_create_reject={("onelake", "*"): _ONELAKE_PERCENT_COLLAPSE},
    ),
    SpecialChar("dollar", "$", expect_deny={("s3", "minio"): _MINIO_NO_IAM_VARS}),
    SpecialChar("squote", "'"),
    SpecialChar("plus", "+"),
    SpecialChar(
        "dquote",
        "%22",
        expect_create_reject={("onelake", "*"): _ONELAKE_PERCENT_COLLAPSE},
    ),
    SpecialChar(
        "space",
        "%20",
        expect_create_reject={
            ("adls", "*"): "Azure ADLS rejects whitespace-only path segments",
            ("onelake", "*"): _ONELAKE_PERCENT_COLLAPSE,
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


def _try_write_payload(
    provider: str, config: dict, target_url: str, payload: bytes
) -> Optional[str]:
    """Attempt a small object PUT at `target_url` using vended creds.
    Returns None on SUCCESS, or a short description of the failure.
    Caller-supplied payload lets a test write a distinguishing value per
    table and detect cross-reads."""
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
            s3.put_object(Bucket=bucket, Key=key, Body=payload)
        except botocore.exceptions.ClientError as e:
            return f"s3 ClientError: {e.response.get('Error', {}).get('Code', '?')}"
        return None
    if provider in ("adls", "onelake"):
        sas_key = next((k for k in config if k.startswith("adls.sas-token.")), None)
        assert sas_key, f"no adls.sas-token.* in config: {list(config)}"
        sas = config[sas_key].lstrip("?")
        parsed = urlparse(target_url)
        fs = parsed.username
        # Vended SAS is a Blob Service SAS — use the blob endpoint (single
        # PUT) rather than DFS (which would need 3-step create/append/flush).
        host = (parsed.hostname or "").replace(".dfs.", ".blob.")
        # Pre-encode `%` → `%25` so that any literal `%XX` in the catalog's
        # raw fs_location reaches Azure as bytes (server URL-decodes once).
        # Without this, `Abc` and `%41bc` would alias on the wire — the same
        # bug we fixed in `AdlsLocation::blob_name` on the Rust side.
        path = parsed.path.replace("%", "%25")
        https_url = f"https://{host}/{fs}{path}?{sas}"
        r = requests.put(
            https_url,
            headers={"x-ms-blob-type": "BlockBlob"},
            data=payload,
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
            data=payload,
            timeout=HTTP_TIMEOUT,
        )
        if 200 <= r.status_code < 300:
            return None
        return f"gcs HTTP {r.status_code}: {r.text[:200]}"
    raise AssertionError(f"unknown provider: {provider}")


def _safe_url(url: str) -> str:
    """Strip query and fragment from a URL so it can be safely embedded in
    error messages without leaking SAS tokens or other auth material."""
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"


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
    err = _try_write_payload(provider, config, target, b"canary")
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
    err = _try_write_payload(provider, config, sibling_location, b"canary")
    assert err is not None, (
        f"vended credentials for {table_location} unexpectedly allowed write "
        f"to {sibling_location} (provider={provider}, char={char.id}). "
        f"Credential scope is too broad."
    )


@dataclasses.dataclass(frozen=True)
class AliasPair:
    """A pair of byte-different path segments that share the same URI-decoded
    form. Under the byte-literal storage-key model these address two distinct
    storage objects; the catalog must accept both as distinct rows and the
    vended creds for one must not unlock the other.
    """

    id: str
    lhs: str  # decoded form, e.g. "Abc"
    rhs: str  # encoded form, e.g. "%41bc"


# Pairs that differ only by percent-encoding of an unreserved/sub-delim char,
# or by hex-case in a surviving %XX. The Rust storage-layer integration test
# (`test_percent_encoding_does_not_alias`) proves SDK-level distinctness for
# each pair on every backend; this Python test proves the full Lakekeeper
# REST → vended-creds → SDK chain preserves it end-to-end.
ALIAS_DISTINCT_PAIRS = [
    AliasPair("alpha_A", "Abc", "%41bc"),
    AliasPair("dash", "foo-bar", "foo%2Dbar"),
    AliasPair("plus", "foo+bar", "foo%2Bbar"),
    AliasPair("hex_case_Q", "%3F", "%3f"),
]


def _try_read(provider: str, config: dict, target_url: str) -> Optional[bytes]:
    """Attempt a small object GET at `target_url` using vended creds.
    Returns the body on SUCCESS, or None on access denial / not-found.
    Any unexpected error raises so the test fails loudly rather than
    silently masking a regression."""
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
            obj = s3.get_object(Bucket=bucket, Key=key)
            return obj["Body"].read()
        except botocore.exceptions.ClientError as e:
            code = e.response.get("Error", {}).get("Code", "?")
            if code in ("AccessDenied", "NoSuchKey", "403", "404"):
                return None
            raise
    if provider in ("adls", "onelake"):
        sas_key = next((k for k in config if k.startswith("adls.sas-token.")), None)
        assert sas_key, f"no adls.sas-token.* in config: {list(config)}"
        sas = config[sas_key].lstrip("?")
        parsed = urlparse(target_url)
        fs = parsed.username
        host = (parsed.hostname or "").replace(".dfs.", ".blob.")
        # urlparse keeps the path raw — but the Azure server URL-decodes
        # exactly once. We need the wire bytes to match the catalog's raw
        # fs_location, so any literal `%` in the user-supplied path must
        # itself be wire-encoded (`%` → `%25`). This is the same fix we
        # made in the Rust SDK call site (`AdlsLocation::blob_name`).
        path = parsed.path.replace("%", "%25")
        https_url = f"https://{host}/{fs}{path}?{sas}"
        r = requests.get(https_url, timeout=HTTP_TIMEOUT)
        if 200 <= r.status_code < 300:
            return r.content
        # 401 (signature mismatch on canonical resource) is the correct outcome
        # when a SAS scoped to path A is presented against URL B — OneLake
        # treats it as "this SAS doesn't authorise that URL", same security
        # meaning as 403/404. Group all three as "denied".
        if r.status_code in (401, 403, 404):
            return None
        raise AssertionError(
            f"adls GET {_safe_url(https_url)} HTTP {r.status_code} "
            f"({r.reason}): {r.text[:200]}"
        )
    if provider == "gcs":
        token = config.get("gcs.oauth2.token")
        assert token, f"no gcs.oauth2.token in config: {list(config)}"
        parsed = urlparse(target_url)
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")
        https_url = f"https://storage.googleapis.com/{bucket}/{quote(key, safe='/')}"
        r = requests.get(
            https_url,
            headers={"Authorization": f"Bearer {token}"},
            timeout=HTTP_TIMEOUT,
        )
        if 200 <= r.status_code < 300:
            return r.content
        if r.status_code in (403, 404):
            return None
        raise AssertionError(
            f"gcs GET {_safe_url(https_url)} HTTP {r.status_code} "
            f"({r.reason}): {r.text[:200]}"
        )
    raise AssertionError(f"unknown provider: {provider}")


@pytest.mark.parametrize("pair", ALIAS_DISTINCT_PAIRS, ids=lambda p: p.id)
def test_alias_distinct_pair_credential_isolation(
    warehouse: conftest.Warehouse, storage_config, pair: AliasPair
):
    """End-to-end proof of the byte-literal model: two byte-different paths
    that the URI spec considers "equivalent" produce two genuinely separate
    tables, and the vended credentials for one cannot reach the other.

    Catches three regression classes:
    1. Catalog re-introduces canonicalisation (one of the two creates fails).
    2. Vended-cred prefix scope decodes the stored fs_location (an over-broad
       prefix would cover BOTH storage paths, allowing cross-table access).
    3. Storage SDK aliases the two keys at the wire layer (regresses the
       fix in `AdlsLocation::blob_name` or its analogue on other clouds).
    """
    if not _vending_enabled(storage_config):
        pytest.skip("requires vended credentials")
    provider = _provider(storage_config)
    if provider == "onelake":
        # OneLake collapses any `%XX` in blob names to its decoded character,
        # so `%41bc` and `Abc` would alias rather than be distinct.
        # Lakekeeper's OneLake storage profile rejects `%`-bearing path
        # segments at create time to avoid silently aliasing tables — meaning
        # the byte-literal invariant this test asserts is, on OneLake,
        # achieved by rejection rather than by isolation.
        pytest.skip("OneLake doesn't support the byte-literal path model")

    ns_name = f"alias_{pair.id}_{uuid.uuid4().hex[:8]}"
    _create_namespace(warehouse, ns_name)
    ns_location = _load_namespace_location(warehouse, ns_name)

    # Same parent directory — only the trailing segment differs. This is the
    # tightest test of byte-literal isolation: if anything in the chain
    # collapses lhs↔rhs, the second create fails or the cross-table read
    # succeeds.
    parent = f"{ns_location}/aliasdir"
    loc_a = f"{parent}/{pair.lhs}/data/"
    loc_b = f"{parent}/{pair.rhs}/data/"
    table_a = "table_a"
    table_b = "table_b"

    # Both creates must succeed. If catalog canonicalisation collapses the
    # two locations, the second create raises 409 / LocationAlreadyTaken.
    _create_table(warehouse, ns_name, table_a, loc_a)
    _create_table(warehouse, ns_name, table_b, loc_b)

    loaded_a = _load_table_with_creds(warehouse, ns_name, table_a)
    loaded_b = _load_table_with_creds(warehouse, ns_name, table_b)
    config_a = loaded_a.get("config", {})
    config_b = loaded_b.get("config", {})
    stored_a = loaded_a["metadata"]["location"].rstrip("/")
    stored_b = loaded_b["metadata"]["location"].rstrip("/")

    # If the catalog round-tripped a different string than what we sent,
    # canonicalisation has crept back in. Skip rather than misdiagnose
    # (the createTable success above already proved no collision).
    if pair.lhs not in stored_a or pair.rhs not in stored_b:
        pytest.skip(
            f"catalog rewrote location segment(s) — "
            f"sent {loc_a!r}/{loc_b!r}, stored {stored_a!r}/{stored_b!r}"
        )
    assert stored_a != stored_b, (
        f"catalog returned identical stored locations for byte-different "
        f"inputs ({pair.lhs!r}, {pair.rhs!r}) — alias collapse: {stored_a!r}"
    )

    # 1. Write a distinguishing payload to A using A's vended creds.
    payload_a = f"PAYLOAD-A-{pair.id}".encode()
    target_a = f"{stored_a}/canary.txt"
    err = _try_write_payload(provider, config_a, target_a, payload_a)
    assert err is None, (
        f"A's vended creds failed at A's own path {target_a!r}: {err}"
    )

    # 2. Try to read A's payload using B's vended creds at A's wire URL.
    # If cred scope leaks (e.g. prefix decoded), this returns the bytes.
    leaked = _try_read(provider, config_b, target_a)
    assert leaked is None, (
        f"ALIAS / SCOPE LEAK: B's vended creds for {stored_b!r} read A's "
        f"object at {target_a!r} (got {leaked!r}). Either the SDK aliased "
        f"the two paths or the cred prefix decoded byte-different forms."
    )

    # 3. Try to read A's payload using B's wire URL with B's creds — i.e.
    # if the SDK or server aliases lhs↔rhs at wire time, B's creds + B's
    # URL would resolve to A's storage object.
    target_b = f"{stored_b}/canary.txt"
    body_b = _try_read(provider, config_b, target_b)
    # Object at B's path doesn't exist yet — must NOT return A's bytes.
    assert body_b is None or body_b != payload_a, (
        f"WIRE-LEVEL ALIAS: reading B's path {target_b!r} returned A's "
        f"payload. SDK or server collapsed {pair.lhs!r}↔{pair.rhs!r}."
    )

    # 4. B remains independently functional — write at its own prefix.
    payload_b = f"PAYLOAD-B-{pair.id}".encode()
    err = _try_write_payload(provider, config_b, target_b, payload_b)
    assert err is None, (
        f"B's vended creds failed at B's own path {target_b!r}: {err}"
    )
    body_b = _try_read(provider, config_b, target_b)
    assert body_b == payload_b, (
        f"B's creds at B's path returned wrong bytes: got {body_b!r}, "
        f"expected {payload_b!r}"
    )
